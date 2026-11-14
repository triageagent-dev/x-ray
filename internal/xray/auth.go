package xray

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// stableUserID returns the first 16 hex chars of SHA-256(raw).
// Used as the subject key for agent_note items — raw SSO IDs are never stored.
func stableUserID(raw string) string {
	h := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(h[:])[:16]
}

// ─── OAuth state cookie (login CSRF protection) ─────────────────────────────────

// oauthStateCookie is the HttpOnly cookie that binds an in-flight OAuth login to
// the browser that started it. The callback requires the query `state` to match
// this cookie, defeating login-CSRF / session-fixation where an attacker injects
// their own authorization code into a victim's browser.
const oauthStateCookie = "xray_oauth_state"

// stateSigningKey is generated once per process. State values are short-lived
// (10 min) and single-use, so a per-process key is sufficient — a restart simply
// invalidates any login in flight, which is harmless.
var stateSigningKey = func() []byte {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return k
}()

// signState returns "<state>.<hex-hmac>" so the callback can verify the cookie
// was issued by this server and not forged or tampered with.
func signState(state string) string {
	mac := hmac.New(sha256.New, stateSigningKey)
	mac.Write([]byte(state))
	return state + "." + hex.EncodeToString(mac.Sum(nil))
}

// setStateCookie issues the signed state cookie. Secure+HttpOnly+SameSite=Lax
// matches the session cookie; Lax is required so the cookie survives the
// cross-site redirect back from the OAuth provider. base is BasePath(PublicURL).
func setStateCookie(w http.ResponseWriter, base, state string) {
	http.SetCookie(w, &http.Cookie{
		Name:     oauthStateCookie,
		Value:    signState(state),
		Path:     base + "/v1/auth/",
		MaxAge:   600,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
}

// clearStateCookie expires the state cookie once the callback has consumed it.
func clearStateCookie(w http.ResponseWriter, base string) {
	http.SetCookie(w, &http.Cookie{
		Name:     oauthStateCookie,
		Path:     base + "/v1/auth/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
}

// stateCookieMatches reports whether the request carries a valid state cookie
// that matches the given query state. Constant-time compares guard against both
// signature- and value-oracle timing attacks.
func stateCookieMatches(r *http.Request, queryState string) bool {
	if queryState == "" {
		return false
	}
	c, err := r.Cookie(oauthStateCookie)
	if err != nil {
		return false
	}
	state, sig, ok := strings.Cut(c.Value, ".")
	if !ok {
		return false
	}
	mac := hmac.New(sha256.New, stateSigningKey)
	mac.Write([]byte(state))
	want := mac.Sum(nil)
	got, err := hex.DecodeString(sig)
	if err != nil || subtle.ConstantTimeCompare(got, want) != 1 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(state), []byte(queryState)) == 1
}

// ─── session store ────────────────────────────────────────────────────────────

type sessionStore struct {
	mu      sync.Mutex
	entries map[string]sessionEntry
	// restored holds sessions rehydrated from CR status, keyed by their TokenHMAC.
	// It is a separate map so that the persisted hash is never itself a valid
	// credential: validate looks a presented token up in entries by its raw value
	// and in restored only by HMAC(token).
	restored map[string]sessionEntry
	// hmacKey signs session tokens before they are persisted to the
	// system_configuration CR status, so the raw token never lands in etcd. It is
	// derived from a stable secret (the OAuth client secret) so the HMAC of a token
	// is identical across restarts and replicas — a session persisted by one pod
	// validates on another. Nil until SetSessionPersistence wires it; persistence
	// is disabled (in-memory only) while nil. See ADR-047.
	hmacKey []byte
	// onChange is called after every create/invalidate so the caller can flush the
	// session set to CR status. Nil disables flushing (local dev without k8s).
	onChange func()
}

type sessionEntry struct {
	username  string // display name / login (human-readable, not unique across providers)
	userID    string // provider-scoped unique ID: "github:<numeric>", "azure:<oid>" or "google:<sub>"
	expiresAt time.Time
}

var sessions = &sessionStore{entries: make(map[string]sessionEntry), restored: make(map[string]sessionEntry)}

// SetSessionPersistence enables CR-backed session persistence: tokens are hashed
// with hmacKey (derived from a stable secret so it survives restarts and matches
// across replicas) and onChange is invoked after each login/logout to flush the
// set to status. Called once at startup from main.go; a nil hmacKey leaves
// sessions in-memory only. See ADR-047.
func SetSessionPersistence(hmacKey []byte, onChange func()) {
	sessions.mu.Lock()
	sessions.hmacKey = hmacKey
	sessions.onChange = onChange
	sessions.mu.Unlock()
}

// tokenHMAC returns hex(HMAC-SHA256(token, hmacKey)). Caller holds no lock
// requirement beyond a stable hmacKey (set once at startup). Returns "" when
// persistence is disabled.
func (s *sessionStore) tokenHMAC(token string) string {
	if len(s.hmacKey) == 0 {
		return ""
	}
	mac := hmac.New(sha256.New, s.hmacKey)
	mac.Write([]byte(token))
	return hex.EncodeToString(mac.Sum(nil))
}

// identityAEAD returns an AES-256-GCM cipher keyed by a derivation of hmacKey
// (domain-separated from the token HMAC so the two uses can never collide). The
// key is the SHA-256 of hmacKey ‖ "identity", i.e. exactly 32 bytes. Returns nil
// when persistence is disabled.
func (s *sessionStore) identityAEAD() cipher.AEAD {
	if len(s.hmacKey) == 0 {
		return nil
	}
	dk := sha256.Sum256(append(append([]byte(nil), s.hmacKey...), []byte("identity")...))
	block, err := aes.NewCipher(dk[:])
	if err != nil {
		return nil // unreachable: dk is always 32 bytes
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil
	}
	return gcm
}

// encryptIdentity seals "username\x00userID" so the cleartext display name and SSO
// ID never reach CR status. Output is hex(nonce ‖ ciphertext). Returns "" when
// persistence is disabled.
func (s *sessionStore) encryptIdentity(username, userID string) string {
	gcm := s.identityAEAD()
	if gcm == nil {
		return ""
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	sealed := gcm.Seal(nonce, nonce, []byte(username+"\x00"+userID), nil)
	return hex.EncodeToString(sealed)
}

// decryptIdentity reverses encryptIdentity. Returns ok=false if persistence is
// disabled, the blob is malformed, or authentication fails (tampered/foreign key).
func (s *sessionStore) decryptIdentity(enc string) (username, userID string, ok bool) {
	gcm := s.identityAEAD()
	if gcm == nil {
		return "", "", false
	}
	raw, err := hex.DecodeString(enc)
	if err != nil || len(raw) < gcm.NonceSize() {
		return "", "", false
	}
	nonce, ct := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", "", false
	}
	u, id, found := strings.Cut(string(pt), "\x00")
	if !found {
		return "", "", false
	}
	return u, id, true
}

func init() {
	go func() {
		for range time.Tick(time.Hour) {
			now := time.Now()
			sessions.mu.Lock()
			for k, e := range sessions.entries {
				if now.After(e.expiresAt) {
					delete(sessions.entries, k)
				}
			}
			for k, e := range sessions.restored {
				if now.After(e.expiresAt) {
					delete(sessions.restored, k)
				}
			}
			sessions.mu.Unlock()
		}
	}()
}

func (s *sessionStore) create(username, userID string) string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	token := hex.EncodeToString(b)
	s.mu.Lock()
	s.entries[token] = sessionEntry{username: username, userID: userID, expiresAt: time.Now().Add(24 * time.Hour)}
	onChange := s.onChange
	s.mu.Unlock()
	if onChange != nil {
		onChange()
	}
	return token
}

// validate returns (username, userID, ok).
//
// A session created on this process is keyed by its raw token. A session restored
// from CR status after a restart (or created on another replica) sits in restored,
// keyed by its TokenHMAC, since the raw token is never persisted — so a raw-token
// miss falls back to a lookup of HMAC(token) there. On that hit the entry moves to
// entries under the raw token so subsequent requests take the fast path. The
// persisted hash itself matches nothing: it is not a key of entries, and
// HMAC(hash) is not a key of restored.
func (s *sessionStore) validate(token string) (string, string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[token]
	if !ok {
		if h := s.tokenHMAC(token); h != "" {
			if he, hok := s.restored[h]; hok {
				delete(s.restored, h)
				s.entries[token] = he
				e, ok = he, true
			}
		}
	}
	if !ok || time.Now().After(e.expiresAt) {
		delete(s.entries, token)
		return "", "", false
	}
	return e.username, e.userID, true
}

func (s *sessionStore) invalidate(token string) {
	s.mu.Lock()
	_, existed := s.entries[token]
	delete(s.entries, token)
	if h := s.tokenHMAC(token); h != "" {
		if _, ok := s.restored[h]; ok {
			existed = true
			delete(s.restored, h)
		}
	}
	onChange := s.onChange
	s.mu.Unlock()
	if existed && onChange != nil {
		onChange()
	}
}

// snapshotSessions returns one persistable row per live session, hashing each
// token so the raw credential never reaches CR status, and prunes expired entries
// from the in-memory map so it (and the status it backs) stays bounded by active
// sessions. Returns nil when persistence is disabled (no hmacKey) or there is
// nothing live to persist. Mirrors snapshotUserDaily. See ADR-047.
func (s *sessionStore) snapshotSessions() []mcell.UserSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.hmacKey) == 0 {
		return nil
	}
	now := time.Now()
	var rows []mcell.UserSession
	for token, e := range s.entries {
		if now.After(e.expiresAt) {
			delete(s.entries, token)
			continue
		}
		rows = append(rows, mcell.UserSession{
			UserHash:    stableUserID(e.userID),
			TokenHMAC:   s.tokenHMAC(token),
			IdentityEnc: s.encryptIdentity(e.username, e.userID),
			ExpiresAt:   e.expiresAt.UTC().Format(time.RFC3339),
		})
	}
	for h, e := range s.restored {
		if now.After(e.expiresAt) {
			delete(s.restored, h)
			continue
		}
		rows = append(rows, mcell.UserSession{
			UserHash:    stableUserID(e.userID),
			TokenHMAC:   h,
			IdentityEnc: s.encryptIdentity(e.username, e.userID),
			ExpiresAt:   e.expiresAt.UTC().Format(time.RFC3339),
		})
	}
	return rows
}

// restoreSessions rehydrates persisted sessions into the in-memory map. Like
// RestoreStats it runs on every system_configuration watch event (not just at
// startup), and flushSessions writes that same CR — so each flush echoes back
// here. We therefore key the restored map by TokenHMAC and never overwrite a token we
// already hold (the live entry is authoritative; a self-echoed snapshot must not
// clobber it), nor restore an expired or unparseable row. A genuine restart starts
// from an empty map and rehydrates fully. Persistence stores only the HMAC, so the
// raw token is unrecoverable — restored entries are keyed by their HMAC and matched
// in validate() by hashing the incoming cookie. See ADR-047.
// restoreResult breaks down a restore pass so the caller can log meaningfully.
// `undecryptable` is the only field that signals a real problem (key mismatch /
// tampering); `added`==0 with `alreadyHeld`>0 is the normal self-echo case (the
// flush re-writes the same CR, so later watch events re-deliver rows we already
// hold) and must NOT warn. `disabled` means persistence is off (no key).
type restoreResult struct {
	added         int
	alreadyHeld   int
	undecryptable int
	disabled      bool
}

func (s *sessionStore) restoreSessions(rows []mcell.UserSession) restoreResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.hmacKey) == 0 {
		return restoreResult{disabled: true}
	}
	var res restoreResult
	if s.restored == nil {
		s.restored = make(map[string]sessionEntry)
	}
	// Index the tokens we already hold (live, raw) by their HMAC so a self-echoed
	// row does not create a duplicate keyed by the hash.
	have := make(map[string]struct{}, len(s.entries))
	for token := range s.entries {
		have[s.tokenHMAC(token)] = struct{}{}
	}
	now := time.Now()
	for _, r := range rows {
		if r.TokenHMAC == "" {
			continue
		}
		exp, err := time.Parse(time.RFC3339, r.ExpiresAt)
		if err != nil || now.After(exp) {
			continue // expired or unparseable — pruned, not an error
		}
		if _, ok := have[r.TokenHMAC]; ok {
			res.alreadyHeld++
			continue // already held as a live raw token (self-echo)
		}
		if _, ok := s.restored[r.TokenHMAC]; ok {
			res.alreadyHeld++
			continue // already restored under its HMAC key (self-echo)
		}
		username, userID, ok := s.decryptIdentity(r.IdentityEnc)
		if !ok {
			res.undecryptable++ // tampered, malformed, or sealed under a different key
			continue
		}
		s.restored[r.TokenHMAC] = sessionEntry{username: username, userID: userID, expiresAt: exp}
		res.added++
	}
	return res
}

// ─── rate limiter ─────────────────────────────────────────────────────────────

// keyedRateLimiter is a per-key token-bucket limiter. It bounds how often a
// given key (user hash, MCP token, or client IP) may perform an expensive
// operation — voice session starts and MCP tool calls drive Gemini/Vertex
// spend, so unbounded calls are a financial DoS vector. Idle keys are pruned
// by a background sweep so the map cannot grow without bound.
type keyedRateLimiter struct {
	mu      sync.Mutex
	entries map[string]*limiterEntry
	every   rate.Limit
	burst   int
}

type limiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// newKeyedRateLimiter allows burst events immediately, then refills at one
// event per `interval`. A zero interval disables limiting (allow always true).
func newKeyedRateLimiter(interval time.Duration, burst int) *keyedRateLimiter {
	l := &keyedRateLimiter{entries: make(map[string]*limiterEntry), burst: burst}
	if interval > 0 {
		l.every = rate.Every(interval)
		go l.sweep()
	}
	return l
}

func (l *keyedRateLimiter) allow(key string) bool {
	// A nil or zero-interval limiter is the "disabled" state — allow everything.
	if l == nil || l.every == 0 {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[key]
	if !ok {
		e = &limiterEntry{limiter: rate.NewLimiter(l.every, l.burst)}
		l.entries[key] = e
	}
	e.lastSeen = time.Now()
	return e.limiter.Allow()
}

// sweep evicts entries unused for an hour to bound memory.
func (l *keyedRateLimiter) sweep() {
	for range time.Tick(time.Hour) {
		cutoff := time.Now().Add(-time.Hour)
		l.mu.Lock()
		for k, e := range l.entries {
			if e.lastSeen.Before(cutoff) {
				delete(l.entries, k)
			}
		}
		l.mu.Unlock()
	}
}

// trustedProxyHops is the number of trusted reverse proxies in front of the
// server. It controls how X-Forwarded-For is interpreted for rate-limit keying.
// Set once at startup via SetTrustedProxyHops (default 0).
//
// The leftmost XFF entry is fully attacker-controlled: any client can send
// `X-Forwarded-For: <random>` and, since each request gets a fresh value, defeat
// per-IP rate limiting entirely. The trustworthy entry is the one the OUTERMOST
// trusted proxy appended — i.e. the Nth from the right when there are N trusted
// hops. With 0 trusted hops we ignore XFF/X-Real-IP completely and key on the
// real TCP peer (RemoteAddr), which cannot be spoofed.
var trustedProxyHops int

// SetTrustedProxyHops configures how many trusted reverse proxies sit in front
// of the server (e.g. 1 for a single ingress/LB that appends the client IP).
// Negative values are clamped to 0.
func SetTrustedProxyHops(n int) {
	if n < 0 {
		n = 0
	}
	trustedProxyHops = n
}

// clientIP derives a spoof-resistant rate-limit key from the request. It trusts
// X-Forwarded-For only up to trustedProxyHops entries from the right (the hops
// we actually control); everything to the left of that is client-supplied and
// ignored. With 0 trusted hops it uses the TCP peer address. realIP (X-Real-IP)
// is honoured only when a trusted proxy is configured, since it is equally
// forgeable by a direct client.
func clientIP(forwarded, realIP, remoteAddr string) string {
	if trustedProxyHops > 0 && forwarded != "" {
		parts := strings.Split(forwarded, ",")
		// The entry appended by the outermost trusted proxy is trustedProxyHops
		// from the right. Anything further left was supplied by an untrusted
		// upstream (possibly the client) and must not be trusted.
		idx := len(parts) - trustedProxyHops
		if idx < 0 {
			idx = 0
		}
		if ip := strings.TrimSpace(parts[idx]); ip != "" {
			return ip
		}
	}
	if trustedProxyHops > 0 && realIP != "" {
		return strings.TrimSpace(realIP)
	}
	// No trusted proxy (or no usable forwarded header): key on the real peer.
	// Strip the port so all connections from one host share a bucket.
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	if remoteAddr != "" {
		return remoteAddr
	}
	return "unknown"
}

// ─── session carriers ─────────────────────────────────────────────────────────

// wsBearerCredentialProtocol is the sentinel a browser sends as the first
// WebSocket subprotocol to mark the second one as a session token.
const wsBearerCredentialProtocol = "bearer"

// wsBearerToken extracts a session token from `Sec-WebSocket-Protocol: bearer,
// <token>`.
//
// This carrier exists because a browser cannot set headers on a WebSocket
// handshake — `new WebSocket(url, protocols)` is the only channel it has for
// sending a credential, so a UI holding a bearer token instead of a cookie has
// no other way to authenticate the upgrade. Same technique the Kubernetes API
// server uses for `kubectl exec`.
//
// Read ONLY from an actual upgrade request. The header is meaningless anywhere
// else, and refusing it elsewhere keeps this from quietly becoming a second,
// less obvious way to authenticate every route.
func wsBearerToken(r *http.Request) string {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return ""
	}
	for _, v := range r.Header.Values("Sec-WebSocket-Protocol") {
		parts := strings.Split(v, ",")
		for i, p := range parts {
			if strings.EqualFold(strings.TrimSpace(p), wsBearerCredentialProtocol) && i+1 < len(parts) {
				if tok := strings.TrimSpace(parts[i+1]); tok != "" {
					return tok
				}
			}
		}
	}
	return ""
}

// sessionFromRequest resolves the caller's session from any accepted carrier,
// in order: the xray_session cookie (same-origin browser), an Authorization:
// Bearer header (API clients, and a UI served from another origin), then the
// WebSocket subprotocol (the same UI's voice upgrade, which cannot set headers).
//
// One function so the three call sites — the requireSession gate, /v1/auth/me,
// and the voice upgrade — can never disagree about who the caller is.
func sessionFromRequest(r *http.Request) (username, userID string, ok bool) {
	if c, err := r.Cookie("xray_session"); err == nil {
		if u, id, valid := sessions.validate(c.Value); valid && grants.allowsUser(id, u) {
			return u, id, true
		}
	}
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		if u, id, valid := sessions.validate(auth[7:]); valid && grants.allowsUser(id, u) {
			return u, id, true
		}
	}
	if tok := wsBearerToken(r); tok != "" {
		if u, id, valid := sessions.validate(tok); valid && grants.allowsUser(id, u) {
			return u, id, true
		}
	}
	return "", "", false
}

// ─── cross-origin UI login hand-off ───────────────────────────────────────────

// loginState is the server-side half of an in-flight OAuth login.
type loginState struct {
	expiresAt time.Time
	// uiOrigin is the allowlisted origin to hand the session token to once the
	// callback completes. Empty — the default — redirects to "/" on this host,
	// which is the same-origin case where the cookie is the whole credential.
	uiOrigin string
}

// resolveUIOrigin validates a requested `?ui=` origin against the configured
// allowlist and returns it, or "" to fall back to the same-origin redirect.
//
// An allowlist is not optional here: this value decides where a freshly minted
// session token is sent, so accepting an arbitrary one would be an open
// redirect that hands the attacker's site a live session. Matching is on the
// exact scheme+host+port with no path, and the allowlist is empty by default,
// so a deployment that never sets --ui-callback-origins cannot be talked into
// this path at all.
func resolveUIOrigin(requested string, allowed []string) string {
	if requested == "" || len(allowed) == 0 {
		return ""
	}
	u, err := url.Parse(requested)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ""
	}
	// Compare the normalised origin, never the raw string: "https://ui.example.com/../evil"
	// and "https://ui.example.com" must not be distinguishable to the allowlist.
	origin := strings.ToLower(u.Scheme + "://" + u.Host)
	for _, a := range allowed {
		av, err := url.Parse(strings.TrimSpace(a))
		if err != nil || av.Scheme == "" || av.Host == "" {
			continue
		}
		if origin == strings.ToLower(av.Scheme+"://"+av.Host) {
			return origin
		}
	}
	return ""
}

// uiCallbackRedirect returns the URL to send the browser to after a successful
// login: the allowlisted UI's own callback page carrying the token, or the
// server's home (base + "/") when no cross-origin hand-off was requested.
func uiCallbackRedirect(uiOrigin, token, base string) string {
	if uiOrigin == "" {
		return base + "/"
	}
	return uiOrigin + "/auth/callback?token=" + url.QueryEscape(token)
}

// BasePath is the path the server is mounted under, taken from its public URL
// ("https://h.example/path" → "/path"), without a trailing slash; "" at the host
// root. Redirects and cookie paths the browser receives sit under it.
func BasePath(publicURL string) string {
	u, err := url.Parse(publicURL)
	if err != nil {
		return ""
	}
	return strings.TrimRight(u.Path, "/")
}
