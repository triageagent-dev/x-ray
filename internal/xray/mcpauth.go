package xray

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rs/zerolog"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// MCP OAuth scopes (least privilege). Read tools require scopeRead; write tools
// (upsert/delete) require scopeWrite.
const (
	scopeRead  = "xray:memory.read"
	scopeWrite = "xray:memory.write"
)

// allMCPScopes is what the authorization server advertises and what a fully
// privileged credential (e.g. the static operator secret) is granted.
var allMCPScopes = []string{scopeRead, scopeWrite}

// mcpAuthorizer is a minimal OAuth 2.1 Authorization Server co-located with the
// MCP resource server (the spec permits this). It brokers the existing
// GitHub/Entra/Google browser login into short-lived, audience-bound JWT access tokens
// and supports RFC 7591 dynamic client registration.
//
// Current limitations: the RS256 signing key is generated per process (tokens do
// not survive a restart, and multi-replica deployments need a shared key), and
// there are no refresh tokens. dev=true short-circuits /authorize to issue a
// token for a synthetic user so the full flow is testable without a real IdP.
//
// In-memory map caps: the OAuth endpoints are public, so an attacker could loop
// registrations / authorize calls to exhaust pod memory. Bound both maps; the GC
// loop prunes expired codes, and registrations evict oldest-first at the cap.
const (
	maxOAuthClients = 10000
	maxAuthCodes    = 10000
)

// ─── persistent MCP OAuth client registry (ADR-049) ──────────────────────────
//
// The DCR client registry is a PACKAGE-LEVEL store (like the session store) rather
// than a field on mcpAuthorizer, for one reason: it must exist and be restorable
// BEFORE the controller-runtime manager starts. The informer replays the
// system_configuration CR exactly once during cache-sync, which drives
// restoreOAuthClients — and that happens long before the authorizer is constructed
// in main.go. A package-level store is populated by that first replay regardless of
// authorizer wiring order, then the authorizer reads/writes it. This is the same
// pattern (and the same ordering lesson) as session persistence (ADR-047).
type oauthClientStore struct {
	mu       sync.Mutex
	clients  map[string]oauthClient // client_id → registration
	onChange func()                 // marks stats dirty so the debounced flusher persists; nil = no persistence
}

var oauthClients = &oauthClientStore{clients: make(map[string]oauthClient)}

// SetOAuthClientPersistence wires the flush hook so a new/updated registration is
// persisted to CR status on the next flush tick. Called once at startup, BEFORE
// mgr.Start, so the first CR replay restores into this store. A nil hook leaves the
// registry in-memory only (local dev without k8s).
func SetOAuthClientPersistence(onChange func()) {
	oauthClients.mu.Lock()
	oauthClients.onChange = onChange
	oauthClients.mu.Unlock()
}

// add records a registration and requests a flush. Evicts oldest at the cap.
func (s *oauthClientStore) add(clientID string, c oauthClient) {
	s.mu.Lock()
	if len(s.clients) >= maxOAuthClients {
		var oldestID string
		var oldest time.Time
		for id, ex := range s.clients {
			if oldestID == "" || ex.issuedAt.Before(oldest) {
				oldestID, oldest = id, ex.issuedAt
			}
		}
		delete(s.clients, oldestID)
	}
	s.clients[clientID] = c
	onChange := s.onChange
	s.mu.Unlock()
	if onChange != nil {
		onChange()
	}
}

// get returns the registration for a client_id.
func (s *oauthClientStore) get(clientID string) (oauthClient, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.clients[clientID]
	return c, ok
}

// count returns how many client registrations are currently known (for logging).
func (s *oauthClientStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

// snapshotOAuthClients returns the registry as persistable rows for CR status.
func (s *oauthClientStore) snapshot() []mcell.OAuthClientReg {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.clients) == 0 {
		return nil
	}
	rows := make([]mcell.OAuthClientReg, 0, len(s.clients))
	for id, c := range s.clients {
		rows = append(rows, mcell.OAuthClientReg{
			ClientID:     id,
			RedirectURIs: append([]string(nil), c.redirectURIs...),
			IssuedAt:     c.issuedAt.UTC().Format(time.RFC3339),
		})
	}
	return rows
}

// restore rehydrates registrations from CR status. Like restoreSessions it runs on
// every system_configuration watch event and flush echoes the same CR back, so it
// never overwrites a client_id already held (the live entry is authoritative).
// Returns the number newly added. Unparseable IssuedAt falls back to a zero time
// (still usable — eviction just treats it as oldest).
func (s *oauthClientStore) restore(rows []mcell.OAuthClientReg) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	restored := 0
	for _, r := range rows {
		if r.ClientID == "" || len(r.RedirectURIs) == 0 {
			continue
		}
		if _, ok := s.clients[r.ClientID]; ok {
			continue // already held — do not clobber a live registration
		}
		issued, _ := time.Parse(time.RFC3339, r.IssuedAt)
		s.clients[r.ClientID] = oauthClient{
			redirectURIs: append([]string(nil), r.RedirectURIs...),
			issuedAt:     issued,
		}
		restored++
	}
	return restored
}

type mcpAuthorizer struct {
	issuer   string // public base URL, e.g. https://xray.example.com
	resource string // MCP resource URL, e.g. https://xray.example.com/mcp
	key      ed25519.PrivateKey
	kid      string
	tokenTTL time.Duration
	dev      bool
	// allowFn returns the live admin allowlist (usernames permitted to use MCP at
	// all). MCP is an admin-only interface: read access alone is privileged because
	// search/get can retrieve any user's private notes by subject. A user not on the
	// list is denied a token entirely. Empty list ⇒ deny everyone (fail-closed); the
	// static --mcp-token-secret bearer is the out-of-band full-access admin path and
	// is unaffected. Sourced live from the system_configuration CR (writeAllowedUsers).
	allowFn func() []string
	limiter *keyedRateLimiter // per-IP throttle for the public OAuth endpoints
	logger  zerolog.Logger
	mu      sync.Mutex
	codes   map[string]authCode
	// DCR client registrations live in the package-level oauthClients store (not a
	// field here) so they can be restored from CR status before this authorizer is
	// even constructed — see oauthClientStore. ADR-049.
}

// authCode is a single-use OAuth authorization code bound to a PKCE challenge,
// the requested resource, and the resolved user/scopes.
type authCode struct {
	challenge   string // PKCE code_challenge (S256, base64url, no padding)
	redirectURI string
	resource    string
	sub         string
	scopes      []string
	expiresAt   time.Time
}

// oauthClient is a dynamically registered public client (RFC 7591). Clients are
// public (PKCE, no secret), so we only need to remember the redirect_uris to
// validate the /authorize redirect_uri against.
type oauthClient struct {
	redirectURIs []string
	issuedAt     time.Time
}

// NewMCPAuthorizer builds the co-located OAuth 2.1 authorization server for MCP.
// issuer is the public base URL; resource is the MCP endpoint URL (issuer+"/mcp"
// by convention). dev=true auto-issues tokens for a synthetic user so the flow is
// testable without a real IdP. seed deterministically derives the Ed25519 signing
// key so every replica converges on the same key (ADR-050); pass
// mcpSigningSeed(...) in production. A nil seed generates a random per-process key
// (single-replica dev). The returned *mcpAuthorizer is passed to both NewMCPServer
// (for token validation) and Server.SetMCPAuth (for the endpoints).
func NewMCPAuthorizer(issuer, resource string, tokenTTL time.Duration, dev bool, writeAllowed []string, seed []byte, logger zerolog.Logger) (*mcpAuthorizer, error) {
	return newMCPAuthorizer(issuer, resource, tokenTTL, dev, writeAllowed, seed, logger)
}

func newMCPAuthorizer(issuer, resource string, tokenTTL time.Duration, dev bool, writeAllowed []string, seed []byte, logger zerolog.Logger) (*mcpAuthorizer, error) {
	// Derive the signing key from the shared seed so all replicas agree (ADR-050).
	// Ed25519's private key IS a 32-byte seed, so derivation is exact and stable —
	// unlike RSA, whose keygen consumes a variable amount of its random stream.
	// nil seed (no OAuth secret configured) ⇒ random key, single-replica only.
	var key ed25519.PrivateKey
	switch {
	case len(seed) == 0:
		_, k, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate Ed25519 signing key: %w", err)
		}
		key = k
	case len(seed) < ed25519.SeedSize:
		return nil, fmt.Errorf("mcp signing seed too short: got %d bytes, need %d", len(seed), ed25519.SeedSize)
	default:
		key = ed25519.NewKeyFromSeed(seed[:ed25519.SeedSize])
	}
	// Default to the static list captured at construction; main.go overrides this
	// with a live getter (SetAllowFn) so CR edits take effect without a restart.
	static := append([]string(nil), writeAllowed...)
	a := &mcpAuthorizer{
		issuer:   strings.TrimRight(issuer, "/"),
		resource: resource,
		key:      key,
		kid:      keyID(key.Public().(ed25519.PublicKey)),
		tokenTTL: tokenTTL,
		dev:      dev,
		allowFn:  func() []string { return static },
		// ~5 req/s sustained per IP, burst 20 — generous for a real client doing
		// register→authorize→token, restrictive against a flood.
		limiter: newKeyedRateLimiter(200*time.Millisecond, 20),
		logger:  logger,
		codes:   make(map[string]authCode),
	}
	go a.gcLoop()
	return a, nil
}

func (a *mcpAuthorizer) gcLoop() {
	for range time.Tick(time.Minute) {
		now := time.Now()
		a.mu.Lock()
		for k, c := range a.codes {
			if now.After(c.expiresAt) {
				delete(a.codes, k)
			}
		}
		a.mu.Unlock()
	}
}

// SetAllowFn installs a live getter for the MCP admin allowlist so changes to the
// system_configuration CR take effect without a restart. main.go wires this to the
// ConfigWatcher; tests and the static-list constructor path leave the default.
func (a *mcpAuthorizer) SetAllowFn(fn func() []string) {
	if fn != nil {
		a.allowFn = fn
	}
}

// isMCPAllowed reports whether a user may use the MCP interface at all. MCP is
// admin-only: read access is privileged (search/get can retrieve any user's
// private notes by subject), so a user not on the allowlist gets no access. The
// match key is the userHash (stable SHA-256[:16] of the SSO ID) — display names
// are spoofable. An empty allowlist denies everyone (fail-closed). --dev bypasses
// the gate so the flow is testable without provisioning the CR.
func (a *mcpAuthorizer) isMCPAllowed(userHash string) bool {
	if a.dev {
		return true
	}
	return slices.Contains(a.allowFn(), userHash)
}

// scopesForUser returns the scopes an allowlisted user is granted. Callers must
// gate on isMCPAllowed first; allowlisted MCP users get full read+write (the
// interface is admin-only, so there is no read-only tier). Non-allowlisted users
// never reach here — they are denied at /authorize.
func (a *mcpAuthorizer) scopesForUser(userHash string) []string {
	if !a.isMCPAllowed(userHash) {
		return nil
	}
	return allMCPScopes
}

// ─── discovery metadata (RFC 9728 + RFC 8414) ───────────────────────────────────

// protectedResourceMetadata serves /.well-known/oauth-protected-resource (RFC 9728).
func (a *mcpAuthorizer) protectedResourceMetadata(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"resource":                 a.resource,
		"authorization_servers":    []string{a.issuer},
		"scopes_supported":         allMCPScopes,
		"bearer_methods_supported": []string{"header"},
	})
}

// authServerMetadata serves /.well-known/oauth-authorization-server (RFC 8414).
func (a *mcpAuthorizer) authServerMetadata(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"issuer":                                a.issuer,
		"authorization_endpoint":                a.issuer + "/authorize",
		"token_endpoint":                        a.issuer + "/token",
		"registration_endpoint":                 a.issuer + "/register",
		"jwks_uri":                              a.issuer + "/.well-known/jwks.json",
		"scopes_supported":                      allMCPScopes,
		"response_types_supported":              []string{"code"},
		"grant_types_supported":                 []string{"authorization_code"},
		"code_challenge_methods_supported":      []string{"S256"},
		"token_endpoint_auth_methods_supported": []string{"none"},
	})
}

// jwks serves the Ed25519 public key (OKP) so clients/resource servers can verify
// tokens. Every replica derives the same key (ADR-050), so this is identical
// across pods.
func (a *mcpAuthorizer) jwks(w http.ResponseWriter, _ *http.Request) {
	pub := a.key.Public().(ed25519.PublicKey)
	writeJSON(w, map[string]any{
		"keys": []map[string]any{{
			"kty": "OKP",
			"crv": "Ed25519",
			"use": "sig",
			"alg": "EdDSA",
			"kid": a.kid,
			"x":   base64.RawURLEncoding.EncodeToString(pub),
		}},
	})
}

// ─── dynamic client registration (RFC 7591) ─────────────────────────────────────

// throttle rate-limits a public OAuth endpoint per client IP. Returns true if the
// request was rejected (and the 429 already written).
func (a *mcpAuthorizer) throttle(w http.ResponseWriter, r *http.Request) bool {
	if a.limiter.allow(clientIP(r.Header.Get("X-Forwarded-For"), r.Header.Get("X-Real-IP"), r.RemoteAddr)) {
		return false
	}
	http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
	return true
}

// isAllowedDCRRedirectURI restricts self-registered (DCR) redirect_uris to what a
// real native/public OAuth client actually uses: loopback (RFC 8252 §7.3 — any
// port, http is fine since it never leaves the device) or a non-http(s) custom URI
// scheme (e.g. com.anthropic.claude://). Without this, an unauthenticated caller
// could register an attacker-chosen https://evil.example redirect_uri via
// handleRegister (RFC 7591 DCR has no auth), and clientRedirectAllowed's exact-match
// check against the registered set would then approve it — it only proves the URI
// was registered by *someone*, not that the someone was legitimate. Rejecting
// arbitrary public-host URIs here closes that open-redirect path at the source: an
// authorization code can never be redirected to a host the caller doesn't control.
//
// This is the ONE function both handleRegister and the /authorize self-heal
// (clientRedirectAllowed) consult, so the two endpoints can never disagree about
// what open DCR would have accepted.
func isAllowedDCRRedirectURI(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || (u.Host == "" && u.Scheme != "") {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		// Custom scheme (native app deep link) — no host to abuse.
		return u.Scheme != ""
	}
	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

// handleRegister implements POST /register. Clients are public (PKCE, no secret),
// so registration just records the client's redirect_uris and returns a client_id.
// Claude Code (and other MCP clients) require this endpoint before they will start
// the authorization-code flow.
func (a *mcpAuthorizer) handleRegister(w http.ResponseWriter, r *http.Request) {
	if a.throttle(w, r) {
		return
	}
	var req struct {
		ClientName              string   `json:"client_name"`
		RedirectURIs            []string `json:"redirect_uris"`
		GrantTypes              []string `json:"grant_types"`
		ResponseTypes           []string `json:"response_types"`
		TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
		Scope                   string   `json:"scope"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		registerError(w, "invalid_client_metadata", "cannot parse registration request")
		return
	}
	if len(req.RedirectURIs) == 0 {
		registerError(w, "invalid_redirect_uri", "redirect_uris is required")
		return
	}
	for _, ru := range req.RedirectURIs {
		if !isAllowedDCRRedirectURI(ru) {
			registerError(w, "invalid_redirect_uri", "redirect_uris must be loopback (localhost/127.0.0.1) or a custom URI scheme")
			return
		}
	}

	clientID := randB64(16)
	// Record in the persistent registry (bounds + evicts at cap, requests a flush
	// so the registration survives a restart — ADR-049).
	oauthClients.add(clientID, oauthClient{redirectURIs: req.RedirectURIs, issuedAt: time.Now()})
	a.logger.Info().Str("client_id", clientID).Str("name", req.ClientName).Int("redirect_uris", len(req.RedirectURIs)).Msg("mcp client registered (DCR)")

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
		"client_id":                  clientID,
		"client_id_issued_at":        time.Now().Unix(),
		"client_name":                req.ClientName,
		"redirect_uris":              req.RedirectURIs,
		"grant_types":                []string{"authorization_code"},
		"response_types":             []string{"code"},
		"token_endpoint_auth_method": "none", // public client (PKCE)
		"scope":                      strings.Join(allMCPScopes, " "),
	})
}

// clientRedirectAllowed validates the /authorize redirect_uri against the
// registered client (fail closed). An empty client_id is rejected in production —
// the redirect_uri allowlist is the defense against redirecting an authorization
// code to an attacker-controlled URL. In --dev an empty client_id is permitted so
// curl can drive the flow without registering first. An unknown (non-empty)
// client_id is self-healed rather than rejected when its redirect_uri is
// DCR-acceptable — see the comment inline below.
func (a *mcpAuthorizer) clientRedirectAllowed(clientID, redirectURI string) bool {
	if clientID == "" {
		return a.dev // prod: a client_id is required; dev: allow unregistered curl
	}
	c, ok := oauthClients.get(clientID)
	if !ok {
		// Self-heal instead of rejecting outright: an unknown client_id is usually a
		// client reusing an id it cached before this pod generation (pre-persistence
		// restart, evicted entry — see ADR-049). Real MCP clients (Claude Code) cache
		// their client_id after the first DCR and do NOT fall back to re-registering
		// when /authorize rejects it, so a lost registry bricked re-auth with
		// "redirect_uri not registered for client" and no client-side recovery path
		// (production incident, 2026-07-24; ported from a sibling project's
		// fix for the same class of bug). If the presented redirect_uri is one
		// open DCR would accept verbatim (isAllowedDCRRedirectURI — the exact
		// handleRegister gate), register it now under this client_id and proceed.
		// Security delta is zero: /register is unauthenticated by design (RFC 7591)
		// and rate-limited identically to /authorize (both go through a.throttle), so
		// an attacker gains nothing here they couldn't get by calling /register
		// first — a public-host redirect_uri still fails closed below, exactly like
		// DCR, and this never widens an EXISTING registration (only fires when the
		// client_id is unknown).
		if isAllowedDCRRedirectURI(redirectURI) {
			oauthClients.add(clientID, oauthClient{redirectURIs: []string{redirectURI}, issuedAt: time.Now()})
			a.logger.Info().Str("client_id", clientID).Str("redirect_uri", redirectURI).
				Msg("mcp client auto-registered on authorize: unknown client_id with DCR-acceptable redirect_uri (registry lost?)")
			return true
		}
		// Unknown client_id and a redirect_uri open DCR would never have accepted —
		// log it: the symptom is the opaque "redirect_uri not registered" the client
		// sees, and the fix is to re-register. Without this the rejection is silent
		// and undiagnosable.
		a.logger.Warn().Str("client_id", clientID).Str("redirect_uri", redirectURI).
			Int("known_clients", oauthClients.count()).
			Msg("mcp authorize denied: unknown client_id (stale cache? re-register needed)")
		return false
	}
	if !slices.Contains(c.redirectURIs, redirectURI) {
		a.logger.Warn().Str("client_id", clientID).Str("redirect_uri", redirectURI).
			Strs("registered_uris", c.redirectURIs).
			Msg("mcp authorize denied: redirect_uri not in client's registered set")
		return false
	}
	return true
}

func registerError(w http.ResponseWriter, code, desc string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
		"error":             code,
		"error_description": desc,
	})
}

// ─── authorization-code flow (OAuth 2.1 + PKCE) ─────────────────────────────────

// handleAuthorize implements GET /authorize. In dev mode it auto-issues a code
// for a synthetic user so the flow is testable without a real IdP. In production
// this is where the existing GitHub/Entra/Google session must be required (Phase 2).
func (a *mcpAuthorizer) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	if a.throttle(w, r) {
		return
	}
	q := r.URL.Query()
	respType := q.Get("response_type")
	redirectURI := q.Get("redirect_uri")
	state := q.Get("state")
	challenge := q.Get("code_challenge")
	method := q.Get("code_challenge_method")
	resource := q.Get("resource")
	clientID := q.Get("client_id")

	if respType != "code" {
		http.Error(w, "unsupported_response_type", http.StatusBadRequest)
		return
	}
	if redirectURI == "" {
		http.Error(w, "invalid_request: redirect_uri required", http.StatusBadRequest)
		return
	}
	// Validate redirect_uri against the registered client (DCR).
	if !a.clientRedirectAllowed(clientID, redirectURI) {
		http.Error(w, "invalid_request: redirect_uri not registered for client", http.StatusBadRequest)
		return
	}
	// PKCE is mandatory (OAuth 2.1).
	if challenge == "" || method != "S256" {
		http.Error(w, "invalid_request: PKCE S256 code_challenge required", http.StatusBadRequest)
		return
	}
	// Resource indicator (RFC 8707): if supplied it must match our resource.
	if resource != "" && resource != a.resource {
		http.Error(w, "invalid_target: unknown resource", http.StatusBadRequest)
		return
	}

	// Resolve the user from the existing X-Ray session (pre-login model): the user
	// must already be logged into the web app via GitHub/Entra/Google in this browser.
	// --dev uses a synthetic user so the flow is testable without a real IdP.
	var username, sub string
	if a.dev {
		username = "dev-user"
		sub = "dev:local"
	} else if c, err := r.Cookie("xray_session"); err == nil {
		if uname, uid, ok := sessions.validate(c.Value); ok && grants.allowsUser(uid, uname) {
			username, sub = uname, uid
		}
	}
	if sub == "" {
		// Not logged in — send the browser to the UI login page. The GitHub/Entra/Google
		// callback redirects to the server's home, not back here, so the user re-triggers MCP auth
		// after logging in. We deliberately do NOT pass a next= return target: it
		// would be an attacker-controlled redirect param on a public endpoint, and
		// nothing consumes it today. If resume-after-login is ever wired up, route
		// the value through a same-origin validator — never redirect to it raw.
		http.Redirect(w, r, BasePath(a.issuer)+"/login", http.StatusFound)
		return
	}

	// MCP is admin-only. A logged-in but non-allowlisted user is denied a token
	// (OAuth 2.0 access_denied per RFC 6749 §4.1.2.1) rather than issued an
	// empty-scope one — they never get a usable credential. The allowlist matches
	// the userHash (stable, non-spoofable), not the display name.
	userHash := stableUserID(sub)
	if a.dev {
		userHash = sub // synthetic dev user: gate is bypassed by isMCPAllowed anyway
	}
	byGrant := !a.dev && grants.isAdmin(sub, username)
	if !a.isMCPAllowed(userHash) && !byGrant {
		a.logger.Warn().Str("userHash", userHash).Msg("mcp authorize denied: user not on allowlist")
		loc, err := url.Parse(redirectURI)
		if err != nil {
			http.Error(w, "invalid_request: bad redirect_uri", http.StatusBadRequest)
			return
		}
		rq := loc.Query()
		rq.Set("error", "access_denied")
		rq.Set("error_description", "user is not authorized to use the MCP interface")
		if state != "" {
			rq.Set("state", state)
		}
		loc.RawQuery = rq.Encode()
		http.Redirect(w, r, loc.String(), http.StatusFound)
		return
	}

	code := randB64(32)
	a.mu.Lock()
	// Bound the map: at cap, evict the oldest code before inserting.
	if len(a.codes) >= maxAuthCodes {
		var oldestK string
		var oldest time.Time
		for k, c := range a.codes {
			if oldestK == "" || c.expiresAt.Before(oldest) {
				oldestK, oldest = k, c.expiresAt
			}
		}
		delete(a.codes, oldestK)
	}
	a.codes[code] = authCode{
		challenge:   challenge,
		redirectURI: redirectURI,
		resource:    a.resource,
		sub:         sub,
		scopes:      a.grantedScopes(userHash, byGrant),
		expiresAt:   time.Now().Add(2 * time.Minute),
	}
	a.mu.Unlock()

	loc, err := url.Parse(redirectURI)
	if err != nil {
		http.Error(w, "invalid_request: bad redirect_uri", http.StatusBadRequest)
		return
	}
	rq := loc.Query()
	rq.Set("code", code)
	if state != "" {
		rq.Set("state", state)
	}
	loc.RawQuery = rq.Encode()
	// Log the userHash (stable, non-spoofable, non-PII), never the raw SSO ID —
	// `sub` is the provider OID (e.g. azure:<oid>) and must not land in logs.
	a.logger.Info().Str("userHash", userHash).Str("redirect", loc.Host).Msg("mcp authorize: code issued")
	http.Redirect(w, r, loc.String(), http.StatusFound)
}

// handleToken implements POST /token (grant_type=authorization_code + PKCE verify).
func (a *mcpAuthorizer) handleToken(w http.ResponseWriter, r *http.Request) {
	if a.throttle(w, r) {
		return
	}
	if err := r.ParseForm(); err != nil {
		tokenError(w, "invalid_request", "cannot parse form")
		return
	}
	if r.Form.Get("grant_type") != "authorization_code" {
		tokenError(w, "unsupported_grant_type", "only authorization_code is supported")
		return
	}
	code := r.Form.Get("code")
	verifier := r.Form.Get("code_verifier")
	resource := r.Form.Get("resource")

	a.mu.Lock()
	ac, ok := a.codes[code]
	if ok {
		delete(a.codes, code) // single-use
	}
	a.mu.Unlock()

	if !ok || time.Now().After(ac.expiresAt) {
		tokenError(w, "invalid_grant", "code invalid or expired")
		return
	}
	// PKCE verification: BASE64URL(SHA256(verifier)) == stored challenge.
	if verifier == "" || !pkceMatch(verifier, ac.challenge) {
		tokenError(w, "invalid_grant", "PKCE verification failed")
		return
	}
	if resource != "" && resource != ac.resource {
		tokenError(w, "invalid_target", "resource mismatch")
		return
	}

	tok, err := a.signToken(ac.sub, ac.scopes)
	if err != nil {
		a.logger.Error().Err(err).Msg("mcp token sign failed")
		tokenError(w, "server_error", "cannot issue token")
		return
	}
	writeJSON(w, map[string]any{
		"access_token": tok,
		"token_type":   "Bearer",
		"expires_in":   int(a.tokenTTL.Seconds()),
		"scope":        strings.Join(ac.scopes, " "),
	})
}

// signToken mints an aud-bound EdDSA JWT access token.
func (a *mcpAuthorizer) signToken(sub string, scopes []string) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":   a.issuer,
		"aud":   a.resource,
		"sub":   sub,
		"scope": strings.Join(scopes, " "),
		"iat":   now.Unix(),
		"exp":   now.Add(a.tokenTTL).Unix(),
	}
	t := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	t.Header["kid"] = a.kid
	return t.SignedString(a.key)
}

// validateMCPToken verifies signature, issuer, audience, expiry, and returns the
// subject + granted scopes. Audience binding (RFC 8707) rejects tokens minted for
// a different resource.
func (a *mcpAuthorizer) validateMCPToken(raw string) (sub string, scopes []string, ok bool) {
	tok, err := jwt.Parse(raw, func(t *jwt.Token) (any, error) {
		if _, isEd := t.Method.(*jwt.SigningMethodEd25519); !isEd {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return a.key.Public(), nil
	}, jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(a.issuer),
		jwt.WithAudience(a.resource),
		jwt.WithExpirationRequired(),
	)
	if err != nil || !tok.Valid {
		return "", nil, false
	}
	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return "", nil, false
	}
	sub, _ = claims["sub"].(string)
	if s, _ := claims["scope"].(string); s != "" {
		scopes = strings.Fields(s)
	}
	return sub, scopes, true
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func pkceMatch(verifier, challenge string) bool {
	sum := sha256.Sum256([]byte(verifier))
	want := base64.RawURLEncoding.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(want), []byte(challenge)) == 1
}

// MCPSigningSeed is the exported entry point for mcpSigningSeed, called from
// main.go with the same OAuth client secrets fed to sessionHMACKey (ADR-050).
func MCPSigningSeed(secrets ...string) []byte { return mcpSigningSeed(secrets...) }

// mcpSigningSeed derives the 32-byte seed for the Ed25519 MCP signing key from
// the configured OAuth client secret(s). It mirrors sessionHMACKey (ADR-047)
// exactly — SHA-256 over the non-empty secrets, each NUL-terminated — but binds a
// DIFFERENT purpose label so the signing key is cryptographically separated from
// the session HMAC key despite sharing root secret material. Stable across
// restarts and identical across replicas (every pod mounts the same Secret), so a
// token signed on one pod validates on another (ADR-050), yet the client secret
// itself is never exposed. Returns nil when no secret is set — the caller then
// falls back to a random per-process key (single-replica / dev only).
func mcpSigningSeed(secrets ...string) []byte {
	h := sha256.New()
	any := false
	for _, s := range secrets {
		if s == "" {
			continue
		}
		any = true
		h.Write([]byte(s))
		h.Write([]byte{0}) // domain-separate the inputs
	}
	if !any {
		return nil
	}
	// Distinct label vs. xray-session-hmac-v1 ⇒ independent key from same secret.
	h.Write([]byte("xray-mcp-signing-v1"))
	return h.Sum(nil) // 32 bytes == ed25519.SeedSize
}

// keyID is the deterministic JWKS key id: base64url(SHA-256(pubkey))[:11]. Derived
// from the public key so every replica computes the same kid (ADR-050), keeping
// the JWKS kid and the JWT header kid in agreement across pods.
func keyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return base64.RawURLEncoding.EncodeToString(sum[:])[:11]
}

func randB64(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func tokenError(w http.ResponseWriter, code, desc string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(map[string]string{ //nolint:errcheck
		"error":             code,
		"error_description": desc,
	})
}

// hasScope reports whether scopes contains want.
func hasScope(scopes []string, want string) bool {
	return slices.Contains(scopes, want)
}

// grantedScopes is scopesForUser widened by an address grant (ADR-066): an admin
// named in the deployment's configuration gets the same full scopes.
func (a *mcpAuthorizer) grantedScopes(userHash string, byGrant bool) []string {
	if byGrant {
		return append([]string(nil), allMCPScopes...)
	}
	return a.scopesForUser(userHash)
}
