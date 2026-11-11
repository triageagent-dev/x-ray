package xray

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	googleAuthURL  = "https://accounts.google.com/o/oauth2/v2/auth"
	googleTokenURL = "https://oauth2.googleapis.com/token"
)

// GoogleOAuthConfig holds the Google OAuth client credentials.
type GoogleOAuthConfig struct {
	ClientID     string
	ClientSecret string
	// Domain is the Google Workspace domain an account must belong to, checked
	// against the ID token's hd claim. Empty admits any Google account.
	Domain    string
	PublicURL string
	// UICallbackOrigins allowlists origins a login may hand the session token
	// to via ?ui= (see resolveUIOrigin). Empty disables the hand-off.
	UICallbackOrigins []string
}

// googleHTTP bounds the token exchange so a stalled endpoint cannot pin a handler.
var googleHTTP = &http.Client{Timeout: 10 * time.Second}

type googleAuth struct {
	cfg      GoogleOAuthConfig
	tokenURL string
	statesMu sync.Mutex
	states   map[string]loginState
}

func newGoogleAuth(cfg GoogleOAuthConfig) *googleAuth {
	g := &googleAuth{cfg: cfg, tokenURL: googleTokenURL, states: make(map[string]loginState)}
	go g.pruneStates()
	return g
}

func (g *googleAuth) pruneStates() {
	for range time.Tick(5 * time.Minute) {
		now := time.Now()
		g.statesMu.Lock()
		for k, st := range g.states {
			if now.After(st.expiresAt) {
				delete(g.states, k)
			}
		}
		g.statesMu.Unlock()
	}
}

func (g *googleAuth) newState(uiOrigin string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	state := hex.EncodeToString(b)
	g.statesMu.Lock()
	g.states[state] = loginState{expiresAt: time.Now().Add(10 * time.Minute), uiOrigin: uiOrigin}
	g.statesMu.Unlock()
	return state
}

func (g *googleAuth) consumeState(state string) (loginState, bool) {
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	st, ok := g.states[state]
	if !ok {
		return loginState{}, false
	}
	delete(g.states, state)
	return st, time.Now().Before(st.expiresAt)
}

func (g *googleAuth) callbackURL() string { return g.cfg.PublicURL + "/v1/auth/google/callback" }

func (g *googleAuth) handleLogin(w http.ResponseWriter, r *http.Request) {
	state := g.newState(resolveUIOrigin(r.URL.Query().Get("ui"), g.cfg.UICallbackOrigins))
	setStateCookie(w, BasePath(g.cfg.PublicURL), state)
	q := url.Values{
		"client_id":     {g.cfg.ClientID},
		"response_type": {"code"},
		"redirect_uri":  {g.callbackURL()},
		"scope":         {"openid email profile"},
		"state":         {state},
	}
	// hd only narrows the account picker; the callback enforces the domain.
	if g.cfg.Domain != "" {
		q.Set("hd", g.cfg.Domain)
	}
	http.Redirect(w, r, googleAuthURL+"?"+q.Encode(), http.StatusFound)
}

func (g *googleAuth) handleCallback(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")

	clearStateCookie(w, BasePath(g.cfg.PublicURL))
	if !stateCookieMatches(r, state) {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}
	st, stateOK := g.consumeState(state)
	if !stateOK {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}

	idToken, err := g.exchangeCode(code)
	if err != nil {
		http.Error(w, "token exchange failed", http.StatusBadGateway)
		return
	}
	login, sub, err := g.identity(idToken, time.Now())
	if err != nil || !grants.allowsUser("google:"+sub, login) {
		http.Error(w, "this Google account may not sign in", http.StatusForbidden)
		return
	}

	// sub is Google's stable account ID; the email can change.
	token := sessions.create(login, "google:"+sub)
	http.SetCookie(w, &http.Cookie{
		Name:     "xray_session",
		Value:    token,
		Path:     BasePath(g.cfg.PublicURL) + "/",
		MaxAge:   86400,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, uiCallbackRedirect(st.uiOrigin, token, BasePath(g.cfg.PublicURL)), http.StatusFound)
}

// exchangeCode trades the authorization code for the ID token.
func (g *googleAuth) exchangeCode(code string) (string, error) {
	resp, err := googleHTTP.PostForm(g.tokenURL, url.Values{
		"client_id":     {g.cfg.ClientID},
		"client_secret": {g.cfg.ClientSecret},
		"code":          {code},
		"redirect_uri":  {g.callbackURL()},
		"grant_type":    {"authorization_code"},
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	var tok struct {
		IDToken   string `json:"id_token"`
		Error     string `json:"error"`
		ErrorDesc string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", err
	}
	if tok.Error != "" {
		return "", fmt.Errorf("google token error: %s — %s", tok.Error, tok.ErrorDesc)
	}
	if tok.IDToken == "" {
		return "", fmt.Errorf("no id_token in response")
	}
	return tok.IDToken, nil
}

// identity checks the ID token's claims and returns (address, sub). The token comes
// straight from Google's token endpoint over TLS, so its signature is not
// re-verified (OpenID Connect Core 3.1.3.7); issuer, audience and expiry still are.
// The domain is enforced on hd, which Google sets only for Workspace accounts: an
// email ending in the domain is not proof, a consumer account can carry any email.
func (g *googleAuth) identity(idToken string, now time.Time) (string, string, error) {
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return "", "", fmt.Errorf("malformed id_token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("malformed id_token payload")
	}
	var c struct {
		Iss           string `json:"iss"`
		Aud           string `json:"aud"`
		Sub           string `json:"sub"`
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		HD            string `json:"hd"`
		Exp           int64  `json:"exp"`
	}
	if err := json.Unmarshal(payload, &c); err != nil {
		return "", "", fmt.Errorf("malformed id_token claims")
	}
	switch {
	case c.Iss != "https://accounts.google.com" && c.Iss != "accounts.google.com":
		return "", "", fmt.Errorf("id_token issuer %q", c.Iss)
	case c.Aud != g.cfg.ClientID:
		return "", "", fmt.Errorf("id_token audience mismatch")
	case now.Unix() >= c.Exp:
		return "", "", fmt.Errorf("id_token expired")
	case c.Sub == "":
		return "", "", fmt.Errorf("id_token has no subject")
	case c.Email == "" || !c.EmailVerified:
		return "", "", fmt.Errorf("email missing or not verified")
	case g.cfg.Domain != "" && !strings.EqualFold(c.HD, g.cfg.Domain):
		return "", "", fmt.Errorf("account is not in the %s workspace", g.cfg.Domain)
	}
	// The full address: deployments grant access by it (ADR-066).
	if !isASCII(c.Email) {
		return "", "", fmt.Errorf("non-ASCII email")
	}
	return strings.ToLower(c.Email), c.Sub, nil
}
