package xray

import (
	"crypto/rand"
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

// AzureOAuthConfig holds the Azure AD OAuth app credentials.
type AzureOAuthConfig struct {
	ClientID     string
	ClientSecret string
	TenantID     string
	PublicURL    string
	// UICallbackOrigins allowlists origins a login may hand the session token
	// to via ?ui=, for a UI served from a different host than this server.
	// Empty (the default) disables the hand-off entirely — see resolveUIOrigin.
	UICallbackOrigins []string
}

type azureAuth struct {
	cfg      AzureOAuthConfig
	statesMu sync.Mutex
	states   map[string]loginState
}

func newAzureAuth(cfg AzureOAuthConfig) *azureAuth {
	a := &azureAuth{cfg: cfg, states: make(map[string]loginState)}
	go a.pruneStates()
	return a
}

func (a *azureAuth) pruneStates() {
	for range time.Tick(5 * time.Minute) {
		now := time.Now()
		a.statesMu.Lock()
		for k, st := range a.states {
			if now.After(st.expiresAt) {
				delete(a.states, k)
			}
		}
		a.statesMu.Unlock()
	}
}

// newState mints a login state, remembering the (already validated) UI origin
// the session token should be handed to when the callback completes.
func (a *azureAuth) newState(uiOrigin string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	state := hex.EncodeToString(b)
	a.statesMu.Lock()
	a.states[state] = loginState{expiresAt: time.Now().Add(10 * time.Minute), uiOrigin: uiOrigin}
	a.statesMu.Unlock()
	return state
}

func (a *azureAuth) consumeState(state string) (loginState, bool) {
	a.statesMu.Lock()
	defer a.statesMu.Unlock()
	st, ok := a.states[state]
	if !ok {
		return loginState{}, false
	}
	delete(a.states, state)
	return st, time.Now().Before(st.expiresAt)
}

func (a *azureAuth) handleLogin(w http.ResponseWriter, r *http.Request) {
	// ?ui= asks for the session token to be handed to a UI on another origin.
	// Resolved (and allowlist-checked) HERE rather than at the callback, so an
	// unapproved origin is dropped before it is ever bound to a live login.
	state := a.newState(resolveUIOrigin(r.URL.Query().Get("ui"), a.cfg.UICallbackOrigins))
	setStateCookie(w, state)
	callbackURL := a.cfg.PublicURL + "/v1/auth/entra/callback"
	authURL := fmt.Sprintf(
		"https://login.microsoftonline.com/%s/oauth2/v2.0/authorize?client_id=%s&response_type=code&redirect_uri=%s&scope=openid+profile+email&state=%s",
		url.PathEscape(a.cfg.TenantID),
		url.QueryEscape(a.cfg.ClientID),
		url.QueryEscape(callbackURL),
		url.QueryEscape(state),
	)
	http.Redirect(w, r, authURL, http.StatusFound)
}

func (a *azureAuth) handleCallback(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")

	// Bind the callback to the browser that started login: the query state must
	// match the signed state cookie set in handleLogin. Defeats login-CSRF.
	clearStateCookie(w)
	if !stateCookieMatches(r, state) {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}
	st, stateOK := a.consumeState(state)
	if !stateOK {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}

	accessToken, err := a.exchangeCode(code)
	if err != nil {
		http.Error(w, "token exchange failed", http.StatusBadGateway)
		return
	}

	login, oid, err := a.azureUserLogin(accessToken)
	if err != nil {
		http.Error(w, "failed to get user info", http.StatusBadGateway)
		return
	}

	// userID is "azure:<oid>" — tenant-scoped object ID, stable and unique.
	userID := "azure:" + oid
	token := sessions.create(login, userID)
	http.SetCookie(w, &http.Cookie{
		Name:     "xray_session",
		Value:    token,
		Path:     "/",
		MaxAge:   86400,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	// Same-origin logins land on "/" and use the cookie just set. A login that
	// asked for an allowlisted UI origin is bounced there with the token
	// instead, because that host will never receive this cookie.
	http.Redirect(w, r, uiCallbackRedirect(st.uiOrigin, token), http.StatusFound)
}

func (a *azureAuth) exchangeCode(code string) (string, error) {
	callbackURL := a.cfg.PublicURL + "/v1/auth/entra/callback"
	resp, err := http.PostForm(
		fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/token", url.PathEscape(a.cfg.TenantID)),
		url.Values{
			"client_id":     {a.cfg.ClientID},
			"client_secret": {a.cfg.ClientSecret},
			"code":          {code},
			"redirect_uri":  {callbackURL},
			"grant_type":    {"authorization_code"},
			"scope":         {"openid profile email"},
		},
	)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
		ErrorDesc   string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", err
	}
	if tok.Error != "" {
		return "", fmt.Errorf("azure token error: %s — %s", tok.Error, tok.ErrorDesc)
	}
	if tok.AccessToken == "" {
		return "", fmt.Errorf("no access_token in response")
	}
	return tok.AccessToken, nil
}

// azureUserLogin fetches the authenticated user's display login and object ID.
// Returns (login, oid, error). oid is the Azure AD object ID — stable and unique within the tenant.
func (a *azureAuth) azureUserLogin(accessToken string) (string, string, error) {
	req, err := http.NewRequest(http.MethodGet, "https://graph.microsoft.com/v1.0/me", nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode >= 400 {
		// Status code only — the Graph response body may carry user/profile detail
		// and reaches logs (AUDIT F-7).
		return "", "", fmt.Errorf("graph API returned %d", resp.StatusCode)
	}
	var u struct {
		ID                string `json:"id"`
		UserPrincipalName string `json:"userPrincipalName"`
		DisplayName       string `json:"displayName"`
		Mail              string `json:"mail"`
	}
	if err := json.Unmarshal(body, &u); err != nil {
		return "", "", err
	}
	if u.ID == "" {
		return "", "", fmt.Errorf("no object ID in graph response")
	}
	// Prefer UPN (e.g. user@example.com), fall back to display name
	var login string
	if u.UserPrincipalName != "" {
		if at := strings.Index(u.UserPrincipalName, "@"); at > 0 {
			login = u.UserPrincipalName[:at]
		} else {
			login = u.UserPrincipalName
		}
	} else if u.DisplayName != "" {
		login = u.DisplayName
	} else {
		return "", "", fmt.Errorf("no usable identity in graph response")
	}
	return login, u.ID, nil
}
