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

// GitHubOAuthConfig holds the OAuth App credentials and org restriction.
type GitHubOAuthConfig struct {
	ClientID     string
	ClientSecret string
	Org          string
	Team         string // optional — if set, user must be an active team member
	PublicURL    string // externally reachable base URL, e.g. https://xray.example.com
	// UICallbackOrigins allowlists origins a login may hand the session token
	// to via ?ui=, for a UI served from a different host than this server.
	// Empty (the default) disables the hand-off entirely — see resolveUIOrigin.
	UICallbackOrigins []string
}

type githubAuth struct {
	cfg      GitHubOAuthConfig
	statesMu sync.Mutex
	states   map[string]loginState // state → in-flight login
}

func newGitHubAuth(cfg GitHubOAuthConfig) *githubAuth {
	g := &githubAuth{
		cfg:    cfg,
		states: make(map[string]loginState),
	}
	go g.pruneStates()
	return g
}

func (g *githubAuth) pruneStates() {
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

// newState mints a login state, remembering the (already validated) UI origin
// the session token should be handed to when the callback completes.
func (g *githubAuth) newState(uiOrigin string) string {
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

func (g *githubAuth) consumeState(state string) (loginState, bool) {
	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	st, ok := g.states[state]
	if !ok {
		return loginState{}, false
	}
	delete(g.states, state)
	return st, time.Now().Before(st.expiresAt)
}

// handleLogin redirects the browser to GitHub's OAuth consent screen.
func (g *githubAuth) handleLogin(w http.ResponseWriter, r *http.Request) {
	// ?ui= asks for the session token to be handed to a UI on another origin.
	// Resolved (and allowlist-checked) HERE rather than at the callback, so an
	// unapproved origin is dropped before it is ever bound to a live login.
	state := g.newState(resolveUIOrigin(r.URL.Query().Get("ui"), g.cfg.UICallbackOrigins))
	setStateCookie(w, state)
	callbackURL := g.cfg.PublicURL + "/v1/auth/github/callback"
	authURL := fmt.Sprintf(
		"https://github.com/login/oauth/authorize?client_id=%s&redirect_uri=%s&scope=read:org&state=%s",
		url.QueryEscape(g.cfg.ClientID),
		url.QueryEscape(callbackURL),
		url.QueryEscape(state),
	)
	http.Redirect(w, r, authURL, http.StatusFound)
}

// handleCallback exchanges the OAuth code for a token, verifies org membership,
// sets an HttpOnly session cookie, and redirects to /.
func (g *githubAuth) handleCallback(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")

	// Bind the callback to the browser that started login: the query state must
	// match the signed state cookie set in handleLogin. Defeats login-CSRF.
	clearStateCookie(w)
	if !stateCookieMatches(r, state) {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}

	// Validate state — consume it atomically so it can't be replayed.
	st, stateOK := g.consumeState(state)
	if !stateOK {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}

	// Exchange code for GitHub access token.
	ghToken, err := g.exchangeCode(code)
	if err != nil {
		http.Error(w, "token exchange failed", http.StatusBadGateway)
		return
	}

	// Get the authenticated user's login and numeric ID.
	login, numericID, err := g.githubUserLogin(ghToken)
	if err != nil {
		http.Error(w, "failed to get user info", http.StatusBadGateway)
		return
	}

	// Verify org membership (skipped if --github-org is empty).
	if g.cfg.Org != "" {
		if err := g.checkOrgMember(ghToken, login); err != nil {
			http.Error(w, fmt.Sprintf("user %q is not a member of org %q: %v", login, g.cfg.Org, err), http.StatusForbidden)
			return
		}
	}

	// Optionally verify team membership.
	if g.cfg.Team != "" {
		if err := g.checkTeamMember(ghToken, login); err != nil {
			http.Error(w, "not a member of the required GitHub team", http.StatusForbidden)
			return
		}
	}

	// Create X-Ray session. userID is "github:<numericID>" — globally unique, never reassigned.
	userID := "github:" + numericID
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

// handleLogout clears the session cookie and invalidates the server-side session.
func (g *githubAuth) handleLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie("xray_session"); err == nil {
		sessions.invalidate(c.Value)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "xray_session",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	http.Redirect(w, r, "/login", http.StatusFound)
}

// exchangeCode POSTs to GitHub to exchange an OAuth code for an access token.
func (g *githubAuth) exchangeCode(code string) (string, error) {
	resp, err := http.PostForm("https://github.com/login/oauth/access_token", url.Values{
		"client_id":     {g.cfg.ClientID},
		"client_secret": {g.cfg.ClientSecret},
		"code":          {code},
		// Re-assert the redirect_uri used at /authorize (RFC 6749 §4.1.3), so the
		// exchange is bound to it as defense-in-depth against code interception
		// (AUDIT F-10).
		"redirect_uri": {g.cfg.PublicURL + "/v1/auth/github/callback"},
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	// GitHub returns application/x-www-form-urlencoded by default.
	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return "", err
	}
	token := vals.Get("access_token")
	if token == "" {
		// Don't embed the provider response body — it can carry sensitive detail
		// and reaches logs (AUDIT F-7). The error type is what the caller needs.
		return "", fmt.Errorf("no access_token in github response (error=%q)", vals.Get("error"))
	}
	return token, nil
}

// githubUserLogin fetches the authenticated user's login name and numeric ID.
// Returns (login, numericID, error). numericID is the stable GitHub user ID
// as a decimal string — globally unique and never reassigned.
func (g *githubAuth) githubUserLogin(ghToken string) (string, string, error) {
	body, err := g.githubAPI(ghToken, "https://api.github.com/user")
	if err != nil {
		return "", "", err
	}
	var u struct {
		Login string `json:"login"`
		ID    int64  `json:"id"`
	}
	if err := json.Unmarshal(body, &u); err != nil {
		return "", "", err
	}
	if u.Login == "" {
		return "", "", fmt.Errorf("empty login in user response")
	}
	return u.Login, fmt.Sprintf("%d", u.ID), nil
}

// checkOrgMember verifies the user belongs to the required org by listing their
// own org memberships. This works for private memberships unlike the
// /orgs/{org}/members/{login} endpoint which returns 404 for hidden members.
func (g *githubAuth) checkOrgMember(ghToken, login string) error {
	body, err := g.githubAPI(ghToken, "https://api.github.com/user/orgs?per_page=100")
	if err != nil {
		return err
	}
	var orgs []struct {
		Login string `json:"login"`
	}
	if err := json.Unmarshal(body, &orgs); err != nil {
		return err
	}
	names := make([]string, len(orgs))
	for i, o := range orgs {
		names[i] = o.Login
		if strings.EqualFold(o.Login, g.cfg.Org) {
			return nil
		}
	}
	return fmt.Errorf("org %q not found in user's org list: %v", g.cfg.Org, names)
}

// checkTeamMember verifies the user has an active membership in the required team.
func (g *githubAuth) checkTeamMember(ghToken, login string) error {
	apiURL := fmt.Sprintf("https://api.github.com/orgs/%s/teams/%s/memberships/%s",
		url.PathEscape(g.cfg.Org), url.PathEscape(g.cfg.Team), url.PathEscape(login))
	body, err := g.githubAPI(ghToken, apiURL)
	if err != nil {
		return err
	}
	var m struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(body, &m); err != nil {
		return err
	}
	if !strings.EqualFold(m.State, "active") {
		return fmt.Errorf("team membership not active (state=%s)", m.State)
	}
	return nil
}

func (g *githubAuth) githubAPI(ghToken, apiURL string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+ghToken)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		// Status code only — the response body may carry sensitive detail and
		// reaches logs (AUDIT F-7).
		return nil, fmt.Errorf("github API %s returned %d", apiURL, resp.StatusCode)
	}
	return body, nil
}
