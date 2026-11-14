package xray

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testGoogleClient = "client-1.apps.googleusercontent.com"

func googleTestToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	enc := func(v any) string {
		b, err := json.Marshal(v)
		require.NoError(t, err)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]any{"alg": "RS256", "typ": "JWT"}) + "." + enc(claims) + ".sig"
}

func workspaceClaims() map[string]any {
	return map[string]any{
		"iss":            "https://accounts.google.com",
		"aud":            testGoogleClient,
		"sub":            "1234567890",
		"email":          "alice@example.com",
		"email_verified": true,
		"hd":             "example.com",
		"exp":            time.Now().Add(time.Hour).Unix(),
	}
}

func newTestGoogleAuth(tokenURL string) *googleAuth {
	g := newGoogleAuth(GoogleOAuthConfig{
		ClientID:          testGoogleClient,
		ClientSecret:      "secret",
		Domain:            "example.com",
		PublicURL:         "https://xray.example",
		UICallbackOrigins: []string{"https://ui.example"},
	})
	if tokenURL != "" {
		g.tokenURL = tokenURL
	}
	return g
}

func TestGoogleIdentity_AcceptsWorkspaceAccount(t *testing.T) {
	g := newTestGoogleAuth("")
	login, sub, err := g.identity(googleTestToken(t, workspaceClaims()), time.Now())
	require.NoError(t, err)
	assert.Equal(t, "alice@example.com", login)
	assert.Equal(t, "1234567890", sub)
}

func TestGoogleIdentity_Refuses(t *testing.T) {
	cases := map[string]func(c map[string]any){
		"other workspace domain":  func(c map[string]any) { c["hd"] = "evil.example" },
		"consumer account, no hd": func(c map[string]any) { delete(c, "hd") },
		"email suffix without hd": func(c map[string]any) { delete(c, "hd"); c["email"] = "x@example.com" },
		"email not verified":      func(c map[string]any) { c["email_verified"] = false },
		"another client's token":  func(c map[string]any) { c["aud"] = "other.apps.googleusercontent.com" },
		"wrong issuer":            func(c map[string]any) { c["iss"] = "https://evil.example" },
		"expired":                 func(c map[string]any) { c["exp"] = time.Now().Add(-time.Minute).Unix() },
		"no subject":              func(c map[string]any) { delete(c, "sub") },
		"no email":                func(c map[string]any) { delete(c, "email") },
		"expires now":             func(c map[string]any) { c["exp"] = time.Now().Unix() },
		"aud as a list":           func(c map[string]any) { c["aud"] = []string{testGoogleClient} },
		"email_verified a string": func(c map[string]any) { c["email_verified"] = "true" },
	}
	g := newTestGoogleAuth("")
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := workspaceClaims()
			mutate(c)
			_, _, err := g.identity(googleTestToken(t, c), time.Now())
			assert.Error(t, err)
		})
	}
	t.Run("malformed token", func(t *testing.T) {
		_, _, err := g.identity("not-a-jwt", time.Now())
		assert.Error(t, err)
	})
}

func TestGoogleIdentity_AcceptsBareIssuerAndDomainCase(t *testing.T) {
	c := workspaceClaims()
	c["iss"] = "accounts.google.com"
	c["hd"] = "Example.COM"
	_, _, err := newTestGoogleAuth("").identity(googleTestToken(t, c), time.Now())
	assert.NoError(t, err)
}

func TestGoogleIdentity_NoDomainStillNeedsVerifiedEmail(t *testing.T) {
	c := workspaceClaims()
	c["email_verified"] = false
	_, _, err := newGoogleAuth(GoogleOAuthConfig{ClientID: testGoogleClient}).identity(googleTestToken(t, c), time.Now())
	assert.Error(t, err)
}

func TestGoogleIdentity_AnyDomainOnlyWhenUnset(t *testing.T) {
	g := newGoogleAuth(GoogleOAuthConfig{ClientID: testGoogleClient})
	c := workspaceClaims()
	delete(c, "hd")
	c["email"] = "someone@gmail.com"
	login, _, err := g.identity(googleTestToken(t, c), time.Now())
	require.NoError(t, err)
	assert.Equal(t, "someone@gmail.com", login)
}

func TestGoogleCallback_SignsInNonASCIIAddressWithNoLists(t *testing.T) {
	c := workspaceClaims()
	c["email"] = "jos\u00e9@example.com"
	w := googleCallback(t, googleTestToken(t, c))
	require.Equal(t, http.StatusFound, w.Code)
	require.NotNil(t, sessionCookie(w))
}

func TestGoogleCallback_NonASCIIAddressIsRefusedByAList(t *testing.T) {
	withGrants(t, "google:k@example.com", "")
	c := workspaceClaims()
	c["email"] = "\u212A@example.com" // the Kelvin sign lower-cases to "k"
	w := googleCallback(t, googleTestToken(t, c))
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Nil(t, sessionCookie(w))
}

func TestGoogleLogin_RedirectsWithStateAndDomainHint(t *testing.T) {
	g := newTestGoogleAuth("")
	w := httptest.NewRecorder()
	g.handleLogin(w, httptest.NewRequest(http.MethodGet, "/v1/auth/google", nil))
	require.Equal(t, http.StatusFound, w.Code)
	loc, err := url.Parse(w.Header().Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, "accounts.google.com", loc.Host)
	q := loc.Query()
	assert.Equal(t, testGoogleClient, q.Get("client_id"))
	assert.Equal(t, "https://xray.example/v1/auth/google/callback", q.Get("redirect_uri"))
	assert.Equal(t, "example.com", q.Get("hd"))
	assert.Contains(t, q.Get("scope"), "openid")
	assert.NotEmpty(t, q.Get("state"))
	assert.NotEmpty(t, w.Result().Cookies(), "the login must set the signed state cookie")
}

// googleLogin starts a login against a fake token endpoint and returns the
// auth, the state Google would echo back and the cookies the browser holds.
func googleLogin(t *testing.T, token http.HandlerFunc, query string) (*googleAuth, string, []*http.Cookie) {
	t.Helper()
	tokenSrv := httptest.NewServer(token)
	t.Cleanup(tokenSrv.Close)
	g := newTestGoogleAuth(tokenSrv.URL)
	w := httptest.NewRecorder()
	g.handleLogin(w, httptest.NewRequest(http.MethodGet, "/v1/auth/google"+query, nil))
	loc, err := url.Parse(w.Header().Get("Location"))
	require.NoError(t, err)
	return g, loc.Query().Get("state"), w.Result().Cookies()
}

func googleReturn(g *googleAuth, state string, cookies []*http.Cookie) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/v1/auth/google/callback?code=the-code&state="+state, nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	w := httptest.NewRecorder()
	g.handleCallback(w, req)
	return w
}

func tokenReply(t *testing.T, idToken string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		assert.Equal(t, "the-code", r.PostForm.Get("code"))
		assert.Equal(t, "secret", r.PostForm.Get("client_secret"))
		json.NewEncoder(w).Encode(map[string]string{"id_token": idToken, "access_token": "at"}) //nolint:errcheck
	}
}

// googleCallback runs a full login that returns idToken.
func googleCallback(t *testing.T, idToken string) *httptest.ResponseRecorder {
	t.Helper()
	g, state, cookies := googleLogin(t, tokenReply(t, idToken), "")
	return googleReturn(g, state, cookies)
}

func sessionCookie(w *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range w.Result().Cookies() {
		if c.Name == "xray_session" && c.Value != "" {
			return c
		}
	}
	return nil
}

func TestGoogleCallback_SignsInWorkspaceUser(t *testing.T) {
	w := googleCallback(t, googleTestToken(t, workspaceClaims()))
	require.Equal(t, http.StatusFound, w.Code)
	c := sessionCookie(w)
	require.NotNil(t, c)
	username, userID, ok := sessions.validate(c.Value)
	require.True(t, ok)
	assert.Equal(t, "alice@example.com", username)
	assert.Equal(t, "google:1234567890", userID)
}

func TestGoogleCallback_RefusesOtherDomain(t *testing.T) {
	c := workspaceClaims()
	c["hd"] = "evil.example"
	w := googleCallback(t, googleTestToken(t, c))
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Nil(t, sessionCookie(w))
}

func TestGoogleCallback_RefusesForeignState(t *testing.T) {
	g := newTestGoogleAuth("")
	w := httptest.NewRecorder()
	g.handleCallback(w, httptest.NewRequest(http.MethodGet, "/v1/auth/google/callback?code=x&state=forged", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, sessionCookie(w))
}

func TestGoogleCallback_StateIsSingleUse(t *testing.T) {
	g, state, cookies := googleLogin(t, tokenReply(t, googleTestToken(t, workspaceClaims())), "")
	require.Equal(t, http.StatusFound, googleReturn(g, state, cookies).Code)
	again := googleReturn(g, state, cookies)
	assert.Equal(t, http.StatusBadRequest, again.Code)
	assert.Nil(t, sessionCookie(again))
}

func TestGoogleCallback_RefusesStateNotBoundToCookie(t *testing.T) {
	g, _, cookies := googleLogin(t, tokenReply(t, googleTestToken(t, workspaceClaims())), "")
	_, otherState, _ := googleLogin(t, tokenReply(t, googleTestToken(t, workspaceClaims())), "")
	w := googleReturn(g, otherState, cookies)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, sessionCookie(w))
}

func TestGoogleCallback_TokenEndpointFailures(t *testing.T) {
	cases := map[string]http.HandlerFunc{
		"oauth error": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{"error":"invalid_grant","error_description":"bad code"}`)) //nolint:errcheck
		},
		"not json":       func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("<html>")) },                //nolint:errcheck
		"empty id_token": func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{"access_token":"at"}`)) }, //nolint:errcheck
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			g, state, cookies := googleLogin(t, h, "")
			w := googleReturn(g, state, cookies)
			assert.Equal(t, http.StatusBadGateway, w.Code)
			assert.Nil(t, sessionCookie(w))
		})
	}
}

func TestGoogleCallback_UIHandOffOnlyToAllowlistedOrigin(t *testing.T) {
	token := googleTestToken(t, workspaceClaims())
	g, state, cookies := googleLogin(t, tokenReply(t, token), "?ui=https://ui.example")
	w := googleReturn(g, state, cookies)
	require.Equal(t, http.StatusFound, w.Code)
	assert.True(t, strings.HasPrefix(w.Header().Get("Location"), "https://ui.example/auth/callback?token="))

	g, state, cookies = googleLogin(t, tokenReply(t, token), "?ui=https://evil.example")
	w = googleReturn(g, state, cookies)
	require.Equal(t, http.StatusFound, w.Code)
	assert.Equal(t, "/", w.Header().Get("Location"))
}

func TestServer_GoogleRoutesAndProvider(t *testing.T) {
	s := NewServer(zerolog.Nop(), NewMemoryStore(), nil, GitHubOAuthConfig{}, AzureOAuthConfig{}, "", 0, nil,
		WithGoogleOAuth(GoogleOAuthConfig{ClientID: testGoogleClient, Domain: "example.com", PublicURL: "https://xray.example"}))
	w := do(t, s.Handler(), "GET", "/v1/auth/providers", nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["google"])
	assert.NotContains(t, resp, "google_domain")
	assert.False(t, s.authDisabled())

	w = do(t, s.Handler(), "GET", "/v1/auth/google", nil)
	assert.Equal(t, http.StatusFound, w.Code)
	assert.True(t, strings.HasPrefix(w.Header().Get("Location"), "https://accounts.google.com/"))
}

func TestMCPSigningSeed_UnchangedWithoutGoogle(t *testing.T) {
	assert.Equal(t, mcpSigningSeed("gh", "az"), mcpSigningSeed("gh", "az", ""))
}
