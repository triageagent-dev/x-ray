package xray

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubTransport answers the provider calls of a sign-in callback without network.
type stubTransport func(*http.Request) (int, string)

func (f stubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	code, body := f(r)
	return &http.Response{
		StatusCode: code,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    r,
	}, nil
}

// stubProviders swaps http.DefaultTransport for the test; callers must not be parallel.
func stubProviders(t *testing.T, f stubTransport) {
	t.Helper()
	prev := http.DefaultTransport
	http.DefaultTransport = f
	t.Cleanup(func() { http.DefaultTransport = prev })
}

type loginStarter interface {
	handleLogin(http.ResponseWriter, *http.Request)
	handleCallback(http.ResponseWriter, *http.Request)
}

// runCallback starts a login the way the browser does and returns the callback's response.
func runCallback(t *testing.T, a loginStarter, path string) *httptest.ResponseRecorder {
	t.Helper()
	lw := httptest.NewRecorder()
	a.handleLogin(lw, httptest.NewRequest(http.MethodGet, path, nil))
	loc := lw.Header().Get("Location")
	require.NotEmpty(t, loc)
	state := loc[strings.Index(loc, "state=")+len("state="):]
	req := httptest.NewRequest(http.MethodGet, path+"/callback?code=c&state="+state, nil)
	for _, c := range lw.Result().Cookies() {
		req.AddCookie(c)
	}
	w := httptest.NewRecorder()
	a.handleCallback(w, req)
	return w
}

func githubStub(t *testing.T) stubTransport {
	return func(r *http.Request) (int, string) {
		switch r.URL.Host + r.URL.Path {
		case "github.com/login/oauth/access_token":
			return 200, "access_token=gh-token"
		case "api.github.com/user":
			return 200, `{"login":"octocat","id":42}`
		}
		t.Errorf("unexpected provider call %s", r.URL)
		return 404, ""
	}
}

func TestGitHubCallback_GatedByAccessLists(t *testing.T) {
	stubProviders(t, githubStub(t))
	g := newGitHubAuth(GitHubOAuthConfig{ClientID: "id", ClientSecret: "s", PublicURL: "https://xray.example"})

	withGrants(t, "github:43", "")
	w := runCallback(t, g, "/v1/auth/github")
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Nil(t, sessionCookie(w))

	withGrants(t, "github:42", "")
	w = runCallback(t, g, "/v1/auth/github")
	require.Equal(t, http.StatusFound, w.Code)
	c := sessionCookie(w)
	require.NotNil(t, c)
	_, uid, ok := sessions.validate(c.Value)
	require.True(t, ok)
	assert.Equal(t, "github:42", uid)
}

func TestEntraCallback_GatedByAccessLists(t *testing.T) {
	stubProviders(t, func(r *http.Request) (int, string) {
		switch r.URL.Host + r.URL.Path {
		case "login.microsoftonline.com/tenant/oauth2/v2.0/token":
			return 200, `{"access_token":"az-token"}`
		case "graph.microsoft.com/v1.0/me":
			return 200, `{"id":"` + oidA + `","userPrincipalName":"carol@example.com"}`
		}
		t.Errorf("unexpected provider call %s", r.URL)
		return 404, ""
	})
	a := newAzureAuth(AzureOAuthConfig{ClientID: "id", ClientSecret: "s", TenantID: "tenant", PublicURL: "https://xray.example"})

	withGrants(t, "azure:"+oidB, "")
	w := runCallback(t, a, "/v1/auth/entra")
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Nil(t, sessionCookie(w))

	withGrants(t, "azure:"+oidA, "")
	w = runCallback(t, a, "/v1/auth/entra")
	require.Equal(t, http.StatusFound, w.Code)
	c := sessionCookie(w)
	require.NotNil(t, c)
	_, uid, ok := sessions.validate(c.Value)
	require.True(t, ok)
	assert.Equal(t, "azure:"+oidA, uid)
}
