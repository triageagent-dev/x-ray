package xray

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A server mounted under a path (--public-url=https://host/path) behind a proxy
// that forwards the path unchanged, next to other services on the same host.

const prefixedURL = "https://h.example/path"

func newPrefixedServer(t *testing.T) *Server {
	t.Helper()
	gh := GitHubOAuthConfig{ClientID: "gh-client", ClientSecret: "gh-secret", Org: "acme", PublicURL: prefixedURL}
	az := AzureOAuthConfig{PublicURL: prefixedURL}
	return NewServer(zerolog.Nop(), NewMemoryStore(), nil, gh, az, "https://h.example", 0, nil, WithBasePath(BasePath(prefixedURL)))
}

func cookieNamed(w *httptest.ResponseRecorder, name string) *http.Cookie {
	for _, c := range w.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

func TestBasePath(t *testing.T) {
	for in, want := range map[string]string{
		"":                        "",
		"https://h.example":       "",
		"https://h.example/":      "",
		"https://h.example/path":  "/path",
		"https://h.example/path/": "/path",
		"https://h.example/a/b":   "/a/b",
	} {
		assert.Equal(t, want, BasePath(in), in)
	}
}

func TestServer_BasePath_ServesUnderPrefix(t *testing.T) {
	h := newPrefixedServer(t).Handler()
	assert.Equal(t, http.StatusOK, do(t, h, "GET", "/path/healthz", nil).Code)
	assert.Equal(t, http.StatusOK, authedDo(t, h, "GET", "/path/v1/memory", nil).Code)
	// The kubelet probes the pod directly, without the prefix.
	assert.Equal(t, http.StatusOK, do(t, h, "GET", "/healthz", nil).Code)
}

func TestServer_BasePath_BarePrefixRedirectsToSlash(t *testing.T) {
	w := do(t, newPrefixedServer(t).Handler(), "GET", "/path", nil)
	assert.Equal(t, http.StatusMovedPermanently, w.Code)
	assert.Equal(t, "/path/", w.Header().Get("Location"))
}

func TestServer_BasePath_PrefixEndsAtSegment(t *testing.T) {
	w := authedDo(t, newPrefixedServer(t).Handler(), "GET", "/pathx/v1/memory", nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestServer_BasePath_BarePrefixKeepsQuery(t *testing.T) {
	w := do(t, newPrefixedServer(t).Handler(), "GET", "/path?next=x", nil)
	assert.Equal(t, "/path/?next=x", w.Header().Get("Location"))
}

func TestServer_BasePath_ServesEmbeddedUI(t *testing.T) {
	ui := fstest.MapFS{
		"index.html":        {Data: []byte(`<title>home</title><script src="/path/_next/static/a.js"></script>`)},
		"login.html":        {Data: []byte(`<title>login</title>`)},
		"_next/static/a.js": {Data: []byte("boot()")},
	}
	gh := GitHubOAuthConfig{ClientID: "gh-client", PublicURL: prefixedURL}
	h := NewServer(zerolog.Nop(), NewMemoryStore(), ui, gh, AzureOAuthConfig{PublicURL: prefixedURL}, "", 0, nil, WithBasePath("/path")).Handler()

	assert.Contains(t, do(t, h, "GET", "/path/", nil).Body.String(), "<title>home</title>")
	assert.Contains(t, do(t, h, "GET", "/path/login", nil).Body.String(), "<title>login</title>")
	assert.Equal(t, "boot()", do(t, h, "GET", "/path/_next/static/a.js", nil).Body.String())
	assert.Equal(t, http.StatusUnauthorized, do(t, h, "GET", "/path/v1/memory", nil).Code, "the API under the prefix still needs a session")
}

func TestServer_BasePath_SignedOutBrowserGoesToPrefixedLogin(t *testing.T) {
	w := do(t, newPrefixedServer(t).Handler(), "GET", "/path/", nil)
	assert.Equal(t, http.StatusFound, w.Code)
	assert.Equal(t, "/path/login", w.Header().Get("Location"))
}

func TestServer_BasePath_LogoutStaysUnderPrefix(t *testing.T) {
	w := do(t, newPrefixedServer(t).Handler(), "GET", "/path/v1/auth/logout", nil)
	assert.Equal(t, http.StatusFound, w.Code)
	assert.Equal(t, "/path/login", w.Header().Get("Location"))
	c := cookieNamed(w, "xray_session")
	require.NotNil(t, c)
	assert.Equal(t, "/path/", c.Path, "the clearing cookie must match the path the session cookie was set on")
}

func TestServer_BasePath_ManifestAndServiceWorker(t *testing.T) {
	type manifest struct {
		ID       string `json:"id"`
		StartURL string `json:"start_url"`
		Scope    string `json:"scope"`
		Icons    []struct {
			Src string `json:"src"`
		} `json:"icons"`
	}
	read := func(h http.Handler, path string) manifest {
		w := do(t, h, "GET", path, nil)
		require.Equal(t, http.StatusOK, w.Code)
		var m manifest
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &m))
		return m
	}

	m := read(newPrefixedServer(t).Handler(), "/path/manifest.json")
	assert.Equal(t, "/path/", m.ID)
	assert.Equal(t, "/path/", m.StartURL)
	assert.Equal(t, "/path/", m.Scope)
	require.NotEmpty(t, m.Icons)
	for _, icon := range m.Icons {
		assert.True(t, strings.HasPrefix(icon.Src, "/path/"), icon.Src)
	}
	sw := do(t, newPrefixedServer(t).Handler(), "GET", "/path/sw.js", nil)
	assert.Contains(t, sw.Body.String(), `const BASE = "/path";`)

	// At the host root nothing changes.
	root := newTestServer(t).Handler()
	m = read(root, "/manifest.json")
	assert.Equal(t, "/", m.StartURL)
	assert.Equal(t, "/icon-192.png", m.Icons[0].Src)
	assert.Contains(t, do(t, root, "GET", "/sw.js", nil).Body.String(), `const BASE = "";`)
}

// RFC 8414 §3.1 and RFC 9728 §3.1 put an issuer's or resource's path after the
// well-known segment, at the host root, which is where MCP clients look first.
func TestServer_BasePath_PathInsertedMetadata(t *testing.T) {
	s := newPrefixedServer(t)
	a, err := newMCPAuthorizer(prefixedURL, prefixedURL+"/mcp", 15*time.Minute, false, nil, nil, zerolog.Nop())
	require.NoError(t, err)
	s.SetMCPAuth(a, nil)
	h := s.Handler()

	var m map[string]any
	w := do(t, h, "GET", "/.well-known/oauth-authorization-server/path", nil)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &m))
	assert.Equal(t, prefixedURL, m["issuer"])
	assert.Equal(t, prefixedURL+"/token", m["token_endpoint"])

	w = do(t, h, "GET", "/.well-known/oauth-protected-resource/path/mcp", nil)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &m))
	assert.Equal(t, prefixedURL+"/mcp", m["resource"])

	// The 401 challenge's resource_metadata URL is under the prefix.
	assert.Equal(t, http.StatusOK, do(t, h, "GET", "/path/.well-known/oauth-protected-resource", nil).Code)
}

func TestMCPAuth_AuthorizeLoginRedirectUnderBasePath(t *testing.T) {
	a, err := newMCPAuthorizer(prefixedURL, prefixedURL+"/mcp", 15*time.Minute, false, nil, nil, zerolog.Nop())
	require.NoError(t, err)
	clientID := registerClient(t, a, "http://localhost:9999/cb")
	_, challenge := pkcePair()
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", clientID)
	q.Set("redirect_uri", "http://localhost:9999/cb")
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	rec := httptest.NewRecorder()
	a.handleAuthorize(rec, httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil))
	require.Equal(t, http.StatusFound, rec.Code)
	assert.Equal(t, "/path/login", rec.Header().Get("Location"))
}

func TestGoogleSignIn_UnderBasePath(t *testing.T) {
	tokenSrv := httptest.NewServer(tokenReply(t, googleTestToken(t, workspaceClaims())))
	t.Cleanup(tokenSrv.Close)
	g := newTestGoogleAuth(tokenSrv.URL)
	g.cfg.PublicURL = prefixedURL

	w := httptest.NewRecorder()
	g.handleLogin(w, httptest.NewRequest(http.MethodGet, "/v1/auth/google", nil))
	loc, err := url.Parse(w.Header().Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, prefixedURL+"/v1/auth/google/callback", loc.Query().Get("redirect_uri"))
	state := cookieNamed(w, oauthStateCookie)
	require.NotNil(t, state)
	assert.Equal(t, "/path/v1/auth/", state.Path, "the browser must send the state cookie back to the prefixed callback")

	w = googleReturn(g, loc.Query().Get("state"), w.Result().Cookies())
	require.Equal(t, http.StatusFound, w.Code)
	assert.Equal(t, "/path/", w.Header().Get("Location"))
	session := sessionCookie(w)
	require.NotNil(t, session)
	assert.Equal(t, "/path/", session.Path, "the session must not reach the other services on the host")
	cleared := cookieNamed(w, oauthStateCookie)
	require.NotNil(t, cleared)
	assert.Equal(t, "/path/v1/auth/", cleared.Path)
}

// A resource URL without a path keeps only the root document: inserting an empty
// path would register the same pattern twice.
func TestServer_BasePath_ResourceWithoutPath(t *testing.T) {
	s := newPrefixedServer(t)
	a, err := newMCPAuthorizer(prefixedURL, "https://mcp.example", 15*time.Minute, false, nil, nil, zerolog.Nop())
	require.NoError(t, err)
	require.NotPanics(t, func() { s.SetMCPAuth(a, nil) })
	assert.Equal(t, http.StatusOK, do(t, s.Handler(), "GET", "/.well-known/oauth-protected-resource", nil).Code)
}
