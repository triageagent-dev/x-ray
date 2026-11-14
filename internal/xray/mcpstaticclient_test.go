package xray

import (
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pre-registered public client (a client given an ID, not registering itself): its
// redirect_uris are fixed at startup, so open DCR need not accept them.

const connectorCallback = "https://connector.example/api/mcp/auth_callback"

func TestMCPAuth_StaticClientAllowsOnlyItsRedirects(t *testing.T) {
	a := newTestAuthorizer(t, false, nil)
	require.NoError(t, a.SetStaticClient("connector-1", []string{connectorCallback}))
	assert.True(t, a.clientRedirectAllowed("connector-1", connectorCallback))
	assert.False(t, a.clientRedirectAllowed("connector-1", "https://evil.example/api/mcp/auth_callback"))
	assert.False(t, a.clientRedirectAllowed("connector-1", "http://localhost:9999/cb"),
		"a static client is not widened by the DCR loopback rule")
	assert.False(t, isAllowedDCRRedirectURI(connectorCallback), "open registration still refuses a public host")
	assert.False(t, a.clientRedirectAllowed("someone-else", connectorCallback))
}

func TestMCPAuth_StaticClientValidation(t *testing.T) {
	a := newTestAuthorizer(t, false, nil)
	assert.Error(t, a.SetStaticClient("short", []string{connectorCallback}), "client ID under 8 characters")
	assert.Error(t, a.SetStaticClient("connector-1", nil), "no redirect URI")
	assert.Error(t, a.SetStaticClient("connector-1", []string{"http://connector.example/cb"}), "plain http off loopback")
	assert.Error(t, a.SetStaticClient("connector-1", []string{"/api/mcp/auth_callback"}), "relative")
	assert.Error(t, a.SetStaticClient("connector-1", []string{"https://connector.example/cb#frag"}), "fragment")
	assert.NoError(t, a.SetStaticClient("connector-1", []string{"http://localhost:3000/cb", connectorCallback}))
}

// A pre-registered client's code goes to an https callback shared by every user of
// that service, so a link that sends a signed-in user straight through /authorize
// could hand their code to someone else's flow. The user approves on a page first.

func staticAuthorize(t *testing.T, a *mcpAuthorizer, sessTok, challenge, extra string) *httptest.ResponseRecorder {
	t.Helper()
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", "connector-1")
	q.Set("redirect_uri", connectorCallback)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	q.Set("state", "st-1")
	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode()+extra, nil)
	req.AddCookie(&http.Cookie{Name: "xray_session", Value: sessTok})
	rec := httptest.NewRecorder()
	a.handleAuthorize(rec, req)
	return rec
}

func consentLink(t *testing.T, page string) string {
	t.Helper()
	m := regexp.MustCompile(`href="([^"]*consent=[^"]*)"`).FindStringSubmatch(page)
	require.Len(t, m, 2, "the page carries an approve link")
	link := html.UnescapeString(m[1])
	u, err := url.Parse(link)
	require.NoError(t, err)
	return "&consent=" + url.QueryEscape(u.Query().Get("consent"))
}

func newStaticAuthorizer(t *testing.T) (*mcpAuthorizer, string) {
	t.Helper()
	a := newTestAuthorizer(t, false, []string{stableUserID("google:42")})
	require.NoError(t, a.SetStaticClient("connector-1", []string{connectorCallback}))
	sessTok := sessions.create("alice", "google:42")
	t.Cleanup(func() { sessions.invalidate(sessTok) })
	return a, sessTok
}

func TestMCPAuth_StaticClientAsksForConsent(t *testing.T) {
	a, sessTok := newStaticAuthorizer(t)
	_, challenge := pkcePair()
	rec := staticAuthorize(t, a, sessTok, challenge, "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("Location"), "no code without the user's approval")
	body := rec.Body.String()
	assert.Contains(t, body, "connector-1")
	assert.Contains(t, body, "connector.example")
	assert.Contains(t, body, "alice")
}

func TestMCPAuth_StaticClientIssuesCodeAfterConsent(t *testing.T) {
	a, sessTok := newStaticAuthorizer(t)
	_, challenge := pkcePair()
	page := staticAuthorize(t, a, sessTok, challenge, "").Body.String()
	rec := staticAuthorize(t, a, sessTok, challenge, consentLink(t, page))
	require.Equal(t, http.StatusFound, rec.Code)
	loc, err := url.Parse(rec.Header().Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, "connector.example", loc.Host)
	assert.NotEmpty(t, loc.Query().Get("code"))
	assert.Equal(t, "st-1", loc.Query().Get("state"))
}

func TestMCPAuth_ConsentIsBoundToSessionAndRequest(t *testing.T) {
	a, sessTok := newStaticAuthorizer(t)
	_, challenge := pkcePair()
	approve := consentLink(t, staticAuthorize(t, a, sessTok, challenge, "").Body.String())

	other := sessions.create("alice", "google:42") // same user, another browser
	defer sessions.invalidate(other)
	assert.Empty(t, staticAuthorize(t, a, other, challenge, approve).Header().Get("Location"),
		"an approval from one browser session does not work in another")

	_, otherChallenge := pkcePair()
	assert.Empty(t, staticAuthorize(t, a, sessTok, otherChallenge, approve).Header().Get("Location"),
		"an approval does not carry over to another code_challenge")
}

func TestMCPAuth_ConsentExpires(t *testing.T) {
	a, sessTok := newStaticAuthorizer(t)
	q := url.Values{"client_id": {"connector-1"}, "redirect_uri": {connectorCallback}, "code_challenge": {"c"}}
	expired := a.consentToken(sessTok, q, time.Now().Add(-time.Second))
	assert.False(t, a.consentValid(sessTok, q, expired))
	assert.True(t, a.consentValid(sessTok, q, a.consentToken(sessTok, q, time.Now().Add(time.Minute))))
}

func TestMCPAuth_TokenRequiresTheCodesClientAndRedirectURI(t *testing.T) {
	for name, tc := range map[string]struct{ field, value, want string }{
		"client_id":    {"client_id", "someone-else", "invalid_grant"},
		"redirect_uri": {"redirect_uri", "https://elsewhere.example/callback", "redirect_uri mismatch"},
	} {
		t.Run(name, func(t *testing.T) {
			a, sessTok := newStaticAuthorizer(t)
			verifier, challenge := pkcePair()
			page := staticAuthorize(t, a, sessTok, challenge, "").Body.String()
			loc, err := url.Parse(staticAuthorize(t, a, sessTok, challenge, consentLink(t, page)).Header().Get("Location"))
			require.NoError(t, err)
			form := url.Values{
				"grant_type":    {"authorization_code"},
				"code":          {loc.Query().Get("code")},
				"code_verifier": {verifier},
				tc.field:        {tc.value},
			}
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			a.handleToken(rec, req)
			assert.Contains(t, rec.Body.String(), tc.want)
		})
	}
}
