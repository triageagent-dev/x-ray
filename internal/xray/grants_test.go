package xray

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Access named by principal in the deployment's configuration (ADR-066): who may
// sign in, with any provider, and who gets the admin interface.

func withGrants(t *testing.T, users, admins string) {
	t.Helper()
	g, err := NewAccessGrants(users, admins, "example.com")
	require.NoError(t, err)
	prev := grants
	SetAccessGrants(g)
	t.Cleanup(func() { grants = prev })
}

func TestNewAccessGrants_NormalisesAndValidates(t *testing.T) {
	g, err := NewAccessGrants(" google:Alice@Example.com, ,google:bob@example.com ", "Google:Carol@example.com", "example.com")
	require.NoError(t, err)
	assert.True(t, g.allowsUser("google:1", "alice@example.com"))
	assert.True(t, g.allowsUser("google:2", "BOB@example.com"))
	assert.True(t, g.isAdmin("google:3", "carol@EXAMPLE.com"))

	_, err = NewAccessGrants("", "", "")
	assert.NoError(t, err, "no lists: nothing to check")
	_, err = NewAccessGrants("google:Alice@EXAMPLE.com", "", "Example.com")
	assert.NoError(t, err)
}

const (
	oidA = "0a1b2c3d-0000-4000-8000-000000000001"
	oidB = "0a1b2c3d-0000-4000-8000-000000000002"
)

func TestNewAccessGrants_ParsesEveryForm(t *testing.T) {
	g, err := NewAccessGrants(" github:42 , AZURE:"+strings.ToUpper(oidA)+",google:Alice@Example.com", "github:7", "example.com")
	require.NoError(t, err)
	assert.True(t, g.allowsUser("github:42", "x"))
	assert.True(t, g.allowsUser("azure:"+oidA, "x"))
	assert.True(t, g.allowsUser("azure:"+strings.ToUpper(oidA), "x"), "an Entra id is compared lower-cased")
	assert.True(t, g.allowsUser("google:1", "alice@example.com"))
	assert.True(t, g.isAdmin("github:7", "x"))
	nu, na := g.Counts()
	assert.Equal(t, 3, nu)
	assert.Equal(t, 1, na)
}

func TestNewAccessGrants_RejectsMalformedPrincipals(t *testing.T) {
	for _, bad := range []string{
		"alice@example.com",             // bare address: no provider
		"gitlab:42",                     // unknown prefix
		"github:octocat",                // a login, not the numeric id
		"github:",                       // empty id
		"github:4 2",                    // not digits
		"azure:alice@example.com",       // a UPN, not the object id
		"azure:1234",                    // not a GUID
		"google:alice",                  // not an address
		"google:alice@other.test",       // outside the domain
		"google:\u212arate@example.com", // strings.ToLower folds the Kelvin sign into "k"
		"google:alice@sub.example.com",  // a subdomain is not the domain
		"google: a@example.com",         // whitespace inside a principal
		"google:a b@example.com",
		"google:a\t@example.com",
		"github:\n42",
		"azure: " + oidA,
		"google:a\u00a0@example.com", // no-break space
	} {
		_, err := NewAccessGrants(bad, "", "example.com")
		assert.Error(t, err, "users: %q", bad)
		_, err = NewAccessGrants("", bad, "example.com")
		assert.Error(t, err, "admins: %q", bad)
	}
	// Without the Workspace domain check a consumer Google account can carry a
	// verified address on any domain, so a principal list would admit a look-alike.
	_, err := NewAccessGrants("google:alice@example.com", "", "")
	assert.Error(t, err, "a google principal needs --google-domain")
	_, err = NewAccessGrants("github:42", "", "")
	assert.NoError(t, err, "github and azure principals do not need the Workspace domain")
	_, err = NewAccessGrants("octocat", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "octocat")
	assert.Contains(t, err.Error(), "github:<numeric user id>")
}

func TestAccessGrants_Decisions(t *testing.T) {
	type who struct {
		userID, username string
		allowed, admin   bool
	}
	pa, pb := "azure:"+oidA, "azure:"+oidB
	for name, tc := range map[string]struct {
		users, admins, domain string
		sessions              []who
	}{
		"no lists admit every provider, nobody is admin": {"", "", "", []who{
			{"google:1", "anyone@example.com", true, false}, {"github:42", "mallory", true, false},
			{pa, "carol@example.com", true, false}}},
		"users list restricts sign-in": {"google:alice@example.com", "google:carol@example.com", "example.com", []who{
			{"google:1", "alice@example.com", true, false},
			{"google:3", "carol@example.com", true, true}, // an admin is also a user
			{"google:2", "mallory@example.com", false, false}}},
		// Either list set means only listed addresses sign in (documented on the flags).
		"admins-only list admits only admins": {"", "google:carol@example.com", "example.com", []who{
			{"google:3", "carol@example.com", true, true}, {"google:1", "alice@example.com", false, false}}},
		"non-Google sessions are gated by id": {"github:42," + pa, "", "", []who{
			{"github:42", "octocat", true, false},
			{"github:43", "octocat", false, false}, // the login is not the key: another id fails
			{pa, "carol@example.com", true, false}, {pb, "carol@example.com", false, false}}},
		"principals do not cross providers": {"google:carol@example.com,github:42", "", "example.com", []who{
			{pa, "carol@example.com", false, false},            // an Entra UPN equal to a listed address
			{"google:42", "mallory@example.com", false, false}, // a Google sub is not a GitHub id
			{"github:99", "carol@example.com", false, false}}},
		"admin per provider": {"github:1", "google:carol@example.com,github:42," + pa, "example.com", []who{
			{"google:3", "carol@example.com", true, true}, {"github:42", "octocat", true, true},
			{pa, "x", true, true},
			{"github:1", "octocat", true, false}, // a user is not an admin
			{pb, "carol@example.com", false, false}}},
	} {
		t.Run(name, func(t *testing.T) {
			g, err := NewAccessGrants(tc.users, tc.admins, tc.domain)
			require.NoError(t, err)
			for _, w := range tc.sessions {
				assert.Equal(t, w.allowed, g.allowsUser(w.userID, w.username), "allowed %s %s", w.userID, w.username)
				assert.Equal(t, w.admin, g.isAdmin(w.userID, w.username), "admin %s %s", w.userID, w.username)
			}
		})
	}
}

// strings.ToLower folds the Kelvin sign (U+212A) to "k", so a non-ASCII address
// must never match a listed ASCII principal; with a list set it is simply refused.
func TestAccessGrants_NonASCIIGoogleAddressMatchesNothing(t *testing.T) {
	g, err := NewAccessGrants("google:k@example.com", "google:k@example.com", "example.com")
	require.NoError(t, err)
	assert.False(t, g.allowsUser("google:1", "\u212A@example.com"))
	assert.False(t, g.isAdmin("google:1", "\u212A@example.com"))
	assert.True(t, g.allowsUser("google:2", "k@example.com"))
}

func TestAccessGrants_RequireProviders(t *testing.T) {
	g, err := NewAccessGrants("google:a@example.com", "github:42,azure:"+oidA, "example.com")
	require.NoError(t, err)
	assert.NoError(t, g.RequireProviders(true, true, true))
	assert.Error(t, g.RequireProviders(false, true, true), "google entry, no google client")
	assert.Error(t, g.RequireProviders(true, false, true), "github entry, no github client")
	assert.Error(t, g.RequireProviders(true, true, false), "azure entry, no azure client")
	assert.NoError(t, AccessGrants{}.RequireProviders(false, false, false))
	gh, err := NewAccessGrants("github:42", "", "")
	require.NoError(t, err)
	assert.NoError(t, gh.RequireProviders(false, true, false))
}

func TestGoogleCallback_RefusesAccountNotOnUsersList(t *testing.T) {
	withGrants(t, "google:bob@example.com", "")
	w := googleCallback(t, googleTestToken(t, workspaceClaims())) // alice@example.com
	assert.Equal(t, http.StatusForbidden, w.Code)
	assert.Nil(t, sessionCookie(w))
}

func TestGoogleCallback_SessionCarriesTheFullAddress(t *testing.T) {
	withGrants(t, "google:alice@example.com", "")
	w := googleCallback(t, googleTestToken(t, workspaceClaims()))
	require.Equal(t, http.StatusFound, w.Code)
	c := sessionCookie(w)
	require.NotNil(t, c)
	username, userID, ok := sessions.validate(c.Value)
	require.True(t, ok)
	assert.Equal(t, "alice@example.com", username)
	assert.Equal(t, "google:1234567890", userID)
}

func TestRequireSession_DropsAUserNoLongerListed(t *testing.T) {
	store := withGlobalSessions(t)
	tok := store.create("alice@example.com", "google:1")
	withGrants(t, "google:bob@example.com", "")
	s := NewServer(zerolog.Nop(), NewMemoryStore(), nil, GitHubOAuthConfig{}, AzureOAuthConfig{}, "", 0, nil,
		WithGoogleOAuth(GoogleOAuthConfig{ClientID: "c", Domain: "example.com"}))
	req := httptest.NewRequest(http.MethodGet, "/v1/memory", nil)
	req.AddCookie(&http.Cookie{Name: "xray_session", Value: tok})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code, "removal from the list ends the session's access")
}

func authorizeAs(t *testing.T, a *mcpAuthorizer, sessTok string) *httptest.ResponseRecorder {
	t.Helper()
	clientID := registerClient(t, a, "http://localhost:9999/cb")
	_, challenge := pkcePair()
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", clientID)
	q.Set("redirect_uri", "http://localhost:9999/cb")
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	req := httptest.NewRequest(http.MethodGet, "/authorize?"+q.Encode(), nil)
	req.AddCookie(&http.Cookie{Name: "xray_session", Value: sessTok})
	rec := httptest.NewRecorder()
	a.handleAuthorize(rec, req)
	return rec
}

func TestMCPAuth_AdminByAddressGetsFullScopes(t *testing.T) {
	withGrants(t, "", "google:carol@example.com")
	a := newTestAuthorizer(t, false, nil) // empty writeAllowedUsers
	tok := sessions.create("carol@example.com", "google:3")
	defer sessions.invalidate(tok)

	rec := authorizeAs(t, a, tok)
	require.Equal(t, http.StatusFound, rec.Code)
	loc, err := url.Parse(rec.Header().Get("Location"))
	require.NoError(t, err)
	code := loc.Query().Get("code")
	require.NotEmpty(t, code)
	a.mu.Lock()
	ac := a.codes[code]
	a.mu.Unlock()
	assert.ElementsMatch(t, allMCPScopes, ac.scopes)
	assert.WithinDuration(t, time.Now().Add(2*time.Minute), ac.expiresAt, 5*time.Second)
}

func TestMCPAuth_UserWhoIsNotAdminIsDenied(t *testing.T) {
	withGrants(t, "google:alice@example.com", "google:carol@example.com")
	a := newTestAuthorizer(t, false, nil)
	tok := sessions.create("alice@example.com", "google:1")
	defer sessions.invalidate(tok)

	rec := authorizeAs(t, a, tok)
	require.Equal(t, http.StatusFound, rec.Code)
	loc, err := url.Parse(rec.Header().Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, "access_denied", loc.Query().Get("error"))
}

func TestMCPAuth_UnlistedSessionMustSignInAgain(t *testing.T) {
	withGrants(t, "google:bob@example.com", "")
	a := newTestAuthorizer(t, false, []string{stableUserID("google:1")})
	tok := sessions.create("alice@example.com", "google:1")
	defer sessions.invalidate(tok)

	rec := authorizeAs(t, a, tok)
	require.Equal(t, http.StatusFound, rec.Code)
	assert.Contains(t, rec.Header().Get("Location"), "/login")
}

func TestVoiceWriters_AddsAnAdminByAddress(t *testing.T) {
	withGrants(t, "", "google:carol@example.com")
	base := []string{"hash-from-the-cell"}
	got := voiceWriters(base, "google:3", "carol@example.com", "carol-hash")
	assert.ElementsMatch(t, []string{"hash-from-the-cell", "carol-hash"}, got)
	assert.Equal(t, []string{"hash-from-the-cell"}, base, "the cell's list is not modified")
	assert.Equal(t, base, voiceWriters(base, "google:1", "alice@example.com", "alice-hash"))
}

func serveWith(t *testing.T, path string, authorize func(*http.Request)) int {
	t.Helper()
	s := NewServer(zerolog.Nop(), NewMemoryStore(), nil, GitHubOAuthConfig{ClientID: "gh"}, AzureOAuthConfig{}, "", 0, nil,
		WithGoogleOAuth(GoogleOAuthConfig{ClientID: "c", Domain: "example.com"}))
	req := httptest.NewRequest(http.MethodGet, path, nil)
	authorize(req)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	return w.Code
}

func TestSessions_UnlistedOnEveryCarrierAndInAuthMe(t *testing.T) {
	store := withGlobalSessions(t)
	alice := store.create("alice@example.com", "google:1")
	withGrants(t, "google:bob@example.com", "")

	bearer := func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+alice) }
	assert.Equal(t, http.StatusUnauthorized, serveWith(t, "/v1/memory", bearer), "the Bearer carrier is filtered too")
	assert.Equal(t, http.StatusUnauthorized, serveWith(t, "/v1/auth/me", bearer), "/v1/auth/me does not show a removed user as signed in")
}

func TestSessions_StaleCookieDoesNotShadowAValidBearer(t *testing.T) {
	store := withGlobalSessions(t)
	alice := store.create("alice@example.com", "google:1")
	bob := store.create("bob@example.com", "google:2")
	withGrants(t, "google:bob@example.com", "")
	both := func(r *http.Request) {
		r.AddCookie(&http.Cookie{Name: "xray_session", Value: alice})
		r.Header.Set("Authorization", "Bearer "+bob)
	}
	assert.Equal(t, http.StatusOK, serveWith(t, "/v1/memory", both))
}

func TestSessions_GitHubSessionIsGatedByTheLists(t *testing.T) {
	store := withGlobalSessions(t)
	mallory := store.create("mallory", "github:42")
	cookie := func(r *http.Request) { r.AddCookie(&http.Cookie{Name: "xray_session", Value: mallory}) }
	withGrants(t, "google:bob@example.com", "")
	assert.Equal(t, http.StatusUnauthorized, serveWith(t, "/v1/memory", cookie), "unlisted GitHub id")
	withGrants(t, "github:42", "")
	assert.Equal(t, http.StatusOK, serveWith(t, "/v1/memory", cookie), "listed GitHub id")
}

func TestVoiceTools_AdminByAddressGetsTheWriteTool(t *testing.T) {
	withGrants(t, "", "google:carol@example.com")
	var ph atomic.Bool
	build := func(userID, username, userHash string) []string {
		return toolNames(buildVoiceTools(NewMemoryStore(), nil, "", username, userHash,
			voiceWriters(nil, userID, username, userHash), []string{}, nil, 0, zerolog.Nop(),
			func() []string { return nil }, &ph, &atomic.Bool{}, make(chan struct{})))
	}
	assert.Contains(t, build("google:3", "carol@example.com", "carol-hash"), "upsert_memory")
	assert.NotContains(t, build("google:1", "alice@example.com", "alice-hash"), "upsert_memory")
}

func TestPromptName_GoogleAddressKeepsOnlyTheLocalPart(t *testing.T) {
	assert.Equal(t, "carol", promptName("google:3", "carol@example.com"))
	assert.Equal(t, "mallory", promptName("github:42", "mallory"))
}

func TestNewAccessGrants_BlankListFailsClosed(t *testing.T) {
	for _, blank := range []string{",", " , ", " ,, "} {
		_, err := NewAccessGrants(blank, "", "example.com")
		require.Error(t, err, "users %q", blank)
		assert.Contains(t, err.Error(), "access users")
		_, err = NewAccessGrants("", blank, "example.com")
		require.Error(t, err, "admins %q", blank)
		assert.Contains(t, err.Error(), "access admins")
	}
	_, err := NewAccessGrants("", "  ", "example.com")
	assert.NoError(t, err, "an unset or whitespace-only value is an empty list")
}
