package xray

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withGlobalSessions swaps the package-level session store for the duration of
// a test and restores it afterwards, so carrier tests can mint real tokens
// without leaking entries into other tests in the package.
func withGlobalSessions(t *testing.T) *sessionStore {
	t.Helper()
	prev := sessions
	s := newTestSessionStore()
	sessions = s
	t.Cleanup(func() { sessions = prev })
	return s
}

func upgradeReq(protocols ...string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/voice", nil)
	r.Header.Set("Upgrade", "websocket")
	for _, p := range protocols {
		r.Header.Add("Sec-WebSocket-Protocol", p)
	}
	return r
}

// ── wsBearerToken ─────────────────────────────────────────────────────────────

func TestWSBearerToken(t *testing.T) {
	tests := []struct {
		name      string
		req       *http.Request
		wantToken string
	}{
		{"browser form", upgradeReq("bearer, tok123"), "tok123"},
		{"no space after comma", upgradeReq("bearer,tok123"), "tok123"},
		{"case-insensitive sentinel", upgradeReq("Bearer, tok123"), "tok123"},
		{"split across header lines", upgradeReq("bearer", "tok123"), ""},
		{"sentinel with no token after it", upgradeReq("bearer"), ""},
		{"empty token after sentinel", upgradeReq("bearer, "), ""},
		{"unrelated subprotocol", upgradeReq("graphql-ws"), ""},
		{"no subprotocol at all", upgradeReq(), ""},
		{"extra protocol before the sentinel", upgradeReq("v1.xray, bearer, tok123"), "tok123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantToken, wsBearerToken(tc.req))
		})
	}
}

// The header is only meaningful on an upgrade. Honouring it on an ordinary
// request would quietly add a second, far less visible way to authenticate
// every route in the server.
func TestWSBearerToken_IgnoredOnNonUpgradeRequest(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/v1/memory", nil)
	r.Header.Set("Sec-WebSocket-Protocol", "bearer, tok123")
	assert.Empty(t, wsBearerToken(r))
}

// ── sessionFromRequest ────────────────────────────────────────────────────────

func TestSessionFromRequest_Carriers(t *testing.T) {
	store := withGlobalSessions(t)
	token := store.create("alice", "github:1")

	t.Run("cookie", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/memory", nil)
		r.AddCookie(&http.Cookie{Name: "xray_session", Value: token})
		user, id, ok := sessionFromRequest(r)
		require.True(t, ok)
		assert.Equal(t, "alice", user)
		assert.Equal(t, "github:1", id)
	})

	t.Run("bearer header", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/memory", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		user, _, ok := sessionFromRequest(r)
		require.True(t, ok)
		assert.Equal(t, "alice", user)
	})

	t.Run("websocket subprotocol", func(t *testing.T) {
		user, id, ok := sessionFromRequest(upgradeReq("bearer, " + token))
		require.True(t, ok)
		assert.Equal(t, "alice", user)
		assert.Equal(t, "github:1", id)
	})

	t.Run("no carrier", func(t *testing.T) {
		_, _, ok := sessionFromRequest(httptest.NewRequest(http.MethodGet, "/v1/memory", nil))
		assert.False(t, ok)
	})

	t.Run("invalid token in every carrier", func(t *testing.T) {
		r := upgradeReq("bearer, not-a-token")
		r.AddCookie(&http.Cookie{Name: "xray_session", Value: "nope"})
		r.Header.Set("Authorization", "Bearer also-nope")
		_, _, ok := sessionFromRequest(r)
		assert.False(t, ok)
	})
}

// A stale cookie must not shadow a good bearer token: a UI on another origin
// can easily hold both — a dead cookie from an earlier same-origin visit and a
// live handed-off token — and stopping at the first carrier would log it out.
func TestSessionFromRequest_FallsThroughExpiredCookie(t *testing.T) {
	store := withGlobalSessions(t)
	token := store.create("alice", "github:1")

	r := upgradeReq("bearer, " + token)
	r.AddCookie(&http.Cookie{Name: "xray_session", Value: "expired-or-revoked"})

	user, _, ok := sessionFromRequest(r)
	require.True(t, ok)
	assert.Equal(t, "alice", user)
}

// ── resolveUIOrigin ───────────────────────────────────────────────────────────

func TestResolveUIOrigin(t *testing.T) {
	allowed := []string{"https://ui.example.com", "http://localhost:8080"}

	tests := []struct {
		name      string
		requested string
		allowed   []string
		want      string
	}{
		{"exact match", "https://ui.example.com", allowed, "https://ui.example.com"},
		{"second entry", "http://localhost:8080", allowed, "http://localhost:8080"},
		{"path is stripped, origin still matches", "https://ui.example.com/anything", allowed, "https://ui.example.com"},
		{"case-insensitive host", "https://UI.Example.com", allowed, "https://ui.example.com"},
		{"unlisted host rejected", "https://evil.example.com", allowed, ""},
		{"scheme must match", "http://ui.example.com", allowed, ""},
		{"port must match", "http://localhost:3000", allowed, ""},
		{"subdomain is not a match", "https://sub.ui.example.com", allowed, ""},
		{"empty allowlist disables the feature", "https://ui.example.com", nil, ""},
		{"empty request", "", allowed, ""},
		{"relative URL has no origin", "/auth/callback", allowed, ""},
		{"garbage", "://", allowed, ""},
		{"userinfo does not smuggle a host past the check", "https://ui.example.com@evil.example.com", allowed, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, resolveUIOrigin(tc.requested, tc.allowed))
		})
	}
}

func TestUICallbackRedirect(t *testing.T) {
	// No hand-off requested: the same-origin default, where the cookie just set
	// is the whole credential and no token should appear in a URL at all.
	assert.Equal(t, "/", uiCallbackRedirect("", "tok123", ""))
	assert.Equal(t, "/path/", uiCallbackRedirect("", "tok123", "/path"))

	assert.Equal(t,
		"https://ui.example.com/auth/callback?token=tok123",
		uiCallbackRedirect("https://ui.example.com", "tok123", ""))

	// Tokens are hex today, but the redirect must not depend on that.
	assert.Equal(t,
		"https://ui.example.com/auth/callback?token=a%2Fb+c%26d",
		uiCallbackRedirect("https://ui.example.com", "a/b c&d", ""))
}

// ── requireSession gate ───────────────────────────────────────────────────────

// The carrier is only useful if the gate honours it: /v1/voice sits behind
// requireSession, so a subprotocol-credentialled upgrade that the gate rejects
// never reaches the voice handler at all.
func TestRequireSession_AcceptsWebSocketSubprotocolCarrier(t *testing.T) {
	store := withGlobalSessions(t)
	token := store.create("alice", "github:1")

	// A ClientID is what switches the gate on; newTestServer leaves it empty,
	// which disables auth entirely and would pass every case vacuously.
	s := NewServer(zerolog.Nop(), NewMemoryStore(), nil, GitHubOAuthConfig{ClientID: "gh"}, AzureOAuthConfig{}, "", 0, nil)

	var reached bool
	h := s.requireSession(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true }))

	t.Run("valid token in subprotocol passes", func(t *testing.T) {
		reached = false
		w := httptest.NewRecorder()
		h.ServeHTTP(w, upgradeReq("bearer, "+token))
		assert.True(t, reached)
	})

	t.Run("invalid token in subprotocol is rejected", func(t *testing.T) {
		reached = false
		w := httptest.NewRecorder()
		h.ServeHTTP(w, upgradeReq("bearer, forged"))
		assert.False(t, reached)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	t.Run("no credential is rejected", func(t *testing.T) {
		reached = false
		w := httptest.NewRecorder()
		h.ServeHTTP(w, upgradeReq())
		assert.False(t, reached)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})

	// The same token presented on an ordinary request must NOT authenticate:
	// wsBearerToken deliberately reads the header only on an upgrade.
	t.Run("subprotocol header on a plain request does not authenticate", func(t *testing.T) {
		reached = false
		r := httptest.NewRequest(http.MethodGet, "/v1/memory", nil)
		r.Header.Set("Sec-WebSocket-Protocol", "bearer, "+token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		assert.False(t, reached)
		assert.Equal(t, http.StatusUnauthorized, w.Code)
	})
}

// ── OAuth login hand-off ──────────────────────────────────────────────────────

// The default deployment must be untouched by the hand-off: with no allowlist
// configured, a login carrying ?ui= still lands on this server's own "/" and
// no token is ever put in a URL.
func TestGitHubLogin_UIHandoffOffByDefault(t *testing.T) {
	g := newGitHubAuth(GitHubOAuthConfig{ClientID: "gh", PublicURL: "https://xray.example.com"})

	r := httptest.NewRequest(http.MethodGet, "/v1/auth/github?ui=https://ui.example.com", nil)
	g.handleLogin(httptest.NewRecorder(), r)

	g.statesMu.Lock()
	defer g.statesMu.Unlock()
	require.Len(t, g.states, 1)
	for _, st := range g.states {
		assert.Empty(t, st.uiOrigin, "an unconfigured allowlist must not bind any UI origin to the login")
	}
}

func TestGitHubLogin_UIHandoffBindsAllowlistedOrigin(t *testing.T) {
	g := newGitHubAuth(GitHubOAuthConfig{
		ClientID:          "gh",
		PublicURL:         "https://xray.example.com",
		UICallbackOrigins: []string{"https://ui.example.com"},
	})

	t.Run("allowlisted origin is bound", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/auth/github?ui=https://ui.example.com", nil)
		g.handleLogin(httptest.NewRecorder(), r)
		g.statesMu.Lock()
		defer g.statesMu.Unlock()
		var found bool
		for _, st := range g.states {
			if st.uiOrigin == "https://ui.example.com" {
				found = true
			}
		}
		assert.True(t, found)
	})

	// The rejection happens at login, not at the callback: an unapproved origin
	// is never bound to a live login in the first place, so there is no window
	// in which a minted token could be sent to it.
	t.Run("unlisted origin is dropped at login", func(t *testing.T) {
		g2 := newGitHubAuth(GitHubOAuthConfig{
			ClientID:          "gh",
			UICallbackOrigins: []string{"https://ui.example.com"},
		})
		r := httptest.NewRequest(http.MethodGet, "/v1/auth/github?ui=https://evil.example.com", nil)
		g2.handleLogin(httptest.NewRecorder(), r)
		g2.statesMu.Lock()
		defer g2.statesMu.Unlock()
		require.Len(t, g2.states, 1)
		for _, st := range g2.states {
			assert.Empty(t, st.uiOrigin)
		}
	})
}

// ── WebSocket handshake ───────────────────────────────────────────────────────

// End-to-end handshake over a real gorilla client: a browser that offers
// `bearer, <token>` must get a 101 back with `bearer` selected. Without the
// echo the socket still opens, but ws.protocol is "" and a client has no way
// to distinguish a server that read the credential from one that ignored it.
func TestVoiceUpgrade_EchoesCredentialSubprotocol(t *testing.T) {
	store := withGlobalSessions(t)
	token := store.create("alice", "github:1")

	// Mirrors the upgrade path in VoiceManager.ServeHTTP; the manager itself
	// needs a live genai client, which a unit test has no business starting.
	upgrader := newWSUpgrader("")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, _, ok := sessionFromRequest(r)
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var hdr http.Header
		if wsBearerToken(r) != "" {
			hdr = http.Header{"Sec-WebSocket-Protocol": []string{wsBearerCredentialProtocol}}
		}
		conn, err := upgrader.Upgrade(w, r, hdr)
		if err != nil {
			return
		}
		defer conn.Close() //nolint:errcheck
		_ = conn.WriteMessage(websocket.TextMessage, []byte(user))
	}))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	t.Run("credential accepted and subprotocol echoed", func(t *testing.T) {
		d := websocket.Dialer{Subprotocols: []string{"bearer", token}}
		conn, resp, err := d.Dial(wsURL, nil)
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
		assert.Equal(t, wsBearerCredentialProtocol, conn.Subprotocol())

		_, msg, err := conn.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, "alice", string(msg), "the session behind the socket is the token's user")
	})

	t.Run("no subprotocol echoed when the cookie carried the session", func(t *testing.T) {
		h := http.Header{"Cookie": []string{"xray_session=" + token}}
		d := websocket.Dialer{}
		conn, resp, err := d.Dial(wsURL, h)
		require.NoError(t, err)
		defer conn.Close() //nolint:errcheck

		assert.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
		assert.Empty(t, conn.Subprotocol())
	})

	t.Run("forged token is refused before the upgrade", func(t *testing.T) {
		d := websocket.Dialer{Subprotocols: []string{"bearer", "forged"}}
		_, resp, err := d.Dial(wsURL, nil)
		require.Error(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
}
