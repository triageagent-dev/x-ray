package xray

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
	"github.com/triageagent-dev/x-ray/v2/pkg/mesh"
	"github.com/triageagent-dev/x-ray/v2/pkg/mesh/meshtest"
	"github.com/triageagent-dev/x-ray/v2/pkg/meshsync"
)

// Two replicas over a real mutual-TLS mesh (ADR-068): what one counts, spends,
// issues or logs in, the other knows.

// joinedStore is a replica that has read its cell (which held no totals) and joined
// the mesh, as one does at start.
func joinedStore(mm *mesh.Mesh, self string) *MemoryStore {
	s := NewMemoryStore()
	s.JoinMesh(mm, self, zerolog.Nop())
	s.restoreMeshTotals("", time.Now())
	return s
}

func TestReplicas_TokenSpendHoldsAcrossReplicas(t *testing.T) {
	ma, mb := meshtest.Pair(t)
	a, b := joinedStore(ma, "a"), joinedStore(mb, "b")
	meshtest.Run(t, ma, mb)

	a.AddUserTokens("user-1", 100)
	require.Eventually(t, func() bool { return b.UserTokensToday("user-1") == 100 }, 3*time.Second, 10*time.Millisecond,
		"what a replica spent is spent for the person on every replica")

	b.AddUserTokens("user-1", 50)
	require.Eventually(t, func() bool { return a.UserTokensToday("user-1") == 150 }, 3*time.Second, 10*time.Millisecond)
	assert.EqualValues(t, 150, b.UserTokensToday("user-1"), "both replicas agree on the sum, so a cap is checked against it")

	// A resend of the same totals adds nothing.
	for _, r := range a.ledgerState(time.Now()) {
		r.Origin = "a"
		b.applyLedger([]meshsync.Record{r}, time.Now())
	}
	assert.EqualValues(t, 150, b.UserTokensToday("user-1"))
}

func TestReplicas_CountersAddUp(t *testing.T) {
	ma, mb := meshtest.Pair(t)
	a, b := joinedStore(ma, "a"), joinedStore(mb, "b")
	meshtest.Run(t, ma, mb)

	for range 3 {
		a.TrackKWVoiceHit()
	}
	for range 4 {
		b.TrackKWVoiceHit()
	}
	a.publishCounters()
	b.publishCounters()

	require.Eventually(t, func() bool {
		return a.Metrics().KWVoiceHits == 7 && b.Metrics().KWVoiceHits == 7
	}, 3*time.Second, 10*time.Millisecond, "each replica reports the fleet's count")
}

func TestReplicas_ARestartedReplicaDoesNotCountThePersistedTotalsTwice(t *testing.T) {
	ma, mb := meshtest.Pair(t)
	a, b := joinedStore(ma, "a"), joinedStore(mb, "b")
	meshtest.Run(t, ma, mb)
	a.AddUserTokens("u", 100)
	require.Eventually(t, func() bool { return b.UserTokensToday("u") == 100 }, 3*time.Second, 10*time.Millisecond)

	// b restarts: it takes the persisted totals back (100 in all, 100 of them a's)
	// and the sender totals they were written with, then hears a's total again.
	saved := b.meshTotals(time.Now())
	require.NotEmpty(t, saved)
	b2 := NewMemoryStore()
	b2.joinFleet("b", func([]meshsync.Record) {})
	b2.restoreMeshTotals(saved, time.Now())
	b2.RestoreStats(&mcell.RuntimeStats{UserDaily: []mcell.UserDailyTokens{{UserHash: "u", Day: utcDay(), Tokens: 100}}})
	for _, r := range a.ledgerState(time.Now()) {
		r.Origin = "a"
		b2.applyLedger([]meshsync.Record{r}, time.Now())
	}
	assert.EqualValues(t, 100, b2.UserTokensToday("u"), "a's total was already in the persisted count")

	a.AddUserTokens("u", 20)
	for _, r := range a.ledgerState(time.Now()) {
		r.Origin = "a"
		b2.applyLedger([]meshsync.Record{r}, time.Now())
	}
	assert.EqualValues(t, 120, b2.UserTokensToday("u"), "what a spent after the totals were saved is added")
}

func TestReplicas_OnlyTheLeaderWritesTheStats(t *testing.T) {
	s := NewMemoryStore()
	assert.True(t, s.leads(), "no election: the only replica")
	s.SetLeader(func() bool { return false })
	assert.False(t, s.leads())
	s.SetLeader(func() bool { return true })
	assert.True(t, s.leads())
}

func TestReplicas_ALoginOnOneReplicaIsKnownToTheOther(t *testing.T) {
	ma, mb := meshtest.Pair(t)
	a := NewMemoryStore()
	a.JoinMesh(ma, "a", zerolog.Nop())
	// b is a bare peer: it sees the frames a sends.
	var mu sync.Mutex
	var frames []sessionsFrame
	mb.Handle(frameSessions, func(_ string, data json.RawMessage) {
		var f sessionsFrame
		require.NoError(t, json.Unmarshal(data, &f))
		mu.Lock()
		frames = append(frames, f)
		mu.Unlock()
	})

	sessions.mu.Lock()
	savedKey := sessions.hmacKey
	sessions.mu.Unlock()
	t.Cleanup(func() {
		sessions.mu.Lock()
		sessions.hmacKey, sessions.onPublish, sessions.onDrop = savedKey, nil, nil
		sessions.entries = map[string]sessionEntry{}
		sessions.restored = map[string]sessionEntry{}
		sessions.mu.Unlock()
	})
	sessions.mu.Lock()
	sessions.hmacKey = []byte("shared-secret")
	sessions.mu.Unlock()
	a.JoinMesh(ma, "a", zerolog.Nop()) // sets the hooks on the session store
	meshtest.Run(t, ma, mb)

	token := sessions.create("alice", "github:1")

	var row mcell.UserSession
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, f := range frames {
			if len(f.Rows) == 1 {
				row = f.Rows[0]
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
	raw, _ := json.Marshal(row)
	assert.NotContains(t, string(raw), token, "the raw token never leaves the replica")
	assert.Equal(t, sessions.tokenHMAC(token), row.TokenHMAC)

	// What a peer sends is accepted: a session made elsewhere validates here.
	elsewhere := &sessionStore{entries: map[string]sessionEntry{}, restored: map[string]sessionEntry{}, hmacKey: []byte("shared-secret")}
	other := elsewhere.create("bob", "github:2")
	frame, _ := json.Marshal(sessionsFrame{Rows: elsewhere.snapshotSessions()})
	mb2 := mb // b sends it to a
	mb2.Broadcast(frameSessions, json.RawMessage(frame))
	require.Eventually(t, func() bool { _, _, ok := sessions.validate(other); return ok }, 3*time.Second, 10*time.Millisecond)

	// A logout elsewhere drops it here.
	mb.Broadcast(frameSessions, sessionsFrame{Drop: []string{elsewhere.tokenHMAC(other)}})
	require.Eventually(t, func() bool { _, _, ok := sessions.validate(other); return !ok }, 3*time.Second, 10*time.Millisecond)

	// And a logout here is announced.
	sessions.invalidate(token)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, f := range frames {
			if len(f.Drop) == 1 && f.Drop[0] == row.TokenHMAC {
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
}

func TestReplicas_ACodeIssuedOnOneReplicaIsRedeemedOnTheOther(t *testing.T) {
	ma, mb := meshtest.Pair(t)
	a, b := newTestAuthorizer(t, true, nil), newTestAuthorizer(t, true, nil)
	// One signing seed, as in production: a token minted on either validates on both.
	b.key, b.kid = a.key, a.kid
	a.JoinMesh(ma)
	b.JoinMesh(mb)
	meshtest.Run(t, ma, mb)

	verifier, challenge := pkcePair()
	a.putCode("code-1", authCode{challenge: challenge, clientID: "c", redirectURI: "http://127.0.0.1/cb", resource: testResource, sub: "github:1", scopes: []string{scopeRead}, expiresAt: time.Now().Add(time.Minute)})
	a.shareCode("code-1", a.codes["code-1"])

	form := url.Values{"grant_type": {"authorization_code"}, "code": {"code-1"}, "code_verifier": {verifier}, "client_id": {"c"}, "redirect_uri": {"http://127.0.0.1/cb"}}
	post := func(h *mcpAuthorizer) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		h.handleToken(w, req)
		return w
	}

	w := post(b)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var out map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
	_, _, ok := a.validateMCPToken(out["access_token"].(string))
	assert.True(t, ok, "the token b minted validates on a")

	// Redeemed once: the other replica forgets it, so it cannot be redeemed there too.
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		_, held := a.codes["code-1"]
		return !held
	}, 3*time.Second, 10*time.Millisecond)
	assert.Equal(t, http.StatusBadRequest, post(a).Code, "a code is single-use across replicas")
}

func TestReplicas_AnOAuthClientRegisteredOnOneReplicaIsKnownToTheOther(t *testing.T) {
	ma, mb := meshtest.Pair(t)
	a := NewMemoryStore()
	var mu sync.Mutex
	var got []mcell.OAuthClientReg
	mb.Handle(frameClients, func(_ string, data json.RawMessage) {
		var rows []mcell.OAuthClientReg
		require.NoError(t, json.Unmarshal(data, &rows))
		mu.Lock()
		got = append(got, rows...)
		mu.Unlock()
	})
	t.Cleanup(func() {
		oauthClients.mu.Lock()
		oauthClients.onAdd = nil
		delete(oauthClients.clients, "client-x")
		oauthClients.mu.Unlock()
	})
	a.JoinMesh(ma, "a", zerolog.Nop())
	meshtest.Run(t, ma, mb)

	oauthClients.add("client-x", oauthClient{redirectURIs: []string{"http://127.0.0.1/cb"}, issuedAt: time.Now()})

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range got {
			if r.ClientID == "client-x" {
				return assert.ObjectsAreEqual([]string{"http://127.0.0.1/cb"}, r.RedirectURIs)
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
}

func TestMergeWithPersisted_NeverWritesLessThanTheCellHolds(t *testing.T) {
	out := &mcell.RuntimeStats{SearchTotal: 5, KWVoiceHits: 9, UserDaily: []mcell.UserDailyTokens{{UserHash: "u1", Day: utcDay(), Tokens: 40}}}
	prev := &mcell.RuntimeStats{SearchTotal: 8, KWVoiceHits: 2, UserDaily: []mcell.UserDailyTokens{
		{UserHash: "u1", Day: utcDay(), Tokens: 70},
		{UserHash: "u2", Day: utcDay(), Tokens: 30},
		{UserHash: "u3", Day: "2001-01-01", Tokens: 99}, // another day: dropped
	}}

	mergeWithPersisted(out, prev)

	assert.EqualValues(t, 8, out.SearchTotal)
	assert.EqualValues(t, 9, out.KWVoiceHits)
	byUser := map[string]int64{}
	for _, u := range out.UserDaily {
		byUser[u.UserHash] = u.Tokens
	}
	assert.Equal(t, map[string]int64{"u1": 70, "u2": 30}, byUser)
}

func TestFlushStats_OnlyTheLeaderWritesAndItSavesTheSenderTotals(t *testing.T) {
	cell := &mcell.MemoryCell{}
	cell.Name, cell.Namespace = SystemConfigCRName, "ns"
	cell.Spec = mcell.MemoryCellSpec{ID: SystemConfigCRName, Kind: "system_configuration", Status: "active"}
	c := fake.NewClientBuilder().WithStatusSubresource(&mcell.MemoryCell{}).WithObjects(cell).WithScheme(testScheme(t)).Build()

	s := NewMemoryStore()
	s.setBackend(c, "ns")
	s.setWriter(c)
	s.joinFleet("b", func([]meshsync.Record) {})
	s.restoreMeshTotals("", time.Now())
	// a peer's total, applied here, is what the saved totals are about
	s.applyLedger([]meshsync.Record{{Family: famSpend, Name: "u", Start: dayStartOf(time.Now()), Epoch: 1, Vals: []int64{60}, Origin: "a"}}, time.Now())
	s.AddUserTokens("u", 10)

	read := func() *mcell.MemoryCell {
		got := &mcell.MemoryCell{}
		require.NoError(t, c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: SystemConfigCRName}, got))
		return got
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var lead atomic.Bool
	s.SetLeader(lead.Load)
	s.StartStatsFlusher(ctx, 10*time.Millisecond)
	s.markStatsDirty()
	time.Sleep(100 * time.Millisecond)
	assert.Nil(t, read().Status.Stats, "a follower does not write the stats")

	lead.Store(true)
	require.Eventually(t, func() bool { return read().Status.Stats != nil }, 2*time.Second, 10*time.Millisecond, "the leader writes them")
	got := read()
	require.Len(t, got.Status.Stats.UserDaily, 1)
	assert.EqualValues(t, 70, got.Status.Stats.UserDaily[0].Tokens, "the person's spend across replicas")
	assert.Contains(t, got.Spec.Data[meshTotalsKey].Bytes, `"o":"a"`, "and what was applied from the peer, so a restart does not count it again")
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sc := runtime.NewScheme()
	require.NoError(t, mcell.AddToScheme(sc))
	return sc
}

// A code is single-use across replicas: when the same code reaches two replicas at
// once, each holding a copy, the replica that issued it lets one of them redeem it.
func TestReplicas_ACodeRedeemedOnTwoReplicasAtOnceIssuesOneToken(t *testing.T) {
	ms := meshtest.Group(t, 3)
	auths := make([]*mcpAuthorizer, len(ms))
	for i, m := range ms {
		auths[i] = newTestAuthorizer(t, true, nil)
		if i > 0 {
			auths[i].key, auths[i].kid = auths[0].key, auths[0].kid
		}
		auths[i].JoinMesh(m)
	}
	meshtest.Run(t, ms...)
	// Wait until every replica can address the others by name.
	require.Eventually(t, func() bool {
		for _, m := range ms {
			if m.Connected() < len(ms)-1 {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	time.Sleep(100 * time.Millisecond) // the hellos that name the peers

	verifier, challenge := pkcePair()
	issued := authCode{challenge: challenge, clientID: "c", redirectURI: "http://127.0.0.1/cb", resource: testResource, sub: "github:1", scopes: []string{scopeRead}, expiresAt: time.Now().Add(time.Minute)}
	auths[0].putCode("code-1", issued)
	auths[0].shareCode("code-1", issued)

	form := url.Values{"grant_type": {"authorization_code"}, "code": {"code-1"}, "code_verifier": {verifier}, "client_id": {"c"}, "redirect_uri": {"http://127.0.0.1/cb"}}
	post := func(a *mcpAuthorizer) int {
		req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		a.handleToken(w, req)
		return w.Code
	}

	// The issuing replica and both others, all at once.
	codes := make([]int, len(auths))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range auths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			codes[i] = post(auths[i])
		}()
	}
	close(start)
	wg.Wait()

	ok := 0
	for _, c := range codes {
		if c == http.StatusOK {
			ok++
		}
	}
	assert.Equal(t, 1, ok, "one token for one code, whichever replicas it reached: %v", codes)
}

func TestReplicas_ACodeWhoseOwnerCannotBeAskedIsNotRedeemed(t *testing.T) {
	ma, mb := meshtest.Pair(t)
	a, b := newTestAuthorizer(t, true, nil), newTestAuthorizer(t, true, nil)
	b.key, b.kid = a.key, a.kid
	a.JoinMesh(ma)
	b.JoinMesh(mb)
	meshtest.Run(t, ma, mb)

	verifier, challenge := pkcePair()
	// b holds a copy of a code whose owner, "gone", is not connected.
	b.putCode("code-2", authCode{challenge: challenge, clientID: "c", redirectURI: "http://127.0.0.1/cb", resource: testResource, sub: "github:1", scopes: []string{scopeRead}, expiresAt: time.Now().Add(time.Minute), owner: "gone"})

	form := url.Values{"grant_type": {"authorization_code"}, "code": {"code-2"}, "code_verifier": {verifier}}
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	b.handleToken(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code, "no token when the replica that decides cannot be asked")
	assert.Contains(t, w.Body.String(), "temporarily_unavailable")
}

// A peer that holds the mesh certificate but not the signing secret cannot plant a
// code: it would be exchanged for a token for whatever subject and scopes it names.
func TestReplicas_AForgedAuthorizationCodeIsDropped(t *testing.T) {
	ma, mb := meshtest.Pair(t)
	a := newTestAuthorizer(t, true, nil)
	a.JoinMesh(ma)
	meshtest.Run(t, ma, mb)

	forged := codeFrame{Code: "evil", Challenge: "c", ClientID: "c", RedirectURI: "http://127.0.0.1/cb", Resource: testResource, Sub: "github:1", Scopes: []string{scopeWrite}, ExpiresAt: time.Now().Add(time.Minute).Unix(), MAC: "00"}
	mb.Broadcast(frameCode, forged)
	// A frame with a MAC made under another key is no better.
	other := newTestAuthorizer(t, true, nil)
	forged.Code = "evil2"
	forged.MAC = other.codeMAC(forged)
	mb.Broadcast(frameCode, forged)

	time.Sleep(300 * time.Millisecond)
	a.mu.Lock()
	defer a.mu.Unlock()
	assert.NotContains(t, a.codes, "evil")
	assert.NotContains(t, a.codes, "evil2")
}

// googleReplica is a Google sign-in on a replica that joined mesh mm as self,
// against the token endpoint at tokenURL.
func googleReplica(mm *mesh.Mesh, self, tokenURL string) *googleAuth {
	g := newTestGoogleAuth(tokenURL)
	joinLoginMesh(mm, self, zerolog.Nop(), g, newGitHubAuth(GitHubOAuthConfig{}), newAzureAuth(AzureOAuthConfig{}))
	return g
}

func googleStart(t *testing.T, g *googleAuth) (string, []*http.Cookie) {
	t.Helper()
	w := httptest.NewRecorder()
	g.handleLogin(w, httptest.NewRequest(http.MethodGet, "/v1/auth/google", nil))
	loc, err := url.Parse(w.Header().Get("Location"))
	require.NoError(t, err)
	return loc.Query().Get("state"), w.Result().Cookies()
}

func TestReplicas_ASignInStartedOnOneReplicaCompletesOnTheOther(t *testing.T) {
	withSessionKey(t, "shared-secret")
	tokenSrv := httptest.NewServer(tokenReply(t, googleTestToken(t, workspaceClaims())))
	t.Cleanup(tokenSrv.Close)
	ma, mb := meshtest.Pair(t)
	a, b := googleReplica(ma, "a", tokenSrv.URL), googleReplica(mb, "b", tokenSrv.URL)
	meshtest.Run(t, ma, mb)

	state, cookies := googleStart(t, a)
	assert.True(t, strings.HasPrefix(state, "a_"), "the state names the replica that started the sign-in")

	w := googleReturn(b, state, cookies)
	require.Equal(t, http.StatusFound, w.Code, w.Body.String())
	assert.NotNil(t, sessionCookie(w))

	again := googleReturn(b, state, cookies)
	assert.Equal(t, http.StatusBadRequest, again.Code, "a sign-in completes once across replicas")
	assert.Nil(t, sessionCookie(again))
}

func TestReplicas_ASignInWhoseOwnerCannotBeAskedIsRefused(t *testing.T) {
	withSessionKey(t, "shared-secret")
	tokenSrv := httptest.NewServer(tokenReply(t, googleTestToken(t, workspaceClaims())))
	t.Cleanup(tokenSrv.Close)
	ma, mb := meshtest.Pair(t)
	googleReplica(ma, "a", tokenSrv.URL)
	b := googleReplica(mb, "b", tokenSrv.URL)
	meshtest.Run(t, ma, mb)

	// Started on "gone", which b is not connected to.
	gone := newTestGoogleAuth(tokenSrv.URL)
	gone.self = "gone"
	state, cookies := googleStart(t, gone)

	w := googleReturn(b, state, cookies)
	assert.Equal(t, http.StatusBadRequest, w.Code, "the person signs in again")
	assert.Nil(t, sessionCookie(w))
}
