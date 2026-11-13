package xray

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// Member access (ADR 067): with --mcp-member-access a signed-in non-admin gets a
// token that reads shared knowledge and keeps their own private notes.

const (
	memberSubA = "github:alice"
	memberSubB = "github:bob"
)

var (
	memberA = stableUserID(memberSubA)
	memberB = stableUserID(memberSubB)
)

func newMemberAuthorizer(t *testing.T, on bool) *mcpAuthorizer {
	t.Helper()
	a := newTestAuthorizer(t, false, []string{stableUserID("github:admin")})
	a.SetMemberAccess(on)
	return a
}

// authorizeAs drives /authorize for a registered DCR client with a session of sub.
func memberAuthorize(t *testing.T, a *mcpAuthorizer, name, sub string) *httptest.ResponseRecorder {
	t.Helper()
	clientID := registerClient(t, a, "http://localhost:9999/cb")
	sessTok := sessions.create(name, sub)
	t.Cleanup(func() { sessions.invalidate(sessTok) })
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

func codeScopes(t *testing.T, a *mcpAuthorizer, rec *httptest.ResponseRecorder) []string {
	t.Helper()
	require.Equal(t, http.StatusFound, rec.Code)
	loc, err := url.Parse(rec.Header().Get("Location"))
	require.NoError(t, err)
	code := loc.Query().Get("code")
	require.NotEmpty(t, code, "a code is issued: %s", loc)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.codes[code].scopes
}

func metadataScopes(t *testing.T, a *mcpAuthorizer) (resource, server []string) {
	t.Helper()
	read := func(h http.HandlerFunc) []string {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		var m struct {
			Scopes []string `json:"scopes_supported"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
		return m.Scopes
	}
	return read(a.protectedResourceMetadata), read(a.authServerMetadata)
}

func challengeScope(t *testing.T, a *mcpAuthorizer) string {
	t.Helper()
	rec := httptest.NewRecorder()
	challenge(rec, a)
	wa := rec.Header().Get("WWW-Authenticate")
	i := strings.Index(wa, `scope="`)
	require.GreaterOrEqual(t, i, 0)
	return strings.TrimSuffix(wa[i+len(`scope="`):], `"`)
}

// 1. Flag off: nothing changes.
func TestMCPMember_FlagOffDeniesNonAdminAndKeepsMetadata(t *testing.T) {
	a := newMemberAuthorizer(t, false)
	rec := memberAuthorize(t, a, "bob", memberSubB)
	require.Equal(t, http.StatusFound, rec.Code)
	loc, _ := url.Parse(rec.Header().Get("Location"))
	assert.Equal(t, "access_denied", loc.Query().Get("error"))
	assert.Empty(t, loc.Query().Get("code"))

	res, srv := metadataScopes(t, a)
	assert.Equal(t, []string{scopeRead, scopeWrite}, res)
	assert.Equal(t, []string{scopeRead, scopeWrite}, srv)
	assert.Equal(t, "xray:memory.read xray:memory.write", challengeScope(t, a))
}

// 2. Flag on: a member gets read + write.own, an admin keeps read + write.
func TestMCPMember_FlagOnIssuesMemberScopes(t *testing.T) {
	a := newMemberAuthorizer(t, true)
	assert.ElementsMatch(t, []string{scopeRead, scopeWriteOwn}, codeScopes(t, a, memberAuthorize(t, a, "bob", memberSubB)))
	assert.ElementsMatch(t, []string{scopeRead, scopeWrite}, codeScopes(t, a, memberAuthorize(t, a, "root", "github:admin")))

	res, srv := metadataScopes(t, a)
	assert.Contains(t, res, scopeWriteOwn)
	assert.Contains(t, srv, scopeWriteOwn)
	assert.Contains(t, challengeScope(t, a), scopeWriteOwn)
}

// ── MCP handler fixtures ──────────────────────────────────────────────────────

type memberRig struct {
	store *MemoryStore
	srv   *httptest.Server
	a     *mcpAuthorizer
	tok   string // alice, member
	sid   string
	admin string
	asid  string
}

// callSeq gives every call its own client address, so the handler's per-IP
// throttle (20 burst) does not trip on a test that makes many calls.
var callSeq atomic.Int64

func newMemberRig(t *testing.T) *memberRig {
	t.Helper()
	SetTrustedProxyHops(1)
	t.Cleanup(func() { SetTrustedProxyHops(0) })
	a := newMemberAuthorizer(t, true)
	store := newTestStore()
	srv := httptest.NewServer(NewMCPServer(store, zerolog.Nop(), func() string { return testMCPToken }, a, nil, "", nil, 0, 0, 0))
	t.Cleanup(srv.Close)
	r := &memberRig{store: store, srv: srv, a: a}
	var err error
	r.tok, err = a.signToken(memberSubA, []string{scopeRead, scopeWriteOwn})
	require.NoError(t, err)
	r.admin, err = a.signToken("github:admin", []string{scopeRead, scopeWrite})
	require.NoError(t, err)
	st, sid := mcpInitWith(t, srv, r.tok)
	require.Equal(t, http.StatusOK, st)
	r.sid = sid
	st, sid = mcpInitWith(t, srv, r.admin)
	require.Equal(t, http.StatusOK, st)
	r.asid = sid
	return r
}

func (r *memberRig) call(t *testing.T, bearer, sid, tool string, args map[string]any) (int, map[string]any) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]any{"name": tool, "arguments": args},
	})
	req, _ := http.NewRequest(http.MethodPost, r.srv.URL+"/mcp", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Mcp-Session-Id", sid)
	req.Header.Set("X-Forwarded-For", "10.9."+strconv.FormatInt(callSeq.Add(1)/250, 10)+"."+strconv.FormatInt(callSeq.Load()%250, 10))
	resp, err := r.srv.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	if resp.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	}
	return resp.StatusCode, out
}

// member calls a tool as alice and returns the decoded JSON text of the result.
func (r *memberRig) member(t *testing.T, tool string, args map[string]any) map[string]any {
	t.Helper()
	st, resp := r.call(t, r.tok, r.sid, tool, args)
	require.Equal(t, http.StatusOK, st)
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(mcpResultText(t, resp)), &out))
	return out
}

func (r *memberRig) put(t *testing.T, scope, kind, title, owner string) string {
	t.Helper()
	res, msg := r.store.Upsert(context.Background(), "", kind, scope, title, "body of "+title, owner, "", "", nil, nil)
	require.Empty(t, msg)
	return res.ID
}

func titles(out map[string]any) []string {
	items, _ := out["items"].([]any)
	var ts []string
	for _, it := range items {
		m, _ := it.(map[string]any)
		ts = append(ts, m["title"].(string))
	}
	return ts
}

// 3. Member reads never return another person's private cells.
func TestMCPMember_ReadsAreOwnerBound(t *testing.T) {
	r := newMemberRig(t)
	r.put(t, scopePublic, "stable_fact", "zebra shared", "")
	r.put(t, scopePrivate, "note", "zebra alice", memberA)
	bobID := r.put(t, scopePrivate, "note", "zebra bob", memberB)
	r.put(t, scopePrivate, "episodic", "zebra alice episode", memberA)
	r.put(t, scopePrivate, "episodic", "zebra bob episode", memberB)
	r.put(t, scopePrivate, "note", "bob extra", memberB)
	legacy := r.put(t, scopePrivate, "note", "legacy", "")

	for _, tc := range []struct {
		tool string
		args func(subject string) map[string]any
	}{
		{"list_memory", func(s string) map[string]any { return map[string]any{"subject": s} }},
		{"search_memory", func(s string) map[string]any { return map[string]any{"query": "zebra", "subject": s} }},
		{"recent_memory", func(s string) map[string]any { return map[string]any{"subject": s} }},
		{"fetch_memory", func(s string) map[string]any { return map[string]any{"ids": []any{bobID}, "subject": s} }},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			// the other person's hash: never their cells
			got := titles(r.member(t, tc.tool, tc.args(memberB)))
			assert.NotContains(t, got, "zebra bob")
			assert.NotContains(t, got, "zebra bob episode")
			// any other non-empty subject: replaced by the member's own hash
			got = titles(r.member(t, tc.tool, tc.args("whatever")))
			assert.NotContains(t, got, "zebra bob")
			assert.NotContains(t, got, "zebra bob episode")
			// empty subject: shared only, no private cell at all
			got = titles(r.member(t, tc.tool, tc.args("")))
			assert.NotContains(t, got, "zebra alice")
			assert.NotContains(t, got, "zebra alice episode")
			assert.NotContains(t, got, "zebra bob")
		})
	}

	// the replacement reaches the member's own cells
	assert.Contains(t, titles(r.member(t, "list_memory", map[string]any{"subject": "x"})), "zebra alice")
	assert.Contains(t, titles(r.member(t, "search_memory", map[string]any{"query": "zebra", "subject": "x"})), "zebra alice")
	assert.Contains(t, titles(r.member(t, "recent_memory", map[string]any{"subject": "x"})), "zebra alice episode")
	// and shared knowledge stays readable with an empty subject
	assert.Contains(t, titles(r.member(t, "list_memory", map[string]any{})), "zebra shared")
	assert.Contains(t, titles(r.member(t, "search_memory", map[string]any{"query": "zebra"})), "zebra shared")
	// fetch by id: own private by any non-empty subject, never another's
	aliceID := r.put(t, scopePrivate, "note", "mine", memberA)
	assert.EqualValues(t, 1, r.member(t, "fetch_memory", map[string]any{"ids": []any{aliceID}, "subject": "x"})["count"])
	assert.EqualValues(t, 0, r.member(t, "fetch_memory", map[string]any{"ids": []any{aliceID}})["count"])
	// an ownerless legacy private cell is never fetched
	require.NotNil(t, r.store.GetItem(legacy))
	assert.EqualValues(t, 0, r.member(t, "fetch_memory", map[string]any{"ids": []any{legacy}})["count"])
	assert.EqualValues(t, 0, r.member(t, "fetch_memory", map[string]any{"ids": []any{legacy}, "subject": "x"})["count"])
	// get_memory counts the shared cells and the member's own, not another's
	out := r.member(t, "get_memory", map[string]any{})
	assert.EqualValues(t, 4, out["total"], "1 shared + alice's 2 notes and 1 episode")
	assert.EqualValues(t, 2, out["by_kind"].(map[string]any)["note"], "alice's two notes, not bob's two")
	assert.EqualValues(t, 1, out["by_kind"].(map[string]any)["stable_fact"])
}

// 4. Member upsert: private only, owner forced, visible to the voice recall path.
func TestMCPMember_UpsertIsPrivateAndOwned(t *testing.T) {
	r := newMemberRig(t)
	out := r.member(t, "upsert_memory", map[string]any{"kind": "note", "scope": "public", "title": "shout", "content": "x"})
	assert.Equal(t, "members can write private notes only", out["error"])
	assert.Empty(t, r.store.List("", scopePublic, "", 0), "nothing shared was written")

	out = r.member(t, "upsert_memory", map[string]any{"kind": "note", "title": "my note", "content": "hello"})
	require.NotContains(t, out, "error")
	id, _ := out["id"].(string)
	it := r.store.GetItem(id)
	require.NotNil(t, it)
	assert.Equal(t, scopePrivate, it.Scope)
	assert.Equal(t, memberA, it.Subject, "owner is the member's hash")

	// a claimed subject is overridden
	out = r.member(t, "upsert_memory", map[string]any{"kind": "note", "scope": "private", "subject": memberB, "title": "forged", "content": "y"})
	require.NotContains(t, out, "error")
	assert.Equal(t, memberA, r.store.GetItem(out["id"].(string)).Subject)

	// the voice recall path for that user sees it, another user's does not
	var voice []string
	for _, n := range r.store.ListRecent("note", scopePrivate, memberA, 10) {
		voice = append(voice, n.Title)
	}
	assert.Contains(t, voice, "my note")
	assert.Empty(t, r.store.ListRecent("note", scopePrivate, memberB, 10))
}

// 5. delete / expire / refute / supersede touch only the member's own private cells.
func TestMCPMember_ChangesOnlyOwnPrivateCells(t *testing.T) {
	type op struct {
		tool string
		args func(id string) map[string]any
		done func(t *testing.T, r *memberRig, id string, out map[string]any)
	}
	ops := []op{
		{"delete_memory", func(id string) map[string]any { return map[string]any{"id": id} },
			func(t *testing.T, r *memberRig, id string, _ map[string]any) { assert.Nil(t, r.store.GetItem(id)) }},
		{"expire_memory", func(id string) map[string]any { return map[string]any{"id": id} },
			func(t *testing.T, r *memberRig, id string, _ map[string]any) {
				assert.Equal(t, "stale", r.store.GetItem(id).Status)
			}},
		{"refute_memory", func(id string) map[string]any { return map[string]any{"id": id, "reason": "wrong"} },
			func(t *testing.T, r *memberRig, id string, _ map[string]any) {
				assert.Equal(t, "stale", r.store.GetItem(id).Status)
			}},
		// scope left out and a forged subject: the replacement is forced private and owned
		{"supersede_memory", func(id string) map[string]any {
			return map[string]any{"old_id": id, "kind": "note", "title": "new", "content": "n", "subject": memberB}
		}, func(t *testing.T, r *memberRig, id string, out map[string]any) {
			assert.Equal(t, "superseded", r.store.GetItem(id).Status)
			repl := r.store.GetItem(out["id"].(string))
			assert.Equal(t, scopePrivate, repl.Scope)
			assert.Equal(t, memberA, repl.Subject)
		}},
	}
	for _, o := range ops {
		t.Run(o.tool, func(t *testing.T) {
			r := newMemberRig(t)
			shared := r.put(t, scopePublic, "stable_fact", "shared fact", "")
			bobs := r.put(t, scopePrivate, "note", "bob note", memberB)
			mine := r.put(t, scopePrivate, "note", "my note", memberA)

			out := r.member(t, o.tool, o.args(shared))
			assert.Equal(t, "forbidden", out["error"], "a shared target")
			assert.Equal(t, "active", r.store.GetItem(shared).Status)

			out = r.member(t, o.tool, o.args(bobs))
			assert.NotEmpty(t, out["error"], "another user's private cell")
			assert.Equal(t, "active", r.store.GetItem(bobs).Status)
			assert.NotNil(t, r.store.GetItem(bobs))

			out = r.member(t, o.tool, o.args(mine))
			assert.NotContains(t, out, "error", "own private cell")
			o.done(t, r, mine, out)
		})
	}

	t.Run("supersede_memory refusals", func(t *testing.T) {
		r := newMemberRig(t)
		mine := r.put(t, scopePrivate, "note", "my note", memberA)
		shared := r.put(t, scopePublic, "stable_fact", "shared fact", "")
		out := r.member(t, "supersede_memory", map[string]any{"old_id": mine, "kind": "note", "scope": "public", "title": "n", "content": "n"})
		assert.Equal(t, "members can write private notes only", out["error"])
		out = r.member(t, "supersede_memory", map[string]any{"old_id": mine, "new_id": shared, "kind": "note", "scope": "private", "title": "n", "content": "n"})
		assert.NotEmpty(t, out["error"], "new_id may not name a shared cell")
		assert.Equal(t, scopePublic, r.store.GetItem(shared).Scope)
	})
}

// consolidate_memory writes shared cells: admin only. A member token without
// write.own (read only) cannot call a write tool either.
func TestMCPMember_WriteToolsAreScopeGated(t *testing.T) {
	r := newMemberRig(t)
	st, _ := r.call(t, r.tok, r.sid, "consolidate_memory", map[string]any{})
	assert.Equal(t, http.StatusForbidden, st)
	st, _ = r.call(t, r.admin, r.asid, "consolidate_memory", map[string]any{})
	assert.Equal(t, http.StatusOK, st)
	tok, err := r.a.signToken(memberSubA, []string{scopeRead})
	require.NoError(t, err)
	_, sid := mcpInitWith(t, r.srv, tok)
	st, _ = r.call(t, tok, sid, "upsert_memory", map[string]any{"kind": "note", "title": "t", "content": "c"})
	assert.Equal(t, http.StatusForbidden, st)
}

// 7. get_memory_context returns shared cells only, for both roles.
func TestMCPMember_ContextNeverReturnsPrivateCells(t *testing.T) {
	r := newMemberRig(t)
	anchor := r.put(t, scopePublic, "stable_fact", "quokka anchor", "")
	priv := r.put(t, scopePrivate, "note", "quokka secret", memberA)
	other := r.put(t, scopePublic, "stable_fact", "quokka neighbour", "")
	require.True(t, r.store.SetLinks(anchor, []mcell.ThoughtLink{{ToID: priv, Relation: "explains", Weight: 1}, {ToID: other, Relation: "explains", Weight: 1}}))
	require.True(t, r.store.SetLinks(priv, []mcell.ThoughtLink{{ToID: anchor, Relation: "explains", Weight: 1}}))

	for name, who := range map[string][2]string{"member": {r.tok, r.sid}, "admin": {r.admin, r.asid}} {
		t.Run(name, func(t *testing.T) {
			for _, a := range []string{"quokka anchor", anchor, "quokka secret", priv} {
				st, resp := r.call(t, who[0], who[1], "get_memory_context", map[string]any{"anchor": a})
				require.Equal(t, http.StatusOK, st)
				var out struct {
					Items []struct{ ID, Title string } `json:"items"`
				}
				require.NoError(t, json.Unmarshal([]byte(mcpResultText(t, resp)), &out))
				for _, it := range out.Items {
					assert.NotEqual(t, priv, it.ID, "anchor %q", a)
					assert.NotEqual(t, "quokka secret", it.Title, "anchor %q", a)
				}
			}
		})
	}
}

// The admin token is not owner-bound: it reads any subject, as today.
func TestMCPMember_AdminStaysUnbound(t *testing.T) {
	r := newMemberRig(t)
	r.put(t, scopePrivate, "note", "zebra bob", memberB)
	st, resp := r.call(t, r.admin, r.asid, "list_memory", map[string]any{"subject": memberB})
	require.Equal(t, http.StatusOK, st)
	assert.Contains(t, mcpResultText(t, resp), "zebra bob")
}

// 9. End to end through the HTTP handlers: /authorize -> /token -> /mcp.
func TestMCPMember_EndToEnd(t *testing.T) {
	a := newTestAuthorizer(t, false, []string{stableUserID("github:admin")})
	a.SetMemberAccess(true)
	require.NoError(t, a.SetStaticClient("connector-1", []string{claudeCallback}))
	store := newTestStore()
	srv := httptest.NewServer(NewMCPServer(store, zerolog.Nop(), func() string { return testMCPToken }, a, nil, "", nil, 0, 0, 0))
	defer srv.Close()
	upsertItem(t, store, "", "note", scopePrivate, "zebra bob", "bob secret", memberB, nil)

	sess := sessions.create("alice", memberSubA)
	defer sessions.invalidate(sess)
	verifier, challenge := pkcePair()
	page := staticAuthorize(t, a, sess, challenge, "").Body.String()
	rec := staticAuthorize(t, a, sess, challenge, consentLink(t, page))
	require.Equal(t, http.StatusFound, rec.Code)
	loc, err := url.Parse(rec.Header().Get("Location"))
	require.NoError(t, err)
	code := loc.Query().Get("code")
	require.NotEmpty(t, code)

	form := url.Values{"grant_type": {"authorization_code"}, "code": {code}, "code_verifier": {verifier}, "client_id": {"connector-1"}, "redirect_uri": {claudeCallback}}
	req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	trec := httptest.NewRecorder()
	a.handleToken(trec, req)
	require.Equal(t, http.StatusOK, trec.Code, trec.Body.String())
	var tok struct {
		Access string `json:"access_token"`
		Scope  string `json:"scope"`
	}
	require.NoError(t, json.Unmarshal(trec.Body.Bytes(), &tok))
	assert.ElementsMatch(t, []string{scopeRead, scopeWriteOwn}, strings.Fields(tok.Scope))

	st, sid := mcpInitWith(t, srv, tok.Access)
	require.Equal(t, http.StatusOK, st)
	r := &memberRig{store: store, srv: srv, a: a}
	code2, resp := r.call(t, tok.Access, sid, "search_memory", map[string]any{"query": "zebra", "subject": memberB})
	require.Equal(t, http.StatusOK, code2)
	assert.NotContains(t, mcpResultText(t, resp), "zebra bob")
	code2, resp = r.call(t, tok.Access, sid, "upsert_memory", map[string]any{"kind": "note", "title": "e2e note", "content": "kept"})
	require.Equal(t, http.StatusOK, code2)
	require.NotContains(t, mcpResultText(t, resp), "error")
	assert.Len(t, store.ListRecent("note", scopePrivate, memberA, 10), 1)
}

// ── review fixes: cascade, oracle, quota, flag tie ───────────────────────────

func (r *memberRig) putCiting(t *testing.T, scope, title, owner, cites string) string {
	t.Helper()
	res, msg := r.store.Upsert(context.Background(), "", "note", scope, title, "cites "+cites, owner, "", "", nil, nil, WithEvidenceRefs([]string{cites}))
	require.Empty(t, msg)
	return res.ID
}

func TestMCPMember_RefuteNeverCascadesOntoOthersCells(t *testing.T) {
	r := newMemberRig(t)
	// a member cannot plant a cell under a dangling id that a shared cell cites
	shared := r.putCiting(t, scopePublic, "derived shared", "", "gone-episode-1")
	out := r.member(t, "upsert_memory", map[string]any{"id": "gone-episode-1", "kind": "note", "title": "t", "content": "c"})
	assert.Equal(t, "not found", out["error"])
	assert.Nil(t, r.store.GetItem("gone-episode-1"))
	assert.Equal(t, "active", r.store.GetItem(shared).Status)

	// even citing the member's own cell, a refutation stops at that cell
	mine := r.put(t, scopePrivate, "note", "my note", memberA)
	sharedCiting := r.putCiting(t, scopePublic, "shared citing mine", "", mine)
	bobCiting := r.putCiting(t, scopePrivate, "bob citing mine", memberB, mine)
	out = r.member(t, "refute_memory", map[string]any{"id": mine, "reason": "wrong"})
	require.NotContains(t, out, "error")
	assert.Empty(t, out["cascaded"])
	assert.Equal(t, "stale", r.store.GetItem(mine).Status)
	assert.Equal(t, "active", r.store.GetItem(sharedCiting).Status)
	assert.Equal(t, "active", r.store.GetItem(bobCiting).Status)

	// admin behaviour is unchanged: the cascade still runs
	target := r.put(t, scopePublic, "stable_fact", "wrong fact", "")
	dependent := r.putCiting(t, scopePublic, "dependent", "", target)
	st, resp := r.call(t, r.admin, r.asid, "refute_memory", map[string]any{"id": target, "reason": "wrong"})
	require.Equal(t, http.StatusOK, st)
	assert.Contains(t, mcpResultText(t, resp), dependent)
	assert.Equal(t, "stale", r.store.GetItem(dependent).Status)
}

func TestMCPMember_NoExistenceOracle(t *testing.T) {
	r := newMemberRig(t)
	bobs := r.put(t, scopePrivate, "note", "bob note", memberB)
	shared := r.put(t, scopePublic, "stable_fact", "shared fact", "")

	del := func(id string) string {
		_, resp := r.call(t, r.tok, r.sid, "delete_memory", map[string]any{"id": id})
		return strings.ReplaceAll(mcpResultText(t, resp), id, "ID")
	}
	assert.Equal(t, del("missing-id"), del(bobs), "delete: a foreign private cell answers like a missing id")
	assert.NotNil(t, r.store.GetItem(bobs))

	ups := func(id string) string {
		_, resp := r.call(t, r.tok, r.sid, "upsert_memory", map[string]any{"id": id, "kind": "note", "title": "t", "content": "c"})
		return mcpResultText(t, resp)
	}
	assert.Equal(t, ups("missing-id"), ups(bobs), "upsert: foreign private")
	assert.Equal(t, ups("missing-id"), ups(shared), "upsert: shared")
	assert.Contains(t, ups(shared), "error", "an id is not a way to take over a cell")
	assert.Equal(t, "shared fact", r.store.GetItem(shared).Title)
	assert.Equal(t, scopePublic, r.store.GetItem(shared).Scope)
	assert.Equal(t, "bob note", r.store.GetItem(bobs).Title)

	sup := func(newID string) string {
		_, resp := r.call(t, r.tok, r.sid, "supersede_memory", map[string]any{"old_id": r.put(t, scopePrivate, "note", "old", memberA), "new_id": newID, "kind": "note", "title": "t", "content": "c"})
		return mcpResultText(t, resp)
	}
	assert.Equal(t, sup("missing-id"), sup(bobs), "supersede new_id: foreign private")
	assert.Equal(t, sup("missing-id"), sup(shared), "supersede new_id: shared")
	assert.Equal(t, scopePublic, r.store.GetItem(shared).Scope, "new_id may not name a shared cell")
}

// Retired cells still sit in the store, so they count against the limit; only
// delete frees room.
func TestMCPMember_PrivateNoteQuota(t *testing.T) {
	r := newMemberRig(t)
	var ids []string
	for i := 0; i < memberPrivateLimit; i++ {
		ids = append(ids, r.put(t, scopePrivate, "note", "n"+strconv.Itoa(i), memberA))
	}
	create := map[string]any{"kind": "note", "title": "one more", "content": "c"}
	supersede := func(old string) map[string]any {
		return map[string]any{"old_id": old, "kind": "note", "title": "new", "content": "c"}
	}
	out := r.member(t, "upsert_memory", create)
	assert.Equal(t, "private note limit reached", out["error"])
	// updating an existing own note is not growth
	own := r.store.ListRecent("note", scopePrivate, memberA, 1)[0].ID
	out = r.member(t, "upsert_memory", map[string]any{"id": own, "kind": "note", "title": "edited", "content": "c"})
	assert.NotContains(t, out, "error")
	// another member is not affected
	r.put(t, scopePrivate, "note", "bob", memberB)
	bobTok, err := r.a.signToken(memberSubB, []string{scopeRead, scopeWriteOwn})
	require.NoError(t, err)
	_, sid := mcpInitWith(t, r.srv, bobTok)
	st, resp := r.call(t, bobTok, sid, "upsert_memory", map[string]any{"kind": "note", "title": "ok", "content": "c"})
	require.Equal(t, http.StatusOK, st)
	assert.NotContains(t, mcpResultText(t, resp), "error")

	assert.Equal(t, "private note limit reached", r.member(t, "supersede_memory", supersede(ids[0]))["error"])
	assert.Equal(t, "active", r.store.GetItem(ids[0]).Status, "a refused supersede changes nothing")

	require.NotContains(t, r.member(t, "expire_memory", map[string]any{"id": ids[1]}), "error")
	require.NotContains(t, r.member(t, "refute_memory", map[string]any{"id": ids[2], "reason": "wrong"}), "error")
	assert.Equal(t, "private note limit reached", r.member(t, "upsert_memory", create)["error"], "expired and refuted cells still count")
	assert.Equal(t, "private note limit reached", r.member(t, "supersede_memory", supersede(ids[3]))["error"])
	assert.Equal(t, memberPrivateLimit, r.store.CountPrivateOwned(memberA))

	require.Equal(t, "deleted", r.member(t, "delete_memory", map[string]any{"id": ids[1]})["status"])
	assert.NotContains(t, r.member(t, "upsert_memory", create), "error", "delete frees room")
	assert.Equal(t, "private note limit reached", r.member(t, "upsert_memory", create)["error"])
}

func TestMCPMember_TokenWithoutSubjectRefused(t *testing.T) {
	r := newMemberRig(t)
	tok, err := r.a.signToken("", []string{scopeRead, scopeWriteOwn})
	require.NoError(t, err)
	st, _ := mcpInitWith(t, r.srv, tok)
	assert.Equal(t, http.StatusForbidden, st)
}

func TestMCPMember_FlagOffRefusesMemberToken(t *testing.T) {
	r := newMemberRig(t)
	r.a.SetMemberAccess(false)
	st, _ := mcpInitWith(t, r.srv, r.tok)
	assert.Equal(t, http.StatusForbidden, st)
	st, _ = r.call(t, r.tok, r.sid, "list_memory", map[string]any{})
	assert.Equal(t, http.StatusForbidden, st)
}

func TestMCPMember_FlagOffLeavesAdminResultsUnchanged(t *testing.T) {
	r := newMemberRig(t)
	r.a.SetMemberAccess(false)
	r.put(t, scopePrivate, "note", "zebra bob", memberB)
	st, resp := r.call(t, r.admin, r.asid, "list_memory", map[string]any{"subject": memberB})
	require.Equal(t, http.StatusOK, st)
	assert.Contains(t, mcpResultText(t, resp), "zebra bob")
	st, resp = r.call(t, r.admin, r.asid, "upsert_memory", map[string]any{"kind": "stable_fact", "scope": "public", "title": "pub", "content": "c", "confidence": "high"})
	require.Equal(t, http.StatusOK, st)
	assert.NotContains(t, mcpResultText(t, resp), "error")
	assert.Len(t, r.store.List("stable_fact", scopePublic, "", 0), 1)
	// the static token is as before
	st, _ = r.call(t, testMCPToken, "", "list_memory", map[string]any{})
	assert.NotEqual(t, http.StatusForbidden, st)
}
