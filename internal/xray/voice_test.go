package xray

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"google.golang.org/genai"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// ── ConfigWatcher ─────────────────────────────────────────────────────────────

func TestConfigWatcher_ParsesVoiceConfigFields(t *testing.T) {
	w := &ConfigWatcher{}
	cell := &mcell.MemoryCell{}
	cell.Name = SystemConfigCRName
	cell.Spec.Kind = "system_configuration"
	cell.Spec.VoiceConfig = &mcell.VoiceSessionConfig{
		SystemPrompt: "you are a test assistant",
		Greeting:     "Hello",
		VoiceName:    "Kore",
		VoiceModel:   "gemini-live-2.5",
		EnabledTools: []string{"search_memory", "get_memory"},
	}

	w.update(cell)
	cfg := w.Get()

	assert.Equal(t, "you are a test assistant", cfg.SystemPrompt)
	assert.Equal(t, "Hello", cfg.Greeting)
	assert.Equal(t, "Kore", cfg.VoiceName)
	assert.Equal(t, "gemini-live-2.5", cfg.VoiceModel)
	assert.Equal(t, []string{"search_memory", "get_memory"}, cfg.EnabledTools)
}

func TestConfigWatcher_EmptyWhenNoVoiceConfig(t *testing.T) {
	w := &ConfigWatcher{}
	cell := &mcell.MemoryCell{}
	cell.Name = SystemConfigCRName
	cell.Spec.Kind = "system_configuration"
	cell.Spec.Content = "ignored"

	w.update(cell)
	cfg := w.Get()

	assert.Empty(t, cfg.SystemPrompt, "content must not be used as system prompt fallback")
}

func TestConfigWatcher_IgnoresWrongCRName(t *testing.T) {
	w := &ConfigWatcher{}
	cell := &mcell.MemoryCell{}
	cell.Name = "some-other-cell"
	cell.Spec.Kind = "system_configuration"
	cell.Spec.Content = "should not be loaded"

	w.update(cell)
	cfg := w.Get()

	assert.Empty(t, cfg.SystemPrompt, "wrong CR name must be ignored")
}

func TestConfigWatcher_IgnoresWrongKind(t *testing.T) {
	w := &ConfigWatcher{}
	cell := &mcell.MemoryCell{}
	cell.Name = SystemConfigCRName
	cell.Spec.Kind = "stable_fact"
	cell.Spec.Content = "should not be loaded"

	w.update(cell)
	cfg := w.Get()

	assert.Empty(t, cfg.SystemPrompt, "wrong kind must be ignored")
}

func TestConfigWatcher_ReturnsEmptyBeforeFirstUpdate(t *testing.T) {
	w := &ConfigWatcher{}
	cfg := w.Get()
	assert.NotNil(t, cfg)
	assert.Empty(t, cfg.SystemPrompt)
}

func TestConfigWatcher_ClearsOnDelete(t *testing.T) {
	w := &ConfigWatcher{}
	cell := &mcell.MemoryCell{}
	cell.Name = SystemConfigCRName
	cell.Spec.Kind = "system_configuration"
	cell.Spec.VoiceConfig = &mcell.VoiceSessionConfig{SystemPrompt: "some prompt"}
	w.update(cell)
	require.NotEmpty(t, w.Get().SystemPrompt)

	w.delete(cell)
	assert.Empty(t, w.Get().SystemPrompt)
}

func TestConfigWatcher_UpdateReplacesOldConfig(t *testing.T) {
	w := &ConfigWatcher{}
	cell := &mcell.MemoryCell{}
	cell.Name = SystemConfigCRName
	cell.Spec.Kind = "system_configuration"
	cell.Spec.VoiceConfig = &mcell.VoiceSessionConfig{VoiceName: "Kore"}
	w.update(cell)
	assert.Equal(t, "Kore", w.Get().VoiceName)

	cell.Spec.VoiceConfig.VoiceName = "Aoede"
	w.update(cell)
	assert.Equal(t, "Aoede", w.Get().VoiceName)
}

// ── loadSessionConfig ─────────────────────────────────────────────────────────

func newVoiceManagerWithConfig(cfg *VoiceSessionConfig) *VoiceManager {
	w := &ConfigWatcher{}
	if cfg != nil {
		w.val.Store(cfg)
	}
	return &VoiceManager{config: w}
}

func TestLoadSessionConfig_ReturnsConfigFromWatcher(t *testing.T) {
	vm := newVoiceManagerWithConfig(&VoiceSessionConfig{
		SystemPrompt: "you are a test assistant",
		Greeting:     "Hello",
		VoiceName:    "Kore",
		VoiceModel:   "gemini-live",
		EnabledTools: []string{"search_memory"},
	})

	prompt, tools, voice, model, greeting, mcpServers, _ := vm.loadSessionConfig()

	assert.Equal(t, "you are a test assistant", prompt)
	assert.Equal(t, []string{"search_memory"}, tools)
	assert.Equal(t, "Kore", voice)
	assert.Equal(t, "gemini-live", model)
	assert.Equal(t, "Hello", greeting)
	assert.Empty(t, mcpServers)
}

func TestLoadSessionConfig_FallsBackToDefault_WhenEmpty(t *testing.T) {
	vm := newVoiceManagerWithConfig(nil)
	prompt, tools, voice, model, greeting, mcpServers, _ := vm.loadSessionConfig()

	assert.NotEmpty(t, prompt)
	assert.Empty(t, tools)
	assert.Empty(t, voice)
	assert.Empty(t, model)
	assert.Empty(t, greeting)
	assert.Empty(t, mcpServers)
}

// ── Upsert blocks system_configuration ───────────────────────────────────────

func TestUpsert_RejectsSystemConfigurationKind(t *testing.T) {
	s := newTestStore()
	_, errMsg := s.Upsert(context.Background(), "", "system_configuration", "global",
		"Config", "some prompt", "docs", "", "", nil, nil)
	assert.NotEmpty(t, errMsg, "Upsert must reject system_configuration kind")
}

// ── buildVoiceTools ───────────────────────────────────────────────────────────

func TestBuildVoiceTools_EndSessionAlwaysPresent(t *testing.T) {
	s := newTestStore()
	var ph atomic.Bool

	tools := buildVoiceTools(s, nil, "", "", "", nil, []string{}, 0, zerolog.Nop(), func() []string { return nil }, &ph, &atomic.Bool{}, make(chan struct{}))
	assert.Contains(t, toolNames(tools), "end_session")
}

func TestBuildVoiceTools_AllowlistFiltersTools(t *testing.T) {
	s := newTestStore()
	var ph atomic.Bool

	names := toolNames(buildVoiceTools(s, nil, "", "", "", nil, []string{"search_memory"}, 0, zerolog.Nop(), func() []string { return nil }, &ph, &atomic.Bool{}, make(chan struct{})))
	assert.Contains(t, names, "search_memory")
	assert.Contains(t, names, "end_session")
	assert.NotContains(t, names, "get_memory")
	assert.NotContains(t, names, "list_memory")
}

func TestBuildVoiceTools_EmptyAllowlistMeansAll(t *testing.T) {
	s := newTestStore()
	var ph atomic.Bool

	// No user — session-scoped tools (fetch_memory, recent_memory) are absent.
	names := toolNames(buildVoiceTools(s, nil, "", "", "", nil, nil, 0, zerolog.Nop(), func() []string { return nil }, &ph, &atomic.Bool{}, make(chan struct{})))
	assert.Contains(t, names, "search_memory")
	assert.Contains(t, names, "get_memory")
	assert.Contains(t, names, "list_memory")
	assert.Contains(t, names, "end_session")
}

func TestBuildVoiceTools_SessionToolsRequireUser(t *testing.T) {
	s := newTestStore()
	var ph atomic.Bool

	// No user — session-scoped tools must be absent.
	noUserNames := toolNames(buildVoiceTools(s, nil, "", "", "", nil, nil, 0, zerolog.Nop(), func() []string { return nil }, &ph, &atomic.Bool{}, make(chan struct{})))
	assert.NotContains(t, noUserNames, "fetch_memory")
	assert.NotContains(t, noUserNames, "recent_memory")

	// With user — session-scoped tools must be present.
	withUserNames := toolNames(buildVoiceTools(s, nil, "", "alice", "github:1", nil, nil, 0, zerolog.Nop(), func() []string { return nil }, &ph, &atomic.Bool{}, make(chan struct{})))
	assert.Contains(t, withUserNames, "fetch_memory")
	assert.Contains(t, withUserNames, "recent_memory")
}

func TestBuildVoiceTools_WriteGateMatchesUserHashNotName(t *testing.T) {
	s := newTestStore()
	var ph atomic.Bool
	build := func(username, userHash string, writeAllowed []string) []string {
		return toolNames(buildVoiceTools(s, nil, "", username, userHash, writeAllowed, nil, 0, zerolog.Nop(), func() []string { return nil }, &ph, &atomic.Bool{}, make(chan struct{})))
	}

	// The allowlist gates the write tool (upsert_memory) by userHash. A matching
	// hash unlocks it even though the display name is arbitrary.
	allowed := build("Display Name", "hash-admin", []string{"hash-admin"})
	assert.Contains(t, allowed, "upsert_memory")

	// Same display name as the allowlisted value, but the hash is not on the list →
	// no write tools (proves the gate keys on the hash, not the spoofable name).
	spoofed := build("hash-admin", "hash-attacker", []string{"hash-admin"})
	assert.NotContains(t, spoofed, "upsert_memory")

	// Empty hash never gets write tools regardless of the list.
	none := build("Display Name", "", []string{""})
	assert.NotContains(t, none, "upsert_memory")
}

// ── end_session tool ──────────────────────────────────────────────────────────

func TestEndSession_SetsPendingHangup(t *testing.T) {
	var ph atomic.Bool
	tool := defEndSession(&ph, make(chan struct{}))

	result, err := tool.handler(context.Background(), nil)

	require.NoError(t, err)
	assert.True(t, ph.Load())
	assert.Contains(t, result, "ok")
}

func TestEndSession_IdempotentWhenCalledTwice(t *testing.T) {
	var ph atomic.Bool
	tool := defEndSession(&ph, make(chan struct{}))
	_, err1 := tool.handler(context.Background(), nil)
	_, err2 := tool.handler(context.Background(), nil)
	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.True(t, ph.Load())
}

// ── defDeleteMemory ownership ─────────────────────────────────────────────────

func TestDeleteMemory_OwnershipCheck(t *testing.T) {
	s := newTestStore()
	userA := stableUserID("azure:user-a")
	userB := stableUserID("azure:user-b")

	// User A creates a note.
	res, errMsg := s.Upsert(context.Background(), "", "note", "private", "A's note", "private", userA, "", "", nil, nil)
	require.Empty(t, errMsg)
	noteID := res.ID

	tool := defDeleteMemory(s, nil, "", userB)

	// User B attempts to delete A's note — must be rejected.
	result, err := tool.handler(context.Background(), map[string]any{"id": noteID})
	require.NoError(t, err)
	assert.Contains(t, result, "forbidden")
	assert.Contains(t, result, "another user")
	assert.NotNil(t, s.Get(noteID), "note must still exist after rejected delete")

	// User A deletes own note — must succeed.
	toolA := defDeleteMemory(s, nil, "", userA)
	result, err = toolA.handler(context.Background(), map[string]any{"id": noteID})
	require.NoError(t, err)
	assert.Contains(t, result, "deleted")
	assert.Nil(t, s.Get(noteID))
}

// ── defFetchMemory ownership (pinned-items fallback path) ─────────────────────

// TestFetchMemory_NoCrossUserPrivateLeak verifies fetch_memory's pinned-items
// fallback (called with no ids) never returns another user's private note,
// even though pinned IDs are client-supplied and memory.Get resolves any item
// by raw ID. Regression test for the cross-user private-note read that used
// to be get_pinned_items, folded into fetch_memory under ADR-051.
func TestFetchMemory_NoCrossUserPrivateLeak(t *testing.T) {
	s := newTestStore()
	userA := stableUserID("azure:user-a")
	userB := stableUserID("azure:user-b")

	// User A pins a private note; user B pins a public item.
	privRes, errMsg := s.Upsert(context.Background(), "", "note", "private", "A's secret", "top secret content", userA, "", "", nil, nil)
	require.Empty(t, errMsg)
	pubRes, errMsg := s.Upsert(context.Background(), "", "stable_fact", "public", "Shared fact", "public content", "docs", "", "", nil, nil)
	require.Empty(t, errMsg)

	// User B supplies BOTH IDs as pinned (the attack: guessing A's note ID).
	pinned := []string{privRes.ID, pubRes.ID}
	tool := defFetchMemory(s, userB, func() []string { return pinned }, 0, zerolog.Nop())

	result, err := tool.handler(context.Background(), nil)
	require.NoError(t, err)
	assert.NotContains(t, result, "top secret content", "another user's private note must never be returned")
	assert.NotContains(t, result, privRes.ID, "another user's private note ID must not be echoed")
	assert.Contains(t, result, "public content", "public items are still returned")

	// User A pinning their OWN private note still works.
	toolA := defFetchMemory(s, userA, func() []string { return pinned }, 0, zerolog.Nop())
	resultA, err := toolA.handler(context.Background(), nil)
	require.NoError(t, err)
	assert.Contains(t, resultA, "top secret content", "owner sees their own pinned private note")
}

// TestFetchMemory_ExplicitIDsBypassPinnedFallback verifies that when the model
// supplies ids explicitly, fetch_memory fetches exactly those (not the pinned
// set) — the same cross-user ownership guard still applies.
func TestFetchMemory_ExplicitIDsBypassPinnedFallback(t *testing.T) {
	s := newTestStore()
	userA := stableUserID("azure:user-a")

	res, errMsg := s.Upsert(context.Background(), "", "stable_fact", "public", "Fact", "fact content", "docs", "", "", nil, nil)
	require.Empty(t, errMsg)

	tool := defFetchMemory(s, userA, func() []string { return nil }, 0, zerolog.Nop())
	result, err := tool.handler(context.Background(), map[string]any{"ids": []any{res.ID}})
	require.NoError(t, err)
	assert.Contains(t, result, "fact content")
}

// ── helpers ───────────────────────────────────────────────────────────────────

func toolNames(tools []voiceToolDef) []string {
	names := make([]string, len(tools))
	for i, t := range tools {
		names[i] = t.decl.Name
	}
	return names
}

// ── search_memory private-note isolation ──────────────────────────────────────

// TestDefSearchMemory_AgentNoteOnlyOwnNotes verifies the voice search_memory tool
// never returns another user's notes, even when the model passes kind=note.
// The shared search must drop private cells (they would otherwise be unscoped);
// notes come only from the userHash-scoped merge.
func TestDefSearchMemory_AgentNoteOnlyOwnNotes(t *testing.T) {
	s := newTestStore()
	userA := stableUserID("azure:user-a")
	userB := stableUserID("azure:user-b")
	upsertItem(t, s, "", "note", "private", "A note", "alpha secret plan", userA, nil)
	upsertItem(t, s, "", "note", "private", "B note", "alpha secret plan", userB, nil)

	// Tool built for user A.
	def := defSearchMemory(s, userA, 0, zerolog.Nop())

	call := func(args map[string]any) []string {
		out, err := def.handler(context.Background(), args)
		require.NoError(t, err)
		var resp struct {
			Items []struct {
				Subject string `json:"subject"`
				Title   string `json:"title"`
			} `json:"items"`
		}
		require.NoError(t, json.Unmarshal([]byte(out), &resp))
		titles := make([]string, 0, len(resp.Items))
		for _, it := range resp.Items {
			assert.NotEqual(t, userB, it.Subject, "another user's note must never be returned")
			titles = append(titles, it.Title)
		}
		return titles
	}

	t.Run("kind=note returns only caller's note", func(t *testing.T) {
		titles := call(map[string]any{"query": "alpha", "kind": "note"})
		assert.Contains(t, titles, "A note")
		assert.NotContains(t, titles, "B note")
	})

	t.Run("general search returns no notes from either user", func(t *testing.T) {
		// userB's note must not leak; userA's own note is merged in via userHash.
		titles := call(map[string]any{"query": "alpha"})
		assert.NotContains(t, titles, "B note")
	})
}

// TestDefSearchMemory_EventTimeAnnotation verifies voice's search_memory tool
// (a second, independent render path from MCP's — see
// docs/todo/graphiti-comparison-refinement.md item #1) also surfaces the
// event_time annotation in its rendered title.
func TestDefSearchMemory_EventTimeAnnotation(t *testing.T) {
	s := newTestStore()
	upsertEventItem(t, s, "", "event", "public", "Deploy widget-api", "content", "", "2023-05-30T00:00:00Z")
	upsertItem(t, s, "", "stable_fact", "public", "widget-api owns billing", "content", "", nil)

	out, err := defSearchMemory(s, "", 0, zerolog.Nop()).handler(context.Background(), map[string]any{"query": "widget-api"})
	require.NoError(t, err)
	var resp struct {
		Items []struct {
			Title string `json:"title"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &resp))
	titles := make([]string, 0, len(resp.Items))
	for _, it := range resp.Items {
		titles = append(titles, it.Title)
	}
	assert.Contains(t, titles, "Deploy widget-api (event_time: 2023-05-30)", "item with EventDate must carry the annotation")
	assert.Contains(t, titles, "widget-api owns billing", "item without EventDate must render its title unchanged")
}

// TestDefSearchMemory_HeadroomTruncatesContent verifies the headroom content
// budget (ADR-038) is wired into search_memory: a long body is previewed and
// flagged truncated, while the zero-limit tool returns it whole.
func TestDefSearchMemory_HeadroomTruncatesContent(t *testing.T) {
	s := newTestStore()
	long := "# Heading\n\n" + strings.Repeat("token ", 300) + "[link](https://example.com/x)"
	upsertItem(t, s, "", "stable_fact", "public", "Big", long, "docs", nil)

	type resp struct {
		Items []struct {
			Content   string `json:"content"`
			Truncated bool   `json:"truncated"`
		} `json:"items"`
	}
	search := func(limit int) resp {
		out, err := defSearchMemory(s, "", limit, zerolog.Nop()).handler(context.Background(), map[string]any{"query": "token"})
		require.NoError(t, err)
		var r resp
		require.NoError(t, json.Unmarshal([]byte(out), &r))
		require.NotEmpty(t, r.Items)
		return r
	}

	capped := search(200)
	assert.True(t, capped.Items[0].Truncated, "long body should be flagged truncated")
	assert.LessOrEqual(t, len([]rune(capped.Items[0].Content)), 210, "content should be capped near budget")
	assert.NotContains(t, capped.Items[0].Content, "https://", "markdown link URL should be stripped")

	whole := search(0)
	assert.False(t, whole.Items[0].Truncated, "zero limit must not truncate")
	assert.Contains(t, whole.Items[0].Content, "https://example.com/x", "zero limit passes content through verbatim")
}

// ── per-modality token split (ADR: voice token attribution) ──────────────────

func TestModalitySplit(t *testing.T) {
	// nil/empty → nil map, indexable without panic.
	require.Nil(t, modalitySplit(nil))
	require.Equal(t, int64(0), modalitySplit(nil)["audio"])

	got := modalitySplit([]*genai.ModalityTokenCount{
		{Modality: genai.MediaModalityAudio, TokenCount: 100},
		{Modality: genai.MediaModalityText, TokenCount: 40},
		{Modality: genai.MediaModalityAudio, TokenCount: 5}, // same modality accumulates
		nil, // tolerated
	})
	assert.Equal(t, int64(105), got["audio"])
	assert.Equal(t, int64(40), got["text"])
}

func TestSessionBreakdown_NilUntilCounted(t *testing.T) {
	s := &VoiceSession{}
	require.Nil(t, s.sessionBreakdown(), "no tokens counted yet → nil")

	s.sessionInAudioTokens.Store(117)
	s.sessionOutTextTokens.Store(3)
	s.sessionCachedTokens.Store(7)
	bd := s.sessionBreakdown()
	require.NotNil(t, bd)
	assert.Equal(t, int64(117), bd.InputAudio)
	assert.Equal(t, int64(3), bd.OutputText)
	assert.Equal(t, int64(7), bd.Cached)
	assert.Equal(t, int64(0), bd.InputText)
}

// A voice session opens with the user's newest private notes in its prompt, and
// never with another user's.
func TestVoiceManager_UserRecall(t *testing.T) {
	s := newTestStore()
	me, other := stableUserID("dev"), stableUserID("github:1")
	_, _ = s.Upsert(t.Context(), "", "note", "private", "Tracing", "Traces go to the\nego collector.", me, "", "", nil, nil)
	_, _ = s.Upsert(t.Context(), "", "note", "private", "Theirs", "not for this user", other, "", "", nil, nil)
	m := &VoiceManager{memory: s}

	got := m.userRecall(me)
	assert.Contains(t, got, "Tracing: Traces go to the ego collector.", "a note is one line")
	assert.NotContains(t, got, "Theirs")
	assert.Empty(t, (&VoiceManager{memory: newTestStore()}).userRecall(me), "no notes, no section")
}

// list_memory shows the caller's own private notes alongside the shared ones.
func TestListMemory_IncludesTheCallersPrivateNotes(t *testing.T) {
	s := newTestStore()
	me, other := stableUserID("dev"), stableUserID("github:1")
	_, _ = s.Upsert(t.Context(), "", "note", "private", "Mine", "x", me, "", "", nil, nil)
	_, _ = s.Upsert(t.Context(), "", "note", "private", "Theirs", "y", other, "", "", nil, nil)

	out, err := defListMemory(s, me).handler(t.Context(), map[string]any{"kind": "note"})
	require.NoError(t, err)
	assert.Contains(t, out, "Mine")
	assert.NotContains(t, out, "Theirs")
}

// Gemini 3 Live closes the session when affective dialog is set; the 2.5 native
// audio models accept it.
func TestLiveSupportsAffectiveDialog(t *testing.T) {
	for model, want := range map[string]bool{
		"gemini-3.8-live":                           false,
		"google/gemini-3.8-live":                    false,
		"google/gemini-live-2.5-flash-native-audio": true,
		"gemini-live-2.5-flash-native-audio":        true,
	} {
		assert.Equal(t, want, liveSupportsAffectiveDialog(model), model)
	}
}

// A code-graph call the model repeats verbatim is recognised; a different
// argument, a different tool and a memory tool are not.
func TestVoiceSession_IsRepeatedCodeCall(t *testing.T) {
	s := &VoiceSession{}
	a := map[string]any{"qn": "m//a.go"}
	assert.False(t, s.isRepeatedCodeCall("get_graph_node", a))
	assert.True(t, s.isRepeatedCodeCall("get_graph_node", map[string]any{"qn": "m//a.go"}))
	assert.False(t, s.isRepeatedCodeCall("get_graph_node", map[string]any{"qn": "m//b.go"}))
	assert.False(t, s.isRepeatedCodeCall("search_graph", a))
	assert.False(t, s.isRepeatedCodeCall("search_memory", a))
	assert.False(t, s.isRepeatedCodeCall("search_memory", a), "memory can change, never blocked")
}

// The tool calls of one turn run in parallel and each reports to the browser while
// the model's audio is still streaming; gorilla/websocket panics on a second
// concurrent writer, which took the server down.
func TestVoiceSession_SendIsSafeConcurrently(t *testing.T) {
	up := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	defer conn.Close()
	s := &VoiceSession{browser: conn}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				assert.NoError(t, s.send(VoiceServerMessage{ToolCall: "search_memory"}))
			}
		}()
	}
	wg.Wait()
}
