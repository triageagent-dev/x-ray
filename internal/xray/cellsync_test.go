package xray

import (
	"context"
	"encoding/base64"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// fakeMapReg records AddSnapshotBytes / RemoveSnapshotByLabel calls for asserting
// the CellSyncer graph-cell wiring without a real recall registry (so no real gob
// is needed — addErr lets a test simulate a bad snapshot).
type fakeMapReg struct {
	added   [][]byte
	removed []string
	addErr  error
}

func (f *fakeMapReg) AddSnapshotBytes(_ context.Context, b []byte) error {
	if f.addErr != nil {
		return f.addErr
	}
	f.added = append(f.added, b)
	return nil
}
func (f *fakeMapReg) RemoveSnapshotByLabel(label string) { f.removed = append(f.removed, label) }

// ── Data field round-trip ─────────────────────────────────────────────────────

func TestDataFieldRoundTrip(t *testing.T) {
	cell := &mcell.MemoryCell{}
	cell.Spec.Data = map[string]mcell.DataEntry{
		"image": {Bytes: "abc123", Mime: "image/jpeg"},
	}
	it := viewOf(itemOfCell(cell))
	img, ok := it.Data["image"]
	assert.True(t, ok)
	assert.Equal(t, "abc123", img.Bytes)
	assert.Equal(t, "image/jpeg", img.Mime)
}

// ── itemOfCell ────────────────────────────────────────────────────────────────

func TestItemOfCell(t *testing.T) {
	spec := mcell.MemoryCellSpec{
		ID:         "cell-1",
		Kind:       "stable_fact",
		Scope:      "public",
		Subject:    "alice",
		Title:      "My fact",
		Content:    "real content",
		Confidence: "high",
		Status:     "active",
		Tags:       []string{"go", "testing"},
		EventDate:  "2026-01-01T00:00:00Z",
	}

	doc := viewOf(itemOfCell(&mcell.MemoryCell{Spec: spec}))

	assert.Equal(t, "cell-1", doc.ID)
	assert.Equal(t, "stable_fact", doc.Kind)
	assert.Equal(t, "public", doc.Scope)
	assert.Equal(t, "alice", doc.Subject)
	assert.Equal(t, "My fact", doc.Title)
	assert.Equal(t, "real content", doc.Content)
	assert.Equal(t, "high", doc.Confidence)
	assert.Equal(t, "active", doc.Status)
	assert.Equal(t, []string{"go", "testing"}, doc.Tags)
	assert.Equal(t, "2026-01-01T00:00:00Z", doc.EventDate)
}

// ── cellResourceName (via indirect coverage) ──────────────────────────────────
// cellResourceName is unexported; test it by verifying its output properties
// without importing it directly.

func TestCellResourceName_Properties(t *testing.T) {
	name1 := cellResourceName("some-id-here")
	name2 := cellResourceName("some-id-here")
	name3 := cellResourceName("different-id")

	// Deterministic
	assert.Equal(t, name1, name2)
	// Different inputs produce different names
	assert.NotEqual(t, name1, name3)
	// Valid k8s name: lowercase, no spaces
	assert.Regexp(t, `^[a-z0-9-]+$`, name1)
	// Ends with 8-char hex suffix
	assert.Regexp(t, `-[0-9a-f]{8}$`, name1)
}

// ── specOf ───────────────────────────────────────────────────────────────────

func TestSpecOf(t *testing.T) {
	it := &MemoryItem{
		ID:         "item-1",
		Kind:       "stable_fact",
		Scope:      "public",
		Subject:    "bob",
		Title:      "Test item",
		Content:    "real content",
		Confidence: "medium",
		Tags:       []string{"tag1"},
		Status:     "active",
		EventDate:  "2026-06-01T00:00:00Z",
	}

	doc := specOf(it, "inst")

	assert.Equal(t, "item-1", doc.ID)
	assert.Equal(t, "stable_fact", doc.Kind)
	assert.Equal(t, "bob", doc.Subject)
	assert.Equal(t, "real content", doc.Content)
	assert.Equal(t, "medium", doc.Confidence)
	assert.Equal(t, []string{"tag1"}, doc.Tags)
}

// ── PublishHit ────────────────────────────────────────────────────────────────

func TestMemoryStore_PublishHit(t *testing.T) {
	s := NewMemoryStore()
	ch, cancel := s.hub.Subscribe("")
	defer cancel()

	s.PublishHit([]string{"id1", "id2"}, "stable_fact", []string{"stable_fact", "note"})

	msg := <-ch
	assert.Contains(t, string(msg.Data), "id1")
	assert.Contains(t, string(msg.Data), `"kinds":["stable_fact","note"]`)
}

// ── graph-carrying cell → in-memory map ───────────────────────────────────────

func graphCell(subject, name string, gobBytes []byte) *mcell.MemoryCell {
	c := &mcell.MemoryCell{}
	c.Name = name
	c.Spec.ID = name
	c.Spec.Kind = "mental_map"
	c.Spec.Subject = subject
	c.Spec.Data = map[string]mcell.DataEntry{
		graphDataKey: {Bytes: base64.StdEncoding.EncodeToString(gobBytes), Mime: "application/x-gzip"},
	}
	return c
}

func TestSyncMapCell_RegistersDecodedBytes(t *testing.T) {
	reg := &fakeMapReg{}
	s := &CellSyncer{mapReg: reg}
	raw := []byte("PRETEND-GOB-BYTES")

	err := s.syncMapCell(context.Background(), graphCell("acme-services", "mental-map-acme-services", raw))
	assert.NoError(t, err)

	// AddSnapshotBytes got the base64-DECODED bytes.
	assert.Len(t, reg.added, 1)
	assert.Equal(t, raw, reg.added[0])
	// Replacing-before-add: label de-registered first so an update swaps cleanly.
	assert.Equal(t, []string{"acme-services"}, reg.removed)
}

func TestSyncMapCell_NoRegistrarIsNoOp(t *testing.T) {
	s := &CellSyncer{} // mapReg nil
	err := s.syncMapCell(context.Background(), graphCell("x", "mental-map-x", []byte("g")))
	assert.NoError(t, err)
}

func TestSyncMapCell_BadBase64Errors(t *testing.T) {
	reg := &fakeMapReg{}
	s := &CellSyncer{mapReg: reg}
	c := graphCell("x", "mental-map-x", nil)
	c.Spec.Data[graphDataKey] = mcell.DataEntry{Bytes: "!!!not base64!!!"}
	err := s.syncMapCell(context.Background(), c)
	assert.Error(t, err)
	assert.Empty(t, reg.added)
}

func TestRemoveMapCell_UsesSubjectLabel(t *testing.T) {
	reg := &fakeMapReg{}
	s := &CellSyncer{mapReg: reg}
	s.removeMapCell(graphCell("acme-services", "mental-map-acme-services", nil))
	assert.Equal(t, []string{"acme-services"}, reg.removed)
}

func TestMapLabelForCell_FallsBackToID(t *testing.T) {
	c := &mcell.MemoryCell{}
	c.Spec.ID = "cell-abc"
	assert.Equal(t, "cell-abc", mapLabelForCell(c)) // no subject → id
	c.Spec.Subject = "svc"
	assert.Equal(t, "svc", mapLabelForCell(c)) // subject wins
}

func TestMapLabelFromResourceName_StripsMentalMapPrefix(t *testing.T) {
	assert.Equal(t, "acme-services", mapLabelFromResourceName("mental-map-acme-services"))
	assert.Equal(t, "some-other-cell", mapLabelFromResourceName("some-other-cell")) // no prefix → raw
}

func TestSyncMapCell_SkipsUnchangedGob(t *testing.T) {
	reg := &fakeMapReg{}
	s := &CellSyncer{mapReg: reg}
	ctx := context.Background()
	cell := graphCell("acme", "mental-map-graph-acme", []byte("GOB-V1"))

	// First sync registers.
	if err := s.syncMapCell(ctx, cell); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	// Second sync of the SAME bytes is a no-op — no re-register, no re-embed.
	if err := s.syncMapCell(ctx, cell); err != nil {
		t.Fatalf("second sync: %v", err)
	}
	assert.Len(t, reg.added, 1, "unchanged gob must not re-register")

	// Changed bytes DO re-register.
	changed := graphCell("acme", "mental-map-graph-acme", []byte("GOB-V2"))
	if err := s.syncMapCell(ctx, changed); err != nil {
		t.Fatalf("changed sync: %v", err)
	}
	assert.Len(t, reg.added, 2, "changed gob must re-register")

	// Delete clears the hash so a recreate re-registers rather than being skipped.
	s.removeMapCell(changed)
	if err := s.syncMapCell(ctx, changed); err != nil {
		t.Fatalf("recreate sync: %v", err)
	}
	assert.Len(t, reg.added, 3, "recreate after delete must re-register")
}

// ── Persist unchanged-skip (ADR-033, ported from the sibling triage repo) ──────
//
// A memory→etcd write edge must be a no-op when nothing changed: re-Persisting
// an identical item (a re-upserting agent loop, a redundant voice/MCP write)
// must not touch the CR at all, or every idempotent write bumps
// resourceVersion and re-fires the CellSyncer's own reconcile.

// persistCounters tracks Update / status-Update calls on the underlying fake
// client via an interceptor, so tests can assert Persist made NO write at all
// — not just "the content still looks the same" after the fact. Atomic, for a
// test that writes from several goroutines.
type persistCounters struct {
	updates       atomic.Int64
	statusUpdates atomic.Int64
}

// newTrackedCellSyncer builds a CellSyncer over a fake client instrumented to
// count Update/status-Update calls.
func newTrackedCellSyncer(t *testing.T) (*CellSyncer, *persistCounters) {
	t.Helper()
	tracked, counters := trackedClient(t, fake.NewClientBuilder())
	return NewCellSyncer(tracked, NewMemoryStore(), zerolog.Nop()), counters
}

// trackedClient builds the fake client b describes, instrumented to count
// Update/status-Update calls.
func trackedClient(t *testing.T, b *fake.ClientBuilder) (client.Client, *persistCounters) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, mcell.AddToScheme(scheme))
	fakeClient := b.WithScheme(scheme).Build()

	counters := &persistCounters{}
	tracked := interceptor.NewClient(fakeClient, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			counters.updates.Add(1)
			return c.Update(ctx, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			counters.statusUpdates.Add(1)
			return c.SubResource(subResourceName).Update(ctx, obj, opts...)
		},
	})
	return tracked, counters
}

func TestPersist_UnchangedIsNoOp(t *testing.T) {
	syncer, counters := newTrackedCellSyncer(t)
	ctx := context.Background()

	it := &MemoryItem{
		ID: "note-unchanged", Kind: "note", Scope: "public",
		Title: "Title", Content: "hello world", Confidence: "medium",
		Tags: []string{"a", "b"},
	}
	require.NoError(t, syncer.Persist(ctx, it, "ns"))
	require.EqualValues(t, 0, counters.updates.Load(), "create path uses Create, not Update")
	require.EqualValues(t, 1, counters.statusUpdates.Load(), "create stamps the Indexed status once")

	name := cellResourceName(it.ID)
	created := &mcell.MemoryCell{}
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, created))

	// Re-Persist the identical item — must be a full no-op: no Update call, no
	// UpdatedAt stamp, no status update.
	require.NoError(t, syncer.Persist(ctx, it, "ns"))
	assert.EqualValues(t, 0, counters.updates.Load(), "unchanged re-Persist must not call Update")
	assert.EqualValues(t, 1, counters.statusUpdates.Load(), "unchanged re-Persist must not touch status either")

	after := &mcell.MemoryCell{}
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, after))
	assert.True(t, after.Spec.UpdatedAt.Time.Equal(created.Spec.UpdatedAt.Time),
		"UpdatedAt must not be stamped when nothing changed")
	assert.Equal(t, created.ResourceVersion, after.ResourceVersion,
		"resourceVersion must not change on an unchanged re-Persist")
}

func TestPersist_ChangedFieldUpdatesAndStampsUpdatedAt(t *testing.T) {
	syncer, counters := newTrackedCellSyncer(t)
	ctx := context.Background()

	it := &MemoryItem{
		ID: "note-changed", Kind: "note", Scope: "public",
		Title: "Title", Content: "v1", Confidence: "medium",
	}
	require.NoError(t, syncer.Persist(ctx, it, "ns"))

	name := cellResourceName(it.ID)
	before := &mcell.MemoryCell{}
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, before))

	// The fake client's stored metav1.Time round-trips through JSON and loses
	// sub-second precision, so a real clock tick (not just a scheduling
	// yield) is needed to make an UpdatedAt bump observable.
	time.Sleep(1100 * time.Millisecond)

	changed := *it
	changed.Content = "v2"
	require.NoError(t, syncer.Persist(ctx, &changed, "ns"))
	assert.EqualValues(t, 1, counters.updates.Load(), "changed content must call Update")
	assert.EqualValues(t, 2, counters.statusUpdates.Load())

	after := &mcell.MemoryCell{}
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, after))
	assert.Equal(t, "v2", after.Spec.Content)
	assert.NotEqual(t, before.ResourceVersion, after.ResourceVersion,
		"a real change must actually write the CR")
	assert.True(t, after.Spec.UpdatedAt.Time.After(before.Spec.UpdatedAt.Time),
		"UpdatedAt must be stamped on a real content change")
}

// TestPersist_EmbeddingOnlyDiffIsStillNoOp: the reconciler's own embedding
// self-write (Spec.Embedding patched back after an async embed) must not read
// back through Persist as a content change on the next idempotent write.
func TestPersist_EmbeddingOnlyDiffIsStillNoOp(t *testing.T) {
	syncer, counters := newTrackedCellSyncer(t)
	ctx := context.Background()

	it := &MemoryItem{
		ID: "note-embed", Kind: "note", Scope: "public",
		Title: "Title", Content: "hello", Confidence: "medium",
	}
	require.NoError(t, syncer.Persist(ctx, it, "ns"))

	// Simulate the reconciler patching an embedding onto the CR out-of-band —
	// the exact write Persist must preserve, never treat as a reason to update.
	name := cellResourceName(it.ID)
	cell := &mcell.MemoryCell{}
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, cell))
	cell.Spec.Embedding = "AAAA-fake-vector"
	require.NoError(t, syncer.Update(ctx, cell))
	updatesAfterEmbed := counters.updates.Load()

	// Re-Persist the SAME logical item — the CR now differs only by Embedding,
	// which must not be seen as a content change.
	require.NoError(t, syncer.Persist(ctx, it, "ns"))
	assert.Equal(t, updatesAfterEmbed, counters.updates.Load(),
		"embedding-only CR diff must not trigger an Update")

	final := &mcell.MemoryCell{}
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, final))
	assert.Equal(t, "AAAA-fake-vector", final.Spec.Embedding, "embedding must be preserved")
}

// TestPersist_UpdatePreservesFieldsWithNoItemCounterpart is the Task 1
// regression test: Persist's update path used to rebuild Spec from scratch and
// silently zero every field the in-memory MemoryItem has no counterpart for
// (EvidenceRefs, Supersedes, Links, VoiceConfig, MCP, UI). It must now
// preserve them across a genuine content-changing write. StaleAfter now HAS a
// MemoryItem counterpart (added for the retention sweep), so it is written
// from the item like any other item-carried field — this test round-trips it
// forward (as a real caller reading-then-editing an item would) and confirms
// it survives too.
func TestPersist_UpdatePreservesFieldsWithNoItemCounterpart(t *testing.T) {
	syncer, _ := newTrackedCellSyncer(t)
	ctx := context.Background()

	it := &MemoryItem{
		ID: "note-preserve", Kind: "note", Scope: "public",
		Title: "Title", Content: "v1", Confidence: "medium",
	}
	require.NoError(t, syncer.Persist(ctx, it, "ns"))

	// Attach fields Persist's create path never writes — the way an external
	// writer (kubectl/a config tool)
	// would — directly on the stored CR.
	name := cellResourceName(it.ID)
	cell := &mcell.MemoryCell{}
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, cell))
	deadline := metav1.NewTime(time.Now().Add(48 * time.Hour).Truncate(time.Second))
	cell.Spec.Links = []mcell.ThoughtLink{{ToID: "other-cell", Relation: "explains", Weight: 0.8, EvidenceRefs: []string{"ev-1"}}}
	cell.Spec.EvidenceRefs = []string{"ev-a", "ev-b"}
	cell.Spec.Supersedes = []string{"old-cell"}
	cell.Spec.StaleAfter = &deadline
	cell.Spec.VoiceConfig = &mcell.VoiceSessionConfig{SystemPrompt: "be terse", VoiceName: "Kore"}
	cell.Spec.MCP = &mcell.MCPConfig{Servers: []mcell.MCPServerConfig{{Name: "triage", URL: "https://triage.internal/mcp"}}}
	cell.Spec.UI = &mcell.UIConfig{Categories: []mcell.CategoryConfig{{Kind: "note", Label: "Notes"}}}
	require.NoError(t, syncer.Update(ctx, cell))

	// Re-Persist with a genuine content change, round-tripping the item's own
	// StaleAfter forward (a real caller reads an item — which now carries
	// StaleAfter — edits it, and writes it back).
	deadlineTime := deadline.Time
	changed := *it
	changed.Content = "v2"
	changed.StaleAfter = &deadlineTime
	require.NoError(t, syncer.Persist(ctx, &changed, "ns"))

	after := &mcell.MemoryCell{}
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, after))
	assert.Equal(t, "v2", after.Spec.Content, "genuine content change must still apply")

	require.Len(t, after.Spec.Links, 1, "Links must survive — no MemoryItem counterpart")
	assert.Equal(t, "other-cell", after.Spec.Links[0].ToID)
	assert.Equal(t, "explains", after.Spec.Links[0].Relation)
	assert.Equal(t, []string{"ev-a", "ev-b"}, after.Spec.EvidenceRefs, "EvidenceRefs must survive")
	assert.Equal(t, []string{"old-cell"}, after.Spec.Supersedes, "Supersedes must survive")
	require.NotNil(t, after.Spec.VoiceConfig, "VoiceConfig must survive")
	assert.Equal(t, "be terse", after.Spec.VoiceConfig.SystemPrompt)
	require.NotNil(t, after.Spec.MCP, "MCP must survive")
	require.Len(t, after.Spec.MCP.Servers, 1)
	assert.Equal(t, "triage", after.Spec.MCP.Servers[0].Name)
	require.NotNil(t, after.Spec.UI, "UI must survive")
	require.Len(t, after.Spec.UI.Categories, 1)
	assert.Equal(t, "Notes", after.Spec.UI.Categories[0].Label)
	require.NotNil(t, after.Spec.StaleAfter, "StaleAfter must round-trip when the item carries it forward")
	assert.True(t, after.Spec.StaleAfter.Time.Equal(deadline.Time))
}

// ── specContentEqual coverage of the new fields ─────────────────────────────

func TestSpecContentEqual_NewFieldsEqualWhenIdentical(t *testing.T) {
	stale := metav1.NewTime(time.Now().Truncate(time.Second))
	build := func() mcell.MemoryCellSpec {
		return mcell.MemoryCellSpec{
			ID: "x", Kind: "note", Status: "active",
			EvidenceRefs: []string{"a", "b"},
			Supersedes:   []string{"old"},
			Links:        []mcell.ThoughtLink{{ToID: "y", Relation: "explains", Weight: 0.5, EvidenceRefs: []string{"e1"}}},
			StaleAfter:   &stale,
			VoiceConfig:  &mcell.VoiceSessionConfig{SystemPrompt: "p", VoiceName: "Kore"},
			MCP:          &mcell.MCPConfig{Servers: []mcell.MCPServerConfig{{Name: "s", URL: "u"}}},
			UI:           &mcell.UIConfig{Categories: []mcell.CategoryConfig{{Kind: "note", Label: "Notes"}}},
		}
	}
	// a and b are built from two independent struct literals (distinct pointers
	// for StaleAfter/VoiceConfig/MCP/UI) — specContentEqual must compare by
	// value, not by pointer identity.
	a := build()
	b := build()
	assert.True(t, specContentEqual(a, b), "identical new-field content built from separate literals must compare equal")
}

func TestSpecContentEqual_NewFieldsDetectDifference(t *testing.T) {
	stale1 := metav1.NewTime(time.Now().Truncate(time.Second))
	stale2 := metav1.NewTime(stale1.Time.Add(time.Hour))

	assert.False(t, specContentEqual(
		mcell.MemoryCellSpec{ID: "x", StaleAfter: &stale1},
		mcell.MemoryCellSpec{ID: "x", StaleAfter: &stale2},
	), "differing StaleAfter must be detected")

	assert.False(t, specContentEqual(
		mcell.MemoryCellSpec{ID: "x", StaleAfter: &stale1},
		mcell.MemoryCellSpec{ID: "x"},
	), "nil vs non-nil StaleAfter must be detected")

	assert.False(t, specContentEqual(
		mcell.MemoryCellSpec{ID: "x", Links: []mcell.ThoughtLink{{ToID: "y", Relation: "explains"}}},
		mcell.MemoryCellSpec{ID: "x", Links: []mcell.ThoughtLink{{ToID: "z", Relation: "explains"}}},
	), "differing Links must be detected")

	assert.False(t, specContentEqual(
		mcell.MemoryCellSpec{ID: "x", EvidenceRefs: []string{"a"}},
		mcell.MemoryCellSpec{ID: "x", EvidenceRefs: []string{"b"}},
	), "differing EvidenceRefs must be detected")

	assert.False(t, specContentEqual(
		mcell.MemoryCellSpec{ID: "x", VoiceConfig: &mcell.VoiceSessionConfig{SystemPrompt: "p1"}},
		mcell.MemoryCellSpec{ID: "x", VoiceConfig: &mcell.VoiceSessionConfig{SystemPrompt: "p2"}},
	), "differing VoiceConfig must be detected")

	assert.False(t, specContentEqual(
		mcell.MemoryCellSpec{ID: "x", MCP: &mcell.MCPConfig{Servers: []mcell.MCPServerConfig{{Name: "a"}}}},
		mcell.MemoryCellSpec{ID: "x", MCP: &mcell.MCPConfig{Servers: []mcell.MCPServerConfig{{Name: "b"}}}},
	), "differing MCP must be detected")

	assert.False(t, specContentEqual(
		mcell.MemoryCellSpec{ID: "x", UI: &mcell.UIConfig{Categories: []mcell.CategoryConfig{{Kind: "a"}}}},
		mcell.MemoryCellSpec{ID: "x", UI: &mcell.UIConfig{Categories: []mcell.CategoryConfig{{Kind: "b"}}}},
	), "differing UI must be detected")
}

// ── syncCell private-cell mirror unchanged-skip (ADR-033) ──────────────────────
//
// The reconcile (etcd→memory) path for private cells writes straight into the
// MemoryStore's in-memory item map. That mirror must be silent when the CR's
// content already matches what's held: no replacement of the stored item, and
// (per MemoryItem never carrying an embedding) an embedding-only CR diff must
// not register as a change either.

func privateCell(id, subject, content string) *mcell.MemoryCell {
	c := &mcell.MemoryCell{}
	c.Name = cellResourceName(id)
	c.Spec = mcell.MemoryCellSpec{
		ID: id, Kind: "note", Scope: scopePrivate, Subject: subject,
		Title: "T", Content: content, Status: "active",
	}
	return c
}

func TestSyncCell_PrivateUnchangedIsSilent(t *testing.T) {
	mem := NewMemoryStore()
	s := NewCellSyncer(fake.NewClientBuilder().Build(), mem, zerolog.Nop())
	ctx := context.Background()

	require.NoError(t, s.SyncCell(ctx, privateCell("priv-1", "alice", "hello")))
	require.NotNil(t, mem.GetItem("priv-1"))

	// Re-sync a distinct-but-identical cell object: the mirror reports no change.
	assert.False(t, mem.MirrorCell(privateCell("priv-1", "alice", "hello")),
		"an unchanged reconcile must not change the store")
}

// TestSyncCell_PrivateEmbeddingOnlyDiffIgnored: the reconciler's embedding
// self-write/clear must not read back as state drift on the in-memory mirror.
func TestSyncCell_PrivateEmbeddingOnlyDiffIgnored(t *testing.T) {
	mem := NewMemoryStore()
	s := NewCellSyncer(fake.NewClientBuilder().Build(), mem, zerolog.Nop())
	ctx := context.Background()

	require.NoError(t, s.SyncCell(ctx, privateCell("priv-2", "alice", "hello")))

	withEmbedding := privateCell("priv-2", "alice", "hello")
	withEmbedding.Spec.Embedding = "some-stale-vector"
	assert.False(t, mem.MirrorCell(withEmbedding), "an embedding-only CR diff must not change the store")
}

// TestSyncCell_PrivateContentChangeApplies: a genuine content change (kubectl
// edit on a private cell) must still land in the in-memory mirror.
func TestSyncCell_PrivateContentChangeApplies(t *testing.T) {
	mem := NewMemoryStore()
	s := NewCellSyncer(fake.NewClientBuilder().Build(), mem, zerolog.Nop())
	ctx := context.Background()

	require.NoError(t, s.SyncCell(ctx, privateCell("priv-3", "alice", "hello")))
	require.NoError(t, s.SyncCell(ctx, privateCell("priv-3", "alice", "edited externally")))

	got := mem.GetItem("priv-3")
	require.NotNil(t, got)
	assert.Equal(t, "edited externally", got.Content)
}

// ── provenance and retirement (ADR-053) ──────────────────────────────────────

func TestPersist_WritesOriginAndEvidenceAndKeepsThemWhenTheItemHasNone(t *testing.T) {
	syncer, _ := newTrackedCellSyncer(t)
	ctx := context.Background()
	name := cellResourceName("fact-1")
	read := func() mcell.MemoryCell {
		var c mcell.MemoryCell
		require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: name}, &c))
		return c
	}

	it := &MemoryItem{
		ID: "fact-1", Kind: "stable_fact", Scope: "public", Title: "T", Content: "v1", Confidence: "high",
		Origin: OriginHuman, EvidenceRefs: []string{"event-1"},
	}
	require.NoError(t, syncer.Persist(ctx, it, "ns"))
	got := read()
	assert.Equal(t, OriginHuman, got.Spec.Origin)
	assert.Equal(t, []string{"event-1"}, got.Spec.EvidenceRefs)

	// An item built without them (an older writer) must not blank what is stored.
	bare := &MemoryItem{ID: "fact-1", Kind: "stable_fact", Scope: "public", Title: "T", Content: "v2", Confidence: "high"}
	require.NoError(t, syncer.Persist(ctx, bare, "ns"))
	got = read()
	assert.Equal(t, "v2", got.Spec.Content)
	assert.Equal(t, OriginHuman, got.Spec.Origin)
	assert.Equal(t, []string{"event-1"}, got.Spec.EvidenceRefs)
}

func TestPersist_KeepsAStaleStatusInsteadOfReactivating(t *testing.T) {
	syncer, _ := newTrackedCellSyncer(t)
	ctx := context.Background()
	it := &MemoryItem{ID: "fact-2", Kind: "stable_fact", Scope: "public", Title: "T", Content: "v1", Confidence: "high"}
	require.NoError(t, syncer.Persist(ctx, it, "ns"))

	retired := *it
	retired.Status = "stale"
	require.NoError(t, syncer.Persist(ctx, &retired, "ns"))

	var got mcell.MemoryCell
	require.NoError(t, syncer.Get(ctx, k8stypes.NamespacedName{Namespace: "ns", Name: cellResourceName("fact-2")}, &got))
	assert.Equal(t, "stale", got.Spec.Status)

	indexed := *it
	indexed.Status = "indexed" // the in-process status of a live, embedded item
	assert.Equal(t, "active", cellStatus(indexed.Status))
	assert.Equal(t, "superseded", cellStatus("superseded"))
}

func TestCellToItem_ReadsProvenance(t *testing.T) {
	cell := &mcell.MemoryCell{Spec: mcell.MemoryCellSpec{ID: "a", EvidenceRefs: []string{"e"}, Supersedes: []string{"s"}}}
	it := viewOf(itemOfCell(cell))
	assert.Equal(t, OriginDerived, effectiveOrigin(it.Origin), "a cell stored without an origin reads as derived")
	assert.Equal(t, []string{"e"}, it.EvidenceRefs)
	assert.Equal(t, []string{"s"}, it.Supersedes)
	cell.Spec.Origin = OriginVerified
	assert.Equal(t, OriginVerified, viewOf(itemOfCell(cell)).Origin)
}
