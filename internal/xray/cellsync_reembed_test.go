package xray

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore/cellsync"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore/index"
)

// ── stored vectors at start, re-embedding of a changed text ──────────────────
//
// A cell's vector is stored on the cell with the fingerprint of the text it was
// made from. At start and on every reconcile a cell whose fingerprint matches is
// seeded from its stored vector, with no embedder call and no write; any other
// cell is embedded and the vector and fingerprint are written back together.

// textVec is a distinct vector per text: the graph collapses identical vectors.
func textVec(text string) []float32 {
	sum := sha256.Sum256([]byte(text))
	v := make([]float32, 8)
	for i := range v {
		v[i] = float32(sum[i]) + 1
	}
	return v
}

// newSemanticStore is a store over an embeddings gateway that answers each text
// with textVec and counts the requests that reach it.
func newSemanticStore(t *testing.T) (*MemoryStore, *atomic.Int64) {
	return newGatewayStore(t, &atomic.Bool{})
}

// newGatewayStore is newSemanticStore whose gateway fails while down is set.
func newGatewayStore(t *testing.T, down *atomic.Bool) (*MemoryStore, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if down.Load() {
			http.Error(w, "embedder down", http.StatusServiceUnavailable)
			return
		}
		var req struct{ Input string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": textVec(req.Input)}}})
	}))
	t.Cleanup(srv.Close)
	emb, err := index.NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 8, 8192, zerolog.Nop())
	require.NoError(t, err)
	return NewMemoryStoreWithEmbedder(emb, zerolog.Nop()), &calls
}

// newSemanticCellSyncer is a CellSyncer over mem and a counting fake client
// that holds cells and, as the CRD declares, a status subresource: an update
// of a cell keeps and returns its stored status.
func newSemanticCellSyncer(t *testing.T, mem *MemoryStore, cells ...client.Object) (*CellSyncer, *persistCounters) {
	t.Helper()
	c, counters := trackedClient(t, fake.NewClientBuilder().WithObjects(cells...).WithStatusSubresource(&mcell.MemoryCell{}))
	return NewCellSyncer(c, mem, zerolog.Nop()), counters
}

func publicCell(id, content string) *mcell.MemoryCell {
	c := &mcell.MemoryCell{}
	c.Name = cellResourceName(id)
	c.Namespace = "ns"
	c.Spec = mcell.MemoryCellSpec{ID: id, Kind: "stable_fact", Scope: "public", Subject: "xray", Title: "T", Content: content, Status: "active"}
	return c
}

// withStoredVector gives c a stored vector and an indexed status, with the
// fingerprint of its current text when current is set and none otherwise (a
// cell written before fingerprints).
func withStoredVector(mem *MemoryStore, c *mcell.MemoryCell, vec []float32, current bool) *mcell.MemoryCell {
	c.Spec.Embedding = index.EncodeEmbedding(vec)
	c.Status.Indexed = true
	if current {
		c.Annotations = map[string]string{index.FingerprintAnnotation: mem.cells.EmbeddingFingerprint(entryOfCell(c), c.Spec.Embedding)}
	}
	return c
}

func getCell(t *testing.T, s *CellSyncer, name string) *mcell.MemoryCell {
	t.Helper()
	c := &mcell.MemoryCell{}
	require.NoError(t, s.Get(context.Background(), k8stypes.NamespacedName{Namespace: "ns", Name: name}, c))
	return c
}

func reconcileCell(t *testing.T, s *CellSyncer, name string) {
	t.Helper()
	_, err := s.Reconcile(context.Background(), ctrl.Request{NamespacedName: k8stypes.NamespacedName{Namespace: "ns", Name: name}})
	require.NoError(t, err)
}

// assertCurrent asserts that c carries the fingerprint of its text and vector.
func assertCurrent(t *testing.T, mem *MemoryStore, c *mcell.MemoryCell) {
	t.Helper()
	assert.Equal(t, mem.cells.EmbeddingFingerprint(entryOfCell(c), c.Spec.Embedding), c.Annotations[index.FingerprintAnnotation], c.Name)
}

// inIndex reports whether id is in the vector index, without an embed call.
func inIndex(mem *MemoryStore, id string) bool {
	_, ok := mem.cells.Neighborhood(id, 1, 1, nil)
	return ok
}

func TestSeedAll_LoadsAStoredVectorWhoseFingerprintMatchesWithNoEmbedAndNoWrite(t *testing.T) {
	ctx := context.Background()
	mem, calls := newSemanticStore(t)
	stored := withStoredVector(mem, publicCell("stored", "a fact embedded by an earlier run"), textVec("q"), true)
	fresh := publicCell("fresh", "a fact never embedded")
	retired := withStoredVector(mem, publicCell("retired", "a refuted fact"), textVec("r"), true)
	retired.Spec.Status = "stale"
	s, counters := newSemanticCellSyncer(t, mem, stored, fresh, retired)

	require.NoError(t, s.SeedAll(ctx, "ns"))
	assert.EqualValues(t, 1, calls.Load(), "only the cell without a vector is embedded")
	assert.Zero(t, mem.embedder.Metrics().CacheHits)
	assert.EqualValues(t, 1, counters.updates.Load(), "only the new vector is written")
	assert.EqualValues(t, 1, counters.statusUpdates.Load())
	assert.Equal(t, stored.ResourceVersion, getCell(t, s, stored.Name).ResourceVersion, "the cell with a current vector is not written")
	assert.True(t, inIndex(mem, "stored"), "the stored vector is in the index after a start")
	assert.True(t, inIndex(mem, "fresh"))
	assert.False(t, inIndex(mem, "retired"), "a retired cell kept as the audit trail stays out")

	writes := counters.updates.Load() + counters.statusUpdates.Load()
	require.NoError(t, s.SeedAll(ctx, "ns"))
	assert.EqualValues(t, 1, calls.Load(), "a second start embeds nothing")
	assert.Equal(t, writes, counters.updates.Load()+counters.statusUpdates.Load(), "and writes nothing")

	res, err := mem.cells.Search(ctx, "q", 5, "")
	require.NoError(t, err)
	require.NotEmpty(t, res)
	assert.Equal(t, "stored", res[0].Document.ID, "semantic search finds the cell by its stored vector")
}

func TestReconcile_ReEmbedsAStoredVectorWhoseTextChanged(t *testing.T) {
	ctx := context.Background()
	mem, calls := newSemanticStore(t)
	c := withStoredVector(mem, publicCell("fact", "the first text"), textVec("first"), true)
	s, counters := newSemanticCellSyncer(t, mem, c)
	require.NoError(t, s.SeedAll(ctx, "ns"))
	require.Zero(t, calls.Load())

	// The text changes outside the server (a GitOps apply, kubectl edit): the
	// stored vector and its fingerprint stay as they were.
	edited := getCell(t, s, c.Name)
	edited.Spec.Content = "the second text"
	require.NoError(t, s.Update(ctx, edited))
	updates := counters.updates.Load()

	reconcileCell(t, s, c.Name)
	assert.EqualValues(t, 1, calls.Load(), "the new text is embedded")
	assert.Equal(t, updates+1, counters.updates.Load(), "vector and fingerprint are written in one update")
	got := getCell(t, s, c.Name)
	assert.NotEqual(t, c.Spec.Embedding, got.Spec.Embedding, "the vector of the old text is replaced")
	assertCurrent(t, mem, got)
	assert.True(t, got.Status.Indexed)

	writes := counters.updates.Load() + counters.statusUpdates.Load()
	reconcileCell(t, s, c.Name)
	assert.EqualValues(t, 1, calls.Load(), "the next reconcile seeds the new vector")
	assert.Zero(t, mem.embedder.Metrics().CacheHits)
	assert.Equal(t, writes, counters.updates.Load()+counters.statusUpdates.Load(), "and writes nothing")
}

func TestSeedAll_AStoredVectorWithoutAFingerprintIsReEmbeddedOnce(t *testing.T) {
	ctx := context.Background()
	mem, calls := newSemanticStore(t)
	legacy := withStoredVector(mem, publicCell("legacy", "written by an earlier version"), textVec("legacy"), false)
	s, counters := newSemanticCellSyncer(t, mem, legacy)

	require.NoError(t, s.SeedAll(ctx, "ns"))
	assert.EqualValues(t, 1, calls.Load(), "a vector of unknown origin is embedded again")
	assert.EqualValues(t, 1, counters.updates.Load())
	got := getCell(t, s, legacy.Name)
	assertCurrent(t, mem, got)
	assert.True(t, inIndex(mem, "legacy"))

	writes := counters.updates.Load() + counters.statusUpdates.Load()
	require.NoError(t, s.SeedAll(ctx, "ns"))
	reconcileCell(t, s, legacy.Name)
	assert.EqualValues(t, 1, calls.Load(), "once: a second start and a reconcile embed nothing")
	assert.Zero(t, mem.embedder.Metrics().CacheHits, "not even from the embedder's cache")
	assert.Equal(t, writes, counters.updates.Load()+counters.statusUpdates.Load(), "and write nothing")
}

func TestReconcile_AnUnchangedIndexedCellWritesNothing(t *testing.T) {
	mem, calls := newSemanticStore(t)
	c := withStoredVector(mem, publicCell("steady", "an unchanged fact"), textVec("steady"), true)
	s, counters := newSemanticCellSyncer(t, mem, c)

	for range 3 {
		reconcileCell(t, s, c.Name)
	}
	assert.Zero(t, counters.updates.Load(), "no update of an unchanged cell: no write loop")
	assert.Zero(t, counters.statusUpdates.Load())
	assert.Zero(t, calls.Load())
	assert.Zero(t, mem.embedder.Metrics().CacheHits)
	assert.Equal(t, c.ResourceVersion, getCell(t, s, c.Name).ResourceVersion)
	assert.True(t, inIndex(mem, "steady"), "every reconcile seeds the cell")
}

func TestPersist_ChangedTextDropsTheStoredEmbeddingAndItsFingerprint(t *testing.T) {
	ctx := context.Background()
	mem, calls := newSemanticStore(t)
	s, counters := newSemanticCellSyncer(t, mem)
	it := &MemoryItem{ID: "note-reembed", Kind: "note", Scope: "public", Title: "Title", Content: "v1", Confidence: "medium"}
	name := cellResourceName(it.ID)
	require.NoError(t, s.Persist(ctx, it, "ns"))
	reconcileCell(t, s, name)
	embedded := getCell(t, s, name)
	require.NotEmpty(t, embedded.Spec.Embedding)
	require.NotEmpty(t, embedded.Annotations[index.FingerprintAnnotation])

	// A change the embedder is not sent (tags) keeps the vector and its fingerprint.
	tagged := *it
	tagged.Tags = []string{"t"}
	require.NoError(t, s.Persist(ctx, &tagged, "ns"))
	got := getCell(t, s, name)
	assert.Equal(t, embedded.Spec.Embedding, got.Spec.Embedding)
	assert.Equal(t, embedded.Annotations[index.FingerprintAnnotation], got.Annotations[index.FingerprintAnnotation])

	writes := counters.updates.Load() + counters.statusUpdates.Load()
	require.NoError(t, s.Persist(ctx, &tagged, "ns"))
	assert.Equal(t, writes, counters.updates.Load()+counters.statusUpdates.Load(), "an unchanged re-Persist stays a no-op")

	changed := tagged
	changed.Content = "v2"
	require.NoError(t, s.Persist(ctx, &changed, "ns"))
	got = getCell(t, s, name)
	assert.Empty(t, got.Spec.Embedding, "the vector of the old text is dropped")
	assert.NotContains(t, got.Annotations, index.FingerprintAnnotation, "with its fingerprint")

	reconcileCell(t, s, name)
	got = getCell(t, s, name)
	assert.NotEmpty(t, got.Spec.Embedding, "the reconciler embeds the new text")
	assertCurrent(t, mem, got)
	assert.EqualValues(t, 2, calls.Load())
}

func TestSyncCell_APrivateCellLosesItsEmbeddingAndFingerprint(t *testing.T) {
	mem, _ := newSemanticStore(t)
	c := withStoredVector(mem, publicCell("secret", "turned private"), textVec("secret"), true)
	c.Spec.Scope = scopePrivate
	c.Spec.Tags = []string{"user:abc"}
	s, _ := newSemanticCellSyncer(t, mem, c)

	require.NoError(t, s.SyncCell(context.Background(), getCell(t, s, c.Name)))
	got := getCell(t, s, c.Name)
	assert.Empty(t, got.Spec.Embedding)
	assert.NotContains(t, got.Annotations, index.FingerprintAnnotation)
	assert.False(t, got.Status.Indexed)
	assert.False(t, inIndex(mem, "secret"))
}

func TestSeedAll_ConvergesWhenItRunsBesideTheFirstReconciles(t *testing.T) {
	ctx := context.Background()
	mem, calls := newSemanticStore(t)
	var cells []client.Object
	var names []string
	for i := range 30 {
		id := fmt.Sprintf("cell-%02d", i)
		c := publicCell(id, "fact number "+id)
		switch i % 3 {
		case 1:
			withStoredVector(mem, c, textVec(id), false) // written before fingerprints
		case 2:
			withStoredVector(mem, c, textVec(id), true) // current
		}
		cells, names = append(cells, c), append(names, c.Name)
	}
	s, counters := newSemanticCellSyncer(t, mem, cells...)

	// At start the backfill and the controller's first pass run side by side
	// over the same cells; one of two writes of a cell can lose on a conflict.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); assert.NoError(t, s.SeedAll(ctx, "ns")) }()
	go func() {
		defer wg.Done()
		for _, n := range names {
			_, err := s.Reconcile(ctx, ctrl.Request{NamespacedName: k8stypes.NamespacedName{Namespace: "ns", Name: n}})
			assert.NoError(t, err)
		}
	}()
	wg.Wait()

	writes, embeds, hits := counters.updates.Load()+counters.statusUpdates.Load(), calls.Load(), mem.embedder.Metrics().CacheHits
	for _, n := range names {
		reconcileCell(t, s, n)
	}
	assert.Equal(t, writes, counters.updates.Load()+counters.statusUpdates.Load(), "a further reconcile of every cell writes nothing")
	assert.Equal(t, embeds, calls.Load(), "and embeds nothing")
	assert.Equal(t, hits, mem.embedder.Metrics().CacheHits, "not even from the embedder's cache")
	for i, n := range names {
		got := getCell(t, s, n)
		assertCurrent(t, mem, got)
		assert.True(t, got.Status.Indexed, n)
		assert.True(t, inIndex(mem, fmt.Sprintf("cell-%02d", i)), n)
	}
}

func TestSyncCell_IndexesExactlyTheCellsMirrorCellKeepsSearchable(t *testing.T) {
	mem, calls := newSemanticStore(t)
	for _, status := range []string{"", "active", "indexed", "stale", "superseded", "draft"} {
		c := publicCell("probe", "x")
		c.Spec.Status = status
		assert.Equal(t, itemOfCell(c).IsActive(), cellsync.Decide(c, time.Now(), syncOptions()).Index,
			"Decide indexes what MirrorCell keeps in the vector index, status %q", status)
	}

	draft := withStoredVector(mem, publicCell("draft", "not yet active"), textVec("draft"), true)
	draft.Spec.Status = "draft"
	s, counters := newSemanticCellSyncer(t, mem, draft)
	for range 2 {
		reconcileCell(t, s, draft.Name)
	}
	assert.False(t, inIndex(mem, "draft"), "an inactive cell is not put back after the mirror dropped it")
	assert.Zero(t, counters.updates.Load()+counters.statusUpdates.Load())
	assert.Zero(t, calls.Load())
}

func TestSyncCell_ASystemConfigurationCellLosesItsEmbeddingAndFingerprint(t *testing.T) {
	mem, _ := newSemanticStore(t)
	c := withStoredVector(mem, publicCell("config", "voice settings"), textVec("config"), true)
	c.Spec.Kind = "system_configuration"
	s, _ := newSemanticCellSyncer(t, mem, c)

	require.NoError(t, s.SyncCell(context.Background(), getCell(t, s, c.Name)))
	got := getCell(t, s, c.Name)
	assert.Empty(t, got.Spec.Embedding)
	assert.NotContains(t, got.Annotations, index.FingerprintAnnotation)
	assert.False(t, inIndex(mem, "config"))
}

func TestSyncCell_AFailingEmbedderKeepsALegacyCellSearchableAndWritesNothing(t *testing.T) {
	var down atomic.Bool
	down.Store(true)
	mem, _ := newGatewayStore(t, &down)
	legacy := withStoredVector(mem, publicCell("legacy", "written by an earlier version"), textVec("legacy"), false)
	s, counters := newSemanticCellSyncer(t, mem, legacy)

	require.Error(t, s.SyncCell(context.Background(), getCell(t, s, legacy.Name)), "the reconcile retries")
	assert.True(t, inIndex(mem, "legacy"), "searchable by its stored vector meanwhile")
	assert.Zero(t, counters.updates.Load()+counters.statusUpdates.Load(), "nothing is written while the embedder is down")
	assert.NotContains(t, getCell(t, s, legacy.Name).Annotations, index.FingerprintAnnotation)

	down.Store(false)
	reconcileCell(t, s, legacy.Name)
	got := getCell(t, s, legacy.Name)
	assertCurrent(t, mem, got)
	assert.EqualValues(t, 1, counters.updates.Load(), "embedded and written once the embedder answers")
}

func TestSyncCell_AVectorReplacedUnderItsAnnotationIsReEmbedded(t *testing.T) {
	mem, calls := newSemanticStore(t)
	c := withStoredVector(mem, publicCell("swapped", "a fact"), textVec("first"), true)
	c.Spec.Embedding = index.EncodeEmbedding(textVec("written by someone else"))
	s, counters := newSemanticCellSyncer(t, mem, c)

	reconcileCell(t, s, c.Name)
	assert.EqualValues(t, 1, calls.Load())
	assert.EqualValues(t, 1, counters.updates.Load())
	assertCurrent(t, mem, getCell(t, s, c.Name))
}
