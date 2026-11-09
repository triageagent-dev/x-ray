package index

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// textGateway is an embeddings server that answers each input text with a fixed
// vector, so a text query lands where the test says it does.
func textGateway(t *testing.T, vecs map[string][]float32) *Embedder {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Input string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		v, ok := vecs[req.Input]
		if !ok {
			http.Error(w, "unknown text "+req.Input, http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": v}}})
	}))
	t.Cleanup(srv.Close)
	e, err := NewEmbedder(context.Background(), srv.URL, "m", "", "", 0, 0, zerolog.Nop())
	require.NoError(t, err)
	return e
}

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// seedEvent seeds an entry that carries the caller-supplied metadata.
func seedEvent(t *testing.T, s *CellStore, e IndexEntry, v []float32) {
	t.Helper()
	require.True(t, s.SeedFromEmbedding(e, v))
}

func TestSearchByEmbedding_Guards(t *testing.T) {
	s := NewCellStore(nil, zerolog.Nop())
	// The graph drops a node whose vector equals another's, so every cell gets
	// its own, all within a hair of the query.
	same := vec(1, 0, 0)
	near := vec(0.8, 0.6, 0) // cosine 0.8 to same
	at := func(i float32) []float32 { return vec(1, 0.001*i, 0) }
	seedEvent(t, s, IndexEntry{ID: "ok", Kind: "event", Disposition: "kept", Partition: "p1", Source: "src-a", Timestamp: t0}, at(1))
	seedEvent(t, s, IndexEntry{ID: "no-outcome", Kind: "event", Partition: "p1", Source: "src-a", Timestamp: t0}, at(2))
	seedEvent(t, s, IndexEntry{ID: "other-partition", Kind: "event", Disposition: "kept", Partition: "p2", Source: "src-a", Timestamp: t0}, at(3))
	seedEvent(t, s, IndexEntry{ID: "other-source", Kind: "event", Disposition: "kept", Partition: "p1", Source: "src-b", Timestamp: t0}, at(4))
	seedEvent(t, s, IndexEntry{ID: "old", Kind: "event", Disposition: "kept", Partition: "p1", Source: "src-a", Timestamp: t0.Add(-48 * time.Hour)}, at(5))
	seedEvent(t, s, IndexEntry{ID: "global", Kind: "event", Disposition: "kept", Source: "src-a", Timestamp: t0}, near)

	ctx := context.Background()
	got, err := s.SearchByEmbedding(ctx, "p1", same, t0.Add(-time.Hour), 0.9, "src-a")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "ok", got.Document.ID, "needs an outcome, the partition, the source and the window")
	assert.InDelta(t, 1.0, got.Score, 1e-4)

	// A cell with no partition is visible to every partition; the others are not.
	got, err = s.SearchByEmbedding(ctx, "p1", near, t0.Add(-time.Hour), 0.75, "src-a")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Contains(t, []string{"ok", "global"}, got.Document.ID)
	assert.NotEqual(t, "other-partition", got.Document.ID)

	// Cosine, not the compressed 1-dist/2 scale: a cosine of 0.8 is below 0.9.
	got, err = s.SearchByEmbedding(ctx, "p1", near, t0.Add(-time.Hour), 0.9, "src-a")
	require.NoError(t, err)
	require.NotNil(t, got, "the exact neighbour clears 0.9")
	assert.Equal(t, "global", got.Document.ID)
	got, err = s.SearchByEmbedding(ctx, "nobody", vec(0, 0, 1), t0.Add(-time.Hour), 0.9, "src-a")
	require.NoError(t, err)
	assert.Nil(t, got, "nothing is that close")

	// An empty source switches the source guard off.
	got, err = s.SearchByEmbedding(ctx, "p1", same, t0.Add(-time.Hour), 0.9, "")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.NotEqual(t, "no-outcome", got.Document.ID)
	assert.NotEqual(t, "other-partition", got.Document.ID)
}

func TestSearchByEmbedding_WrongDimensionFindsNothing(t *testing.T) {
	s := NewCellStore(nil, zerolog.Nop())
	seedEvent(t, s, IndexEntry{ID: "a", Disposition: "kept"}, vec(1, 0, 0, 0))
	got, err := s.SearchByEmbedding(context.Background(), "", []float32{1, 0}, time.Time{}, 0, "")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestGetRecentBySubject(t *testing.T) {
	s := NewCellStore(nil, zerolog.Nop())
	v := vec(1, 0, 0)
	for i, id := range []string{"e1", "e2", "e3", "e4"} {
		seedEvent(t, s, IndexEntry{ID: id, Kind: "event", Service: "svc", Disposition: "kept", Partition: "p1",
			Timestamp: t0.Add(time.Duration(i) * time.Hour)}, v)
	}
	seedEvent(t, s, IndexEntry{ID: "other-service", Service: "other", Disposition: "kept", Timestamp: t0}, v)
	seedEvent(t, s, IndexEntry{ID: "no-outcome", Service: "svc", Timestamp: t0}, v)
	seedEvent(t, s, IndexEntry{ID: "other-partition", Service: "svc", Disposition: "kept", Partition: "p2", Timestamp: t0}, v)
	seedEvent(t, s, IndexEntry{ID: "unscoped", Service: "svc", Disposition: "kept", Timestamp: t0.Add(30 * time.Minute)}, v)

	ctx := context.Background()
	got, err := s.GetRecentBySubject(ctx, "p1", "svc", 0, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, []string{"e1", "unscoped", "e2", "e3", "e4"}, ids(got), "oldest first; the unscoped cell is visible, the other partition's is not")

	got, err = s.GetRecentBySubject(ctx, "p1", "svc", 2, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, []string{"e3", "e4"}, ids(got), "topN keeps the newest")

	got, err = s.GetRecentBySubject(ctx, "p1", "svc", 0, t0.Add(90*time.Minute))
	require.NoError(t, err)
	assert.Equal(t, []string{"e3", "e4"}, ids(got), "since drops older cells")
}

func ids(docs []IndexEntry) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = d.ID
	}
	return out
}

func TestSearchSince_FiltersKindAndWindow(t *testing.T) {
	emb := textGateway(t, map[string][]float32{"find it": vec(1, 0, 0)})
	s := NewCellStore(emb, zerolog.Nop())
	v := vec(1, 0, 0)
	seedEvent(t, s, IndexEntry{ID: "new", Kind: "event", Timestamp: t0}, v)
	seedEvent(t, s, IndexEntry{ID: "old", Kind: "event", Timestamp: t0.Add(-72 * time.Hour)}, v)
	seedEvent(t, s, IndexEntry{ID: "fact", Kind: "stable_fact", Timestamp: t0}, v)

	got, err := s.SearchSince(context.Background(), "find it", 5, "event", t0.Add(-time.Hour))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "new", got[0].Document.ID)

	got, err = s.SearchSince(context.Background(), "find it", 5, "event", time.Time{})
	require.NoError(t, err)
	assert.Len(t, got, 2, "a zero since keeps the whole kind")
}

func TestSearchExhaustiveTimed_KeepPrefilterAndKind(t *testing.T) {
	emb := textGateway(t, map[string][]float32{"q": vec(1, 0, 0)})
	s := NewCellStore(emb, zerolog.Nop())
	seedEvent(t, s, IndexEntry{ID: "a", Kind: "stable_fact", Tags: []string{"x"}}, vec(1, 0, 0))
	seedEvent(t, s, IndexEntry{ID: "b", Kind: "stable_fact", Tags: []string{"y"}, Subject: "s1"}, vec(0.9, 0.1, 0))
	seedEvent(t, s, IndexEntry{ID: "c", Kind: "note"}, vec(1, 0, 0))

	keepX := func(e IndexEntry) bool { return len(e.Tags) > 0 && e.Tags[0] == "x" }
	got, timing, err := s.SearchExhaustiveTimed(context.Background(), "q", "", keepX)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "a", got[0].Document.ID)
	assert.Greater(t, timing.Embed+timing.Index, time.Duration(-1))

	got, _, err = s.SearchExhaustiveTimed(context.Background(), "q", "stable_fact", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, []string{got[0].Document.ID, got[1].Document.ID}, "every subject's cells of that kind, best first")
}

func TestSearchTimed_ReportsThePhaseSplit(t *testing.T) {
	emb := textGateway(t, map[string][]float32{"q": vec(1, 0, 0)})
	s := NewCellStore(emb, zerolog.Nop())
	s.SetScoreGap(0)
	seedEvent(t, s, IndexEntry{ID: "a", Kind: "note"}, vec(1, 0, 0))
	got, timing, err := s.SearchTimed(context.Background(), "q", 3, "")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.GreaterOrEqual(t, timing.Embed, time.Duration(0))
}

func TestQueryEmbedCache_CounterAndEntries(t *testing.T) {
	emb := textGateway(t, map[string][]float32{"q": vec(1, 0, 0)})
	s := NewCellStore(emb, zerolog.Nop())
	var hits, misses int
	s.SetQueryEmbedCounter(func(hit bool) {
		if hit {
			hits++
		} else {
			misses++
		}
	})
	seedEvent(t, s, IndexEntry{ID: "a", Kind: "note"}, vec(1, 0, 0))
	for range 3 {
		_, err := s.Search(context.Background(), "q", 3, "")
		require.NoError(t, err)
	}
	assert.Equal(t, 1, misses)
	assert.Equal(t, 2, hits)
	assert.Equal(t, 1, s.QueryEmbedCacheEntries())
	assert.Equal(t, 0, NewCellStore(nil, zerolog.Nop()).QueryEmbedCacheEntries())
}
