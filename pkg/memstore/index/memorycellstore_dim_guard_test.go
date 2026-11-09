package index

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestSeedFromEmbedding_EstablishesDimFromFirstVector: a nil-embedder store
// (no configured dimension) accepts its first vector and locks to its
// length; a second vector of the same length still seeds.
func TestSeedFromEmbedding_EstablishesDimFromFirstVector(t *testing.T) {
	store := newRawStore(nil)

	if !store.SeedFromEmbedding(IndexEntry{ID: "a"}, []float32{1, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatal("first vector into an empty store must be accepted")
	}
	if !store.SeedFromEmbedding(IndexEntry{ID: "b"}, []float32{0, 1, 0, 0, 0, 0, 0, 0}) {
		t.Fatal("a second vector matching the established dimension must be accepted")
	}
	if got := store.CollectionSize(); got != 2 {
		t.Fatalf("CollectionSize = %d, want 2", got)
	}
}

// TestSeedFromEmbedding_RejectsDimensionMismatch: a nil-embedder store still
// rejects a later vector whose length disagrees with the one already
// established, and stays usable for the dimension it holds.
func TestSeedFromEmbedding_RejectsDimensionMismatch(t *testing.T) {
	store := newRawStore(nil)

	wrongDims := make([]float32, 768)
	wrongDims[0] = 1
	if !store.SeedFromEmbedding(IndexEntry{ID: "legacy-note"}, wrongDims) {
		t.Fatal("first vector must be accepted and establishes the dimension")
	}

	rightDims := make([]float32, 256)
	rightDims[0] = 1
	if store.SeedFromEmbedding(IndexEntry{ID: "fresh-event"}, rightDims) {
		t.Fatal("a 256-dim vector must be rejected once the dimension is established at 768")
	}
	if got := store.CollectionSize(); got != 1 {
		t.Fatalf("CollectionSize = %d, want 1 (the rejected vector must not enter the collection)", got)
	}

	if !store.SeedFromEmbedding(IndexEntry{ID: "legacy-note-2"}, wrongDims) {
		t.Fatal("a second 768-dim vector must still be accepted after a rejected insert")
	}
}

// TestMemoryCellStore_ConfiguredDimsProtectsBulkSeed is the
// regression test: a stale wrong-dimension cell seeded FIRST (bulk-seed list
// order is arbitrary) must not lock the index to the wrong dimension when
// the embedder's own configured output dimension is known. Correct cells
// seed normally despite arriving after the bad one, and a live query still
// finds them.
func TestMemoryCellStore_ConfiguredDimsProtectsBulkSeed(t *testing.T) {
	ctx := context.Background()
	srv := fixedEmbeddingHTTPServer(t, []float32{1, 0, 0, 0}) // query embed: 4 dims
	embedder, err := NewEmbedder(ctx, srv.URL, "test-model", "", "", 4, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)

	stale := make([]float32, 768)
	stale[0] = 1
	if store.SeedFromEmbedding(IndexEntry{ID: "stale-cell"}, stale) {
		t.Fatal("a 768-dim cell must be rejected: the configured embedder dimension (4) is authoritative from construction")
	}

	if !store.SeedFromEmbedding(IndexEntry{ID: "good-1", Disposition: "signal"}, []float32{1, 0, 0, 0}) {
		t.Fatal("a 4-dim cell must be accepted")
	}
	if !store.SeedFromEmbedding(IndexEntry{ID: "good-2", Disposition: "signal"}, []float32{0, 1, 0, 0}) {
		t.Fatal("a second 4-dim cell must be accepted")
	}
	if got := store.CollectionSize(); got != 2 {
		t.Fatalf("CollectionSize = %d, want 2 (the stale cell must never enter the collection)", got)
	}

	results, err := store.Search(ctx, "any query", 5, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %+v", len(results), results)
	}
}

// statefulEmbeddingServer returns vecs[0] on the first request and vecs[1]
// on every request after, simulating an embedder whose output shape
// changes mid-process.
func statefulEmbeddingServer(t *testing.T, vecs ...[]float32) *httptest.Server {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		vec := vecs[calls]
		if calls < len(vecs)-1 {
			calls++
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"embedding": vec}},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestIndex_RejectsDimensionMismatch: once Index has established the
// dimension from the embedder's own output, a later embed of a different
// length is rejected, not inserted.
func TestIndex_RejectsDimensionMismatch(t *testing.T) {
	ctx := context.Background()
	srv := statefulEmbeddingServer(t, []float32{1, 0, 0, 0}, []float32{1, 0, 0, 0, 0, 0, 0, 0})
	embedder, err := NewEmbedder(ctx, srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)

	if _, err := store.Index(ctx, IndexEntry{ID: "first", Kind: "note", Title: "t", Content: "c"}); err != nil {
		t.Fatalf("first Index: %v", err)
	}
	if _, err := store.Index(ctx, IndexEntry{ID: "second", Kind: "note", Title: "t2", Content: "c2"}); err == nil {
		t.Fatal("Index must reject an embedding whose dimension disagrees with the established one")
	}
	if got := store.CollectionSize(); got != 1 {
		t.Fatalf("CollectionSize = %d, want 1 (the rejected embed must not enter the collection)", got)
	}
}

// TestSearchByEmbedding_RejectsDimensionMismatchQuery: a caller-supplied
// query vector of the wrong length must not reach fogfish/hnsw. It is a
// clean miss, not a panic.
func TestSearchByEmbedding_RejectsDimensionMismatchQuery(t *testing.T) {
	store := newRawStore(nil)
	established := make([]float32, 4)
	established[0] = 1
	store.SeedFromEmbedding(IndexEntry{ID: "seeded", Disposition: "signal"}, established)

	mismatched := make([]float32, 8)
	mismatched[0] = 1

	hit, err := store.SearchByEmbedding(context.Background(), "", mismatched, time.Time{}, 0, "")
	if err != nil {
		t.Fatalf("SearchByEmbedding must not error on a dimension mismatch, got: %v", err)
	}
	if hit != nil {
		t.Fatalf("expected no hit for a mismatched-dimension query, got %+v", hit)
	}
}

// TestSearch_RejectsDimensionMismatchQuery: once the index dimension is
// established, a live query embed of a different length (e.g. a provider
// swap mid-process) is rejected, not passed into fogfish/hnsw.
func TestSearch_RejectsDimensionMismatchQuery(t *testing.T) {
	ctx := context.Background()
	srv := statefulEmbeddingServer(t, []float32{1, 0, 0, 0}, []float32{1, 0, 0, 0, 0, 0, 0, 0})
	embedder, err := NewEmbedder(ctx, srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)

	if _, err := store.Index(ctx, IndexEntry{ID: "doc-1", Kind: "stable_fact", Title: "test"}); err != nil {
		t.Fatalf("Index: %v", err)
	}

	results, err := store.Search(ctx, "any query", 5, "")
	if err != nil {
		t.Fatalf("Search must not error on a dimension-mismatched query embedding, got: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no results for a mismatched-dimension query, got %+v", results)
	}
}
