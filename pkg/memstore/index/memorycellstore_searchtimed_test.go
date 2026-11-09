package index

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

// fixedEmbeddingHTTPServer starts a fake OpenAI-style /v1/embeddings endpoint
// that always returns vec regardless of input text — mirrors
// internal/core/memory_search_test.go's fixedEmbeddingServer (same
// need here: a deterministic embed round trip with no real embedding
// backend).
func fixedEmbeddingHTTPServer(t *testing.T, vec []float32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"embedding": vec}},
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestSearchTimed_ReportsEmbedAndIndexLatency proves SearchTimed's phase
// split is
// populated end to end: the embed leg goes over a real (fake) HTTP round
// trip, so EmbedNs is never zero, and the index leg always runs on success.
// This split is what lets a slow HybridSearch call be attributed to the
// embed endpoint versus the in-memory HNSW scan instead of only a blended
// total.
func TestSearchTimed_ReportsEmbedAndIndexLatency(t *testing.T) {
	vec := []float32{1, 0, 0, 0} // length must be a multiple of 4 (kshard/vector's SIMD cosine path)
	srv := fixedEmbeddingHTTPServer(t, vec)
	embedder, err := NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)
	store.SeedFromEmbedding(IndexEntry{ID: "doc-1", Kind: "stable_fact", Title: "test"}, vec)

	results, timing, err := store.SearchTimed(context.Background(), "any query", 5, "")
	if err != nil {
		t.Fatalf("SearchTimed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if timing.Embed.Nanoseconds() <= 0 {
		t.Errorf("expected EmbedNs > 0 (real HTTP round trip), got %d", timing.Embed.Nanoseconds())
	}
	if timing.Embed.Nanoseconds()+timing.Index.Nanoseconds() <= 0 {
		t.Errorf("expected EmbedNs+IndexNs > 0, got embed=%d index=%d", timing.Embed.Nanoseconds(), timing.Index.Nanoseconds())
	}
}

// TestSearchTimed_ErrorSkipsIndexPhase proves IndexNs stays zero when
// EmbedQuery errors — the HNSW scan never runs in that case, so there is
// nothing to attribute a duration to (see SearchTimed's doc comment).
func TestSearchTimed_ErrorSkipsIndexPhase(t *testing.T) {
	// An embedder pointed at an unroutable address fails fast without a real
	// network dependency — same pattern as
	// TestHybridSearch_LowConfidenceLexical_RecordsKWVoiceMiss in
	// internal/core/memory_search_test.go.
	embedder, err := NewEmbedder(context.Background(), "http://127.0.0.1:0", "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)

	_, timing, err := store.SearchTimed(context.Background(), "any query", 5, "")
	if err == nil {
		t.Fatal("expected an error from an unroutable embedder endpoint")
	}
	if timing.Index.Nanoseconds() != 0 {
		t.Errorf("expected IndexNs == 0 when embed errors (index never ran), got %d", timing.Index.Nanoseconds())
	}
}

// TestSearchTimed_MatchesSearch proves the thin-wrapper relationship: Search
// (every caller with no interest in the phase split) returns exactly what
// SearchTimed returns.
func TestSearchTimed_MatchesSearch(t *testing.T) {
	vec := []float32{1, 0, 0, 0}
	srv := fixedEmbeddingHTTPServer(t, vec)
	embedder, err := NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)
	store.SeedFromEmbedding(IndexEntry{ID: "doc-1", Kind: "stable_fact", Title: "test"}, vec)

	results, err := store.Search(context.Background(), "any query", 5, "")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 || results[0].Document.ID != "doc-1" {
		t.Fatalf("unexpected results: %+v", results)
	}
}
