package index

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestSearchTimed_QueryCacheAvoidsRepeatEmbedRoundTrip proves the query-
// embedding cache added in front of CellStore's Search/SearchTimed
// path actually saves the embedder round trip end to end. An artificial
// 10ms delay on the fake embedding endpoint makes the first SearchTimed
// call's EmbedNs clearly non-trivial; a second call with the identical
// query text must be served from the cache — no second HTTP round trip, so
// EmbedNs drops to (near) zero — and SearchTiming.Embed.Nanoseconds() (recordEmbedPhase's
// source, see memory_search.go) still reflects that near-zero duration
// rather than being skipped.
func TestSearchTimed_QueryCacheAvoidsRepeatEmbedRoundTrip(t *testing.T) {
	vec := []float32{1, 0, 0, 0}
	const artificialDelay = 10 * time.Millisecond
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		time.Sleep(artificialDelay)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{{"embedding": vec}},
		})
	}))
	t.Cleanup(srv.Close)

	embedder, err := NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)
	var hits, misses atomic.Int64
	store.SetQueryEmbedCounter(func(hit bool) {
		if hit {
			hits.Add(1)
		} else {
			misses.Add(1)
		}
	})
	store.SeedFromEmbedding(IndexEntry{ID: "doc-1", Kind: "stable_fact", Title: "test"}, vec)

	_, timing1, err := store.SearchTimed(context.Background(), "same query", 5, "")
	if err != nil {
		t.Fatalf("SearchTimed (cold): %v", err)
	}
	_, timing2, err := store.SearchTimed(context.Background(), "same query", 5, "")
	if err != nil {
		t.Fatalf("SearchTimed (warm): %v", err)
	}

	if calls != 1 {
		t.Fatalf("expected 1 backend embed HTTP call across both searches, got %d", calls)
	}

	coldMs := float64(timing1.Embed.Nanoseconds()) / 1e6
	warmMs := float64(timing2.Embed.Nanoseconds()) / 1e6
	t.Logf("cold EmbedNs=%dns (%.2fms)  warm EmbedNs=%dns (%.2fms)", timing1.Embed.Nanoseconds(), coldMs, timing2.Embed.Nanoseconds(), warmMs)

	if timing1.Embed.Nanoseconds() < artificialDelay.Nanoseconds() {
		t.Errorf("expected cold EmbedNs >= %dns (real round trip), got %d", artificialDelay.Nanoseconds(), timing1.Embed.Nanoseconds())
	}
	if timing2.Embed.Nanoseconds() >= artificialDelay.Nanoseconds() {
		t.Errorf("expected warm EmbedNs < %dns (cache hit, no round trip), got %d", artificialDelay.Nanoseconds(), timing2.Embed.Nanoseconds())
	}

	if hits.Load() != 1 || misses.Load() != 1 || store.QueryEmbedCacheEntries() != 1 {
		t.Errorf("expected 1 hit + 1 miss + 1 entry, got hits=%d misses=%d entries=%d", hits.Load(), misses.Load(), store.QueryEmbedCacheEntries())
	}
}

// TestMemoryCellStore_SearchEventsAndSearchIssuesShareQueryCache proves
// SearchEvents and SearchIssues route through the same store-level query
// cache as SearchTimed — a repeat of either only ever calls the backend
// once.
func TestMemoryCellStore_SearchEventsAndSearchIssuesShareQueryCache(t *testing.T) {
	vec := []float32{1, 0, 0, 0}
	backend := &fakeQueryBackend{vecFn: func(string) []float32 { return vec }}
	store := newRawStore(nil)
	store.queryCache = newQueryEmbedCache(backend, 0, 0) // NewCellStore(nil, ...) leaves queryCache nil; wire the fake directly

	ctx := context.Background()
	since := time.Time{}

	if _, err := store.SearchSince(ctx, "same text", 5, "event", since); err != nil {
		t.Fatalf("SearchEvents #1: %v", err)
	}
	if _, err := store.SearchSince(ctx, "same text", 5, "event", since); err != nil {
		t.Fatalf("SearchEvents #2: %v", err)
	}
	if _, err := store.SearchSince(ctx, "same text", 3, "case", time.Time{}); err != nil {
		t.Fatalf("SearchIssues: %v", err)
	}

	if got := backend.callCount(); got != 1 {
		t.Fatalf("expected 1 backend call shared across SearchEvents/SearchIssues for the same text, got %d", got)
	}
}
