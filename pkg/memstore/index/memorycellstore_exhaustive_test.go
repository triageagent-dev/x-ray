package index

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

// TestSearchExhaustiveTimed_RankedOrder_NoTruncation proves
// SearchExhaustiveTimed returns EVERY qualifying entry, sorted best-first,
// with no topK-style truncation — the property a hybrid search's starvation
// fallback depends on:
// a SearchTimed top-k window can be starved by post-fetch filtering, so the
// fallback needs the full ranked population, not another truncated window.
func TestSearchExhaustiveTimed_RankedOrder_NoTruncation(t *testing.T) {
	vec := []float32{1, 0, 0, 0}
	srv := fixedEmbeddingHTTPServer(t, vec)
	embedder, err := NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)

	// Seed 20 entries at strictly decreasing similarity to the query axis, so
	// the expected rank order is unambiguous and easy to assert exactly.
	// query = {1,0,0,0}; entry i's vector leans away from axis 0 a little
	// more each time, so cos(query, entry_i) strictly decreases as i grows.
	const n = 20
	for i := 0; i < n; i++ {
		id := entryID(i)
		v := []float32{1, float32(i) * 0.05, 0, 0}
		store.SeedFromEmbedding(IndexEntry{ID: id, Kind: "stable_fact", Title: id}, v)
	}

	results, timing, err := store.SearchExhaustiveTimed(context.Background(), "any query", "", nil)
	if err != nil {
		t.Fatalf("SearchExhaustiveTimed: %v", err)
	}
	if len(results) != n {
		t.Fatalf("expected all %d entries with no truncation, got %d", n, len(results))
	}
	if timing.Embed.Nanoseconds() <= 0 {
		t.Errorf("expected EmbedNs > 0 (real HTTP round trip), got %d", timing.Embed.Nanoseconds())
	}
	for i := 1; i < len(results); i++ {
		if results[i-1].Score < results[i].Score {
			t.Fatalf("results not sorted best-first at index %d: %+v", i, results)
		}
	}
	if results[0].Document.ID != entryID(0) {
		t.Fatalf("expected entry-0 (nearest the query axis) ranked first, got %s", results[0].Document.ID)
	}
	if results[n-1].Document.ID != entryID(n-1) {
		t.Fatalf("expected entry-%d (farthest from the query axis) ranked last, got %s", n-1, results[n-1].Document.ID)
	}
}

// TestSearchExhaustiveTimed_KindFilter proves kindFilter restricts the
// exhaustive scan to one kind, exactly like searchByEmbedding's exact-scan
// branch — SearchExhaustiveTimed shares that scoring code (scanAllEntries).
func TestSearchExhaustiveTimed_KindFilter(t *testing.T) {
	vec := []float32{1, 0, 0, 0}
	srv := fixedEmbeddingHTTPServer(t, vec)
	embedder, err := NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)

	store.SeedFromEmbedding(IndexEntry{ID: "fact-1", Kind: "stable_fact"}, []float32{1, 0, 0, 0})
	store.SeedFromEmbedding(IndexEntry{ID: "fact-2", Kind: "stable_fact"}, []float32{1, 0.1, 0, 0})
	store.SeedFromEmbedding(IndexEntry{ID: "event-1", Kind: "event"}, []float32{1, 0.2, 0, 0})

	results, _, err := store.SearchExhaustiveTimed(context.Background(), "any query", "stable_fact", nil)
	if err != nil {
		t.Fatalf("SearchExhaustiveTimed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected exactly the 2 stable_fact entries, got %d: %+v", len(results), results)
	}
	for _, r := range results {
		if r.Document.Kind != "stable_fact" {
			t.Fatalf("kindFilter leaked a non-matching kind: %+v", r.Document)
		}
	}
}

// TestSearchExhaustiveTimed_KeepPreFilter proves the optional keep callback
// drops candidates before they are ranked/returned — the document-level
// pre-filter hybridSearch's exhaustive fallback uses to skip hydrating
// wrong-partition candidates (docVisibleToPartition, internal/core).
func TestSearchExhaustiveTimed_KeepPreFilter(t *testing.T) {
	vec := []float32{1, 0, 0, 0}
	srv := fixedEmbeddingHTTPServer(t, vec)
	embedder, err := NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewEmbedder: %v", err)
	}
	store := newRawStore(embedder)

	store.SeedFromEmbedding(IndexEntry{ID: "keep-me", Kind: "stable_fact", Tags: []string{"partition/partition-a"}}, []float32{1, 0, 0, 0})
	store.SeedFromEmbedding(IndexEntry{ID: "drop-me", Kind: "stable_fact", Tags: []string{"partition/partition-b"}}, []float32{1, 0.05, 0, 0})

	keep := func(doc IndexEntry) bool {
		for _, tag := range doc.Tags {
			if tag == "partition/partition-a" {
				return true
			}
		}
		return false
	}

	results, _, err := store.SearchExhaustiveTimed(context.Background(), "any query", "", keep)
	if err != nil {
		t.Fatalf("SearchExhaustiveTimed: %v", err)
	}
	if len(results) != 1 || results[0].Document.ID != "keep-me" {
		t.Fatalf("expected keep to filter out drop-me, got %+v", results)
	}
}

func entryID(i int) string {
	return "entry-" + string(rune('a'+i))
}
