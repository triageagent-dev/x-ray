package index

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func topKEntries(n, dim int) map[string]cellEntry {
	rng := rand.New(rand.NewSource(7))
	entries := make(map[string]cellEntry, n)
	for i := 0; i < n; i++ {
		emb := make([]float32, dim)
		for j := range emb {
			emb[j] = rng.Float32()*2 - 1
		}
		id := fmt.Sprintf("c%04d", i)
		entries[id] = cellEntry{Cell: IndexEntry{ID: id, Kind: fmt.Sprintf("k%d", i%4), Title: "t", Content: "c"}, Embedding: emb}
	}
	return entries
}

// TestScanEntries_TopKMatchesFullSort proves the bounded scan returns the same
// scores, in the same order, as scoring every entry and sorting them all.
func TestScanEntries_TopKMatchesFullSort(t *testing.T) {
	entries := topKEntries(300, 16)
	q := make([]float32, 16)
	for i := range q {
		q[i] = float32(i%3) - 1
	}
	for _, kind := range []string{"", "k1"} {
		all := scanEntries(q, entries, 0, kind, nil)
		require := assert.New(t)
		require.True(sort.SliceIsSorted(all, func(i, j int) bool { return all[i].Score > all[j].Score }))
		for _, k := range []int{1, 5, 10, len(all), len(all) + 50} {
			got := scanEntries(q, entries, k, kind, nil)
			want := all
			if k < len(want) {
				want = want[:k]
			}
			require.Equal(len(want), len(got), "kind=%q k=%d", kind, k)
			for i := range want {
				require.Equal(want[i].Score, got[i].Score, "kind=%q k=%d rank %d", kind, k, i)
			}
		}
	}
	assert.Empty(t, scanEntries(q, entries, 10, "no-such-kind", nil))
}

// BenchmarkScanEntries_KindFilter measures one kind-filtered search over 2,000
// 768-dimension entries asking for ten.
func BenchmarkScanEntries_KindFilter(b *testing.B) {
	entries := topKEntries(2000, 768)
	q := make([]float32, 768)
	q[3] = 1
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = scanEntriesByKind(q, entries, 10, "k1")
	}
}
