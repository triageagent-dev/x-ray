package memstore

import (
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// restoreCorpus builds n active items shaped like a live memory corpus: a few
// hundred words each drawn from a skewed (zipf) vocabulary of a few thousand
// words, mixing camelCase identifiers, dashed ids and punctuation so every
// tokenisation path (lower-cased, normalised, joins) sees realistic input.
// Deterministic for a given n.
func restoreCorpus(n, wordsPerItem int) []Item {
	rng := rand.New(rand.NewSource(1))
	syll := []string{"ka", "lo", "mi", "ren", "tor", "vex", "pod", "svc", "ing", "res", "api", "dns", "log", "err", "cfg", "net", "db", "io", "ux", "ok"}
	vocab := make([]string, 4000)
	for i := range vocab {
		var sb strings.Builder
		for k := 0; k < 2+rng.Intn(3); k++ {
			sb.WriteString(syll[rng.Intn(len(syll))])
		}
		w := sb.String()
		switch rng.Intn(10) {
		case 0:
			w = strings.ToUpper(w[:1]) + w[1:] + strings.ToUpper(syll[rng.Intn(len(syll))][:1]) + "x"
		case 1:
			w = w + "-" + syll[rng.Intn(len(syll))]
		}
		vocab[i] = w
	}
	zipf := rand.NewZipf(rng, 1.2, 8, uint64(len(vocab)-1))
	now := metav1.NewTime(time.Unix(1_700_000_000, 0))
	items := make([]Item, n)
	for i := range items {
		var sb strings.Builder
		for w := 0; w < wordsPerItem; w++ {
			sb.WriteString(vocab[zipf.Uint64()])
			switch rng.Intn(12) {
			case 0:
				sb.WriteString(". ")
			case 1:
				sb.WriteString(", ")
			default:
				sb.WriteByte(' ')
			}
		}
		it := Item{}
		it.ID = fmt.Sprintf("memory-%06d", i)
		it.Kind = "note"
		it.Status = "active"
		it.Title = vocab[zipf.Uint64()] + " " + vocab[zipf.Uint64()] + " " + vocab[zipf.Uint64()]
		it.Content = sb.String()
		it.CreatedAt, it.UpdatedAt = now, now
		items[i] = it
	}
	return items
}

func heapInuse() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// BenchmarkRestoreHeap reports the heap a Store holds after Restore of a
// 2,000-item corpus (the size of the profiled core replica), as heap-B/item:
// the live-heap growth Restore leaves behind, with the input items still
// reachable on both sides of the measurement. Run with -benchtime=1x.
func BenchmarkRestoreHeap(b *testing.B) {
	const n = 2000
	items := restoreCorpus(n, 100)
	var total uint64
	for i := 0; i < b.N; i++ {
		before := heapInuse()
		s := NewStore()
		s.Restore(items)
		after := heapInuse()
		total += after - before
		runtime.KeepAlive(s)
		runtime.KeepAlive(items)
	}
	b.ReportMetric(float64(total)/float64(b.N)/n, "heap-B/item")
}
