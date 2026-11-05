package index

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/kshard/vector"

	memoryv1 "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// SearchSince returns the top-k cells of one kind most similar to the query text
// that carry a Timestamp inside the "since" window (a zero since keeps the whole
// kind). A caller names its own kinds; the index knows none.
func (s *CellStore) SearchSince(ctx context.Context, query string, topK int, kind string, since time.Time) ([]CellSearchResult, error) {
	if topK <= 0 {
		topK = 5
	}
	embedding, err := s.embedQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	if s.dimMismatch(embedding) {
		return nil, nil
	}
	// Over-fetch, then drop what falls outside the window.
	raw := s.searchAll(embedding, topK*3, kind)
	results := make([]CellSearchResult, 0, topK)
	for _, r := range raw {
		if !since.IsZero() && r.Document.Timestamp.Before(since) {
			continue
		}
		results = append(results, r)
		if len(results) >= topK {
			break
		}
	}
	return results, nil
}

// SearchExhaustiveTimed ranks every entry of every subject against the query by
// exact cosine, with no graph and no fetch window, optionally restricted to a
// kind and narrowed by keep before scoring. A graph search returns its top-K
// window, and a caller that rejects hits afterwards (visibility, state) can see
// the whole window rejected although other cells would have qualified; this
// hands back the full ranked population. keep is a pre-filter for a caller that
// can already tell from the entry (its tags, say) that it would reject a cell.
// No score gap is applied.
func (s *CellStore) SearchExhaustiveTimed(ctx context.Context, query, kind string, keep func(IndexEntry) bool) ([]CellSearchResult, SearchTiming, error) {
	embedStart := time.Now()
	embedding, err := s.embedQuery(ctx, query)
	timing := SearchTiming{Embed: time.Since(embedStart)}
	if err != nil {
		return nil, timing, fmt.Errorf("embed query: %w", err)
	}
	if s.dimMismatch(embedding) {
		return nil, timing, nil
	}
	indexStart := time.Now()
	var all []CellSearchResult
	for _, idx := range s.snapshotSubjects() {
		idx.mu.RLock()
		all = append(all, scanEntries(embedding, idx.entries, 0, kind, keep)...)
		idx.mu.RUnlock()
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	timing.Index = time.Since(indexStart)
	reportTiming(ctx, timing)
	return all, timing, nil
}

// SearchByEmbedding returns the single most similar cell above minSimilarity
// (cosine) among the cells that carry a Disposition, are visible to partition
// (their Partition equals it or is empty), fall inside the "since" window and,
// when source is not empty, name exactly that Source.
//
// The source guard is a correctness guard: templated texts embed almost
// identically across sources, so the nearest vector alone can belong to another
// source whose recorded outcome does not apply. The partition rule is the one
// GetRecentBySubject applies.
func (s *CellStore) SearchByEmbedding(_ context.Context, partition string, embedding []float32, since time.Time, minSimilarity float32, source string) (*CellSearchResult, error) {
	if s.dimMismatch(embedding) {
		return nil, nil
	}
	q := cellNodeEmbedding{Vec: vector.F32(embedding)}
	// Over-fetch to survive the filters below; when a source is required the
	// nearest vectors may all belong to other sources, so widen the pool.
	fetch := 3
	if source != "" {
		fetch = 23
	}
	var best *CellSearchResult
	for _, idx := range s.snapshotSubjects() {
		idx.freshen()
		idx.mu.RLock()
		if len(idx.entries) == 0 {
			idx.mu.RUnlock()
			continue
		}
		for _, nb := range idx.index.Search(q, fetch, 100) {
			entry, ok := idx.entries[nb.Key]
			if !ok {
				continue
			}
			cell := entry.Cell
			if cell.Disposition == "" || (cell.Partition != "" && cell.Partition != partition) {
				continue
			}
			if !since.IsZero() && cell.Timestamp.Before(since) {
				continue
			}
			if source != "" && cell.Source != source {
				continue
			}
			// The vector kernel returns dist = (1-cos)/2, so cos = 1-2*dist.
			sim := float32(1.0) - 2*idx.index.Distance(q, nb)
			if sim < minSimilarity {
				continue
			}
			if best == nil || sim > best.Score {
				best = &CellSearchResult{Document: cell, Score: sim}
			}
		}
		idx.mu.RUnlock()
	}
	return best, nil
}

// GetRecentBySubject returns up to topN cells of one service, oldest first,
// inside the "since" window, keeping the newest when topN cuts. Only cells that
// carry a Disposition are returned. A cell is visible when its Partition equals
// partition or is empty; one naming another partition is never returned, so a
// same-named service in two partitions never shares history.
func (s *CellStore) GetRecentBySubject(_ context.Context, partition, service string, topN int, since time.Time) ([]IndexEntry, error) {
	docs := make([]IndexEntry, 0)
	for _, idx := range s.snapshotSubjects() {
		idx.mu.RLock()
		for _, e := range idx.entries {
			c := e.Cell
			if c.Service != service || c.Disposition == "" {
				continue
			}
			if c.Partition != "" && c.Partition != partition {
				continue
			}
			if !since.IsZero() && c.Timestamp.Before(since) {
				continue
			}
			docs = append(docs, c)
		}
		idx.mu.RUnlock()
	}
	sort.SliceStable(docs, func(i, j int) bool { return docs[i].Timestamp.Before(docs[j].Timestamp) })
	if topN > 0 && len(docs) > topN {
		docs = docs[len(docs)-topN:]
	}
	return docs, nil
}

// QueryEmbedCacheEntries is how many queries the query-embedding cache holds;
// 0 when the store has no embedder.
func (s *CellStore) QueryEmbedCacheEntries() int {
	if s.queryCache == nil {
		return 0
	}
	return s.queryCache.Stats().Entries
}

// SetQueryEmbedCounter sets where the query-embedding cache reports each
// lookup's outcome, true for a hit. Call before the store is searched.
func (s *CellStore) SetQueryEmbedCounter(fn func(hit bool)) {
	if s.queryCache != nil {
		s.queryCache.count = fn
	}
}

// FingerprintAnnotation is the MemoryCell annotation that records what the
// cell's stored embedding (Spec.Embedding) was made from:
// EmbeddingFingerprint of the cell's text and of that embedding. It is written
// in the same update as the embedding, so the two never disagree. It is an
// annotation, not a spec or status field, so that it needs no CRD schema
// change: deployments run the chart's CRDs, which Helm installs but never
// upgrades, and the API server prunes a field its schema does not know, so a
// fingerprint kept in an unknown status field would vanish on every write and
// the cell would be re-embedded forever.
const FingerprintAnnotation = "xray.triageagent.dev/embedding-fingerprint"

// EmbeddingFingerprint is the FingerprintAnnotation value for a cell whose
// index entry is doc and whose stored embedding is embedding. It is the
// lowercase hex SHA-256 of the embedder's model, its output dimension and the
// exact text it is sent for doc (cut to 1800 bytes and to the embedder's input
// cap), a dot, and the first 16 hex digits of the SHA-256 of embedding. The
// first part changes with the text, model or dimension; the second binds the
// annotation to the vector it was written with, so a vector that another writer
// replaced without the annotation is not taken as current. It detects a
// mismatch, it does not authenticate the writer.
func (s *CellStore) EmbeddingFingerprint(doc IndexEntry, embedding string) string {
	model := s.embedder.Model()
	h := sha256.New()
	// The model is length-prefixed and the dimension is digits, both closed by
	// a NUL, and the text runs to the end: no model or text can make two
	// different triples hash the same bytes.
	fmt.Fprintf(h, "%d:%s\x00%d\x00", len(model), model, s.embedder.OutputDims())
	_, _ = io.WriteString(h, s.embedder.inputText(cellEmbedText(doc)))
	vec := sha256.Sum256([]byte(embedding))
	return hex.EncodeToString(h.Sum(nil)) + "." + hex.EncodeToString(vec[:8])
}

// SeedOrEmbed makes sure a cell is in the index. A cell whose stored embedding
// is current, that is its FingerprintAnnotation equals
// EmbeddingFingerprint(doc, Spec.Embedding) and the embedding decodes to the
// index's dimension, is seeded from it with no embedder call, and nothing needs
// writing: the returned string is empty. Any other cell is embedded now: one
// with no embedding, one whose text, model or dimension changed since it was
// embedded, one embedded before the fingerprint existed (no annotation), one
// whose embedding was replaced without its annotation, or one whose embedding
// cannot be decoded or has the wrong dimension. The returned string is then the
// new embedding, and the caller persists it on the cell together with
// FingerprintAnnotation set to EmbeddingFingerprint(doc, embedded), in one
// write; without the annotation the next sync embeds the cell again.
//
// When that embed fails and the cell carries a stored embedding that is not
// current but decodes to the index's known dimension, the stored vector is
// seeded so the cell stays searchable, and the error is returned: nothing is to
// be written, and the caller retries. doc is the cell's index entry.
func (s *CellStore) SeedOrEmbed(ctx context.Context, cell *memoryv1.MemoryCell, doc IndexEntry) (embedded string, err error) {
	var stale []float32 // a stored vector of another text or origin, the fallback if the embed fails
	if cell.Spec.Embedding != "" {
		vec, derr := DecodeEmbedding(cell.Spec.Embedding)
		switch {
		case derr != nil:
			s.logger.Warn().Err(derr).Str("cell", doc.ID).Msg("cell index: stored embedding cannot be decoded; re-embedding")
		case cell.Annotations[FingerprintAnnotation] != s.EmbeddingFingerprint(doc, cell.Spec.Embedding):
			s.logger.Debug().Str("cell", doc.ID).Msg("cell index: stored embedding was made from another text, model or dimension, or has no fingerprint; re-embedding")
			stale = vec
		case s.SeedFromEmbedding(doc, vec):
			return "", nil
		default:
			s.logger.Warn().Str("cell", doc.ID).Int("dims", len(vec)).Msg("cell index: stored embedding has the wrong dimension; re-embedding")
		}
	}
	vec, err := s.Index(ctx, doc)
	if err != nil {
		// Only into a store whose dimension is known: a stale vector must not set
		// it, or one from another model would lock the store to its shape.
		if stale != nil && s.hasDim(len(stale)) && s.SeedFromEmbedding(doc, stale) {
			return "", fmt.Errorf("%w (the stored embedding is seeded until the cell can be embedded)", err)
		}
		return "", err
	}
	return EncodeEmbedding(vec), nil
}
