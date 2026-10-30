package index

import (
	"container/heap"
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/fogfish/hnsw"
	"github.com/kshard/vector"
	"github.com/rs/zerolog"
)

// IndexEntry is the per-cell record indexed in CellStore: the
// searchable metadata projection of a memory cell (Title+Content is what gets
// embedded; the other fields drive filtering). Not a document/file.
type IndexEntry struct {
	ID         string
	Kind       string
	Scope      string
	Subject    string
	Title      string
	Content    string
	Confidence string
	Status     string
	Tags       []string
	EventDate  string // RFC3339, only set for kind=event

	// Caller-supplied metadata for cells that stand for something that happened
	// (an event, a filed report). Zero for ordinary memory items. The index only
	// stores and filters on it; what the values mean is the caller's.

	// Timestamp is when the underlying occurrence happened; the "since" window of
	// GetRecentBySubject and SearchByEmbedding filters on it.
	Timestamp time.Time
	// Source is where the occurrence came from. SearchByEmbedding can require an
	// exact match, so a near-identical text from another source is not a hit.
	Source string
	// Service is the key GetRecentBySubject filters on.
	Service string
	// Disposition is the outcome recorded for the occurrence. Empty while it is
	// still in flight; SearchByEmbedding and GetRecentBySubject skip such cells.
	Disposition string
	// Partition is the partition the cell belongs to. Empty means visible to
	// every partition; a cell naming another partition is never returned.
	Partition string
	// GroupID ties the cell to a caller-side group (may be empty).
	GroupID string
	// Type, Message, Severity and Classification are free labels the caller
	// reads back from a hit.
	Type, Message, Severity, Classification string
}

// CellSearchResult is a memory cell returned by CellStore.Search.
type CellSearchResult struct {
	Document IndexEntry
	Score    float32
}

// cellEntry is a single indexed cell with its embedding.
type cellEntry struct {
	Cell      IndexEntry
	Embedding []float32
}

// cellNodeEmbedding is the HNSW vector node for a memory cell.
type cellNodeEmbedding struct {
	Key string
	Vec vector.F32
}

var cellSurface = vector.ContraMap[vector.F32, cellNodeEmbedding]{
	Surface:   vector.Cosine(),
	ContraMap: func(e cellNodeEmbedding) vector.F32 { return e.Vec },
}

// subjectIndex is a per-subject HNSW index — subjects never share a graph.
type subjectIndex struct {
	mu      sync.RWMutex
	entries map[string]cellEntry
	index   *hnsw.HNSW[cellNodeEmbedding]
	// dirty is set when the graph holds nodes that entries no longer backs: a
	// delete (hnsw has no incremental delete) or an update that replaced an
	// embedding. freshen rebuilds once before the next graph read, so a delete
	// storm costs one rebuild instead of one per delete. Until then every read
	// filters hits by presence in entries, so a deleted id is never returned.
	dirty bool
	// rebuilds counts the graph rebuilds freshen has run, so a test can pin that
	// a delete storm costs one.
	rebuilds int
}

func newSubjectIndex() *subjectIndex {
	return &subjectIndex{entries: make(map[string]cellEntry), index: newGraph()}
}

func newGraph() *hnsw.HNSW[cellNodeEmbedding] {
	return hnsw.New(cellSurface,
		hnsw.WithEfConstruction(200),
		hnsw.WithM(16),
		hnsw.WithM0(32),
	)
}

// freshen rebuilds the graph if it is dirty. The graph is built outside the
// lock from a snapshot and swapped in under a brief write lock, so readers wait
// only for the swap, not the build (~4s at 10k nodes). Entries inserted while
// the build ran went to the old graph; they are added to the new one before it
// is swapped in. A delete during the build leaves its node in the new graph
// and sets dirty again, so the next read rebuilds.
func (s *subjectIndex) freshen() {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return
	}
	s.dirty = false
	// The snapshot keeps only what the build and the catch-up compare read, an
	// embedding per id, not the whole entry (title, content, tags, ...).
	snapshot := make(map[string][]float32, len(s.entries))
	for id, e := range s.entries {
		snapshot[id] = e.Embedding
	}
	s.mu.Unlock()

	g := newGraph()
	for id, emb := range snapshot {
		if len(emb) > 0 {
			g.Insert(cellNodeEmbedding{Key: id, Vec: vector.F32(emb)})
		}
	}

	s.mu.Lock()
	for id, e := range s.entries {
		if old, ok := snapshot[id]; ok && slices.Equal(old, e.Embedding) {
			continue
		}
		if len(e.Embedding) > 0 {
			g.Insert(cellNodeEmbedding{Key: id, Vec: vector.F32(e.Embedding)})
		}
	}
	s.index = g
	s.rebuilds++
	s.mu.Unlock()
}

func (s *subjectIndex) insert(doc IndexEntry, embedding []float32) {
	s.mu.Lock()
	// Re-seeding an unchanged cell (every reconcile does) must not dirty the
	// graph; replacing an embedding leaves the old node behind, which does.
	if old, ok := s.entries[doc.ID]; ok && !slices.Equal(old.Embedding, embedding) {
		s.dirty = true
	}
	s.entries[doc.ID] = cellEntry{Cell: doc, Embedding: embedding}
	s.index.Insert(cellNodeEmbedding{Key: doc.ID, Vec: vector.F32(embedding)})
	s.mu.Unlock()
}

// remove drops id from entries and reports whether it was there. The graph is
// rebuilt lazily (see dirty).
func (s *subjectIndex) remove(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.entries[id]; !ok {
		return false
	}
	delete(s.entries, id)
	s.dirty = true
	return true
}

func (s *subjectIndex) search(embedding []float32, topK int, kindFilter string) []CellSearchResult {
	s.freshen()

	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.entries) == 0 {
		return nil
	}
	// A kind-filtered search exact-scans that kind's entries instead of asking
	// the graph. Fetching topK*N global neighbours and dropping the other kinds
	// starves a minority kind: its cells never reach the fetch window when a
	// populous kind fills it, so the search returns nothing for a kind that has
	// plenty of matching cells. Scanning one kind is at most a few hundred dot
	// products.
	if kindFilter != "" {
		return scanEntriesByKind(embedding, s.entries, topK, kindFilter)
	}

	q := cellNodeEmbedding{Vec: vector.F32(embedding)}
	neighbors := s.index.Search(q, topK, 50)
	results := make([]CellSearchResult, 0, len(neighbors))
	for _, nb := range neighbors {
		entry, ok := s.entries[nb.Key]
		if !ok {
			continue
		}
		dist := s.index.Distance(q, nb)
		sim := float32(1.0) - dist/2.0
		results = append(results, CellSearchResult{Document: entry.Cell, Score: sim})
		if len(results) >= topK {
			break
		}
	}

	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	return results
}

// scanEntriesByKind scores every entry of kind (every entry when kind is empty)
// against embedding with the same cosine surface the graph uses, best first,
// truncated to topK (all of them when topK <= 0). The caller holds the index's
// read lock.
func scanEntriesByKind(embedding []float32, entries map[string]cellEntry, topK int, kind string) []CellSearchResult {
	return scanEntries(embedding, entries, topK, kind, nil)
}

// scanEntries is scanEntriesByKind with a document pre-filter: keep, when set,
// decides before scoring whether an entry is a candidate.
func scanEntries(embedding []float32, entries map[string]cellEntry, topK int, kind string, keep func(IndexEntry) bool) []CellSearchResult {
	// With a topK only that many results are ever kept, in a min-heap, so a
	// search over thousands of entries holds topK results instead of one per
	// candidate.
	capacity := len(entries)
	if topK > 0 && topK < capacity {
		capacity = topK
	}
	results := make(resultHeap, 0, capacity)
	for _, entry := range entries {
		if (kind != "" && entry.Cell.Kind != kind) || len(entry.Embedding) == 0 {
			continue
		}
		if keep != nil && !keep(entry.Cell) {
			continue
		}
		dist := cellSurface.Surface.Distance(embedding, entry.Embedding)
		r := CellSearchResult{Document: entry.Cell, Score: float32(1.0) - dist/2.0}
		switch {
		case topK <= 0 || len(results) < topK:
			heap.Push(&results, r)
		case ranksAbove(r.Score, results[0].Score):
			results[0] = r
			heap.Fix(&results, 0)
		}
	}
	out := []CellSearchResult(results)
	sort.Slice(out, func(i, j int) bool { return ranksAbove(out[i].Score, out[j].Score) })
	return out
}

// ranksAbove orders scores best first with NaN last. A zero-norm embedding
// scores NaN (0/0 in the cosine), and every comparison with NaN is false, so
// a NaN at the heap root would never be displaced and the scan would drop every
// later candidate.
func ranksAbove(a, b float32) bool {
	if a != a {
		return false
	}
	return b != b || a > b
}

// resultHeap is a min-heap on rank: its root is the weakest result kept.
type resultHeap []CellSearchResult

func (h resultHeap) Len() int           { return len(h) }
func (h resultHeap) Less(i, j int) bool { return ranksAbove(h[j].Score, h[i].Score) }
func (h resultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *resultHeap) Push(x any)        { *h = append(*h, x.(CellSearchResult)) }
func (h *resultHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// CellStore holds a per-subject HNSW index. Each subject's cells are
// isolated so cross-subject interference is impossible.
type CellStore struct {
	mu       sync.RWMutex
	subjects map[string]*subjectIndex
	embedder *Embedder
	logger   zerolog.Logger

	// queryCache is the exact-match LRU in front of every query-side embed this
	// store makes. Documents are embedded once, at index time, and never cached.
	// nil when embedder is nil.
	queryCache *queryEmbedCache

	// dim is the embedding dimension of every vector in the store (one embedder,
	// so one dimension across subjects): the embedder's configured output
	// dimension when known, else the first vector inserted or seeded. 0 means
	// not yet known. Guarded by mu.
	dim int
}

// NewCellStore creates an empty CellStore.
func NewCellStore(embedder *Embedder, logger zerolog.Logger) *CellStore {
	s := &CellStore{
		subjects: make(map[string]*subjectIndex),
		embedder: embedder,
		logger:   logger,
	}
	if embedder != nil {
		s.queryCache = newQueryEmbedCache(embedder, defaultQueryEmbedCacheCapacity, embedder.maxInputChars)
		s.dim = embedder.OutputDims()
	}
	return s
}

// embedQuery routes a search-query embedding through the store's cache.
func (s *CellStore) embedQuery(ctx context.Context, text string) ([]float32, error) {
	if s.queryCache == nil {
		return s.embedder.EmbedQuery(ctx, text)
	}
	return s.queryCache.Get(ctx, text)
}

// SearchTiming is the embed-versus-index split of one search: an embedder
// latency spike and an in-memory scan spike are indistinguishable from the total.
type SearchTiming struct {
	Embed time.Duration
	Index time.Duration
}

type timingSinkKey struct{}

// WithTimingSink asks Search, SearchSubject and SearchExhaustive to report their
// phase split to fn.
func WithTimingSink(ctx context.Context, fn func(SearchTiming)) context.Context {
	return context.WithValue(ctx, timingSinkKey{}, fn)
}

func reportTiming(ctx context.Context, t SearchTiming) {
	if fn, ok := ctx.Value(timingSinkKey{}).(func(SearchTiming)); ok {
		fn(t)
	}
}

// QueryEmbedCacheStats reads the query-embedding cache counters; zero when the
// store has no cache.
func (s *CellStore) QueryEmbedCacheStats() QueryEmbedCacheStats {
	if s.queryCache == nil {
		return QueryEmbedCacheStats{}
	}
	return s.queryCache.Stats()
}

// checkDim sets the store dimension from vec when it is unknown, else reports
// whether vec has it, logging a mismatch.
func (s *CellStore) checkDim(id string, vec []float32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dim == 0 {
		s.dim = len(vec)
		return true
	}
	if len(vec) != s.dim {
		s.logger.Warn().Str("id", id).Int("got", len(vec)).Int("want", s.dim).
			Msg("memory cell store: embedding dimension mismatch, rejecting insert")
		return false
	}
	return true
}

// dimMismatch reports whether a query vector disagrees with the store
// dimension. A store with nothing indexed never mismatches.
func (s *CellStore) dimMismatch(vec []float32) bool {
	s.mu.RLock()
	dim := s.dim
	s.mu.RUnlock()
	if dim == 0 || len(vec) == dim {
		return false
	}
	s.logger.Warn().Int("got", len(vec)).Int("want", dim).
		Msg("memory cell store: query embedding dimension mismatch, rejecting search")
	return true
}

// snapshotSubjects copies the subject index pointers under the lock, so a
// concurrent getOrCreate cannot race the iteration.
func (s *CellStore) snapshotSubjects() []*subjectIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*subjectIndex, 0, len(s.subjects))
	for _, idx := range s.subjects {
		out = append(out, idx)
	}
	return out
}

// SetScoreGap is kept for callers that still set a cutoff. The store no longer
// cuts its results: a cutoff applied here, before the caller has filtered what
// it may see and fused the semantic channel with the lexical one, starves a
// caller whose answers sit a few points below a hit it cannot read. A caller
// that wants a cutoff applies CutAtGap or CutAtLargestGap to the results it
// kept.
//
// Deprecated: it has no effect.
func (s *CellStore) SetScoreGap(float32) {}

// CutAtGap drops the tail of results (sorted by Score, best first) that falls
// more than gap below the top score. A zero or negative gap keeps everything.
func CutAtGap(results []CellSearchResult, gap float32) []CellSearchResult {
	if gap <= 0 || len(results) <= 1 {
		return results
	}
	floor := results[0].Score - gap
	for i, r := range results {
		if r.Score < floor {
			return results[:i]
		}
	}
	return results
}

// CutAtLargestGap drops the tail of results (sorted by Score, best first) at
// their steepest score drop. On a corpus whose documents resemble each other,
// every document scores something against every query, so a query with one
// right answer still fills the window with near-ties a few points below it.
// The signal is not the absolute score, which moves with the query, but where
// the list falls off a cliff; keeping everything above the largest single drop
// adapts to how many answers the query has, which a fixed threshold cannot. It
// trades recall for precision, so it is for a caller that wants it.
func CutAtLargestGap(results []CellSearchResult) []CellSearchResult {
	if len(results) < 2 {
		return results
	}
	// Ties go to the earliest gap, and "tie" allows a small margin: a list that
	// declines evenly has no cliff, and float32 rounding would otherwise pick an
	// arbitrary one of its equal gaps.
	const margin = 1.0001
	best, cut := float32(0), len(results)
	for i := 0; i < len(results)-1; i++ {
		if d := results[i].Score - results[i+1].Score; d > best*margin {
			best, cut = d, i+1
		}
	}
	return results[:cut]
}

func (s *CellStore) getOrCreate(subject string) *subjectIndex {
	s.mu.RLock()
	idx := s.subjects[subject]
	s.mu.RUnlock()
	if idx != nil {
		return idx
	}
	s.mu.Lock()
	if s.subjects[subject] == nil {
		s.subjects[subject] = newSubjectIndex()
	}
	idx = s.subjects[subject]
	s.mu.Unlock()
	return idx
}

// Index embeds and stores a memory cell document.
func (s *CellStore) Index(ctx context.Context, doc IndexEntry) ([]float32, error) {
	embedding, err := s.embedder.EmbedDocument(ctx, cellEmbedText(doc))
	if err != nil {
		return nil, fmt.Errorf("embed cell %s: %w", doc.ID, err)
	}
	if !s.checkDim(doc.ID, embedding) {
		return nil, fmt.Errorf("embed cell %s: dimension %d does not match index dimension", doc.ID, len(embedding))
	}
	s.getOrCreate(doc.Subject).insert(doc, embedding)
	return embedding, nil
}

// SeedFromEmbedding inserts a pre-embedded cell without calling the embedding
// API. It returns false, inserting nothing, when the vector's length disagrees
// with the store dimension (a cell embedded by another model): the caller
// should re-embed it. An empty embedding has nothing to seed and returns true.
//
// The dimension is the embedder's configured output dimension when known, so a
// stale wrong-dimension cell seeded first cannot lock the store to a wrong
// shape; a seeded vector only sets it when it is not known.
func (s *CellStore) SeedFromEmbedding(doc IndexEntry, embedding []float32) bool {
	if len(embedding) == 0 {
		return true
	}
	if !s.checkDim(doc.ID, embedding) {
		return false
	}
	s.getOrCreate(doc.Subject).insert(doc, embedding)
	return true
}

// Search returns the top-k results across every subject's index. An optional
// kindFilter (e.g. "stable_fact") restricts them to one kind. Subject-scoped
// search is SearchSubject.
func (s *CellStore) Search(ctx context.Context, query string, topK int, kindFilter string) ([]CellSearchResult, error) {
	results, _, err := s.SearchTimed(ctx, query, topK, kindFilter)
	return results, err
}

// SearchTimed is Search that also returns the embed-versus-index split.
func (s *CellStore) SearchTimed(ctx context.Context, query string, topK int, kindFilter string) ([]CellSearchResult, SearchTiming, error) {
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
	all := s.searchAll(embedding, topK, kindFilter)
	timing.Index = time.Since(indexStart)
	reportTiming(ctx, timing)
	logSearchScores(s.logger, "Search", query, all)
	if len(all) > topK {
		all = all[:topK]
	}
	return all, timing, nil
}

// searchAll merges the top-k of every subject's index, best first, with no
// score cutoff.
func (s *CellStore) searchAll(embedding []float32, topK int, kindFilter string) []CellSearchResult {
	var all []CellSearchResult
	for _, idx := range s.snapshotSubjects() {
		all = append(all, idx.search(embedding, topK, kindFilter)...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	return all
}

// SearchSubject searches within the given subject's index and merges results
// from the global (empty subject) index, returning top-k by score.
func (s *CellStore) SearchSubject(ctx context.Context, query, subject string, topK int, kindFilter string) ([]CellSearchResult, error) {
	embedStart := time.Now()
	embedding, err := s.embedQuery(ctx, query)
	timing := SearchTiming{Embed: time.Since(embedStart)}
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	if s.dimMismatch(embedding) {
		return nil, nil
	}
	indexStart := time.Now()
	s.mu.RLock()
	subjectIdx := s.subjects[subject]
	globalIdx := s.subjects[""]
	s.mu.RUnlock()

	var results []CellSearchResult
	if subjectIdx != nil {
		results = append(results, subjectIdx.search(embedding, topK, kindFilter)...)
	}
	if globalIdx != nil && subject != "" {
		results = append(results, globalIdx.search(embedding, topK, kindFilter)...)
	}

	timing.Index = time.Since(indexStart)
	reportTiming(ctx, timing)
	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	logSearchScores(s.logger, "SearchSubject", query, results)
	if len(results) > topK {
		results = results[:topK]
	}
	return results, nil
}

// logSearchScores emits the per-result cosine score spread (absolute score and
// %-of-top) at trace level, so the --semantic-score-gap cutoff can be observed
// and re-tuned against live data. Silent below --log-level=trace.
func logSearchScores(logger zerolog.Logger, where, query string, results []CellSearchResult) {
	if len(results) == 0 || logger.GetLevel() > zerolog.TraceLevel {
		return
	}
	ev := logger.Trace().Str("where", where).Str("query", query).Int("n", len(results))
	top := results[0].Score
	for i, r := range results {
		frac := float32(1)
		if top != 0 {
			frac = r.Score / top
		}
		ev = ev.Str(fmt.Sprintf("r%02d", i),
			fmt.Sprintf("%.4f (%.1f%% of top) %q", r.Score, frac*100, truncateTitle(r.Document.Title, 40)))
	}
	ev.Msg("semantic score spread")
}

func truncateTitle(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// Delete removes a memory cell by ID across all subject indexes. The graph is
// rebuilt lazily before the next search (see subjectIndex.dirty), so a sweep
// that deletes hundreds of cells costs one rebuild.
func (s *CellStore) Delete(_ context.Context, id string) error {
	for _, idx := range s.snapshotSubjects() {
		if idx.remove(id) {
			return nil
		}
	}
	return nil
}

// CollectionSize returns total indexed cells across all subjects.
func (s *CellStore) CollectionSize() int64 {
	var n int64
	for _, idx := range s.snapshotSubjects() {
		idx.mu.RLock()
		n += int64(len(idx.entries))
		idx.mu.RUnlock()
	}
	return n
}

func cellEmbedText(doc IndexEntry) string {
	parts := []string{doc.Kind}
	if doc.Scope != "" {
		parts = append(parts, doc.Scope)
	}
	if doc.Subject != "" {
		parts = append(parts, doc.Subject)
	}
	parts = append(parts, doc.Title, doc.Content)
	return capEmbedText(strings.Join(parts, " "))
}

// maxCellEmbedChars caps the text one cell sends to the embedder. A local
// embedder rejects an input over its batch size (2048 tokens) outright, and a
// long note passed that and failed to index forever. JSON runs about 2 bytes a
// token and the tokenizer never makes more tokens than bytes, so a cap under
// 2048 bytes is the one that always fits; the head (kind, subject, title, the
// start of the content) is what similarity needs.
const maxCellEmbedChars = 1800

// capEmbedText cuts s to maxCellEmbedChars bytes on a rune boundary.
func capEmbedText(s string) string {
	if len(s) <= maxCellEmbedChars {
		return s
	}
	cut := maxCellEmbedChars
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

// SearchExhaustive ranks every entry the caller may see by exact cosine, with no
// graph and no fetch window: the subject's own cells and the global ones, or
// every subject's when subject is empty. It exists for one caller. A graph
// search returns its top-K window, and a caller that drops hits afterwards (a
// cell deleted or retired on another replica is still indexed here) can see the
// whole window rejected although other cells would have qualified. This hands
// back the full ranked population so the caller's filter gets every candidate.
// No score gap is applied.
func (s *CellStore) SearchExhaustive(ctx context.Context, query, subject string, kindFilter string) ([]CellSearchResult, error) {
	embedding, err := s.embedQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	if s.dimMismatch(embedding) {
		return nil, nil
	}
	var idxs []*subjectIndex
	if subject == "" {
		idxs = s.snapshotSubjects()
	} else {
		s.mu.RLock()
		for _, k := range []string{subject, ""} {
			if idx := s.subjects[k]; idx != nil {
				idxs = append(idxs, idx)
			}
		}
		s.mu.RUnlock()
	}
	var all []CellSearchResult
	for _, idx := range idxs {
		idx.mu.RLock()
		all = append(all, scanEntriesByKind(embedding, idx.entries, 0, kindFilter)...)
		idx.mu.RUnlock()
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	return all, nil
}

// rebuildCount is the number of graph rebuilds across every subject index.
func (s *CellStore) rebuildCount() int {
	n := 0
	for _, idx := range s.snapshotSubjects() {
		idx.mu.RLock()
		n += idx.rebuilds
		idx.mu.RUnlock()
	}
	return n
}

// anyDirty reports whether a delete or replaced embedding left a graph stale.
func (s *CellStore) anyDirty() bool {
	for _, idx := range s.snapshotSubjects() {
		idx.mu.RLock()
		d := idx.dirty
		idx.mu.RUnlock()
		if d {
			return true
		}
	}
	return false
}
