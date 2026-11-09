// Package index provides semantic search over a recall graph. It embeds each
// node's text via the shared embedder (internal/embedder, any OpenAI-compatible
// endpoint) and answers queries with exact cosine similarity.
//
// At ~10^3-10^4 nodes a brute-force scan is fast (sub-millisecond) and exact, so
// no ANN index is used; swap in HNSW (github.com/fogfish/hnsw, as the snapshot
// codec already does) if the node count grows past ~10^5.
package index

import (
	"context"
	"fmt"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fogfish/hnsw"
	"github.com/kshard/vector"
	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/embedder"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/graph"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/hub"
)

// Embedder is the subset of embedder.Embedder this package needs.
type Embedder interface {
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, error)
	EmbedQuery(ctx context.Context, text string) ([]float32, error)
}

// annNode is the HNSW vector node: the node's qualified name keyed to its vector.
type annNode struct {
	QN  string
	Vec vector.F32
}

// annSurface maps an annNode to its vector under cosine distance — the metric
// HNSW traverses on. Mirrors the parent memory store's surface.
var annSurface = vector.ContraMap[vector.F32, annNode]{
	Surface:   vector.Cosine(),
	ContraMap: func(n annNode) vector.F32 { return n.Vec },
}

// Index embeds graph nodes and serves semantic searches. It caches embeddings
// by node text so an incremental rebuild only embeds genuinely new/changed text
// — unchanged nodes (which become fresh *Node objects on each rebuild) reuse
// their cached vector instead of hitting the embedder again.
//
// Search runs over an HNSW approximate-nearest-neighbour index (O(log N) per
// query) so it scales to many-repo node counts; a brute-force exact fallback is
// retained (useBruteForce) for correctness A/B testing.
type Index struct {
	g        *graph.Graph
	embedder Embedder
	logger   zerolog.Logger

	mu    sync.Mutex
	cache map[string][]float32 // node text -> vector

	ann           *hnsw.HNSW[annNode] // approximate-nearest-neighbour index
	useBruteForce bool                // force the exact O(N·D) scan (for A/B testing)

	// pendingHNSW is a serialized ANN graph from a snapshot. When set (via
	// LoadCacheAndHNSW), Build deserializes it instead of rebuilding from vectors
	// — the rebuild is the dominant cold-load cost. Cleared after a successful
	// load; on any mismatch (vector count differs) Build falls back to rebuild.
	pendingHNSW map[string][]byte

	// noEmbed marks an Index that must never call the embedder for a node with
	// no cached vector — set when building a `snapshot -no-embed` map (the
	// embed pass is being skipped on purpose) or when loading one back
	// (SnapshotHeader.NoEmbed). Build leaves such nodes unembedded instead of
	// calling out, so a search over a no-embed map degrades to its lexical
	// channel (rebuildLexLocked still indexes every node's text) rather than
	// silently re-embedding a whole repo per query, or failing outright when
	// the embedder is unreachable — the exact situation -no-embed exists for.
	noEmbed bool

	// lex is the keyword channel fused with the semantic one in Search (see
	// lexical.go). Rebuilt from the graph whenever Build completes, so it tracks
	// cold builds, snapshot seeds, and incremental updates alike. It carries its
	// own RWMutex rather than sharing ix.mu, since searches read it concurrently.
	lex *trigramIndex

	// stats accumulates search-quality metrics (docs/todo/search-metrics.md).
	// New gives every Index a private instance so it always works standalone;
	// engine.Registry overrides it with a SHARED, process-lifetime instance via
	// UseStats for maps that get evicted/reloaded (Mode B) — see SearchStats'
	// doc comment for why that matters. Set once, before the Index is published
	// to any concurrent searcher; Search reads it under ix.mu alongside the
	// other per-search-configurable fields it already locks for.
	stats *SearchStats
}

// New wires an index to a graph and embedder.
func New(g *graph.Graph, e Embedder, logger zerolog.Logger) *Index {
	return &Index{g: g, embedder: e, logger: logger, cache: map[string][]float32{}, lex: newTrigramIndex(), stats: NewSearchStats()}
}

// UseStats swaps in a shared SearchStats instance, so its counts survive
// this *Index being replaced (e.g. evict/reload in Mode B) as long as every
// replacement is given the same instance. Must be called before the Index is
// shared with any concurrent caller — see the field's doc comment. A nil s
// is a no-op (keeps the private instance New created).
func (ix *Index) UseStats(s *SearchStats) {
	if s == nil {
		return
	}
	ix.mu.Lock()
	ix.stats = s
	ix.mu.Unlock()
}

// Stats returns the Index's current SearchStats — the shared, injected
// instance if UseStats was called, otherwise the private one New created.
func (ix *Index) Stats() *SearchStats {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.stats
}

// rebuildLexLocked repopulates the keyword index from the current graph. Called
// at the end of every Build path so the lexical channel can never describe an
// older graph than the vectors do. Caller must hold ix.mu.
func (ix *Index) rebuildLexLocked() {
	if ix.lex == nil {
		ix.lex = newTrigramIndex()
	}
	ix.lex.reset()
	for _, n := range ix.g.Nodes() {
		ix.lex.index(n.QN, nodeTitle(n), nodeText(n))
	}
}

// SetBruteForce toggles the exact brute-force scan instead of HNSW — used to
// A/B test recall/latency of the ANN index against the exact baseline.
func (ix *Index) SetBruteForce(b bool) {
	ix.mu.Lock()
	ix.useBruteForce = b
	ix.mu.Unlock()
}

// SetNoEmbed marks the index as never calling the embedder for a node with no
// cached vector (see the noEmbed field). Call before Build.
func (ix *Index) SetNoEmbed(b bool) {
	ix.mu.Lock()
	ix.noEmbed = b
	ix.mu.Unlock()
}

// NoEmbed reports whether the index was built, or loaded, no-embed.
func (ix *Index) NoEmbed() bool {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.noEmbed
}

// CacheSnapshot returns a copy of the text→vector cache, for persisting a graph
// snapshot. The copy decouples the on-disk payload from later mutation
// of the live cache.
func (ix *Index) CacheSnapshot() map[string][]float32 {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	out := make(map[string][]float32, len(ix.cache))
	maps.Copy(out, ix.cache)
	return out
}

// PruneCache drops every cached vector whose text no current node has, and
// returns how many it dropped. The cache is keyed by text and only ever grows:
// a node whose body changed leaves its old text behind. Written into a
// snapshot, that dead weight is carried into every rebuild seeded from it, so
// a snapshot seeded from its predecessor prunes before it is written.
func (ix *Index) PruneCache() int {
	live := map[string]bool{}
	for _, n := range ix.g.Nodes() {
		live[nodeText(n)] = true
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	dropped := 0
	for txt := range ix.cache {
		if !live[txt] {
			delete(ix.cache, txt)
			dropped++
		}
	}
	return dropped
}

// LoadCache pre-populates the text→vector cache from a snapshot before Build, so
// a snapshot-backed graph embeds nothing: every node's text is already a cache
// hit and Build only (re)builds the HNSW index. Used by the read-only snapshot
// server.
func (ix *Index) LoadCache(c map[string][]float32) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	maps.Copy(ix.cache, c)
}

// SetPendingHNSW stages a serialized ANN graph (from a snapshot body) for Build
// to deserialize instead of rebuilding. Pass nil/empty to keep the rebuild path.
func (ix *Index) SetPendingHNSW(dump map[string][]byte) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	ix.pendingHNSW = dump
}

// rebuildANNLocked builds a fresh HNSW graph from every node that currently has
// a vector. Caller holds ix.mu. Called after Build/EmbedMissing so the ANN index
// always reflects the embedded graph.
func (ix *Index) rebuildANNLocked() {
	g := hnsw.New(annSurface,
		hnsw.WithEfConstruction(200),
		hnsw.WithM(16),
		hnsw.WithM0(32),
	)
	n := 0
	for _, nd := range ix.g.Nodes() {
		if nd.Vec == nil {
			continue
		}
		g.Insert(annNode{QN: nd.QN, Vec: vector.F32(nd.Vec)})
		n++
	}
	ix.ann = g
	ix.logger.Debug().Int("vectors", n).Msg("built HNSW index")
}

// NewEmbedder builds the shared embedder pointed at a llama.cpp-compatible
// /v1/embeddings endpoint (e.g. http://localhost:8090), truncated to dims.
// maxInputChars hard-caps a node's text before the embed call: set it below the
// model's token window × ~3.5 so oversized nodes are TRUNCATED here rather than
// REJECTED by the model (nomic 2048-tok → 8192; bge/MiniLM 512-tok → ~1800).
// That ratio is a guess and dense text breaks it, so maxInputTokens (the
// model's window in tokens, 0 = unknown) cuts an input over the window to the
// longest prefix that fits, counted by the endpoint's tokenizer.
func NewEmbedder(ctx context.Context, endpoint, model string, dims, maxInputChars, maxInputTokens int, logger zerolog.Logger) (*embedder.Embedder, error) {
	return embedder.NewEmbedder(ctx, endpoint, model, dims, maxInputChars, maxInputTokens, logger)
}

// Build embeds every node, reusing cached vectors for unchanged text and only
// calling the embedder for cache misses. Safe to call repeatedly after a graph
// rebuild — it becomes an incremental embed.
func (ix *Index) Build(ctx context.Context) error {
	nodes := ix.g.Nodes()

	ix.mu.Lock()
	// Partition into cache hits (apply immediately) and misses (embed in batch).
	var missNodes []*graph.Node
	var missTexts []string
	hits := 0
	for _, n := range nodes {
		txt := nodeText(n)
		if vec, ok := ix.cache[txt]; ok {
			ix.g.SetVec(n.QN, vec)
			hits++
			continue
		}
		missNodes = append(missNodes, n)
		missTexts = append(missTexts, txt)
	}
	noEmbed := ix.noEmbed
	ix.mu.Unlock()

	if noEmbed && len(missTexts) > 0 {
		// Never call the embedder: leave these nodes without a vector. The
		// lexical channel still indexes every node's text (rebuildLexLocked),
		// so Search degrades to its lexical channel instead of silently
		// re-embedding a whole repo per query, or failing outright when the
		// embedder is unreachable — see the noEmbed field's doc comment.
		ix.mu.Lock()
		ix.rebuildLexLocked()
		ix.mu.Unlock()
		ix.logger.Info().Int("cache_hits", hits).Int("embedded", 0).Int("unembedded", len(missTexts)).Bool("noEmbed", true).
			Msg("recall index ready (no-embed: lexical search only)")
		return nil
	}

	if len(missTexts) == 0 {
		// All vectors cached (snapshot load). If the snapshot also carried a
		// serialized HNSW, deserialize it and SKIP the rebuild — the rebuild is
		// the dominant cold-load cost. Fall back to rebuild on any load failure or
		// vector-count mismatch (stale/partial dump).
		ix.mu.Lock()
		dump := ix.pendingHNSW
		ix.pendingHNSW = nil
		ix.mu.Unlock()
		if loaded := ix.tryLoadHNSW(dump); loaded {
			ix.mu.Lock()
			ix.rebuildLexLocked()
			ix.mu.Unlock()
			ix.logger.Info().Int("cache_hits", hits).Int("embedded", 0).Msg("recall index ready (all cached, HNSW from snapshot)")
			return nil
		}
		ix.mu.Lock()
		ix.rebuildANNLocked()
		ix.rebuildLexLocked()
		ix.mu.Unlock()
		ix.logger.Info().Int("cache_hits", hits).Int("embedded", 0).Msg("recall index ready (all cached)")
		return nil
	}
	ix.logger.Info().Int("cache_hits", hits).Int("to_embed", len(missTexts)).Msg("embedding recall index")
	vecs, err := ix.embedder.EmbedBatch(ctx, missTexts)
	if err != nil {
		return fmt.Errorf("embed batch: %w", err)
	}
	ix.mu.Lock()
	for i, n := range missNodes {
		ix.g.SetVec(n.QN, vecs[i])
		ix.cache[missTexts[i]] = vecs[i]
	}
	// The graph's vectors are now complete; (re)build the ANN index over them.
	ix.rebuildANNLocked()
	ix.rebuildLexLocked()
	ix.mu.Unlock()
	ix.logger.Info().Int("cache_hits", hits).Int("embedded", len(vecs)).Msg("recall index embedded")
	return nil
}

// EmbedMissing fills in vectors for any node currently lacking one, reusing the
// text cache. Used after an incremental graph rebuild — equivalent to Build now
// that Build is cache-aware, kept as an intent-revealing name.
func (ix *Index) EmbedMissing(ctx context.Context) error {
	return ix.Build(ctx)
}

// Hit is one semantic search result.
type Hit struct {
	QN       string  `json:"qn"`
	Repo     string  `json:"repo,omitempty"`
	Kind     string  `json:"kind"`
	Title    string  `json:"title"`
	File     string  `json:"file"`
	Line     int     `json:"line"`
	Score    float32 `json:"score"`
	Preview  string  `json:"preview"`
	Language string  `json:"language"`

	// Parent is the qualified name of the node that contains this one, set
	// only for a child node (a catalog entry's part, a doc's section). A search
	// over a catalog whose entries carry parts returns the parts — the team
	// map answered "most experienced student" with five Answer parts — and a
	// brief hit gave no way back to the entity that holds the facts short of a
	// second call per hit.
	Parent string `json:"parent,omitempty"`

	// Fused is the fusion score this hit was ORDERED by, and the only quantity a
	// caller may re-sort on. Score is a cosine — it says how similar a hit is,
	// not why it was selected — so sorting by it reorders a list its own numbers
	// did not choose, burying keyword-promoted hits. Not serialized: a
	// within-query ordinal means nothing to a reader.
	Fused float64 `json:"-"`
}

// Select is the predicate the CALLER named, as distinct from the kinds a calling
// tool scopes itself to. The two are different requests: a tool's scope says
// which part of a map it is willing to answer from, while a caller naming a set
// means the members ARE the answer. So a Select is resolved against the graph and
// ranked within, rather than the query ranking everything and non-members being
// dropped afterwards — which returns a member only when the query happened to
// retrieve it.
type Select struct {
	// Kinds, when non-empty, selects nodes of those kinds. A map's kinds are
	// its own vocabulary (Student, Service, Doc), reported by graph.Stats.
	Kinds map[string]bool
	// Attr, when set, selects nodes carrying that exact key/value.
	Attr AttrFilter
	// Order, when set, sorts the selected set by an attribute instead of by
	// similarity to the query. A superlative — "most experienced student",
	// "oldest incident" — is a sort, not a similarity, and no similarity rank
	// can answer it: the team map ranked a kind=Student selection by cosine
	// and put an 11-year student first, so answering needed all 37 entries read
	// back and compared by the caller. Naming an order selects the whole scope
	// (Select.Empty is false), because ordering only means something over a set
	// the caller chose, never over the few nodes a query happened to retrieve.
	Order Order
}

// Order is an attribute to sort a selection by, and the direction. The zero
// value orders nothing.
type Order struct {
	Key  string
	Desc bool
}

// Empty reports whether the order names an attribute.
func (o Order) Empty() bool { return o.Key == "" }

// ParseOrder reads "key", "key:asc" or "key:desc". An empty string yields the
// zero order; an unknown direction is an error rather than a silent default, so
// a typo cannot quietly invert the answer to a superlative.
func ParseOrder(s string) (Order, error) {
	if s == "" {
		return Order{}, nil
	}
	key, dir, hasDir := strings.Cut(s, ":")
	if key == "" {
		return Order{}, fmt.Errorf("order must be \"key\", \"key:asc\" or \"key:desc\", got %q", s)
	}
	o := Order{Key: key}
	if hasDir {
		switch strings.ToLower(dir) {
		case "asc":
		case "desc":
			o.Desc = true
		default:
			return Order{}, fmt.Errorf("order direction must be \"asc\" or \"desc\", got %q", dir)
		}
	}
	return o, nil
}

// Empty reports whether the caller named nothing, in which case Search ranks the
// map as usual.
func (s Select) Empty() bool { return len(s.Kinds) == 0 && s.Attr.Empty() && s.Order.Empty() }

// matches reports whether a node belongs to the named set.
func (s Select) matches(n *graph.Node) bool {
	if len(s.Kinds) > 0 && !s.Kinds[n.Kind] {
		return false
	}
	if !s.Attr.Empty() && n.Attrs[s.Attr.Key] != s.Attr.Value {
		return false
	}
	return true
}

// Search returns the top-k nodes most similar to query. kinds restricts results
// to those node kinds (nil/empty = any); repoFilter restricts to one repository.
//
// Default path is HNSW (O(log N) traversal of the ANN index); a kind/repo filter
// is applied AFTER the ANN fetch, so the fetch is widened to compensate for hits
// dropped by the filter. SetBruteForce switches to the exact O(N·D) scan for A/B
// comparison of recall and latency.
func (ix *Index) Search(ctx context.Context, query string, kinds map[string]bool, repoFilter string, sel Select, k int) ([]Hit, error) {
	t0 := time.Now()
	qv, err := ix.embedder.EmbedQuery(ctx, query)
	embedDur := time.Since(t0)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	t1 := time.Now()

	ix.mu.Lock()
	brute, ann, stats := ix.useBruteForce, ix.ann, ix.stats
	ix.mu.Unlock()

	// The caller named a SET. Resolve it from the graph, then rank inside it.
	// The similarity channels cannot do this: they choose candidates by the
	// query and only then drop the ones outside the set, so a member is returned
	// solely when the query happened to retrieve it — the reason searchHNSW
	// over-fetches, and the reason a predicate matching 30 entries could answer
	// with 2. The tool's own kinds stay a filter: scoping is not naming, and
	// selecting on every search would trade an O(log N) traversal for a
	// whole-graph scan.
	if !sel.Empty() {
		hits := ix.searchSelected(qv, kinds, repoFilter, sel, k)
		stats.recordSearch()
		stats.recordLatency(embedDur, time.Since(t1))
		publishHits(hits, query)
		return hits, nil
	}

	// Semantic channel. Over-fetch so fusion has depth to work with: an item
	// ranked 8th semantically and 1st lexically should still surface, which it
	// cannot if the semantic list was already truncated to k.
	semK := k * 3
	if semK < 20 {
		semK = 20
	}
	var sem []Hit
	if brute || ann == nil {
		sem = ix.searchBrute(qv, kinds, repoFilter, sel, semK)
	} else {
		sem = ix.searchHNSW(qv, ann, kinds, repoFilter, sel, semK)
	}
	stats.recordSemanticCall()

	q := normalizeQuery(query)
	lex, abstained, rejected := ix.searchLexical(query, qv, kinds, repoFilter, sel, semK)
	if abstained {
		stats.recordLexicalAbstention()
	}
	stats.recordCoverageRejections(rejected)

	exact := ix.searchExactTitle(q, qv, kinds, repoFilter, sel)

	if len(lex) == 0 && len(exact) == 0 {
		// No keyword signal (query was all stopwords, or nothing matched every
		// token) — semantic alone, unchanged from the pre-hybrid behaviour.
		if len(sem) > k {
			sem = sem[:k]
		}
		// Stamp the score the list is already ordered by, so a caller merging
		// maps ranks these against fused hits on one scale.
		for i := range sem {
			sem[i].Fused = 1.0 / float64(rrfK+i)
		}
		// Every returned hit is semantic-only by definition: the lexical
		// channel contributed nothing to this search.
		stats.recordFusion(0, len(sem), 0)
		stats.recordSearch()
		stats.recordLatency(embedDur, time.Since(t1))
		publishHits(sem, query)
		return sem, nil
	}
	hits, promotions, semanticOnly, both := fuseRRF(exact, lex, sem, k)
	stats.recordFusion(promotions, semanticOnly, both)
	stats.recordSearch()
	stats.recordLatency(embedDur, time.Since(t1))
	publishHits(hits, query)
	return hits, nil
}

// publishHits fans a search's results out over the hub WebSocket (GET /ws) as
// one HitEvent. This is the single choke point every search surface — the MCP
// tools (internal/mcpserver), the HTTP /search routes (cmd/xray-memory), and
// direct in-process callers — funnels through, so the live "graph in use"
// stream reflects every surface identically instead of drifting if each
// caller published its own event. A no-op when hits is empty, and cheaper
// still (see hub.broadcast) when nobody is subscribed.
func publishHits(hits []Hit, query string) {
	if len(hits) == 0 {
		return
	}
	qns := make([]string, len(hits))
	kinds := make([]string, len(hits))
	for i, h := range hits {
		qns[i] = h.QN
		kinds[i] = h.Kind
	}
	hub.PublishHit(hub.HitEvent{QNs: qns, Kinds: kinds, Query: query})
}

// CountMatching reports how many nodes the predicate selects, independent of any
// query or reply cap. A caller that names a set needs to know the set's real
// size: truncating a ranked list drops a less-relevant tail, but truncating a
// selection changes the answer, and doing that silently is indistinguishable
// from the set being that small.
func (ix *Index) CountMatching(kinds map[string]bool, repoFilter string, sel Select) int {
	n := 0
	for _, nd := range ix.g.Nodes() {
		if wants(nd, kinds, repoFilter, sel) {
			n++
		}
	}
	return n
}

// searchSelected ranks a set the caller NAMED rather than one a query found:
// every node matching the predicate is a candidate, ordered by similarity to the
// query. Ordering inside the set is by cosine alone — the set membership is the
// hard requirement and is already satisfied, so there is nothing for a keyword
// channel to promote a node past — UNLESS the caller named an attribute to
// order by: a superlative ("most experienced", "oldest incident") is a sort,
// not a similarity, and no similarity rank can answer it. The team map
// ranked kind=Student by cosine and led with an 11-year student while three
// 30-year students sat further down, so the caller had to read all 37 back.
// With an order the cosine only breaks ties. A node with no vector still
// belongs to the set and sorts last rather than being dropped, because it
// MATCHES; omitting it would put the query back in charge of membership.
func (ix *Index) searchSelected(qv []float32, kinds map[string]bool, repoFilter string, sel Select, k int) []Hit {
	var hits []Hit
	for _, n := range ix.g.Nodes() {
		if !wants(n, kinds, repoFilter, sel) {
			continue
		}
		var cos float32
		if n.Vec != nil {
			cos = cosine(qv, n.Vec)
		}
		hits = append(hits, ix.hit(n, cos))
	}
	sort.SliceStable(hits, func(i, j int) bool {
		if hits[i].Score != hits[j].Score {
			return hits[i].Score > hits[j].Score
		}
		return hits[i].QN < hits[j].QN
	})
	if !sel.Order.Empty() {
		hits = orderByAttr(hits, sel.Order, func(h Hit) (string, bool) {
			n, ok := ix.g.Get(h.QN)
			if !ok {
				return "", false
			}
			v, has := n.Attrs[sel.Order.Key]
			return v, has
		})
	}
	if len(hits) > k {
		hits = hits[:k]
	}
	// Stamp the score this list is ordered by, so a caller merging several maps
	// ranks these on the same scale as fused hits.
	for i := range hits {
		hits[i].Fused = 1.0 / float64(rrfK+i)
	}
	return hits
}

// orderByAttr sorts hits by an attribute the caller named, keeping the incoming
// (cosine) order among equal values. Two values compare numerically when BOTH
// parse as numbers — "8" must sort below "11", which a string compare denies —
// and as strings otherwise. A hit without the attribute sorts LAST in either
// direction: it belongs to the set, but it has no place in this order, and
// putting it first under desc would answer "most experienced" with a node that
// never said.
func orderByAttr(hits []Hit, o Order, attrOf func(Hit) (string, bool)) []Hit {
	type keyed struct {
		h     Hit
		v     string
		has   bool
		num   float64
		isNum bool
	}
	ks := make([]keyed, len(hits))
	for i, h := range hits {
		v, has := attrOf(h)
		k := keyed{h: h, v: v, has: has}
		if has {
			if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
				k.num, k.isNum = f, true
			}
		}
		ks[i] = k
	}
	sort.SliceStable(ks, func(i, j int) bool {
		a, b := ks[i], ks[j]
		if a.has != b.has {
			return a.has // the one with a value goes first
		}
		if !a.has {
			return false
		}
		var less bool
		switch {
		case a.isNum && b.isNum:
			if a.num == b.num {
				return false
			}
			less = a.num < b.num
		default:
			if a.v == b.v {
				return false
			}
			less = a.v < b.v
		}
		if o.Desc {
			return !less
		}
		return less
	})
	out := make([]Hit, len(ks))
	for i, k := range ks {
		out[i] = k.h
	}
	return out
}

// searchExactTitle returns the nodes whose title IS the query, normalized the
// same way on both sides. Typing a name verbatim is NAMING a node, not
// describing it, so it is a third channel rather than a filter or a post-hoc
// hoist: similarity and trigram overlap both rank a near-spelled neighbour above
// the node actually named when the two share a prefix. It votes on the same
// 1/(rrfK+rank) scale as the other channels, so it stays comparable across maps.
func (ix *Index) searchExactTitle(normQuery string, qv []float32, kinds map[string]bool, repoFilter string, sel Select) []Hit {
	if ix.lex == nil {
		return nil
	}
	qns := ix.lex.exactTitle(normQuery)
	if len(qns) == 0 {
		return nil
	}
	hits := make([]Hit, 0, len(qns))
	for _, qn := range qns {
		n, ok := ix.g.Get(qn)
		if !ok || !wants(n, kinds, repoFilter, sel) {
			continue
		}
		var cos float32
		if n.Vec != nil {
			cos = cosine(qv, n.Vec)
		}
		hits = append(hits, ix.hit(n, cos))
	}
	return hits
}

// searchLexical ranks nodes by keyword match: candidates must trigram-match at
// least minCoverage of the content-bearing query tokens, then are ordered by
// lexScore. Returns a nil hit list when the query carries no usable tokens,
// which makes the lexical channel abstain (reported via abstained=true) rather
// than vote arbitrarily; a nil hit list from a query that DID carry tokens but
// matched nothing is not an abstention (abstained=false) — the channel ran and
// came up empty, a different, unremarkable outcome. rejected is how many
// token-matching candidates the minCoverage gate then dropped.
//
// qv is the query vector, used only to stamp each hit with its true cosine so a
// result found by keyword alone still reports the same Score field as one found
// semantically. Without it, lexical-only hits would surface with Score 0 and
// callers reading Score as a similarity would see a hit that looks broken.
func (ix *Index) searchLexical(query string, qv []float32, kinds map[string]bool, repoFilter string, sel Select, k int) (hits []Hit, abstained bool, rejected int) {
	q := normalizeQuery(query)
	toks := kwTokens(q)
	if len(toks) == 0 {
		return nil, true, 0
	}
	if ix.lex == nil {
		return nil, false, 0
	}
	cands, rejected := ix.lex.candidates(tokenGrams(toks))
	if len(cands) == 0 {
		return nil, false, rejected
	}

	type scored struct {
		h     Hit
		score int
	}
	out := make([]scored, 0, len(cands))
	for qn, c := range cands {
		n, ok := ix.g.Get(qn)
		if !ok || !wants(n, kinds, repoFilter, sel) {
			continue
		}
		var cos float32
		if n.Vec != nil {
			cos = cosine(qv, n.Vec)
		}
		out = append(out, scored{
			h:     ix.hit(n, cos),
			score: lexScore(nodeTitle(n), nodeText(n), q, toks, c.matched, c.frac),
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		return out[i].h.QN < out[j].h.QN
	})
	if len(out) > k {
		out = out[:k]
	}
	hs := make([]Hit, 0, len(out))
	for _, s := range out {
		hs = append(hs, s.h)
	}
	return hs, false, rejected
}

// CutAtLargestGap drops the tail of a ranked list at its steepest score drop.
//
// Hits are scored by cosine, and on a corpus where documents resemble each other
// every node scores SOMETHING against every query: a query with one right answer
// still fills k with near-ties a few points below it. The signal is not the
// absolute score — that floor moves with the query — but where the list falls
// off a cliff. Keeping everything above the largest single drop adapts to how
// many answers the query actually has, which a fixed threshold cannot: measured
// over an 86-case suite it keeps ~2 hits for a one-answer query and ~9 for an
// eight-answer one, lifting precision on single-answer queries from 0.05 to 0.76.
//
// It trades recall for precision (1.00 to 0.84 on those same queries), so it is
// opt-in: a caller that wants the full ranked list still gets it.
func CutAtLargestGap(hits []Hit) []Hit {
	if len(hits) < 2 {
		return hits
	}
	// Ties go to the EARLIEST gap, and "tie" allows a small margin: a list that
	// declines evenly has no cliff at all, and float32 rounding would otherwise
	// pick an arbitrary one of its equal gaps (0.3-0.2 rounds larger than
	// 0.5-0.4). Cutting early there is both deterministic and the lean answer.
	const margin = 1.0001
	best, cut := float32(0), len(hits)
	for i := 0; i < len(hits)-1; i++ {
		if d := hits[i].Score - hits[i+1].Score; d > best*margin {
			best, cut = d, i+1
		}
	}
	return hits[:cut]
}

// AttrFilter narrows a search to nodes carrying an exact attribute value
// (NodeInfo.Attrs, set by catalog entries). The zero value matches everything.
//
// Deliberately ONE exact key=value with no operators, ranges or booleans: search
// is a single cheap call with no query language, and a caller that needs more
// than this should ask a better question rather than grow a filter DSL here.
type AttrFilter struct{ Key, Value string }

// ParseAttrFilter reads a "key=value" pair. An empty string yields the zero
// filter; a string with no "=" is an error rather than a silent no-op, so a
// malformed filter cannot quietly widen a search the caller meant to narrow.
func ParseAttrFilter(s string) (AttrFilter, error) {
	if s == "" {
		return AttrFilter{}, nil
	}
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return AttrFilter{}, fmt.Errorf("attr filter must be \"key=value\", got %q", s)
	}
	return AttrFilter{Key: k, Value: v}, nil
}

// Empty reports whether the filter narrows anything.
func (f AttrFilter) Empty() bool { return f.Key == "" }

// AttrVocabulary is what a scope's nodes say about themselves: which attribute
// keys they carry and, for one key the caller asked about, which values. It
// exists so a reply can NAME a vocabulary mismatch instead of silently ranking
// around it: the team map was asked for kind=Student order=experience:desc,
// no Student carries "experience", orderByAttr put every node in its "no
// attribute, sorts last" bucket and the set came back in cosine order — the
// reply looked healthy and the caller read an 8-year student as the most
// experienced when three 30-year students sat further down. A second call,
// attr=experience=30, returned 0 with no note at all.
type AttrVocabulary struct {
	// Nodes is the size of the scope. Zero means the note has nothing to say
	// about attributes (an unknown kind is already reported as such).
	Nodes int
	// Keys maps each attribute key to how many scope nodes carry it.
	Keys map[string]int
	// Values maps each distinct value of the key asked about to how many scope
	// nodes carry it; empty when no key was asked or none carries it.
	Values map[string]int
}

// AttrVocabulary scans the scope an order or attr would be applied to — the
// tool's kinds, the repo filter and the selection's KINDS, deliberately not its
// Attr, since the question is what that scope holds — and reports the keys its
// nodes carry and the values of one key. Membership goes through wants with the
// selection's attr blanked, so the scope can never diverge from the set the
// search itself ranks. A plain scan over the graph; it runs only when a reply
// needs a note, never on the search path.
func (ix *Index) AttrVocabulary(kinds map[string]bool, repoFilter string, sel Select, key string) AttrVocabulary {
	scope := Select{Kinds: sel.Kinds}
	v := AttrVocabulary{Keys: map[string]int{}, Values: map[string]int{}}
	for _, n := range ix.g.Nodes() {
		if !wants(n, kinds, repoFilter, scope) {
			continue
		}
		v.Nodes++
		for k, val := range n.Attrs {
			v.Keys[k]++
			if key != "" && k == key {
				v.Values[val]++
			}
		}
	}
	return v
}

// TitledNodes returns up to max nodes of the same scope AttrVocabulary scans
// whose title, or the last segment of whose qn, IS name — case-insensitive,
// and a leading '@' on either side is ignored. An attr filter whose KEY is
// unknown often carries a value that names a node rather than an attribute:
// the team map was asked four times for attr=github=octocat42, "github" is
// no attribute, and octocat42 is a Student's title (its qn ends ::octocat42)
// — 0 results, no note, four times. Plain scan, note path only.
func (ix *Index) TitledNodes(kinds map[string]bool, repoFilter string, sel Select, name string, max int) []*graph.Node {
	want := normalizeHandle(name)
	if want == "" || max <= 0 {
		return nil
	}
	scope := Select{Kinds: sel.Kinds}
	var out []*graph.Node
	for _, n := range ix.g.Nodes() {
		if !wants(n, kinds, repoFilter, scope) {
			continue
		}
		if normalizeHandle(nodeTitle(n)) == want || normalizeHandle(qnLastSegment(n.QN)) == want {
			out = append(out, n)
			if len(out) == max {
				break
			}
		}
	}
	return out
}

// normalizeHandle lower-cases a name and drops one leading '@', so a handle
// typed the way people write it matches the title a catalog stored.
func normalizeHandle(s string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(s), "@"))
}

// qnLastSegment is the symbol part of a qualified name: what follows the last
// "::" (repo//path::Symbol), else the last path element.
func qnLastSegment(qn string) string {
	if i := strings.LastIndex(qn, "::"); i >= 0 {
		return qn[i+2:]
	}
	if i := strings.LastIndex(qn, "/"); i >= 0 {
		return qn[i+1:]
	}
	return qn
}

// wants reports whether a node passes the kind/repo/attr filters.
func wants(n *graph.Node, kinds map[string]bool, repoFilter string, sel Select) bool {
	if len(kinds) > 0 && !kinds[n.Kind] {
		return false
	}
	if repoFilter != "" && n.Repo != repoFilter {
		return false
	}
	return sel.matches(n)
}

// searchHNSW traverses the ANN index. With a filter present it over-fetches
// (×4, min +20) so enough survivors remain after filtering to fill k. Only the
// tool's scope is compensated for here: a set the caller NAMED never reaches
// this path, because Search resolves it against the graph instead of hoping the
// ANN neighbourhood contains enough of it.
func (ix *Index) searchHNSW(qv []float32, ann *hnsw.HNSW[annNode], kinds map[string]bool, repoFilter string, sel Select, k int) []Hit {
	fetchK := k
	if len(kinds) > 0 || repoFilter != "" {
		// Over-fetch generously: when a kind filter is set (e.g. code-only),
		// the nearest ANN neighbours can ALL be the excluded kind (docs/sections),
		// which would filter down to zero. Fetch a large fixed floor so enough of
		// the requested kind survives to fill k. Cheap at this graph size.
		fetchK = k*4 + 20
		if fetchK < 200 {
			fetchK = 200
		}
	}
	q := annNode{Vec: vector.F32(qv)}
	neighbors := ann.Search(q, fetchK, 64)

	hits := make([]Hit, 0, k)
	for _, nb := range neighbors {
		n, ok := ix.g.Get(nb.QN)
		if !ok || n.Vec == nil {
			continue
		}
		if !wants(n, kinds, repoFilter, sel) {
			continue
		}
		hits = append(hits, ix.hit(n, cosine(qv, n.Vec)))
		if len(hits) >= k {
			break
		}
	}
	return hits
}

// searchBrute is the exact O(N·D) cosine scan over every embedded node — the
// correctness baseline the HNSW path is A/B tested against.
func (ix *Index) searchBrute(qv []float32, kinds map[string]bool, repoFilter string, sel Select, k int) []Hit {
	type scored struct {
		n     *graph.Node
		score float32
	}
	var all []scored
	for _, n := range ix.g.Nodes() {
		if n.Vec == nil || !wants(n, kinds, repoFilter, sel) {
			continue
		}
		all = append(all, scored{n: n, score: cosine(qv, n.Vec)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].score > all[j].score })
	if k > len(all) {
		k = len(all)
	}
	hits := make([]Hit, 0, k)
	for _, s := range all[:k] {
		hits = append(hits, ix.hit(s.n, s.score))
	}
	return hits
}

// hit renders a graph node + its similarity score as a search Hit. A child
// node's Parent is resolved through the graph's own "contains" edge rather than
// assembled from ParentName, because parents are keyed differently by kind (a
// Doc's QN is its path, a catalog entry's is path::name) and a guessed QN that
// misses reads as "that does not exist".
func (ix *Index) hit(n *graph.Node, score float32) Hit {
	return Hit{
		QN:       n.QN,
		Repo:     n.Repo,
		Kind:     n.Kind,
		Title:    nodeTitle(n),
		File:     n.FilePath,
		Line:     n.LineStart,
		Score:    score,
		Preview:  nodeText(n),
		Language: n.Language,
		Parent:   ix.parentOf(n),
	}
}

// parentOf returns the QN of the node that contains n, for a child node; "" for
// a top-level node or when no containing node carries n's ParentName (a file's
// "contains" edge to a plain symbol is not a parent in this sense).
func (ix *Index) parentOf(n *graph.Node) string {
	if n.ParentName == "" {
		return ""
	}
	for _, e := range ix.g.Callers(n.QN, "contains") {
		if p, ok := ix.g.Get(e.Source); ok && p.Name == n.ParentName {
			return p.QN
		}
	}
	return ""
}

// maxBodyEmbedChars bounds how much of a code node's Body is folded into its
// embedding text. The signature still leads (so exact-name/signature queries
// stay strong), but a bounded slice of the implementation lets PARAPHRASE
// queries — "per-user daily token quota reporting" → func UserQuota — match the
// intent expressed in the body, not just the signature tokens. Kept small so the
// body doesn't dominate the vector or blow the embedder's input window.
const maxBodyEmbedChars = 600

// nodeText is the embeddable representation of a node — what search matches on.
func nodeText(n *graph.Node) string {
	// Documentation nodes carry their own prose; embed it directly.
	if n.Content != "" {
		return n.Content
	}
	if n.Kind == "File" {
		return fmt.Sprintf("%s source file (%s)", n.FilePath, n.Language)
	}
	s := fmt.Sprintf("%s %s", n.Kind, nodeTitle(n))
	if n.Params != "" {
		s += n.Params
	}
	if n.ReturnType != "" {
		s += " " + n.ReturnType
	}
	s += " in " + n.FilePath
	// Fold a bounded slice of the body in AFTER the signature+path, so the
	// signature leads the vector but the implementation's vocabulary is also
	// matchable by semantic/paraphrase queries. Body is the clean declaration
	// source (set for Func/Test/Class/Type); empty for Doc/File nodes.
	if body := n.Body; body != "" {
		if len(body) > maxBodyEmbedChars {
			body = body[:maxBodyEmbedChars]
		}
		s += "\n" + body
	}
	return s
}

func nodeTitle(n *graph.Node) string {
	if n.Kind == "File" {
		return n.FilePath
	}
	return graph.DisplayTitle(n)
}

func cosine(a, b []float32) float32 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}
