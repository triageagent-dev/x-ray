package xray

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
	"github.com/triageagent-dev/x-ray/v2/internal/observability"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore/hub"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore/index"
)

// Upsert size caps bound an in-process item so one oversized write cannot
// exhaust pod memory. The MCP server bypasses the HTTP body limit, so these are
// enforced at the store boundary too.
const (
	maxTitleChars       = 1 << 10 // 1 KiB
	maxContentChars     = 1 << 18 // 256 KiB
	maxImageBase64Chars = 1 << 21 // 2 MiB of base64 (~1.5 MiB raster)
)

// defaultTTLByKind stamps a retention deadline on a newly created item that
// carries none: decay by default. Every kind not listed (stable_fact,
// operational_pattern, watch_item, system_identity, mental_map, episodic, ...) is
// left with no deadline and never expires on its own.
var defaultTTLByKind = map[string]time.Duration{
	"event":         30 * 24 * time.Hour,
	"recent_change": 90 * 24 * time.Hour,
	"note":          90 * 24 * time.Hour,
	// A hypothesis is a guess and an unknown a gap: both go stale unless
	// something renews them.
	"hypothesis": 24 * time.Hour,
	"unknown":    7 * 24 * time.Hour,
}

// defaultSemanticScoreGap is the default absolute cosine-similarity drop, below
// the best match the caller can see, past which semantic results are dropped as
// near-misses. Overridable with --semantic-score-gap.
const defaultSemanticScoreGap float32 = 0.02

// dedupKinds are the kinds a writer can restate under a fresh id: a restated
// fact lands on the cell it repeats instead of minting a peer.
var dedupKinds = map[string]bool{
	"stable_fact": true, "operational_pattern": true, "watch_item": true, "hypothesis": true,
}

// MemoryStore is the server's memory: the memstore engine (items, lexical and
// semantic search, rerank, links, provenance), a cell index when there is an
// embedder, and a hub that carries changes to connected clients, behind the
// method set the REST API, the MCP tools and the voice tools use. Items cross
// this boundary as MemoryItem, the wire shape.
type MemoryStore struct {
	*Telemetry

	eng      *memstore.Store
	cells    *index.CellStore // nil if no embedder
	embedder *index.Embedder  // nil in keyword mode
	logger   zerolog.Logger
	hub      *hub.Hub

	// instance is the identity of this instance, written as the Subject of a
	// private item, whose owner is its user: tag. Empty for a single-instance
	// server.
	instance string

	mu        sync.RWMutex
	searcher  *memstore.Searcher
	namespace string

	noWriteDedup atomic.Bool
	linkExpand   atomic.Bool

	// episodicTTL is the max age of an episodic memory before it is forgotten. 0
	// disables expiry. Read paths skip items older than this; the reaper deletes
	// their cells. Set once at startup.
	episodicTTL time.Duration
}

// NewMemoryStore creates a keyword-only MemoryStore (no embedding endpoint).
func NewMemoryStore() *MemoryStore { return newMemoryStore(nil, zerolog.Nop()) }

// NewMemoryStoreWithEmbedder creates a MemoryStore with a semantic index.
func NewMemoryStoreWithEmbedder(embedder *index.Embedder, logger zerolog.Logger) *MemoryStore {
	return newMemoryStore(embedder, logger)
}

func newMemoryStore(embedder *index.Embedder, logger zerolog.Logger) *MemoryStore {
	s := &MemoryStore{
		Telemetry: newTelemetry(logger),
		embedder:  embedder,
		logger:    logger,
		hub:       hub.New(nil),
	}
	if embedder != nil {
		s.cells = index.NewCellStore(embedder, logger)
		s.Telemetry.embedMetrics = func() (int64, int64) {
			m := embedder.Metrics()
			return m.EmbedCalls, m.CacheHits
		}
	}
	// The store's own check that a stable fact has high confidence is off: the
	// server enforces it where outside writers arrive (ValidateWrite), and its own
	// code writes facts as it likes.
	voc := memstore.BaseVocabulary()
	voc.FactConfidenceOptional = true
	s.eng = memstore.NewStore(memstore.WithConfig(memstore.Config{
		Vocabulary:   voc,
		DefaultTTL:   defaultTTLByKind,
		RenewOnMerge: true,
		NewID:        newID,
		SkipIDText:   isGeneratedID,
		Logger:       logger,
	}))
	s.eng.SetChangeHook(s.publishChange)
	s.searcher = memstore.NewSearcher(memstore.SearchConfig{
		Store:          s.eng,
		Cells:          func() *index.CellStore { return s.cells },
		LinkExpansion:  s.linkExpand.Load,
		SubjectRouting: true,
		Visible:        visibleToSubject,
		DocVisible:     docVisibleToSubject,
		SemanticGap:    defaultSemanticScoreGap,
		Stats:          telemetrySink{s.Telemetry},
		Logger:         logger,
	})
	s.Telemetry.count = s.Count
	return s
}

// visibleToSubject is the search's visibility rule: the "partition" the engine
// is given is the caller's subject. A search names no owner, so a private item is
// never visible to it; with a subject, only that subject's shared items are.
func visibleToSubject(it memstore.Item, subject string) bool {
	if memstore.IsPrivateItem(it) {
		return false
	}
	return subject == "" || strings.EqualFold(it.Subject, subject)
}

func docVisibleToSubject(doc index.IndexEntry, subject string) bool {
	if doc.Scope == memstore.ScopePrivate {
		return false
	}
	return subject == "" || strings.EqualFold(doc.Subject, subject)
}

// telemetrySink feeds the engine's search counters into the server's telemetry
// and its timings into OpenTelemetry.
type telemetrySink struct{ t *Telemetry }

func (k telemetrySink) Add(name string, n int64) {
	c := &k.t.counters
	switch name {
	case memstore.StatTotal:
		c.total.Add(n)
	case memstore.StatKeywordHits:
		c.kwHits.Add(n)
	case memstore.StatKWVoiceHits:
		c.kwVoiceHits.Add(n)
	case memstore.StatKWVoiceMisses:
		c.kwVoiceMisses.Add(n)
	case memstore.StatKWTrigramHits:
		c.kwTrigramHits.Add(n)
	case memstore.StatSemanticCalls:
		c.semCalls.Add(n)
	case memstore.StatSemanticFallbacks:
		c.semFallbacks.Add(n)
	default:
		return
	}
	k.t.markStatsDirty()
}

func (k telemetrySink) Observe(prefix string, d time.Duration) {
	ctx := context.Background()
	switch {
	case strings.HasPrefix(prefix, "search.mode."):
		observability.RecordMemorySearchDuration(ctx, strings.TrimPrefix(prefix, "search.mode."), d.Seconds())
	case prefix == "search.phase.embed" || prefix == "search.phase.index":
		observability.RecordMemorySearchPhase(ctx, strings.TrimPrefix(prefix, "search.phase."), d.Seconds())
	}
}

// ── switches ──────────────────────────────────────────────────────────

func (s *MemoryStore) reconfigure(mut func(*memstore.SearchConfig)) {
	s.mu.Lock()
	s.searcher = s.searcher.With(mut)
	s.mu.Unlock()
}

func (s *MemoryStore) srch() *memstore.Searcher {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.searcher
}

// SetSourceCollapse turns on source collapse for links of the given relation;
// "" turns it off. A hit with an outbound link of that relation is replaced by
// the item it links to (a fact by the episode it was drawn from).
func (s *MemoryStore) SetSourceCollapse(relation string) {
	s.reconfigure(func(c *memstore.SearchConfig) { c.SourceRelation = relation })
}

// SetCandidateFanout overrides how many times the limit each search channel
// pulls while source collapse will shrink the pool again. Values below 1 mean 1.
func (s *MemoryStore) SetCandidateFanout(n int) {
	if n < 1 {
		n = 1
	}
	s.reconfigure(func(c *memstore.SearchConfig) { c.Fanout = n })
}

// SetLexicalShortCircuit turns the lexical short-circuit on or off. On (the
// default of an unconfigured store), a confident top lexical hit returns without
// consulting the semantic channel; off, a search always fuses the two when there
// is a vector index. The right call for short named-entity queries is on, and
// for long natural-language questions asked over long cells it is off.
func (s *MemoryStore) SetLexicalShortCircuit(on bool) {
	s.reconfigure(func(c *memstore.SearchConfig) { c.AlwaysFuse = !on })
}

// SetRecallShaping turns the near-duplicate collapse and the state-aware rerank
// on or off (default on). Off, a search returns the fused order unchanged.
func (s *MemoryStore) SetRecallShaping(on bool) {
	s.reconfigure(func(c *memstore.SearchConfig) { c.NoShaping = !on })
}

// SetWriteDedup turns write-side near-duplicate merging on or off (default on):
// an Upsert that restates an existing stable_fact, operational_pattern,
// watch_item or hypothesis lands on that cell instead of minting a peer.
func (s *MemoryStore) SetWriteDedup(on bool) { s.noWriteDedup.Store(!on) }

// SetLinkExpansion turns the optional one-hop ThoughtLink expansion of search
// results on or off (default off).
func (s *MemoryStore) SetLinkExpansion(on bool) { s.linkExpand.Store(on) }

// SetSemanticScoreGap overrides the semantic relevance cutoff: the absolute
// cosine gap below the best visible hit past which semantic results are dropped.
// 0 disables the cutoff.
func (s *MemoryStore) SetSemanticScoreGap(gap float32) {
	s.reconfigure(func(c *memstore.SearchConfig) { c.SemanticGap = gap })
}

// SetEpisodicTTL sets the max age before an episodic memory is forgotten. 0
// disables expiry. Call once at startup before serving.
func (s *MemoryStore) SetEpisodicTTL(ttl time.Duration) { s.episodicTTL = ttl }

// SetCacheReader wires the cluster reader the stats flusher reads with.
func (s *MemoryStore) SetCacheReader(r client.Reader, namespace string) {
	s.mu.Lock()
	s.namespace = namespace
	s.mu.Unlock()
	s.Telemetry.setBackend(r, namespace)
}

// SetWriter wires the cluster client the stats flusher writes with.
func (s *MemoryStore) SetWriter(c client.Client) { s.Telemetry.setWriter(c) }

// ── events ────────────────────────────────────────────────────────────

// publishChange carries an upsert the engine made, here or from a cell another
// replica wrote, to connected clients. A shared item reaches every client; a
// private one reaches its owner only. A delete is published by the method that
// made it, which knows whose the item was.
func (s *MemoryStore) publishChange(op string, item *memstore.Item, _, _ string) {
	if op != "upsert" || item == nil || item.Kind == "system_configuration" {
		return
	}
	if !item.IsActive() {
		// A retired item leaves every client's view, as a deleted one does.
		s.publishDelete(*item)
		return
	}
	evt := hub.ChangeEvent{Op: "upsert", Item: viewOf(*item)}
	if memstore.IsPrivateItem(*item) {
		if owner := memstore.PrivateOwner(*item); owner != "" {
			s.hub.PublishChangeTo(owner, evt)
		}
		return
	}
	s.hub.PublishChange(evt)
}

// publishDelete tells connected clients an item is gone: its owner alone if it
// was private, so a bare id does not reveal that a note existed.
func (s *MemoryStore) publishDelete(it memstore.Item) {
	evt := hub.ChangeEvent{Op: "delete", ID: it.ID, Kind: it.Kind}
	if memstore.IsPrivateItem(it) {
		if owner := memstore.PrivateOwner(it); owner != "" {
			s.hub.PublishChangeTo(owner, evt)
		}
		return
	}
	s.hub.PublishChange(evt)
}

// PublishHit broadcasts a memory search hit event. kinds must be parallel to
// itemIDs. The event reaches every connected client, so it carries ids, kinds
// and the tags of shared items only: the query is what a person asked and is not
// theirs to share, and callers pass shared items only (see sharedHit). The tags
// let the graph place a hit in a category split off its kind by tag (ADR-035).
func (s *MemoryStore) PublishHit(itemIDs []string, kind string, kinds []string) {
	// The tags are re-read from the store on purpose: this is the one place that
	// decides what the shared stream may name, whatever the caller passed.
	tags := make([][]string, len(itemIDs))
	tagged := false
	for i, id := range itemIDs {
		if it := s.Get(id); it != nil && !isPrivateItem(it.Kind, it.Scope) && len(it.Tags) > 0 {
			tags[i], tagged = it.Tags, true
		}
	}
	if !tagged {
		tags = nil // a store without tags keeps the event as it was
	}
	s.hub.PublishHit(hub.HitEvent{ItemIDs: itemIDs, Kind: kind, Kinds: kinds, Tags: tags})
}

// sharedHit publishes a hit for the shared items among items. A private item's
// id is not named on the stream that every client reads.
func (s *MemoryStore) sharedHit(items []*MemoryItem, kind string) {
	ids, kinds := make([]string, 0, len(items)), make([]string, 0, len(items))
	for _, it := range items {
		if !isPrivateItem(it.Kind, it.Scope) {
			ids, kinds = append(ids, it.ID), append(kinds, it.Kind)
		}
	}
	if len(ids) > 0 {
		s.PublishHit(ids, kind, kinds)
	}
}

// Hub is the hub that carries memory events to connected clients.
func (s *MemoryStore) Hub() *hub.Hub { return s.hub }

// ── search ────────────────────────────────────────────────────────────

// Search returns items matching the query. It runs lexical search first; when a
// semantic index is configured and the top lexical hit is not a confident match
// it fuses semantic results in by rank fusion, then reranks. With a subject, the
// subject's shared items are searched and so are the private notes of the person
// the subject names (lexically: a private note is never embedded). A query that
// is a cell's id goes first.
func (s *MemoryStore) Search(ctx context.Context, query, kind, subject string, limit int) []*MemoryItem {
	if limit <= 0 {
		limit = 10
	}
	if query == "" {
		return nil
	}
	results := s.srch().HybridSearch(ctx, query, kind, subject, subject, limit)
	out := make([]*MemoryItem, 0, len(results)+1)
	seen := make(map[string]struct{}, len(results)+1)
	add := func(v *MemoryItem) {
		if _, dup := seen[v.ID]; dup || len(out) >= limit {
			return
		}
		seen[v.ID] = struct{}{}
		out = append(out, v)
	}
	if hit := s.ownersExactIDMatch(query, kind, subject); hit != nil {
		add(hit)
	}
	shared := make([]*MemoryItem, 0, len(results))
	for _, r := range results {
		v := viewOf(r.Item)
		v.Via = r.Via
		v.Collapsed = r.CollapsedCount
		shared = append(shared, v)
	}
	var own []*MemoryItem
	if subject != "" {
		own = views(s.eng.KeywordSearchOwn(subject, query, kind, limit))
	}
	for _, v := range interleave(shared, own) {
		add(v)
	}
	return out
}

// interleave merges two ranked lists alternately, the first list's item ahead at
// each rank. The lists are ranked by different means (the shared list by fused
// lexical and semantic relevance, the person's private notes by lexical match
// alone), so their scores cannot be compared; alternating keeps either from
// crowding the other out of a limit.
func interleave(first, second []*MemoryItem) []*MemoryItem {
	out := make([]*MemoryItem, 0, len(first)+len(second))
	for i := 0; i < len(first) || i < len(second); i++ {
		if i < len(first) {
			out = append(out, first[i])
		}
		if i < len(second) {
			out = append(out, second[i])
		}
	}
	return out
}

// ownersExactIDMatch is the exact-id promotion for a private item: the search
// engine never promotes one, since it names no caller, but its owner pasting its
// id means that cell.
func (s *MemoryStore) ownersExactIDMatch(query, kind, subject string) *MemoryItem {
	id := strings.TrimSpace(query)
	if subject == "" || id == "" || len(id) > 256 || strings.ContainsAny(id, " \t\r\n") {
		return nil
	}
	it, ok := s.eng.Get(id)
	if !ok || !it.IsActive() || !memstore.IsPrivateItem(it) || memstore.PrivateOwner(it) != subject {
		return nil
	}
	if kind != "" && it.Kind != kind {
		return nil
	}
	return viewOf(it)
}

// KeywordSearch scans active items by lexical match on title and content. With a
// subject it also covers that person's private notes.
func (s *MemoryStore) KeywordSearch(query, kind, subject string, limit int) []*MemoryItem {
	if subject == "" {
		return views(s.eng.KeywordSearch(query, kind, "", limit))
	}
	// The subject narrows shared items; the person's own private notes are
	// owned by it, and their Subject names the instance.
	pool := s.eng.KeywordSearchForUser(subject, query, kind, "", limit*4)
	out := make([]memstore.Item, 0, limit)
	for _, it := range pool {
		if len(out) >= limit {
			break
		}
		if memstore.IsPrivateItem(it) || strings.EqualFold(it.Subject, subject) {
			out = append(out, it)
		}
	}
	return views(out)
}

// KeywordSearchPerKind is KeywordSearch with per-kind fairness: at most perKind
// results per kind, so a dominant kind cannot monopolise the limit and starve
// smaller categories. Used by the graph search, which spans all kinds over a
// skewed store.
func (s *MemoryStore) KeywordSearchPerKind(query, kind, subject string, limit, perKind int) []*MemoryItem {
	return views(s.eng.KeywordSearchPerKind(query, kind, subject, limit, perKind))
}

// ── writes ────────────────────────────────────────────────────────────

// UpsertOption sets an optional field of an Upsert.
type UpsertOption func(*upsertOpts)

type upsertOpts struct {
	evidenceRefs []string
	supersedes   []string
	noDedup      bool
	staleAfter   *time.Time
}

// withStaleAfter sets the item's deadline explicitly: on a create it wins over
// the per-kind default, on an update it renews the deadline. Without it an
// update leaves the stored deadline as it is.
func withStaleAfter(t time.Time) UpsertOption {
	return func(o *upsertOpts) { o.staleAfter = &t }
}

// WithEvidenceRefs records the cells or events the item was drawn from. Refute
// follows them.
func WithEvidenceRefs(refs []string) UpsertOption {
	return func(o *upsertOpts) { o.evidenceRefs = refs }
}

// UpsertResult is the outcome of an Upsert: the id, whether the item was
// created, updated or merged into the cell it restated, and the stored item.
type UpsertResult struct {
	ID     string      `json:"id"`
	Status string      `json:"status"` // "created", "updated" or "merged"
	Item   *MemoryItem `json:"-"`
}

// Upsert creates or updates a memory item matched by ID (if provided) or
// auto-generates one. The write's provenance comes from ctx (withWriteOrigin): an
// identified person is human, anything else derived. A restated stable_fact,
// operational_pattern, watch_item or hypothesis lands on the cell it repeats
// (status "merged"). The owner of a private item is its subject.
func (s *MemoryStore) Upsert(ctx context.Context, id, kind, scope, title, content, subject, confidence, eventDate string, data map[string]mcell.DataEntry, tags []string, opts ...UpsertOption) (UpsertResult, string) {
	if kind == "" || scope == "" || content == "" {
		return UpsertResult{}, "kind, scope, and content are required"
	}
	var o upsertOpts
	for _, opt := range opts {
		opt(&o)
	}
	if msg := writeBoundary(kind, title, content, data); msg != "" {
		return UpsertResult{}, msg
	}
	return s.write(ctx, id, kind, scope, title, content, subject, confidence, eventDate, data, tags, o)
}

// writeBoundary checks what any write must satisfy. It bounds item size, since
// the store is in process with no global cap and the MCP server bypasses the HTTP
// body limit (separate port), and refuses a second system_configuration, a
// singleton per subject that would poison the session config.
func writeBoundary(kind, title, content string, data map[string]mcell.DataEntry) string {
	if len(title) > maxTitleChars {
		return fmt.Sprintf("title exceeds %d characters", maxTitleChars)
	}
	if len(content) > maxContentChars {
		return fmt.Sprintf("content exceeds %d characters", maxContentChars)
	}
	if img, ok := data["image"]; ok && len(img.Bytes) > maxImageBase64Chars {
		return fmt.Sprintf("image exceeds %d bytes (base64)", maxImageBase64Chars)
	}
	if kind == "system_configuration" {
		return "system_configuration items are managed via CR; use kubectl to update"
	}
	return ""
}

func (s *MemoryStore) write(ctx context.Context, id, kind, scope, title, content, subject, confidence, eventDate string, data map[string]mcell.DataEntry, tags []string, o upsertOpts) (UpsertResult, string) {
	private := scope == memstore.ScopePrivate
	incoming := writeOriginFrom(ctx)
	if incoming == "" {
		incoming = OriginDerived
	}

	var it memstore.Item
	prior, hasPrior := memstore.Item{}, false
	if id != "" {
		prior, hasPrior = s.eng.Get(id)
	}
	if hasPrior && memstore.IsPrivateItem(prior) && memstore.PrivateOwner(prior) != subject {
		// A private item belongs to its owner. Another caller cannot overwrite it,
		// republish it or take it over by naming its id.
		return UpsertResult{}, "id is not available"
	}
	if hasPrior {
		it = prior
		it.Kind, it.Scope, it.Title, it.Content = kind, scope, title, content
		it.Confidence, it.EventDate = confidence, eventDate
		if data != nil {
			it.Data = data
		}
		if tags != nil {
			it.Tags = tags
		}
		if o.evidenceRefs != nil {
			it.EvidenceRefs = o.evidenceRefs
		}
		if o.supersedes != nil {
			it.Supersedes = mergeTags(it.Supersedes, o.supersedes)
		}
	} else {
		it = memstore.Item{MemoryCellSpec: mcell.MemoryCellSpec{
			ID: id, Kind: kind, Scope: scope, Title: title, Content: content,
			Confidence: confidence, EventDate: eventDate, Data: data, Tags: tags,
			EvidenceRefs: o.evidenceRefs, Supersedes: o.supersedes,
		}}
	}
	it.Origin = incoming
	if o.staleAfter != nil {
		it.StaleAfter = timePtrToMetaTime(o.staleAfter)
	}
	if private {
		it.Subject = s.instance
		if subject != "" && memstore.PrivateOwner(it) != subject {
			it.Tags = append(stripOwnerTags(it.Tags), memstore.PrivateOwnerTagPrefix+subject)
		}
	} else {
		it.Subject = subject
	}

	var res memstore.Item
	var mergedInto, errMsg string
	if !hasPrior && !o.noDedup && dedupKinds[kind] && !private && !s.noWriteDedup.Load() {
		res, mergedInto, errMsg = s.eng.UpsertDedup(it)
	} else {
		res, errMsg = s.eng.Upsert(it)
	}
	if errMsg != "" {
		return UpsertResult{}, errMsg
	}
	status := "created"
	if hasPrior {
		status = "updated"
	}
	if mergedInto != "" {
		status = "merged"
	}

	if s.cells != nil && !private {
		embedCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := s.cells.Index(embedCtx, entryOf(res)); err != nil {
			s.logger.Warn().Err(err).Str("id", res.ID).Msg("embed failed")
		}
	}
	return UpsertResult{ID: res.ID, Status: status, Item: viewOf(res)}, ""
}

func stripOwnerTags(tags []string) []string {
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		if !strings.HasPrefix(t, memstore.PrivateOwnerTagPrefix) {
			out = append(out, t)
		}
	}
	return out
}

func entryOf(it memstore.Item) index.IndexEntry {
	return index.IndexEntry{
		ID:         it.ID,
		Kind:       it.Kind,
		Scope:      it.Scope,
		Subject:    it.Subject,
		Title:      it.Title,
		Content:    it.Content,
		Confidence: it.Confidence,
		Tags:       it.Tags,
		Status:     "active",
		EventDate:  it.EventDate,
	}
}

func mergeTags(a, b []string) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, list := range [][]string{a, b} {
		for _, t := range list {
			if _, dup := seen[t]; dup {
				continue
			}
			seen[t] = struct{}{}
			out = append(out, t)
		}
	}
	return out
}

// isGeneratedID reports whether id is one newID produced: 16 hex digits, which
// say nothing and are left out of the text an item is searched by.
func isGeneratedID(id string) bool {
	if len(id) != 16 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

// SetLinks replaces the ThoughtLinks of an item. It is the write path for links
// on items that were never persisted: an ingest pipeline deriving atomic-fact
// cells from an episode links each fact back to its source with it. It reports
// false when id names no item.
func (s *MemoryStore) SetLinks(id string, links []mcell.ThoughtLink) bool {
	return s.eng.Mutate(id, func(it *memstore.Item) bool {
		it.Links = append([]mcell.ThoughtLink(nil), links...)
		return true
	})
}

// Delete removes a memory item by ID. It reports whether the item was held. The
// vector goes too, whether or not the item was held here: a vector left behind
// keeps taking top-k slots. A private item's delete reaches only its owner.
func (s *MemoryStore) Delete(id string) bool {
	it, held := s.eng.Get(id)
	s.eng.Delete(id)
	if s.cells != nil {
		_ = s.cells.Delete(context.Background(), id)
	}
	if held {
		s.publishDelete(it)
	} else {
		s.hub.PublishChange(hub.ChangeEvent{Op: "delete", ID: id})
	}
	return held
}

// retired takes a retired item out of the vector index and tells its audience.
func (s *MemoryStore) retired(it memstore.Item) {
	if s.cells != nil {
		_ = s.cells.Delete(context.Background(), it.ID)
	}
	s.publishDelete(it)
}

// Expire retires a cell without a verdict on it: it is out of date, not wrong
// (Refute is for wrong). The cell is kept with status stale and leaves every
// index. The returned item is already stored in the process; a caller backed by
// the cluster persists it. nil when id names no cell.
func (s *MemoryStore) Expire(id string) *MemoryItem {
	id = strings.TrimSpace(id)
	before, ok := s.eng.Get(id)
	if !ok || !s.eng.Expire(id) {
		return nil
	}
	s.retired(before)
	after, _ := s.eng.Get(id)
	return viewOf(after)
}

// Supersede replaces the cell oldID with a new one: the new cell is written (as
// Upsert writes it, so origin, id handling and size caps apply) and lists oldID
// in Supersedes, and the old cell is retired with status superseded. A derived
// write cannot supersede a human cell. The returned items are the new cell then
// the old one, both already stored in the process.
func (s *MemoryStore) Supersede(ctx context.Context, oldID, newID, kind, scope, title, content, subject, confidence string, tags []string, opts ...UpsertOption) (changed []*MemoryItem, errMsg string) {
	old, ok := s.eng.Get(strings.TrimSpace(oldID))
	if !ok {
		return nil, "not_found"
	}
	if newID == old.ID {
		return nil, "the replacement must have a different id from the cell it supersedes"
	}
	origin := writeOriginFrom(ctx)
	if origin == "" {
		origin = OriginDerived
	}
	if effectiveOrigin(old.Origin) == OriginHuman && origin == OriginDerived {
		return nil, errDerivedReplacesHuman
	}
	var o upsertOpts
	for _, opt := range opts {
		opt(&o)
	}
	// Dedup is off for this write: the caller named the cell being replaced, so a
	// near-duplicate of that same cell must not swallow its replacement.
	o.noDedup = true
	o.supersedes = mergeTags(o.supersedes, []string{old.ID})
	if kind == "" || scope == "" || content == "" {
		return nil, "kind, scope, and content are required"
	}
	if msg := writeBoundary(kind, title, content, nil); msg != "" {
		return nil, msg
	}
	res, errMsg := s.write(ctx, newID, kind, scope, title, content, subject, confidence, "", nil, tags, o)
	if errMsg != "" {
		return nil, errMsg
	}
	s.eng.MarkSuperseded(old.ID, res.ID)
	s.retired(old)
	oldAfter, _ := s.eng.Get(old.ID)
	newer, _ := s.eng.Get(res.ID)
	return []*MemoryItem{viewOf(newer), viewOf(oldAfter)}, ""
}

// Refute marks a cell wrong: it records {reason, evidence, at} under
// Data["refuted"], retires the cell (status stale) and retires every active
// derived cell that cites it, one hop only: an evidence ref naming the id or a
// link to it. Observed, human and verified cells are never cascaded. The
// returned items are the changed cells, the refuted one first, already stored in
// the process; a caller backed by the cluster persists each. found is false when
// id names no cell.
func (s *MemoryStore) Refute(ctx context.Context, id, reason string, evidence []string) (changed []*MemoryItem, found bool) {
	id = strings.TrimSpace(id)
	refuted, cascaded := s.eng.Refute(id, reason, evidence, time.Now())
	if !refuted {
		return nil, false
	}
	for _, cid := range append([]string{id}, cascaded...) {
		// A retired item stays in the store with its scope and owner, which is
		// all the delete event needs.
		after, _ := s.eng.Get(cid)
		s.retired(after)
		changed = append(changed, viewOf(after))
	}
	return changed, true
}

// ConsolidationGroup is one near-duplicate cluster found by ConsolidateMemory:
// Keep is the id a reader should trust going forward, Superseded lists every
// other member's id (parallel to Scores, each member's score against Keep).
type ConsolidationGroup struct {
	Kind       string    `json:"kind"`
	Keep       string    `json:"keep"`
	KeepTitle  string    `json:"keepTitle"`
	Superseded []string  `json:"superseded"`
	Scores     []float64 `json:"scores"`
}

// ConsolidationReport is the outcome of one ConsolidateMemory sweep.
type ConsolidationReport struct {
	Scanned    int                  `json:"scanned"`
	Groups     []ConsolidationGroup `json:"groups"`
	Superseded int                  `json:"superseded"`
	Applied    bool                 `json:"applied"`
}

// ConsolidateMemory groups the active shared cells of the dedup kinds (optionally
// of one subject) into near-duplicate clusters and, when apply is true, retires
// every member but each cluster's keeper (status superseded, the keeper's
// Supersedes lists them). apply=false returns the same groups and writes
// nothing. The second result is the changed cells, which a caller backed by the
// cluster persists.
func (s *MemoryStore) ConsolidateMemory(subject string, apply bool) (ConsolidationReport, []*MemoryItem) {
	report := ConsolidationReport{Applied: apply}
	var changed []*MemoryItem
	for _, kind := range []string{"stable_fact", "operational_pattern", "watch_item", "hypothesis"} {
		var items []memstore.Item
		for _, it := range s.eng.Filtered("active", kind, "", subject, "", 0) {
			if !memstore.IsPrivateItem(it) && (subject == "" || it.Subject == subject) {
				items = append(items, it)
			}
		}
		report.Scanned += len(items)
		if len(items) < 2 {
			continue
		}
		sort.SliceStable(items, func(i, j int) bool {
			if !items[i].UpdatedAt.Equal(&items[j].UpdatedAt) {
				return items[i].UpdatedAt.After(items[j].UpdatedAt.Time)
			}
			return items[i].ID < items[j].ID
		})
		for _, cluster := range memstore.ClusterNearDuplicates(items) {
			if len(cluster) < 2 {
				continue
			}
			keep := cluster[0]
			for _, c := range cluster[1:] {
				if memstore.BetterRoutingFact(c, keep) {
					keep = c
				}
			}
			group := ConsolidationGroup{Kind: kind, Keep: keep.ID, KeepTitle: keep.Title}
			for _, c := range cluster {
				if c.ID == keep.ID {
					continue
				}
				score, _ := memstore.NearDuplicateMemory(keep, c)
				group.Superseded = append(group.Superseded, c.ID)
				group.Scores = append(group.Scores, score)
			}
			report.Groups = append(report.Groups, group)
			report.Superseded += len(group.Superseded)
			if !apply {
				continue
			}
			for _, id := range group.Superseded {
				if c, ok := s.eng.Get(id); ok {
					s.eng.MarkSuperseded(id, keep.ID)
					s.retired(c)
					after, _ := s.eng.Get(id)
					changed = append(changed, viewOf(after))
				}
			}
			kept, _ := s.eng.Get(keep.ID)
			changed = append(changed, viewOf(kept))
		}
	}
	return report, changed
}

// ScoredItem is a cell reached by Context and how strongly.
type ScoredItem struct {
	Item  *MemoryItem
	Score float32
}

// Context returns the cells an anchor activates: the anchor cells themselves and
// everything reachable over ThoughtLinks, by spreading activation. Each hop
// multiplies the score by 0.6 and the link's weight, a cell keeps the best score
// any path gave it, and at most maxDepth hops are followed (default 3, at most
// 8). Only shared, active cells take part.
func (s *MemoryStore) Context(anchor string, maxDepth int) (anchors []*MemoryItem, activated []ScoredItem) {
	if maxDepth > 8 {
		maxDepth = 8
	}
	a, act := s.eng.Context(anchor, maxDepth)
	anchors = views(a)
	for _, r := range act {
		activated = append(activated, ScoredItem{Item: viewOf(r.Item), Score: r.Score})
	}
	return anchors, activated
}

// ── reads ─────────────────────────────────────────────────────────────

// GetItem returns a memory item by ID in any status, or nil if not found.
func (s *MemoryStore) GetItem(id string) *MemoryItem {
	it, ok := s.eng.Get(id)
	if !ok {
		return nil
	}
	return viewOf(it)
}

// Get returns a single active memory item by ID, or nil if not found.
func (s *MemoryStore) Get(id string) *MemoryItem {
	it, ok := s.eng.Get(id)
	if !ok || !it.IsActive() {
		return nil
	}
	return viewOf(it)
}

// matching returns the active items filtered by kind, scope and subject: the
// shared ones, whose Subject is the subject, and, when a subject is given, the
// private ones that subject owns. Without a subject no private item is returned.
func (s *MemoryStore) matching(kind, scope, subject string) []memstore.Item {
	out := s.eng.Filtered("active", kind, scope, "", "", 0)
	kept := out[:0]
	for _, it := range out {
		if it.Kind == "system_configuration" || (subject != "" && it.Subject != subject) {
			continue
		}
		kept = append(kept, it)
	}
	out = kept
	if subject != "" {
		for _, it := range s.eng.ForUser(subject, "active", kind, scope, "", "", 0) {
			if memstore.IsPrivateItem(it) {
				out = append(out, it)
			}
		}
		// Shared and private items are two lists; one order puts the newest first
		// so a limit does not cut off the private ones.
		sort.SliceStable(out, func(i, j int) bool {
			if !out[i].UpdatedAt.Equal(&out[j].UpdatedAt) {
				return out[i].UpdatedAt.After(out[j].UpdatedAt.Time)
			}
			return out[i].ID < out[j].ID
		})
	}
	return out
}

// List returns active items filtered by kind, scope and subject up to limit.
func (s *MemoryStore) List(kind, scope, subject string, limit int) []*MemoryItem {
	items := s.matching(kind, scope, subject)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return views(items)
}

// listRecentCap bounds the candidate set ListRecent fetches before sorting.
const listRecentCap = 500

// ListRecent returns active items matching kind/scope/subject sorted newest-first
// by CreatedAt (UpdatedAt breaks ties), capped to limit. Unlike List, the result
// is time-ordered: this is the recall path for episodic ("what happened last")
// memory, where "the latest creation time" is the selection criterion.
func (s *MemoryStore) ListRecent(kind, scope, subject string, limit int) []*MemoryItem {
	if limit <= 0 {
		return nil
	}
	all := s.List(kind, scope, subject, listRecentCap)
	// Forget expired episodic memories at read time: a stale memory must not
	// surface even before the reaper deletes its cell.
	if kind == "episodic" {
		kept := all[:0]
		for _, it := range all {
			if !s.episodicExpired(it) {
				kept = append(kept, it)
			}
		}
		all = kept
	}
	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.After(all[j].CreatedAt)
		}
		return all[i].UpdatedAt.After(all[j].UpdatedAt)
	})
	if len(all) > limit {
		all = all[:limit]
	}
	return all
}

// ListSummary is List with light rows and the total that matched.
func (s *MemoryStore) ListSummary(kind, scope, subject string, limit int) (items []MemoryItemSummary, total int) {
	all := s.matching(kind, scope, subject)
	total = len(all)
	for _, it := range all {
		if len(items) >= limit {
			break
		}
		items = append(items, s.summaryOf(it))
	}
	return items, total
}

func (s *MemoryStore) summaryOf(it memstore.Item) MemoryItemSummary {
	v := viewOf(it)
	return MemoryItemSummary{
		ID: v.ID, Kind: v.Kind, Scope: v.Scope, Subject: v.Subject, Title: v.Title, Tags: v.Tags,
		Status: v.Status, UpdatedAt: v.UpdatedAt, HitCount: s.hub.HitCount(it.ID),
	}
}

// ListSummaryPerKind is ListSummary over the shared items with at most perKind
// rows of any one kind, and the true total of each kind: the UI shows those as
// the real category counts, independent of the per-kind render cap.
func (s *MemoryStore) ListSummaryPerKind(perKind int) (items []MemoryItemSummary, total int, totalsByKind map[string]int) {
	items, total, totalsByKind, _ = s.ListSummaryPerCategory(perKind, nil)
	return items, total, totalsByKind
}

// ListSummaryPerCategory is ListSummaryPerKind with the cap and the totals taken
// per display category (ADR-035): a category configured with a tag narrows its
// kind, so a kind split by tag into several categories gets perCategory rows for
// each of them instead of perCategory rows to share. An item's category key is
// "kind|tag" for the first tag of its kind's tagged categories it carries (the
// UI's categoryOf order), else its kind. Without tagged categories this is
// ListSummaryPerKind.
func (s *MemoryStore) ListSummaryPerCategory(perCategory int, categories []CategoryConfig) (items []MemoryItemSummary, total int, totalsByKind, totalsByCategory map[string]int) {
	keyer := categoryKeyer(categories)
	key := func(it MemoryItemSummary) string { return keyer(it.Kind, it.Tags) }
	all, _ := s.ListSummary("", "", "", 1<<30)
	totalsByKind = make(map[string]int, 16)
	totalsByCategory = make(map[string]int, 16)
	for _, it := range all {
		totalsByKind[it.Kind]++
		totalsByCategory[key(it)]++
	}
	total = len(all)
	if perCategory <= 0 {
		return all, total, totalsByKind, totalsByCategory
	}
	perCategoryCount := make(map[string]int, 16)
	for _, it := range all {
		k := key(it)
		if perCategoryCount[k] >= perCategory {
			continue
		}
		perCategoryCount[k]++
		items = append(items, it)
	}
	return items, total, totalsByKind, totalsByCategory
}

// categoryKeyer returns an item's display category key (ADR-035, the UI's
// categoryOf order): "kind|tag" for the first tagged category of its kind whose
// tag it carries, else its kind.
func categoryKeyer(categories []CategoryConfig) func(kind string, tags []string) string {
	tagsByKind := make(map[string][]string)
	for _, c := range categories {
		if c.Tag != "" {
			tagsByKind[c.Kind] = append(tagsByKind[c.Kind], c.Tag)
		}
	}
	return func(kind string, tags []string) string {
		for _, tag := range tagsByKind[kind] {
			if slices.Contains(tags, tag) {
				return kind + "|" + tag
			}
		}
		return kind
	}
}

// FilterItems returns the active items of a subject and kind (empty matches
// all), whatever their scope, but never the system configuration.
func (s *MemoryStore) FilterItems(subject, kind string) []*MemoryItem {
	var out []*MemoryItem
	for _, it := range s.eng.AllItems() {
		if !it.IsActive() || it.Kind == "system_configuration" {
			continue
		}
		if kind != "" && it.Kind != kind {
			continue
		}
		v := viewOf(it)
		if subject != "" && v.Subject != subject {
			continue
		}
		out = append(out, v)
	}
	return out
}

// AllItems returns the active shared items of a subject (empty matches all).
func (s *MemoryStore) AllItems(subject string) []*MemoryItem {
	var out []*MemoryItem
	for _, it := range s.eng.Items() {
		if !it.IsActive() {
			continue
		}
		if subject != "" && it.Subject != subject {
			continue
		}
		out = append(out, viewOf(it))
	}
	return out
}

// KnowledgeIndex lists the titles of the shared active items of a subject.
func (s *MemoryStore) KnowledgeIndex(subject string) []string {
	items := s.AllItems(subject)
	seen := make(map[string]struct{}, len(items))
	var titles []string
	for _, it := range items {
		if _, ok := seen[it.Title]; !ok {
			seen[it.Title] = struct{}{}
			titles = append(titles, it.Title)
		}
	}
	return titles
}

// Count is the number of active items.
func (s *MemoryStore) Count() int { return s.eng.CountActive() }

// EmbedMetrics returns the embedder's call and cache counters.
func (s *MemoryStore) EmbedMetrics() index.EmbedMetrics {
	if s.embedder == nil {
		return index.EmbedMetrics{}
	}
	return s.embedder.Metrics()
}

// MemoryNeighbor is one nearest cell of a cell, with its similarity.
type MemoryNeighbor struct {
	ID     string   `json:"id"`
	Kind   string   `json:"kind"`
	Title  string   `json:"title"`
	Status string   `json:"status"`
	Tags   []string `json:"tags,omitempty"`
	Score  float32  `json:"score"`
}

// MemoryNeighborEdge links two neighbours that sit close to each other.
type MemoryNeighborEdge struct {
	Source string  `json:"source"`
	Target string  `json:"target"`
	Score  float32 `json:"score"`
}

// MemoryNeighborhood is the nearest cells of a cell and the edges between them.
type MemoryNeighborhood struct {
	ID        string               `json:"id"`
	Indexed   bool                 `json:"indexed"`
	Neighbors []MemoryNeighbor     `json:"neighbors"`
	Edges     []MemoryNeighborEdge `json:"edges"`
}

// memoryNeighborEdgesPerNeighbor is how many neighbour-to-neighbour links each
// neighbour keeps.
const memoryNeighborEdgesPerNeighbor = 3

// Neighborhood returns the top nearest shared cells of a cell and the edges
// between them. Indexed is false when the cell is not in the vector index.
func (s *MemoryStore) Neighborhood(id string, top int) *MemoryNeighborhood {
	out := &MemoryNeighborhood{ID: id, Neighbors: []MemoryNeighbor{}, Edges: []MemoryNeighborEdge{}}
	if s.cells == nil {
		return out
	}
	if top <= 0 {
		top = 20
	}
	if top > 50 {
		top = 50
	}
	nh, ok := s.cells.Neighborhood(id, top, memoryNeighborEdgesPerNeighbor, func(c index.IndexEntry) bool {
		return isActive(c.Status) && !isPrivateItem(c.Kind, c.Scope)
	})
	if !ok {
		return out
	}
	out.Indexed = true
	for _, n := range nh.Neighbors {
		out.Neighbors = append(out.Neighbors, MemoryNeighbor{ID: n.Document.ID, Kind: n.Document.Kind, Title: n.Document.Title, Status: n.Document.Status, Tags: n.Document.Tags, Score: n.Score})
	}
	for _, e := range nh.Edges {
		out.Edges = append(out.Edges, MemoryNeighborEdge{Source: e.A, Target: e.B, Score: e.Score})
	}
	return out
}

// ── cells ─────────────────────────────────────────────────────────────

// MirrorCell reflects a cell into the store: the keyword and vector-independent
// state, and the change events, follow what the cluster holds. A change this
// process made itself is a no-op. A private cell written under the older rule,
// where Subject named the owner, reads as owned by that subject until the cell is
// migrated. It reports whether the store changed.
func (s *MemoryStore) MirrorCell(cell *mcell.MemoryCell) bool {
	item := itemOfCell(cell)
	if memstore.IsPrivateItem(item) && memstore.PrivateOwner(item) == "" && item.Subject != "" && item.Subject != s.instance {
		item.Tags = append(append([]string(nil), item.Tags...), memstore.PrivateOwnerTagPrefix+item.Subject)
		item.Subject = s.instance
	}
	changed := s.eng.Mirror(item)
	if !item.IsActive() {
		s.dropVector(item.ID)
	}
	return changed
}

// dropVector takes a cell out of the vector index.
func (s *MemoryStore) dropVector(id string) {
	if s.cells != nil {
		_ = s.cells.Delete(context.Background(), id)
	}
}

// ForgetCell drops an item whose cell was deleted, here or on another replica,
// from the store and the vector index.
func (s *MemoryStore) ForgetCell(id string) {
	it, held := s.eng.Get(id)
	s.eng.Delete(id)
	if s.cells != nil {
		_ = s.cells.Delete(context.Background(), id)
	}
	if held {
		s.publishDelete(it)
	}
}

// ExportAll returns a copy of every item the store holds, private and retired
// ones included, for a program that persists the store itself.
func (s *MemoryStore) ExportAll() []*MemoryItem { return views(s.eng.AllItems()) }

// Restore puts an item back as it was exported: ids, origin, status, links,
// timestamps and deadline are kept as given. An active shared item is indexed
// again (keyword and, with an embedder, semantic); a retired or private one is
// held and left out of the shared indexes, exactly as Upsert would leave it.
func (s *MemoryStore) Restore(ctx context.Context, it *MemoryItem) {
	item := itemOfView(it, s.instance)
	if item.Status == "" {
		item.Status = "active"
	}
	s.eng.Put(item)
	if !item.IsActive() || memstore.IsPrivateItem(item) || s.cells == nil || s.embedder == nil {
		return
	}
	embedCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := s.cells.Index(embedCtx, entryOf(item)); err != nil {
		s.logger.Warn().Err(err).Str("id", item.ID).Msg("restore: embed failed; the item is kept for keyword search")
	}
}

// ── episodic memory ───────────────────────────────────────────────────

// episodicExpired reports whether an episodic memory is older than episodicTTL.
// TTL<=0 disables expiry. Age is measured from CreatedAt so a re-read does not
// extend a memory's life.
func (s *MemoryStore) episodicExpired(it *MemoryItem) bool {
	if s.episodicTTL <= 0 || it.Kind != "episodic" {
		return false
	}
	return time.Since(it.CreatedAt) > s.episodicTTL
}

// StartEpisodicReaper runs a background sweep that deletes episodic memories
// older than episodicTTL (the "forget" mechanism). One goroutine, a ticker, exits
// on ctx cancel. A no-op when TTL<=0 or every<=0. syncer removes the backing
// cell (nil keeps the sweep in process).
func (s *MemoryStore) StartEpisodicReaper(ctx context.Context, syncer *CellSyncer, every time.Duration) {
	if s.episodicTTL <= 0 || every <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.reapExpiredEpisodic(ctx, syncer)
			}
		}
	}()
}

func (s *MemoryStore) reapExpiredEpisodic(ctx context.Context, syncer *CellSyncer) {
	s.mu.RLock()
	ns := s.namespace
	s.mu.RUnlock()
	for _, it := range s.eng.AllItems() {
		v := viewOf(it)
		if !s.episodicExpired(v) {
			continue
		}
		s.Delete(it.ID)
		if syncer != nil {
			if err := syncer.Remove(ctx, it.ID, ns); err != nil {
				s.logger.Warn().Err(err).Str("id", it.ID).Msg("episodic reaper: delete cell failed")
			}
		}
	}
}

// isActive reports whether a status is one of a live item.
func isActive(status string) bool { return status == "active" || status == "indexed" }

// Scope is a two-value axis: scope answers "private or shared?", the owner of a
// private cell is its user: tag, and Subject names the instance (or, on the wire,
// the owner). A public cell is shared knowledge, visible on the graph and in
// user-facing search.
const (
	scopePrivate = memstore.ScopePrivate // isolated to its owner
	scopePublic  = memstore.ScopePublic  // shared; visible to everyone
)

func isPrivateScope(scope string) bool { return scope == scopePrivate }

// isPrivateItem reports whether a cell is private: scope=private, or the reserved
// system_configuration kind (internal config, never shared regardless of scope).
func isPrivateItem(kind, scope string) bool {
	return isPrivateScope(scope) || kind == "system_configuration"
}
