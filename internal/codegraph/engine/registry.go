package engine

// The registry is the heart of Mode B: it owns one fully isolated
// per-project code graph + index, loaded lazily from a snapshot, routed to by an
// explicit project arg or a semantic match on the query, and evicted from memory
// after an idle TTL (its tiny routing vector stays resident so it can be revived).
//
// There is no merged graph: each project's *graph.Graph + *index.Index (with its
// own HNSW + embedding cache) is independent, so search ranking never bleeds
// across projects and a project's whole footprint drops in one eviction.

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/mcpserver"
)

// projectEntry is one snapshot-backed project. summaryVec is ALWAYS resident
// (survives eviction) so the router can still resolve an evicted project and
// trigger its reload; ixr/loaded are dropped on eviction and rebuilt on demand.
type projectEntry struct {
	label         string
	repoPath      string
	snapshotPath  string // on-disk source; empty when the snapshot is carried in memory
	snapshotBytes []byte // in-memory gzipped-gob source (e.g. from a MemoryCell); nil for file-backed entries
	summary       string
	summarySource string // "explicit" | "readme" | "stats" | "" (pre-SummarySource snapshot) — see SnapshotHeader.SummarySource
	summaryVec    []float32
	nodes         int // from the snapshot header — reported by /status without loading
	edges         int
	// encrypted records how the source was found on disk (or in the blob). A
	// map read encrypted is never flushed back as plaintext — flush refuses
	// rather than downgrade — so a key removed from a running server cannot
	// quietly turn a protected catalog into a readable file on the volume.
	encrypted bool

	mu         sync.RWMutex
	ixr        *Indexer // nil when evicted; holds THIS project's own graph+index+HNSW
	loaded     bool
	dirty      bool // in-memory graph has edits not yet flushed to the snapshot
	notesOnly  bool // the resident graph holds Memory nodes and nothing else: an agent-notes map, swept for expired notes
	lastAccess time.Time
}

// Registry holds every registered project and routes resolves to the right one.
// It implements mcpserver.Resolver.
type Registry struct {
	mu          sync.RWMutex
	entries     map[string]*projectEntry
	embedder    index.Embedder
	fingerprint EmbedFingerprint
	fanout      int
	ttl         time.Duration
	writable    bool          // snapshot-dir is read-write: worked-copy edits flush to disk
	snapshotDir string        // where a map created at runtime (Remember) is written; set by SetSnapshotDir
	keys        SnapshotKeys  // opens encrypted snapshots, keeps them encrypted on flush; a recipient encrypts all writes (SetSnapshotKeys)
	memoryTTL   time.Duration // lifetime of a note that names none; mcpserver.DefaultMemoryTTL unless SetMemoryTTL says otherwise, negative = never
	sweepMu     sync.Mutex    // serializes expiry sweeps so two searches finding the same dead note rebuild once, not twice
	logger      zerolog.Logger

	// stats and searchStats are the registry's own two metrics surfaces (see
	// stats.go and index/stats.go): routing/map-lifecycle counters owned
	// directly here, and a single query-mechanics SearchStats instance
	// injected into every *index.Index this registry builds (see load) so its
	// counts survive a project's evict/reload cycle. Both live for the
	// Registry's whole lifetime.
	stats       *RegistryStats
	searchStats *index.SearchStats
}

// NewRegistry builds an empty registry. fanout is how many projects a semantic
// (no explicit project) route consults; ttl is the idle-eviction window (0 =
// never evict).
func NewRegistry(embedder index.Embedder, fp EmbedFingerprint, fanout int, ttl time.Duration, writable bool, logger zerolog.Logger) *Registry {
	if fanout < 1 {
		fanout = 1
	}
	return &Registry{
		entries:     map[string]*projectEntry{},
		embedder:    embedder,
		fingerprint: fp,
		fanout:      fanout,
		ttl:         ttl,
		writable:    writable,
		memoryTTL:   mcpserver.DefaultMemoryTTL,
		logger:      logger,
		stats:       NewRegistryStats(),
		searchStats: index.NewSearchStats(),
	}
}

// SetSnapshotKeys installs the key material every snapshot read and write goes
// through: an identity opens encrypted files and keeps them encrypted on flush;
// only an explicit recipient encrypts plaintext or newly created maps. See
// snapshotkeys.go for the model and why. Call before AddSnapshot.
func (r *Registry) SetSnapshotKeys(k SnapshotKeys) {
	r.mu.Lock()
	r.keys = k
	r.mu.Unlock()
}

// snapshotKeys returns the installed keys under the read lock.
func (r *Registry) snapshotKeys() SnapshotKeys {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.keys
}

// checkFingerprint rejects a snapshot whose embedding space is incomparable to
// the server's. src labels the snapshot in the error/log (a path or a synthetic
// tag). Dims define the vector SPACE — a mismatch means the snapshot's vectors
// are incomparable to query embeddings, so reject. The model NAME is advisory:
// in practice the embedder (e.g. a llama.cpp sidecar) ignores the label and
// serves whatever GGUF is loaded, so a label difference (nomic vs
// nomic-embed-text) over the same model is harmless — warn, don't reject.
func (r *Registry) checkFingerprint(h *SnapshotHeader, src string) error {
	if h.Fingerprint.Dims != r.fingerprint.Dims || h.Fingerprint.MaxInputChars != r.fingerprint.MaxInputChars ||
		h.Fingerprint.MaxInputTokens != r.fingerprint.MaxInputTokens {
		return fmt.Errorf("snapshot %s built with %+v, server uses %+v — rebuild with `xray-memory snapshot`",
			src, h.Fingerprint, r.fingerprint)
	}
	if h.Fingerprint.Model != r.fingerprint.Model {
		r.logger.Warn().Str("path", src).
			Str("snapshotModel", h.Fingerprint.Model).Str("serverModel", r.fingerprint.Model).
			Msg("snapshot embed-model label differs from server (dims match; assuming same model)")
	}
	return nil
}

// AddSnapshot registers a project from its snapshot header WITHOUT loading the
// body: the entry starts evicted (ixr=nil) carrying only its routing vector; the
// graph is read on first Resolve. A fingerprint mismatch is rejected here.
func (r *Registry) AddSnapshot(path string) error {
	keys := r.snapshotKeys()
	h, err := ReadSnapshotHeaderWith(path, keys)
	if err != nil {
		return err
	}
	// The header read above already decrypted if needed; this cheap sniff is
	// what the entry remembers so flush can refuse a plaintext downgrade.
	encrypted, err := IsEncryptedSnapshot(path)
	if err != nil {
		return err
	}
	if err := r.checkFingerprint(h, path); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// A loaded entry backed by this very file keeps its worked copy: the write
	// that woke the watcher is most often our OWN flush (Remember flushes on
	// every note; evict flushes before dropping), and replacing the entry then
	// would discard the resident graph — and any note applied between the
	// flush and this debounced re-registration would land on an entry object
	// nothing routes to any more. Only the header-derived fields are refreshed.
	// An entry that is not loaded is replaced as before, so an external rebuild
	// still takes effect; a loaded map rebuilt externally is picked up on its
	// next evict/reload.
	if e := r.entries[h.Label]; e != nil && e.snapshotPath == path {
		e.mu.Lock()
		resident := e.loaded && e.ixr != nil
		if resident {
			e.summary, e.summarySource, e.summaryVec = h.Summary, h.SummarySource, h.SummaryVec
			e.nodes, e.edges = h.Nodes, h.Edges
			e.encrypted = encrypted
		}
		e.mu.Unlock()
		if resident {
			r.logger.Debug().Str("project", h.Label).Str("path", path).Msg("refreshed snapshot header; worked copy kept")
			return nil
		}
	}
	r.entries[h.Label] = &projectEntry{
		label:         h.Label,
		repoPath:      h.RepoPath,
		snapshotPath:  path,
		summary:       h.Summary,
		summarySource: h.SummarySource,
		summaryVec:    h.SummaryVec,
		nodes:         h.Nodes,
		edges:         h.Edges,
		encrypted:     encrypted,
	}
	r.logger.Info().Str("project", h.Label).Int("nodes", h.Nodes).Int("edges", h.Edges).
		Str("path", path).Str("summarySource", h.SummarySource).Bool("encrypted", encrypted).Msg("registered snapshot")
	return nil
}

// AddSnapshotBytes registers a snapshot carried IN MEMORY (a gzipped-gob blob,
// e.g. stored in a MemoryCell) rather than on disk. Like AddSnapshot it reads
// only the header up front and defers the body to first Resolve; the bytes are
// retained on the entry so an evicted graph reloads without a file. A fingerprint
// mismatch is rejected. Registering the same label again replaces the entry (drop
// the old in-memory graph first via RemoveSnapshotByLabel if it may be loaded).
//
// The routing vector (SummaryVec) is what semantic search matches against; an
// entry without one is INVISIBLE to a query-only search (rankBySummary skips it).
// Externally-built gobs may ship an empty SummaryVec, so when it's missing we
// synthesize it here by embedding the header Summary (or the label as a last
// resort) — otherwise a cell-carried map could only be reached by an exact map=
// arg, never by a bare query. A synthesis failure is non-fatal: the map still
// registers and remains reachable by exact label.
func (r *Registry) AddSnapshotBytes(ctx context.Context, b []byte) error {
	h, err := ReadSnapshotHeaderFromWith(bytes.NewReader(b), r.snapshotKeys())
	if err != nil {
		return err
	}
	if err := r.checkFingerprint(h, "<memory:"+h.Label+">"); err != nil {
		return err
	}

	summaryVec := h.SummaryVec
	if len(summaryVec) == 0 {
		routeText := h.Summary
		if routeText == "" {
			routeText = h.Label
		}
		if vec, err := r.embedder.EmbedQuery(ctx, routeText); err != nil {
			r.logger.Warn().Err(err).Str("map", h.Label).
				Msg("no summary vector in gob and synthesis failed — map reachable by exact name only")
		} else {
			summaryVec = vec
			r.logger.Info().Str("map", h.Label).Msg("synthesized routing vector for gob lacking SummaryVec")
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[h.Label] = &projectEntry{
		label:         h.Label,
		repoPath:      h.RepoPath,
		snapshotBytes: b,
		summary:       h.Summary,
		summarySource: h.SummarySource,
		summaryVec:    summaryVec,
		nodes:         h.Nodes,
		edges:         h.Edges,
		encrypted:     IsEncryptedSnapshotBytes(b),
	}
	r.logger.Info().Str("map", h.Label).Int("nodes", h.Nodes).Int("edges", h.Edges).
		Int("bytes", len(b)).Bool("routable", len(summaryVec) > 0).Msg("registered in-memory snapshot")
	return nil
}

// RemoveSnapshotByLabel de-registers a project by label and drops its in-memory
// graph. No-op if absent. Used to replace/evict a memory-carried snapshot.
func (r *Registry) RemoveSnapshotByLabel(label string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[label]; ok {
		delete(r.entries, label)
		r.logger.Info().Str("project", label).Msg("de-registered snapshot")
	}
}

// RemoveSnapshotByPath de-registers whichever project was loaded from path (its
// snapshot file was deleted). Drops the in-memory graph too. No-op if no entry
// references that path.
func (r *Registry) RemoveSnapshotByPath(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for label, e := range r.entries {
		if e.snapshotPath == path {
			delete(r.entries, label)
			r.logger.Info().Str("project", label).Str("path", path).Msg("deregistered snapshot (file removed)")
			return
		}
	}
}

// Labels returns the registered project labels (sorted), for status/logging.
func (r *Registry) Labels() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.entries))
	for l := range r.entries {
		out = append(out, l)
	}
	sort.Strings(out)
	return out
}

// Resolve routes a search to one or more loaded map graphs, in three steps:
// an exact `mapName` label is a free fast path; then labels NAMED in the route
// text seed the set; then the semantic rank fills whatever fanout is left. A
// `mapName` hint that is not a label (e.g. "mcp governance" for the label
// security-policies) is embedded and cosine-matched against every map's
// summary vector, the same brute-force scan used for query routing. Maps are
// returned best-first; each is loaded (from snapshot if evicted) and touched.
func (r *Registry) Resolve(ctx context.Context, mapName, query string) ([]mcpserver.Resolved, error) {
	// Fast path: an exact label match needs no embedding.
	if mapName != "" {
		if e := r.entry(mapName); e != nil {
			r.stats.recordExplicitRoute()
			res, err := r.load(ctx, e)
			if err != nil {
				return nil, err
			}
			return []mcpserver.Resolved{res}, nil
		}
	}

	// The text to match is the map hint (if any) plus the query — so an inexact
	// map name resolves to the closest map by meaning instead of failing on a
	// string mismatch.
	routeText := query
	if mapName != "" {
		routeText = mapName
		if query != "" {
			routeText = mapName + " " + query
		}
	}

	// An explicit map hint should resolve to exactly one map, not fan out — the
	// caller named a target.
	fanout := r.fanout
	if mapName != "" {
		fanout = 1
	}

	// Labels named in the route text seed the set. A caller that writes the map
	// into its query instead of the mapName argument ("... in project Team")
	// has still named a target, and the summary vectors cannot recover it: a
	// label is an identifier, and one dense vector per map does not resolve
	// identifiers — it ranks by topic. Seeding rather than overriding leaves the
	// remaining fanout to the semantic rank, so a label that is also an ordinary
	// word does not hijack a query it merely appears in.
	// A map the caller NAMED scores 1: naming it is a stronger statement of
	// relevance than any summary cosine, and the score is what orders hits when
	// several maps answer at once.
	picked := make([]routedEntry, 0, fanout)
	for _, e := range r.labelsNamedIn(routeText) {
		if len(picked) >= fanout {
			break
		}
		picked = append(picked, routedEntry{e: e, score: 1})
	}
	semantic := len(picked) < fanout
	if semantic {
		qv, err := r.embedder.EmbedQuery(ctx, routeText)
		if err != nil {
			return nil, fmt.Errorf("embed for routing: %w", err)
		}
		for _, re := range r.rankBySummary(qv) {
			if len(picked) >= fanout {
				break
			}
			if !hasEntry(picked, re.e) {
				picked = append(picked, re)
			}
		}
	}
	if len(picked) == 0 {
		return nil, fmt.Errorf("no maps registered")
	}
	if semantic {
		r.stats.recordSemanticRoute(len(picked))
	} else {
		r.stats.recordExplicitRoute()
	}
	out := make([]mcpserver.Resolved, 0, len(picked))
	for _, re := range picked {
		res, err := r.load(ctx, re.e)
		if err != nil {
			r.logger.Warn().Err(err).Str("map", re.e.label).Msg("route: load failed, skipping")
			continue
		}
		// A negative cosine would inverting the ordering below, so the floor is
		// zero: a map that matched nothing still contributes its hits, last.
		res.RouteScore = float64(re.score)
		if res.RouteScore < 0 {
			res.RouteScore = 0
		}
		out = append(out, res)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("routing matched maps but none could be loaded")
	}
	return out, nil
}

// ResolveByQN routes a node-keyed tool (get_code_node, trace_code_calls, impact)
// by the project prefix of a repo-namespaced qualified name ("label//..."). No
// embedding is needed — the project is named explicitly inside the qn.
func (r *Registry) ResolveByQN(ctx context.Context, qn string) (mcpserver.Resolved, error) {
	label := qn
	if i := strings.Index(qn, "//"); i > 0 {
		label = qn[:i]
	}
	e := r.entry(label)
	if e == nil {
		return mcpserver.Resolved{}, fmt.Errorf("no snapshot for project %q (from qn %q)", label, qn)
	}
	return r.load(ctx, e)
}

// UpdateByPaths lives in update.go, untagged: it re-parses changed files into the
// owning project's worked copy in either build. A CGO-free build cannot parse
// source, so it indexes Markdown/YAML and reports code files as skipped rather
// than refusing the whole batch.

// resolve maps a caller-supplied path to the project that owns it, returning the
// absolute path to use. Absolute inputs match by containment; relative inputs are
// resolved against each served repo root and must name a file under exactly one
// of them — see Indexer.locate for why the process working directory is not used.
// Returns (nil, "") when nothing resolves, so the caller can report the path back
// instead of silently dropping it.
func (r *Registry) resolve(p string) (*projectEntry, string) {
	if filepath.IsAbs(p) {
		return r.ownerOf(p), p
	}
	if !filepath.IsLocal(p) {
		return nil, "" // "../x" leaves every repo; Indexer.locate refuses it too
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var match *projectEntry
	var abs string
	for _, e := range r.entries {
		if e.repoPath == "" {
			continue
		}
		cand := filepath.Join(e.repoPath, p)
		if _, err := os.Stat(cand); err == nil {
			if match != nil {
				return nil, "" // same relative path under several repos: refuse to guess
			}
			match, abs = e, cand
		}
	}
	if match == nil {
		return nil, ""
	}
	return match, abs
}

// ownerOf returns the registered project whose repoPath contains abs, or nil.
func (r *Registry) ownerOf(abs string) *projectEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if e.repoPath == "" {
			continue
		}
		if rel, rerr := filepath.Rel(e.repoPath, abs); rerr == nil && filepath.IsLocal(rel) {
			return e
		}
	}
	return nil
}

// flush rewrites an entry's snapshot from its current (possibly edited) in-memory
// graph, so the on-disk copy stays fresh. Caller must NOT hold e.mu. No-op if the
// entry is not loaded. Clears the dirty flag on success.
func (r *Registry) flush(ctx context.Context, e *projectEntry) error {
	if !r.writable {
		return nil // read-only snapshot dir (e.g. OCI image volume): never write back
	}
	e.mu.Lock()
	if !e.loaded || e.ixr == nil || !e.dirty {
		e.mu.Unlock()
		return nil
	}
	ixr := e.ixr
	label, path, wasEncrypted := e.label, e.snapshotPath, e.encrypted
	e.mu.Unlock()

	// The write policy is SnapshotKeys.WriteRecipient: a map that arrived
	// encrypted is re-encrypted (to the recipient if one is set, else to the
	// identity's own key); a plaintext or server-created map stays plaintext
	// unless a recipient was named. Never downgrade: reading an encrypted map
	// needed the identity, so a nil recipient here means the keys were removed
	// from a running registry — the assertion is what makes "the file on the
	// volume is protected" a property of the file, not of the flag order.
	keys := r.snapshotKeys()
	rcpt := keys.WriteRecipient(wasEncrypted)
	if wasEncrypted && rcpt == nil {
		return fmt.Errorf("refusing to flush %s as plaintext: it was read encrypted and no -snapshot-key/-snapshot-recipient is configured", path)
	}
	encrypted := rcpt != nil

	snap, err := ixr.SnapshotFor(ctx, label, r.fingerprint, r.embedder)
	if err != nil {
		return fmt.Errorf("snapshot %s for flush: %w", label, err)
	}
	if err := writeSnapshotTo(path, snap, rcpt); err != nil {
		return fmt.Errorf("write snapshot %s: %w", path, err)
	}
	e.mu.Lock()
	e.dirty = false
	e.encrypted = encrypted
	// The header counts are what /status reports without loading; a worked
	// copy that grew since registration would otherwise be reported at its
	// registration-time size for as long as it stays resident. The summary
	// and its vector are what routing matches against: they follow the file
	// too, so the resident entry routes as what is on disk without waiting
	// for the watcher's debounced re-registration.
	e.nodes, e.edges = snap.Header.Nodes, snap.Header.Edges
	e.summary, e.summarySource, e.summaryVec = snap.Header.Summary, snap.Header.SummarySource, snap.Header.SummaryVec
	e.mu.Unlock()
	r.logger.Info().Str("project", label).Str("path", path).Bool("encrypted", encrypted).Msg("flushed worked copy to snapshot")
	return nil
}

// FlushAll flushes every dirty project — call on graceful shutdown so edits made
// since the last eviction are not lost.
func (r *Registry) FlushAll(ctx context.Context) {
	r.mu.RLock()
	entries := make([]*projectEntry, 0, len(r.entries))
	for _, e := range r.entries {
		entries = append(entries, e)
	}
	r.mu.RUnlock()
	for _, e := range entries {
		if err := r.flush(ctx, e); err != nil {
			r.logger.Error().Err(err).Str("project", e.label).Msg("flush on shutdown failed")
		}
	}
}

// entry looks up a registered project by label.
// entry looks up one map by label, case-insensitively. A label is an identifier
// a caller types or an agent lifts out of prose ("Team" for team), and a
// case difference silently dropping that caller into semantic routing reads as a
// routing bug rather than a typo.
func (r *Registry) entry(label string) *projectEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e := r.entries[label]; e != nil {
		return e
	}
	for l, e := range r.entries {
		if strings.EqualFold(l, label) {
			return e
		}
	}
	return nil
}

// labelsNamedIn returns the maps whose label appears as a whole word in text,
// ordered by where each first appears (ties by label, so the result is stable).
// Whole-word matching is what keeps a label from firing on a substring of a
// longer word.
func (r *Registry) labelsNamedIn(text string) []*projectEntry {
	if text == "" {
		return nil
	}
	lower := strings.ToLower(text)
	type found struct {
		e  *projectEntry
		at int
	}
	var hits []found
	r.mu.RLock()
	for l, e := range r.entries {
		if at := wordIndex(lower, strings.ToLower(l)); at >= 0 {
			hits = append(hits, found{e: e, at: at})
		}
	}
	r.mu.RUnlock()
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].at != hits[j].at {
			return hits[i].at < hits[j].at
		}
		return hits[i].e.label < hits[j].e.label
	})
	out := make([]*projectEntry, len(hits))
	for i, h := range hits {
		out[i] = h.e
	}
	return out
}

// wordIndex returns the index of the first whole-word occurrence of word in s
// (both already lowercased), or -1. A boundary is any byte that is not an ASCII
// letter or digit, so "team" matches in "project Team." but not in
// "teams"; a hyphen inside a label is interior, not a boundary.
func wordIndex(s, word string) int {
	if word == "" {
		return -1
	}
	for from := 0; from+len(word) <= len(s); {
		i := strings.Index(s[from:], word)
		if i < 0 {
			return -1
		}
		i += from
		if isWordEdge(s, i-1) && isWordEdge(s, i+len(word)) {
			return i
		}
		from = i + 1
	}
	return -1
}

// isWordEdge reports whether position i in s is off the end or holds a byte that
// cannot be part of a label word.
func isWordEdge(s string, i int) bool {
	if i < 0 || i >= len(s) {
		return true
	}
	c := s[i]
	return !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9')
}

// hasEntry reports whether es already holds e, so the semantic fill cannot
// re-add a map the label seed already picked.
func hasEntry(es []routedEntry, e *projectEntry) bool {
	for _, x := range es {
		if x.e == e {
			return true
		}
	}
	return false
}

// ResolveMap loads (reviving from snapshot if evicted) the named map for the
// browser graph UI and returns its graph. An empty label picks the first map in
// sorted order — so /ui has something to show without an explicit selector.
func (r *Registry) ResolveMap(ctx context.Context, label string) (mcpserver.Resolved, error) {
	if label == "" {
		if labels := r.Labels(); len(labels) > 0 {
			label = labels[0]
		}
	}
	e := r.entry(label)
	if e == nil {
		return mcpserver.Resolved{}, fmt.Errorf("no snapshot for project %q", label)
	}
	return r.load(ctx, e)
}

// Totals sums the node and edge counts across all registered projects (from
// their headers, no graph load) — a one-line "size of everything served".
func (r *Registry) Totals() (nodes, edges int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		nodes += e.nodes
		edges += e.edges
	}
	return nodes, edges
}

// ProjectInfo is one project's label + residency, for the UI selector.
type ProjectInfo struct {
	Label  string `json:"label"`
	Loaded bool   `json:"loaded"`
}

// ProjectList returns every registered project with its current residency,
// sorted by label — feeds the /projects endpoint and the UI's project selector.
func (r *Registry) ProjectList() []ProjectInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	labels := make([]string, 0, len(r.entries))
	for l := range r.entries {
		labels = append(labels, l)
	}
	sort.Strings(labels)
	out := make([]ProjectInfo, 0, len(labels))
	for _, l := range labels {
		e := r.entries[l]
		e.mu.RLock()
		out = append(out, ProjectInfo{Label: e.label, Loaded: e.loaded})
		e.mu.RUnlock()
	}
	return out
}

// routedEntry is one map the router picked, with the score it was picked ON.
// The score travels with the entry because a caller merging hits from several
// maps needs to know how well each map matched — hits from disjoint maps are
// otherwise incomparable. See mcpserver.Resolved.RouteScore.
type routedEntry struct {
	e     *projectEntry
	score float32
}

// rankBySummary returns entries sorted by descending cosine of their summary
// vector to qv, each carrying that cosine. Entries with no summary vector are
// skipped.
func (r *Registry) rankBySummary(qv []float32) []routedEntry {
	r.mu.RLock()
	var all []routedEntry
	for _, e := range r.entries {
		e.mu.RLock()
		vec := e.summaryVec
		e.mu.RUnlock()
		if vec == nil {
			continue
		}
		all = append(all, routedEntry{e: e, score: cosine32(qv, vec)})
	}
	r.mu.RUnlock()
	sort.Slice(all, func(i, j int) bool { return all[i].score > all[j].score })
	return all
}

// load ensures the entry's graph is in memory (reloading from snapshot if it was
// evicted), bumps lastAccess, and returns a resolved handle. Double-checked so
// concurrent resolves load the body once.
// load makes e resident and, when it is a notes map on a writable dir, sweeps
// its expired notes first. The sweep sits here rather than in the search
// path because every reader — search, get_graph_node, stats, the direct
// twins — reaches a map through load, so one hook keeps them all from
// serving a note past its expiry, and the counts a selection reports stay
// exact. The scan is a pass over the notes map's nodes (small by
// construction) and the rebuild+flush only happens when something died.
func (r *Registry) load(ctx context.Context, e *projectEntry) (mcpserver.Resolved, error) {
	res, err := r.loadEntry(ctx, e)
	if err != nil {
		return res, err
	}
	if r.writable {
		if n, err := r.sweepExpired(ctx, e, time.Now()); err != nil {
			r.logger.Warn().Err(err).Str("map", e.label).Msg("memory: expiry sweep failed")
		} else if n > 0 {
			r.logger.Info().Str("map", e.label).Int("removed", n).Msg("memory: swept expired notes")
		}
	}
	return res, nil
}

// loadEntry is load without the sweep: the residency step alone.
func (r *Registry) loadEntry(ctx context.Context, e *projectEntry) (mcpserver.Resolved, error) {
	e.mu.RLock()
	if e.loaded && e.ixr != nil {
		res := mcpserver.Resolved{Label: e.label, Graph: e.ixr.Graph(), Index: e.ixr.Index(), Summary: e.summary, SummarySource: e.summarySource, RouteScore: 1}
		e.mu.RUnlock()
		e.touch()
		return res, nil
	}
	e.mu.RUnlock()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.loaded && e.ixr != nil { // lost the race; another goroutine loaded it
		e.lastAccess = time.Now()
		return mcpserver.Resolved{Label: e.label, Graph: e.ixr.Graph(), Index: e.ixr.Index(), Summary: e.summary, SummarySource: e.summarySource, RouteScore: 1}, nil
	}

	var snap *Snapshot
	var err error
	keys := r.snapshotKeys()
	if e.snapshotBytes != nil {
		snap, err = ReadSnapshotFromWith(bytes.NewReader(e.snapshotBytes), keys)
		if err != nil {
			return mcpserver.Resolved{}, fmt.Errorf("read in-memory snapshot %s: %w", e.label, err)
		}
	} else {
		snap, err = ReadSnapshotWith(e.snapshotPath, keys)
		if err != nil {
			return mcpserver.Resolved{}, fmt.Errorf("read snapshot %s: %w", e.snapshotPath, err)
		}
	}
	ixr, err := NewIndexerFromSnapshot(ctx, snap, r.embedder, r.logger)
	if err != nil {
		return mcpserver.Resolved{}, err
	}
	// An explicit router summary must survive the reload, or the first flush
	// after it re-derives one from stats and overwrites the header: the
	// session map went from "explicit: notes an agent chose to remember" to
	// "stats: code graph (Memory=4). Symbols:" on its first remember after a
	// restart, and stopped routing. summaryFor prepends "<label>: " and may
	// have capped the text; the prefix is stripped, the cap is accepted.
	if snap.Header.SummarySource == "explicit" {
		ixr.SetExplicitSummary(strings.TrimPrefix(snap.Header.Summary, e.label+": "))
	}
	// Inject the registry's shared SearchStats before the Index is published
	// (still under e.mu here) — see index.SearchStats' doc comment for why
	// this is what keeps its counts process-lifetime cumulative across this
	// very evict/reload cycle instead of resetting on every reload.
	ixr.Index().UseStats(r.searchStats)
	e.ixr = ixr
	e.loaded = true
	e.notesOnly = isNotesMap(ixr.Graph())
	e.lastAccess = time.Now()
	r.stats.recordLoad()
	r.logger.Info().Str("project", e.label).Msg("reloaded project graph from snapshot")
	return mcpserver.Resolved{Label: e.label, Graph: ixr.Graph(), Index: ixr.Index(), Summary: e.summary, SummarySource: e.summarySource, RouteScore: 1}, nil
}

// touch bumps lastAccess under the entry lock.
func (e *projectEntry) touch() {
	e.mu.Lock()
	e.lastAccess = time.Now()
	e.mu.Unlock()
}

// StartSweeper runs the idle-eviction loop until ctx is done. No-op when ttl<=0.
func (r *Registry) StartSweeper(ctx context.Context) {
	if r.ttl <= 0 {
		return
	}
	interval := r.ttl / 2
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				r.evictIdle()
			}
		}
	}()
}

// isSnapshotFile reports whether a path is a graph snapshot we serve.
func isSnapshotFile(path string) bool {
	return strings.HasSuffix(path, ".graph.gob.gz")
}

// watchDebounce is the quiet period a snapshot path must go without a new
// write event before it is (re)registered. A snapshot is written in many small
// Write syscalls (gzip flushes in chunks), so each write fires its own fsnotify
// event; without debouncing we would AddSnapshot ~once per chunk and could even
// read a half-written file mid-stream. Waiting for the writes to settle collapses
// the burst into a single registration of the finished file.
const watchDebounce = 400 * time.Millisecond

// WatchDir watches the snapshot directory and keeps the registry in sync with
// the files on disk without a restart: a new <label>.graph.gob.gz is
// registered (and a refreshed one re-registered) once its writes settle; a
// removed one is de-registered immediately. Returns after starting the watcher;
// the watch goroutine stops on ctx cancellation.
//
// Create/Write events are DEBOUNCED per path (see watchDebounce) so a multi-chunk
// write registers exactly once, on the finished file. Remove/Rename de-register
// by path and cancel any pending registration for it. Errors on a single file are
// logged, not fatal — the watcher keeps running.
func (r *Registry) WatchDir(ctx context.Context, dir string) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	if err := w.Add(dir); err != nil {
		w.Close()
		return err
	}
	r.logger.Info().Str("dir", dir).Msg("watching snapshot dir for changes")
	go func() {
		defer w.Close()

		// One pending timer per path. A new write resets that path's timer;
		// the timer firing sends the path on `ready` so registration happens on
		// THIS goroutine (no extra locking around AddSnapshot). Remove cancels
		// the pending timer so a delete during a rewrite doesn't re-register.
		timers := make(map[string]*time.Timer)
		ready := make(chan string, 16)
		stopAll := func() {
			for _, t := range timers {
				t.Stop()
			}
		}
		defer stopAll()

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				if !isSnapshotFile(ev.Name) {
					continue
				}
				switch {
				case ev.Op&(fsnotify.Create|fsnotify.Write) != 0:
					path := ev.Name
					if t, exists := timers[path]; exists {
						t.Reset(watchDebounce)
						continue
					}
					timers[path] = time.AfterFunc(watchDebounce, func() {
						// Non-blocking-ish: the buffered channel + small fan-in
						// keep this from wedging the timer goroutine.
						select {
						case ready <- path:
						case <-ctx.Done():
						}
					})
				case ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0:
					if t, exists := timers[ev.Name]; exists {
						t.Stop()
						delete(timers, ev.Name)
					}
					r.RemoveSnapshotByPath(ev.Name)
				}
			case path, ok := <-ready:
				if !ok {
					return
				}
				delete(timers, path)
				if err := r.AddSnapshot(path); err != nil {
					r.logger.Warn().Err(err).Str("path", path).Msg("watch: failed to (re)register snapshot")
				}
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				r.logger.Warn().Err(err).Msg("snapshot dir watcher error")
			}
		}
	}()
	return nil
}

// evictIdle drops the in-memory graph+index of any project idle longer than the
// TTL — after FLUSHING a dirty worked copy back to its snapshot first, so edits
// made via update_code_graph are persisted, not lost. The summary vector is kept
// (routing still resolves it); the snapshot file is the durable source for
// reload. Dropping only the entry's *Indexer pointer (never mutating a graph in
// place) keeps in-flight readers — which already hold their own *graph.Graph
// reference — safe via the GC.
func (r *Registry) evictIdle() {
	ctx := context.Background()
	now := time.Now()
	r.mu.RLock()
	entries := make([]*projectEntry, 0, len(r.entries))
	for _, e := range r.entries {
		entries = append(entries, e)
	}
	r.mu.RUnlock()

	for _, e := range entries {
		e.mu.RLock()
		idleEnough := e.loaded && now.Sub(e.lastAccess) > r.ttl
		e.mu.RUnlock()
		if !idleEnough {
			continue
		}

		// Persist a dirty worked copy before dropping it (flush takes/release
		// e.mu itself; it is a no-op when the entry is clean).
		if err := r.flush(ctx, e); err != nil {
			r.logger.Error().Err(err).Str("project", e.label).Msg("flush before evict failed; keeping resident")
			continue // don't evict unflushed edits
		}

		e.mu.Lock()
		// Re-check under the write lock: a concurrent resolve may have touched it.
		if e.loaded && now.Sub(e.lastAccess) > r.ttl {
			idle := now.Sub(e.lastAccess)
			e.ixr = nil
			e.loaded = false
			e.mu.Unlock()
			r.stats.recordEviction()
			r.logger.Info().Str("project", e.label).Dur("idle", idle).Msg("evicted project graph (idle)")
			continue
		}
		e.mu.Unlock()
	}
}

// RecordSearch attributes one map's search + returned-hit count to
// RegistryStats. Implements mcpserver.Resolver — the mcpserver package's
// search loop calls this once per resolved map, right after that map's
// index.Index.Search returns, since Registry itself never calls Search and
// so never sees a hit count on its own.
func (r *Registry) RecordSearch(label string, hits int) {
	r.stats.RecordSearch(label, hits)
}

// Metrics composes every metrics surface this registry owns into one
// JSON-friendly map: query-mechanics search stats (shared across every map,
// process-lifetime cumulative — see index.SearchStats), routing/map-lifecycle
// stats (this registry's own), and — when the configured embedder exposes it
// — its query-embedding call/cache-hit counters (the one place that number is
// actually tracked; see index.EmbedStatter). Feeds both the /metrics HTTP
// endpoint and the get_graph_stats MCP tool so the two surfaces can never
// drift apart.
func (r *Registry) Metrics() map[string]any {
	regSnap := r.stats.Snapshot()
	out := map[string]any{
		"search":       r.searchStats.Snapshot(),
		"routing":      regSnap.Routing,
		"maps":         regSnap.Maps,
		"mapLoads":     regSnap.MapLoads,
		"mapEvictions": regSnap.MapEvictions,
	}
	if es, ok := r.embedder.(index.EmbedStatter); ok {
		out["embed"] = es.Metrics()
	}
	return out
}

// Status reports per-project residency for an optional read-only status endpoint.
func (r *Registry) Status() map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	projects := make([]map[string]any, 0, len(r.entries))
	labels := make([]string, 0, len(r.entries))
	for l := range r.entries {
		labels = append(labels, l)
	}
	sort.Strings(labels)
	for _, l := range labels {
		e := r.entries[l]
		e.mu.RLock()
		// Header counts are always available (no body load needed). When the graph
		// is resident, use its live stats — they reflect any in-memory updates.
		nodes, edges := e.nodes, e.edges
		p := map[string]any{
			"label":    e.label,
			"loaded":   e.loaded,
			"snapshot": filepath.Base(e.snapshotPath),
			// summary/summarySource expose the router's actual input for
			// diagnosis (was invisible before — see summaryFor in snapshot.go):
			// "explicit" (a -summary flag), "readme", or "stats". "" means a
			// snapshot built before SummarySource existed.
			"summary":       e.summary,
			"summarySource": e.summarySource,
		}
		if e.loaded && e.ixr != nil {
			st := e.ixr.Graph().Stats()
			nodes, edges = st.Nodes, st.Edges
			p["embedded"] = st.Embedded
			p["lastAccess"] = e.lastAccess.Format(time.RFC3339)
		}
		p["nodes"] = nodes
		p["edges"] = edges
		e.mu.RUnlock()
		projects = append(projects, p)
	}
	return map[string]any{"projects": projects}
}

// cosine32 is cosine similarity over two float32 vectors (registry routing).
func cosine32(a, b []float32) float32 {
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
