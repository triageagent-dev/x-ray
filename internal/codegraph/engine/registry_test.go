//go:build cgo

package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/mcpserver"
)

// writeFixtureRepo creates a tiny Go repo with a README carrying a routing token.
func writeFixtureRepo(t *testing.T, token string) string {
	t.Helper()
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "README.md"),
		"# "+token+" service\n\n## Overview\n\nThis is the "+token+" project. It handles "+token+" things.\n")
	mustWrite(t, filepath.Join(dir, "main.go"),
		"package main\n\nfunc "+token+"Handler() int { return helper() }\n\nfunc helper() int { return 1 }\n")
	return dir
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// buildSnapshot parses a fixture repo with the stub embedder and writes a
// snapshot, returning its path and the fingerprint used.
func buildSnapshot(t *testing.T, label, repoDir string, emb *stubEmbedder) (string, EmbedFingerprint) {
	t.Helper()
	ctx := context.Background()
	fp := EmbedFingerprint{Model: "stub", Dims: emb.dims, MaxInputChars: 8192}
	ixr, err := NewIndexer(ctx, []string{label + "=" + repoDir}, emb, testLogger(), false)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	snap, err := ixr.SnapshotFor(ctx, label, fp, emb)
	if err != nil {
		t.Fatalf("SnapshotFor: %v", err)
	}
	path := filepath.Join(t.TempDir(), label+".graph.gob.gz")
	if err := WriteSnapshot(path, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	return path, fp
}

func TestSnapshotRoundTrip(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	path, fp := buildSnapshot(t, "alpha", writeFixtureRepo(t, "alpha"), emb)

	h, err := ReadSnapshotHeader(path)
	if err != nil {
		t.Fatalf("ReadSnapshotHeader: %v", err)
	}
	if h.Label != "alpha" || h.Fingerprint != fp {
		t.Fatalf("header mismatch: %+v (fp %+v)", h, fp)
	}
	if len(h.SummaryVec) != emb.dims || h.Summary == "" {
		t.Fatalf("summary not populated: vec=%d summary=%q", len(h.SummaryVec), h.Summary)
	}

	full, err := ReadSnapshot(path)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if len(full.Body.Files) == 0 || len(full.Body.Cache) == 0 {
		t.Fatalf("empty body: files=%d cache=%d", len(full.Body.Files), len(full.Body.Cache))
	}
}

func TestFingerprintToleranceModelAdvisoryDimsStrict(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	path, _ := buildSnapshot(t, "alpha", writeFixtureRepo(t, "alpha"), emb)

	// Different MODEL LABEL but same dims/maxInputChars → accepted (label is
	// advisory: the embedder serves the same GGUF regardless of the name).
	regLabel := NewRegistry(emb, EmbedFingerprint{Model: "nomic-embed-text", Dims: 64, MaxInputChars: 8192}, 1, 0, false, testLogger())
	if err := regLabel.AddSnapshot(path); err != nil {
		t.Fatalf("model-label mismatch must be accepted, got: %v", err)
	}

	// Different DIMS → rejected (incompatible vector space).
	regDims := NewRegistry(emb, EmbedFingerprint{Model: "stub", Dims: 128, MaxInputChars: 8192}, 1, 0, false, testLogger())
	if err := regDims.AddSnapshot(path); err == nil {
		t.Fatalf("dims mismatch must be rejected")
	}
}

// A snapshot built before the token window existed decodes MaxInputTokens 0:
// a server that cuts to a window must reject it, so the map is rebuilt.
func TestFingerprintTokenWindowStrict(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	path, _ := buildSnapshot(t, "alpha", writeFixtureRepo(t, "alpha"), emb)

	reg := NewRegistry(emb, EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192, MaxInputTokens: 512}, 1, 0, false, testLogger())
	if err := reg.AddSnapshot(path); err == nil {
		t.Fatalf("a snapshot without the token window must be rejected by a server with one")
	}
}

func TestLoadWithoutEmbeddingNodes(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	path, _ := buildSnapshot(t, "alpha", writeFixtureRepo(t, "alpha"), emb)

	// A loader whose EmbedBatch PANICS: if loading embeds any node, the test fails.
	loader := &stubEmbedder{dims: 64, panicBatch: true}
	snap, err := ReadSnapshot(path)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	ixr, err := NewIndexerFromSnapshot(context.Background(), snap, loader, testLogger())
	if err != nil {
		t.Fatalf("NewIndexerFromSnapshot: %v", err)
	}
	st := ixr.Graph().Stats()
	if st.Nodes == 0 || st.Embedded != st.Nodes {
		t.Fatalf("expected all %d nodes embedded from cache, got embedded=%d", st.Nodes, st.Embedded)
	}
}

// TestNoEmbedBuildAndLoadNeverCallEmbedder covers the `snapshot -no-embed`
// path end to end: building never calls the embedder (for nodes or for the
// router summary), the written snapshot carries zero vectors, and loading it
// back — as `services`/`serve-snapshots` would — never calls the embedder
// either. Both embedder calls PANIC, so a regression that embeds anything
// fails loudly instead of just being slow.
func TestNoEmbedBuildAndLoadNeverCallEmbedder(t *testing.T) {
	ctx := context.Background()
	repoDir := writeFixtureRepo(t, "gamma")
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}

	builder := &stubEmbedder{dims: 64, panicBatch: true, panicQuery: true}
	ixr, err := NewIndexerSeeded(ctx, []string{"gamma=" + repoDir}, builder, testLogger(), false, nil, true)
	if err != nil {
		t.Fatalf("NewIndexerSeeded(noEmbed=true): %v", err)
	}
	if st := ixr.Graph().Stats(); st.Nodes == 0 || st.Embedded != 0 {
		t.Fatalf("no-embed build: nodes=%d embedded=%d, want embedded=0", st.Nodes, st.Embedded)
	}
	if !ixr.Index().NoEmbed() {
		t.Fatal("Index built with noEmbed=true must report NoEmbed()")
	}

	snap, err := ixr.SnapshotFor(ctx, "gamma", fp, builder)
	if err != nil {
		t.Fatalf("SnapshotFor: %v", err)
	}
	if !snap.Header.NoEmbed {
		t.Fatal("Header.NoEmbed must be true for a -no-embed build")
	}
	if len(snap.Header.SummaryVec) != 0 {
		t.Fatalf("Header.SummaryVec must be empty for a -no-embed build, got %d dims", len(snap.Header.SummaryVec))
	}
	if len(snap.Body.Cache) != 0 {
		t.Fatalf("Body.Cache must be empty for a -no-embed build, got %d vectors", len(snap.Body.Cache))
	}

	path := filepath.Join(t.TempDir(), "gamma.graph.gob.gz")
	if err := WriteSnapshot(path, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	reread, err := ReadSnapshot(path)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}

	loader := &stubEmbedder{dims: 64, panicBatch: true, panicQuery: true}
	rixr, err := NewIndexerFromSnapshot(ctx, reread, loader, testLogger())
	if err != nil {
		t.Fatalf("NewIndexerFromSnapshot: %v", err)
	}
	if !rixr.Index().NoEmbed() {
		t.Fatal("loading a -no-embed snapshot must mark the Index NoEmbed()")
	}
	if st := rixr.Graph().Stats(); st.Nodes == 0 || st.Embedded != 0 {
		t.Fatalf("no-embed load: nodes=%d embedded=%d, want embedded=0", st.Nodes, st.Embedded)
	}

	// Registered on disk (the search/serve-snapshots path), the map is
	// reachable by its exact label but invisible to query-only routing: its
	// summary vector is empty, and Registry.rankBySummary skips entries with
	// no summary vector. The registry's own embedder legitimately embeds the
	// QUERY for routing/search regardless of a map's vectors, so only node
	// embedding (panicBatch) is forbidden here — panicQuery would fire on that
	// unrelated, expected call.
	regEmb := &stubEmbedder{dims: 64, panicBatch: true}
	reg := NewRegistry(regEmb, fp, 1, 0, false, testLogger())
	if err := reg.AddSnapshot(path); err != nil {
		t.Fatalf("AddSnapshot: %v", err)
	}
	// The query deliberately never names the label itself — labelsNamedIn would
	// otherwise pick the map by the word "gamma" appearing in the text, which
	// is a different routing path than the summary-vector rank this asserts.
	if _, err := reg.Resolve(ctx, "", "what does this project handle"); err == nil {
		t.Fatal("query-only routing must not resolve a no-embed map (no summary vector to rank)")
	}
	if _, err := reg.Resolve(ctx, "gamma", "what does this project handle"); err != nil {
		t.Fatalf("resolving by exact label must still work: %v", err)
	}
}

func TestSemanticRoutingAndByQN(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}

	aPath, _ := buildSnapshot(t, "alpha", writeFixtureRepo(t, "alpha"), emb)
	bPath, _ := buildSnapshot(t, "beta", writeFixtureRepo(t, "beta"), emb)

	reg := NewRegistry(emb, fp, 1, 0, false, testLogger())
	if err := reg.AddSnapshot(aPath); err != nil {
		t.Fatalf("AddSnapshot alpha: %v", err)
	}
	if err := reg.AddSnapshot(bPath); err != nil {
		t.Fatalf("AddSnapshot beta: %v", err)
	}

	ctx := context.Background()
	res, err := reg.Resolve(ctx, "", "tell me about alpha things")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(res) != 1 || res[0].Label != "alpha" {
		t.Fatalf("expected route to alpha, got %+v", res)
	}

	// Explicit project routing (exact label fast path).
	res, err = reg.Resolve(ctx, "beta", "anything")
	if err != nil || len(res) != 1 || res[0].Label != "beta" {
		t.Fatalf("explicit route to beta failed: %+v err=%v", res, err)
	}

	// Inexact project hint resolves SEMANTICALLY (no exact label "alpha-thing"),
	// not a string error — the hint embeds near the alpha summary.
	res, err = reg.Resolve(ctx, "alpha-thing", "")
	if err != nil || len(res) != 1 || res[0].Label != "alpha" {
		t.Fatalf("inexact hint must semantically route to alpha, got %+v err=%v", res, err)
	}

	// QN-prefix routing for node-keyed tools.
	rq, err := reg.ResolveByQN(ctx, "beta//main.go::betaHandler")
	if err != nil || rq.Label != "beta" {
		t.Fatalf("ResolveByQN beta failed: %+v err=%v", rq, err)
	}
	if _, err := reg.ResolveByQN(ctx, "ghost//x"); err == nil {
		t.Fatalf("expected error routing unknown project")
	}
}

// TestRoutingAndSearchStatsWiring checks the search-metrics wiring added to
// Registry: routing outcomes (explicit vs semantic) are counted, RecordSearch
// attributes a search's hits to its map label, and — the important one — the
// shared index.SearchStats instance injected into every *index.Index this
// registry builds (see load) keeps its counts cumulative across an
// evict/reload cycle instead of resetting when a fresh *Index replaces the
// evicted one. The evict/reload mechanics mirror TestTTLEvictAndReload.
func TestRoutingAndSearchStatsWiring(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}
	path, _ := buildSnapshot(t, "alpha", writeFixtureRepo(t, "alpha"), emb)

	reg := NewRegistry(emb, fp, 1, time.Hour, false, testLogger())
	if err := reg.AddSnapshot(path); err != nil {
		t.Fatalf("AddSnapshot: %v", err)
	}
	ctx := context.Background()

	// Explicit route (exact label fast path) + a real search through it.
	res, err := reg.Resolve(ctx, "alpha", "alpha things")
	if err != nil || len(res) != 1 {
		t.Fatalf("Resolve explicit: %+v err=%v", res, err)
	}
	hits, err := res[0].Index.Search(ctx, "alpha things", nil, "", index.Select{}, 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	reg.RecordSearch(res[0].Label, len(hits))

	// Semantic route: no explicit map name, and the query names no registered
	// label either, so it must go through embedding-based routing rather than
	// the exact-label fast path or the named-label seed.
	if _, err := reg.Resolve(ctx, "", "which handler returns a helper value"); err != nil {
		t.Fatalf("Resolve semantic: %v", err)
	}

	m := reg.Metrics()
	routing, ok := m["routing"].(RoutingSnapshot)
	if !ok {
		t.Fatalf("Metrics()[%q] has unexpected type %T", "routing", m["routing"])
	}
	if routing.ExplicitRoutes < 1 {
		t.Errorf("expected at least 1 explicit route, got %+v", routing)
	}
	if routing.SemanticRoutes < 1 {
		t.Errorf("expected at least 1 semantic route, got %+v", routing)
	}

	maps, ok := m["maps"].(map[string]MapCounters)
	if !ok {
		t.Fatalf("Metrics()[%q] has unexpected type %T", "maps", m["maps"])
	}
	if maps["alpha"].Searches != 1 || maps["alpha"].Hits != int64(len(hits)) {
		t.Errorf("expected alpha map counters {searches:1 hits:%d}, got %+v", len(hits), maps["alpha"])
	}

	// Force eviction, then reload — a fresh *Index gets built underneath, but
	// the registry's shared SearchStats must keep counting.
	e := reg.entry("alpha")
	e.mu.Lock()
	e.lastAccess = time.Now().Add(-2 * time.Hour)
	e.mu.Unlock()
	reg.evictIdle()

	res2, err := reg.Resolve(ctx, "alpha", "alpha things")
	if err != nil || len(res2) != 1 {
		t.Fatalf("Resolve after evict: %+v err=%v", res2, err)
	}
	if _, err := res2[0].Index.Search(ctx, "alpha things", nil, "", index.Select{}, 5); err != nil {
		t.Fatalf("Search after reload: %v", err)
	}

	if got := reg.searchStats.Snapshot().Searches; got != 2 {
		t.Errorf("shared SearchStats.Searches after evict/reload = %d, want 2 (cumulative, not reset)", got)
	}

	regSnap := reg.stats.Snapshot()
	if regSnap.MapLoads < 2 {
		t.Errorf("expected at least 2 map loads (initial + reload), got %d", regSnap.MapLoads)
	}
	if regSnap.MapEvictions < 1 {
		t.Errorf("expected at least 1 eviction, got %d", regSnap.MapEvictions)
	}
}

func TestTTLEvictAndReload(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}
	path, _ := buildSnapshot(t, "alpha", writeFixtureRepo(t, "alpha"), emb)

	reg := NewRegistry(emb, fp, 1, time.Hour, false, testLogger())
	if err := reg.AddSnapshot(path); err != nil {
		t.Fatalf("AddSnapshot: %v", err)
	}
	ctx := context.Background()

	// Load it, then force its lastAccess into the past and evict.
	if _, err := reg.Resolve(ctx, "alpha", "x"); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	e := reg.entry("alpha")
	e.mu.Lock()
	e.lastAccess = time.Now().Add(-2 * time.Hour)
	loadedBefore := e.loaded
	e.mu.Unlock()
	if !loadedBefore {
		t.Fatalf("expected loaded before evict")
	}

	reg.evictIdle()

	e.mu.RLock()
	evicted := !e.loaded && e.ixr == nil
	vecKept := e.summaryVec != nil
	e.mu.RUnlock()
	if !evicted {
		t.Fatalf("expected evicted (ixr=nil, loaded=false)")
	}
	if !vecKept {
		t.Fatalf("summary vector must survive eviction for routing")
	}

	// Reload on demand.
	res, err := reg.Resolve(ctx, "alpha", "x")
	if err != nil || len(res) != 1 || res[0].Graph.Stats().Nodes == 0 {
		t.Fatalf("reload after evict failed: %+v err=%v", res, err)
	}
}

func TestUpdateMarksDirtyAndFlushesOnEvict(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}
	repoDir := writeFixtureRepo(t, "alpha")

	// Build the snapshot at a stable path we can re-read after flush.
	ctx := context.Background()
	ixr, err := NewIndexer(ctx, []string{"alpha=" + repoDir}, emb, testLogger(), false)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	snap, err := ixr.SnapshotFor(ctx, "alpha", fp, emb)
	if err != nil {
		t.Fatalf("SnapshotFor: %v", err)
	}
	snapPath := filepath.Join(t.TempDir(), "alpha.graph.gob.gz")
	if err := WriteSnapshot(snapPath, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}

	reg := NewRegistry(emb, fp, 1, time.Hour, true, testLogger())
	if err := reg.AddSnapshot(snapPath); err != nil {
		t.Fatalf("AddSnapshot: %v", err)
	}

	// Add a new file on disk and update the worked copy through the registry.
	mustWrite(t, filepath.Join(repoDir, "extra.go"),
		"package main\n\nfunc ExtraThing() int { return 7 }\n")
	changed, _, _, _, err := reg.UpdateByPaths(ctx, []string{filepath.Join(repoDir, "extra.go")})
	if err != nil || changed == 0 {
		t.Fatalf("UpdateByPaths: changed=%d err=%v", changed, err)
	}
	e := reg.entry("alpha")
	e.mu.RLock()
	dirty := e.dirty
	e.mu.RUnlock()
	if !dirty {
		t.Fatalf("expected entry marked dirty after update")
	}

	// Force eviction: it must flush first, persisting the new node to disk.
	e.mu.Lock()
	e.lastAccess = time.Now().Add(-2 * time.Hour)
	e.mu.Unlock()
	reg.evictIdle()

	// The on-disk snapshot must now contain extra.go.
	reread, err := ReadSnapshot(snapPath)
	if err != nil {
		t.Fatalf("ReadSnapshot after flush: %v", err)
	}
	if _, ok := reread.Body.Files["extra.go"]; !ok {
		t.Fatalf("flushed snapshot missing extra.go; files=%v", keysOf(reread.Body.Files))
	}
}

func TestReadOnlyRefusesUpdateAndSkipsFlush(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}
	repoDir := writeFixtureRepo(t, "alpha")
	ctx := context.Background()

	ixr, err := NewIndexer(ctx, []string{"alpha=" + repoDir}, emb, testLogger(), false)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	snap, err := ixr.SnapshotFor(ctx, "alpha", fp, emb)
	if err != nil {
		t.Fatalf("SnapshotFor: %v", err)
	}
	snapPath := filepath.Join(t.TempDir(), "alpha.graph.gob.gz")
	if err := WriteSnapshot(snapPath, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	before, _ := os.Stat(snapPath)

	// Read-only registry (writable=false).
	reg := NewRegistry(emb, fp, 1, time.Hour, false, testLogger())
	if err := reg.AddSnapshot(snapPath); err != nil {
		t.Fatalf("AddSnapshot: %v", err)
	}

	// update_code_graph is refused outright in read-only mode.
	if _, _, _, _, err := reg.UpdateByPaths(ctx, []string{filepath.Join(repoDir, "main.go")}); err == nil {
		t.Fatalf("read-only UpdateByPaths must return an error")
	}

	// Even a (hypothetically) dirty entry must not be written back on evict.
	e := reg.entry("alpha")
	if _, lerr := reg.load(ctx, e); lerr != nil {
		t.Fatalf("load: %v", lerr)
	}
	e.mu.Lock()
	e.dirty = true
	e.lastAccess = time.Now().Add(-2 * time.Hour)
	e.mu.Unlock()
	reg.evictIdle()

	after, _ := os.Stat(snapPath)
	if before != nil && after != nil && !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("read-only mode must not rewrite the snapshot on evict")
	}
}

func TestWatchDirRegistersAndDeregisters(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}

	// Build two snapshots; serve a dir that starts with only the first.
	aPath, _ := buildSnapshot(t, "alpha", writeFixtureRepo(t, "alpha"), emb)
	bPath, _ := buildSnapshot(t, "beta", writeFixtureRepo(t, "beta"), emb)

	dir := t.TempDir()
	copyFile(t, aPath, filepath.Join(dir, "alpha.graph.gob.gz"))

	reg := NewRegistry(emb, fp, 1, 0, false, testLogger())
	if err := reg.AddSnapshot(filepath.Join(dir, "alpha.graph.gob.gz")); err != nil {
		t.Fatalf("AddSnapshot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := reg.WatchDir(ctx, dir); err != nil {
		t.Fatalf("WatchDir: %v", err)
	}

	// Drop beta in — the watcher should register it without a restart.
	bDst := filepath.Join(dir, "beta.graph.gob.gz")
	copyFile(t, bPath, bDst)
	if !waitFor(func() bool { return reg.entry("beta") != nil }) {
		t.Fatalf("beta not registered after file add; labels=%v", reg.Labels())
	}

	// Remove beta — the watcher should de-register it.
	if err := os.Remove(bDst); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if !waitFor(func() bool { return reg.entry("beta") == nil }) {
		t.Fatalf("beta not deregistered after file remove; labels=%v", reg.Labels())
	}
}

// waitFor polls cond for up to ~2s (fsnotify delivery is async).
func waitFor(cond func() bool) bool {
	for i := 0; i < 200; i++ {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

func keysOf[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestAddSnapshotBytes registers a snapshot carried in memory (gob bytes, as a
// MemoryCell would hold it) and proves it resolves + loads its graph lazily from
// those bytes — the file-free transport path.
func TestAddSnapshotBytes(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}
	repoDir := writeFixtureRepo(t, "alpha")
	ctx := context.Background()

	ixr, err := NewIndexer(ctx, []string{"alpha=" + repoDir}, emb, testLogger(), false)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	snap, err := ixr.SnapshotFor(ctx, "alpha", fp, emb)
	if err != nil {
		t.Fatalf("SnapshotFor: %v", err)
	}
	// Serialize to gzipped-gob bytes exactly as the on-disk snapshot is written,
	// then hand the bytes (not a path) to the registry.
	snapPath := filepath.Join(t.TempDir(), "alpha.graph.gob.gz")
	if err := WriteSnapshot(snapPath, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	gobBytes, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("read snapshot bytes: %v", err)
	}

	reg := NewRegistry(emb, fp, 1, time.Hour, false, testLogger())
	if err := reg.AddSnapshotBytes(ctx, gobBytes); err != nil {
		t.Fatalf("AddSnapshotBytes: %v", err)
	}

	// Header registered without loading the body: label present, path empty.
	e := reg.entry("alpha")
	if e == nil {
		t.Fatalf("entry not registered from bytes")
	}
	if e.snapshotPath != "" {
		t.Fatalf("in-memory entry must have empty snapshotPath, got %q", e.snapshotPath)
	}

	// Resolve forces the lazy body decode — from bytes, not a file.
	res, err := reg.Resolve(ctx, "alpha", "x")
	if err != nil || len(res) != 1 {
		t.Fatalf("Resolve from bytes: res=%+v err=%v", res, err)
	}
	if res[0].Graph.Stats().Nodes == 0 {
		t.Fatalf("graph loaded from bytes has zero nodes")
	}

	// Evict, then reload — must re-decode from the retained bytes, not a file.
	e.mu.Lock()
	e.lastAccess = time.Now().Add(-2 * time.Hour)
	e.mu.Unlock()
	reg.evictIdle()
	res, err = reg.Resolve(ctx, "alpha", "x")
	if err != nil || len(res) != 1 || res[0].Graph.Stats().Nodes == 0 {
		t.Fatalf("reload from bytes after evict failed: res=%+v err=%v", res, err)
	}

	// A fingerprint mismatch is rejected.
	bad := NewRegistry(emb, EmbedFingerprint{Model: "stub", Dims: 128, MaxInputChars: 8192}, 1, time.Hour, false, testLogger())
	if err := bad.AddSnapshotBytes(ctx, gobBytes); err == nil {
		t.Fatalf("expected fingerprint-dims mismatch to be rejected")
	}
}

// TestRemoveSnapshotByLabel de-registers an in-memory snapshot by label.
func TestRemoveSnapshotByLabel(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}
	repoDir := writeFixtureRepo(t, "alpha")
	ctx := context.Background()

	ixr, err := NewIndexer(ctx, []string{"alpha=" + repoDir}, emb, testLogger(), false)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	snap, err := ixr.SnapshotFor(ctx, "alpha", fp, emb)
	if err != nil {
		t.Fatalf("SnapshotFor: %v", err)
	}
	snapPath := filepath.Join(t.TempDir(), "alpha.graph.gob.gz")
	if err := WriteSnapshot(snapPath, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	gobBytes, _ := os.ReadFile(snapPath)

	reg := NewRegistry(emb, fp, 1, time.Hour, false, testLogger())
	if err := reg.AddSnapshotBytes(ctx, gobBytes); err != nil {
		t.Fatalf("AddSnapshotBytes: %v", err)
	}
	if reg.entry("alpha") == nil {
		t.Fatalf("entry missing before remove")
	}
	reg.RemoveSnapshotByLabel("alpha")
	if reg.entry("alpha") != nil {
		t.Fatalf("entry still present after RemoveSnapshotByLabel")
	}
	reg.RemoveSnapshotByLabel("nonexistent") // no-op, must not panic
}

// TestSnapshotMetaRoundTrip proves the opaque per-node attribute bag (Body.Meta)
// survives snapshot → gob bytes → reload verbatim, and that xray neither requires
// nor interprets it (a snapshot with nil Meta loads fine).
func TestSnapshotMetaRoundTrip(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}
	repoDir := writeFixtureRepo(t, "alpha")
	ctx := context.Background()

	ixr, err := NewIndexer(ctx, []string{"alpha=" + repoDir}, emb, testLogger(), false)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	meta := map[string]map[string]string{
		"svcmap//services/auth.svc::auth": {
			"tier": "1", "domain": "Platform", "team": "platform",
			"symptoms": "auth failed, 401, token expired",
			"aliases":  "ops-ecosystem-auth,helmrelease/auth",
		},
	}
	ixr.SetMeta(meta)
	if got := ixr.Meta(); got["svcmap//services/auth.svc::auth"]["tier"] != "1" {
		t.Fatalf("SetMeta/Meta round-trip in memory failed: %v", got)
	}

	snap, err := ixr.SnapshotFor(ctx, "alpha", fp, emb)
	if err != nil {
		t.Fatalf("SnapshotFor: %v", err)
	}
	if snap.Body.Meta["svcmap//services/auth.svc::auth"]["domain"] != "Platform" {
		t.Fatalf("Meta not written into SnapshotBody")
	}

	// Serialize to gob bytes and reload — the format-level round-trip.
	snapPath := filepath.Join(t.TempDir(), "alpha.graph.gob.gz")
	if err := WriteSnapshot(snapPath, snap); err != nil {
		t.Fatalf("WriteSnapshot: %v", err)
	}
	reread, err := ReadSnapshot(snapPath)
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	got := reread.Body.Meta["svcmap//services/auth.svc::auth"]
	if got["tier"] != "1" || got["symptoms"] != "auth failed, 401, token expired" {
		t.Fatalf("Meta did not survive gob round-trip: %v", got)
	}

	// Reconstructed indexer exposes it via Meta().
	rixr, err := NewIndexerFromSnapshot(ctx, reread, emb, testLogger())
	if err != nil {
		t.Fatalf("NewIndexerFromSnapshot: %v", err)
	}
	if rixr.Meta()["svcmap//services/auth.svc::auth"]["team"] != "platform" {
		t.Fatalf("reloaded indexer lost Meta")
	}
}

// TestSnapshotNilMetaLoads proves a snapshot with no Meta (older builds, or a
// graph that carries none) loads without error — Meta is optional/additive.
func TestSnapshotNilMetaLoads(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}
	repoDir := writeFixtureRepo(t, "alpha")
	ctx := context.Background()

	ixr, err := NewIndexer(ctx, []string{"alpha=" + repoDir}, emb, testLogger(), false)
	if err != nil {
		t.Fatalf("NewIndexer: %v", err)
	}
	snap, err := ixr.SnapshotFor(ctx, "alpha", fp, emb) // no SetMeta → nil
	if err != nil {
		t.Fatalf("SnapshotFor: %v", err)
	}
	if snap.Body.Meta != nil {
		t.Fatalf("expected nil Meta, got %v", snap.Body.Meta)
	}
	rixr, err := NewIndexerFromSnapshot(ctx, snap, emb, testLogger())
	if err != nil {
		t.Fatalf("load with nil Meta failed: %v", err)
	}
	if rixr.Meta() != nil {
		t.Fatalf("expected nil Meta after reload")
	}
}

// TestNamedLabelRoutesAheadOfSemanticRank covers the routing hint a caller gives
// by naming a map INSIDE the query instead of in the mapName argument — what an
// agent does when it writes "... in project Zulu" rather than filling the map
// field. The summary vectors cannot recover that hint (a label is an identifier;
// one vector per map ranks by topic), so Resolve matches labels directly.
// The fixtures deliberately give each map a label that does NOT appear in its
// own content, so a route to it proves the label matched rather than the topic.
func TestNamedLabelRoutesAheadOfSemanticRank(t *testing.T) {
	emb := &stubEmbedder{dims: 64}
	fp := EmbedFingerprint{Model: "stub", Dims: 64, MaxInputChars: 8192}

	zPath, _ := buildSnapshot(t, "zulu", writeFixtureRepo(t, "alpha"), emb)
	yPath, _ := buildSnapshot(t, "yankee", writeFixtureRepo(t, "beta"), emb)

	newReg := func(fanout int) *Registry {
		reg := NewRegistry(emb, fp, fanout, 0, false, testLogger())
		for _, p := range []string{zPath, yPath} {
			if err := reg.AddSnapshot(p); err != nil {
				t.Fatalf("AddSnapshot %s: %v", p, err)
			}
		}
		return reg
	}
	ctx := context.Background()

	// Baseline: with no label named, this query routes on topic alone.
	reg := newReg(1)
	res, err := reg.Resolve(ctx, "", "tell me about beta things")
	if err != nil || len(res) != 1 || res[0].Label != "yankee" {
		t.Fatalf("topic-only route should reach yankee, got %+v err=%v", labelsIn(res), err)
	}

	// Same topical pull, now naming the other map: the named label takes the slot.
	res, err = reg.Resolve(ctx, "", "tell me about beta things in project zulu")
	if err != nil || len(res) != 1 || res[0].Label != "zulu" {
		t.Fatalf("named label must win the fanout slot, got %+v err=%v", labelsIn(res), err)
	}

	// Capitalised as a user would write it, and as an agent copies out of prose.
	res, err = reg.Resolve(ctx, "", "tell me about beta things in project Zulu")
	if err != nil || len(res) != 1 || res[0].Label != "zulu" {
		t.Fatalf("named label must match case-insensitively, got %+v err=%v", labelsIn(res), err)
	}

	// Naming a map SEEDS the set rather than replacing it: with room to fan out,
	// the named map leads and the semantic rank still fills the rest.
	reg = newReg(2)
	res, err = reg.Resolve(ctx, "", "tell me about beta things in project zulu")
	if err != nil || len(res) != 2 || res[0].Label != "zulu" {
		t.Fatalf("named label must lead a wider fanout, got %+v err=%v", labelsIn(res), err)
	}
	if res[1].Label != "yankee" {
		t.Fatalf("semantic rank must fill the remaining slot, got %+v", labelsIn(res))
	}

	// A mapName argument in the caller's own casing still takes the fast path.
	reg = newReg(1)
	res, err = reg.Resolve(ctx, "Zulu", "anything at all")
	if err != nil || len(res) != 1 || res[0].Label != "zulu" {
		t.Fatalf("explicit mapName must match case-insensitively, got %+v err=%v", labelsIn(res), err)
	}
}

func labelsIn(res []mcpserver.Resolved) []string {
	out := make([]string, len(res))
	for i, r := range res {
		out[i] = r.Label
	}
	return out
}
