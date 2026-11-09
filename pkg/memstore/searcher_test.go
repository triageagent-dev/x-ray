package memstore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	memoryv1 "github.com/triageagent-dev/x-ray/v2/api/v1"
	xindex "github.com/triageagent-dev/x-ray/v2/pkg/memstore/index"
)

// fixture is a store, a cell index fed by hand-made vectors, and a searcher over
// them. The embedder behind the index answers each query text with the vector
// the test registered, and counts its calls.
type fixture struct {
	store  *Store
	cells  *xindex.CellStore
	search *Searcher
	embeds *atomic.Int64
	vecs   map[string][]float32
}

func vec4(a, b, c, d float32) []float32 { return []float32{a, b, c, d} }

func newFixture(t *testing.T, cfg SearchConfig) *fixture {
	t.Helper()
	f := &fixture{store: NewStore(), embeds: new(atomic.Int64), vecs: map[string][]float32{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Input string }
		_ = json.NewDecoder(r.Body).Decode(&req)
		f.embeds.Add(1)
		v, ok := f.vecs[req.Input]
		if !ok {
			v = vec4(0, 0, 0, 1)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": v}}})
	}))
	t.Cleanup(srv.Close)
	emb, err := xindex.NewEmbedder(context.Background(), srv.URL, "m", "", "", 0, 0, zerolog.Nop())
	require.NoError(t, err)
	f.cells = xindex.NewCellStore(emb, zerolog.Nop())
	f.cells.SetScoreGap(0)
	cfg.Store = f.store
	if cfg.Cells == nil {
		cfg.Cells = func() *xindex.CellStore { return f.cells }
	}
	f.search = NewSearcher(cfg)
	return f
}

// add stores an item and indexes its vector.
func (f *fixture) add(t *testing.T, it Item, v []float32) Item {
	t.Helper()
	got := upsert(t, f.store, it)
	if v != nil {
		require.True(t, f.cells.SeedFromEmbedding(xindex.IndexEntry{ID: got.ID, Kind: got.Kind, Subject: got.Subject, Title: got.Title, Content: got.Content, Tags: got.Tags}, v))
	}
	return got
}

func resultIDs(rs []SearchResult) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Item.ID
	}
	return out
}

func TestHybridSearch_AConfidentLexicalAnswerSkipsTheEmbedder(t *testing.T) {
	f := newFixture(t, SearchConfig{})
	f.add(t, mk("a", "note", "cache eviction policy", "idle entries are evicted after ten minutes"), vec4(1, 0, 0, 0))
	f.add(t, mk("b", "note", "billing export", "runs every night"), vec4(0, 1, 0, 0))

	got := f.search.HybridSearch(context.Background(), "cache eviction policy", "", "", "", 5)
	require.NotEmpty(t, got)
	assert.Equal(t, "a", got[0].Item.ID)
	assert.Zero(t, f.embeds.Load(), "a confident lexical answer costs no embedding")
	assert.Equal(t, int64(1), f.search.Stats().Snapshot().KWVoiceHits)

	f.vecs["cache eviction policy"] = vec4(1, 0, 0, 0)
	full := f.search.HybridSearchFull(context.Background(), "cache eviction policy", "", "", "", 5)
	assert.Equal(t, "a", full[0].Item.ID)
	assert.EqualValues(t, 1, f.embeds.Load(), "the full search always consults the index")
}

func TestHybridSearch_FusesSemanticMatchesWhenTheLexicalAnswerIsNotConfident(t *testing.T) {
	f := newFixture(t, SearchConfig{})
	f.add(t, mk("lex", "note", "queue backlog", "messages wait in the queue"), vec4(0, 1, 0, 0))
	f.add(t, mk("sem", "note", "unrelated words", "totally different vocabulary here"), vec4(1, 0, 0, 0))
	f.vecs["where do messages pile up"] = vec4(1, 0, 0, 0)

	got := f.search.HybridSearch(context.Background(), "where do messages pile up", "", "", "", 5)
	assert.ElementsMatch(t, []string{"lex", "sem"}, resultIDs(got), "both channels contribute")
	snap := f.search.Stats().Snapshot()
	assert.Equal(t, int64(1), snap.SemanticCalls)
	assert.Equal(t, int64(1), snap.KWVoiceMisses)
	assert.Equal(t, int64(1), snap.FusedCount)
}

func TestHybridSearch_VisibilityAppliesToEveryChannel(t *testing.T) {
	visible := func(it Item, partition string) bool {
		p := PartitionFromTags(it.Tags)
		return p == "" || p == partition
	}
	f := newFixture(t, SearchConfig{Visible: visible})
	f.add(t, mk("mine", "note", "shared topic notes", "alpha notes", "tenant/a"), vec4(1, 0, 0, 0))
	f.add(t, mk("theirs", "note", "shared topic notes", "alpha notes theirs", "tenant/b"), vec4(0.99, 0.1, 0, 0))
	f.add(t, mk("open", "note", "shared topic notes", "alpha notes open"), vec4(0.98, 0.2, 0, 0))
	f.vecs["shared topic"] = vec4(1, 0, 0, 0)

	for _, q := range []string{"shared topic notes", "shared topic"} {
		got := resultIDs(f.search.HybridSearchFull(context.Background(), q, "", "", "a", 10))
		assert.NotContains(t, got, "theirs", q)
		assert.Contains(t, got, "mine", q)
	}
	all := resultIDs(f.search.HybridSearchFull(context.Background(), "shared topic notes", "", "", PartitionAll, 10))
	assert.Contains(t, all, "theirs", "PartitionAll lifts the filter")

	exact := f.search.HybridSearch(context.Background(), "theirs", "", "", "a", 5)
	assert.NotContains(t, resultIDs(exact), "theirs", "an exact id is not a way around visibility")
}

func TestHybridSearch_StarvationFallbackRescuesAWindowFilledByInvisibleCells(t *testing.T) {
	visible := func(it Item, partition string) bool { return PartitionFromTags(it.Tags) == partition }
	docVisible := func(d xindex.IndexEntry, partition string) bool { return PartitionFromTags(d.Tags) == partition }
	f := newFixture(t, SearchConfig{Visible: visible, DocVisible: docVisible})
	for i := 0; i < 8; i++ {
		f.add(t, mk(fmt.Sprintf("other-%d", i), "note", "zzz", fmt.Sprintf("qqq%d", i), "tenant/b"), vec4(1, float32(i)*0.01, 0, 0))
	}
	f.add(t, mk("wanted", "note", "yyy", "xxx", "tenant/a"), vec4(0.2, 1, 0, 0))
	f.vecs["a question with no shared words"] = vec4(1, 0, 0, 0)

	got := f.search.HybridSearch(context.Background(), "a question with no shared words", "", "", "a", 1)
	assert.Equal(t, []string{"wanted"}, resultIDs(got), "limit 1 fetches 4 cells, all invisible; the exact scan finds the visible one")
	assert.Equal(t, int64(1), f.search.Stats().Snapshot().ExhaustiveFallbacks)
}

func TestHybridSearch_ExactIDGoesFirstAtTheFixedScore(t *testing.T) {
	hydrated := map[string]Item{"data-1": mk("data-1", "dataset", "elsewhere", "kept out of the working set")}
	f := newFixture(t, SearchConfig{Hydrate: func(_ context.Context, id string) (Item, bool) {
		it, ok := hydrated[id]
		if ok {
			it.Status = "active"
		}
		return it, ok
	}})
	f.add(t, mk("note-1", "note", "alpha", "beta"), vec4(1, 0, 0, 0))
	f.add(t, mk("other", "note", "note-1 mentioned", "in the text of another note one"), vec4(0, 1, 0, 0))

	got := f.search.HybridSearch(context.Background(), "note-1", "", "", "", 5)
	require.NotEmpty(t, got)
	assert.Equal(t, "note-1", got[0].Item.ID)
	assert.Equal(t, ExactIDMatchScore, got[0].Score)

	got = f.search.HybridSearch(context.Background(), "data-1", "", "", "", 5)
	require.NotEmpty(t, got)
	assert.Equal(t, "data-1", got[0].Item.ID, "a cell outside the working set is hydrated by its id")

	priv := mk("mine-private", "note", "t", "c", PrivateOwnerTagPrefix+"u")
	priv.Scope = ScopePrivate
	upsert(t, f.store, priv)
	got = f.search.HybridSearch(context.Background(), "mine-private", "", "", "", 5)
	assert.NotContains(t, resultIDs(got), "mine-private", "a private item is never matched")

	got = f.search.HybridSearch(context.Background(), "NOTE-1", "note", "", "", 5)
	for _, r := range got {
		assert.NotEqual(t, ExactIDMatchScore, r.Score, "the fast path promotes an id only when its case matches")
	}
	got = f.search.HybridSearchFull(context.Background(), "NOTE-1", "note", "", "", 5)
	require.NotEmpty(t, got)
	assert.Equal(t, "note-1", got[0].Item.ID, "the full search folds case")
	assert.Equal(t, ExactIDMatchScore, got[0].Score)
}

func TestHybridSearchFull_MergesLiveMatchesAfterTheStoredOnes(t *testing.T) {
	var excluded map[string]struct{}
	f := newFixture(t, SearchConfig{Live: func(q, kind, partition string, exclude map[string]struct{}) []SearchResult {
		excluded = exclude
		return []SearchResult{
			{Item: mk("live-evt-1", "event", "live one", "from the host")},
			{Item: mk("live-evt-2", "event", "live two", "from the host")},
		}
	}})
	f.add(t, mk("stored", "note", "live topic words", "stored text"), vec4(1, 0, 0, 0))
	f.vecs["live topic words"] = vec4(1, 0, 0, 0)

	got := f.search.HybridSearchFull(context.Background(), "live topic words", "", "", "", 10)
	assert.Equal(t, []string{"stored", "live-evt-1", "live-evt-2"}, resultIDs(got))
	assert.Contains(t, excluded, "stored", "the host is told which ids are already in the result")

	got = f.search.HybridSearch(context.Background(), "live topic words", "", "", "", 10)
	assert.Equal(t, []string{"stored"}, resultIDs(got), "the plain search never calls the host")

	exact := newFixture(t, SearchConfig{Live: func(string, string, string, map[string]struct{}) []SearchResult {
		return []SearchResult{{Item: mk("LIVE-ID", "event", "t", "c")}}
	}})
	got = exact.search.HybridSearchFull(context.Background(), "live-id", "", "", "", 5)
	require.NotEmpty(t, got)
	assert.Equal(t, "LIVE-ID", got[0].Item.ID)
	assert.Equal(t, ExactIDMatchScore, got[0].Score, "a live match whose id is the query is promoted")
}

// ── rerank ────────────────────────────────────────────────────────────

func TestRerank_StateAnchorAndWatchMultipliers(t *testing.T) {
	open := map[string]bool{"open": true, "resolved": false}
	s := NewSearcher(SearchConfig{
		State:  func(it Item) (bool, bool) { o, ok := open[it.ID]; return o, ok },
		Anchor: func(it Item) bool { return it.ID == "anchor" },
	})
	now := metav1.NewTime(time.Now())
	at := func(id, kind string) Item {
		it := mk(id, kind, id, id)
		it.Confidence = "" // the confidence multiplier is checked in its own test
		it.UpdatedAt = now
		return it
	}
	out := s.Rerank([]Item{at("plain", "note"), at("open", "report"), at("resolved", "report"), at("anchor", "note"), at("watch", KindWatchItem)}, 10)
	score := map[string]float64{}
	for _, r := range out {
		score[r.Item.ID] = r.Score
	}
	assert.InDelta(t, RerankBaseScore(1)*1.6, score["open"], 1e-6)
	assert.InDelta(t, RerankBaseScore(2)*0.85, score["resolved"], 1e-6)
	assert.InDelta(t, RerankBaseScore(3)*1.4, score["anchor"], 1e-6)
	assert.InDelta(t, RerankBaseScore(4)*1.7, score["watch"], 1e-6)
	assert.InDelta(t, RerankBaseScore(0), score["plain"], 1e-6)
}

func TestRerank_PinnedMultiplierValues(t *testing.T) {
	assert.Equal(t, 0.08, rerankRankDecay)
	assert.Equal(t, 1.6, openMultiplier)
	assert.Equal(t, 0.85, resolvedMultiplier)
	assert.Equal(t, 1.4, anchorMultiplier)
	assert.Equal(t, 1.7, watchItemActiveMultiplier)
	assert.Equal(t, 7*24*time.Hour, recencyHalfLife)
	assert.Equal(t, 0.85, recencyFloor)
	assert.Equal(t, 60, maxRerankFetchLimit)
	assert.Equal(t, 1.0, recencyMultiplier(time.Time{}, time.Now()), "an unset time is neutral")
	assert.InDelta(t, 0.925, recencyMultiplier(time.Now().Add(-7*24*time.Hour), time.Now()), 0.001, "half way at the half-life")
}

func TestRerank_CollapsesItemsSharingAKeyAndNearDuplicateKnowledge(t *testing.T) {
	s := NewSearcher(SearchConfig{CollapseKey: func(it Item) string {
		if it.Kind == "report" {
			return "report:" + it.Subject
		}
		return ""
	}})
	r1 := mk("r1", "report", "t", "c")
	r1.Subject = "svc"
	r2 := mk("r2", "report", "t", "c")
	r2.Subject = "svc"
	r3 := mk("r3", "report", "t", "c")
	r3.Subject = "other"
	body := "The scheduler assigns pending jobs to the least loaded worker and retries a job twice before it gives up."
	k1 := mk("k1", KindStableFact, "Scheduler assigns pending jobs", body)
	k2 := mk("k2", KindStableFact, "Scheduler assigns pending jobs", body+" ")

	out := s.Rerank([]Item{r1, r2, r3, k1, k2}, 10)
	assert.ElementsMatch(t, []string{"r1", "r3", "k1"}, resultIDs(out), "the later duplicate of each group is folded away")
	for _, r := range out {
		if r.Item.ID == "r1" || r.Item.ID == "k1" {
			assert.Equal(t, 1, r.CollapsedCount, r.Item.ID)
		}
	}
	assert.Empty(t, NewSearcher(SearchConfig{}).Rerank(nil, 5))
	assert.Len(t, s.Rerank([]Item{r1, r3, k1}, 2), 2, "the result is cut to the limit")
}

// ── link expansion ───────────────────────────────────────────────────

func TestHybridSearch_LinkExpansionIsSwitchableAndRespectsVisibility(t *testing.T) {
	on := false
	visible := func(it Item, partition string) bool { return PartitionFromTags(it.Tags) != "b" }
	f := newFixture(t, SearchConfig{Visible: visible, LinkExpansion: func() bool { return on }})
	head := mk("head", "note", "zebra crossing", "stripes on a road", "tenant/a")
	head.Links = []memoryv1.ThoughtLink{
		{ToID: "friend", Relation: "explains", Weight: 0.9},
		{ToID: "foe", Relation: "explains", Weight: 0.9},
		{ToID: "gone", Relation: "explains", Weight: 0.9},
		{ToID: "head", Relation: "explains", Weight: 0.9},
	}
	f.add(t, head, nil)
	f.add(t, mk("friend", "note", "different words entirely", "no overlap"), nil)
	f.add(t, mk("foe", "note", "other words entirely", "no overlap here", "tenant/b"), nil)

	off := f.search.HybridSearch(context.Background(), "zebra crossing", "", "", "a", 6)
	assert.Equal(t, []string{"head"}, resultIDs(off))

	on = true
	got := f.search.HybridSearch(context.Background(), "zebra crossing", "", "", "a", 6)
	require.Equal(t, []string{"head", "friend"}, resultIDs(got), "an invisible or missing target, a self link and the host's off switch are all respected")
	assert.Equal(t, "head", got[1].Via)
	assert.Zero(t, got[1].Score, "an expanded result is not scored")

	one := f.search.HybridSearch(context.Background(), "zebra crossing", "", "", "a", 1)
	assert.Equal(t, []string{"head"}, resultIDs(one), "expansion stays within the limit")
}

// ── consolidate ──────────────────────────────────────────────────────

func TestConsolidate_ResolvesThePartitionStrictlyAndSupersedesDuplicates(t *testing.T) {
	known := map[string]string{"a": "alpha"}
	f := newFixture(t, SearchConfig{
		ResolveStrict: func(p string) (string, bool) { c, ok := known[p]; return c, ok },
		Visible:       func(it Item, partition string) bool { return PartitionFromTags(it.Tags) == partition },
	})
	body := "The ingest worker batches events in groups of five hundred and flushes the batch every two seconds."
	f.add(t, mk("old", KindStableFact, "Ingest batching", body, "tenant/alpha"), nil)
	time.Sleep(2 * time.Millisecond)
	f.add(t, mk("new", KindStableFact, "Ingest batching", body+" ", "tenant/alpha"), nil)
	f.add(t, mk("elsewhere", KindStableFact, "Ingest batching", body, "tenant/beta"), nil)

	assert.Equal(t, "partition is required", f.search.Consolidate(" ", false).Error)
	assert.Equal(t, "unknown partition zzz", f.search.Consolidate("zzz", false).Error)

	dry := f.search.Consolidate("a", false)
	require.Empty(t, dry.Error)
	assert.Equal(t, "alpha", dry.Partition)
	assert.False(t, dry.Applied)
	assert.Equal(t, 2, dry.Scanned, "only the partition's own cells are scanned")
	require.Len(t, dry.Groups, 1)
	assert.Equal(t, 1, dry.Superseded)
	st, _ := f.store.Get(dry.Groups[0].Superseded[0])
	assert.Equal(t, "active", st.Status, "a dry run writes nothing")

	done := f.search.Consolidate("a", true)
	require.Len(t, done.Groups, 1)
	loser := done.Groups[0].Superseded[0]
	keeper := done.Groups[0].Keep
	st, _ = f.store.Get(loser)
	assert.Equal(t, "superseded", st.Status)
	k, _ := f.store.Get(keeper)
	assert.Contains(t, k.Supersedes, loser)
	other, _ := f.store.Get("elsewhere")
	assert.Equal(t, "active", other.Status)
}

// ── stats and context ─────────────────────────────────────────────────

type recordingSink struct {
	adds map[string]int64
	obs  map[string]int
}

func (r *recordingSink) Add(n string, v int64)             { r.adds[n] += v }
func (r *recordingSink) Observe(n string, _ time.Duration) { r.obs[n]++ }

func TestSearchStats_ForwardToTheHostSinkAndAttributeTheCaller(t *testing.T) {
	sink := &recordingSink{adds: map[string]int64{}, obs: map[string]int{}}
	var dirty int
	f := newFixture(t, SearchConfig{Stats: sink, Dirty: func() { dirty++ }})
	f.add(t, mk("a", "note", "unique phrase here", "text"), nil)
	ctx := WithSearchCaller(context.Background(), "voice")
	f.search.HybridSearch(ctx, "unique phrase here", "", "", "", 3)

	assert.EqualValues(t, 1, sink.adds[StatTotal])
	assert.EqualValues(t, 1, sink.adds[StatKeywordHits])
	assert.Equal(t, 1, sink.obs["search.caller.voice"])
	assert.Equal(t, 1, sink.obs["search.phase.lexical"])
	assert.Equal(t, 1, sink.obs["search.phase.rerank"])
	assert.Equal(t, 1, dirty, "Dirty fires after every search")
	snap := f.search.Stats().Snapshot()
	require.Len(t, snap.ByCaller, 1)
	assert.Equal(t, "voice", snap.ByCaller[0].Caller)
}

func TestSearcher_NilSeamsBehaveAsAbsentFeatures(t *testing.T) {
	var s *Searcher
	assert.Nil(t, s.Stats())
	bare := NewSearcher(SearchConfig{Store: NewStore()})
	upsert(t, bare.Store(), mk("a", "note", "hello world", "text"))
	got := bare.HybridSearch(context.Background(), "hello world", "", "", "", 5)
	assert.Equal(t, []string{"a"}, resultIDs(got), "no cells, no visibility predicate, no hydrate: lexical only")
	assert.Nil(t, bare.HybridSearch(context.Background(), "  ", "", "", "", 5))
	assert.Nil(t, NewSearcher(SearchConfig{}).HybridSearch(context.Background(), "q", "", "", "", 5), "no store, no results")
}

// ── semantic cutoff after filtering ───────────────────────────────────

func cutoffFixture(t *testing.T, cfg SearchConfig) *fixture {
	t.Helper()
	cfg.Visible = func(it Item, partition string) bool { return PartitionFromTags(it.Tags) != "b" }
	f := newFixture(t, cfg)
	// The nearest cell belongs to a partition the caller cannot see.
	f.add(t, mk("hidden", "note", "zzz", "qqq", "tenant/b"), vec4(1, 0, 0, 0))
	f.add(t, mk("near", "note", "yyy", "ppp"), vec4(0.9, 0.43, 0, 0))
	f.add(t, mk("also", "note", "xxx", "ooo"), vec4(0.85, 0.52, 0, 0))
	f.add(t, mk("far", "note", "www", "nnn"), vec4(0.1, 0.99, 0, 0))
	f.vecs["no lexical overlap at all"] = vec4(1, 0, 0, 0)
	return f
}

func TestHybridSearch_AnInvisibleTopHitDoesNotStarveTheVisibleAnswersBelowIt(t *testing.T) {
	f := cutoffFixture(t, SearchConfig{})
	got := resultIDs(f.search.HybridSearch(context.Background(), "no lexical overlap at all", "", "", "a", 10))
	assert.Equal(t, []string{"near", "also", "far"}, got[:3], "all visible cells arrive, ranked by similarity")
	assert.NotContains(t, got, "hidden")
}

func TestHybridSearch_SemanticGapIsMeasuredAgainstTheVisibleTop(t *testing.T) {
	f := cutoffFixture(t, SearchConfig{SemanticGap: 0.02})
	got := resultIDs(f.search.HybridSearch(context.Background(), "no lexical overlap at all", "", "", "a", 10))
	assert.Contains(t, got, "near", "the cutoff starts from the best cell the caller can see")
	assert.Contains(t, got, "also", "within the gap of the visible top")
	assert.NotContains(t, got, "far")
}

func TestHybridSearch_SemanticCliffKeepsEverythingAboveTheSteepestDrop(t *testing.T) {
	f := cutoffFixture(t, SearchConfig{SemanticCliff: true})
	got := resultIDs(f.search.HybridSearch(context.Background(), "no lexical overlap at all", "", "", "a", 10))
	assert.Equal(t, []string{"near", "also"}, got, "the far cell falls off the cliff")
}

func TestHybridSearch_LinkExpansionFollowsTheStrongestLinksOfTheTopHitsFirst(t *testing.T) {
	f := newFixture(t, SearchConfig{LinkExpansion: func() bool { return true }})
	head := mk("head", "note", "kestrel flight", "a bird in the air")
	head.Links = []memoryv1.ThoughtLink{
		{ToID: "weak", Relation: "explains", Weight: 0.1},
		{ToID: "strong", Relation: "explains", Weight: 0.9},
		{ToID: "middle", Relation: "explains", Weight: 0.5},
	}
	f.add(t, head, nil)
	for _, id := range []string{"weak", "strong", "middle"} {
		f.add(t, mk(id, "note", "unrelated "+id, "other words "+id), nil)
	}

	// limit 5 leaves room for two expansions (limit/2), in order of link weight.
	got := resultIDs(f.search.HybridSearch(context.Background(), "kestrel flight", "", "", "", 5))
	assert.Equal(t, []string{"head", "strong", "middle"}, got)
}
