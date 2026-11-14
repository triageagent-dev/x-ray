package memory_test

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/triageagent-dev/x-ray/v2/pkg/memory"
	"github.com/triageagent-dev/x-ray/v2/pkg/xray"
)

func newStore(t *testing.T, opts ...memory.Option) *memory.Store {
	t.Helper()
	s, err := memory.New(context.Background(), opts...)
	require.NoError(t, err)
	return s
}

// The SDK's client and this store implement one interface.
var _ xray.Memory = (*memory.Store)(nil)

func TestStore_WriteSearchFetchDelete(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	res, err := s.Upsert(ctx, xray.Write{
		Kind: xray.KindStableFact, Scope: xray.ScopePublic, Confidence: "high",
		Title: "Checkout retries on 503", Content: "The payment client retries a 503 three times with jitter.",
		Subject: "checkout", Tags: []string{"payments"},
	})
	require.NoError(t, err)
	assert.Equal(t, "created", res.Status)

	hits, err := s.Search(ctx, "payment retries 503", xray.WithLimit(5))
	require.NoError(t, err)
	require.NotEmpty(t, hits)
	assert.Equal(t, res.ID, hits[0].ID)
	assert.Equal(t, xray.OriginDerived, hits[0].Origin)

	got, err := s.Get(ctx, res.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"payments"}, got.Tags)

	sum, err := s.Summary(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, sum.Total)

	require.NoError(t, s.Delete(ctx, res.ID))
	_, err = s.Get(ctx, res.ID)
	assert.True(t, xray.IsNotFound(err))
}

func TestStore_RefusesWhatTheServerRefuses(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	for name, w := range map[string]xray.Write{
		"unknown kind":              {Kind: "no-such-kind", Scope: xray.ScopePublic, Title: "t", Content: "c"},
		"stable fact w/o high conf": {Kind: xray.KindStableFact, Scope: xray.ScopePublic, Title: "t", Content: "c"},
		"private w/o subject":       {Kind: xray.KindNote, Scope: xray.ScopePrivate, Title: "t", Content: "c"},
		"bad event date":            {Kind: xray.KindEvent, Scope: xray.ScopePublic, Title: "t", Content: "c", EventDate: "tomorrow"},
	} {
		_, err := s.Upsert(ctx, w)
		var te *xray.ToolError
		assert.True(t, errors.As(err, &te), name)
	}
	_, err := s.Search(ctx, "x", xray.WithKind("no-such-kind"))
	assert.Error(t, err)
}

func TestStore_PrivateItemsNeedTheirSubject(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	res, err := s.Upsert(ctx, xray.Write{Kind: xray.KindNote, Scope: xray.ScopePrivate, Subject: "agent-a",
		Title: "My scratch", Content: "working notes for agent a"})
	require.NoError(t, err)

	hits, _ := s.Search(ctx, "scratch")
	assert.Empty(t, hits)
	hits, _ = s.Search(ctx, "scratch", xray.WithSubject("agent-a"))
	assert.Len(t, hits, 1)

	err = s.Delete(ctx, res.ID, xray.WithSubject("agent-b"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "forbidden")
	require.NoError(t, s.Delete(ctx, res.ID, xray.WithSubject("agent-a")))
}

func TestStore_RestatedFactMerges(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	w := xray.Write{Kind: xray.KindStableFact, Scope: xray.ScopePublic, Confidence: "high",
		Title: "Deploys freeze on Fridays", Content: "No production deploys are made after noon on Friday.", Subject: "release"}
	first, err := s.Upsert(ctx, w)
	require.NoError(t, err)
	w.Content = "No production deploys are made after noon on a Friday."
	second, err := s.Upsert(ctx, w)
	require.NoError(t, err)
	assert.Equal(t, "merged", second.Status)
	assert.Equal(t, first.ID, second.MergedInto)

	off := newStore(t, memory.WithWriteDedup(false))
	a, _ := off.Upsert(ctx, w)
	w.Content = "No production deploys are made after noon on Friday."
	b, _ := off.Upsert(ctx, w)
	assert.NotEqual(t, a.ID, b.ID, "with dedup off a restatement is its own item")
}

func TestStore_RefuteCascades(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	base, err := s.Upsert(ctx, xray.Write{Kind: xray.KindHypothesis, Scope: xray.ScopePublic,
		Title: "Cache is cold after deploy", Content: "Latency spikes come from a cold cache after each rollout."})
	require.NoError(t, err)
	child, err := s.Upsert(ctx, xray.Write{Kind: xray.KindWatchItem, Scope: xray.ScopePublic,
		Title: "Warm the cache before rollout", Content: "Pre-warm the edge cache ahead of every release.",
		EvidenceRefs: []string{base.ID}})
	require.NoError(t, err)

	out, err := s.Refute(ctx, base.ID, "the spikes predate the cache", nil)
	require.NoError(t, err)
	assert.Equal(t, []string{child.ID}, out.Cascaded)
	hits, _ := s.Search(ctx, "cache rollout")
	assert.Empty(t, hits)
}

// A human item is never replaced by what a model wrote, in process too.
func TestStore_DerivedWriteCannotReplaceHuman(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	file := filepath.Join(dir, "m.json")

	human := newStore(t, memory.WithOrigin(xray.OriginHuman), memory.WithFile(file))
	res, err := human.Upsert(ctx, xray.Write{ID: "policy", Kind: xray.KindOperationalPattern, Scope: xray.ScopePublic,
		Title: "Rollbacks", Content: "Always roll back before debugging."})
	require.NoError(t, err)
	require.NoError(t, human.Close())

	model := newStore(t, memory.WithFile(file)) // derived by default, same file
	_, err = model.Upsert(ctx, xray.Write{ID: res.ID, Kind: xray.KindOperationalPattern, Scope: xray.ScopePublic,
		Title: "Rollbacks", Content: "Debug first, then decide."})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "human")
	got, err := model.Get(ctx, res.ID)
	require.NoError(t, err)
	assert.Equal(t, xray.OriginHuman, got.Origin)
}

func TestStore_FileKeepsEverythingAcrossARestart(t *testing.T) {
	ctx := context.Background()
	file := filepath.Join(t.TempDir(), "sub", "memory.json")

	s := newStore(t, memory.WithFile(file))
	pub, err := s.Upsert(ctx, xray.Write{Kind: xray.KindOperationalPattern, Scope: xray.ScopePublic,
		Title: "Queue drains hourly", Content: "The ingest queue drains once an hour.", Tags: []string{"ops"}})
	require.NoError(t, err)
	priv, err := s.Upsert(ctx, xray.Write{Kind: xray.KindNote, Scope: xray.ScopePrivate, Subject: "me",
		Title: "Private", Content: "only for me"})
	require.NoError(t, err)
	wrong, err := s.Upsert(ctx, xray.Write{Kind: xray.KindHypothesis, Scope: xray.ScopePublic,
		Title: "Wrong theory", Content: "The moon causes the outages."})
	require.NoError(t, err)
	_, err = s.Refute(ctx, wrong.ID, "no evidence", nil)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	info, err := os.Stat(file)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	again := newStore(t, memory.WithFile(file))
	hits, err := again.Search(ctx, "queue drains")
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, pub.ID, hits[0].ID)
	assert.Equal(t, []string{"ops"}, hits[0].Tags)

	got, err := again.Get(ctx, priv.ID, xray.WithSubject("me"))
	require.NoError(t, err)
	assert.Equal(t, "only for me", got.Content)
	_, err = again.Get(ctx, priv.ID)
	assert.True(t, xray.IsNotFound(err), "a private item stays private after a restart")

	hits, _ = again.Search(ctx, "moon outages")
	assert.Empty(t, hits, "a refuted item stays out of search after a restart")
}

// A file written by an earlier release names a private item's owner as its
// subject. It loads, the owner still reads it, and nobody else does.
func TestStore_FileFromAnEarlierReleaseStillLoads(t *testing.T) {
	ctx := context.Background()
	file := filepath.Join(t.TempDir(), "memory.json")
	legacy := `[
	 {"id":"shared-1","kind":"stable_fact","scope":"public","title":"Queue drains hourly","content":"The ingest queue drains once an hour.","confidence":"high","status":"active","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"},
	 {"id":"mine-1","kind":"note","scope":"private","subject":"me","title":"Scratch","content":"only for me","status":"active","createdAt":"2026-01-01T00:00:00Z","updatedAt":"2026-01-01T00:00:00Z"}
	]`
	require.NoError(t, os.WriteFile(file, []byte(legacy), 0o600))

	s := newStore(t, memory.WithFile(file))
	hits, err := s.Search(ctx, "queue drains")
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, "shared-1", hits[0].ID)

	got, err := s.Get(ctx, "mine-1", xray.WithSubject("me"))
	require.NoError(t, err)
	assert.Equal(t, "only for me", got.Content)
	_, err = s.Get(ctx, "mine-1", xray.WithSubject("someone-else"))
	assert.True(t, xray.IsNotFound(err))
	_, err = s.Get(ctx, "mine-1")
	assert.True(t, xray.IsNotFound(err))

	require.NoError(t, s.Close())
	again := newStore(t, memory.WithFile(file))
	got, err = again.Get(ctx, "mine-1", xray.WithSubject("me"))
	require.NoError(t, err)
	assert.Equal(t, "only for me", got.Content, "the file written back reads the same")
}

// synonyms stands in for what a real model knows: these words mean the same.
var synonyms = map[string]string{"incident": "outage", "incidents": "outage", "outages": "outage"}

// hashEmbed is a bag-of-words embedding: texts that share words, or synonyms,
// are close.
func hashEmbed(text string) []float32 {
	v := make([]float32, 16)
	for _, w := range strings.Fields(strings.ToLower(text)) {
		w = strings.Trim(w, ".,:;!?")
		if len(w) < 3 {
			continue
		}
		if canon, ok := synonyms[w]; ok {
			w = canon
		}
		h := fnv.New32a()
		_, _ = h.Write([]byte(w))
		v[h.Sum32()%16]++
	}
	var n float64
	for _, x := range v {
		n += float64(x * x)
	}
	if n == 0 {
		v[0] = 1
		return v
	}
	for i := range v {
		v[i] /= float32(math.Sqrt(n))
	}
	return v
}

func embeddingServer(t *testing.T, calls *int) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		*calls++
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": hashEmbed(req.Input)}}})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestStore_SemanticSearchWithAnEmbeddingEndpoint(t *testing.T) {
	ctx := context.Background()
	var calls int
	s := newStore(t, memory.WithEmbeddings(embeddingServer(t, &calls), "fake", 16))

	for _, w := range []xray.Write{
		{Kind: xray.KindOperationalPattern, Scope: xray.ScopePublic, Title: "Retry policy",
			Content: "payments client retries failures three times with jitter"},
		{Kind: xray.KindOperationalPattern, Scope: xray.ScopePublic, Title: "Runbook",
			Content: "during an outage page the platform team and open a bridge"},
		{Kind: xray.KindOperationalPattern, Scope: xray.ScopePublic, Title: "Release freeze",
			Content: "production deploys stop on friday afternoon"},
	} {
		_, err := s.Upsert(ctx, w)
		require.NoError(t, err)
	}
	assert.GreaterOrEqual(t, calls, 3, "each item was embedded on write")

	// "incident" appears nowhere in the store, so keyword search finds nothing:
	// only the vector, which treats it as the same thing as "outage", can.
	before := calls
	hits, err := s.Search(ctx, "incident", xray.WithLimit(1))
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, "Runbook", hits[0].Title)
	assert.Greater(t, calls, before, "the query was embedded")

	plain := newStore(t)
	_, err = plain.Upsert(ctx, xray.Write{Kind: xray.KindOperationalPattern, Scope: xray.ScopePublic, Title: "Runbook",
		Content: "during an outage page the platform team and open a bridge"})
	require.NoError(t, err)
	none, _ := plain.Search(ctx, "incident")
	assert.Empty(t, none, "without an embedding endpoint, a synonym finds nothing")
}

// windowServer is llama.cpp with a 512-token window: POST /tokenize counts one
// token per two bytes plus two special tokens, and /v1/embeddings refuses an
// input over the window with 400. It counts tokenize calls and refusals.
func windowServer(t *testing.T, tokenizes, refused *int) string {
	t.Helper()
	count := func(s string) int { return (len(s)+1)/2 + 2 }
	mux := http.NewServeMux()
	mux.HandleFunc("/tokenize", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		*tokenizes++
		_ = json.NewEncoder(w).Encode(map[string]any{"tokens": make([]int, count(req.Content))})
	})
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if count(req.Input) > 512 {
			*refused++
			http.Error(w, `{"error":{"code":400,"type":"exceed_context_size_error"}}`, http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": hashEmbed(req.Input)}}})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestStore_WithMaxInputTokensCutsAnItemToTheModelsWindow(t *testing.T) {
	dense := xray.Write{Kind: xray.KindOperationalPattern, Scope: xray.ScopePublic, Title: "Key rotation controls",
		Content: strings.Repeat("| control | owner | review cadence |\n", 33)} // about 620 tokens
	for _, window := range []int{0, 512} {
		var tokenizes, refused int
		opts := []memory.Option{memory.WithEmbeddings(windowServer(t, &tokenizes, &refused), "nomic", 16)}
		if window > 0 {
			opts = append(opts, memory.WithMaxInputTokens(window))
		}
		_, err := newStore(t, opts...).Upsert(context.Background(), dense)
		require.NoError(t, err)
		if window == 0 {
			assert.Zero(t, tokenizes, "without the option nothing is counted")
			assert.Equal(t, 1, refused, "without the option the model refuses the item")
		} else {
			assert.Positive(t, tokenizes, "the option reaches the embedder")
			assert.Zero(t, refused, "with the option the item is cut to fit")
		}
	}
}
