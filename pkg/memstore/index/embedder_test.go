package index

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func l2norm(v []float32) float64 {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	return math.Sqrt(s)
}

func TestTruncateNormalize(t *testing.T) {
	// A non-unit 4-vector so we can see renormalization happen.
	vec := []float32{3, 4, 12, 84} // arbitrary

	t.Run("truncates to dims and renormalizes to unit length", func(t *testing.T) {
		got := TruncateNormalize(vec, 2)
		require.Len(t, got, 2)
		assert.InDelta(t, 1.0, l2norm(got), 1e-6, "truncated vector must be unit-norm")
		// direction preserved: first two components proportional to {3,4}
		assert.InDelta(t, 3.0/5.0, got[0], 1e-6)
		assert.InDelta(t, 4.0/5.0, got[1], 1e-6)
	})

	t.Run("dims >= len returns vector unchanged (no truncation, no renorm)", func(t *testing.T) {
		got := TruncateNormalize(vec, len(vec))
		assert.Equal(t, vec, got, "dims == len is a passthrough")
		got = TruncateNormalize(vec, len(vec)+10)
		assert.Equal(t, vec, got, "dims > len is a passthrough")
	})

	t.Run("dims <= 0 returns native vector unchanged", func(t *testing.T) {
		assert.Equal(t, vec, TruncateNormalize(vec, 0))
		assert.Equal(t, vec, TruncateNormalize(vec, -1))
	})

	t.Run("zero vector does not divide by zero", func(t *testing.T) {
		got := TruncateNormalize([]float32{0, 0, 0, 0}, 2)
		require.Len(t, got, 2)
		assert.Equal(t, []float32{0, 0}, got)
	})

	t.Run("already-unit truncation stays unit", func(t *testing.T) {
		// unit 3-vector; truncating to 2 then renormalizing must still be unit
		u := []float32{0.6, 0.8, 0}
		got := TruncateNormalize(u, 2)
		assert.InDelta(t, 1.0, l2norm(got), 1e-6)
	})
}

// gateway starts an OpenAI-compatible embeddings server. tokens < 0 omits the
// usage block; status != 200 makes every call fail.
func gateway(t *testing.T, status int, tokens int64, calls *atomic.Int64) *Embedder {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if status != http.StatusOK {
			http.Error(w, "boom", status)
			return
		}
		resp := map[string]any{"data": []map[string]any{{"embedding": []float32{1, 0, 0, 0}}}}
		if tokens >= 0 {
			resp["usage"] = map[string]any{"prompt_tokens": tokens}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	e, err := NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	require.NoError(t, err)
	return e
}

type usageCall struct {
	model  string
	tokens int64
	failed bool
}

func TestEmbedder_ModelAndUsageSink(t *testing.T) {
	var calls atomic.Int64
	e := gateway(t, http.StatusOK, 7, &calls)
	assert.Equal(t, "test-model", e.Model())

	var got []usageCall
	e.SetUsageSink(func(_ context.Context, model string, latency time.Duration, tokens int64, failed bool) {
		assert.GreaterOrEqual(t, latency, time.Duration(0))
		got = append(got, usageCall{model, tokens, failed})
	})

	_, err := e.EmbedDocument(context.Background(), "hello there")
	require.NoError(t, err)
	assert.Equal(t, []usageCall{{"test-model", 7, false}}, got, "the provider's token count is charged")

	// A repeat of the same text is served from the embedder's cache: no
	// provider request, so no usage record.
	_, err = e.EmbedDocument(context.Background(), "hello there")
	require.NoError(t, err)
	assert.Len(t, got, 1, "a cached hit is not a provider request")
	assert.EqualValues(t, 1, calls.Load())
}

func TestEmbedder_UsageSinkEstimatesTokensWhenTheProviderGivesNone(t *testing.T) {
	var calls atomic.Int64
	e := gateway(t, http.StatusOK, -1, &calls)
	var got []usageCall
	e.SetUsageSink(func(_ context.Context, model string, _ time.Duration, tokens int64, failed bool) {
		got = append(got, usageCall{model, tokens, failed})
	})
	_, err := e.EmbedQuery(context.Background(), "twelve chars") // 12 chars
	require.NoError(t, err)
	assert.Equal(t, []usageCall{{"test-model", 3, false}}, got, "4 characters per token")
}

func TestEmbedder_ErrorSinkAndFailedUsage(t *testing.T) {
	var calls atomic.Int64
	e := gateway(t, http.StatusInternalServerError, 0, &calls)
	var usage []usageCall
	var paths []string
	e.SetUsageSink(func(_ context.Context, model string, _ time.Duration, tokens int64, failed bool) {
		usage = append(usage, usageCall{model, tokens, failed})
	})
	e.SetErrorSink(func(_ context.Context, model, path string, err error) {
		assert.Equal(t, "test-model", model)
		assert.Error(t, err)
		paths = append(paths, path)
	})
	_, err := e.EmbedDocument(context.Background(), "x")
	require.Error(t, err)
	assert.Equal(t, []string{"/v1/embeddings"}, paths)
	require.Len(t, usage, 1)
	assert.True(t, usage[0].failed)
}

func TestEmbedder_SinksAreNilSafe(t *testing.T) {
	var e *Embedder
	e.SetUsageSink(nil)
	e.SetErrorSink(nil)
}
