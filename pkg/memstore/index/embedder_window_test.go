package index

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// windowCount is the fake tokenizer: one token per two runes plus the two
// special tokens.
func windowCount(s string) int { return (utf8.RuneCountInString(s)+1)/2 + 2 }

// windowGateway starts a server with a 512-token window and returns an
// embedder against it, built with the plain constructor. With tokenizeStatus
// 200 it is llama.cpp: POST /tokenize counts with windowCount and
// /v1/embeddings refuses an input over the window with llama.cpp's 400 instead
// of truncating. With any other status /tokenize answers that status and
// /v1/embeddings accepts any input, as a gateway that truncates on its side
// does. The last input embedded is stored in sent.
func windowGateway(t *testing.T, tokenizeStatus int, tokenizes *atomic.Int64, sent *atomic.Value) *Embedder {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Content string `json:"content"`
			Input   string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if r.URL.Path == "/tokenize" {
			tokenizes.Add(1)
			if tokenizeStatus != http.StatusOK {
				http.Error(w, "no tokenizer", tokenizeStatus)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tokens": make([]int, windowCount(req.Content))})
			return
		}
		if n := windowCount(req.Input); n > 512 && tokenizeStatus == http.StatusOK {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"code": 400, "type": "exceed_context_size_error", "n_prompt_tokens": n, "n_ctx": 512,
				"message": "the request exceeds the available context size, try increasing it",
			}})
			return
		}
		sent.Store(req.Input)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data":  []map[string]any{{"embedding": []float32{1, 0, 0, 0}}},
			"usage": map[string]any{"prompt_tokens": windowCount(req.Input)},
		})
	}))
	t.Cleanup(srv.Close)
	e, err := NewEmbedder(context.Background(), srv.URL, "test-model", "", "", 0, 0, zerolog.Nop())
	require.NoError(t, err)
	return e
}

// overWindowCell is 600 tokens to the fake tokenizer, within the 1800-byte
// cell cap: the shape of the dense policy section that failed in issue #61.
var overWindowCell = strings.Repeat("| control | owner | review cadence |\n", 40)[:1196]

func TestEmbedder_SetMaxInputTokensCutsAnOverWindowInput(t *testing.T) {
	var tokenizes atomic.Int64
	var sent atomic.Value
	e := windowGateway(t, http.StatusOK, &tokenizes, &sent)
	e.SetMaxInputTokens(512)

	var usage []usageCall
	e.SetUsageSink(func(_ context.Context, model string, _ time.Duration, tokens int64, failed bool) {
		usage = append(usage, usageCall{model, tokens, failed})
	})

	_, err := e.EmbedDocument(context.Background(), overWindowCell)
	require.NoError(t, err, "an over-window input must be cut and accepted")
	got, _ := sent.Load().(string)
	assert.True(t, strings.HasPrefix(overWindowCell, got), "the sent text is a prefix of the input")
	assert.LessOrEqual(t, windowCount(got), 512)
	assert.Positive(t, tokenizes.Load())

	require.Len(t, usage, 1, "a cut embed is one provider request; tokenize calls are not provider requests")
	assert.Equal(t, usageCall{"test-model", int64(windowCount(got)), false}, usage[0])
}

func TestEmbedder_WithoutWindowTheModelRefusesTheInput(t *testing.T) {
	var tokenizes atomic.Int64
	var sent atomic.Value
	e := windowGateway(t, http.StatusOK, &tokenizes, &sent)

	_, err := e.EmbedDocument(context.Background(), overWindowCell)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 400", "with no window the input goes out whole and the model refuses it")
	assert.Zero(t, tokenizes.Load(), "no window, no tokenizer call")
}

// A failed count is not a failed embed: the byte-capped text goes out and the
// server's answer decides.
func TestEmbedder_WhenTheTokenizerFailsTheByteCappedTextIsSent(t *testing.T) {
	long := strings.Repeat(overWindowCell, 7) // 8372 bytes, over the 8000-byte default cap
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
		var tokenizes atomic.Int64
		var sent atomic.Value
		e := windowGateway(t, status, &tokenizes, &sent)
		e.SetMaxInputTokens(512)
		var usage []usageCall
		e.SetUsageSink(func(_ context.Context, model string, _ time.Duration, tokens int64, failed bool) {
			usage = append(usage, usageCall{model, tokens, failed})
		})

		_, err := e.EmbedDocument(context.Background(), long)
		require.NoError(t, err, "status %d: a tokenizer failure must not fail the embed", status)
		assert.Equal(t, long[:defaultMaxInputChars], sent.Load(), "status %d: the byte-capped text is sent", status)
		assert.Equal(t, int64(1), tokenizes.Load(), "status %d", status)
		require.Len(t, usage, 1, "status %d: one provider request", status)
		assert.False(t, usage[0].failed, "status %d", status)
	}
}
