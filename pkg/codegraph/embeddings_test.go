package codegraph_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/triageagent-dev/x-ray/v2/pkg/codegraph"
)

// SetMaxInputTokens reaches the embedder NewEmbeddings made: against llama.cpp
// with a 512-token window, an over-window node is cut instead of refused.
func TestEmbeddings_SetMaxInputTokensReachesTheEmbedder(t *testing.T) {
	count := func(s string) int { return (len(s)+1)/2 + 2 } // one token per two bytes, plus BOS/EOS
	mux := http.NewServeMux()
	mux.HandleFunc("/tokenize", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Content string `json:"content"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		_ = json.NewEncoder(w).Encode(map[string]any{"tokens": make([]int, count(req.Content))})
	})
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Input string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if count(req.Input) > 512 {
			http.Error(w, "the request exceeds the available context size", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": []float32{1, 0, 0, 0}}}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	node := strings.Repeat("| control | owner | review cadence |\n", 33) // about 620 tokens

	emb, err := codegraph.NewEmbeddings(context.Background(), srv.URL, "nomic", 4, 8192)
	require.NoError(t, err)
	_, err = emb.EmbedBatch(context.Background(), []string{node})
	require.Error(t, err, "without a window the model refuses the node")

	emb, err = codegraph.NewEmbeddings(context.Background(), srv.URL, "nomic", 4, 8192)
	require.NoError(t, err)
	emb.SetMaxInputTokens(512)
	_, err = emb.EmbedBatch(context.Background(), []string{node})
	assert.NoError(t, err, "with the window the node is cut to fit")
}
