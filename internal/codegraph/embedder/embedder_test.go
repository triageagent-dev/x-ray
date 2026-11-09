package embedder

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/rs/zerolog"
)

// llama is a llama.cpp-shaped server with a token window: POST /tokenize counts
// one token per two runes plus two special tokens, and /v1/embeddings refuses an
// input over the window with llama.cpp's 400 instead of truncating it. With
// tokenizeStatus set, /tokenize answers that status instead and /v1/embeddings
// accepts any input, as a gateway that truncates on its side does. It records
// every input the embeddings route received.
type llama struct {
	window         int
	tokenizeStatus int
	mu             sync.Mutex
	inputs         []string
	tokenizes      int
}

func (l *llama) count(s string) int { return (utf8.RuneCountInString(s)+1)/2 + 2 }

func (l *llama) serve(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Content string `json:"content"`
			Input   string `json:"input"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		l.mu.Lock()
		defer l.mu.Unlock()
		switch r.URL.Path {
		case "/tokenize":
			l.tokenizes++
			if l.tokenizeStatus != 0 {
				http.Error(w, "no tokenizer", l.tokenizeStatus)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tokens": make([]int, l.count(req.Content))})
		case "/v1/embeddings":
			l.inputs = append(l.inputs, req.Input)
			if n := l.count(req.Input); n > l.window && l.tokenizeStatus == 0 {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
					"code": 400, "type": "exceed_context_size_error", "n_prompt_tokens": n, "n_ctx": l.window,
					"message": "the request exceeds the available context size, try increasing it",
				}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]any{{"embedding": []float32{1, 0, 0, 0}}}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// overWindow is 600 tokens to the fake tokenizer, within the byte cap.
var overWindow = strings.Repeat("| control | owner | review cadence |\n", 40)[:1196]

func TestEmbed_SetMaxInputTokensCutsAnOverWindowInput(t *testing.T) {
	l := &llama{window: 512}
	e, err := NewEmbedder(context.Background(), l.serve(t), "nomic", 0, 8192, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	e.SetMaxInputTokens(512)
	if _, err := e.EmbedDocument(context.Background(), overWindow); err != nil {
		t.Fatalf("an over-window input must be cut and accepted: %v", err)
	}
	if len(l.inputs) != 1 || l.count(l.inputs[0]) > 512 || !strings.HasPrefix(overWindow, l.inputs[0]) {
		t.Fatalf("sent %d inputs; want one prefix of the text within 512 tokens", len(l.inputs))
	}
}

func TestEmbed_WithoutWindowTheModelRefusesTheInput(t *testing.T) {
	l := &llama{window: 512}
	e, err := NewEmbedder(context.Background(), l.serve(t), "nomic", 0, 8192, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	_, err = e.EmbedDocument(context.Background(), overWindow)
	if err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("with no window the input goes out whole and the model refuses it; got %v", err)
	}
	if l.tokenizes != 0 {
		t.Errorf("with no window the tokenizer was called %d times, want 0", l.tokenizes)
	}
}

func TestEmbed_CharCapCutsOnARuneBoundary(t *testing.T) {
	l := &llama{window: 512}
	e, err := NewEmbedder(context.Background(), l.serve(t), "nomic", 0, 5, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.EmbedDocument(context.Background(), "абв"); err != nil { // 6 bytes, cap 5
		t.Fatal(err)
	}
	if len(l.inputs) != 1 || l.inputs[0] != "аб" {
		t.Fatalf("sent %q, want the cut before the split rune: %q", l.inputs, "аб")
	}
}

// A failed count is not a failed embed: the byte-capped text goes out and the
// server's answer decides.
func TestEmbed_WhenTheTokenizerFailsTheByteCappedTextIsSent(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
		l := &llama{window: 512, tokenizeStatus: status}
		e, err := NewEmbedder(context.Background(), l.serve(t), "nomic", 0, 1000, zerolog.Nop())
		if err != nil {
			t.Fatal(err)
		}
		e.SetMaxInputTokens(512)
		if _, err := e.EmbedDocument(context.Background(), overWindow); err != nil {
			t.Fatalf("status %d: a tokenizer failure must not fail the embed: %v", status, err)
		}
		if len(l.inputs) != 1 || l.inputs[0] != overWindow[:1000] || l.tokenizes != 1 {
			t.Fatalf("status %d: sent %d inputs after %d tokenize calls; want the 1000-byte cap of the text, once, after one call", status, len(l.inputs), l.tokenizes)
		}
	}
}
