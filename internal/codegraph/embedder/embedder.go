// Package embedder turns text into vectors via an OpenAI-compatible
// /v1/embeddings endpoint — llama.cpp, Ollama, vLLM, LM Studio or any gateway
// that speaks the same shape. Everything runs against a URL you control; there
// is no cloud SDK and no credential handling here.
package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/tokencap"
)

// defaultMaxInputChars caps input length before the embed call. Local models
// have small context windows (a llama.cpp slot is ctx/slots tokens), so at
// ~4 chars per token this keeps a 2k-token window from being overrun. Override
// per-model via NewEmbedder's maxInputChars.
const defaultMaxInputChars = 8000

// Embedder generates vector embeddings through an OpenAI-compatible gateway,
// memoizing exact-text lookups so repeated queries cost nothing.
type Embedder struct {
	endpoint      string // base URL, e.g. "http://localhost:8090"
	model         string
	outputDims    int // 0 = model native; otherwise Matryoshka truncate + renormalize
	maxInputChars int
	window        *tokencap.Cap // nil: no token window configured
	client        *http.Client
	logger        zerolog.Logger
	cacheMu       sync.RWMutex
	cache         map[string][]float32
	cacheHits     atomic.Int64
	embedCalls    atomic.Int64
}

// NewEmbedder creates an Embedder against an OpenAI-compatible endpoint.
// outputDims reduces the output dimension (0 = model native); it is applied
// client-side as a Matryoshka truncation, see truncateNormalize.
// maxInputChars hard-caps input length in bytes before the call (0 =
// defaultMaxInputChars). maxInputTokens is the model's input window in tokens
// (0 = unknown): an input over it is cut to the longest prefix that fits,
// counted by the endpoint's own tokenizer.
func NewEmbedder(_ context.Context, endpoint, model string, outputDims, maxInputChars, maxInputTokens int, logger zerolog.Logger) (*Embedder, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("embeddings endpoint is required (e.g. -embed-endpoint http://localhost:8090)")
	}
	if maxInputChars <= 0 {
		maxInputChars = defaultMaxInputChars
	}
	e := &Embedder{
		endpoint:      strings.TrimRight(endpoint, "/"),
		model:         model,
		outputDims:    outputDims,
		maxInputChars: maxInputChars,
		client:        &http.Client{Timeout: 30 * time.Second},
		cache:         make(map[string][]float32),
		logger:        logger,
	}
	if maxInputTokens > 0 {
		e.window = tokencap.New(e.endpoint, e.client, maxInputTokens)
	}
	return e, nil
}

// EmbedDocument returns the embedding vector for a document to be indexed.
func (e *Embedder) EmbedDocument(ctx context.Context, text string) ([]float32, error) {
	return e.embed(ctx, text, "RETRIEVAL_DOCUMENT")
}

// EmbedQuery returns the embedding vector for a search query.
func (e *Embedder) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	return e.embed(ctx, text, "RETRIEVAL_QUERY")
}

// embedBatchConcurrency bounds how many embed requests are in flight at once in
// EmbedBatch. A llama.cpp gateway serves --parallel slots, so firing requests
// concurrently lets continuous batching fold them into shared forward passes —
// a cold embed of a repo's ~1800 nodes drops from minutes to seconds. Bounded
// so we don't overrun the server's slots.
const embedBatchConcurrency = 24

// EmbedBatch returns embeddings for multiple texts, preserving input order. It
// issues up to embedBatchConcurrency requests concurrently and fails fast on
// the first error, cancelling the rest.
func (e *Embedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	if len(texts) == 0 {
		return results, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, embedBatchConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, t := range texts {
		mu.Lock()
		stop := firstErr != nil
		mu.Unlock()
		if stop {
			break
		}

		// Acquire a slot, OR bail on cancellation. We only launch the goroutine
		// (whose deferred `<-sem` releases a slot) when `sem <- struct{}{}` actually
		// succeeds — otherwise that receive would drain a token never sent,
		// unbalancing the semaphore and deadlocking remaining iterations.
		acquired := false
		select {
		case sem <- struct{}{}:
			acquired = true
		case <-ctx.Done():
			mu.Lock()
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			mu.Unlock()
		}
		if !acquired {
			break // context cancelled — stop scheduling, wg.Wait drains in-flight work
		}

		wg.Add(1)
		go func(i int, t string) {
			defer wg.Done()
			defer func() { <-sem }()
			v, err := e.embed(ctx, t, "RETRIEVAL_DOCUMENT")
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel() // stop the rest
				}
				mu.Unlock()
				return
			}
			results[i] = v
		}(i, t)
	}

	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return results, nil
}

// EmbedMetrics holds cache and API call counters.
type EmbedMetrics struct {
	CacheHits  int64 `json:"cacheHits"`
	EmbedCalls int64 `json:"embedCalls"`
}

// Metrics returns a snapshot of cache hit and API call counters.
func (e *Embedder) Metrics() EmbedMetrics {
	return EmbedMetrics{
		CacheHits:  e.cacheHits.Load(),
		EmbedCalls: e.embedCalls.Load(),
	}
}

// embed is the cached path shared by document and query embedding. taskType
// only participates in the cache key: the same text embedded as a document and
// as a query must not collide, even though this gateway shape has no task
// parameter to send.
func (e *Embedder) embed(ctx context.Context, text, taskType string) ([]float32, error) {
	if len(text) > e.maxInputChars {
		e.logger.Warn().Int("orig_chars", len(text)).Int("cap", e.maxInputChars).
			Str("text", truncate(text, 80)).Msg("embed input truncated to char cap (embedding quality reduced; raise the model context window or shorten the input)")
		text = capBytes(text, e.maxInputChars)
	}
	cacheKey := taskType + "\x00" + text

	e.cacheMu.RLock()
	cached, ok := e.cache[cacheKey]
	e.cacheMu.RUnlock()
	if ok {
		e.cacheHits.Add(1)
		e.logger.Trace().Str("text", truncate(text, 80)).Str("task", strings.ToLower(taskType)).Msg("embed cache hit")
		return cached, nil
	}

	// The token window applies to what is sent only: the cache key stays the
	// byte-capped text, and the cut is a deterministic function of it.
	sent, origTokens, err := e.window.Fit(ctx, text)
	if err != nil {
		return nil, err
	}
	if len(sent) < len(text) {
		e.logger.Warn().Int("orig_tokens", origTokens).Int("orig_chars", len(text)).Int("sent_chars", len(sent)).
			Str("text", truncate(text, 80)).Msg("embed input cut to the model's token window (embedding quality reduced; shorten the input)")
	}

	e.embedCalls.Add(1)
	start := time.Now()
	vec, err := e.embedGateway(ctx, sent)
	if err != nil {
		return nil, err
	}
	e.logger.Trace().Str("text", truncate(text, 80)).Str("task", strings.ToLower(taskType)).Dur("dur", time.Since(start)).Msg("embed api call")

	e.cacheMu.Lock()
	e.cache[cacheKey] = vec
	e.cacheMu.Unlock()
	return vec, nil
}

// capBytes cuts s to at most n bytes on a rune boundary, so a cut never sends
// half of a multi-byte character.
func capBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// embeddingRequest is the OpenAI-compatible request body.
//
// Note: we deliberately do NOT send the OpenAI "dimensions" param. Dimension
// reduction for Matryoshka models is done client-side in embedGateway
// (truncate + L2-renormalize) because (a) some servers ignore the param
// (llama.cpp returns native dims regardless), and (b) at least one server
// (Ollama) truncates without correctly renormalizing, producing measurably
// worse vectors than the proper MRL procedure. Doing it ourselves is uniform
// and correct across every gateway.
type embeddingRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

// truncateNormalize applies the Matryoshka reduction: keep the first dims
// components, then L2-renormalize. dims <= 0 or dims >= len(vec) returns the
// vector unchanged. Renormalization is required — a raw slice of a unit vector
// is not itself a unit vector, so cosine distances would be skewed without it.
func truncateNormalize(vec []float32, dims int) []float32 {
	if dims > 0 && dims < len(vec) {
		vec = vec[:dims]
	} else {
		return vec // native dims, already normalized by the model
	}
	var sumSq float64
	for _, v := range vec {
		sumSq += float64(v) * float64(v)
	}
	norm := math.Sqrt(sumSq)
	if norm == 0 {
		return vec
	}
	out := make([]float32, len(vec))
	for i, v := range vec {
		out[i] = float32(float64(v) / norm)
	}
	return out
}

// embeddingResponse is the OpenAI-compatible response body.
type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (e *Embedder) embedGateway(ctx context.Context, text string) ([]float32, error) {
	reqBody, err := json.Marshal(embeddingRequest{Model: e.model, Input: text})
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint+"/v1/embeddings", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("create embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read embedding response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding request failed (status %d): %s", resp.StatusCode, string(body))
	}

	var embResp embeddingResponse
	if err := json.Unmarshal(body, &embResp); err != nil {
		return nil, fmt.Errorf("unmarshal embedding response: %w", err)
	}
	if embResp.Error != nil {
		return nil, fmt.Errorf("embedding API error: %s", embResp.Error.Message)
	}
	if len(embResp.Data) == 0 || len(embResp.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("embedding response contains no data")
	}
	return truncateNormalize(embResp.Data[0].Embedding, e.outputDims), nil
}
