package index

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

	"github.com/rs/zerolog"
	"google.golang.org/genai"

	"github.com/triageagent-dev/x-ray/v2/internal/tokencap"
)

// defaultMaxInputChars caps input to stay within the 2048-token context window
// of text-embedding-005 (Vertex AI). ~4 chars per token × 2000 tokens = 8000
// chars. Gateway/sidecar models with a smaller window (e.g. a llama.cpp slot at
// ctx/slots tokens) need a smaller cap — override via NewEmbedder's maxInputChars.
const defaultMaxInputChars = 8000

// maxEmbedCacheEntries bounds the embedder's exact-text cache. Past it new
// vectors are simply not cached: the cache holds every text embedded, so left
// unbounded it grows with each unique query and document for the life of the
// process. Search queries have their own LRU in front (CellStore).
const maxEmbedCacheEntries = 20000

// Embedder generates vector embeddings. It has two modes:
//
//   - Vertex AI (genai != nil): uses google.golang.org/genai SDK with BackendVertexAI,
//     authenticated via Workload Identity (ADC). Created when endpoint == "".
//
//   - Gateway (endpoint != ""): routes through an OpenAI-compatible /v1/embeddings endpoint.
type Embedder struct {
	endpoint      string        // gateway mode: base URL (e.g. "http://localhost:6000")
	model         string        // embedding model name
	outputDims    int           // 0 = model default; reduces output vector dimension (Vertex AI native; gateway via OpenAI "dimensions" param, e.g. nomic-embed-text MRL truncation)
	maxInputChars int           // hard cap on input length before the embed call; 0 = defaultMaxInputChars
	window        *tokencap.Cap // gateway mode only; nil: no token window configured
	client        *http.Client  // gateway mode only
	genai         *genai.Client // non-nil when using Vertex AI directly
	logger        zerolog.Logger
	cacheMu       sync.RWMutex
	cache         map[string][]float32 // exact query → vector cache
	cacheHits     atomic.Int64
	embedCalls    atomic.Int64
	usage         UsageFunc // nil: calls are not accounted
	errSink       ErrorFunc // nil: failures are not reported
}

// UsageFunc receives every provider request the embedder makes: the model, how
// long it took, its input tokens (the provider's count, or an estimate of 4
// characters per token when it gives none) and whether it failed. A request
// answered from the embedder's cache is not reported. ctx is the caller's, so a
// receiver can attribute the call.
type UsageFunc func(ctx context.Context, model string, latency time.Duration, inputTokens int64, failed bool)

// ErrorFunc receives every failed provider request: the model, the endpoint
// path it went to ("/v1/embeddings" for a gateway, "vertex" for Vertex AI) and
// the error. ctx is the caller's, so a receiver can tell a cancelled caller from
// a failing endpoint.
type ErrorFunc func(ctx context.Context, model, path string, err error)

// SetUsageSink accounts every provider request this embedder makes. The sink
// may be called from several goroutines (EmbedBatch). Set it
// before the embedder is shared.
func (e *Embedder) SetUsageSink(fn UsageFunc) {
	if e != nil {
		e.usage = fn
	}
}

// SetErrorSink reports every failed provider request this embedder makes. Set
// it before the embedder is shared.
func (e *Embedder) SetErrorSink(fn ErrorFunc) {
	if e != nil {
		e.errSink = fn
	}
}

// SetMaxInputTokens sets the gateway model's input window in tokens: an input
// over it is cut to the longest prefix that fits, counted by the endpoint's
// own tokenizer (llama.cpp POST /tokenize). When the count fails the
// byte-capped text is sent and the failure is logged. n <= 0 is a no-op, and so
// is Vertex AI mode, which truncates server-side. Set it before the embedder is
// shared.
func (e *Embedder) SetMaxInputTokens(n int) {
	if e != nil && e.genai == nil && n > 0 {
		e.window = tokencap.New(e.endpoint, e.client, n)
	}
}

// Model is the embedding model name.
func (e *Embedder) Model() string {
	if e == nil {
		return ""
	}
	return e.model
}

// NewEmbedder creates an Embedder.
// When endpoint == "", uses Vertex AI via Workload Identity (ADC); project and location are required.
// When endpoint != "", routes through the OpenAI-compatible gateway; project and location are unused.
// outputDims reduces the output vector dimension (0 = model default). Applied to
// Vertex AI natively, and to the gateway via the OpenAI-compatible "dimensions"
// param (e.g. Matryoshka models like nomic-embed-text truncate cleanly to 128/256/512).
// maxInputChars hard-caps input length in bytes before the embed call (0 = defaultMaxInputChars);
// set it below the model's token window × ~4 to avoid the gateway rejecting oversized input.
// Bytes are not tokens, so SetMaxInputTokens fits the input to the window.
func NewEmbedder(ctx context.Context, endpoint, model, project, location string, outputDims, maxInputChars int, logger zerolog.Logger) (*Embedder, error) {
	if maxInputChars <= 0 {
		maxInputChars = defaultMaxInputChars
	}
	e := &Embedder{
		endpoint:      strings.TrimRight(endpoint, "/"),
		model:         model,
		outputDims:    outputDims,
		maxInputChars: maxInputChars,
		cache:         make(map[string][]float32),
		logger:        logger,
	}
	if endpoint == "" {
		gc, err := newVertexEmbedClient(ctx, project, location)
		if err != nil {
			return nil, fmt.Errorf("init Vertex AI embedder: %w", err)
		}
		e.genai = gc
	} else {
		e.client = &http.Client{Timeout: 30 * time.Second}
	}
	return e, nil
}

func newVertexEmbedClient(_ context.Context, project, location string) (*genai.Client, error) {
	if project == "" {
		return nil, fmt.Errorf("vectorStore.googleCloudProject is required for Vertex AI embeddings")
	}
	if location == "" {
		location = "europe-west4"
	}
	return genai.NewClient(context.Background(), &genai.ClientConfig{
		Project:  project,
		Location: location,
		Backend:  genai.BackendVertexAI,
		HTTPOptions: genai.HTTPOptions{
			APIVersion: "v1beta1",
		},
	})
}

// OutputDims returns the configured Matryoshka output dimension. 0 means the
// model's own size, with no truncation.
func (e *Embedder) OutputDims() int {
	if e == nil {
		return 0
	}
	return e.outputDims
}

// inputText is the text the embedder sends for text: text cut to its input cap.
func (e *Embedder) inputText(text string) string {
	if e == nil || len(text) <= e.maxInputChars {
		return text
	}
	return text[:e.maxInputChars]
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
// EmbedBatch. The /v1/embeddings gateway (llama.cpp) serves --parallel slots, so
// firing requests concurrently lets continuous batching fold them into shared
// forward passes — a large cold-embed (e.g. a code graph's ~1800 nodes) drops
// from minutes (serial round-trips) to seconds. Bounded so we don't overrun the
// server's slots or a Vertex rate limit. Vertex's own batching is unaffected
// (each e.embed call is still one item); this only parallelises the round-trips.
// Matched to the llama.cpp sidecar's --parallel slot count so every lane is fed.
const embedBatchConcurrency = 24

// EmbedBatch returns embeddings for multiple texts, preserving input order. It
// issues up to embedBatchConcurrency requests concurrently (see the const) and
// fails fast on the first error, cancelling the rest.
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

func (e *Embedder) embed(ctx context.Context, text, taskType string) ([]float32, error) {
	if len(text) > e.maxInputChars {
		e.logger.Warn().Int("orig_chars", len(text)).Int("cap", e.maxInputChars).
			Str("text", truncate(text, 80)).Msg("embed input truncated to char cap (embedding quality reduced; raise the model context window or shorten the cell)")
		text = e.inputText(text)
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

	// The token window applies to what is sent only: the cache key and the
	// fingerprint stay the byte-capped text, and the cut is a deterministic
	// function of it. A tokenize call is not a provider request, so it is not
	// timed or reported to the usage sink. A failed count is not a failed
	// embed: Fit returns the byte-capped text, and the server's answer decides.
	sent, origTokens, err := e.window.Fit(ctx, text)
	if err != nil {
		e.logger.Warn().Err(err).Str("text", truncate(text, 80)).Msg("embed input not counted against the token window; sending it byte-capped")
	} else if len(sent) < len(text) {
		e.logger.Warn().Int("orig_tokens", origTokens).Int("orig_chars", len(text)).Int("sent_chars", len(sent)).
			Str("text", truncate(text, 80)).Msg("embed input cut to the model's token window (embedding quality reduced; shorten the cell)")
	}

	e.embedCalls.Add(1)
	start := time.Now()
	var vec []float32
	var tokens int64
	if e.genai != nil {
		vec, tokens, err = e.embedVertex(ctx, text, taskType)
	} else {
		vec, tokens, err = e.embedGateway(ctx, sent)
	}
	if e.usage != nil {
		if tokens <= 0 {
			tokens = int64((len(sent) + 3) / 4)
		}
		e.usage(ctx, e.model, time.Since(start), tokens, err != nil)
	}
	if err != nil {
		if e.errSink != nil {
			path := "/v1/embeddings"
			if e.genai != nil {
				path = "vertex"
			}
			e.errSink(ctx, e.model, path, err)
		}
		return nil, err
	}
	e.logger.Trace().Str("text", truncate(text, 80)).Str("task", strings.ToLower(taskType)).Dur("dur", time.Since(start)).Msg("embed api call")

	e.cacheMu.Lock()
	if len(e.cache) < maxEmbedCacheEntries {
		e.cache[cacheKey] = vec
	}
	e.cacheMu.Unlock()
	return vec, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func (e *Embedder) embedVertex(ctx context.Context, text string, taskType string) ([]float32, int64, error) {
	cfg := &genai.EmbedContentConfig{
		TaskType:     taskType,
		AutoTruncate: true,
	}
	if e.outputDims > 0 {
		d := int32(e.outputDims)
		cfg.OutputDimensionality = &d
	}
	resp, err := e.genai.Models.EmbedContent(ctx, e.model, genai.Text(text), cfg)
	if err != nil {
		return nil, 0, fmt.Errorf("vertex embed: %w", err)
	}
	if len(resp.Embeddings) == 0 {
		return nil, 0, fmt.Errorf("vertex embed: empty response")
	}
	var tokens int64
	if st := resp.Embeddings[0].Statistics; st != nil {
		tokens = int64(st.TokenCount)
	}
	vals := resp.Embeddings[0].Values
	result := make([]float32, len(vals))
	for i, v := range vals {
		result[i] = float32(v)
	}
	return result, tokens, nil
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

// TruncateNormalize applies the Matryoshka reduction: keep the first dims
// components, then L2-renormalize. dims <= 0 or dims >= len(vec) returns the
// vector unchanged (still renormalized when truncated). Renormalization is
// required — a raw slice of a unit vector is not itself a unit vector, so
// cosine distances would be skewed without it.
func TruncateNormalize(vec []float32, dims int) []float32 {
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
	Usage *struct {
		PromptTokens int64 `json:"prompt_tokens"`
	} `json:"usage,omitempty"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (e *Embedder) embedGateway(ctx context.Context, text string) ([]float32, int64, error) {
	reqBody, err := json.Marshal(embeddingRequest{Model: e.model, Input: text})
	if err != nil {
		return nil, 0, fmt.Errorf("marshal embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint+"/v1/embeddings", bytes.NewReader(reqBody))
	if err != nil {
		return nil, 0, fmt.Errorf("create embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("embedding request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, fmt.Errorf("read embedding response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("embedding request failed (status %d): %s", resp.StatusCode, string(body))
	}

	var embResp embeddingResponse
	if err := json.Unmarshal(body, &embResp); err != nil {
		return nil, 0, fmt.Errorf("unmarshal embedding response: %w", err)
	}
	if embResp.Error != nil {
		return nil, 0, fmt.Errorf("embedding API error: %s", embResp.Error.Message)
	}
	if len(embResp.Data) == 0 || len(embResp.Data[0].Embedding) == 0 {
		return nil, 0, fmt.Errorf("embedding response contains no data")
	}
	var tokens int64
	if embResp.Usage != nil {
		tokens = embResp.Usage.PromptTokens
	}
	return TruncateNormalize(embResp.Data[0].Embedding, e.outputDims), tokens, nil
}
