// Package tokencap cuts embedding input to the model's token window. The input
// caps elsewhere count bytes, while the model's limit is tokens, and the two
// diverge on dense text (tables, code, non-Latin scripts); llama.cpp answers
// an input over its window with HTTP 400 instead of truncating it. The window
// is configured, like the embedding dimension, and the tokens are counted by
// the endpoint's own tokenizer (llama.cpp POST /tokenize), so the cut matches
// what the model will read.
package tokencap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
)

// specialTokens is what the embeddings route adds to every input (BOS/EOS).
// The tokenizer never makes more tokens than bytes, so a text of n bytes
// counts at most n+specialTokens and needs no count when that fits.
const specialTokens = 2

// maxProbes bounds the counts one cut may spend. Bisection over a few
// thousand rune offsets converges in about 13.
const maxProbes = 24

// Cap fits text to a token window. A nil *Cap, or one with window 0, passes
// text through unchanged.
type Cap struct {
	endpoint string
	client   *http.Client
	window   int
}

// New returns a Cap that counts tokens at endpoint's POST /tokenize with
// client, against a window of window tokens (0 = no cut).
func New(endpoint string, client *http.Client, window int) *Cap {
	return &Cap{endpoint: strings.TrimRight(endpoint, "/"), client: client, window: window}
}

// Fit returns the longest prefix of text, cut on a rune boundary, whose token
// count is within the window, and text's own token count (0 when text was too
// short to need counting). The prefix returned was always counted and fit.
func (c *Cap) Fit(ctx context.Context, text string) (string, int, error) {
	if c == nil || c.window <= 0 || len(text)+specialTokens <= c.window {
		return text, 0, nil
	}
	total, err := c.count(ctx, text)
	if err != nil || total <= c.window {
		return text, total, err
	}
	// ends[i] is the byte length of the prefix of i runes.
	ends := make([]int, 0, len(text)+1)
	for i := range text {
		ends = append(ends, i)
	}
	ends = append(ends, len(text))
	// lo fits once fitted is set; hi never fits. The first probe is the
	// prefix proportional to the overshoot, then bisection.
	lo, hi, fitted := 0, len(ends)-1, false
	mid := sort.SearchInts(ends, len(text)*c.window/total)
	for probes := 0; hi-lo > 1 && probes < maxProbes; probes++ {
		mid = min(max(mid, lo+1), hi-1)
		n, err := c.count(ctx, text[:ends[mid]])
		if err != nil {
			return "", total, err
		}
		if n <= c.window {
			lo, fitted = mid, true
		} else {
			hi = mid
		}
		mid = (lo + hi) / 2
	}
	if !fitted {
		return "", total, fmt.Errorf("tokenize: no prefix of the input fits the %d-token window", c.window)
	}
	return text[:ends[lo]], total, nil
}

// count returns the number of tokens the endpoint's tokenizer makes of text,
// special tokens included, as the embeddings route counts them.
func (c *Cap) count(ctx context.Context, text string) (int, error) {
	body, err := json.Marshal(map[string]any{"content": text, "add_special": true})
	if err != nil {
		return 0, fmt.Errorf("marshal tokenize request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/tokenize", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create tokenize request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("tokenize request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read tokenize response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("tokenize request failed (status %d): %s", resp.StatusCode, string(raw))
	}
	// Tokens are ids, or objects when pieces are asked for; only their number
	// matters here.
	var out struct {
		Tokens []json.RawMessage `json:"tokens"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return 0, fmt.Errorf("unmarshal tokenize response: %w", err)
	}
	if out.Tokens == nil {
		return 0, fmt.Errorf("tokenize response has no tokens: %s", string(raw))
	}
	return len(out.Tokens), nil
}
