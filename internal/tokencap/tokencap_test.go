package tokencap

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"
)

// fakeCount is the fake tokenizer: one token per two runes, plus the two
// special tokens (BOS/EOS) the embeddings route adds.
func fakeCount(s string) int { return (utf8.RuneCountInString(s)+1)/2 + 2 }

// tokenizer starts a llama.cpp-shaped POST /tokenize server that counts with
// fakeCount and counts its requests. status != 200 makes every call fail.
func tokenizer(t *testing.T, status int, calls *atomic.Int64) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if status != http.StatusOK {
			http.Error(w, "boom", status)
			return
		}
		var req struct {
			Content    string `json:"content"`
			AddSpecial bool   `json:"add_special"`
		}
		if r.Method != http.MethodPost || r.URL.Path != "/tokenize" || json.NewDecoder(r.Body).Decode(&req) != nil || !req.AddSpecial {
			http.Error(w, "want POST /tokenize with add_special", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tokens": make([]int, fakeCount(req.Content))})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// assertLongestFit checks that got is a valid UTF-8 prefix of text within the
// window, and that one more rune of text would not fit.
func assertLongestFit(t *testing.T, text, got string, window int) {
	t.Helper()
	if !utf8.ValidString(got) || !strings.HasPrefix(text, got) {
		t.Fatalf("result is not a valid UTF-8 prefix of the input: %q", got)
	}
	if n := fakeCount(got); n > window {
		t.Fatalf("result counts %d tokens, window is %d", n, window)
	}
	_, size := utf8.DecodeRuneInString(text[len(got):])
	if n := fakeCount(text[:len(got)+size]); n <= window {
		t.Fatalf("one more rune still fits (%d tokens <= %d): the cut is not the longest prefix", n, window)
	}
}

func TestFit_TextThatFitsIsNotCounted(t *testing.T) {
	var calls atomic.Int64
	c := New(tokenizer(t, http.StatusOK, &calls), http.DefaultClient, 512)

	// 510 bytes plus the two special tokens can never exceed 512 tokens.
	short := strings.Repeat("a", 510)
	got, n, err := c.Fit(context.Background(), short)
	if err != nil || got != short || n != 0 {
		t.Fatalf("Fit(510 bytes) = %d chars, %d tokens, %v; want the text unchanged and not counted", len(got), n, err)
	}
	if calls.Load() != 0 {
		t.Fatalf("a text with fewer bytes than the window made %d tokenizer calls, want 0", calls.Load())
	}

	// 900 bytes may exceed the window, so it is counted once (452 tokens) and
	// sent whole.
	mid := strings.Repeat("b", 900)
	got, n, err = c.Fit(context.Background(), mid)
	if err != nil || got != mid || n != fakeCount(mid) {
		t.Fatalf("Fit(900 bytes) = %d chars, %d tokens, %v; want the text unchanged at %d tokens", len(got), n, err, fakeCount(mid))
	}
	if calls.Load() != 1 {
		t.Fatalf("a counted text that fits made %d tokenizer calls, want 1", calls.Load())
	}
}

func TestFit_CutsToTheLongestPrefixInTheWindow(t *testing.T) {
	var calls atomic.Int64
	c := New(tokenizer(t, http.StatusOK, &calls), http.DefaultClient, 512)

	text := strings.Repeat("policy section with a table row | ", 40)[:1196] // 600 tokens
	if fakeCount(text) != 600 {
		t.Fatalf("fixture counts %d tokens, want 600", fakeCount(text))
	}
	got, n, err := c.Fit(context.Background(), text)
	if err != nil {
		t.Fatal(err)
	}
	if n != 600 {
		t.Errorf("original token count = %d, want 600", n)
	}
	assertLongestFit(t, text, got, 512)
	if calls.Load() > 25 {
		t.Errorf("the cut took %d tokenizer calls, want at most 25 (one count plus 24 probes)", calls.Load())
	}
}

func TestFit_MixedCyrillicAndLatin(t *testing.T) {
	var calls atomic.Int64
	c := New(tokenizer(t, http.StatusOK, &calls), http.DefaultClient, 128)

	text := strings.Repeat("Політика key rotation для HSM, ротація ключів щокварталу. ", 10)
	got, n, err := c.Fit(context.Background(), text)
	if err != nil {
		t.Fatal(err)
	}
	if n != fakeCount(text) {
		t.Errorf("original token count = %d, want %d", n, fakeCount(text))
	}
	assertLongestFit(t, text, got, 128)
}

func TestFit_TokenizerErrorIsReturned(t *testing.T) {
	var calls atomic.Int64
	c := New(tokenizer(t, http.StatusInternalServerError, &calls), http.DefaultClient, 16)
	if _, _, err := c.Fit(context.Background(), strings.Repeat("x", 100)); err == nil {
		t.Fatal("a tokenizer answering 500 must fail the fit")
	}
}

func TestFit_NilAndZeroWindowAreNoOps(t *testing.T) {
	var calls atomic.Int64
	url := tokenizer(t, http.StatusOK, &calls)
	long := strings.Repeat("z", 5000)

	var nilCap *Cap
	if got, n, err := nilCap.Fit(context.Background(), long); err != nil || got != long || n != 0 {
		t.Errorf("nil Cap: got %d chars, %d tokens, %v; want the text unchanged", len(got), n, err)
	}
	if got, n, err := New(url, http.DefaultClient, 0).Fit(context.Background(), long); err != nil || got != long || n != 0 {
		t.Errorf("window 0: got %d chars, %d tokens, %v; want the text unchanged", len(got), n, err)
	}
	if calls.Load() != 0 {
		t.Errorf("a no-op cap made %d tokenizer calls, want 0", calls.Load())
	}
}
