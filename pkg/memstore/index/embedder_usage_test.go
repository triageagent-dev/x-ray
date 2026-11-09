package index

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestEmbedder_UsageSink: every embedding call reaches the sink with the
// provider's token count, or an estimate when the provider gives none, and
// failures are reported as failed.
func TestEmbedder_UsageSink(t *testing.T) {
	withUsage := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Fail") != "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if withUsage {
			_, _ = w.Write([]byte(`{"data":[{"embedding":[0.6,0.8]}],"usage":{"prompt_tokens":7}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"embedding":[0.6,0.8]}]}`))
	}))
	defer srv.Close()
	e, err := NewEmbedder(context.Background(), srv.URL, "embed-model", "", "", 0, 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	type call struct {
		model  string
		tokens int64
		failed bool
	}
	var calls []call
	e.SetUsageSink(func(_ context.Context, model string, _ time.Duration, tokens int64, failed bool) {
		calls = append(calls, call{model, tokens, failed})
	})
	if _, err := e.EmbedQuery(context.Background(), "payment errors"); err != nil {
		t.Fatal(err)
	}
	withUsage = false
	if _, err := e.EmbedQuery(context.Background(), "0123456789abcdef"); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0] != (call{"embed-model", 7, false}) || calls[1].tokens != 4 {
		t.Errorf("calls = %+v, want provider count 7 then estimate 4", calls)
	}
	srv.Close()
	_, _ = e.EmbedQuery(context.Background(), "x")
	if len(calls) != 3 || !calls[2].failed {
		t.Errorf("a failed call must be reported failed: %+v", calls)
	}
}
