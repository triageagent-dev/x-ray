//go:build cgo

package codegraph_test

import (
	"context"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/triageagent-dev/x-ray/v2/pkg/codegraph"
	"github.com/triageagent-dev/x-ray/v2/pkg/xray"
)

var _ xray.CodeGraph = (*codegraph.Graph)(nil)

// bagEmbedder is a small stand-in for a real embedding model: texts that share
// words are close. It needs no network.
type bagEmbedder struct{}

func (bagEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = bag(t)
	}
	return out, nil
}

func (bagEmbedder) EmbedQuery(_ context.Context, text string) ([]float32, error) {
	return bag(text), nil
}

func bag(text string) []float32 {
	v := make([]float32, 32)
	for _, w := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		if len(w) < 3 {
			continue
		}
		h := fnv.New32a()
		_, _ = h.Write([]byte(w))
		v[h.Sum32()%32]++
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

func embeddings() *codegraph.Embeddings {
	return codegraph.CustomEmbeddings(bagEmbedder{}, "bag", 32, 8192)
}

const payGo = `package pay

// RetryCharge retries a failed card charge three times with jitter.
func RetryCharge(id string) error {
	for i := 0; i < 3; i++ {
		if err := Charge(id); err == nil {
			return nil
		}
	}
	return Charge(id)
}

// Charge asks the card network to take a payment.
func Charge(id string) error { return nil }
`

const checkoutGo = `package pay

// Checkout completes an order and charges the card.
func Checkout(order string) error { return RetryCharge(order) }
`

const adrMD = "# ADR 7: Retry payments\n\n## Decision\n\nWe retry a failed card charge three times because the card network is flaky.\n"

func writeRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range map[string]string{"pay/pay.go": payGo, "pay/checkout.go": checkoutGo, "docs/adr-7.md": adrMD} {
		p := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	}
	return dir
}

func build(t *testing.T) (*codegraph.Graph, string) {
	t.Helper()
	dir := writeRepo(t)
	g, err := codegraph.Build(context.Background(), embeddings(), codegraph.Repo{Label: "shop", Path: dir})
	require.NoError(t, err)
	return g, dir
}

func TestBuild_SearchNodeImpactRisk(t *testing.T) {
	ctx := context.Background()
	g, _ := build(t)
	assert.Equal(t, []string{"shop"}, g.Labels())

	res, err := g.SearchGraph(ctx, "retry a failed card charge", xray.Limit(3))
	require.NoError(t, err)
	require.NotEmpty(t, res.Results)
	var retry string
	for _, h := range res.Results {
		if strings.HasSuffix(h.QN, "::RetryCharge") {
			retry = h.QN
		}
	}
	require.NotEmpty(t, retry, "the function is found by what it does: %+v", res.Results)

	n, err := g.Node(ctx, retry)
	require.NoError(t, err)
	assert.Equal(t, "go", n.Language)
	assert.Contains(t, n.Content, "RetryCharge")
	assert.NotEmpty(t, n.Calls, "RetryCharge calls Charge")
	assert.NotEmpty(t, n.CalledBy, "Checkout calls RetryCharge")

	imp, err := g.Impact(ctx, []string{retry}, 2, 50)
	require.NoError(t, err)
	var reached []string
	for _, x := range imp.Impacted {
		reached = append(reached, x.QN)
	}
	assert.Contains(t, strings.Join(reached, " "), "Checkout")

	risk, err := g.Risk(ctx, retry, 0, 0)
	require.NoError(t, err)
	assert.Contains(t, []string{"LOW", "MEDIUM", "HIGH"}, risk.Level)
	assert.GreaterOrEqual(t, risk.DirectCallers, 1)

	_, err = g.Node(ctx, "shop//nope.go::Missing")
	assert.True(t, xray.IsNotFound(err))
}

func TestSearchGraphDocs_FindsTheDecision(t *testing.T) {
	g, _ := build(t)
	res, err := g.SearchGraphDocs(context.Background(), "why do we retry the card charge", xray.Limit(3))
	require.NoError(t, err)
	require.NotEmpty(t, res.Results)
	assert.Contains(t, res.Results[0].QN, "adr-7.md")
}

func TestUpdateGraph_PicksUpAnEdit(t *testing.T) {
	ctx := context.Background()
	g, dir := build(t)
	before, err := g.SearchGraph(ctx, "refund a payment", xray.Limit(5))
	require.NoError(t, err)
	for _, h := range before.Results {
		assert.NotContains(t, h.QN, "RefundCharge")
	}

	p := filepath.Join(dir, "pay", "refund.go")
	require.NoError(t, os.WriteFile(p, []byte("package pay\n\n// RefundCharge refunds a payment to the card.\nfunc RefundCharge(id string) error { return nil }\n"), 0o644))
	up, err := g.UpdateGraph(ctx, p, "/somewhere/else/x.go")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, up.Changed, 1)
	assert.Equal(t, []string{"/somewhere/else/x.go"}, up.Unowned, "a path outside the repository is reported and never read")

	after, err := g.SearchGraph(ctx, "refund a payment", xray.Limit(5))
	require.NoError(t, err)
	var found bool
	for _, h := range after.Results {
		found = found || strings.HasSuffix(h.QN, "::RefundCharge")
	}
	assert.True(t, found, "%+v", after.Results)
}

func TestSaveLoad_RoundTripWithoutReparsing(t *testing.T) {
	ctx := context.Background()
	g, _ := build(t)
	snap := filepath.Join(t.TempDir(), "out", "shop.graph.gob.gz")
	require.NoError(t, g.Save(ctx, snap))

	loaded, err := codegraph.Load(ctx, snap, embeddings())
	require.NoError(t, err)
	res, err := loaded.SearchGraph(ctx, "retry a failed card charge", xray.Limit(3))
	require.NoError(t, err)
	require.NotEmpty(t, res.Results)

	wrong := codegraph.CustomEmbeddings(bagEmbedder{}, "bag", 64, 8192)
	_, err = codegraph.Load(ctx, snap, wrong)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "built with", "a snapshot from another embedding space is refused")
}

// The window is part of the fingerprint: a snapshot built without it is
// refused by embeddings that set one, and loads with embeddings that set the same.
func TestLoad_TheTokenWindowIsPartOfTheFingerprint(t *testing.T) {
	ctx := context.Background()
	g, _ := build(t)
	snap := filepath.Join(t.TempDir(), "shop.graph.gob.gz")
	require.NoError(t, g.Save(ctx, snap))

	windowed := embeddings()
	windowed.SetMaxInputTokens(512)
	_, err := codegraph.Load(ctx, snap, windowed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxInputTokens:512", "a snapshot built without the window is refused")

	wg, err := codegraph.Build(ctx, windowed, codegraph.Repo{Label: "shop", Path: writeRepo(t)})
	require.NoError(t, err)
	wsnap := filepath.Join(t.TempDir(), "shop.graph.gob.gz")
	require.NoError(t, wg.Save(ctx, wsnap))
	_, err = codegraph.Load(ctx, wsnap, windowed)
	require.NoError(t, err, "a snapshot built with the window loads with the same window")
}
