package codegraph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/engine"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/mcpserver"
	"github.com/triageagent-dev/x-ray/v2/pkg/xray"
)

// ErrNoParser is returned by Build, UpdateGraph and Reindex in a build without
// cgo: the Tree-sitter parser is not linked in. Load a snapshot instead, or
// build with CGO_ENABLED=1.
var ErrNoParser = errors.New("codegraph: this build has no Tree-sitter parser; build with CGO_ENABLED=1, or Load a snapshot")

// Embedder turns text into vectors. EmbedBatch is used when a graph is built,
// EmbedQuery once per search.
type Embedder = index.Embedder

// Embeddings is an Embedder together with the fingerprint a snapshot records:
// the model, the vector size and the input cap. A snapshot can only be loaded by
// a program whose embeddings carry the same fingerprint, because its vectors
// are meaningless in another space.
type Embeddings struct {
	Embedder
	fp engine.EmbedFingerprint
}

// NewEmbeddings embeds through an OpenAI-compatible /v1/embeddings endpoint
// (llama.cpp, Ollama, vLLM, LM Studio or a hosted one). dims is the vector size
// to truncate to (256 suits nomic) and maxInputChars caps the text per node
// (8192 fits a 2048-token model).
func NewEmbeddings(ctx context.Context, endpoint, model string, dims, maxInputChars int) (*Embeddings, error) {
	if dims <= 0 {
		dims = 256
	}
	if maxInputChars <= 0 {
		maxInputChars = 8192
	}
	e, err := index.NewEmbedder(ctx, endpoint, model, dims, maxInputChars, zerolog.Nop())
	if err != nil {
		return nil, fmt.Errorf("codegraph: embedder: %w", err)
	}
	return &Embeddings{Embedder: e, fp: engine.EmbedFingerprint{Model: model, Dims: dims, MaxInputChars: maxInputChars}}, nil
}

// SetMaxInputTokens sets the model's input window in tokens (512 for
// nomic-embed-text-v2-moe) and records it in the fingerprint, so a snapshot
// built without the same window is refused on Load. With NewEmbeddings, a node
// over the window is cut to the longest prefix that fits, counted by the
// endpoint's own tokenizer (llama.cpp POST /tokenize); an endpoint without one
// gets the text as maxInputChars caps it. A custom Embedder gets the window
// when it has a SetMaxInputTokens(int) method; otherwise the window is only
// recorded, as model and dims are. n <= 0 is a no-op. Set it before Build or
// Load.
func (e *Embeddings) SetMaxInputTokens(n int) {
	if n <= 0 {
		return
	}
	if s, ok := e.Embedder.(interface{ SetMaxInputTokens(int) }); ok {
		s.SetMaxInputTokens(n)
	}
	e.fp.MaxInputTokens = n
}

// CustomEmbeddings wraps your own Embedder. model, dims and maxInputChars are
// recorded in snapshots and checked on load; dims must be the length of every
// vector the embedder returns.
func CustomEmbeddings(e Embedder, model string, dims, maxInputChars int) *Embeddings {
	return &Embeddings{Embedder: e, fp: engine.EmbedFingerprint{Model: model, Dims: dims, MaxInputChars: maxInputChars}}
}

// Repo is a source tree to index. Label names it in the graph: qualified names
// read label//path::Symbol.
type Repo struct {
	Label string
	Path  string
}

// Graph is an in-process code graph. It is safe for concurrent use.
type Graph struct {
	ixr *engine.Indexer
	emb *Embeddings
}

var _ xray.CodeGraph = (*Graph)(nil)

// Load reads a snapshot written by Save or by "xray graph snapshot" and returns
// a graph over it. Nothing is parsed or embedded: the snapshot carries the
// graph, the vectors and the built HNSW index, so loading is fast and works in a
// build without cgo. The embeddings must match the snapshot's fingerprint.
func Load(ctx context.Context, path string, emb *Embeddings, opts ...LoadOption) (*Graph, error) {
	var cfg loadConfig
	for _, o := range opts {
		o(&cfg)
	}
	keys := engine.SnapshotKeys{}
	if cfg.keyFile != "" {
		k, err := engine.LoadSnapshotKeys(cfg.keyFile, "")
		if err != nil {
			return nil, fmt.Errorf("codegraph: snapshot key: %w", err)
		}
		keys = k
	}
	snap, err := engine.ReadSnapshotWith(path, keys)
	if err != nil {
		return nil, fmt.Errorf("codegraph: read %s: %w", path, err)
	}
	if snap.Header.Fingerprint != emb.fp {
		return nil, fmt.Errorf("codegraph: %s was built with %+v, these embeddings are %+v", path, snap.Header.Fingerprint, emb.fp)
	}
	ixr, err := engine.NewIndexerFromSnapshot(ctx, snap, emb.Embedder, zerolog.Nop())
	if err != nil {
		return nil, fmt.Errorf("codegraph: %w", err)
	}
	return &Graph{ixr: ixr, emb: emb}, nil
}

// LoadOption configures Load.
type LoadOption func(*loadConfig)

type loadConfig struct{ keyFile string }

// WithSnapshotKey opens an age-encrypted snapshot with the identity in file
// (made by "xray graph keygen").
func WithSnapshotKey(file string) LoadOption { return func(c *loadConfig) { c.keyFile = file } }

// Save writes the graph, with its vectors and HNSW index, to a snapshot file
// that Load and "xray graph serve-snapshots" read. A
// snapshot holds one repository, so Save needs a graph built from one; build
// one Graph per repository to snapshot several.
func (g *Graph) Save(ctx context.Context, path string) error {
	labels := g.ixr.Labels()
	if len(labels) != 1 {
		return fmt.Errorf("codegraph: Save writes one repository, this graph has %d", len(labels))
	}
	snap, err := g.ixr.SnapshotFor(ctx, labels[0], g.emb.fp, g.emb.Embedder)
	if err != nil {
		return fmt.Errorf("codegraph: snapshot: %w", err)
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("codegraph: %w", err)
		}
	}
	return engine.WriteSnapshot(path, snap)
}

// Labels lists the repositories in the graph.
func (g *Graph) Labels() []string { return g.ixr.Labels() }

func (g *Graph) deps() mcpserver.Deps {
	return mcpserver.Deps{Graph: g.ixr.Graph(), Index: g.ixr.Index(), Updater: g.ixr, Logger: zerolog.Nop()}
}

// Handler serves the graph over MCP (streamable HTTP, at /mcp) and returns it
// as an http.Handler, so the program can offer its own graph to other agents:
// mux.Handle("/mcp", g.Handler()). The handler has no authentication; put it
// behind your own.
func (g *Graph) Handler() http.Handler { return mcpserver.New(g.deps()) }

func (g *Graph) search(ctx context.Context, docs bool, query string, opts []xray.GraphOption) (*xray.SearchResult, error) {
	q := xray.ResolveGraph(opts...)
	kinds := mcpserver.KindsForFilter("code")
	if docs {
		kinds = mcpserver.KindsForFilter("docs")
	}
	var sel index.Select
	if q.Kind != "" {
		sel.Kinds = map[string]bool{}
		for _, k := range strings.Split(q.Kind, ",") {
			sel.Kinds[strings.TrimSpace(k)] = true
		}
		kinds = nil
	}
	if q.Attr != "" {
		a, err := index.ParseAttrFilter(q.Attr)
		if err != nil {
			return nil, fmt.Errorf("codegraph: %w", err)
		}
		sel.Attr = a
	}
	if q.Order != "" {
		o, err := index.ParseOrder(q.Order)
		if err != nil {
			return nil, fmt.Errorf("codegraph: %w", err)
		}
		sel.Order = o
	}
	raw, err := mcpserver.SearchDirectIndex(ctx, g.ixr.Index(), query, q.Map, q.Limit, kinds, sel, q.Cutoff)
	if err != nil {
		return nil, fmt.Errorf("codegraph: search: %w", err)
	}
	var out xray.SearchResult
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("codegraph: decode search: %w", err)
	}
	return &out, nil
}

// SearchGraph finds code and manifests by meaning. OnMap names a repository
// label; without it every repository in the graph is searched.
func (g *Graph) SearchGraph(ctx context.Context, query string, opts ...xray.GraphOption) (*xray.SearchResult, error) {
	return g.search(ctx, false, query, opts)
}

// SearchGraphDocs finds documentation prose by meaning: ADRs, READMEs, todos.
func (g *Graph) SearchGraphDocs(ctx context.Context, query string, opts ...xray.GraphOption) (*xray.SearchResult, error) {
	return g.search(ctx, true, query, opts)
}

// Node returns the full content of the node qn and its relations.
func (g *Graph) Node(_ context.Context, qn string) (*xray.Node, error) {
	m, ok := mcpserver.NodeJSON(g.ixr.Graph(), qn)
	if !ok {
		return nil, &xray.ToolError{Tool: "get_graph_node", Message: "not found"}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out xray.Node
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("codegraph: decode node: %w", err)
	}
	return &out, nil
}

// Impact returns what a change to the seed symbols reaches over calls and
// imports. depth and maxNodes of 0 use 2 and 200.
func (g *Graph) Impact(_ context.Context, seeds []string, depth, maxNodes int) (*xray.Impact, error) {
	raw, err := mcpserver.ImpactDirectGraph(g.ixr.Graph(), seeds, depth, maxNodes)
	if err != nil {
		return nil, fmt.Errorf("codegraph: %w", err)
	}
	var out xray.Impact
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("codegraph: decode impact: %w", err)
	}
	return &out, nil
}

// Risk scores the change risk of the symbol qn as LOW, MEDIUM or HIGH.
func (g *Graph) Risk(_ context.Context, qn string, depth, maxNodes int) (*xray.Risk, error) {
	if _, ok := g.ixr.Graph().Get(qn); !ok {
		return nil, &xray.ToolError{Tool: "get_graph_risk", Message: "not found"}
	}
	if depth <= 0 {
		depth = 2
	}
	if maxNodes <= 0 {
		maxNodes = 200
	}
	r := g.ixr.Graph().Risk(qn, depth, maxNodes)
	return &xray.Risk{QN: r.QN, Level: r.Level, DirectCallers: r.DirectCallers, ModuleSpread: r.ModuleSpread, ImpactedCount: r.ImpactedCount, Modules: r.Modules}, nil
}

// GraphStats reports node and edge counts, languages and index state as JSON.
func (g *Graph) GraphStats(context.Context, ...xray.GraphOption) (json.RawMessage, error) {
	return json.Marshal(g.ixr.Status())
}

// UpdateGraph re-indexes changed files. Only nodes whose text changed reach the
// embedder. A path outside every indexed repository comes back in Unowned and is
// never read. Needs a build with cgo for source files.
func (g *Graph) UpdateGraph(ctx context.Context, paths ...string) (*xray.UpdateResult, error) {
	if !parserAvailable {
		return nil, ErrNoParser
	}
	changed, removed, skipped, unowned, err := g.ixr.Update(ctx, paths)
	if err != nil {
		return nil, fmt.Errorf("codegraph: update: %w", err)
	}
	return &xray.UpdateResult{Changed: changed, Removed: removed, Skipped: skipped, Unowned: unowned}, nil
}
