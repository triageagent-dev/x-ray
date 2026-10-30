//go:build cgo

package engine

// Live-parse Indexer paths: building a graph straight from repo source on disk
// and updating it incrementally. These call the Tree-sitter parser (via the
// parse helpers in parse.go), so they require CGO and are excluded from the
// CGO-free static build, which loads pre-built snapshots instead
// (NewIndexerFromSnapshot in indexer.go).

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/graph"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/parser"
)

// NewIndexer parses each repo fully and builds the initial graph + embeddings.
// followSymlinks is passed straight through to the walk (see FilepathWalk) and
// remembered on the Indexer so a later Reindex uses the same setting.
func NewIndexer(ctx context.Context, repoSpecs []string, embedder index.Embedder, logger zerolog.Logger, followSymlinks bool) (*Indexer, error) {
	return NewIndexerSeeded(ctx, repoSpecs, embedder, logger, followSymlinks, nil, false)
}

// NewIndexerSeeded is NewIndexer with the embedding cache pre-loaded from seed
// (a previous snapshot's Body.Cache, built under the same fingerprint), so only
// nodes whose text is new are embedded. The repo is still parsed in full: the
// seed saves the embed pass, never the facts, so a rebuild picks up every
// extractor change. Vectors are keyed by node text, not identity, which is what
// makes a vector from an older build valid for an unchanged node in a new one.
// Unused seed entries are pruned before anything can be written.
//
// noEmbed is the `snapshot -no-embed` case: the repo is still parsed and
// linked in full, but the embedder is never called for any node, seeded or
// not (callers pass a nil/empty seed alongside it — see runSnapshot). Kept as
// its own parameter rather than inferred from seed being empty, since an
// ordinary cold build (no previous snapshot to seed from) also has an empty
// seed and must still embed normally.
//
// Why: `snapshot` rebuilt a 35k-node repository in 18 minutes, nearly
// all of it re-embedding text the snapshot it replaced already held.
func NewIndexerSeeded(ctx context.Context, repoSpecs []string, embedder index.Embedder, logger zerolog.Logger, followSymlinks bool, seed map[string][]float32, noEmbed bool) (*Indexer, error) {
	ixr := &Indexer{logger: logger, followSymlinks: followSymlinks}
	p := parser.New()
	defer p.Close()

	for _, spec := range repoSpecs {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		label, path := SplitRepoSpec(spec)
		abs, _ := filepath.Abs(path)
		rs := &repoState{label: label, path: abs, files: map[string]parser.Result{}}

		t0 := time.Now()
		nodes, edges, files, langs, err := parseRepoInto(p, abs, followSymlinks, rs)
		if err != nil {
			return nil, fmt.Errorf("parse repo %s: %w", path, err)
		}
		if files == 0 {
			// Nothing parseable is never what a build was asked for: an empty
			// graph would be embedded, written and served as if it were the repo.
			return nil, fmt.Errorf("parse repo %s: no parseable files under %s (nothing to index)", label, abs)
		}
		logger.Info().
			Str("repo", label).Str("path", abs).
			Int("files", files).Int("nodes", len(nodes)).Int("edges", len(edges)).
			Str("langs", joinCounts(langs)).Dur("took", time.Since(t0)).
			Msg("parsed repo")
		ixr.repos = append(ixr.repos, rs)
	}

	ixr.g = graph.New()
	ixr.buildGraphLocked()

	ixr.ix = index.New(ixr.g, embedder, logger)
	if noEmbed {
		ixr.ix.SetNoEmbed(true)
	}
	if len(seed) > 0 {
		ixr.ix.LoadCache(seed)
	}
	t0 := time.Now()
	if err := ixr.ix.Build(ctx); err != nil {
		return nil, fmt.Errorf("build index: %w", err)
	}
	if len(seed) > 0 {
		logger.Info().Int("seeded", len(seed)).Int("pruned", ixr.ix.PruneCache()).Msg("embedding cache seeded")
	}
	st := ixr.g.Stats()
	logger.Info().
		Int("nodes", st.Nodes).Int("edges", st.Edges).Int("embedded", st.Embedded).Bool("noEmbed", noEmbed).
		Dur("took", time.Since(t0)).Msg("recall index ready")
	ixr.lastBuilt = time.Now()
	return ixr, nil
}

// Reindex re-parses every repo from disk and rebuilds fully.
func (ixr *Indexer) Reindex(ctx context.Context) error {
	ixr.mu.Lock()
	defer ixr.mu.Unlock()
	p := parser.New()
	defer p.Close()
	for _, rs := range ixr.repos {
		rs.files = map[string]parser.Result{}
		if _, _, _, _, err := parseRepoInto(p, rs.path, ixr.followSymlinks, rs); err != nil {
			return err
		}
	}
	ixr.buildGraphLocked()
	if err := ixr.ix.Build(ctx); err != nil {
		return err
	}
	ixr.lastBuilt = time.Now()
	return nil
}

// parseRepoInto walks a repo and records each file's parsed Result into rs.files,
// also returning aggregate counts (for logging).
func parseRepoInto(p *parser.Parser, repoAbs string, followSymlinks bool, rs *repoState) (nodes []parser.NodeInfo, edges []parser.EdgeInfo, files int, langs map[string]int, err error) {
	langs = map[string]int{}
	err = FilepathWalk(repoAbs, followSymlinks, func(rel string, src []byte) {
		res := ParseOneFile(p, rel, src)
		if len(res.Nodes) == 0 {
			return
		}
		rs.files[rel] = res
		files++
		for _, n := range res.Nodes {
			if n.Kind != "File" {
				langs[n.Language]++
			}
		}
		nodes = append(nodes, res.Nodes...)
		edges = append(edges, res.Edges...)
	})
	return nodes, edges, files, langs, err
}
