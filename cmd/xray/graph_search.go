package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/engine"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/mcpserver"
)

func runSearch(argv []string) error {
	return runSearchTo(context.Background(), os.Stdout, os.Stderr, argv)
}

// runSearchTo is search_graph without a server: it registers the snapshots in
// -snapshot-dir on a read-only engine.Registry and asks it through
// mcpserver.SearchDirect, the path serve-snapshots' /search and the MCP tools
// take, so the ranked pointers and the routing are theirs. The embedder is still
// needed — a query is embedded before it can be ranked or routed — but nothing
// else is: no listener, no process to keep running. The registry is read-only,
// so a search never writes back into the directory it reads.
//
// -server keeps the old path, GET /search on a running process, for a server
// whose maps are not on this machine.
func runSearchTo(ctx context.Context, stdout, stderr io.Writer, argv []string) error {
	fs := flag.NewFlagSet("search", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var so searchOpts
	fs.StringVar(&so.kind, "kind", "all", "all|code|docs — code is search_graph, docs is search_graph_docs")
	fs.IntVar(&so.limit, "limit", 0, "max hits (0 = default 5; capped at 20, or 100 with -attr)")
	fs.StringVar(&so.project, "project", "", "map label; empty routes by meaning across every map in -snapshot-dir")
	fs.StringVar(&so.attr, "attr", "", "attribute filter k=v")
	fs.StringVar(&so.order, "order", "", "sort a selection by attribute: key[:desc]")
	fs.BoolVar(&so.gap, "gap", false, "cut the list at the first large score gap")
	dir := fs.String("snapshot-dir", defaultSnapshotDir(), "directory of <label>.graph.gob.gz snapshots. Default $"+envSnapshotDir+", else ~/.xray-maps")
	fanout := fs.Int("route-fanout", 1, "maps a search with no -project consults")
	endpoint := fs.String("embedding-endpoint", "http://localhost:8090", "OpenAI-compatible /v1/embeddings endpoint, for the query only")
	model := fs.String("embedding-model", "nomic", "embedding model name (must match the snapshots' fingerprint)")
	dims := fs.Int("embedding-dims", 256, "embedding output dimensions (must match the snapshots' fingerprint)")
	maxInputChars := fs.Int("embedding-max-input-chars", 8192, "hard char cap on input before embedding (must match the snapshots' fingerprint)")
	maxInputTokens := fs.Int("embedding-max-input-tokens", 0, "the model's input window in tokens (0 = unknown; must match the snapshots' fingerprint); an input over it is cut to the longest prefix that fits, counted by the endpoint's own tokenizer (llama.cpp POST /tokenize). nomic-embed-text-v2-moe reads 512.")
	logLevel := fs.String("log-level", "warn", "debug|info|warn|error, to stderr")
	server := fs.String("server", "", "ask a running serve/serve-snapshots at this URL instead of reading snapshots")
	keyPath := snapshotKeyFlag(fs)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: xray graph search [flags] <query>")
		fs.PrintDefaults()
	}
	if err := fs.Parse(argv); err != nil {
		return err
	}
	// Words after the flags are the query, so it needs no quoting.
	q := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if q == "" {
		fs.Usage()
		return fmt.Errorf("give a query")
	}
	if *server != "" {
		return clientGet(searchURL(*server, q, so))
	}

	attr, err := index.ParseAttrFilter(so.attr)
	if err != nil {
		return err
	}
	order, err := index.ParseOrder(so.order)
	if err != nil {
		return err
	}
	keys, err := loadSnapshotKeys(*keyPath, "")
	if err != nil {
		return err
	}
	logger := newLogger(*logLevel)
	emb, err := index.NewEmbedder(ctx, *endpoint, *model, *dims, *maxInputChars, logger)
	if err != nil {
		return fmt.Errorf("embedder: %w", err)
	}
	emb.SetMaxInputTokens(*maxInputTokens)
	fp := engine.EmbedFingerprint{Model: *model, Dims: *dims, MaxInputChars: *maxInputChars, MaxInputTokens: *maxInputTokens}
	return searchSnapshots(ctx, stdout, emb, fp, keys, *dir, *fanout, q, so, index.Select{Attr: attr, Order: order}, logger)
}

// searchSnapshots is the part of runSearchTo below the flags, with the embedder
// injected so a test runs it against a stub.
func searchSnapshots(ctx context.Context, stdout io.Writer, emb index.Embedder, fp engine.EmbedFingerprint, keys engine.SnapshotKeys,
	dir string, fanout int, q string, so searchOpts, sel index.Select, logger zerolog.Logger) error {
	reg := engine.NewRegistry(emb, fp, fanout, 0, false, logger)
	reg.SetSnapshotKeys(keys)

	// A named map needs only its own file; routing needs every header.
	var paths []string
	if so.project != "" {
		p, err := snapshotPath(dir, so.project)
		if err != nil {
			return err
		}
		paths = []string{p}
	} else {
		var err error
		if paths, err = filepath.Glob(filepath.Join(dir, "*.graph.gob.gz")); err != nil {
			return err
		}
	}
	var sealed []string
	registered := 0
	for _, p := range paths {
		if err := reg.AddSnapshot(p); err != nil {
			if errors.Is(err, engine.ErrSnapshotEncrypted) && so.project == "" {
				sealed = append(sealed, strings.TrimSuffix(filepath.Base(p), ".graph.gob.gz"))
				continue
			}
			if so.project != "" {
				return fmt.Errorf("map %q: %w", so.project, err)
			}
			logger.Warn().Err(err).Str("path", p).Msg("skip snapshot")
			continue
		}
		registered++
	}
	// One line, not one per map: a search runs often, and an encrypted map the
	// shell holds no key for is a standing condition, not news each time.
	if len(sealed) > 0 {
		sort.Strings(sealed)
		logger.Warn().Strs("maps", sealed).Msgf("not searched: encrypted, no -snapshot-key ($%s)", envSnapshotKey)
	}
	if registered == 0 {
		return fmt.Errorf("no usable *.graph.gob.gz snapshots in %s", dir)
	}

	out, err := mcpserver.SearchDirect(ctx, mcpserver.Deps{Resolver: reg, Logger: logger},
		so.project, q, so.limit, mcpserver.KindsForFilter(so.kind), sel, so.gap)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, out)
	return err
}

// snapshotPath is where map label's snapshot lives in dir. The label turns into
// a file name, so one that would leave dir is refused rather than joined.
func snapshotPath(dir, label string) (string, error) {
	if label == "" || !filepath.IsLocal(label) || strings.ContainsRune(label, filepath.Separator) {
		return "", fmt.Errorf("%q is not a map label", label)
	}
	p := filepath.Join(dir, label+".graph.gob.gz")
	if _, err := os.Stat(p); err != nil {
		return "", fmt.Errorf("no snapshot for map %q: %w", label, err)
	}
	return p, nil
}
