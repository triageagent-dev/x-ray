//go:build cgo

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/engine"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
)

// summaryFlag collects repeated `-summary label=text` occurrences into a
// label -> text map. One flag occurrence per label, so an operator-written
// summary can contain commas, quotes, whatever prose needs — unlike -repo's
// single comma-joined value, splitting a summary's own text on commas would
// mangle it. Consistent with -repo's "label=" grammar, just repeatable instead
// of joined.
type summaryFlag map[string]string

func (s summaryFlag) String() string {
	if len(s) == 0 {
		return ""
	}
	labels := make([]string, 0, len(s))
	for l := range s {
		labels = append(labels, l)
	}
	sort.Strings(labels)
	return strings.Join(labels, ",")
}

func (s summaryFlag) Set(v string) error {
	label, text, ok := strings.Cut(v, "=")
	if !ok || label == "" || text == "" {
		return fmt.Errorf(`-summary must be "label=text", got %q`, v)
	}
	s[label] = text
	return nil
}

// runSnapshot builds one offline graph snapshot per repo (the one and only embed
// pass) and writes each as <label>.graph.gob.gz. These snapshots are what the
// CGO-free static build and the read-only serve-snapshots server load
// at startup — parse-free and embed-free.
func runSnapshot(argv []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	repos := fs.String("repo", ".", "comma-separated repo paths (label=path or path); each becomes one snapshot")
	snapDir := fs.String("snapshot-dir", ".xray-cache", "directory to write <label>.graph.gob.gz snapshots into")
	endpoint := fs.String("embedding-endpoint", "http://localhost:8090", "OpenAI-compatible /v1/embeddings endpoint (llama.cpp)")
	model := fs.String("embedding-model", "nomic", "embedding model name")
	dims := fs.Int("embedding-dims", 256, "embedding output dimensions (Matryoshka truncation)")
	maxInputChars := fs.Int("embedding-max-input-chars", 8192, "hard char cap on a node's text before embedding")
	maxInputTokens := fs.Int("embedding-max-input-tokens", 0, "the model's input window in tokens (0 = unknown); an input over it is cut to the longest prefix that fits, counted by the endpoint's own tokenizer (llama.cpp POST /tokenize). nomic-embed-text-v2-moe reads 512.")
	followSymlinks := fs.Bool("follow-symlinks", false, "descend into symlinked directories (default off — a repo with a symlink into a large tree, or into its own parent, would otherwise quietly balloon or hang every future build)")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	noSeed := fs.Bool("no-seed", false, "embed every node, ignoring the vectors an existing <snapshot-dir>/<label>.graph.gob.gz already holds")
	noEmbed := fs.Bool("no-embed", false, "parse and link only: write the graph with zero vectors and never call the embedding endpoint (works fine with the embedder down or unset). Rules out search over the result — and routing to it by meaning, not just by label — until it is rebuilt without this flag; `services`/`dump`/`impact`, which read the graph alone, are unaffected")
	keyPath := snapshotKeyFlag(fs)
	recipient := snapshotRecipientFlag(fs)
	summaries := summaryFlag{}
	fs.Var(&summaries, "summary", `explicit router description for one map, "label=what questions this map answers" (repeatable — one -summary per label when building several with -repo a=...,b=...). Overrides the README/stats fallback outright for that label; a label with no -summary keeps falling back to its README, then a stats-derived line. Write it as what questions the map is about, not a file/symbol count — that count-only text is what made routing indistinguishable across docs-only maps before this flag existed.`)
	_ = fs.Parse(argv)

	logger := newLogger(*logLevel)
	ctx := context.Background()

	keys, err := loadSnapshotKeys(*keyPath, *recipient)
	if err != nil {
		return err
	}
	embedder, err := index.NewEmbedder(ctx, *endpoint, *model, *dims, *maxInputChars, *maxInputTokens, logger)
	if err != nil {
		return fmt.Errorf("embedder: %w", err)
	}
	fp := engine.EmbedFingerprint{Model: *model, Dims: *dims, MaxInputChars: *maxInputChars, MaxInputTokens: *maxInputTokens}

	for _, spec := range strings.Split(*repos, ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		label, _ := engine.SplitRepoSpec(spec)
		out := filepath.Join(*snapDir, label+".graph.gob.gz")
		var seed map[string][]float32
		if !*noSeed && !*noEmbed {
			seed = snapshotSeed(out, fp, keys, logger)
		}
		// Build one project's full graph (parse + link always runs); with
		// -no-embed the embed pass itself — and the seed that would feed it —
		// is skipped outright, so the written snapshot carries zero vectors and
		// the embedding endpoint is never called.
		ixr, err := engine.NewIndexerSeeded(ctx, []string{spec}, embedder, logger, *followSymlinks, seed, *noEmbed)
		if err != nil {
			return fmt.Errorf("index %q: %w", spec, err)
		}
		if s := summaries[label]; s != "" {
			ixr.SetExplicitSummary(s)
		}

		snap, err := ixr.SnapshotFor(ctx, label, fp, embedder)
		if err != nil {
			return fmt.Errorf("snapshot %q: %w", label, err)
		}

		if err := engine.WriteSnapshotWith(out, snap, keys); err != nil {
			return fmt.Errorf("write %q: %w", out, err)
		}
		st := ixr.Graph().Stats()
		logger.Info().
			Str("repo", label).Str("path", out).Bool("encrypted", keys.Encrypts()).
			Int("nodes", st.Nodes).Int("edges", st.Edges).Int("embedded", st.Embedded).
			Bool("noEmbed", *noEmbed).
			Int("summaryChars", len(snap.Header.Summary)).
			Str("summarySource", snap.Header.SummarySource).
			Msg("wrote snapshot")
	}
	return nil
}

// snapshotSeed returns the embedding cache of the snapshot a build is about to
// replace, or nil when there is none to trust: no file, unreadable (an
// encrypted one without -snapshot-key), or embedded under another fingerprint —
// vectors from another model or size are in a different space. A seed only
// saves embedder calls, so every failure falls back to a full embed.
func snapshotSeed(path string, fp engine.EmbedFingerprint, keys engine.SnapshotKeys, logger zerolog.Logger) map[string][]float32 {
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	snap, err := engine.ReadSnapshotWith(path, keys)
	if err != nil {
		logger.Warn().Err(err).Str("path", path).Msg("seed skipped: embedding every node (previous snapshot unreadable)")
		return nil
	}
	if snap.Header.Fingerprint != fp {
		logger.Warn().Interface("snapshot", snap.Header.Fingerprint).Interface("build", fp).Str("path", path).
			Msg("seed skipped: embedding every node (embedding fingerprint differs)")
		return nil
	}
	logger.Info().Str("path", path).Int("vectors", len(snap.Body.Cache)).Msg("seeding embeddings from the previous snapshot")
	return snap.Body.Cache
}
