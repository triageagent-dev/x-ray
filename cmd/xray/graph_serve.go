//go:build cgo

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/engine"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/httpguard"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/hub"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/mcpserver"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/parser"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/ui"
)

// runServe parses one or more repositories into an in-memory code graph, embeds
// every node via the shared llama.cpp/nomic endpoint, and serves the recall
// MCP tools over HTTP. It also exposes /update, /status, /reindex so a thin CLI
// (driven by editor/agent hooks) can keep the live graph current incrementally.
func runServe(argv []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	repos := fs.String("repo", ".", "comma-separated repo paths (label=path or path); each becomes a repo namespace")
	addr := fs.String("addr", "127.0.0.1:8084", "MCP HTTP listen address; loopback by default — give a host (\":8084\" for every interface) to expose it, and set -auth-token-file when you do")
	endpoint := fs.String("embedding-endpoint", "http://localhost:8090", "OpenAI-compatible /v1/embeddings endpoint (llama.cpp)")
	model := fs.String("embedding-model", "nomic", "embedding model name")
	dims := fs.Int("embedding-dims", 256, "embedding output dimensions (Matryoshka truncation)")
	maxInputChars := fs.Int("embedding-max-input-chars", 8192, "hard cap in bytes on a node's text before embedding — set below the model's token window × ~3.5 (nomic 2048-tok → 8192; bge/MiniLM 512-tok → ~1800). A bound, not a fit: bytes per token vary with the text, so a dense input under the cap can still exceed the window and be rejected by the model; -embedding-max-input-tokens cuts it to the window")
	maxInputTokens := fs.Int("embedding-max-input-tokens", 0, "the model's input window in tokens (0 = unknown); an input over it is cut to the longest prefix that fits, counted by the endpoint's own tokenizer (llama.cpp POST /tokenize). nomic-embed-text-v2-moe reads 512.")
	bruteForce := fs.Bool("brute-force", false, "use the exact O(N·D) brute-force cosine scan instead of the HNSW ANN index (for A/B comparison; HNSW is the default)")
	snapshotDir := fs.String("snapshot-dir", "", "if set, periodically + on shutdown write each repo's live graph to <dir>/<label>.graph.gob.gz (with persisted HNSW), so the latest .gob is always on disk and pushable as an OCI snapshot image. Empty = in-memory only.")
	snapshotEvery := fs.Duration("snapshot-interval", 5*time.Minute, "how often to flush the live graph to -snapshot-dir (0 = only on shutdown)")
	noSeed := fs.Bool("no-seed", false, "ignore an existing <snapshot-dir>/<label>.graph.gob.gz and always cold-build (parse + embed every node)")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	keyPath := snapshotKeyFlag(fs)
	recipient := snapshotRecipientFlag(fs)
	access := registerAccessFlags(fs)
	_ = fs.Parse(argv)

	logger := newLogger(*logLevel)
	ctx := context.Background()

	keys, err := loadSnapshotKeys(*keyPath, *recipient)
	if err != nil {
		return err
	}
	accessCfg, err := access.config(*addr, logger)
	if err != nil {
		return err
	}
	embedder, err := index.NewEmbedder(ctx, *endpoint, *model, *dims, *maxInputChars, *maxInputTokens, logger)
	if err != nil {
		return fmt.Errorf("embedder: %w", err)
	}

	fp := engine.EmbedFingerprint{Model: *model, Dims: *dims, MaxInputChars: *maxInputChars, MaxInputTokens: *maxInputTokens}

	// Warm seed: when -snapshot-dir already holds this repo's gob, rebuild the live
	// graph from it instead of re-parsing and re-embedding every node. The gob
	// carries the text→vector cache and the serialized HNSW, so startup is a load
	// rather than a full embed pass. Falls back to a cold build whenever the gob is
	// absent, unreadable, or built under a different embedding fingerprint — seeding
	// is an optimization and must never be the reason the server refuses to start.
	specs := strings.Split(*repos, ",")
	ixr, seedEncrypted, err := seedFromSnapshot(ctx, specs, *snapshotDir, *noSeed, fp, embedder, keys, logger)
	if err != nil {
		return err
	}
	if ixr == nil {
		if ixr, err = engine.NewIndexer(ctx, specs, embedder, logger, false); err != nil {
			return err
		}
	}
	if *bruteForce {
		ixr.Index().SetBruteForce(true)
		logger.Info().Msg("search: brute-force exact scan (HNSW disabled via -brute-force)")
	}

	// Parsing allocates large transient Tree-sitter trees; once the graph +
	// embeddings are built the live working set is only a few MB. Return the
	// transient parse arena to the OS so steady-state footprint reflects the graph.
	runtime.GC()
	debug.FreeOSMemory()

	mux := http.NewServeMux()
	// Memory is left nil: the live Indexer has no Remember yet, so the tool is
	// not offered here (it is on serve-snapshots, whose registry persists maps).
	mux.Handle("/mcp", mcpserver.New(mcpserver.Deps{Graph: ixr.Graph(), Index: ixr.Index(), Updater: ixr, Logger: logger}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// GET /ui (+ /ui/*.js) — the graph app in browser/debug mode. The same page
	// renders inline as an MCP App via the show_code_graph tool.
	uiHandler := ui.Handler()
	mux.Handle("/ui", uiHandler)
	mux.Handle("/ui/", uiHandler)
	// GET /ws — live hub.Frame stream (search hits + incremental graph
	// updates) so /ui can render a "graph in use" effect instead of polling.
	mux.HandleFunc("/ws", hub.ServeWS(logger))
	registerAdminEndpoints(mux, ixr, logger)

	logger.Info().Str("addr", *addr).Msg("recall MCP server ready")

	// In-memory only unless -snapshot-dir is set. When set, flush the live graph
	// (with its persisted HNSW) to disk periodically AND on SIGINT/SIGTERM, so the
	// latest .gob.gz is always available — instant warm reload, and pushable as an
	// OCI snapshot image. The graph stays the source of truth; this just mirrors it.
	srv := &http.Server{Addr: *addr, Handler: httpguard.Wrap(mux, accessCfg)}
	if *snapshotDir == "" {
		return srv.ListenAndServe()
	}

	flush := func(reason string) {
		t0 := time.Now()
		n, err := snapshotAll(ctx, ixr, *snapshotDir, fp, embedder, keys, seedEncrypted)
		if err != nil {
			logger.Warn().Err(err).Str("reason", reason).Msg("snapshot flush failed")
			return
		}
		logger.Info().Int("repos", n).Str("dir", *snapshotDir).Str("reason", reason).Dur("took", time.Since(t0)).Msg("flushed live graph to snapshot")
	}

	// Periodic flush ticker (skip when interval is 0 — shutdown-only).
	if *snapshotEvery > 0 {
		go func() {
			t := time.NewTicker(*snapshotEvery)
			defer t.Stop()
			for range t.C {
				flush("periodic")
			}
		}()
	}

	// Serve in the background; block on a shutdown signal, then flush a final time.
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh:
		return err
	case sig := <-sigCh:
		logger.Info().Str("signal", sig.String()).Msg("shutdown: flushing live graph to snapshot")
		flush("shutdown")
		shutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return nil
	}
}

// seedFromSnapshot rebuilds the live graph from a previously flushed gob instead
// of parsing and embedding the repo from scratch. It returns (nil, nil) whenever
// seeding does not apply — no snapshot dir, -no-seed, more than one repo served,
// no gob for that label, or a gob that cannot be used — and the caller then cold-
// builds. A non-nil error is returned only for a condition the operator must see.
//
// Seeding is deliberately conservative:
//   - single repo only, matching snapshotAll's own constraint, so the flush that
//     produced the gob and the seed that consumes it stay symmetric;
//   - the embedding fingerprint must match exactly, since a gob's cached vectors
//     are meaningless under a different model/dims (same check the registry runs);
//   - the gob's recorded repo path must still be the path being served, otherwise
//     the cached file→node mapping describes a different tree and incremental
//     `update` calls would write into the wrong repo namespace.
//
// The graph is a mirror of files on disk, so a seed is only as fresh as the last
// flush; edits made while the server was down are picked up by the next `update`
// for those files, not by seeding.
//
// The second result says whether the seed file was encrypted: the flush must
// keep it that way under a key alone (engine.SnapshotKeys.WriteRecipient), so
// the flag travels from the read to every later write of the same file.
func seedFromSnapshot(ctx context.Context, specs []string, snapshotDir string, noSeed bool, fp engine.EmbedFingerprint, embedder index.Embedder, keys engine.SnapshotKeys, logger zerolog.Logger) (*engine.Indexer, bool, error) {
	if snapshotDir == "" || noSeed {
		return nil, false, nil
	}
	if len(specs) != 1 {
		logger.Info().Int("repos", len(specs)).Msg("seed skipped: cold build (snapshot seed supports one repo)")
		return nil, false, nil
	}
	label, repoPath := engine.SplitRepoSpec(strings.TrimSpace(specs[0]))
	absRepo, err := filepath.Abs(repoPath)
	if err != nil {
		return nil, false, fmt.Errorf("resolve repo path %q: %w", repoPath, err)
	}
	path := filepath.Join(snapshotDir, label+".graph.gob.gz")
	if _, statErr := os.Stat(path); statErr != nil {
		logger.Info().Str("path", path).Msg("seed skipped: cold build (no snapshot yet)")
		return nil, false, nil
	}

	t0 := time.Now()
	encrypted, err := engine.IsEncryptedSnapshot(path)
	if err != nil {
		logger.Warn().Err(err).Str("path", path).Msg("seed skipped: cold build (snapshot unreadable)")
		return nil, false, nil
	}
	snap, err := engine.ReadSnapshotWith(path, keys)
	if err != nil {
		logger.Warn().Err(err).Str("path", path).Msg("seed skipped: cold build (snapshot unreadable)")
		return nil, false, nil
	}
	if snap.Header.Fingerprint != fp {
		logger.Warn().
			Interface("snapshot", snap.Header.Fingerprint).Interface("server", fp).Str("path", path).
			Msg("seed skipped: cold build (embedding fingerprint mismatch)")
		return nil, false, nil
	}
	if snapAbs, absErr := filepath.Abs(snap.Header.RepoPath); absErr != nil || snapAbs != absRepo {
		logger.Warn().
			Str("snapshot_repo", snap.Header.RepoPath).Str("serving", absRepo).Str("path", path).
			Msg("seed skipped: cold build (snapshot describes a different repo path)")
		return nil, false, nil
	}

	if snap.Header.StaleFacts() {
		// Loading would serve the facts the older extractors produced until the
		// next cold build. Re-parse instead, and keep only the vectors: they are
		// keyed by node text, so unchanged nodes still embed nothing.
		logger.Info().Str("path", path).Int("facts", snap.Header.Facts).Int("want", parser.FactsVersion).
			Msg("snapshot predates this binary's extractors: re-parsing, reusing its embeddings")
		ixr, err := engine.NewIndexerSeeded(ctx, specs, embedder, logger, false, snap.Body.Cache, false)
		if err != nil {
			return nil, false, err
		}
		return ixr, encrypted, nil
	}
	ixr, err := engine.NewIndexerFromSnapshot(ctx, snap, embedder, logger)
	if err != nil {
		logger.Warn().Err(err).Str("path", path).Msg("seed failed: falling back to cold build")
		return nil, false, nil
	}
	st := ixr.Graph().Stats()
	logger.Info().
		Str("path", path).Str("label", label).
		Int("nodes", st.Nodes).Int("edges", st.Edges).Int("embedded", st.Embedded).
		Time("built_at", snap.Header.BuiltAt).Dur("took", time.Since(t0)).Bool("encrypted", encrypted).
		Msg("seeded live graph from snapshot (no embed pass)")
	return ixr, encrypted, nil
}

// snapshotAll writes each served repo's current in-memory graph (with its built
// HNSW) to <dir>/<label>.graph.gob.gz. Returns the number of repos written.
// SnapshotFor requires a single-repo indexer; with multiple repos served it logs
// and skips (the local-dev graph is single-repo — the common case).
//
// loadedEncrypted is what seedFromSnapshot found on disk: with a key alone an
// encrypted seed is written back encrypted, a plaintext one stays plaintext;
// a -snapshot-recipient encrypts either.
func snapshotAll(ctx context.Context, ixr *engine.Indexer, dir string, fp engine.EmbedFingerprint, embedder index.Embedder, keys engine.SnapshotKeys, loadedEncrypted bool) (int, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	labels := ixr.Labels()
	if len(labels) != 1 {
		return 0, fmt.Errorf("snapshot flush supports one repo, %d served", len(labels))
	}
	label := labels[0]
	snap, err := ixr.SnapshotFor(ctx, label, fp, embedder)
	if err != nil {
		return 0, err
	}
	path := filepath.Join(dir, label+".graph.gob.gz")
	if err := engine.WriteSnapshotFor(path, snap, keys, loadedEncrypted); err != nil {
		return 0, err
	}
	return 1, nil
}

// registerAdminEndpoints wires the incremental-update control plane that the CLI
// (and hooks) drive: POST /update {paths:[]}, GET /status, POST /reindex.
func registerAdminEndpoints(mux *http.ServeMux, ixr *engine.Indexer, logger zerolog.Logger) {
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, ixr.Status())
	})

	// GET /graph — lean whole-graph dump (titles + edges, no node bodies) for the
	// force-directed UI. Read-only.
	mux.HandleFunc("/graph", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, ixr.Graph().Dump())
	})

	// GET /node?qn=… — one node's full content + relationships, identical shape to
	// the MCP get_code_node tool (NodeJSON is the shared builder). The UI calls
	// this lazily when a graph node is clicked.
	mux.HandleFunc("/node", func(w http.ResponseWriter, r *http.Request) {
		qn := r.URL.Query().Get("qn")
		if qn == "" {
			http.Error(w, "qn query param required", http.StatusBadRequest)
			return
		}
		out, ok := mcpserver.NodeJSON(ixr.Graph(), qn)
		if !ok {
			http.Error(w, "node not found", http.StatusNotFound)
			return
		}
		writeJSON(w, out)
	})

	// GET /search?q=…&kind=all|code|docs&limit=…[&attr=k=v][&order=key[:desc]][&project=repo-label] — the same
	// hybrid search (HNSW semantic + trigram lexical, fused by RRF) that backs
	// search_graph/search_graph_docs, reachable over plain HTTP for the browser
	// UI (which can't speak MCP directly outside an MCP-Apps host). project, when
	// this server serves more than one repo (-repo a=...,b=...), narrows to one
	// repo namespace — same repoFilter Index.Search already takes; empty searches
	// every served repo. Goes through mcpserver.SearchDirectIndex, the same
	// ix.Search call the MCP tools make, so /metrics-style search counters (once
	// wired) see UI searches identically to MCP ones.
	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if q == "" {
			http.Error(w, "q query param required", http.StatusBadRequest)
			return
		}
		attr, aerr := index.ParseAttrFilter(r.URL.Query().Get("attr"))
		if aerr != nil {
			http.Error(w, aerr.Error(), http.StatusBadRequest)
			return
		}
		// order sorts a selection by an attribute (the MCP tools' order arg),
		// so the UI can answer a superlative the same way the tools do.
		order, oerr := index.ParseOrder(r.URL.Query().Get("order"))
		if oerr != nil {
			http.Error(w, oerr.Error(), http.StatusBadRequest)
			return
		}
		out, err := mcpserver.SearchDirectIndex(r.Context(), ixr.Index(), q,
			r.URL.Query().Get("project"), searchLimitParam(r),
			mcpserver.KindsForFilter(r.URL.Query().Get("kind")), index.Select{Attr: attr, Order: order}, r.URL.Query().Get("cutoff") == "gap")
		if err != nil {
			logger.Error().Err(err).Str("q", q).Msg("search failed")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(out))
	})

	mux.HandleFunc("/update", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Paths []string `json:"paths"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		t0 := time.Now()
		changed, removed, skipped, unowned, err := ixr.Update(r.Context(), body.Paths)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logger.Info().Int("changed", changed).Int("removed", removed).Int("skipped", skipped).
			Strs("unowned", unowned).Dur("took", time.Since(t0)).Msg("incremental update")
		// unowned is always present, even when empty: a client that never sees the
		// key cannot tell "all paths indexed" from "this server predates the field".
		writeJSON(w, map[string]any{"changed": changed, "removed": removed, "skipped": skipped, "unowned": unowned, "tookMs": time.Since(t0).Milliseconds()})
	})

	mux.HandleFunc("/reindex", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		t0 := time.Now()
		if err := ixr.Reindex(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logger.Info().Dur("took", time.Since(t0)).Msg("full reindex")
		writeJSON(w, map[string]any{"reindexed": true, "tookMs": time.Since(t0).Milliseconds()})
	})
}
