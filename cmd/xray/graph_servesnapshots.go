package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/engine"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/httpguard"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/hub"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/mcpserver"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/ui"
	buildinfo "github.com/triageagent-dev/x-ray/v2/internal/version"
)

// runServeSnapshots is the read-only Mode B server: it serves the
// recall MCP read tools over per-project graphs loaded from offline snapshots,
// with semantic routing and idle-TTL eviction. It NEVER embeds nodes — snapshots
// carry their vectors — so the embedder is used only to embed queries (search
// and routing). Repos are baked into snapshots by `xray graph snapshot`; there is
// no -repo flag and no update/reindex control plane (snapshots are immutable).
func runServeSnapshots(argv []string) error {
	fs := flag.NewFlagSet("serve-snapshots", flag.ExitOnError)
	snapDir := fs.String("snapshot-dir", "", "directory of <label>.graph.gob.gz snapshots to serve (required)")
	addr := fs.String("addr", "127.0.0.1:8085", "MCP HTTP listen address; loopback by default — give a host (\":8085\" for every interface, as the container does) to expose it, and set -auth-token-file when you do")
	graphTTLMin := fs.Int("graph-ttl-min", 0, "evict a project's in-memory graph after this many idle MINUTES (0 = never; reloads from snapshot on next use)")
	fanout := fs.Int("route-fanout", 1, "number of projects a semantic (no explicit project) search consults")
	writable := fs.Bool("writable", false, "treat -snapshot-dir as READ-WRITE: update_code_graph edits the worked copy and flushes back to the .gob.gz on eviction/shutdown. Default false (read-only): the dir is assumed immutable (e.g. an OCI image volume), update_code_graph is refused, and no flush is attempted")
	endpoint := fs.String("embedding-endpoint", "http://localhost:8090", "OpenAI-compatible /v1/embeddings endpoint (llama.cpp) — used for QUERY embedding only")
	model := fs.String("embedding-model", "nomic", "embedding model name (must match the snapshots' fingerprint)")
	dims := fs.Int("embedding-dims", 256, "embedding output dimensions (must match the snapshots' fingerprint)")
	maxInputChars := fs.Int("embedding-max-input-chars", 8192, "hard char cap on input before embedding (must match the snapshots' fingerprint)")
	maxInputTokens := fs.Int("embedding-max-input-tokens", 0, "the model's input window in tokens (0 = unknown; must match the snapshots' fingerprint); an input over it is cut to the longest prefix that fits, counted by the endpoint's own tokenizer (llama.cpp POST /tokenize). nomic-embed-text-v2-moe reads 512.")
	skillsMap := fs.String("skills-map", "skills", "map label backing list_skills/get_skill (one Doc node per SKILL.md); empty disables both tools")
	memoryMap := fs.String("memory-map", "session", "map label that remember writes into by default; created on first use; empty disables the tool (needs -writable)")
	memoryTTL := fs.String("memory-ttl", mcpserver.FormatTTL(mcpserver.DefaultMemoryTTL), "how long a note lives when remember names no ttl: a duration (7d, 24h, 90m) or never; expired notes are swept on read and hourly")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	keyPath := snapshotKeyFlag(fs)
	recipient := snapshotRecipientFlag(fs)
	access := registerAccessFlags(fs)
	_ = fs.Parse(argv)

	if *snapDir == "" {
		return fmt.Errorf("-snapshot-dir is required (build snapshots with `xray graph snapshot`)")
	}

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

	embedder, err := index.NewEmbedder(ctx, *endpoint, *model, *dims, *maxInputChars, logger)
	if err != nil {
		return fmt.Errorf("embedder: %w", err)
	}
	embedder.SetMaxInputTokens(*maxInputTokens)
	fp := engine.EmbedFingerprint{Model: *model, Dims: *dims, MaxInputChars: *maxInputChars, MaxInputTokens: *maxInputTokens}

	noteTTL, err := mcpserver.ParseTTL(*memoryTTL)
	if err != nil {
		return fmt.Errorf("-memory-ttl: %w", err)
	}

	graphTTL := time.Duration(*graphTTLMin) * time.Minute
	reg := engine.NewRegistry(embedder, fp, *fanout, graphTTL, *writable, logger)
	reg.SetSnapshotDir(*snapDir) // where a memory map created by remember is written
	reg.SetMemoryTTL(noteTTL)
	reg.SetSnapshotKeys(keys)
	// The key reads, the recipient writes (engine/snapshotkeys.go). Say which
	// of the two this process has, because the difference decides what the
	// session notes map looks like on disk after the first remember.
	if keys.CanDecrypt() {
		logger.Info().Str("key", keys.KeyPath()).Str("identityRecipient", keys.IdentityRecipient()).
			Msg("snapshots: key loaded (reads encrypted maps; encrypted maps stay encrypted on flush; plaintext maps stay plaintext)")
	}
	if keys.Encrypts() {
		logger.Info().Str("recipient", keys.Recipient()).
			Msgf("snapshots: writes encrypt to %s (new maps and plaintext maps included)", keys.Recipient())
		if !keys.CanDecrypt() {
			logger.Warn().Msg("snapshots: recipient set without a key — this server encrypts what it writes but cannot read it back after a restart")
		}
	}
	if keys.RecipientMismatch() {
		logger.Warn().Str("recipient", keys.Recipient()).Str("identityRecipient", keys.IdentityRecipient()).
			Msg("snapshots: -snapshot-recipient differs from the key's own public key — flushes go to a key this server cannot open after a restart (fine while rotating, a lockout otherwise)")
	}
	if !keys.CanDecrypt() && !keys.Encrypts() {
		logger.Info().Msg("snapshots: no key; encrypted snapshots are skipped, flushes are plaintext")
	}

	// remember needs somewhere to persist: on a read-only dir the tool is left
	// unregistered rather than registered-and-failing.
	var memory mcpserver.Rememberer
	if *memoryMap != "" && *writable {
		memory = reg
		logger.Info().Str("map", *memoryMap).Str("defaultTTL", mcpserver.FormatTTL(noteTTL)).Msgf("memory: notes map %q, default ttl %s", *memoryMap, mcpserver.FormatTTL(noteTTL))
	} else if *memoryMap != "" {
		logger.Info().Msg("remember disabled: snapshot dir is read-only (run with -writable to enable); recall still serves the notes map if it is shipped")
	}

	matches, err := filepath.Glob(filepath.Join(*snapDir, "*.graph.gob.gz"))
	if err != nil {
		return fmt.Errorf("glob snapshots: %w", err)
	}
	if len(matches) == 0 {
		return fmt.Errorf("no *.graph.gob.gz snapshots in %s — build them with `xray graph snapshot`", *snapDir)
	}
	registered := 0
	for _, path := range matches {
		if err := reg.AddSnapshot(path); err != nil {
			// An encrypted map without the key is a configuration gap, not a
			// broken file: warn with the flag to pass and keep serving the rest.
			if errors.Is(err, engine.ErrSnapshotEncrypted) {
				logger.Warn().Err(err).Str("path", path).Msg("skip snapshot: encrypted and no matching -snapshot-key")
				continue
			}
			logger.Error().Err(err).Str("path", path).Msg("skip snapshot")
			continue
		}
		registered++
	}
	if registered == 0 {
		return fmt.Errorf("no usable snapshots in %s (all failed fingerprint/format checks)", *snapDir)
	}

	// Snapshots are registered header-only (bodies load lazily on first use), so
	// steady-state footprint is just the routing vectors until a project is hit.
	runtime.GC()
	debug.FreeOSMemory()

	reg.StartSweeper(ctx)
	// Expired notes are also swept on every load of a notes map (so no reader
	// sees one); the timer is for a map nobody reads, so its file still shrinks.
	reg.StartMemorySweeper(ctx, time.Hour)

	// Watch the snapshot dir so snapshots added/refreshed/removed at runtime (e.g.
	// a remounted OCI image-volume) are picked up without a restart.
	if err := reg.WatchDir(ctx, *snapDir); err != nil {
		logger.Warn().Err(err).Msg("snapshot dir watch disabled (added snapshots need a restart)")
	}

	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpserver.NewReadOnly(mcpserver.Deps{Resolver: reg, Logger: logger, SkillsMap: *skillsMap, Memory: memory, MemoryMap: *memoryMap, MemoryTTL: noteTTL}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, reg.Status())
	})

	// GET /version — the build version stamped into the binary (internal/version).
	// The UI reads this for its HUD; it is also the quickest way to tell which
	// build a running server is, without shelling into it.
	mux.HandleFunc("/version", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]string{"version": buildinfo.Version})
	})

	// GET /metrics — process-lifetime search-quality metrics (search-metrics.md):
	// hybrid search counters (semantic/lexical partition, keyword promotions,
	// latency), routing outcomes, per-map search/hit totals, map load/eviction
	// counts, and query-embedding call/cache-hit counts. Same numbers as
	// get_graph_stats' "_metrics" block, reachable without an MCP client.
	// Unauthenticated and read-only, matching every other endpoint here.
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, reg.Metrics())
	})

	// GET /ui (+ /ui/*.js) — the browser graph view. Multi-project: the page reads
	// a project selector (populated from /projects) and passes ?project= to /graph
	// and /node, which resolve through the registry (loading/reviving as needed).
	uiHandler := ui.Handler()
	mux.Handle("/ui", uiHandler)
	mux.Handle("/ui/", uiHandler)

	// GET /ws — live hub.Frame stream (search hits + incremental graph
	// updates) so /ui can render a "graph in use" effect instead of polling.
	mux.HandleFunc("/ws", hub.ServeWS(logger))

	// GET /projects — labels + residency for the UI's project selector.
	mux.HandleFunc("/projects", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, reg.ProjectList())
	})

	// GET /graph?project=… — the selected project's whole-graph dump for the UI.
	mux.HandleFunc("/graph", func(w http.ResponseWriter, r *http.Request) {
		res, err := reg.ResolveMap(r.Context(), r.URL.Query().Get("project"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		writeJSON(w, res.Graph.Dump())
	})

	// GET /search?q=…&kind=all|code|docs&limit=…&project=…[&attr=k=v][&order=key[:desc]] — the same hybrid
	// search (HNSW semantic channel + trigram lexical channel, fused by RRF —
	// index.Index.Search) that backs search_graph/search_graph_docs, reachable
	// over plain HTTP for the browser UI (which can't speak MCP directly outside
	// an MCP-Apps host). project omitted lets the registry route the query
	// semantically across every loaded map, exactly like /graph and /node do when
	// theirs is omitted; kind mirrors the UI's All/Docs/Code segmented control
	// onto the same codeKinds/docKinds split those MCP tools already use
	// (KindsForFilter, so the mapping can't drift out of sync with them). This
	// goes through mcpserver.SearchDirect — the identical Resolve + Index.Search
	// + RecordSearch path search_graph/search_graph_docs use — so the
	// search-quality counters on /metrics count UI searches exactly like MCP
	// ones.
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
		out, err := mcpserver.SearchDirect(r.Context(),
			mcpserver.Deps{Resolver: reg, Logger: logger},
			r.URL.Query().Get("project"), q, searchLimitParam(r),
			mcpserver.KindsForFilter(r.URL.Query().Get("kind")), index.Select{Attr: attr, Order: order}, r.URL.Query().Get("cutoff") == "gap")
		if err != nil {
			logger.Error().Err(err).Str("q", q).Msg("search failed")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(out))
	})

	// GET /node?qn=… — one node's content + relationships. The qn is repo-namespaced
	// (label//…), so it routes to the owning project by prefix.
	mux.HandleFunc("/node", func(w http.ResponseWriter, r *http.Request) {
		qn := r.URL.Query().Get("qn")
		if qn == "" {
			http.Error(w, "qn query param required", http.StatusBadRequest)
			return
		}
		res, err := reg.ResolveByQN(r.Context(), qn)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		out, ok := mcpserver.NodeJSON(res.Graph, qn)
		if !ok {
			http.Error(w, "node not found", http.StatusNotFound)
			return
		}
		writeJSON(w, out)
	})

	totalNodes, totalEdges := reg.Totals()
	logger.Info().
		Str("addr", *addr).Int("snapshots", registered).
		Int("totalNodes", totalNodes).Int("totalEdges", totalEdges).
		Int("graphTTLMin", *graphTTLMin).Int("routeFanout", *fanout).
		Bool("writable", *writable).Str("skillsMap", *skillsMap).
		Str("memoryMap", *memoryMap).Bool("remember", memory != nil).
		Msg("serving from snapshots; embedder used for queries only")

	srv := &http.Server{Addr: *addr, Handler: httpguard.Wrap(mux, accessCfg)}

	// Graceful shutdown: on SIGINT/SIGTERM, flush every dirty worked copy back to
	// its snapshot so edits made since the last eviction survive the restart.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info().Msg("shutdown: flushing dirty worked copies")
		reg.FlushAll(ctx)
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
