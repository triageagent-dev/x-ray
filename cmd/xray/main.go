package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/go-logr/zerologr"
	corev1 "k8s.io/api/core/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/scheme"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
	msengine "github.com/triageagent-dev/x-ray/v2/internal/codegraph/engine"
	msindex "github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	graphmcp "github.com/triageagent-dev/x-ray/v2/internal/codegraph/mcpserver"
	arconfig "github.com/triageagent-dev/x-ray/v2/internal/config"
	"github.com/triageagent-dev/x-ray/v2/internal/observability"
	"github.com/triageagent-dev/x-ray/v2/internal/version"
	"github.com/triageagent-dev/x-ray/v2/internal/webui"
	"github.com/triageagent-dev/x-ray/v2/internal/xray"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore/index"
)

func init() {
	// Set controller-runtime logger before any k8s machinery starts — prevents
	// "log.SetLogger was never called" warning from the reflector goroutines.
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	logf.SetLogger(zerologr.New(&log.Logger))
}

func main() {
	// Mode dispatch. `xray graph <sub>` runs the code graph (parse,
	// snapshot, serve retrieval — see graph.go); bare `xray` runs the server:
	// API, controller, voice and MCP. One binary, two modes.
	if len(os.Args) > 1 && os.Args[1] == "graph" {
		runGraph(os.Args[2:])
		return
	}

	var (
		httpAddr                  string
		mcpAddr                   string
		metricsAddr               string
		mcpTokenSecret            string
		logLevel                  string
		namespace                 string
		embedEndpoint             string
		embedModel                string
		embedDims                 int
		embedMaxInputChars        int
		semanticScoreGap          float64
		searchLexicalShortCircuit bool
		searchRecallShaping       bool
		memoryWriteDedup          bool
		memMapsDir                string
		webUI                     bool
		memMapsKeyFile            string
		memMapsRouteFanout        int
		memMapsTTLMin             int
		gcpProject                string
		gcpLocation               string
		voiceLocation             string
		geminiAPIKey              string
		voiceModel                string
		voiceName                 string
		userDailyAudioTokenLimit  int64
		voiceMaxOutputTokens      int
		headroomLimit             int
		affectiveDialog           bool
		proactiveAudio            bool
		maxConcurrentVoice        int
		maxRequestBytes           int64
		mcpListMemoryMaxLimit     int
		mcpSearchMemoryMaxLimit   int
		mcpRecentMemoryMaxLimit   int
		otelEndpoint              string
		otelTokenSecret           string
		registryPullSecret        string
		otelProject               string
		githubClientID            string
		githubClientSecret        string
		githubOrg                 string
		githubTeam                string
		publicURL                 string
		uiCallbackOrigins         string
		azureClientID             string
		azureClientSecret         string
		azureTenantID             string
		googleClientID            string
		googleClientSecret        string
		googleDomain              string
		dev                       bool
		allowAnyUser              bool
		mcpOAuth                  bool
		mcpResourceURL            string
		mcpTokenTTL               time.Duration
		episodicTTL               time.Duration
		retentionSweepInterval    time.Duration
		trustedProxyHops          int
	)

	flag.StringVar(&httpAddr, "http-address", ":8080", "HTTP server listen address")
	flag.StringVar(&mcpAddr, "mcp-address", ":8083", "MCP server listen address (empty to disable)")
	flag.StringVar(&metricsAddr, "metrics-address", ":9090", "Prometheus /metrics listen address (empty to disable)")
	flag.StringVar(&mcpTokenSecret, "mcp-token-secret", "", "k8s Secret name containing MCP Bearer token (key: token)")
	flag.StringVar(&logLevel, "log-level", "info", "Log level (trace, debug, info, warn, error)")
	flag.StringVar(&namespace, "namespace", "xray", "Kubernetes namespace")
	flag.StringVar(&embedEndpoint, "embedding-endpoint", "", "OpenAI-compatible embedding endpoint (enables HNSW semantic memory)")
	flag.StringVar(&embedModel, "embedding-model", "gemini-embedding-001", "Embedding model name")
	flag.IntVar(&embedDims, "embedding-dims", 128, "Reduced output dimensionality (Vertex AI native, or gateway via OpenAI \"dimensions\" param for MRL models; 0 = model default)")
	flag.IntVar(&embedMaxInputChars, "embedding-max-input-chars", 0, "Hard cap on embed input length in chars before the API call (0 = default 8000, ~2000 tokens). Set below the model's token window × ~4 for small-context gateway/sidecar models to avoid status-400 rejections.")
	flag.Float64Var(&semanticScoreGap, "semantic-score-gap", 0, "Semantic relevance cutoff: drop results more than this absolute cosine gap below the top hit (0, the default, disables the cutoff; 0.02 was the previous default)")
	flag.BoolVar(&searchRecallShaping, "search-recall-shaping", true, "Collapse near-duplicate facts in Search results and re-rank by state (an active watch_item first, a mild recency decay); false returns the fused order unchanged")
	flag.BoolVar(&memoryWriteDedup, "memory-write-dedup", true, "Land an upsert that restates a stable_fact, operational_pattern, watch_item or hypothesis on the cell it repeats instead of minting a second one")
	flag.BoolVar(&searchLexicalShortCircuit, "search-lexical-shortcircuit", false, "Let a full-coverage keyword hit short-circuit Search and skip semantic retrieval (previous behaviour); default false always fuses keyword and semantic results by RRF")
	flag.StringVar(&memMapsDir, "mental-maps-dir", "", "Directory of *.graph.gob.gz mental maps (code, service, or other graphs) to serve as NATIVE voice tools (search_graph etc.; ADR-045; empty = off). Requires --embedding-endpoint; --embedding-dims/--embedding-max-input-chars must match the maps' fingerprint.")
	flag.IntVar(&memMapsRouteFanout, "mental-maps-route-fanout", 1, "Number of maps a semantic (no explicit map) graph search consults")
	flag.StringVar(&memMapsKeyFile, "mental-maps-snapshot-key", "", "age identity file (from `xray graph keygen`) that opens encrypted snapshots in --mental-maps-dir; a map read encrypted stays encrypted when flushed. Default $XRAY_SNAPSHOT_KEY")
	flag.IntVar(&memMapsTTLMin, "mental-maps-ttl-min", 0, "Evict a map's in-memory graph after this many idle MINUTES (0 = never; reloads from the .gob on next use)")
	flag.StringVar(&gcpProject, "gcp-project", "", "GCP project for Vertex AI embeddings (alternative to --embedding-endpoint)")
	flag.StringVar(&gcpLocation, "gcp-location", "europe-west4", "GCP location for Vertex AI embeddings (and for voice, unless --voice-location is set)")
	flag.StringVar(&voiceLocation, "voice-location", "", "GCP location for Gemini Live voice on Vertex AI (default: --gcp-location). gemini-3.8-live is served only on the multi-regions eu and us")
	flag.StringVar(&geminiAPIKey, "gemini-api-key", "", "Google AI API key for Gemini Live voice (alternative to --gcp-project)")
	flag.StringVar(&voiceModel, "voice-model", "gemini-3.8-live", "Gemini Live model name (baseline; CR system_configuration.voiceModel overrides per session)")
	flag.StringVar(&voiceName, "voice-name", "Kore", "Gemini prebuilt voice name")
	flag.Int64Var(&userDailyAudioTokenLimit, "user-daily-audio-token-limit", 500000, "Max Gemini Live AUDIO tokens (audio-in + audio-out) an authenticated user may spend per UTC day (0 = unlimited; CR maxUserDailyAudioTokens overrides). Audio is ~all of voice cost and equals the Live Audio billing SKUs. See ADR-042")
	flag.IntVar(&maxConcurrentVoice, "max-concurrent-voice", 20, "Cap on simultaneous voice sessions across all users (0 = unlimited)")
	flag.IntVar(&voiceMaxOutputTokens, "voice-max-output-tokens", 8192, "Per-turn Gemini Live output token cap so a runaway/adversarial turn fails fast instead of draining the daily quota (0 = unlimited). See AUDIT F-6")
	flag.IntVar(&headroomLimit, "headroom-content-limit", 0, "Max chars per search-result item content after markdown-noise stripping; bounds LLM context cost (0 = disabled; CR maxItemContentChars overrides). See ADR-038")
	flag.BoolVar(&affectiveDialog, "voice-affective-dialog", false, "Enable Gemini Live affective dialog: model detects emotional tone and adapts responses (native-audio only)")
	flag.BoolVar(&proactiveAudio, "voice-proactive-audio", false, "Enable Gemini Live proactive audio: model may stay silent on out-of-context/background speech (native-audio only)")
	flag.Int64Var(&maxRequestBytes, "max-request-bytes", 1<<20, "Max request body size in bytes for non-WebSocket endpoints (0 = unlimited)")
	flag.IntVar(&mcpListMemoryMaxLimit, "mcp-list-memory-max-limit", xray.DefaultMCPListMemoryMaxLimit, "Hard ceiling on list_memory's limit param, regardless of what the MCP caller requests. See ADR-051")
	flag.IntVar(&mcpSearchMemoryMaxLimit, "mcp-search-memory-max-limit", xray.DefaultMCPSearchMemoryMaxLimit, "Hard ceiling on search_memory's limit param, regardless of what the MCP caller requests. See ADR-051")
	flag.IntVar(&mcpRecentMemoryMaxLimit, "mcp-recent-memory-max-limit", xray.DefaultMCPRecentMemoryMaxLimit, "Hard ceiling on recent_memory's limit param, regardless of what the MCP caller requests. See ADR-051")
	flag.StringVar(&otelEndpoint, "otel-endpoint", "", "OTLP gRPC endpoint for tracing (empty = disabled)")
	flag.StringVar(&registryPullSecret, "registry-pull-secret", "", "k8s dockerconfigjson Secret (in --namespace) with the registry credentials for pulling a mental_map cell's graphRef artifact; empty = the process's docker config (anonymous in a pod)")
	flag.StringVar(&otelTokenSecret, "otel-token-secret", "", "k8s Secret name containing the bearer token for the OTLP endpoint (key: token)")
	flag.StringVar(&otelProject, "otel-project", "", "openinference.project.name attribute sent with every span")
	flag.StringVar(&githubClientID, "github-client-id", "", "GitHub OAuth App client ID")
	flag.StringVar(&githubClientSecret, "github-client-secret", "", "GitHub OAuth App client secret")
	flag.StringVar(&githubOrg, "github-org", "", "Required GitHub org for access")
	flag.StringVar(&githubTeam, "github-team", "", "Optional GitHub team slug within org")
	flag.StringVar(&publicURL, "public-url", "", "Externally reachable base URL for OAuth callbacks (e.g. https://xray.example.com)")
	flag.StringVar(&uiCallbackOrigins, "ui-callback-origins", "", "Comma-separated origins a login may hand the session token to via ?ui=<origin>, for a UI served from a DIFFERENT host than this server (e.g. https://ui.example.com,http://localhost:8080). Empty (default) disables the hand-off: logins always land on this server's own \"/\" and authenticate by cookie. Each entry is matched as an exact scheme+host+port; anything unlisted is ignored, so this can never become an open redirect for a freshly minted token.")
	flag.StringVar(&azureClientID, "azure-client-id", "", "Azure AD OAuth app client ID")
	flag.StringVar(&azureClientSecret, "azure-client-secret", "", "Azure AD OAuth app client secret")
	flag.StringVar(&azureTenantID, "azure-tenant-id", "", "Azure AD tenant ID")
	flag.StringVar(&googleClientID, "google-client-id", "", "Google OAuth client ID")
	flag.StringVar(&googleClientSecret, "google-client-secret", "", "Google OAuth client secret")
	flag.StringVar(&googleDomain, "google-domain", "", "Required Google Workspace domain (the ID token's hd claim) for access")
	flag.BoolVar(&webUI, "ui", true, "Serve the embedded web UI at / (when this binary was built with one)")
	flag.BoolVar(&dev, "dev", false, "Dev mode: disable auth requirement and allow all CORS origins")
	flag.BoolVar(&allowAnyUser, "allow-any-user", false, "Let any GitHub account (no --github-org), any Entra account (no --azure-tenant-id) or any Google account (no --google-domain) sign in. Without it the server refuses to start with a provider and no restriction")
	flag.BoolVar(&mcpOAuth, "mcp-oauth", false, "Enable the co-located OAuth 2.1 authorization server for MCP (RFC 9728 discovery + /authorize + /token, mounted on the main server)")
	flag.StringVar(&mcpResourceURL, "mcp-resource-url", "", "MCP resource URL advertised in OAuth metadata (default: <public-url>/mcp)")
	flag.DurationVar(&mcpTokenTTL, "mcp-token-ttl", 15*time.Minute, "Lifetime of issued MCP access tokens")
	flag.DurationVar(&episodicTTL, "episodic-ttl", 7*24*time.Hour, "Max age of an episodic session memory before it is forgotten — dropped from get_recent_memory and deleted by the reaper (0 = never forget). See ADR-020")
	flag.DurationVar(&retentionSweepInterval, "retention-sweep-interval", time.Hour, "How often the general retention sweep runs, hard-deleting memory items past their StaleAfter deadline (0 = disabled). Durable/config kinds (system_configuration, system_identity, policy, mental_map/map-carrying cells) and episodic memories (owned by --episodic-ttl instead) are exempt regardless of StaleAfter; an item with no StaleAfter set never expires.")
	flag.IntVar(&trustedProxyHops, "trusted-proxy-hops", 0, "Number of trusted reverse proxies in front of the server (e.g. 1 for a single ingress/LB that appends the client IP). Controls how X-Forwarded-For is read for rate-limit keying: only this many entries from the right are trusted; 0 (default) ignores XFF and keys on the TCP peer address, which cannot be spoofed.")
	flag.Parse()

	arconfig.DefaultNamespace = namespace
	xray.SetTrustedProxyHops(trustedProxyHops)

	// Each OAuth credential defaults to its env var when the flag is unset.
	// Passing a secret as --flag=$(VAR) interpolates the literal into argv, where
	// it leaks via /proc/<pid>/cmdline and `ps`; the chart injects these as env
	// vars from a k8s Secret and omits the flags. An explicit flag still wins.
	envDefault := func(flag *string, key string) {
		if *flag == "" {
			*flag = os.Getenv(key)
		}
	}
	envDefault(&githubClientID, "GITHUB_CLIENT_ID")
	envDefault(&githubClientSecret, "GITHUB_CLIENT_SECRET")
	envDefault(&azureClientID, "AZURE_CLIENT_ID")
	envDefault(&azureClientSecret, "AZURE_CLIENT_SECRET")
	envDefault(&azureTenantID, "AZURE_TENANT_ID")
	envDefault(&googleClientID, "GOOGLE_CLIENT_ID")
	envDefault(&googleClientSecret, "GOOGLE_CLIENT_SECRET")
	envDefault(&googleDomain, "GOOGLE_DOMAIN")

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs
	log.Logger = log.Output(zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: "15:04:05.000",
	}).With().Timestamp().Logger()
	switch logLevel {
	case "trace":
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	logger := log.Logger
	log.Info().Str("version", version.Version).Str("addr", httpAddr).Bool("dev", dev).Msg("xray server starting")

	// Production safety checks — fatal unless --dev is set.
	if !dev {
		if githubClientID == "" && azureClientID == "" && googleClientID == "" {
			log.Fatal().Msg("no OAuth provider configured — set --github-client-id, --azure-client-id or --google-client-id, or pass --dev for local development")
		}
		if mcpAddr != "" && mcpTokenSecret == "" {
			log.Fatal().Msg("--mcp-address requires --mcp-token-secret in production — pass --dev to skip")
		}
		// Without --public-url the CORS origin falls back to "*" and the voice
		// WebSocket origin check is disabled (see allowedOrigin below). Fail closed.
		if publicURL == "" {
			log.Fatal().Msg("--public-url is required in production — it scopes CORS and the voice WebSocket origin check; pass --dev for local development")
		}
	}

	// --dev means sign-in is off. Together with a provider the server would run two
	// authorisation models at once (MCP and CORS follow the flag, the API follows
	// the provider), so the combination is refused.
	if dev && (githubClientID != "" || azureClientID != "" || googleClientID != "") {
		log.Fatal().Msg("--dev turns sign-in off and cannot be combined with --github-client-id, --azure-client-id or --google-client-id")
	}
	if dev && !isLoopbackAddr(httpAddr) {
		log.Warn().Str("addr", httpAddr).Msg("--dev with sign-in off is listening beyond loopback: anyone who can reach it can read and write memory")
	}

	// A provider without an org or tenant admits every account of that provider.
	// That is a choice to make out loud, not a default.
	if githubClientID != "" && githubOrg == "" {
		if !allowAnyUser {
			log.Fatal().Msg("github sign-in needs --github-org (any GitHub account would otherwise get a session); pass --allow-any-user to admit everyone")
		}
		log.Warn().Msg("github OAuth without --github-org: any GitHub user can log in (--allow-any-user)")
	}
	if azureClientID != "" && azureTenantID == "" {
		if !allowAnyUser {
			log.Fatal().Msg("entra sign-in needs --azure-tenant-id (any Entra account would otherwise get a session); pass --allow-any-user to admit everyone")
		}
		log.Warn().Msg("azure OAuth without --azure-tenant-id: any Entra user can log in (--allow-any-user)")
	}
	if googleClientID != "" && googleDomain == "" {
		if !allowAnyUser {
			log.Fatal().Msg("google sign-in needs --google-domain (any Google account would otherwise get a session); pass --allow-any-user to admit everyone")
		}
		log.Warn().Msg("google OAuth without --google-domain: any Google user can log in (--allow-any-user)")
	}

	otelToken := &observability.BearerToken{}
	if err := observability.ConfigureOTel(context.Background(), otelEndpoint, otelProject, otelToken, logger); err != nil {
		log.Warn().Err(err).Msg("otel tracing init failed — continuing without tracing")
	}

	// Memory store: HNSW with Vertex AI or gateway, keyword fallback if neither set.
	var memory *xray.MemoryStore
	if embedEndpoint != "" || gcpProject != "" {
		embedder, err := index.NewEmbedder(context.Background(), embedEndpoint, embedModel, gcpProject, gcpLocation, embedDims, embedMaxInputChars, log.Logger)
		if err != nil {
			log.Warn().Err(err).Msg("embedding init failed — using keyword memory")
			memory = xray.NewMemoryStore()
		} else {
			log.Info().Str("endpoint", embedEndpoint).Str("project", gcpProject).Str("model", embedModel).Float64("score_gap", semanticScoreGap).Bool("lexical_shortcircuit", searchLexicalShortCircuit).Msg("HNSW semantic memory enabled")
			memory = xray.NewMemoryStoreWithEmbedder(embedder, logger)
			memory.SetSemanticScoreGap(float32(semanticScoreGap))
			memory.SetLexicalShortCircuit(searchLexicalShortCircuit)
		}
	} else {
		memory = xray.NewMemoryStore()
	}
	memory.SetEpisodicTTL(episodicTTL) // forget episodic memories older than this (ADR-020)
	memory.SetRecallShaping(searchRecallShaping)
	memory.SetWriteDedup(memoryWriteDedup)

	var mcpTokenVal atomic.Value
	mcpTokenVal.Store("")

	secretStore := xray.NewSecretStore()
	var cellSyncer *xray.CellSyncer // non-nil when k8s is available
	configWatcher := xray.NewConfigWatcher(memory)

	var vm *xray.VoiceManager

	// Code graph (ADR-045): when a snapshot dir is configured, load the per-project
	// graphs and expose their read functions as NATIVE in-process voice tools
	// (search_code etc. — direct calls, no MCP). CGO-free: snapshots carry parsed
	// nodes + cached vectors, so the embedder only embeds queries at search time.
	// NON-FATAL: a bad/mismatched snapshot dir logs and continues WITHOUT code
	// tools — it must never take down the server (and with it MCP auth).
	// Built BEFORE the k8s block so the CellSyncer can wire the registry as a
	// MapRegistrar before SeedAll / the first reconcile fires (else graph-carrying
	// cells present at startup would never register as maps).
	var memMapsDeps *graphmcp.Deps
	if memMapsDir != "" {
		if embedEndpoint == "" {
			log.Error().Msg("--mental-maps-dir set but --embedding-endpoint is empty; code-graph tools disabled")
		} else if deps := loadMemsnapGraph(memMapsDir, memMapsKeyFile, embedEndpoint, embedModel, embedDims, embedMaxInputChars, memMapsRouteFanout, memMapsTTLMin, logger); deps != nil {
			memMapsDeps = deps
		}
	}

	// k8s / controller-runtime setup — gracefully disabled when no kubeconfig available.
	k8sCfg, cfgErr := ctrl.GetConfig()
	if cfgErr != nil {
		logger.Warn().Err(cfgErr).Msg("no k8s config — Secret watch and MemoryCell sync disabled")
	} else {
		// Build scheme: core k8s types (Secret) + MemoryCell CRD.
		sc, _ := (&scheme.Builder{GroupVersion: mcell.GroupVersion}).Build()
		_ = mcell.AddToScheme(sc)
		_ = clientgoscheme.AddToScheme(sc)

		mgr, mgrErr := ctrl.NewManager(k8sCfg, ctrl.Options{
			Scheme: sc,
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{namespace: {}},
				// managedFields are server-side-apply bookkeeping this process never
				// reads, and a large share of each cached object.
				DefaultTransform: cache.TransformStripManagedFields(),
			},
			// Disable built-in metrics and health servers — we run our own HTTP server.
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
			LeaderElection:         false,
			Logger:                 zerologr.New(&logger),
		})
		if mgrErr != nil {
			logger.Warn().Err(mgrErr).Msg("controller-runtime manager init failed — k8s sync disabled")
		} else {
			syncer := xray.NewCellSyncer(mgr.GetClient(), memory, logger)
			syncer.SetRegistryPullSecret(registryPullSecret)
			cellSyncer = syncer

			// Let graph-carrying MemoryCells (Data["graph"], or Data["graphRef"] — a
			// digest-pinned OCI artifact the server pulls) register as in-memory maps
			// via the same registry the mounted snapshots use — no image-volume, no
			// restart. Deps.Resolver is the concrete *engine.Registry, which satisfies
			// MapRegistrar. Wired here — before SetupWithManager and SeedAll — so startup
			// and replayed cells register. No-op when the code graph is off.
			if memMapsDeps != nil {
				if reg, ok := memMapsDeps.Resolver.(xray.MapRegistrar); ok {
					syncer.SetMapRegistrar(reg)
					log.Info().Msg("graph-carrying MemoryCells will register as in-memory maps")
				}
			}

			// Register field indexes before the cache starts.
			if err := registerCellIndexes(mgr); err != nil {
				logger.Warn().Err(err).Msg("cell field index registration failed")
			}

			// Register the reconciler.
			if err := syncer.SetupWithManager(mgr); err != nil {
				logger.Warn().Err(err).Msg("CellSyncer controller setup failed")
			}

			// Dedicated informer for the system_configuration MemoryCell — isolated from
			// the general memory cache so voice config is never affected by List queries.
			if cellInf, cellInfErr := mgr.GetCache().GetInformer(context.Background(), &mcell.MemoryCell{}); cellInfErr == nil {
				cellInf.AddEventHandler(configWatcher.EventHandlerFuncs()) //nolint:errcheck
				logger.Info().Str("cr", xray.SystemConfigCRName).Msg("system_configuration watcher registered")
			} else {
				logger.Warn().Err(cellInfErr).Msg("system_configuration informer failed")
			}

			// Wire all Secrets into secretStore — used for MCP server token resolution.
			if secInf, secInfErr := mgr.GetCache().GetInformer(context.Background(), &corev1.Secret{}); secInfErr == nil {
				secInf.AddEventHandler(secretStore.SecretEventHandlerFuncs()) //nolint:errcheck
				logger.Info().Msg("secret store watcher registered")
			} else {
				logger.Warn().Err(secInfErr).Msg("secret store informer failed")
			}

			if otelTokenSecret != "" {
				if informer, ierr := mgr.GetCache().GetInformer(context.Background(), &corev1.Secret{}); ierr == nil {
					load := func(obj any) {
						if sec, ok := obj.(*corev1.Secret); ok && sec.Namespace == namespace && sec.Name == otelTokenSecret {
							otelToken.Set(string(sec.Data["token"]))
							logger.Info().Str("secret", otelTokenSecret).Msg("otel token loaded")
						}
					}
					informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{ //nolint:errcheck
						AddFunc:    load,
						UpdateFunc: func(_, obj any) { load(obj) },
					})
				}
			}

			if mcpTokenSecret != "" {
				informer, ierr := mgr.GetCache().GetInformer(context.Background(), &corev1.Secret{})
				if ierr == nil {
					informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{ //nolint:errcheck
						AddFunc: func(obj any) {
							sec, ok := obj.(*corev1.Secret)
							if !ok || sec.Namespace != namespace || sec.Name != mcpTokenSecret {
								return
							}
							mcpTokenVal.Store(string(sec.Data["token"]))
							logger.Info().Str("secret", mcpTokenSecret).Msg("mcp token loaded")
						},
						UpdateFunc: func(_, obj any) {
							sec, ok := obj.(*corev1.Secret)
							if !ok || sec.Namespace != namespace || sec.Name != mcpTokenSecret {
								return
							}
							mcpTokenVal.Store(string(sec.Data["token"]))
							logger.Info().Str("secret", mcpTokenSecret).Msg("mcp token reloaded")
						},
					})
				}
			}

			// Set the session-persistence HMAC key BEFORE the manager starts. The
			// informer replays the existing system_configuration CR as an Add event
			// during WaitForCacheSync (below), which drives restoreSessions exactly
			// once. If the key were set after mgr.Start (as it was originally), that
			// one restore would run with an empty key, silently restore nothing, and
			// never fire again (the CR is unchanged) — so every restart logged users
			// out despite a valid row in the CR. The flush callback only marks stats
			// dirty; the actual CR write needs the writer wired below, which happens
			// before any flush can fire. See ADR-047.
			if key := sessionHMACKey(githubClientSecret, azureClientSecret, googleClientSecret); key != nil {
				xray.SetSessionPersistence(key, memory.FlushStats)
				logger.Info().Msg("session persistence enabled — logins survive restart, shared across replicas")
			} else {
				logger.Warn().Msg("no OAuth client secret — session persistence disabled (logins lost on restart)")
			}

			// Persist MCP OAuth dynamic-client registrations to CR status (ADR-049),
			// also wired BEFORE mgr.Start for the same reason: the CR replays once
			// during cache-sync and that is when restoreOAuthClients must already have
			// its flush hook set. Without this, Claude Code's cached client_id is
			// unknown after a restart and /authorize returns "redirect_uri not
			// registered". The registrations are non-secret (client_id + redirect_uris).
			xray.SetOAuthClientPersistence(memory.FlushStats)
			logger.Info().Msg("MCP OAuth client persistence enabled — DCR registrations survive restart")

			// Start the manager (cache + reconciler) in the background.
			mgrCtx, mgrCancel := context.WithCancel(context.Background())
			go func() {
				if err := mgr.Start(mgrCtx); err != nil {
					logger.Error().Err(err).Msg("controller-runtime manager exited")
				}
			}()

			// Block until the cache has a full initial list from the API server.
			// mgrCtx is the process lifetime — no artificial timeout; if the API server
			// is unreachable the pod stays unready and Kubernetes won't route traffic to it.
			if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
				logger.Warn().Msg("cache sync interrupted — serving with partial data")
			}

			// Wire the cache reader so List/KeywordSearch use cache indexes.
			memory.SetCacheReader(mgr.GetCache(), namespace)
			// Wire the full client so flushStats can update CR status.
			memory.SetWriter(mgr.GetClient())
			logger.Info().Msg("cache reader wired — keyword search uses cache indexes")

			// Start the single debounced stats-flusher (ADR-047): the hot path
			// (search/voice/login) only marks stats dirty; this loop writes the CR at
			// most once per interval, and only when something changed. Replaces the
			// old per-request synchronous flush so search throughput is not gated on an
			// etcd round-trip and N replicas don't hammer one object. mgrCtx bounds it
			// to process lifetime and triggers a final flush on shutdown.
			memory.StartStatsFlusher(mgrCtx, 5*time.Second)

			// Forget mechanism (ADR-020): periodically delete episodic memories past
			// --episodic-ttl. No-op when the TTL is 0. Hourly is fine — read-time
			// filtering already hides expired memories instantly; this only reclaims
			// storage. mgrCtx bounds it to process lifetime.
			memory.StartEpisodicReaper(mgrCtx, cellSyncer, time.Hour)

			// General retention sweep (decay by default): periodically delete any
			// memory item past its StaleAfter deadline, across all non-exempt kinds
			// — the general counterpart to the episodic-only reaper above. No-op
			// when --retention-sweep-interval is 0. mgrCtx bounds it to process
			// lifetime.
			cellSyncer.StartRetentionSweep(mgrCtx, namespace, retentionSweepInterval)

			// Embedding backfill: give CRs that lack spec.embedding a vector and patch it back.
			// The cache now holds all items; this pass only calls the embedder for new/stripped CRs.
			go func() {
				if err := syncer.SeedAll(mgrCtx, namespace); err != nil {
					logger.Warn().Err(err).Msg("MemoryCell embedding backfill failed")
				}
			}()

			// Cancel manager on process exit.
			go func() {
				ch := make(chan os.Signal, 1)
				signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
				<-ch
				mgrCancel()
			}()
		}
	}

	uiOrigins := splitAndTrim(uiCallbackOrigins)
	if len(uiOrigins) > 0 {
		logger.Info().Strs("origins", uiOrigins).Msg("cross-origin UI login hand-off enabled (?ui=)")
	}
	ghCfg := xray.GitHubOAuthConfig{
		ClientID:          githubClientID,
		ClientSecret:      githubClientSecret,
		Org:               githubOrg,
		Team:              githubTeam,
		PublicURL:         publicURL,
		UICallbackOrigins: uiOrigins,
	}
	azCfg := xray.AzureOAuthConfig{
		ClientID:          azureClientID,
		ClientSecret:      azureClientSecret,
		TenantID:          azureTenantID,
		PublicURL:         publicURL,
		UICallbackOrigins: uiOrigins,
	}
	// In dev mode CORS allows all origins; in prod lock to the configured public URL.
	allowedOrigin := publicURL
	if dev {
		allowedOrigin = ""
	}
	// The web UI is part of the binary when it was built with one ("make ui" stages
	// the export for //go:embed), and --ui=false serves the API alone. A binary
	// built without the export has no UI to serve, which is not an error.
	var uiFS fs.FS
	if webUI {
		if assets, ok := webui.Assets(); ok {
			uiFS = assets
			logger.Info().Msg("web UI embedded: serving it at /")
		} else {
			logger.Info().Msg("this binary was built without the web UI (make ui); serving the API only")
		}
	}
	srv := xray.NewServer(logger, memory, uiFS, ghCfg, azCfg, allowedOrigin, maxRequestBytes, configWatcher,
		xray.WithGoogleOAuth(xray.GoogleOAuthConfig{
			ClientID:          googleClientID,
			ClientSecret:      googleClientSecret,
			Domain:            googleDomain,
			PublicURL:         publicURL,
			UICallbackOrigins: uiOrigins,
		}))

	if gcpProject != "" || geminiAPIKey != "" {
		voiceLoc := voiceLocation
		if voiceLoc == "" {
			voiceLoc = gcpLocation
		}
		vm = xray.NewVoiceManager(memory, configWatcher, secretStore, cellSyncer, namespace, gcpProject, voiceLoc, geminiAPIKey, voiceModel, voiceName, userDailyAudioTokenLimit, voiceMaxOutputTokens, headroomLimit, affectiveDialog, proactiveAudio, allowedOrigin, maxConcurrentVoice, logger)
		vm.SetMemMaps(memMapsDeps) // native code-graph voice tools (ADR-045); no-op when nil
		srv.SetVoiceManager(vm)
		log.Info().Msg("voice (Gemini Live) enabled at /v1/voice")
	}

	// MCP OAuth: co-locate an OAuth 2.1 authorization server with the MCP resource
	// endpoint on the main server. The endpoint accepts X-Ray-issued JWTs and, when
	// configured, the static --mcp-token-secret as a full-scope fallback.
	if mcpOAuth {
		issuer := publicURL
		if issuer == "" {
			issuer = "http://localhost" + httpAddr // dev default
		}
		resourceURL := mcpResourceURL
		if resourceURL == "" {
			resourceURL = strings.TrimRight(issuer, "/") + "/mcp"
		}
		// Derive the MCP JWT signing key from the same OAuth client secrets as the
		// session HMAC key (ADR-050), so every replica converges on one key and
		// cross-replica token validation works. nil seed (no secret) ⇒ random
		// per-process key, single-replica only.
		mcpSeed := xray.MCPSigningSeed(githubClientSecret, azureClientSecret, googleClientSecret)
		mcpAuth, err := xray.NewMCPAuthorizer(issuer, resourceURL, mcpTokenTTL, dev, nil, mcpSeed, logger)
		if err != nil {
			log.Fatal().Err(err).Msg("MCP authorizer init failed")
		}
		// MCP is admin-only: gate OAuth token issuance on the live writeAllowedUsers
		// allowlist (userHashes) from the system_configuration CR — same source the
		// voice write-gate uses. Empty list ⇒ deny all (fail-closed); the static
		// --mcp-token-secret bearer remains the out-of-band full-access admin path.
		mcpAuth.SetAllowFn(func() []string { return configWatcher.Get().WriteAllowedUsers })
		mcpHandler := xray.NewMCPServer(memory, logger, func() string { return mcpTokenVal.Load().(string) }, mcpAuth, cellSyncer, namespace, memMapsDeps, mcpListMemoryMaxLimit, mcpSearchMemoryMaxLimit, mcpRecentMemoryMaxLimit)
		srv.SetMCPAuth(mcpAuth, mcpHandler)
		log.Info().Str("issuer", issuer).Str("resource", resourceURL).Bool("dev", dev).Msg("MCP OAuth enabled at /mcp (+ /.well-known, /authorize, /token)")
	}

	httpServer := &http.Server{
		Addr:        httpAddr,
		Handler:     srv.Handler(),
		IdleTimeout: 120 * time.Second,
		// ReadTimeout and WriteTimeout intentionally unset — WebSocket connections
		// hijack the underlying TCP conn and manage their own timeouts; HTTP-level
		// timeouts fire on quiet voice sessions and drop the connection mid-session.
	}

	// Standalone MCP listener on :8083 (static-secret only). Skipped when
	// --mcp-oauth is on, since MCP is then served on the main server under /mcp.
	if mcpAddr != "" && !mcpOAuth {
		mcpServer := &http.Server{
			Addr:        mcpAddr,
			Handler:     xray.NewMCPServer(memory, logger, func() string { return mcpTokenVal.Load().(string) }, nil, cellSyncer, namespace, memMapsDeps, mcpListMemoryMaxLimit, mcpSearchMemoryMaxLimit, mcpRecentMemoryMaxLimit),
			ReadTimeout: 30 * time.Second,
		}
		go func() {
			log.Info().Str("addr", mcpAddr).Msg("xray MCP server ready")
			if err := mcpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error().Err(err).Msg("xray MCP server failed")
			}
		}()
	}

	if metricsAddr != "" {
		metricsHandler, err := observability.ConfigureMetrics(logger)
		if err != nil {
			log.Error().Err(err).Msg("metrics init failed — /metrics disabled")
		} else {
			mux := http.NewServeMux()
			mux.Handle("/metrics", metricsHandler)
			metricsServer := &http.Server{Addr: metricsAddr, Handler: mux, ReadTimeout: 30 * time.Second}
			go func() {
				log.Info().Str("addr", metricsAddr).Msg("Prometheus metrics server ready at /metrics")
				if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Error().Err(err).Msg("metrics server failed")
				}
			}()
		}
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		log.Info().Msg("shutting down")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		httpServer.Shutdown(ctx) //nolint:errcheck
	}()

	log.Info().Str("addr", httpAddr).Msg("xray server ready")
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("server failed")
	}
}

// registerCellIndexes adds field indexes to the manager cache for MemoryCell.
// Must be called before mgr.Start().
func registerCellIndexes(mgr ctrl.Manager) error {
	ctx := context.Background()
	idx := mgr.GetFieldIndexer()

	fields := map[string]func(*mcell.MemoryCell) string{
		"spec.kind":    func(c *mcell.MemoryCell) string { return c.Spec.Kind },
		"spec.subject": func(c *mcell.MemoryCell) string { return c.Spec.Subject },
		"spec.status":  func(c *mcell.MemoryCell) string { return c.Spec.Status },
		"spec.id":      func(c *mcell.MemoryCell) string { return c.Spec.ID },
	}
	for field, extract := range fields {
		f := extract // capture
		if err := idx.IndexField(ctx, &mcell.MemoryCell{}, field,
			func(obj client.Object) []string {
				return []string{f(obj.(*mcell.MemoryCell))}
			}); err != nil {
			return err
		}
	}
	return nil
}

// loadMemsnapGraph builds the read-only code-graph registry from a snapshot dir and
// returns graphmcp.Deps for the NATIVE voice tools (no MCP). Returns nil (and logs)
// on any failure — the caller continues without code tools, so a snapshot problem
// can never crash the server. Snapshots are rebuilt offline by the CGO build of this binary.
func loadMemsnapGraph(dir, keyFile, embedEndpoint, embedModel string, embedDims, embedMaxInputChars, fanout, ttlMin int, logger zerolog.Logger) *graphmcp.Deps {
	emb, err := msindex.NewEmbedder(context.Background(), embedEndpoint, embedModel, embedDims, embedMaxInputChars, logger)
	if err != nil {
		log.Error().Err(err).Msg("code-graph embedder init failed; code tools disabled")
		return nil
	}
	fp := msengine.EmbedFingerprint{Model: embedModel, Dims: embedDims, MaxInputChars: embedMaxInputChars}
	reg := msengine.NewRegistry(emb, fp, fanout, time.Duration(ttlMin)*time.Minute, false, logger)
	if keyFile == "" {
		keyFile = os.Getenv("XRAY_SNAPSHOT_KEY")
	}
	if keyFile != "" {
		keys, err := msengine.LoadSnapshotKeys(keyFile, "")
		if err != nil {
			log.Error().Err(err).Msg("snapshot key unreadable; code-graph tools disabled")
			return nil
		}
		reg.SetSnapshotKeys(keys)
	}

	// Find snapshots at the top level AND one level down. Per-repo image-volumes
	// (ADR-045) each mount at /snapshots/<repo>/, so a flat top-level glob would
	// miss them; a single packed image lands them at the top level. Cover both.
	var matches []string
	if err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries; don't abort the whole walk
		}
		if !d.IsDir() && strings.HasSuffix(p, ".graph.gob.gz") {
			matches = append(matches, p)
		}
		return nil
	}); err != nil {
		log.Error().Err(err).Str("dir", dir).Msg("code-graph walk failed; code tools disabled")
		return nil
	}
	registered := 0
	for _, path := range matches {
		if err := reg.AddSnapshot(path); err != nil {
			log.Error().Err(err).Str("path", path).Msg("skip code-graph snapshot (fingerprint/format)")
			continue
		}
		registered++
	}
	// Zero file snapshots is NOT fatal: maps can also arrive at runtime carried in
	// MemoryCells (Data["graph"] → CellSyncer.AddSnapshotBytes). Keep the registry
	// alive so those can register. Only a hard walk failure disables the feature
	// (handled above). When files were found but ALL rejected, that's a fingerprint/
	// format mismatch worth flagging — but still non-fatal.
	if registered == 0 {
		if len(matches) > 0 {
			log.Warn().Str("dir", dir).Int("found", len(matches)).
				Msg("all file snapshots rejected (fingerprint/format?) — registry kept for MemoryCell-carried maps only")
		} else {
			log.Info().Str("dir", dir).Msg("no file snapshots — registry ready for MemoryCell-carried maps")
		}
	}
	reg.StartSweeper(context.Background())
	// Watch the snapshot dir for file-dropped maps only when it exists. With OCI
	// map mounts removed (maps arrive via MemoryCells), the dir is often absent —
	// watching it would just log a misleading "no such file or directory" warning.
	// Cell-carried maps reload via the CellSyncer, not this watcher, so skipping it
	// costs nothing here.
	if _, statErr := os.Stat(dir); statErr == nil {
		if err := reg.WatchDir(context.Background(), dir); err != nil {
			log.Warn().Err(err).Msg("code-graph dir watch disabled (added snapshots need a restart)")
		}
	} else {
		log.Info().Str("dir", dir).Msg("code-graph dir absent — file-drop watch skipped (MemoryCell-carried maps unaffected)")
	}
	nodes, edges := reg.Totals()
	log.Info().Int("snapshots", registered).Int("nodes", nodes).Int("edges", edges).Msg("code graph loaded (native voice tools)")
	return &graphmcp.Deps{Resolver: reg, Logger: logger}
}

// sessionHMACKey derives the key used to hash session tokens before they are
// persisted to CR status (ADR-047). It is SHA-256 over the configured OAuth
// client secret(s): stable across restarts and identical across replicas (both
// read the same Secret), so a token hashed on one pod validates on another, yet
// it never exposes the client secret itself. Returns nil when no secret is set —
// the caller then leaves sessions in-memory only.
func sessionHMACKey(secrets ...string) []byte {
	h := sha256.New()
	any := false
	for _, s := range secrets {
		if s == "" {
			continue
		}
		any = true
		h.Write([]byte(s))
		h.Write([]byte{0}) // domain-separate the inputs
	}
	if !any {
		return nil
	}
	// Bind the digest to this purpose so it can't collide with another use of the
	// same secret material.
	h.Write([]byte("xray-session-hmac-v1"))
	return h.Sum(nil)
}

// splitAndTrim turns a comma-separated flag value into a slice, dropping empty
// entries so a trailing comma or a blank Helm value yields nil rather than a
// list containing "" — which resolveUIOrigin would otherwise have to special-case.
func splitAndTrim(csv string) []string {
	var out []string
	for _, p := range strings.Split(csv, ",") {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// isLoopbackAddr reports whether a listen address binds only the loopback
// interface. An empty host (":8080") and the unspecified addresses bind all of
// them.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
