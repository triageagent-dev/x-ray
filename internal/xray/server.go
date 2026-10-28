package xray

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/triageagent-dev/x-ray/v2/internal/webui"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/rs/cors"
	"github.com/rs/zerolog"

	"github.com/triageagent-dev/x-ray/v2/internal/version"
)

// Server is the X-Ray HTTP server.
type Server struct {
	logger        zerolog.Logger
	memory        *MemoryStore
	uiFS          fs.FS // nil = no embedded UI
	voice         *VoiceManager
	github        *githubAuth
	azure         *azureAuth
	google        *googleAuth
	allowedOrigin string // CORS origin; empty = "*" (dev only)
	basePath      string // path the server is mounted under ("/path"); "" at the host root
	maxBodyBytes  int64  // request body cap for non-WS routes; 0 = unlimited
	devMu         sync.Mutex
	devToken      string // session of the built-in dev user (auth disabled only)
	mux           *http.ServeMux
	api           huma.API
	mcpAuth       *mcpAuthorizer // nil = MCP OAuth disabled
	config        *ConfigWatcher // nil-safe: ConfigWatcher.Get() never returns nil
}

// ServerOption configures an optional part of the Server.
type ServerOption func(*serverOptions)

type serverOptions struct {
	google   GoogleOAuthConfig
	basePath string
}

// WithGoogleOAuth turns on Google sign-in.
func WithGoogleOAuth(cfg GoogleOAuthConfig) ServerOption {
	return func(o *serverOptions) { o.google = cfg }
}

// WithBasePath mounts the server under a path (BasePath of the public URL) for
// a proxy that forwards that path unchanged. Requests outside it are still
// served as before, which keeps the kubelet probes and in-cluster clients working.
func WithBasePath(p string) ServerOption {
	return func(o *serverOptions) { o.basePath = p }
}

func NewServer(logger zerolog.Logger, memory *MemoryStore, uiFS fs.FS, ghCfg GitHubOAuthConfig, azCfg AzureOAuthConfig, allowedOrigin string, maxBodyBytes int64, config *ConfigWatcher, opts ...ServerOption) *Server {
	var o serverOptions
	for _, opt := range opts {
		opt(&o)
	}
	s := &Server{
		logger:        logger,
		memory:        memory,
		uiFS:          uiFS,
		github:        newGitHubAuth(ghCfg),
		azure:         newAzureAuth(azCfg),
		google:        newGoogleAuth(o.google),
		allowedOrigin: allowedOrigin,
		basePath:      o.basePath,
		maxBodyBytes:  maxBodyBytes,
		config:        config,
	}
	s.build()
	return s
}

// SetVoiceManager attaches a VoiceManager and registers the /v1/voice route.
// Must be called before the server starts accepting connections.
func (s *Server) SetVoiceManager(vm *VoiceManager) {
	s.voice = vm
	s.mux.Handle("/v1/voice", vm)
}

// SetMCPAuth mounts the MCP resource endpoint and the co-located OAuth 2.1
// authorization server on the main mux. auth provides discovery + token
// issuance; mcpHandler is the bearer-guarded MCP JSON-RPC handler (built via
// NewMCPServer with the same authorizer). Either argument may be nil to skip.
// Must be called before the server starts accepting connections.
func (s *Server) SetMCPAuth(auth *mcpAuthorizer, mcpHandler http.Handler) {
	s.mcpAuth = auth
	if auth != nil {
		// Discovery + token endpoints are public (exempted in requireSession).
		s.mux.HandleFunc("GET /.well-known/oauth-protected-resource", auth.protectedResourceMetadata)
		s.mux.HandleFunc("GET /.well-known/oauth-authorization-server", auth.authServerMetadata)
		s.mux.HandleFunc("GET /.well-known/jwks.json", auth.jwks)
		// Under a base path, MCP clients look for the documents at the host root
		// with the path inserted (RFC 8414 §3.1, RFC 9728 §3.1).
		if p := BasePath(auth.issuer); p != "" {
			s.mux.HandleFunc("GET /.well-known/oauth-authorization-server"+p, auth.authServerMetadata)
		}
		if u, err := url.Parse(auth.resource); err == nil && BasePath(auth.issuer) != "" && strings.TrimSuffix(u.Path, "/") != "" {
			s.mux.HandleFunc("GET /.well-known/oauth-protected-resource"+u.Path, auth.protectedResourceMetadata)
		}
		s.mux.HandleFunc("POST /register", auth.handleRegister)
		s.mux.HandleFunc("GET /authorize", auth.handleAuthorize)
		s.mux.HandleFunc("POST /token", auth.handleToken)
	}
	if mcpHandler != nil {
		// /mcp does its own bearer validation (JWT or static secret) and emits
		// the RFC 9728 challenge — it is NOT covered by requireSession.
		s.mux.Handle("/mcp", mcpHandler)
		s.mux.Handle("/mcp/", mcpHandler)
	}
}

func (s *Server) Handler() http.Handler {
	// Auth wraps the API mux — all /v1/* except auth/health require a valid session token
	authed := s.requireSession(s.withDevUser(s.mux))
	var h http.Handler = authed
	if s.uiFS != nil {
		h = s.withUI(authed)
	}
	h = s.limitBody(h)
	h = s.securityHeaders(h)
	origins := []string{"*"}
	if s.allowedOrigin != "" {
		origins = []string{s.allowedOrigin}
	}
	return s.stripBasePath(cors.New(cors.Options{
		AllowedOrigins: origins,
		// DELETE + Mcp-Session-Id support the MCP streamable HTTP transport.
		AllowedMethods:   []string{"GET", "POST", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Authorization", "Content-Type", "Mcp-Session-Id"},
		ExposedHeaders:   []string{"Mcp-Session-Id", "WWW-Authenticate"},
		AllowCredentials: false,
	}).Handler(h))
}

// stripBasePath serves requests under the base path as if they had arrived at
// the root. The bare base path redirects to its slash form, the UI's home.
func (s *Server) stripBasePath(next http.Handler) http.Handler {
	if s.basePath == "" {
		return next
	}
	strip := http.StripPrefix(s.basePath, next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == s.basePath:
			target := s.basePath + "/"
			if r.URL.RawQuery != "" {
				target += "?" + r.URL.RawQuery
			}
			http.Redirect(w, r, target, http.StatusMovedPermanently)
		case strings.HasPrefix(r.URL.Path, s.basePath+"/"):
			strip.ServeHTTP(w, r)
		default:
			next.ServeHTTP(w, r)
		}
	})
}

// contentSecurityPolicy is served on every response. The Next.js static export
// inlines its bootstrap/hydration scripts and CSS (no nonces are emitted), so
// 'unsafe-inline' is required for script-src/style-src. Everything else loads
// from the same origin: self-hosted fonts (next/font copies them into the
// export), avatar images (plus data: URIs for thumbnails), and the same-origin
// SSE + voice WebSocket (covered by connect-src 'self').
//
// default-src 'self'  — fallback for unlisted fetch directives
// script-src ... blob: — the voice AudioWorklet (use-voice-session.ts) is loaded
//
//	from a Blob URL via audioWorklet.addModule(); worklet modules are gated by
//	script-src (worker-src as fallback), so both must allow blob:.
//
// object-src 'none'   — block <object>/<embed> plugin injection
// base-uri 'self'     — block <base> tag injection redirecting relative URLs
// frame-ancestors 'none' — mirrors X-Frame-Options: DENY (clickjacking)
const contentSecurityPolicy = "default-src 'self'; " +
	"script-src 'self' 'unsafe-inline' blob:; " +
	"worker-src 'self' blob:; " +
	"style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; " +
	"font-src 'self'; " +
	"connect-src 'self'; " +
	"object-src 'none'; " +
	"base-uri 'self'; " +
	"frame-ancestors 'none'"

// securityHeaders sets defense-in-depth response headers. HSTS is only sent
// over HTTPS (TLS is terminated at the gateway; honor X-Forwarded-Proto).
func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr := w.Header()
		hdr.Set("X-Content-Type-Options", "nosniff")
		hdr.Set("X-Frame-Options", "DENY")
		hdr.Set("Referrer-Policy", "strict-origin-when-cross-origin")
		hdr.Set("Content-Security-Policy", contentSecurityPolicy)
		if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
			hdr.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		next.ServeHTTP(w, r)
	})
}

// limitBody caps request body size for non-WebSocket routes. The voice
// WebSocket hijacks the connection, so it is exempt — MaxBytesReader would
// otherwise cap the post-upgrade stream.
func (s *Server) limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.maxBodyBytes > 0 && r.URL.Path != "/v1/voice" {
			r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

// requireSession validates all requests except the auth/health exemption list.
// Carriers are resolved by sessionFromRequest: the xray_session HttpOnly cookie
// (same-origin browser), Authorization: Bearer (MCP clients, and a UI served
// from another origin), and the WebSocket subprotocol (that UI's /v1/voice
// upgrade, which cannot set headers).
// Auth is disabled entirely when no GitHub client ID is configured.
func (s *Server) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Auth not configured — allow everything through.
		if s.authDisabled() {
			next.ServeHTTP(w, r)
			return
		}

		path := r.URL.Path
		// Public — no session required
		if strings.HasPrefix(path, "/v1/auth/") ||
			path == "/v1/version" ||
			path == "/v1/categories" ||
			path == "/healthz" ||
			path == "/manifest.json" ||
			path == "/sw.js" ||
			// MCP OAuth: discovery + token are public; /authorize and /mcp
			// handle their own auth (session redirect / bearer challenge).
			strings.HasPrefix(path, "/.well-known/") ||
			path == "/authorize" ||
			path == "/token" ||
			path == "/register" ||
			path == "/mcp" || strings.HasPrefix(path, "/mcp/") {
			next.ServeHTTP(w, r)
			return
		}

		if _, _, ok := sessionFromRequest(r); ok {
			next.ServeHTTP(w, r)
			return
		}

		// Unauthenticated — redirect browsers, return 401 for API/WS
		if strings.HasPrefix(path, "/v1/") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`)) //nolint:errcheck
			return
		}
		http.Redirect(w, r, s.basePath+"/login", http.StatusFound)
	})
}

// withUI wraps the API mux: API paths go to the mux, everything else is served
// from the embedded Next.js static export (internal/webui), which resolves
// export routes, falls back to the app shell for client-side routes, and gives
// each document a script-src naming exactly its own inline scripts.
func (s *Server) withUI(next http.Handler) http.Handler {
	ui := webui.Handler(s.uiFS)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		// API, health, PWA files, and MCP OAuth endpoints go to the mux, not to
		// the static export.
		if strings.HasPrefix(path, "/v1/") ||
			path == "/healthz" ||
			path == "/manifest.json" ||
			path == "/sw.js" ||
			path == "/mcp" || strings.HasPrefix(path, "/mcp/") ||
			strings.HasPrefix(path, "/.well-known/") ||
			path == "/authorize" ||
			path == "/token" ||
			path == "/register" {
			next.ServeHTTP(w, r)
			return
		}
		ui.ServeHTTP(w, r)
	})
}

// allowedImageMime normalizes and allowlists a stored image MIME type to the
// raster formats the upsert path produces. Returns "" for anything else
// (notably text/html and image/svg+xml, which can carry executable script).
// An empty input defaults to image/jpeg, matching the historical default.
func allowedImageMime(mime string) string {
	switch strings.ToLower(strings.TrimSpace(mime)) {
	case "", "image/jpeg", "image/jpg":
		return "image/jpeg"
	case "image/png":
		return "image/png"
	case "image/webp":
		return "image/webp"
	case "image/gif":
		return "image/gif"
	default:
		return ""
	}
}

func (s *Server) build() {
	mux := http.NewServeMux()
	s.mux = mux

	apiConfig := huma.DefaultConfig("X-Ray API", "1.0.0")
	apiConfig.DocsPath = ""
	s.api = humago.New(mux, apiConfig)

	s.registerAuthRoutes()
	s.registerMemoryRoutes()
	s.registerMisc()
}

// ─── oauth ────────────────────────────────────────────────────────────────────

func (s *Server) registerAuthRoutes() {
	s.mux.HandleFunc("/v1/auth/github", s.github.handleLogin)
	s.mux.HandleFunc("/v1/auth/github/callback", s.github.handleCallback)
	s.mux.HandleFunc("/v1/auth/entra", s.azure.handleLogin)
	s.mux.HandleFunc("/v1/auth/entra/callback", s.azure.handleCallback)
	s.mux.HandleFunc("/v1/auth/google", s.google.handleLogin)
	s.mux.HandleFunc("/v1/auth/google/callback", s.google.handleCallback)
	s.mux.HandleFunc("/v1/auth/logout", s.github.handleLogout)

	// /v1/auth/providers — which providers are configured, for rendering login
	// buttons. Unauthenticated, so it returns only booleans: org/team/tenant
	// restriction details are recon data and must not leak to anonymous callers
	// (the login UI only consumes `github`/`azure`/`google`).
	s.mux.HandleFunc("/v1/auth/providers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"github": s.github.cfg.ClientID != "",
			"azure":  s.azure.cfg.ClientID != "",
			"google": s.google.cfg.ClientID != "",
		})
	})

	// /v1/auth/me — returns the login for the current session. With auth
	// disabled (--dev) every request without a session is the built-in dev user.
	s.mux.HandleFunc("/v1/auth/me", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		username, userID, _ := sessionFromRequest(r)
		if username == "" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`)) //nolint:errcheck
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"username": username, "userID": userID}) //nolint:errcheck
	})

	// /v1/user/notes — returns agent_note items for the logged-in user.
	// Returns [] when auth is disabled (no user context).
	s.mux.HandleFunc("/v1/user/notes", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		owner, ok := s.noteOwner(r)
		if !ok {
			// Signed out with auth on — no notes to show.
			json.NewEncoder(w).Encode(map[string]any{"items": []*MemoryItem{}}) //nolint:errcheck
			return
		}
		items := s.memory.List("note", scopePrivate, owner, 200)
		json.NewEncoder(w).Encode(map[string]any{"items": items}) //nolint:errcheck
	})

	// /v1/user/notes/{id} — fetch a single agent_note for the authenticated user.
	// Returns 404 if the item doesn't exist or belongs to another user.
	s.mux.HandleFunc("GET /v1/user/notes/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		owner, ok := s.noteOwner(r)
		if !ok {
			http.NotFound(w, r)
			return
		}
		id := r.PathValue("id")
		it := s.memory.Get(id)
		if it == nil || !isPrivateScope(it.Scope) || it.Subject != owner {
			http.NotFound(w, r)
			return
		}
		json.NewEncoder(w).Encode(it) //nolint:errcheck
	})
}

// ─── memory REST + SSE ───────────────────────────────────────────────────────

type memoryListInput struct {
	Kind    string `query:"kind"`
	Scope   string `query:"scope"`
	Subject string `query:"subject"`
	Limit   int    `query:"limit"`
	Q       string `query:"q"`       // keyword search — if set, overrides list with KeywordSearch results
	PerKind int    `query:"perKind"` // when >0 and kind unset, cap search results per kind (fairness across categories)
}

type memoryListOutput struct {
	Body struct {
		Items []*MemoryItem `json:"items"`
		Total int           `json:"total"`
	}
}

type memorySummaryInput struct {
	Kind    string `query:"kind"`
	Scope   string `query:"scope"`
	Subject string `query:"subject"`
	Top     int    `query:"top"`
	// PerKind bounds the graph preview to at most this many active items per
	// kind (not a global cap), so every category is represented even when one
	// kind dominates; a kind split by tag into several ui.categories gets this
	// many per category. When > 0 it takes precedence over Top; Total is still
	// the true full count. The UI passes its GRAPH_NODES_PER_KIND constant here.
	PerKind int `query:"perKind"`
}

type memorySummaryOutput struct {
	Body struct {
		Items []MemoryItemSummary `json:"items"`
		Total int                 `json:"total"`
		// TotalsByKind is the true count per kind over the full store (not the
		// per-kind render cap), so the UI labels each category with its real
		// count. Only populated for the perKind (graph preview) path.
		TotalsByKind map[string]int `json:"totalsByKind,omitempty"`
		// TotalsByCategory is the same count per display category: "kind|tag"
		// for a category of ui.categories configured with a tag, "kind" for the
		// rest of that kind (ADR-035). Only populated for the perKind path.
		TotalsByCategory map[string]int `json:"totalsByCategory,omitempty"`
	}
}

type memoryGetInput struct {
	ID string `path:"id"`
}

type memoryGetOutput struct {
	Body *MemoryItem
}

// memoryNeighborsInput is GET /v1/memory/neighbors's input.
type memoryNeighborsInput struct {
	ID  string `query:"id" required:"true" doc:"Centre memory item id"`
	Top int    `query:"top" doc:"Neighbours to return (default 20, at most 50)"`
}

type memoryNeighborsOutput struct {
	Body *MemoryNeighborhood
}

// memoryGetItemInput is GET /v1/memory/item's input — the query-parameter
// form of memoryGetInput, for an id that cannot safely ride a {id} path
// segment (see that route's registration comment below): a '/' inside a
// query VALUE is never treated as a path separator by any
// correctly-implemented URL parser or proxy, unlike a literal slash in a
// path segment.
type memoryGetItemInput struct {
	ID string `query:"id" required:"true" doc:"Memory item id — may contain '/', '|', spaces, or non-ASCII characters"`
}

func (s *Server) registerMemoryRoutes() {
	huma.Register(s.api, huma.Operation{
		OperationID: "list-memory",
		Method:      http.MethodGet,
		Path:        "/v1/memory",
		Summary:     "List memory items",
	}, func(_ context.Context, in *memoryListInput) (*memoryListOutput, error) {
		limit := in.Limit
		if limit <= 0 {
			limit = 200
		}
		var items []*MemoryItem
		if in.Q != "" {
			// Per-kind fairness: cap results per kind so a dominant kind (e.g.
			// 'event', which vastly outnumbers others in an incident store) can't
			// monopolise the limit and starve smaller categories like
			// 'active_concern'. Only applied when the caller doesn't scope to a
			// single kind. Mirrors the graph preview's per-category cap.
			if in.Kind == "" && in.PerKind > 0 {
				items = s.memory.KeywordSearchPerKind(in.Q, in.Kind, in.Subject, limit, in.PerKind)
			} else {
				items = s.memory.KeywordSearch(in.Q, in.Kind, in.Subject, limit)
			}
		} else {
			items = s.memory.List(in.Kind, in.Scope, in.Subject, limit)
		}
		// Private cells are personal — never expose via the shared memory endpoint.
		items = slices.DeleteFunc(items, func(it *MemoryItem) bool {
			return isPrivateItem(it.Kind, it.Scope)
		})
		for _, it := range items {
			if _, ok := it.Data["image"]; ok {
				it.ImageURL = "/v1/memory/" + it.ID + "/image"
			}
		}
		out := &memoryListOutput{}
		out.Body.Items = items
		out.Body.Total = len(items)
		return out, nil
	})

	huma.Register(s.api, huma.Operation{
		OperationID: "memory-summary",
		Method:      http.MethodGet,
		Path:        "/v1/memory/summary",
		Summary:     "Lightweight memory index — id/title/kind/tags only, no content",
	}, func(_ context.Context, in *memorySummaryInput) (*memorySummaryOutput, error) {
		var items []MemoryItemSummary
		var total int
		var totalsByKind, totalsByCategory map[string]int
		if in.PerKind > 0 {
			// Bounded graph preview: up to PerKind active items per category, with
			// Total and the totals reporting the true full store size so the UI
			// can label each category with its real count, not the capped one.
			var categories []CategoryConfig
			if s.config != nil {
				categories = s.config.Get().Categories
			}
			items, total, totalsByKind, totalsByCategory = s.memory.ListSummaryPerCategory(in.PerKind, categories)
		} else {
			top := in.Top
			if top <= 0 {
				top = 200
			}
			items, total = s.memory.ListSummary(in.Kind, in.Scope, in.Subject, top)
		}
		// Private cells are personal — never expose via the shared summary endpoint.
		items = slices.DeleteFunc(items, func(it MemoryItemSummary) bool {
			return isPrivateItem(it.Kind, it.Scope)
		})
		out := &memorySummaryOutput{}
		out.Body.Items = items
		out.Body.Total = total // true full count (ignores the preview cap)
		out.Body.TotalsByKind = totalsByKind
		out.Body.TotalsByCategory = totalsByCategory
		return out, nil
	})

	// This path form does not reliably carry an id containing '/'. A LITERAL
	// unescaped slash 404s outright: it splits the request into more path
	// segments than this single-segment {id} pattern expects. A properly
	// percent-encoded slash (%2F) IS matched fine by Go's own stdlib
	// http.ServeMux (1.22+ pattern routing decodes the captured segment back
	// to the original string), but that is not a reliable fix once a request
	// passes through an L7 proxy/load balancer in front of this binary — many
	// normalize or reject an encoded slash before it ever reaches the
	// backend, the same class of transport surprise as the SSE heartbeat fix
	// elsewhere in this package. Ids that embed a source path (e.g. an
	// incident/event id carrying `events/gcp-status/compute-engine`) hit
	// this. See GET /v1/memory/item below for the fix that has no such
	// ambiguity: a '/' inside a query VALUE is never treated as a path
	// separator by any correctly-implemented URL parser or proxy.
	huma.Register(s.api, huma.Operation{
		OperationID: "get-memory-item",
		Method:      http.MethodGet,
		Path:        "/v1/memory/{id}",
		Summary:     "Get a single memory item by ID (id must not contain '/' — see GET /v1/memory/item)",
	}, func(_ context.Context, in *memoryGetInput) (*memoryGetOutput, error) {
		return s.getMemoryItemByID(in.ID)
	})

	// GET /v1/memory/item?id=... — single item by id passed as a QUERY
	// parameter rather than a path segment, so an id containing '/', '|',
	// spaces, or non-ASCII characters round-trips correctly regardless of
	// what a proxy in front of this binary does with an encoded slash in a
	// path (see GET /v1/memory/{id}'s comment above). Registered as a
	// literal path segment — like "summary"/"events" above, "item" wins over
	// the {id} wildcard for this exact segment regardless of registration
	// order (Go 1.22 ServeMux resolves the more specific pattern), so an
	// actual memory item whose id happens to be the literal string "item" is
	// unreachable via the {id} route — the same accepted precedent as those
	// other literal segments.
	huma.Register(s.api, huma.Operation{
		OperationID: "get-memory-item-by-query",
		Method:      http.MethodGet,
		Path:        "/v1/memory/item",
		Summary:     "Get a single memory item by ID passed as a query parameter — use for ids containing '/'",
	}, func(_ context.Context, in *memoryGetItemInput) (*memoryGetOutput, error) {
		return s.getMemoryItemByID(in.ID)
	})

	// GET /v1/memory/neighbors?id=... — the centre cell's semantic neighbourhood:
	// its nearest cells by embedding and the links among them that mark
	// sub-clusters, for a graph view. The centre keeps getMemoryItemByID's 404
	// posture (unknown and private are indistinguishable). Registered as a
	// literal segment, so it wins over {id} like "item" and "summary" do.
	huma.Register(s.api, huma.Operation{
		OperationID: "get-memory-neighbors",
		Method:      http.MethodGet,
		Path:        "/v1/memory/neighbors",
		Summary:     "Semantic neighbourhood of a memory item",
	}, func(_ context.Context, in *memoryNeighborsInput) (*memoryNeighborsOutput, error) {
		if _, err := s.getMemoryItemByID(in.ID); err != nil {
			return nil, err
		}
		return &memoryNeighborsOutput{Body: s.memory.Neighborhood(in.ID, in.Top)}, nil
	})

	// Image endpoint — serves base64-encoded image stored in a memory item.
	// Must be registered before /v1/memory/events to avoid prefix ambiguity.
	s.mux.HandleFunc("GET /v1/memory/{id}/image", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		it := s.memory.GetItem(id)
		if it == nil {
			http.NotFound(w, r)
			return
		}
		img, ok := it.Data["image"]
		if !ok || img.Bytes == "" {
			http.NotFound(w, r)
			return
		}
		raw, err := base64.StdEncoding.DecodeString(img.Bytes)
		if err != nil {
			http.Error(w, "invalid image data", http.StatusInternalServerError)
			return
		}
		// The stored MIME is attacker-controlled (via upsert_memory). Allowlist
		// it to raster types and always send nosniff so a stored text/html or
		// image/svg+xml payload can't execute as same-origin script (stored XSS).
		mime := allowedImageMime(img.Mime)
		if mime == "" {
			http.Error(w, "unsupported image type", http.StatusUnsupportedMediaType)
			return
		}
		w.Header().Set("Content-Type", mime)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Disposition", "inline")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		w.WriteHeader(http.StatusOK)
		w.Write(raw) //nolint:errcheck
	})

	// SSE stream: broadcasts MemoryHitEvents when voice calls search_memory.
	// Registered outside Huma so we control the raw response writer.
	// Must use "GET " prefix to avoid conflicting with GET /v1/memory/{id}.
	s.mux.HandleFunc("GET /v1/memory/events", func(w http.ResponseWriter, r *http.Request) {
		// Resolve the caller so private events (personal notes) are delivered to
		// this user's channel only.
		owner, _ := s.noteOwner(r)
		serveMemoryEvents(s.memory.hub, w, r, owner)
	})
}

// getMemoryItemByID is the shared lookup behind both GET /v1/memory/{id} and
// GET /v1/memory/item?id=... — same 404 semantics either way (missing item,
// or a private cell, which is served only via /v1/user/notes/{id} with an
// ownership check).
func (s *Server) getMemoryItemByID(id string) (*memoryGetOutput, error) {
	it := s.memory.Get(id)
	if it == nil {
		return nil, huma.Error404NotFound("memory item not found")
	}
	if isPrivateItem(it.Kind, it.Scope) {
		return nil, huma.Error404NotFound("memory item not found")
	}
	if _, ok := it.Data["image"]; ok {
		it.ImageURL = "/v1/memory/" + it.ID + "/image"
	}
	return &memoryGetOutput{Body: it}, nil
}

// ─── misc ─────────────────────────────────────────────────────────────────────

// callerIdentity resolves the caller from the xray_session cookie or a Bearer
// token. Returns empty strings when unauthenticated. userID is the raw,
// provider-scoped SSO ID (pre-hash).
// authDisabled reports that no sign-in provider is configured (--dev).
func (s *Server) authDisabled() bool {
	return s.github.cfg.ClientID == "" && s.azure.cfg.ClientID == "" && s.google.cfg.ClientID == ""
}

// devUser is the built-in user of a server with sign-in off. It is a real
// session, so voice, personal notes, the live event stream and the UI all see
// one stable identity instead of a missing one.
const (
	devUsername = "dev"
	devUserID   = "dev"
)

// withDevUser gives every request that carries no valid session the dev user's,
// when sign-in is off. Behind a sign-in provider it does nothing.
func (s *Server) withDevUser(next http.Handler) http.Handler {
	if !s.authDisabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, _, ok := sessionFromRequest(r); !ok {
			r.Header.Set("Authorization", "Bearer "+s.devSession())
		}
		next.ServeHTTP(w, r)
	})
}

// devSession returns a live session token for the dev user, creating one when
// the last has expired.
func (s *Server) devSession() string {
	s.devMu.Lock()
	defer s.devMu.Unlock()
	if s.devToken != "" {
		if _, _, ok := sessions.validate(s.devToken); ok {
			return s.devToken
		}
	}
	s.devToken = sessions.create(devUsername, devUserID)
	return s.devToken
}

// noteOwner returns the subject hash the caller's private notes are stored
// under. ok is false for a caller with no session.
func (s *Server) noteOwner(r *http.Request) (hash string, ok bool) {
	if _, userID := s.callerIdentity(r); userID != "" {
		return stableUserID(userID), true
	}
	return "", false
}

func (s *Server) callerIdentity(r *http.Request) (username, userID string) {
	if c, err := r.Cookie("xray_session"); err == nil {
		username, userID, _ = sessions.validate(c.Value)
	}
	if username == "" {
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			username, userID, _ = sessions.validate(auth[7:])
		}
	}
	return username, userID
}

func (s *Server) registerMisc() {
	s.mux.HandleFunc("GET /v1/memory/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		m := map[string]any{
			"search": s.memory.Metrics(),
			"embed":  s.memory.EmbedMetrics(),
			"items":  s.memory.Count(),
			"tools":  s.memory.ToolMetrics(),
		}
		if s.voice != nil {
			m["voice"] = s.voice.VoiceMetrics()
			// Per-user daily token quota for the authenticated caller, if any.
			// limit > 0 with ok=false means a cap exists but the caller is
			// unauthenticated; limit == 0 means unlimited — the UI shows that.
			if _, rawUserID := s.callerIdentity(r); rawUserID != "" {
				used, limit, remaining, ok := s.voice.UserQuota(rawUserID)
				if ok {
					m["quota"] = map[string]any{"used": used, "limit": limit, "remaining": remaining}
				} else if limit == 0 {
					m["quota"] = map[string]any{"limit": 0} // unlimited
				}
			}
		}
		json.NewEncoder(w).Encode(m) //nolint:errcheck
	})

	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) //nolint:errcheck
	})

	// /v1/version — unauthenticated, so it exposes only the human-readable
	// version string the UI displays. Build commit/date and the OAuth
	// org/team/tenant fields are recon data and are deliberately omitted here
	// (they remain available to authenticated tooling via other surfaces).
	s.mux.HandleFunc("/v1/version", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"version": version.Version,
		})
	})

	// /v1/categories — unauthenticated (the Voronoi graph loads pre-login),
	// serves the per-deployment UI category taxonomy from the
	// system_configuration cell (ADR-035 item 4). Empty list means the UI
	// falls back to its in-repo default taxonomy.
	s.mux.HandleFunc("/v1/categories", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var categories []CategoryConfig
		if s.config != nil {
			categories = s.config.Get().Categories
		}
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"categories": categories,
		})
	})

	// PWA manifest — served dynamically.
	base := s.basePath
	s.mux.HandleFunc("/manifest.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/manifest+json")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"id":               base + "/",
			"name":             "X-Ray",
			"short_name":       "X-Ray",
			"description":      "X-Ray memory runtime",
			"lang":             "en",
			"dir":              "ltr",
			"start_url":        base + "/",
			"scope":            base + "/",
			"display":          "standalone",
			"background_color": "#000000",
			"theme_color":      "#000000",
			"orientation":      "portrait-primary",
			"categories":       []string{"productivity", "utilities"},
			"icons": []map[string]string{
				// Raster PNGs satisfy the Chrome/Android installability check (SVG is rejected there).
				{"src": base + "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
				{"src": base + "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"},
				{"src": base + "/icon-maskable-512.png", "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
				{"src": base + "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any"},
			},
		})
	})

	// Service worker — minimal shell cache
	baseJS, _ := json.Marshal(base) // a JSON string is a JS string literal
	sw := []byte("const BASE = " + string(baseJS) + ";\n" + xraySW)
	s.mux.HandleFunc("/sw.js", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/javascript")
		w.Header().Set("Cache-Control", "no-cache")
		w.Write(sw) //nolint:errcheck
	})
}

const xraySW = `const CACHE = "xray-shell-v1";
const SHELL = [BASE + "/", BASE + "/icon.svg", BASE + "/manifest.json"];

self.addEventListener("install", e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener("activate", e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", e => {
  if (e.request.method !== "GET") return;
  const url = new URL(e.request.url);
  if (url.pathname.startsWith(BASE + "/v1/")) return;
  e.respondWith(
    fetch(e.request).catch(() => caches.match(e.request).then(r => r || caches.match(BASE + "/")))
  );
});
`
