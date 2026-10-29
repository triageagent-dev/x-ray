# Application Passport — X-Ray

## 1. Identity

| Field | Value |
|---|---|
| Component | X-Ray (`xray`) |
| Description | Agentic memory substrate for local and Kubernetes use: typed memory as `MemoryCell` custom resources, HNSW + keyword search, a code graph, an embedded web UI, MCP server, REST API, SSE stream and a Gemini Live voice WebSocket |
| Repo | https://github.com/triageagent-dev/x-ray (public) |
| Registry | ghcr.io/triageagent-dev/xray |
| Version | v2.4.0 |
| Go module | github.com/triageagent-dev/x-ray/v2 |
| Helm chart | charts/xray, version 2.0.0, appVersion v2.0.0 |
| CRD | xray.triageagent.dev/v1 `MemoryCell` (short name `mcell`, namespaced) |
| Owner | triageagent-dev (GitHub organization) |
| License | Apache-2.0 |
| Passport date | 2026-10-29 |
| Audit status | No CRITICAL/HIGH CVEs; no HIGH code findings open (v2.4.0); MEDIUM and LOW code findings open |
| Code review | Findings open, none HIGH (2026-10-29) |

## 2. Classification

| Field | Value |
|---|---|
| Class | Service (HTTP server with an in-process Kubernetes controller for `MemoryCell`) |
| Type | Memory service + MCP server + voice agent (uses models) |
| Capabilities | Memory read/write/search (keyword, HNSW semantic), REST API, SSE live events, MCP tools, Gemini Live voice with tool calls, OAuth 2.1 authorization server for MCP (optional), code-graph read tools from mental maps, mental-map OCI pull, `xray graph` CLI (CGO build only) |
| Model role | Voice conversation model with function calling (Gemini Live); embedding model for retrieval |
| Runtime | Kubernetes Deployment (1 replica default), or a local process |
| Public accessibility | Service type ClusterIP; HTTPRoute and Ingress templates disabled by default; MCP on a public host is opt-in (`mcp.exposeOnPublicHost`) |
| Production data access | Read/write: MemoryCell resources and their status in the release namespace; read: all Secrets in the release namespace |
| PII | User display name, hashed SSO identifier (SHA-256 prefix), voice transcripts, private user notes and session memories |
| Sensitivity | Private per-user notes (`scope=private`), session tokens persisted in the system configuration cell status (HMAC-hashed, identity AES-GCM encrypted), OAuth client credentials, MCP bearer token |
| Sign-in providers | GitHub OAuth (`--github-org` required, optional team), Microsoft Entra OAuth (`--azure-tenant-id` required), Google OAuth (`--google-domain` required, checked on the ID token `hd` claim; ID token `iss`, `aud`, `exp` and `email_verified` also checked; user id `google:<sub>`); `--allow-any-user` admits every account of a provider |
| MCP authentication | Static shared bearer secret (Secret named by `--mcp-token-secret`), or OAuth 2.1 with PKCE S256, dynamic client registration, EdDSA JWT access tokens (`--mcp-oauth`); signing key and session HMAC key derived from the OAuth client secrets of the configured providers (GitHub, Entra, Google) |
| MCP scopes | `xray:memory.read`, `xray:memory.write` |
| Dev mode | `--dev` (chart value `dev`, default false): sign-in off, CORS any origin, built-in user `dev`; refused at startup together with a sign-in provider; warns when not listening on loopback |

## 3. Artifact

| Field | Value |
|---|---|
| Form | OCI image (single static binary `xray`) and Helm chart |
| Language | Go 1.27.3; UI in TypeScript (Next.js 16 static export, React 18) |
| Build tool | ko (`.ko.yaml`, CGO_ENABLED=0) for the image; `make build`, `make graph-build` (CGO, Tree-sitter) for local binaries |
| Base image | cgr.dev/chainguard/static:latest (no digest pin) |
| Embedded UI | Next.js export embedded with `go:embed` (internal/webui), served with a per-document script hash CSP |
| Runtime user | Base image default non-root user; no `securityContext` set in the chart |
| Sidecar image | ghcr.io/triageagent-dev/nomic-embed:v1.18.1-4ccc0ff (tag, no digest), native sidecar, model nomic-embed-text-v1.5 Q8_0 GGUF |
| Image pull policy | Always |
| Image signing / attestation | None configured in the workflows |
| CI | GitHub Actions (build.yml, release.yml), workflow permissions `contents: read` plus job-level overrides, actions pinned by major-version tag |
| Go library packages | Module github.com/triageagent-dev/x-ray/v2: `pkg/memory`, `pkg/memstore`, `pkg/memstore/index`, `pkg/memstore/hub`, `pkg/memstore/cellsync`, `pkg/memstore/memtest`, `pkg/codegraph`, `pkg/xray`; private-item owner carried as a `user:<hash>` tag |
| Snapshot format | gzip + gob, optionally wrapped in age (X25519 recipients, chunked ChaCha20-Poly1305) |

## 4. AI / LLM Supply Chain

| Item | Value | Note |
|---|---|---|
| Voice model | gemini-3.8-live (flag `--voice-model`; `system_configuration.voiceModel` overrides) | Gemini Live bidirectional audio over WebSocket |
| Voice backend | Vertex AI (`--gcp-project`, `--voice-location`) or Google AI API (`--gemini-api-key`) | Affective dialog never sent to `gemini-3*` models |
| Embedding model (chart default) | nomic-embed-text via in-pod llama.cpp sidecar, 256 dims | OpenAI-compatible endpoint on localhost:8090 |
| Embedding model (flag default) | gemini-embedding-001 via Vertex AI, 128 dims | Used when `--gcp-project` is set without an endpoint |
| LLM auth (Vertex) | Workload Identity / application default credentials | Chart annotates the ServiceAccount with `iam.gke.io/gcp-service-account` |
| LLM auth (Google AI) | API key from `--gemini-api-key` | Chart value `voice.geminiApiKey` exists and is not rendered into the Deployment |
| Voice tools | get_memory, list_memory, search_memory, save_memory, end_session; fetch_memory, recent_memory, delete_memory (signed-in users); upsert_memory (users in `writeAllowedUsers`); search_graph tools when a mental map is loaded; tools of remote MCP servers listed in `system_configuration` | Allowlist from `voiceConfig.enabledTools` |
| Per-answer / per-session tool-call bound | 6 / 40 (`end_session` and `save_memory` excluded) | internal/xray/voice.go |
| Per-turn output token cap | `--voice-max-output-tokens`, default 8192 | 0 = unlimited |
| Per-user daily audio-token cap | `--user-daily-audio-token-limit` default 500000 (0 = unlimited); `system_configuration.maxUserDailyAudioTokens` overrides when non-zero | Chart default for the CR value: 0 |
| Concurrent voice sessions | `--max-concurrent-voice` default 20 (0 = unlimited); slot reserved before the session starts | |
| Voice session start rate limit | Per user (or client IP), burst 5, refill 1 per 6 s | |
| WebSocket frame cap | 1 MiB | |
| Tool result size budget | `--headroom-content-limit` default 0 (disabled); `maxItemContentChars` in the CR overrides | |
| System prompt source | `system_configuration` MemoryCell (`voiceConfig.systemPrompt`); built-in default when empty | Display name of the caller appended after control-character stripping and 128-character cap |
| Prompt context from memory | Newest 10 private notes and 3 session memories of the caller, 240 characters each, appended to the system prompt | |
| Input/output guardrail classifier | None present | |
| Egress: Vertex AI / Gemini API | Gemini Live WebSocket and embeddings; Workload Identity or API key | |
| Egress: embedding endpoint | `--embedding-endpoint` (chart: http://localhost:8090), no authentication | |
| Egress: remote MCP servers | URLs from `mcp.servers` in `system_configuration`; bearer token from a named Secret | |
| Egress: OTLP | `--otel-endpoint` (gRPC), optional bearer token from Secret; spans carry session id, hashed user id, transcripts and tool arguments (clipped) | Disabled when the flag is empty |

## 5. Privileges

| Scope | Resource | Verbs | Note |
|---|---|---|---|
| Role `<release>-secret-reader` (namespace) | secrets (all in namespace) | get, list, watch | Process caches Secret contents for MCP token, OTLP token and remote MCP token lookup |
| Role `<release>-secret-reader` (namespace) | xray.triageagent.dev memorycells | get, list, watch, create, update, patch, delete | |
| Role `<release>-secret-reader` (namespace) | xray.triageagent.dev memorycells/status | get, update, patch | Session set, OAuth client registrations and usage counters are written to the system configuration cell status |
| ServiceAccount | `<release>` in `.Values.namespace` | | Annotated for a GCP service account via Workload Identity |
| ClusterRole / ClusterRoleBinding | None | | |
| Controller manager cache | Single namespace (`--namespace`) | | Leader election off, metrics and health probe binders off |
| Local process (non-cluster) | Reads age identity file (`--mental-maps-snapshot-key`, `XRAY_SNAPSHOT_KEY`), snapshot directory (`--mental-maps-dir`), kubeconfig when present | | |
| `xray graph serve` | Reads the repository paths it is given; writes snapshots when `-writable` | | Loopback listener by default |

## 6. Network Surface

| Port | Protocol | Exposed by | Purpose | Deployed auth |
|---|---|---|---|---|
| 80 (chart `--http-address=:80`; flag default :8080) | HTTP | Service port 80 | Embedded UI, REST `/v1/*`, SSE `/v1/memory/events`, voice WebSocket `/v1/voice`, `/healthz` | Session cookie or bearer from GitHub/Entra/Google sign-in; public paths: `/v1/auth/*`, `/v1/version`, `/v1/categories`, `/healthz`, `/manifest.json`, `/sw.js`. No auth under `--dev`. Process exits at startup without a sign-in provider, `--public-url` and (with MCP address set) an MCP token secret, unless `--dev` |
| 80 (when `--mcp-oauth`) | HTTP | Service port 80 | `/mcp`, `/authorize`, `/token`, `/register`, `/.well-known/*` | OAuth JWT (audience bound) or static shared secret on `/mcp`; discovery, `/register`, `/token` public; `/authorize` needs a session cookie and `writeAllowedUsers` membership (bypassed under `--dev`) |
| 8083 | HTTP | Service port `mcp` (when `mcp.enabled` and OAuth off) | Standalone MCP server (streamable HTTP) | Static shared bearer secret from the Secret named by `--mcp-token-secret`; fail-closed when empty; per-IP rate limit 5 req/s, burst 20 |
| 9090 | HTTP | Pod annotation `prometheus.io/scrape` | `/metrics` (Prometheus) | None |
| 8090 | HTTP | Pod-local (sidecar) | Embedding endpoint | None |
| Gateway / Ingress | HTTPS | `httpRoute.enabled`, `ingress.enabled` (both false by default); optional Cloud Armor backend policy | Public host routing to port 80; `/mcp` to 8083 only with `mcp.exposeOnPublicHost` or `mcp.hostname` | TLS terminated at gateway; HSTS sent when `X-Forwarded-Proto` is https |
| Response headers | | All responses | `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy`, CSP; HTML documents carry a CSP with `script-src 'self' blob:` plus SHA-256 hashes of their inline scripts | |
| CORS | | `--public-url` origin only; any origin under `--dev` | Credentials not allowed | |
| Request body cap | | `--max-request-bytes` 1 MiB (non-WebSocket routes) | | |
| Cookies | | `xray_session` (HttpOnly, Secure, SameSite=Lax, 24 h), `xray_oauth_state` (HttpOnly, Secure, SameSite=Lax, 10 min) | | |
| Egress hosts | | github.com, api.github.com, login.microsoftonline.com, graph.microsoft.com, oauth2.googleapis.com (Google sign-in token exchange), Vertex AI / Gemini API, OCI registry named by a mental-map `graphRef`, OTLP endpoint, remote MCP server URLs | | |

## 7. Dependency Vulnerabilities

| Ecosystem | Manifest | Components (SBOM) | CRITICAL | HIGH | MEDIUM | LOW | UNKNOWN |
|---|---|---|---|---|---|---|---|
| Go | go.mod | 141 | 0 | 0 | 0 | 0 | 3 |
| npm | ui/package-lock.json | 203 | 0 | 0 | 0 | 0 | 0 |
| Go (nested module) | adapters/adk/go.mod | 140 | 0 | 0 | 0 | 0 | 3 |

| Scan detail | Value |
|---|---|
| SBOM format | CycloneDX 1.6 (syft), `bin/sbom/` |
| Scanner | trivy (vuln scanner), 2026-10-29 |
| UNKNOWN entry | golang.org/x/crypto 0.58.0, GO-2026-5932 (openpgp package); no import of `golang.org/x/crypto/openpgp` in the repository's Go sources |
| UNKNOWN entry | golang.org/x/net 0.61.0, CVE-2026-78661 and CVE-2026-97045 (html package; fixed in 0.62.0); `golang.org/x/net/html` is not in the compiled package set of either Go module |
