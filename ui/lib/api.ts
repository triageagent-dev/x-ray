// API client — all fetch calls go through here so the base URL is consistent.

export interface MemoryItem {
  id: string
  kind: string
  scope: string
  subject?: string
  title: string
  content: string
  confidence?: string
  tags?: string[]
  status: string
  eventDate?: string  // RFC3339, only set for kind=event
  imageUrl?: string   // set client-side when imageData is present
  createdAt: string
  updatedAt: string
}

export interface MemoryItemSummary {
  id: string
  kind: string
  scope: string
  subject?: string
  title: string
  tags?: string[]
  status: string
  updatedAt: string
  hitCount: number
}

export interface MemoryHitEvent {
  itemIds: string[]
  query: string
  kind?: string
  // Per-item kind, parallel to itemIds (same order, same length) — added so
  // the graph can resolve a hit's UI category even when the cell itself
  // isn't currently rendered on the bounded per-category preview (see
  // memory-graph.tsx's region-flash effect). Additive: an older server that
  // never sends this field leaves it undefined, and hitKindMap below
  // degrades that to "no per-item kind info" rather than throwing.
  kinds?: string[]
  // Per-item tags, parallel to itemIds (null for a private item), so the
  // graph can place a hit in a category a deployment split off its kind by
  // tag (ADR-035). Absent from an older server; hitTagMap degrades to kind.
  tags?: (string[] | null)[]
}

/**
 * Maps each hit item ID to its own kind, given a HitEvent's parallel `kinds`
 * array. Returns undefined — not a partial map — when `kinds` is absent or
 * shorter than `itemIds` (an older server, or a caller that didn't fill it):
 * callers must treat undefined as "no per-item kind info available" and fall
 * back to cell-only behavior, never index past the end of a short array.
 */
export function hitKindMap(evt: MemoryHitEvent | null | undefined): Map<string, string> | undefined {
  if (!evt?.itemIds?.length || !evt.kinds || evt.kinds.length < evt.itemIds.length) return undefined
  const m = new Map<string, string>()
  evt.itemIds.forEach((id, i) => m.set(id, evt.kinds![i]))
  return m
}

/**
 * Maps each hit item ID to its tags, given a HitEvent's parallel `tags` array.
 * Undefined when `tags` is absent or shorter than `itemIds` (an older server):
 * callers then place hits by kind alone.
 */
export function hitTagMap(evt: MemoryHitEvent | null | undefined): Map<string, string[]> | undefined {
  if (!evt?.itemIds?.length || !evt.tags || evt.tags.length < evt.itemIds.length) return undefined
  const m = new Map<string, string[]>()
  evt.itemIds.forEach((id, i) => m.set(id, evt.tags![i] ?? []))
  return m
}

const base = process.env.NEXT_PUBLIC_API_URL ?? ""

// Version prefix of the API surface. X-Ray's own server serves it at /v1; the
// Triage deployment of the same API serves the identical surface at /v2 — same
// paths, same shapes, same auth, just a different prefix. Everything that
// builds a URL goes through `api` below so the two can never drift apart.
//
// Build-time, not runtime: the export is static, so the value is inlined by
// Next.js and the image is bound to it. `make image API_PREFIX=/v2` sets both
// this and the server's own API_PREFIX, which must agree or its proxy forwards
// paths the API does not answer.
export const API_PREFIX = process.env.NEXT_PUBLIC_API_PREFIX || "/v1"

// Base for every API URL: the origin (empty in the deployed same-origin case)
// plus the version prefix.
export const api = `${base}${API_PREFIX}`

// Where each capability actually lives on the surface behind API_PREFIX.
//
// The two deployments do not merely differ by prefix. v2 moved voice under
// /agent, replaced the SSE event stream with a WebSocket — the SSE route was
// removed 2026-07-31 because the server's 30s WriteTimeout bounds an entire
// streamed response and killed it every ~31s regardless of heartbeats — and
// dropped per-user notes entirely.
//
// Verified against the live v2 deployment by probing, not from its OpenAPI
// document: everything registered outside Huma is absent from that document,
// and an unauthenticated probe cannot tell a missing route from a gated one
// because the auth middleware runs BEFORE routing. 404 vs 400/401 with a valid
// token is the only reliable signal.
export const endpoints = API_PREFIX === "/v1"
  ? {
      voice: `${API_PREFIX}/voice`,
      events: { transport: "sse" as const, path: `${API_PREFIX}/memory/events` },
      userNotes: true,
      tenants: false,
    }
  : {
      voice: `${API_PREFIX}/agent/voice/session`,
      events: { transport: "ws" as const, path: `${API_PREFIX}/memory/ws` },
      userNotes: false,
      tenants: true,
    }

// ── Tenant ───────────────────────────────────────────────────────────────
//
// The API is multi-tenant and every /memory route takes ?tenant=. With the
// parameter ABSENT it does not fall back to a default — it returns every
// tenant the caller may see, blended into one response. That is why a missing
// tenant looks like working software instead of an error: the graph fills, the
// counts are plausible, and nothing says they span 18 tenants at once.
//
// Held here rather than threaded through each call site, for the same reason
// credentials are: apiFetch is the one choke point every request passes
// through, so a caller added later cannot forget it.
let currentTenant = ""
// The deployment's default tenant, from GET /agent/tenants. Only consulted by
// itemVisibleToTenant, for the one case where an item carries no tenant tag
// but its kind belongs to a tenant anyway.
let defaultTenant = ""

export function setApiTenant(t: string) { currentTenant = t }
export function getApiTenant(): string { return currentTenant }
export function setApiDefaultTenant(t: string) { defaultTenant = t }

const TENANT_TAG_PREFIX = "tenant/"

// Kinds whose UNTAGGED items are still one tenant's own data — a real
// incident, a raw event, a per-service counter — and so belong to the
// deployment's default tenant rather than to everyone. Mirrors the server's
// tenantScopedDataKinds; every other untagged kind is a genuinely global cell.
const TENANT_SCOPED_DATA_KINDS = new Set([
  "active_concern", "event", "analytics_counter", "precursor_pattern", "incident_claim",
])

/**
 * Whether an item belongs in the selected tenant's view.
 *
 * The live event stream is the one read path apiFetch does not scope: the
 * server filters its frames to the caller's GRANTS, not to a selected tenant,
 * so a user entitled to several tenants receives every one of their upserts
 * on a socket that knows nothing about the switcher. Without this, a
 * tenant-scoped list re-fills with other tenants' items within a minute of
 * sitting open — the exact blending ?tenant= exists to prevent, back through
 * the side door.
 *
 * The tenant rides on the item itself, as a `tenant/<name>` tag, and this
 * applies the same rule the server applies to a ?tenant= read: a tagged item
 * is visible to its tag's tenant only; an untagged item is global unless its
 * kind is structurally one tenant's data, in which case it belongs to the
 * default tenant. No tenant selected means no narrowing, which is also what
 * the fetch path does: an absent ?tenant= returns everything the grant allows.
 */
export function itemVisibleToTenant(
  item: Pick<MemoryItem, "kind" | "tags">,
  tenant: string = currentTenant,
  deploymentDefault: string = defaultTenant,
): boolean {
  if (!tenant) return true
  const tag = item.tags?.find((t) => t.startsWith(TENANT_TAG_PREFIX))
  if (tag) return tag.slice(TENANT_TAG_PREFIX.length) === tenant
  if (TENANT_SCOPED_DATA_KINDS.has(item.kind)) return !!deploymentDefault && deploymentDefault === tenant
  return true
}

/**
 * Scope a request to the selected tenant.
 *
 * Only /memory* takes the parameter — auth, categories and the agent routes
 * do not — so this does not blanket-append. An explicit tenant already in the
 * URL wins, which is what lets a caller ask about a tenant that is not the
 * selected one.
 */
function withTenant(input: string): string {
  if (!currentTenant) return input
  try {
    const origin = typeof window !== "undefined" ? window.location.origin : "http://localhost"
    const u = new URL(input, origin)
    if (!u.pathname.startsWith(`${API_PREFIX}/memory`)) return input
    if (u.searchParams.has("tenant")) return input
    u.searchParams.set("tenant", currentTenant)
    return /^https?:/i.test(input) ? u.toString() : u.pathname + u.search
  } catch {
    return input // not a URL we can reason about; send it untouched
  }
}

/**
 * Tenants this caller may view, and the deployment's default.
 *
 * The controller filters this to the caller's own scope, so a bound user gets
 * a single entry and the client never has to filter the list itself.
 */
export async function fetchTenants(): Promise<{ tenants: string[]; default: string }> {
  // X-Ray's own server has no tenants; asking would log a 404 on every load.
  if (!endpoints.tenants) return { tenants: [], default: "" }
  const r = await apiFetch(`${api}/agent/tenants`)
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  const d = await r.json()
  return { tenants: d.tenants ?? [], default: d.default ?? "" }
}

// Credential for a WebSocket upgrade. A browser cannot set headers on one, so
// a bearer token travels as a subprotocol; the literal "bearer" marker is
// echoed back by the server per subprotocol negotiation and is not itself a
// credential. Undefined is the same-origin cookie case, which needs nothing.
export function wsProtocols(): string[] | undefined {
  const token = storedToken()
  return token ? ["bearer", token] : undefined
}

/**
 * sessionStorage key holding a session token, for the case where the
 * `xray_session` cookie cannot reach us.
 *
 * The cookie is the normal credential and needs none of this: it is set
 * `Secure`, `HttpOnly` and host-only, so it works whenever the page and the API
 * share an origin — which the deployed setup guarantees, because the UI server
 * proxies `/v1` rather than sending the browser elsewhere.
 *
 * It does NOT work in two real cases: a UI on another host (`npm run dev`
 * against a remote API), and a plain-http localhost origin, where `Secure`
 * stops the cookie being stored at all. A token obtained from the login
 * hand-off (see app/auth/callback) covers both.
 *
 * sessionStorage, not localStorage: the credential dies with the tab rather
 * than persisting on disk for every later visit.
 */
export const SESSION_TOKEN_KEY = "xray.token"

export function storedToken(): string | null {
  if (typeof window === "undefined") return null
  try {
    return sessionStorage.getItem(SESSION_TOKEN_KEY)
  } catch {
    return null // storage blocked (private mode / embedded)
  }
}

/**
 * sessionStorage key marking a login this TAB started.
 *
 * Without it, /auth/callback would accept any `?token=` a URL happened to
 * carry, so a single link — `/auth/callback?token=<attacker's session>` —
 * would silently sign a victim into the ATTACKER's account, and everything
 * they wrote afterwards would land in the attacker's store. That is login CSRF
 * (session fixation), and the token being valid is exactly what makes it work.
 *
 * The marker closes it because sessionStorage is scoped per origin AND per
 * tab: an attacker's page cannot write this key into the victim's tab, so a
 * token arriving where no login began is rejected. It is also carried to the
 * provider as `state`; if the API echoes it back, the callback demands an
 * exact match, which additionally pins the token to THIS login rather than
 * merely to this tab.
 */
export const LOGIN_STATE_KEY = "xray.login"

/** How long a started login stays redeemable. */
const LOGIN_WINDOW_MS = 10 * 60_000

function randomState(): string {
  try {
    const b = new Uint8Array(16)
    crypto.getRandomValues(b)
    return Array.from(b, x => x.toString(16).padStart(2, "0")).join("")
  } catch {
    // A non-secure context has no crypto.getRandomValues. The marker's
    // security comes from sessionStorage's origin+tab scoping, not from this
    // value being unguessable, so a weaker one still closes the hole.
    return `${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`
  }
}

/**
 * Records that a login is starting in this tab and returns the provider URL to
 * navigate to. Call it from the click handler, never on render.
 */
export function beginLogin(provider: string): string {
  const state = randomState()
  try {
    sessionStorage.setItem(LOGIN_STATE_KEY, JSON.stringify({ state, at: Date.now() }))
  } catch {
    // Storage blocked: the callback will reject the token rather than accept
    // one it cannot bind. The same-origin cookie flow is unaffected.
  }
  const url = new URL(`${api}/auth/${provider}`, window.location.origin)
  url.searchParams.set("state", state)
  // Only meaningful cross-origin: it asks the API to hand the session back to
  // this UI. Same-origin, the cookie is the credential and this is inert.
  if (base) url.searchParams.set("ui", window.location.origin)
  return url.toString()
}

/**
 * Redeems the pending-login marker. Returns true only when this tab started a
 * login recently and — if the callback carried a `state` — it matches.
 * Single-use: the marker is cleared whether or not it validated.
 */
export function consumeLogin(returnedState: string | null): boolean {
  let raw: string | null = null
  try {
    raw = sessionStorage.getItem(LOGIN_STATE_KEY)
    sessionStorage.removeItem(LOGIN_STATE_KEY)
  } catch {
    return false
  }
  if (!raw) return false
  try {
    const { state, at } = JSON.parse(raw) as { state?: string; at?: number }
    if (typeof at !== "number" || Date.now() - at > LOGIN_WINDOW_MS) return false
    if (returnedState !== null && returnedState !== state) return false
    return true
  } catch {
    return false
  }
}

/**
 * fetch() with the session credential attached, whichever form it takes.
 *
 * Every API call goes through this so the two carriers can never drift apart:
 * `credentials: "include"` for the same-origin cookie, plus an
 * `Authorization: Bearer` header when a token is stored. Sending both is
 * harmless — the server checks the cookie first and falls through to the
 * header only when it is absent or expired.
 */
export async function apiFetch(input: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers)
  const token = storedToken()
  if (token) headers.set("Authorization", `Bearer ${token}`)
  const res = await fetch(withTenant(input), { credentials: "include", redirect: "manual", ...init, headers })

  // X-Ray answers an unauthenticated API call with a 302 to its login page
  // rather than a 401. Followed, that redirect ends on an HTML document and
  // every caller here explodes on `.json()` with "Unexpected token '<'" —
  // burying the one fact that actually matters, which is that there is no
  // session. redirect:"manual" makes the browser surface it instead as an
  // opaque response (type "opaqueredirect", status 0, no readable body), so
  // translate that into the 401 the callers and the auth gate in
  // xray-layout-client.tsx already know how to act on.
  if (res.type === "opaqueredirect") {
    return new Response(JSON.stringify({ error: "unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    })
  }
  return res
}

export async function fetchMemory(
  params: { kind?: string; scope?: string; subject?: string; limit?: number } = {}
): Promise<MemoryItem[]> {
  const qs = new URLSearchParams()
  if (params.kind) qs.set("kind", params.kind)
  if (params.scope) qs.set("scope", params.scope)
  if (params.subject) qs.set("subject", params.subject)
  if (params.limit) qs.set("limit", String(params.limit))
  const url = `${api}/memory${qs.toString() ? "?" + qs : ""}`
  const r = await apiFetch(url)
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  const d = await r.json()
  return (d.items ?? []).map(normalizeImage)
}

// normalizeImage ensures imageUrl is set. Server sets imageUrl to /v1/memory/{id}/image
// when Data["image"] is present. For legacy items that still have the thumbnail
// embedded in content, extract it and clear the prefix.
function normalizeImage(item: MemoryItem): MemoryItem {
  if (item.imageUrl) return item
  const m = item.content.match(/^!\[thumbnail\]\((data:[^)]+)\)\n\n/)
  if (m) {
    item.imageUrl = m[1]
    item.content = item.content.slice(m[0].length)
  }
  return item
}

export async function fetchMemorySummary(
  params: { kind?: string; scope?: string; subject?: string; top?: number; perKind?: number } = {}
): Promise<{ items: MemoryItemSummary[]; total: number; totalsByKind: Record<string, number>; totalsByCategory: Record<string, number> }> {
  const qs = new URLSearchParams()
  if (params.kind) qs.set("kind", params.kind)
  if (params.scope) qs.set("scope", params.scope)
  if (params.subject) qs.set("subject", params.subject)
  if (params.top) qs.set("top", String(params.top))
  if (params.perKind) qs.set("perKind", String(params.perKind))
  const url = `${api}/memory/summary${qs.toString() ? "?" + qs : ""}`
  const r = await apiFetch(url)
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  const d = await r.json()
  return { items: d.items ?? [], total: d.total ?? 0, totalsByKind: d.totalsByKind ?? {}, totalsByCategory: d.totalsByCategory ?? {} }
}

export async function fetchMemorySearch(q: string, limit = 50, perKind?: number, kind?: string): Promise<MemoryItem[]> {
  const qs = new URLSearchParams({ q, limit: String(limit) })
  if (perKind) qs.set("perKind", String(perKind))
  if (kind) qs.set("kind", kind)
  const r = await apiFetch(`${api}/memory?${qs}`)
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  const d = await r.json()
  return (d.items ?? []).map(normalizeImage)
}

export interface RuntimeMetrics {
  items: number
  voice?: { totalCalls: number; activeCalls: number }
  search?: { totalSearches: number; keywordHits: number; kwVoiceHits: number; kwVoiceMisses: number; kwTrigramHits: number; semanticCalls: number; semanticFallbacks: number }
  embed?: { cacheHits: number; embedCalls: number }
  tools?: Record<string, { calls: number; avg_us: number }>
  quota?: { used?: number; limit: number; remaining?: number }
  // v2 reports these and v1 does not. Optional rather than versioned so the
  // HUD renders whatever the deployment actually returns.
  llmBudget?: { tokensToday: number; limit: number }
  llm?: { calls: number; agentCalls: number; promptTokens: number; completionTokens: number }
  classifier?: { signal: number; noise: number; incident: number; unknown: number; embeddingCalls: number }
  guardrail?: { infraPassed: number; noiseDropped: number }
  rag?: { retrievals: number; hits: number }
  semanticCache?: { hits: number }
}

// v2 renamed every metrics container and one field inside them, so a HUD
// written against v1's names silently rendered nothing but the item count —
// each `if (m.search)` was testing a key that no longer existed. Reconstruct
// the shape here, in the one place that already owns the wire format, rather
// than teaching the component two vocabularies.
//
// Nothing is invented and nothing is conflated: `memorySearchStats` carries
// exactly the fields `search` did (only `total` was renamed from
// `totalSearches`), and v2's extra containers are surfaced under their own
// names instead of being folded into `embed`, which measures something else.
function normalizeMetrics(d: Record<string, unknown>): RuntimeMetrics {
  const s = d.memorySearchStats as Record<string, number> | undefined
  if (!s) return d as unknown as RuntimeMetrics // v1 already speaks this shape

  return {
    items: (d.items as number) ?? 0,
    voice: d.voice as RuntimeMetrics["voice"],
    search: {
      totalSearches: s.total ?? 0,
      keywordHits: s.keywordHits ?? 0,
      kwTrigramHits: s.kwTrigramHits ?? 0,
      kwVoiceHits: s.kwVoiceHits ?? 0,
      kwVoiceMisses: s.kwVoiceMisses ?? 0,
      semanticCalls: s.semanticCalls ?? 0,
      semanticFallbacks: s.semanticFallbacks ?? 0,
    },
    llmBudget: d.llmBudget as RuntimeMetrics["llmBudget"],
    llm: d.llmStats as RuntimeMetrics["llm"],
    classifier: d.classifierStats as RuntimeMetrics["classifier"],
    guardrail: d.guardrailStats as RuntimeMetrics["guardrail"],
    rag: d.ragStats as RuntimeMetrics["rag"],
    semanticCache: d.semanticCacheStats as RuntimeMetrics["semanticCache"],
  }
}

export async function fetchMetrics(): Promise<RuntimeMetrics> {
  const r = await apiFetch(`${api}/memory/metrics`)
  if (!r.ok) throw new Error(`HTTP ${r.status}`)
  return normalizeMetrics(await r.json())
}

// A `note` may be public (shared knowledge, served by /v1/memory/item) or private
// (personal, served only by the ownership-checked /v1/user/notes/{id}). The caller's
// `kind` hint doesn't distinguish scope, so try the shared endpoint first and fall
// back to the personal one on 404 — covers both note scopes and notes opened from
// the graph without kind context.
//
// Uses GET /v1/memory/item?id=... rather than interpolating id into the path
// (/v1/memory/{id}) — some memory kinds (e.g. a claim/incident id that embeds
// a source path) contain a literal '/', which splits the request into more
// path segments than the {id} route expects and 404s. A '/' inside a query
// VALUE is never treated as a path separator by any correctly-implemented URL
// parser or proxy, so the query form round-trips any id.
export async function fetchMemoryItem(id: string, _kind?: string): Promise<MemoryItem> {
  const qs = new URLSearchParams({ id })
  const r = await apiFetch(`${api}/memory/item?${qs}`)
  if (r.ok) return normalizeImage(await r.json())
  if (r.status === 404) {
    // encodeURIComponent, not raw: an id carrying '/', '?' or '#' would
    // otherwise re-target this request at a different endpoint — with the
    // caller's credentials attached.
    const r2 = await apiFetch(`${api}/user/notes/${encodeURIComponent(id)}`)
    if (r2.ok) return normalizeImage(await r2.json())
  }
  throw new Error(`HTTP ${r.status}`)
}

export async function fetchUserNotes(): Promise<MemoryItem[]> {
  // Absent on v2. Several components call this on mount, so without the guard
  // each one logs its own 404 for a route that is never coming back.
  if (!endpoints.userNotes) return []
  const r = await apiFetch(`${api}/user/notes`)
  if (!r.ok) return []
  const d = await r.json()
  return (d.items ?? []).map(normalizeImage)
}

export interface MemoryChangeEvent {
  op: "upsert" | "delete"
  item?: MemoryItem
  id?: string
}

// One shared connection fans out to all subscribers. GCLB drops the stream at
// its 40s backend timeout, so sharing means one reconnect (one console error)
// per cycle instead of one per subscriber.
type MemorySubscriber = {
  onHit: (evt: MemoryHitEvent) => void
  onChange?: (evt: MemoryChangeEvent) => void
}

let sharedSource: EventSource | WebSocket | null = null
let reconnectTimer: ReturnType<typeof setTimeout> | null = null
let reconnectDelay = 0
const subscribers = new Set<MemorySubscriber>()

const RECONNECT_MIN_MS = 1_000
const RECONNECT_MAX_MS = 30_000

// Fan one decoded event payload out to every subscriber.
//
// Upserts are gated on the selected tenant here, at the one point every
// subscriber shares, for the same reason ?tenant= is added inside apiFetch:
// a consumer added later cannot forget it. Deletes carry only an id and pass
// through — an id the view never held is a no-op — and hits name ids, kinds
// and tags with nothing to filter on; a hit for a cell the graph does not show
// matches nothing.
function fanOut(data: { op?: string; item?: MemoryItem } & Record<string, unknown>) {
  if (data.op) {
    if (data.op === "upsert" && data.item) {
      if (!itemVisibleToTenant(data.item)) return
      data.item = normalizeImage(data.item)
    }
    subscribers.forEach((s) => s.onChange?.(data as MemoryChangeEvent))
  } else {
    subscribers.forEach((s) => s.onHit(data as unknown as MemoryHitEvent))
  }
}

function openMemoryStream() {
  if (endpoints.events.transport === "sse") {
    // EventSource reconnects on its own, so it needs none of the scheduling below.
    const es = new EventSource(`${base}${endpoints.events.path}`)
    es.onmessage = (e) => {
      try { fanOut(JSON.parse(e.data)) } catch { /* ignore malformed */ }
    }
    sharedSource = es
    return
  }

  const proto = window.location.protocol === "https:" ? "wss:" : "ws:"
  const ws = new WebSocket(`${proto}//${window.location.host}${endpoints.events.path}`, wsProtocols())
  sharedSource = ws

  ws.onopen = () => { reconnectDelay = 0 }
  ws.onmessage = (e) => {
    try {
      // v2 envelope: {"type": "hit"|"change"|"metrics", "data": ...}. The
      // explicit discriminator replaces the old shape-sniffing, and `metrics`
      // is a third frame type SSE had no way to carry — ignored here, the HUD
      // polls /memory/metrics for it.
      const msg = JSON.parse(e.data)
      if (msg.type === "hit" || msg.type === "change") fanOut(msg.data)
    } catch { /* ignore malformed */ }
  }
  // A WebSocket does not reconnect itself the way EventSource does, and this
  // connection is dropped on a timer by design, so reconnecting is not an
  // error path — it is the steady state. Back off so a backend that is down
  // is not hammered once a second by every open tab.
  ws.onclose = () => {
    if (sharedSource !== ws) return // superseded by an unsubscribe
    sharedSource = null
    if (subscribers.size === 0) return
    reconnectDelay = reconnectDelay ? Math.min(reconnectDelay * 2, RECONNECT_MAX_MS) : RECONNECT_MIN_MS
    reconnectTimer = setTimeout(() => { reconnectTimer = null; if (subscribers.size) openMemoryStream() }, reconnectDelay)
  }
}

export function subscribeMemoryEvents(
  onHit: (evt: MemoryHitEvent) => void,
  onChange?: (evt: MemoryChangeEvent) => void,
): () => void {
  const sub: MemorySubscriber = { onHit, onChange }
  subscribers.add(sub)

  if (!sharedSource && !reconnectTimer) openMemoryStream()

  return () => {
    subscribers.delete(sub)
    if (subscribers.size === 0) {
      if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null }
      reconnectDelay = 0
      sharedSource?.close()
      sharedSource = null
    }
  }
}
