# TODO: Memory Search Optimisation + Proper Controller

Tracks all tasks from the keyword-first search / embedding cache / scale work
and the follow-on controller-runtime migration.

---

## Done

- [x] **#1 Export `KeywordSearch` on `MemoryStore`**
  Renamed `keywordSearch` → `KeywordSearch` so voice tools can call it directly.

- [x] **#2 Keyword-first logic in `defSearchMemory`**
  `search_memory` voice tool tries `KeywordSearch` first; falls back to semantic only
  when fewer than 2 keyword hits. Eliminates most Vertex AI calls during conversation.

- [x] **#3 Tests for keyword-first fallback**
  `TestMemoryStore_KeywordSearch_Direct`, `_NoEmbedderNeeded`,
  `_Search_KeywordFirstFallsBackToSemantic`, `_Search_SkipsSemanticWhenKeywordSufficient`
  in `internal/xray/memory_test.go`.

- [x] **#4 `SearchMetrics` tracker on `MemoryStore`**
  Atomic counters: `totalSearches`, `keywordHits`, `semanticCalls`, `semanticFallbacks`.
  Exposed via `Metrics() SearchMetrics`.

- [x] **#5 `GET /v1/memory/metrics` endpoint**
  Returns `{search, embed, items}` JSON. `embed` forwards `EmbedMetrics`
  (`cacheHits`, `semCacheHits`, `embedCalls`) from the embedder.

- [x] **#6 Exact query cache in `Embedder`**
  `map[string][]float32` keyed by `taskType+"\x00"+text`. Short-circuits re-embedding
  identical queries across the session.

- [x] **#7 Semantic query cache in `Embedder`**
  After computing a new vector, scans cached vectors with cosine similarity
  (threshold 0.97 via `github.com/kshard/vector`). Near-duplicate paraphrased queries
  reuse the stored vector without an API call. Stores exact-key → cached-vec mapping
  so subsequent identical calls short-circuit at the exact cache.

- [x] **#8 `GET /v1/memory/summary` endpoint**
  Lightweight index: id/title/kind/scope/subject/tags/status/updatedAt/hitCount — no
  content or imageData. Designed for graph initial load at scale. `top` param, default 200.

- [x] **#9 Demand ranking via Hub hit counts**
  `Hub` tracks per-item SSE hit counts (`hitCounts map[string]*atomic.Int64`).
  `HitCount(id)` exposed; `ListSummary` populates `hitCount` per item so the graph
  can surface most-accessed nodes.

- [x] **#10 `MemoryGraph` loads from `/v1/memory/summary`**
  Initial load uses `fetchMemorySummary({ top: SUMMARY_TOP })` instead of full
  `fetchMemory`. State changed from `MemoryItem[]` to `MemoryItemSummary[]`.
  SSE change handler converts `MemoryItem` → `MemoryItemSummary` on upsert events.

- [x] **#11 Server-side search fallback in UI**
  When client-side filter returns no matches, falls back to `GET /v1/memory?q=`
  (300 ms debounce). Results merged into `serverResults` state and shown in graph.

- [x] **#12 D3 render budget cap**
  `MAX_RENDER_NODES = 300` — `filteredItems` sorted by `hitCount` descending then
  sliced. Badge overlay (`showing X of Y`) appears bottom-right when cap is active.

- [x] **#13 Full-content HNSW indexing**
  `cellEmbedText` embeds `title + ": " + content` (thumbnail prefix stripped) instead
  of title only. Semantic search now matches on full item text.

- [x] **Fix embedding model default**
  `bge-small-en-v1.5` → `text-embedding-005`. `maxInputChars` raised 1200 → 8000
  to match the 2048-token context window of text-embedding-005.

- [x] **`OutputDimensionality` + `TaskType` support in Vertex AI embedder**
  `--embedding-dims` flag (default 256) reduces output from 768 → 256 floats via
  Matryoshka truncation (~95% retrieval quality, 3× smaller CR storage).
  Documents use `RETRIEVAL_DOCUMENT`, queries use `RETRIEVAL_QUERY`.

- [x] **Remove avatar route**
  `/avatar` SPA route, `AvatarDef`, `PinPad`, `auth-context`, `use-pin-auth`,
  `avatar-view` deleted from UI. `avatarDefs` parameter removed from `NewServer`.
  PIN auth will be re-added separately if needed.

---

## Pending

- [x] **#14 Strip stale embeddings from all `MemoryCell` CRs**
  Done 2026-11-05, no manual strip needed: a stored vector carries the fingerprint
  of the text it was made from (`xray.triageagent.dev/embedding-fingerprint`, ADR-005
  amendment), the server re-embeds a cell whose text changed, and re-embeds once every
  cell stored before fingerprints. The steps below are kept as history.

  After deploying the content-embedding change, existing CRs hold title-only vectors
  that are now stale. Patch them to remove `spec.embedding` so the reconciler
  re-embeds with full content on next sync.

  ```bash
  kubectl get memorycell -n xray-xray -o name | \
    xargs -I{} kubectl patch {} -n xray-xray --type=json \
    -p='[{"op":"remove","path":"/spec/embedding"}]'
  ```

  Run once after `helm upgrade` confirms the new binary is live.

---

## Controller migration

- [x] **#15 Wire `CellSyncer` into a controller-runtime `Manager`**
  Replaced hand-wired `cache.New` + raw `AddEventHandler` block in `main.go` with
  `ctrl.NewManager`. `CellSyncer` registered via `SetupWithManager` — gets reconcile
  loop with retry/backoff, handles Add/Update/Delete CR events. Built-in metrics and
  health probe servers disabled (`BindAddress: "0"`); Secret watch and MCP token
  informer now use `mgr.GetCache()`.

- [x] **#16 Register field indexes on `MemoryCell` in cache**
  `registerCellIndexes()` called before `mgr.Start()` registers: `spec.kind`,
  `spec.subject`, `spec.status`, `spec.id`. Enables `MatchingFields` list queries
  against the cache without full-scan.

- [x] **#17 Replace `MemoryStore` read path with cache-backed `List`**
  `SetCacheReader(r, namespace)` wired after `WaitForCacheSync`. `KeywordSearch`,
  `List`, `ListSummary`, `AllItems`, `KnowledgeIndex`, `Count`, `Get`, `GetItem` all
  query the cache first using `MatchingFields` for kind/subject; ephemeral items from
  `s.items` (agent_note, in-flight Upserts) are merged in. Map-scan fallback retained
  for local dev (no cache reader) and cache errors.

- [x] **#19 Block HTTP server start until cache is synced**
  `mgr.GetCache().WaitForCacheSync(ctx)` (90s timeout) called before
  `httpServer.ListenAndServe`. `SetCacheReader` called immediately after sync so the
  first request hits the cache. `SeedAll` (embedding backfill) runs concurrently in
  a goroutine — does not block HTTP start.

- [ ] **#18 Remove `seedItemDirect`, `SeedFromDoc`, and `items` map**
  Still needed: `SeedFromDoc` seeds the HNSW index from stored vectors;
  `seedItemDirect` populates the map in keyword-only mode (no embedder). Remove once
  HNSW is replaced by cache-backed semantic search, or `SeedAll` is refactored to
  drive embedding writes through the reconciler without touching `s.items`.

---

## API rename (ADR-014)

- [x] **#20 Rename `AwarenessMemoryCell` → `MemoryCell`, `triageagent.dev` → `xray.ai`**
  - `api/v1alpha1/awarenessmemorycell_types.go` → `memorycell_types.go`; all five types renamed
  - `groupversion_info.go`: group `triageagent.dev` → `xray.ai`
  - `zz_generated.deepcopy.go`: all receiver types updated
  - `internal/xray/cellsync.go`, `memory.go`, `mcpserver.go`, `cellsync_test.go`: import alias `triagev1alpha1` → `mcell`
  - `internal/vectorstore/memorycellstore.go`, `memorycellstore_test.go`: `AwarenessMemoryCellDocument` → `MemoryCellDocument`
  - `cmd/xray/main.go`: import alias + type refs updated
  - `charts/xray-xray/templates/rbac.yaml`: apiGroup `xray.ai`, resource `memorycells`
  - In-cluster: new CRD `memorycells.xray.ai` applied; 43 CRs migrated; old CRD pending delete
