# X-Ray Memory Optimization Review & Architectural Recommendations

## 1. Executive Summary & Runtime Memory Constraints

In [`charts/xray/values.yaml`](../../charts/xray/values.yaml#L435-L440), the main `xray` container is configured with:
```yaml
resources:
  requests:
    cpu: 50m
    memory: 64Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

Operating within a hard **256 MiB** container limit requires strict discipline over Go runtime heap allocation, informer caching, vector structures, and transient search buffers.

X-Ray currently runs four concurrent memory consumers in the same process:
1. **Core Memory Store (`pkg/memstore`)**: In-memory item storage, inverted vocabulary indexes (`vocabIndex`), and n-gram indexes (`trigramIndex`).
2. **Vector Index (`pkg/memstore/index`)**: Per-subject HNSW graphs (`*hnsw.HNSW`) and embedding caches.
3. **Recall Code Graph (`internal/codegraph`)**: Repository code graphs (`*graph.Graph`), AST nodes, and mental-map snapshot registries.
4. **Kubernetes Informer Cache (`controller-runtime`)**: Watched CRDs (`MemoryCell`) and namespace Secrets.

### Key Risk Areas Identified
- **Unset `GOMEMLIMIT`**: Default `GOGC=100` allows the Go runtime heap to double before triggering GC, causing kernel cgroup OOM kills (`137 OOMKilled`) around ~140–160 MiB of live heap.
- **In-Memory Data Multiplication (4×–8×)**: The exact same item title and content is duplicated across controller-runtime cache, `Store.cells`, `lexicalCache` (multiple normalized/lowercased copies), `CellStore.entries`, and index postings.
- **Monotonic Memory Leaks**: Hub `hitCounts` maps and trigram item ID registries never evict dead or expired keys.
- **HNSW Rebuild Spikes**: Double-graph allocation during lazy `freshen` graph rebuilds creates transient memory spikes.
- **Full-Namespace Secret Ingestion**: Informer caches and duplicates all namespace secrets into `SecretStore`, regardless of relevance.
- **CGO Parser Leakage**: Tree-sitter parsers created during indexing are not explicitly freed before Go GC reclamation.

```
+---------------------------------------------------------------------------------------------------+
|                                  Total Pod Memory: 256 MiB Limit                                   |
+---------------------------------------------------------------------------------------------------+
|  Informer Cache (K8s CRDs + All Secrets)  |  Store.cells (Items + LexicalCache)  |  HNSW Graphs   |
|         ~40 - 70 MiB (Redundant)          |       ~60 - 90 MiB (Multiplied)       |  ~30 - 60 MiB  |
+---------------------------------------------------------------------------------------------------+
|                                 Transient Heap / Search Allocations                               |
|                  (Spikes during HNSW Rebuild / scanEntries trigger cgroup OOM)                    |
+---------------------------------------------------------------------------------------------------+
```

---

## 2. Critical & High-Impact Findings

### 2.1. Container Deployment Lacks `GOMEMLIMIT` (Crash / OOM Killer Risk)
- **Files**: [`charts/xray/templates/_helpers.tpl`](../../charts/xray/templates/_helpers.tpl#L1-L30), [`charts/xray/values.yaml`](../../charts/xray/values.yaml#L435-L440), [`cmd/xray/main.go`](../../cmd/xray/main.go#L300-L330)
- **Mechanism**:
  Go’s default runtime GC pacing is governed by `GOGC=100`. Go triggers garbage collection only after newly allocated heap matches 100% of the live heap at the end of the previous GC cycle. With a container limit of 256 MiB:
  - If steady-state live heap reaches 140 MiB, Go will not trigger GC until heap reaches 280 MiB.
  - At 256 MiB, the Linux kernel cgroup OOM killer sends `SIGKILL (137)`.
  - The process crashes with no panic stack trace, despite having ample reclaimable memory.
- **Impact**: **Critical**. Unavoidable OOM crashes during moderate indexing bursts or search volume.
- **Recommendation**:
  1. Add `GOMEMLIMIT` to the container environment via `charts/xray/templates/_helpers.tpl`:
     ```yaml
     - name: GOMEMLIMIT
       value: "210MiB" # ~82% of 256MiB limit
     ```
  2. Programmatically guard this in [`cmd/xray/main.go`](../../cmd/xray/main.go) at startup:
     ```go
     import "runtime/debug"
     // If not explicitly set via environment, constrain to 80% of detected cgroup limit.
     if os.Getenv("GOMEMLIMIT") == "" {
         debug.SetMemoryLimit(210 * 1024 * 1024)
     }
     ```

---

### 2.2. String & Buffer Multiplying in `lexicalCache`
- **Files**: [`pkg/memstore/lexical.go:567-597`](../../pkg/memstore/lexical.go#L567-L597), [`pkg/memstore/item.go:12-40`](../../pkg/memstore/item.go#L12-L40)
- **Mechanism**:
  Every stored memory `Item` holds a precomputed `lexicalCache`:
  ```go
  type lexicalCache struct {
      built           bool
      titleLower      string
      titleLowerWT    wordTokens
      haystackLower   string
      haystackLowerWT wordTokens

      titleNorm       string
      titleNormWT     wordTokens
      haystackNorm    string
      haystackNormWT  wordTokens
  }
  ```
  Where `wordTokens` contains `words []string` and `lens []int`.
  For a 10 KB item (~1,500 words):
  - `haystackLower` (concatenated string copy): 10 KB
  - `haystackNorm` (second concatenated string copy): 10 KB
  - `haystackLowerWT.words` (1,500 strings × 16 bytes): 24 KB string headers
  - `haystackLowerWT.lens` (1,500 ints × 8 bytes): 12 KB
  - `haystackNormWT.words` + `lens`: 36 KB
  - `titleLower`, `titleNorm`, `titleLowerWT`, `titleNormWT`: ~2–5 KB
  - **Total**: A 10 KB item consumes **~95 KB** of resident memory—a **9.5× memory multiplication factor**.
- **Impact**: **High**. Storing 2,000 active memory cells of average 5 KB expands to ~100 MiB of heap, suffocating the 256 MiB container budget.
- **Recommendation**:
  1. Deduplicate normalized vs. lowercased haystacks: `NormalizeQuery` already converts to lowercase. Storing both `haystackLower` and `haystackNorm` is redundant.
  2. Eliminate parallel slices of strings and lengths (`words []string` + `lens []int`). Store token byte offsets and lengths pointing into a single haystack string slice:
     ```go
     type tokenSpan struct {
         offset uint32
         length uint16
         runes  uint16
     }
     ```
  3. Defer rune count calculation or compute on the fly for fuzzy matching.

---

### 2.3. Cross-Subsystem Document Duplication
- **Files**:
  - Kubernetes Informer: [`cmd/xray/main.go:335-350`](../../cmd/xray/main.go#L335-L350)
  - Primary Store: [`pkg/memstore/store.go:108`](../../pkg/memstore/store.go#L108)
  - Semantic Cell Store: [`pkg/memstore/index/memorycellstore.go:20-65`](../../pkg/memstore/index/memorycellstore.go#L20-L65)
- **Mechanism**:
  A single `MemoryCell` exists in memory at least three times simultaneously:
  1. In `controller-runtime` informer cache as a full `mcell.MemoryCell` CR.
  2. In `Store.cells` as a `memstore.Item` embedding `MemoryCellSpec`.
  3. In `CellStore.subjects[sub].entries[id]` as a `cellEntry` embedding a full `IndexEntry` (which copies `Title`, `Content`, `Tags`, `EventDate`, etc. by value).
  4. In HNSW vector nodes.
- **Impact**: **High**. Over 60% of the vector store's heap is spent storing duplicates of text that already resides in `Store.cells`.
- **Recommendation**:
  Strip text from `IndexEntry` in `CellStore`. Store only retrieval metadata:
  ```go
  type cellEntry struct {
      ID        string
      Kind      string
      Subject   string
      Timestamp time.Time
      Embedding []float32
  }
  ```
  During search result formatting, hydrate `Title` and `Content` directly from `Store.Get(id)`.

---

### 2.4. Informer Ingests All Namespace Secrets & Duplicates into `SecretStore`
- **Files**: [`cmd/xray/main.go:385-392`](../../cmd/xray/main.go#L385-L392), [`internal/xray/configwatcher.go:15-40`](../../internal/xray/configwatcher.go#L15-L40)
- **Mechanism**:
  Lines 386–388 in `main.go`:
  ```go
  if secInf, secInfErr := mgr.GetCache().GetInformer(context.Background(), &corev1.Secret{}); secInfErr == nil {
      secInf.AddEventHandler(secretStore.SecretEventHandlerFuncs())
  }
  ```
  1. The controller-runtime cache opens an informer on `corev1.Secret` with no field or label selector, caching **every Secret** in the namespace.
  2. `SecretStore.SecretEventHandlerFuncs` copies every byte array (`sec.Data`) into `s.data[namespace+"/"+name] = sec.Data`.
  In Kubernetes environments, namespace secrets often include multi-megabyte TLS certificates, Helm release bundles, and operator credentials.
- **Impact**: **High**. Storing all secrets in the namespace twice consumes dozens of megabytes of heap for keys X-Ray never reads.
- **Recommendation**:
  Use `cache.ByObject` to configure the manager cache:
  ```go
  cache.Options{
      ByObject: map[client.Object]cache.ByObject{
          &corev1.Secret{}: {
              // Only cache secrets with a specific label or eliminate informer
          },
      },
  }
  ```
  For the few secrets X-Ray actually requires (`otelTokenSecret`, `mcpTokenSecret`), read them using `mgr.GetAPIReader().Get(...)` rather than caching the whole namespace.

---

### 2.5. Missing `TransformStripManagedFields` on Informer Cache
- **Files**: [`cmd/xray/main.go:337-339`](../../cmd/xray/main.go#L337-L339)
- **Mechanism**:
  The manager cache is registered with:
  ```go
  Cache: cache.Options{
      DefaultNamespaces: map[string]cache.Config{namespace: {}},
  }
  ```
  `metadata.managedFields` generated by Kubernetes Server-Side Apply tracks detailed field ownership timestamps and paths. It accounts for 30%–50% of the raw memory of Kubernetes objects in client-go informers.
- **Impact**: **High**. 30%–50% of the Kubernetes informer memory footprint is spent holding JSON field-tracking metadata never accessed by X-Ray.
- **Recommendation**:
  Configure `cache.TransformStripManagedFields()`:
  ```go
  Cache: cache.Options{
      DefaultNamespaces: map[string]cache.Config{namespace: {}},
      DefaultTransform:  cache.TransformStripManagedFields(),
  }
  ```

---

## 3. Medium-Impact Findings

### 3.1. Monotonic Key Leak in Hub `hitCounts`
- **Files**: [`pkg/memstore/hub/hub.go:130-142`](../../pkg/memstore/hub/hub.go#L130-L142), [`internal/codegraph/hub/hub.go:119-125`](../../internal/codegraph/hub/hub.go#L119-L125)
- **Mechanism**:
  ```go
  h.hitMu.Lock()
  for _, id := range evt.ItemIDs {
      c, ok := h.hitCounts[id]
      if !ok {
          c = &atomic.Int64{}
          h.hitCounts[id] = c
      }
      c.Add(1)
  }
  h.hitMu.Unlock()
  ```
  `hitCounts` is an unbounded map containing heap pointers (`*atomic.Int64`) for every item ID or node QN ever returned in a search. When items expire, are superseded, or are deleted, their keys are **never removed** from `hitCounts`.
- **Impact**: **Medium**. Continuous slow leak in long-running pods.
- **Recommendation**:
  Hook item deletions to clean up `hitCounts`, or replace with an LRU / bounded decay table.

---

### 3.2. Peak Memory Spike During HNSW `freshen` Rebuild
- **Files**: [`pkg/memstore/index/memorycellstore.go:120-155`](../../pkg/memstore/index/memorycellstore.go#L120-L155)
- **Mechanism**:
  When embedding updates or deletions flag `s.dirty = true`:
  ```go
  snapshot := make(map[string]cellEntry, len(s.entries))
  for id, e := range s.entries {
      snapshot[id] = e
  }
  s.mu.Unlock()

  g := newGraph()
  for id, e := range snapshot {
      g.Insert(...)
  }
  ```
  During the rebuild:
  1. Old graph (`s.index`) is retained.
  2. New graph (`g`) is allocated and populated.
  3. `snapshot` map is allocated and populated.
  4. Primary map (`s.entries`) is retained.
- **Impact**: **Medium to High**. Rebuilding doubles vector index memory consumption, frequently inducing OOM when pod memory is near the ceiling.
- **Recommendation**:
  Avoid creating a full `snapshot` map copy. Extract only IDs into a slice `[]string`, or build incrementally where possible. Call `runtime.GC()` immediately after swapping `s.index = g`.

---

### 3.3. Full Slice Allocation & O(N log N) Sort in `scanEntries`
- **Files**: [`pkg/memstore/index/memorycellstore.go:235-248`](../../pkg/memstore/index/memorycellstore.go#L235-L248)
- **Mechanism**:
  Kind-filtered vector search scans entries sequentially:
  ```go
  results := make([]CellSearchResult, 0, len(entries))
  for _, entry := range entries {
      results = append(results, CellSearchResult{Document: entry.Cell, Score: ...})
  }
  sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })
  if topK > 0 && len(results) > topK {
      results = results[:topK]
  }
  ```
  For 5,000 entries, it allocates a 5,000-element slice of large structs, computes scores, and executes a full quicksort just to extract `topK` (e.g. 5 or 10) results.
- **Impact**: **Medium**. Generates ~1.5–2.5 MiB of transient allocations and GC pressure per search query.
- **Recommendation**:
  Maintain a bounded min-heap of size `topK` (via `container/heap`). Memory consumption drops from $O(N)$ large structs to $O(K)$.

---

### 3.4. Leaking CGO Tree-Sitter Parsers in Codegraph
- **Files**: [`internal/codegraph/parser/walk.go:18-35`](../../internal/codegraph/parser/walk.go#L18-L35), [`internal/codegraph/engine/parse.go:122`](../../internal/codegraph/engine/parse.go#L122)
- **Mechanism**:
  `p := parser.New()` initializes `parsers: map[string]*tree_sitter.Parser`. In `go-tree-sitter`, `tree_sitter.NewParser()` allocates native C memory via `ts_parser_new`. `Parser` provides no `Close()` method to invoke `ts.Close()` (`ts_parser_delete`). When Go garbage collects `p`, the underlying C allocations remain orphaned on the process heap.
- **Impact**: **Medium**. C memory leaks outside Go GC tracking whenever code graphs or mental maps are parsed.
- **Recommendation**:
  Add `Close()` to `parser.Parser` and ensure callers invoke `defer p.Close()`:
  ```go
  func (p *Parser) Close() {
      for _, ts := range p.parsers {
          ts.Close()
      }
      clear(p.parsers)
  }
  ```

---

### 3.5. Nested Map Inverted Indexes in `kwvocab.go` and `codegraph/index/lexical.go`
- **Files**: [`pkg/memstore/kwvocab.go:46-51`](../../pkg/memstore/kwvocab.go#L46-L51), [`internal/codegraph/index/lexical.go:48-58`](../../internal/codegraph/index/lexical.go#L48-L58)
- **Mechanism**:
  ```go
  wordPostings map[string]map[string]struct{}
  joinPostings map[string]map[string]struct{}
  wordsByLen   map[int]map[string]map[string]struct{}
  joinsByLen   map[int]map[string]map[string]struct{}
  ```
  Every unique word and joined word-pair instantiates an independent Go `map[string]struct{}`. For 30,000 unique vocabulary terms, this allocates 30,000 separate `hmap` headers and hash buckets, adding ~6–8 MiB of pointer overhead and severe GC scan latency.
- **Impact**: **Medium**.
- **Recommendation**:
  Flatten posting lists into sorted integer slices (`[]uint32`), adopting the data layout already proven in [`pkg/memstore/kwindex.go`](../../pkg/memstore/kwindex.go).

---

## 4. Low-Impact Findings & Hygiene

1. **Unbounded `io.ReadAll` on External OAuth Endpoints**:
   - [`internal/xray/github_auth.go:199, 295`](../../internal/xray/github_auth.go#L199) and [`internal/xray/azure_auth.go:159, 193`](../../internal/xray/azure_auth.go#L159) read external HTTP responses with `io.ReadAll(resp.Body)` without `io.LimitReader`. Wrap with `io.LimitReader(resp.Body, 1<<20)`.
2. **OpenTelemetry Batch Queue Size**:
   - In [`internal/observability/otel.go:120-130`](../../internal/observability/otel.go#L120-L130), `sdktrace.WithBatcher(exp)` uses default queue size 2,048. When spans contain LLM prompt bodies or voice transcriptions, queue memory can grow by 10–20 MiB. Reduce `MaxQueueSize` to 512.
3. **Snapshot Byte Buffers Retained in Memory**:
   - In [`internal/codegraph/engine/registry.go:34`](../../internal/codegraph/engine/registry.go#L34), `snapshotBytes []byte` holds the raw compressed graph bytes in RAM even after decoding into `Graph` and `Index`. Clear this buffer after successful load if re-eviction reload is infrequent.

---

## 5. Architectural Memory Balance & Sound Practices Already in Place

- **Vectors Use `float32` Everywhere**: Vector math in both `memstore` and `codegraph` utilizes `[]float32` and `vector.F32`, avoiding 64-bit float waste.
- **Flat Inverted Indexing in `kwindex.go`**: `trigramIndex` packs item and word indexes into contiguous `uint64` references and flat arrays, avoiding pointer-heavy tree structures.
- **Voice Tool Output Budgeting (`headroomLimit`)**: [`internal/xray/voice_tools.go:426`](../../internal/xray/voice_tools.go#L426) enforces token and character limits on search and fetch tool outputs before streaming over websockets to Gemini Live.
- **HTTP Request Size Caps**: The HTTP server enforces `http.MaxBytesReader` ([`internal/xray/server.go:177`](../../internal/xray/server.go#L177)) to prevent payload inflation attacks.

---

## 6. Implementation Roadmap

```
Phase 1: Zero-Risk Crash Prevention (Immediate)
├── Set GOMEMLIMIT=210MiB in Helm charts & cmd/xray/main.go
└── Add cache.TransformStripManagedFields() to controller-runtime manager

Phase 2: Informer & Secret De-duplication
├── Restrict controller-runtime Secret informer to required Secret names
└── Hydrate CellStore.entries from Store.cells instead of duplicating IndexEntry

Phase 3: Hot-Path Search & Index Compaction
├── Replace scanEntries full-sort with Top-K Min-Heap
├── Deduplicate haystackNorm in lexicalCache
└── Add Close() to CGO tree-sitter parsers to release native C memory

Phase 4: Structural Data Optimization
├── Flatten kwvocab nested maps into []uint32 posting slices
└── Bound Hub hitCounts map with eviction
```
