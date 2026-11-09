# ADR-045: xray-code — Offline Snapshots, Per-Project Graphs, and a Persistent Worked Copy

**Status:** Implemented
**Commit:** —
**Date:** 2026-06-08
**Author:** Denys Vasyliev

---

## Context

ADR-043 built `xray-code` as a **single merged in-memory graph**: `serve`
parses every served repo at startup, embeds every node, and merges them into one
`graph.Graph` + one `index.Index` + one HNSW. That fits one or a few repos. It
does **not** fit running against **~25 projects**:

- **Cold start is expensive.** Every `serve` re-parses *and re-embeds* all repos
  — thousands of embedder round-trips before the server answers a query.
- **One shared graph cross-contaminates ranking.** Unrelated projects share one
  HNSW; a search for "voice session" can surface a near-namesake from a different
  repo. There is no way to drop one project's footprint.
- **All-or-nothing RAM.** Every project is resident whether queried or not.

xray-code is used in **two distinct ways**, and only the second needs to change:

- **Mode A — local in-repo** (`serve`, wired as `xray-recall-local`): a live graph
  with incremental `update` hooks (PostToolUse). It works well and **stays
  unchanged** — no snapshots, no registry, no TTL.
- **Mode B — online long-term memory** (NEW): a server over the ~25 projects used
  as a **cache in front of RAG** by X-Ray's conversational layer. Here the graph
  should load instantly (no startup embed), isolate projects, and bound RAM.

A late refinement made Mode B a **persistent worked copy** rather than strictly
read-only: a remote client may edit a project's graph via `update_code_graph`,
and the edited graph is flushed back to disk before it is evicted, so the
on-disk snapshot stays fresh and survives restarts.

---

## Decision

Add a **Mode B** server, `serve-snapshots`, built on three pieces. Mode A is
untouched.

### 1. Offline snapshots — one file per project (`snapshot.go`)

A new `snapshot` subcommand parses + embeds each repo **once** and writes one
`<label>.graph.gob.gz` per project containing:

- a `SnapshotHeader` — version, label, repo path, an **embedding fingerprint**
  (`{model, dims, maxInputChars}`), a synthesized router **summary**, and the
  summary's embedding (`SummaryVec`);
- a `SnapshotBody` — the parsed per-file `parser.Result`s and the
  **text→vector embedding cache** verbatim.

Format is `encoding/gob` in a gzip stream, header then body as two values, so the
header can be read alone. `serve-snapshots` loads a snapshot via
`NewIndexerFromSnapshot`, which pre-populates the index cache (`LoadCache`) before
`Build` — so **every node is a cache hit and no node is ever embedded at load**.
The embedder is used only to embed the *query* (search) and the *routing summary*.
A header fingerprint that does not match the server's embed config is rejected
(the vectors live in a different space) — rebuild with `snapshot`.

The router summary prefers the repo README's section prose, falling back to a
stats line (kinds + sample symbol names) when there is no README.

### 2. Per-project graphs + a semantic router (`registry.go`)

Each project is **fully isolated**: its own `graph.Graph` + `index.Index` + HNSW
+ embedding cache. There is no merged graph anywhere. A `Registry` owns one
`projectEntry` per project; the entry's tiny `summaryVec` is **always resident**,
while its graph (`ixr`) is loaded lazily and dropped on eviction.

The `mcpserver` package gains a `Resolver` interface (`Resolve`, `ResolveByQN`,
`UpdateByPaths`) and a `NewReadOnly` registration path whose tools resolve a
graph **per call** instead of closing over a fixed one:

- **search_code / search_docs** route by `(project, query)`: an explicit
  `project` (alias `repo`) wins; otherwise the query is embedded and matched by
  cosine against each project's resident `summaryVec`, taking the top
  `--route-fanout` (default **1**) and merging hits by score.
- **get_code_node / trace_code_calls / get_code_impact** route by the project
  **prefix of the repo-namespaced qn** (`label//…`) — no embedding needed.

### 3. TTL eviction with flush-on-evict (the worked copy)

A sweeper evicts any project idle longer than `--graph-ttl` (0 = never): it drops
the entry's `ixr` (graph + index + HNSW) but **keeps the summary vector** so
routing still resolves it and triggers a reload from the snapshot on next use
(fast, no re-embed). Eviction frees RAM and bounds the resident set.

`update_code_graph` is exposed on `serve-snapshots`: a remote client re-indexes
changed files (re-parse + re-embed only changed nodes) in the worked copy and the
project is marked **dirty**. A dirty project is **flushed back to its
`.gob.gz`** before it is evicted, and all dirty projects are flushed on graceful
shutdown (SIGINT/SIGTERM). So the on-disk snapshot is the durable, fresh copy;
edits are not lost across eviction or restart.

### Concurrency

Eviction only drops the entry's `*Indexer` pointer under the entry lock; it never
mutates a graph in place, so an in-flight reader that already holds a `Resolved`
keeps its `*graph.Graph` alive via the GC — the same atomic-swap discipline the
graph already uses (ADR-043). Loads are double-checked so concurrent resolves
load a body once. Flush runs before the drop; if it fails the project is kept
resident rather than losing unflushed edits.

### 4. Code nodes carry their full source body — embedded by signature, displayed by body

A snapshot stores, per code node (`Func`/`Test`/`Class`/`Type`), its **full
declaration source** in `Body` (the parser slices the Tree-sitter declaration
range, capped at `maxCodeBodyChars` = 8 KiB), in addition to its signature,
location, and edges. This keeps Mode B self-contained: `get_code_node` and the
`/ui` node panel show the **actual implementation** even though the repo is not on
disk — the body travels inside the `.gob.gz`.

Crucially, **what is embedded and what is displayed are decoupled**:

- **Embedding/search** uses the synthesized **signature** (`nodeText`:
  kind + name + params + return type + file), not the body. So adding bodies does
  **not** change vectors, search ranking, or routing — the body never dilutes the
  embedding, and existing snapshots stay comparable.
- **Display** (`get_code_node`, UI) returns `Body` when present. Docs reassemble
  from their sections; manifests/sections carry their own `Body`; code nodes now
  carry theirs.

**Cost:** snapshots grow by roughly the source size of the indexed code (bounded
per node by the 8 KiB cap). For the ~25-project corpus this is a few MB per
project — acceptable for the self-contained-offline benefit, and bodies are
dropped from RAM on TTL eviction along with the rest of the graph, reloaded from
the snapshot on next use. (Doc/Manifest bodies were already stored this way; this
extends the same treatment to code.)

---

## Consequences

**Positive**
- `serve-snapshots` starts instantly: no node embedding, only header decode per
  project; bodies load on first use.
- Per-project isolation removes cross-repo ranking bleed; TTL bounds RAM for many
  projects regardless of access pattern.
- The worked copy is editable remotely and **self-persisting**: dump-before-evict
  + flush-on-shutdown keep the disk snapshot fresh, so a project can be edited as
  a remote working copy and survive restarts.
- Mode A (local live `xray-recall-local`) is completely unchanged.

**Negative / risks**
- The embed fingerprint couples a snapshot to one model/dims; changing the model
  invalidates all snapshots (rebuild required) — surfaced as a hard error, not a
  silent stale read.
- Flush-on-evict has a crash window: edits since the last flush are lost if the
  process is killed (not gracefully) between flushes. Accepted for the worked-copy
  use; write-through or debounced flush are available if the window matters.
- `--route-fanout=1` may miss cross-cutting queries that span projects; raise
  fanout to merge across projects at the cost of touching more graphs per call.

**Alternatives rejected**
- *Keep Mode B strictly read-only* (immutable snapshots, refresh only via
  rebuild) — simpler, but gives up the remote worked-copy workflow the user
  wanted.
- *One combined snapshot for all projects* — fights lazy load + per-project TTL
  (all-or-nothing again).
- *Fold Mode A into Mode B* — would make local dev depend on building a snapshot
  first; the two servers have distinct jobs.

---

## Implementation Notes

| Path | Role |
|------|------|
| `tools/xray-code/snapshot.go` | Snapshot format (header/body/fingerprint), gob+gzip read/write, router summary, `snapshot` subcommand |
| `tools/xray-code/registry.go` | `Registry` + `projectEntry`: lazy load, semantic route, qn route, `UpdateByPaths`, flush-on-evict, sweeper |
| `tools/xray-code/indexer.go` | `NewIndexerFromSnapshot` (load with no node embed), `SnapshotFor` (capture files+cache+summary) |
| `tools/xray-code/index/index.go` | `CacheSnapshot` / `LoadCache` accessors over the text→vector cache |
| `tools/xray-code/mcpserver/{mcpserver,readonly}.go` | `Resolver`/`Resolved` types, `NewReadOnly`, per-call resolved tools, worked-copy `update_code_graph` |
| `tools/xray-code/serve_snapshots.go` | `serve-snapshots` subcommand: build registry from `-snapshot-dir`, sweeper, graceful flush-on-shutdown |

Tests (`registry_test.go`): snapshot round-trip; **load-without-embed** (a
panicking `EmbedBatch` proves no node is embedded on load); semantic routing +
`ResolveByQN`; TTL evict + reload (summary vector survives); update → dirty →
flush-on-evict persists the edit to disk. Run with `CGO_ENABLED=1 go test ./...`.

### Usage

```bash
# Build one snapshot per project (the only embed pass).
xray-code snapshot -repo a=/path/a -repo b=/path/b -snapshot-dir ~/.xray-cache

# Serve them: instant start, semantic routing, 30m idle TTL.
xray-code serve-snapshots -snapshot-dir ~/.xray-cache -graph-ttl 30m -route-fanout 1
```

Follow-ups: Helm chart for the in-cluster worked-copy server; debounced/
write-through flush option; HNSW-shared variant if per-project HNSW memory
becomes a concern at higher project counts.

## Amendments

### 2026-11-09 — The fingerprint carries the token window

The snapshot fingerprint is `{model, dims, maxInputChars, maxInputTokens}`. The byte cap (`--embedding-max-input-chars`) is a bound; the token window (`--embedding-max-input-tokens`) is the fit, counted by the endpoint's own tokenizer. A snapshot written before the field decodes `maxInputTokens` as 0, compares unequal to a server that sets a window, and is rebuilt (issue #61). `TestFingerprintTokenWindowStrict` covers the strict comparison.
