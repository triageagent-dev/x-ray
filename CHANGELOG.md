# Changelog

## [Unreleased]

### Fixed
- A kind-filtered search of the cell index no longer drops results when a cell has a zero-norm embedding. It scores NaN, and NaN at the root of the top-K heap was never displaced, so the search returned the first entries it visited instead of the best. This regressed in 2.6.0, from the top-K scan.
- The server does not set a Go memory limit for a container under 80 MiB, where the scaled limit was zero or too small to run in.
- A tree-sitter parser is closed when setting its language fails; it leaked before.

## [2.6.0] - 2026-10-30

### Added
- **Regions for voice.** The voice tool `get_memory` also returns the graph's regions, the display categories of ADR-035 in their configured order: per region its item count, and for cells carrying a `doc:<id>` tag (the sections of one source document) the number of documents and sections and up to 50 document titles. A voice agent can now say how many documents there are and what a region holds; it used to count cells by kind, sections included, or count a capped `list_memory` page.

### Fixed
- The server sets the Go memory limit to 80% of the container's memory limit at start (cgroup v2, then v1) unless `GOMEMLIMIT` is set. Without one the collector waits for the heap to double, which in a 256Mi pod can be past the limit, so the pod was killed before a collection ran.
- The tree-sitter parsers a code-graph parse, update or re-index creates are freed when it is done. They were never closed, so every one leaked a native parser per language it touched.
- The GitHub and Azure sign-in responses are read up to 1 MiB, as the Google one already was.

### Changed
- A kind-filtered search of the cell index keeps only the best `topK` results instead of scoring every entry into a slice and sorting all of it: 690 KB and 5 allocations a search over 2,000 entries down to 8 KB.
- Rebuilding the cell index's graph after a delete snapshots one embedding per id, not every entry.
- The controller cache drops `managedFields` from the MemoryCells and Secrets it holds.

## [2.5.0] - 2026-10-30

### Changed
- A memory store holds about half the heap per item after `Restore`: 48 KB down to 25 KB per item on a 2,000-item benchmark (`BenchmarkRestoreHeap`). The vocabulary index kept one Go map per word or adjacent-word join, nearly always for a single item; each entry is now a sorted `[]uint32` over interned item ids, and joins are held in one map instead of two. The lexical cache keeps each word as a 12-byte span into text it already retains, where a string header and an 8-byte length cost 24. Posting-list slack is released once `Restore` finishes. Search results and speed are unchanged.

## [2.4.4] - 2026-10-29

### Fixed
- A deployment that splits one kind into several `ui.categories` by tag (ADR-035) gets each of them capped and counted on its own in the graph. The categories shared the graph's per-kind cap, so the first tag in id order filled it and the others drew few or no nodes, and every such region was labelled with the whole kind's count. `/v1/memory/summary` now caps per category and also returns `totalsByCategory`, which the graph uses for the region labels.

## [2.4.3] - 2026-10-29

### Fixed
- Swiping between tabs on a phone no longer stutters. Every touchmove re-rendered every tab, the Home graph included, and moving the graph repainted its SVG, which draws a drop-shadow filter on every tag line. The drag is now written to the panels once per animation frame, each panel keeps its own compositor layer, and the tab sections are memoised.
- The item view on a phone slides in when opened and slides out when closed, by the swipe or the back button; it used to appear and vanish at once. Its swipe-to-close re-rendered the item, markdown included, on every touchmove and dropped frames; the drag is now written to its style once per animation frame.

## [2.4.2] - 2026-10-29

### Fixed
- The mobile graph draws at most 25 nodes in total. The limit was 25 per kind, so a store with many kinds put up to 425 animated SVG nodes on a phone. The graph's regions now show only the total count per kind, as a bare number. The desktop graph is unchanged.

## [2.4.1] - 2026-10-29

### Fixed
- A voice session now saves its episodic memory before it hangs up. The chart's prompt asked for one at the end of a session, but its goodbye rule and the `end_session` description only said to say goodbye and hang up, so the model skipped the save and the next session started with nothing to recall. Both now put `save_memory` (kind=episodic) first, then the goodbye, then `end_session`.
- A voice session no longer crashes the server with `panic: concurrent write to websocket connection` when the model asks for several tools at once. The tool calls of a turn run in parallel and each wrote to the browser connection while the model's audio was being written; every write now goes through one lock.

## [2.4.0] - 2026-10-29

### Added
- **Google sign-in.** `--google-client-id`, `--google-client-secret` and `--google-domain` (or `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_DOMAIN`, chart `google.secretName`). Only accounts of that Google Workspace domain get a session: the check is on the ID token's `hd` claim, not the email suffix. Without `--google-domain` the server refuses to start unless `--allow-any-user`. Existing deployments derive the same session and MCP signing keys.

### Fixed
- `/v1/categories` leaves out a category's unset `tier` and `angle`. They were sent as 0, which the UI keeps, so a deployment that only relabels its categories had every graph region drawn at the centre.

## [2.3.0] - 2026-10-27

### Added
- **The memory engine as a Go library.** `pkg/memstore` (items, store, hybrid search), `pkg/memstore/index` (HNSW index and embedder), `pkg/memstore/hub` (live events, with a transport between replicas), `pkg/memstore/cellsync` (the decision for a cell, and the private-owner migration) and `pkg/memstore/memtest`. The server and `pkg/memory` run on the same engine, with their options kept (`WithEmbeddings`, `WithWriteDedup`, `WithRecallShaping`, `WithOrigin`). See ADR-062.
- `memstore.Lexicon`: the store's vocabulary index on its own (`Index`, `Remove`, `Candidates`, `DocFreq`, `Len`), for a host that prefilters its own documents with the same substring and one-edit matching.
- Engine switches, each off or equal to the engine's behaviour by default: per-kind default deadlines and renewal on merge, `Vocabulary.FactConfidenceOptional`, `Config.SkipIDText`, `cellsync.Options.KeepRetired`, source collapse and link expansion on the searcher.

### Changed
- **A cell past its retention deadline is deleted when the deadline falls** (`cellsync.Decide`), not at the next periodic sweep. `--retention-sweep-interval` stays as a backstop. Additive: nothing is deleted that the sweep would have kept.
- **The owner of a private item is a `user:<hash>` tag**, and the cell's `subject` names the instance that holds it. A private cell written by an earlier release (the owner in `subject`) is converted on first sync, idempotently, and reads the same meanwhile. The MCP and REST `subject` argument means what it did. The library reads a person's private items with `Store.KeywordSearchForUser` and `VisibleToUser`.
- **The server keeps its store resident**, fed by the cell watch, and no longer reads the cluster for keyword search. A mirrored item carries no embedding.
- A retired cell (refuted, superseded, stale) reaches connected clients as a delete event, whether it was retired here or on another replica.

### Fixed
- **Search ranking** (`docs/analysis/memory-search-review.md`). Punctuation is stripped from query and document tokens, so `bigquery.` matches `bigquery`. Conversational filler (`what`, `is`, `with`) no longer counts as evidence. A lexical answer is confident only when the matched words carry 75% of the query's weight, which means the semantic search runs when it should. The semantic score gap is applied after visibility, not inside the vector store. Keyword, semantic and exact-title rankings are fused with a deterministic tie-break. Provenance (`human`, `verified`), confidence and, for events, the event date weigh in the rerank. A fact that is restated keeps the higher-confidence, higher-origin, newer item.
- **Equal traversal scores are ordered by id**, so a graph walk returns the same result twice.
- **A person's private notes are no longer crowded out of a search or a list.** `MemoryStore.Search` ranks them on their own (`Store.KeywordSearchOwn`) and alternates them with the shared hits, and a list with a subject and no scope orders shared and private items together, newest first.
- **A lazily expired item leaves the lexical indexes**, as one retired by `Expire` does, so it no longer counts in word frequencies. `Store.CloneInto` builds the copy's indexes and inherits the vocabulary when given none, so a clone is searchable. `Store.GetFold` passes its result through the `Read` hook like `Get`. Refuting an item no longer copies the whole store.
- A two-token query no longer drops the item that matches only one of the tokens when that token carries enough of the query's weight (the reviewer's coverage exemption); the item with both tokens still ranks first.

## [2.2.1] - 2026-10-25

First public release. It replaces v2.2.0, which was withdrawn.

### Added
- **Typed memory** on Kubernetes. Items are `MemoryCell` custom resources in the API group `xray.triageagent.dev/v1`, each with a kind, a scope (`public` or `private`), a subject, tags, a confidence, typed links and an optional expiry. etcd persists, replicates and audits them.
- **Recall.** Search fuses keyword and semantic ranking, collapses near-duplicate facts and weighs query words by how distinctive they are. With an embedding endpoint it builds an HNSW index per subject; without one it falls back to keyword search.
- **Provenance.** Every item records who wrote it. A write that names a person is `human`, anything else is `derived`, and a model's write never replaces a person's. A refuted item retires the items drawn from it.
- **Code graph** (`xray graph`). Go and ten other languages, Markdown and YAML become one graph per repository with resolved call and import edges, served as `search_graph`, `get_graph_node`, `get_graph_impact` and `get_graph_risk`. Snapshots can be age-encrypted. Tree-sitter grammars are C, so only the CGO build (`make graph-build`, `bin/xray-parser`) parses.
- **Mental maps from a registry.** A `mental_map` cell with `spec.data.graphRef` (a digest-pinned OCI reference) is pulled and registered, so one deployment answers `search_memory` and the code-graph tools. `--registry-pull-secret` names a private registry's credentials, and `--mental-maps-snapshot-key` opens an encrypted snapshot.
- **Web UI**, embedded in the binary and served at `/`: the memory graph, search, per-kind lists, voice and a live view of recalls, lit the way optogenetics lights a neuron.
- **Voice** over a Gemini Live WebSocket, default model `gemini-3.8-live` (on Vertex, in the `eu` and `us` multi-regions; set `--voice-location`). A session opens with the user's newest notes and session memories. Each answer is limited to 6 tool calls and each session to 40, and `end_session` hangs up after the goodbye.
- **Sign-in off** (`--dev`) runs as a built-in user, so voice, personal notes and the live stream work locally.
- **Interfaces.** MCP over streamable HTTP with OAuth 2.1 or a shared secret, a REST API, a server-sent event stream, and Go packages to embed the memory store and the code graph in an agent, with an adapter for Google ADK.
- **Access control.** GitHub or Microsoft Entra sign-in, an optional GitHub org and team restriction, and read and write scopes for MCP clients.
- **Traces.** OpenTelemetry export with `--otel-endpoint`, `--otel-project` and `--otel-token-secret` (a bearer token read from a Secret), second-sized latency buckets, and a `voice first audio` log line per turn.
- **One Helm chart**, `xray`, and a static `xray` image, published to `ghcr.io/triageagent-dev` and built with ko.

### Security
- **A persisted session hash no longer works as a session token.** Restored sessions sit in their own map and are matched only by the HMAC of the presented token. Before, presenting the HMAC stored in the system configuration cell's status as a bearer token or cookie authenticated as that user until the session expired. Sessions that were restored and never used are also written back unchanged instead of hashed a second time.
- **`--dev` cannot be combined with a sign-in provider.** The server refuses to start with `--dev` and `--github-client-id` or `--azure-client-id`, so MCP, CORS and the API can no longer follow different authorisation models. `--dev` listening beyond loopback logs a warning.
- **A sign-in provider needs an org or tenant.** GitHub sign-in without `--github-org` and Entra sign-in without `--azure-tenant-id` refuse to start; `--allow-any-user` admits every account of the provider on purpose. The chart's `github.org` and `github.team` values were never rendered and are removed; set the flags in `args`.
- **Voice spend limits default to a limit.** `--user-daily-audio-token-limit` defaults to 500000 and `--max-concurrent-voice` to 20 (the chart values with these names were never passed to the binary and are removed). A session over its daily cap is closed by the server 45 seconds after the model is asked to say goodbye, and a concurrency slot is reserved before the session starts so parallel starts cannot exceed the cap.
- **The live event stream no longer carries what was asked.** A voice or MCP recall publishes the ids and kinds of the shared items it returned, never the query text or a private note's id.
- **A private item cannot be overwritten by naming its id.** Upserting an id that belongs to another subject's private item is refused.
