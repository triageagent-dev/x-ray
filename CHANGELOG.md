# Changelog

## [Unreleased]

### Added
- **A pre-registered public MCP client with fixed redirect URIs: `--mcp-static-client-id` and `--mcp-static-client-redirect-uris`, with `--mcp-oauth` (ADR-065).** Open registration accepts only loopback and custom-scheme redirect URIs, so a client that is given a client ID and posts the code to its own https callback could not connect. The pre-registered client must use one of its redirect URIs exactly (https, or http on loopback), and the user approves it on a consent page bound to the browser session and the request. Open registration is unchanged.
- **`--access-users` and `--access-admins` (or `ACCESS_USERS`, `ACCESS_ADMINS`) name who may sign in and who gets the admin interface, with any sign-in provider (ADR-066).** Sign-in admitted whoever a provider's gate admitted, and the admins were a hand-kept list of user hashes on the configuration cell. With either list set, only the listed principals sign in. A principal is `google:<address>` in `--google-domain`, `github:<numeric user id>` or `azure:<object id>`; ids rather than logins, because a login can be renamed and taken by someone else. The lists are checked at every sign-in callback, on every session use, at `/authorize` and for voice writes. A malformed principal, a principal for a provider that is not configured, a Google principal outside `--google-domain` or a list that parses to nothing stops the server at start. `writeAllowedUsers` still grants the admin interface to a user who signs in. With neither list set, sign-in and the admin interface work as before.
- **The server can run under a path of a shared host: a path in `--public-url` (`https://gw.example.com/path`) mounts it there (ADR-064, #68).** X-Ray assumed it owned the host root, so behind a proxy that routes one path to it, redirects, the PWA and the cookies went to the root, and the session cookie reached every other service on the host. With a path, the server serves under it, and the login and logout redirects, the session and state cookies, the PWA manifest and the service worker stay under it. The OAuth metadata is also served at the host root with the path inserted (RFC 8414, RFC 9728), where MCP clients look first; route those two paths to X-Ray as well. The proxy forwards the path unchanged. `make build BASE_PATH=/path` builds the UI for it, and an image is bound to the path it was built for (the server warns at startup if they differ). Without a path nothing changes.

### Changed
- `/token` rejects a `client_id` or `redirect_uri` that is sent and does not match the authorization code, for every client (RFC 6749, 4.1.3). PKCE still guards a request that omits them.
- A Google session is named by its full address, lower-cased, instead of the local part (a voice prompt still gets the local part), and an address that is not ASCII matches no access list (it signs in as before when no list is set). With an access list set, a Google session made before the upgrade matches no list and signs in again. Notes, quotas and the admin lists are keyed by the account id, so they are unaffected. The web UI's menu shows the full address, and panels pinned in the browser were stored under the old name, so they are pinned again once.
- The CORS and voice WebSocket origin check use the scheme and host of `--public-url`, which is all a browser sends as `Origin`, so a trailing slash or a path no longer breaks them. The service worker starts with an empty `BASE` constant at the host root.

## [2.9.0] - 2026-11-10

### Added
- **`--otel-service-name` sets the `service.name` of an instance's spans (#72).** Every instance sent its spans as `xray`, so a trace backend that routes by service name treated all instances alike, and `--otel-project` sets only the openinference project. The flag defaults to `xray`, so nothing changes unless it is set. Prometheus metrics are pulled per target and keep `xray`.
- **Voice sessions can compress their context, behind `--voice-context-trigger-tokens` (default 0, off; ADR-039).** Resumption keeps a session alive across the connection recycle, but Gemini Live also limits audio-only sessions to 15 minutes unless the context is compressed. With a trigger set (5000-128000 tokens), the server asks Gemini for a sliding window: once the context passes the trigger the oldest turns are dropped, never the system instruction. `--voice-context-target-tokens` (below the trigger; 0 leaves it to the server, half the trigger) is the size it is cut to. The pair can also be set per instance with `contextTriggerTokens` and `contextTargetTokens` in the configuration cell's `voiceConfig`, and the chart sets it with `voice.contextTriggerTokens` and `voice.contextTargetTokens`. A pair that fails the check stops the server at startup; one in the cell is logged and ignored. A resumed connection keeps the compression. The `MemoryCell` CRD gains two fields; Helm does not upgrade CRDs, so apply `charts/xray/crds/` before setting them.

### Fixed
- **A recall lights the tagged region it belongs to (#69).** When a kind is split into categories by tag (ADR-035), a hit lit only the kind's bare region. A hit event now carries the tags of shared items, re-read from the store (a private item carries none, and the query is still never sent), and the graph places a hit by its kind and tags. An older server sends no tags and hits are placed by kind alone.
- Synapse lines in the graph are thin and faint at rest and take the full style on hover, so a dense store does not bury the regions. The stats text in the graph's corner scales with the screen (11-20 px) instead of a fixed 10 px.

## [2.8.0] - 2026-11-09

### Added
- **`--embedding-max-input-tokens` cuts embedding input to the model's window (#61).** The input cap counts bytes and the model's limit is tokens; a dense policy section reached 607 tokens within 1800 bytes against nomic-embed-text-v2-moe's 512, and llama.cpp answered 400 instead of truncating, which failed `xray graph snapshot` and left such a cell unindexed. With the flag set (0, the default, is off), an input over the window is cut, when it is sent, to the longest prefix that fits, counted by the endpoint's own `POST /tokenize`; cache keys and fingerprints keep the byte-capped text. The graph snapshot fingerprint carries the window, so a map built without it is rebuilt.
- The window is set with the flag (the server and the `xray graph` commands), with `memory.WithMaxInputTokens` in `pkg/memory`, and with `(*codegraph.Embeddings).SetMaxInputTokens` in `pkg/codegraph`; the embedders take it through `SetMaxInputTokens`, next to `SetUsageSink`. No constructor signature changes.
- When the endpoint cannot count (an OpenAI-compatible gateway with no `/tokenize`, or a failing tokenizer), the byte-capped text is sent with a warning and the server's answer decides; a tokenizer error is not an embed error. A 404 or 405 from `/tokenize` is logged once and the embedder stops counting for the life of the process.
- **Voice sessions survive the Gemini Live connection recycle, behind `--voice-session-resumption` (default off, ADR-039).** Gemini Live closes a connection about every 10 minutes and warns with a GoAway 30–60 s before; X-Ray ignored it, so sessions ended at 9m31s–9m50s with `GoAway deadline exceeded`, and an upstream drop without a close frame (`1006 unexpected EOF`) ended them at any time. With the flag on, the session keeps the latest resumable handle and reconnects with it on a dropped connection, and after a GoAway at once when nobody is speaking, or, while the user or the model is speaking, once that turn has ended and the handle that covers it has arrived; the browser stays connected and the conversation continues from the server's state. On Vertex the session also asks for the index of the last client message each handle covers, keeps what it sent since (up to about 10 s of microphone audio), and sends the speech in it again on the new connection at the pace it was spoken (a burst gets the stream ended with 429), so what the user was saying is not lost in a reconnect. One attempt per drop: a rejected handle ends the session as before. The configuration cell's `voiceConfig.sessionResumption` overrides the flag per instance, and the chart's `voice.sessionResumption` adds the flag. The `MemoryCell` CRD gains the field; Helm does not upgrade CRDs, so apply `charts/xray/crds/` by hand before setting it.

### Fixed
- A Gemini Live GoAway is logged with the time left and recorded on the session span; it used to be invisible.
- When a voice session ends, the browser gets `done` and a WebSocket close frame at once, 1000 for a normal end and 1011 when the voice model connection failed. The server never closed the socket, so the browser learned of the end only after its next frame, 16–104 s late in production logs, and then saw 1006.

### Changed
- A graph map node (a code symbol, or a document cell in a document map) whose text is cut at the byte cap inside a multi-byte character now ends one character earlier, on the character boundary, so its vector is recomputed once after the upgrade.
- The released image is built for `linux/amd64` and `linux/arm64` (`make push` passes `--platform` to ko), so the chart runs on arm64 nodes.

## [2.7.0] - 2026-11-06

### Fixed
- **Cells whose vectors are stored on the cell are found by meaning again after a restart.** Since 2.3.0 the server left a cell out of the vector index when it already carried its embedding and an indexed status. The vector index lives in the process, so after every restart semantic search missed exactly the cells whose vectors had been stored, and found only those embedded since the start. Every public cell is now loaded at start and on every reconcile, from its stored vector, with no embedding call and, once the cell is marked indexed, no write to the cluster. Seeding an unchanged cell again adds nothing to the index, and a cell whose subject changed leaves its old place, so a cell never shows up twice in search results or makes the index grow.
- **A changed text is embedded again.** A cell kept the vector of its old text after its title or content changed (a GitOps apply, an MCP or voice upsert), so semantic search kept answering for the old text until someone removed `spec.embedding` by hand. A stored vector now carries the annotation `xray.triageagent.dev/embedding-fingerprint`, a SHA-256 of the embedding model, its dimension and the text sent to it, and a short hash of the vector itself, written in the same update as the vector. A cell whose text, model, dimension or vector no longer matches is embedded again, and an upsert that changes the text drops the old vector. While the embedder is down, such a cell stays searchable by the vector it carries and is embedded when the embedder answers. No CRD change.
- A cell written before this version has a stored vector but no fingerprint, so it is embedded once more after the upgrade, then not again. The first start can make a few more embedding calls than there are such cells: the startup pass and the controller's first reconciles run side by side, and a write that loses that race is dropped and the next reconcile finds the cell current.
- The ADK adapter module (`adapters/adk`) builds again: its go.mod and go.sum follow the root module they replace, after 2.6.0 and 2.6.1 moved the shared dependencies. `go mod tidy` only; no code change. The `adapters` CI job had failed on `main` with "updates to go.mod needed".

### Changed
- `cellsync.Decide` sets `Index` for every public cell this instance owns whose item is active (an empty or `indexed` status reads as active), unless it is a config or never-embed cell, whether or not it was indexed before. A cell of another instance, a retired cell kept by `KeepRetired` and any other inactive cell are not indexed.
- `index.CellStore.EmbeddingFingerprint(doc, embedding)` is the value of `index.FingerprintAnnotation`, and `SeedOrEmbed` seeds a stored embedding only when the annotation equals it. A host that persists the embedding `SeedOrEmbed` returns writes the annotation in the same update, or the cell is embedded again on every sync. When the embed fails, `SeedOrEmbed` seeds a stale stored vector of the index's dimension and still returns the error.

### Security
- **Every listener is plain TCP: `godebug multipathtcp=0` in the main module (CVE-2026-98432).** Go 1.24 and later ask the Linux kernel for a Multipath TCP socket on every `net.Listen`, and the kernel use-after-free in IPv4 TCP connection setup (CVE-2026-98432, CVSS 9.8, fixed in 5.10.272, 5.15.223, 6.1.190, 6.6.159, 6.12.113, 6.18.56) is reachable, as far as the published exploit goes, only through an MPTCP listener. No client of ours negotiates multipath, so the default is off at build time for every binary; a patched kernel stays the fix. The build is verified with `go version -m <binary>` showing `DefaultGODEBUG=multipathtcp=0`.

## [2.6.1] - 2026-10-30

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
