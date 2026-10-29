# X-Ray

An **agentic memory substrate** for local and **Kubernetes-native** use. It holds two kinds of memory behind one **MCP** endpoint: authored memory, typed items that agents and people write and that live as `MemoryCell` custom resources, and derived memory, a graph of code, documentation and manifests built from files that already exist. Both are searched by keyword and by meaning, and served over MCP, **REST** and **Gemini Live voice**. One Go binary. It runs no model-driven background work: consolidating memory is the job of the agent that uses it.

## Architecture

X-Ray is one binary that holds two kinds of memory. The **memory server** holds authored memory: items that agents and people write, kept as `MemoryCell` objects. `xray graph` holds derived memory: a graph of code, documentation and Kubernetes manifests, rebuilt from sources that already exist. The server reads the same snapshots, so one endpoint answers `search_memory` and `search_graph` ([Code graph](#code-graph)). Either half runs without the other, on a laptop or in a cluster.

Data crosses three boundaries: from clients into the HTTP and MCP servers, from the server to the Kubernetes API through a controller-runtime cache, and from the server to model providers for embeddings and voice. With a cluster, the `MemoryCell` objects are the store. Without one, the server runs on process memory alone and warns at start.

### Listeners

| Address | Flag | Serves |
|---|---|---|
| :8080 | `--http-address` | REST, server-sent events, the voice WebSocket, sign-in, and the OAuth server and `/mcp` when `--mcp-oauth` is on |
| :8083 | `--mcp-address` | MCP with the shared bearer secret; empty disables it. The chart empties it and serves MCP on the main port |
| :9090 | `--metrics-address` | Prometheus `/metrics`; empty disables it |

### Base components

| Component | Role |
|---|---|
| HNSW | Nearest-neighbour search over embeddings. The memory store keeps one index per subject over its cells, rebuilt from the cells on restart without calling the embedder. Each map carries its own index inside its snapshot. Without an embedding endpoint both fall back to keyword search |
| Tree-sitter | The parser behind the code graph: Go with resolved call, import and inherit edges, ten more languages through a generic extractor. The grammars are C and link into the CGO build, which is why only that build parses ([Code graph](#code-graph)) |
| Kubernetes | `MemoryCell` custom resources are the store: etcd persists, replicates and audits them |
| MCP | The agent interface to both kinds of memory, over streamable HTTP with OAuth 2.1 or a shared secret |

### Out of scope

- Model-driven background work. The server makes no model call except to embed text and to hold a voice session. Consolidation, summarising and generalising belong to the agent, which writes the result back with `upsert_memory`.
- Anything beyond the API and the web UI it serves: sign-in providers other than GitHub, Microsoft Entra and Google, and tenant switching ([Web UI](#web-ui)).

## Memory model

A memory item is a `MemoryCell` in the group `xray.triageagent.dev`, version `v1`, short name `mcell`. Its `spec` carries the item.

```
apiVersion: xray.triageagent.dev/v1
kind: MemoryCell
metadata:
  name: checkout-retries-3f2a9c1e
  namespace: xray
spec:
  id: checkout-retries
  kind: operational_pattern
  scope: public
  subject: checkout
  title: Checkout retries on 503
  content: |
    The payment client retries a 503 three times with jitter.
  confidence: high
  status: active
  tags: [payments]
  createdAt: "2026-10-24T09:00:00Z"
  updatedAt: "2026-10-24T09:00:00Z"
```

| Field | Meaning |
|---|---|
| id | Stable identifier. An upsert with an existing `id` updates the item; without one the server generates an `id` |
| kind | The type of the item, from the table below |
| scope | `public` (the default) or `private` |
| subject | The topic of a public item, or the owner of a private one |
| title, content | Short title and a Markdown body |
| confidence, status | `high`, `medium` or `low`; and a status, `active` on creation |
| tags | Free labels. A tag selects a UI category when a deployment defines one |
| eventDate | When the item happened, RFC 3339 or a bare date |
| staleAfter | Deadline at which the server deletes the item ([Retention](#retention)) |
| origin | Who wrote it: `human`, `observed`, `verified` or `derived` ([below](#provenance)) |
| supersedes, evidenceRefs | Ids of the items this one absorbed, and the ids of the cells or events it was drawn from |
| links | Typed edges to other cells: `toId`, `relation`, `weight`, `evidenceRefs` |
| data | Named blobs. An `image` entry holds a thumbnail, downscaled to 512 by 512 JPEG on upsert |
| embedding | Base64 little-endian float32 vector. Written by the reconciler, never by callers |

### Kinds

| Kind | Holds | Default expiry |
|---|---|---|
| stable_fact | Durable facts | none |
| system_identity | What exists: services, repositories, people | none |
| operational_pattern | Recurring behaviour worth remembering | none |
| active_concern | Open problems being tracked | none |
| watch_item | Things to keep an eye on | none |
| hypothesis | Unconfirmed explanations | none |
| unknown | Known gaps | none |
| event | Something that happened at a time; carries `eventDate` | 30 days |
| recent_change | Releases and changes | 90 days |
| note | Free notes | 90 days |
| episodic | Per-user session memory written by voice | `--episodic-ttl`, 7 days |
| system_configuration | Deployment settings: voice behaviour and UI categories; always private and left out of search | none |
| mental_map | The record of a mounted or carried map ([Storage and restart](#storage-and-restart)) | none |

The first ten rows are the kinds `upsert_memory` lists. The last three are written by the server and the chart.

### Scope

Privacy is a `scope`, separate from `kind`. A `private` item belongs to the owner named by the `subject` it was written with: reading, fetching or deleting it needs the caller to present that owner, and its live events go only to the owner. The owner is stored as a `user:<owner>` tag; the cell's own `subject` names the instance that holds it. A `public` item is shared knowledge, visible on the graph and in search. One predicate, private when `scope` is `private` or the kind is `system_configuration`, guards every read path. Callers that set a scope are trusted for it: the owner is a key the caller presents, not an authenticated identity. Voice writes are always private, with the subject set to a hash of the signed-in user.

### Provenance

Every cell carries an `origin`. A write is `human` when its credential names a person: an MCP token issued through sign-in, or a voice session of a signed-in user. It is `derived` when it does not: the shared MCP secret names nobody. The caller cannot choose; the server sets it. `observed` (written by code from facts) and `verified` (derived, then checked) are for cells that code writes or a cluster admin applies. A cell stored without an origin reads as derived.

A derived write cannot replace a human cell; the upsert returns an error. A person can replace a derived one. `upsert_memory` takes `evidence_refs`, the ids a cell was drawn from, and `refute_memory` follows them: it retires a cell that is wrong (status `stale`), records the reason and evidence under `data.refuted`, and retires every active derived cell that cites it by evidence ref or link, one hop. Human, observed and verified cells are never retired by the cascade.

### Writing rules

A write through MCP or a voice session needs a `kind` from the table above (the server-written kinds are not writable), and a `stable_fact` needs `confidence` `high`: a fact that is stable is one the writer is sure of. A kind filter on search or list must name a searchable kind, so a mistyped one is an error and not an empty result. A cell can be replaced (`supersede_memory` writes the new cell, lists the old id in `supersedes` and retires the old one as `superseded`), expired when it is out of date (`expire_memory`, status `stale`) or refuted when it is wrong.

### Duplicates

An upsert that restates a `stable_fact`, `operational_pattern`, `watch_item` or `hypothesis` lands on the cell it repeats (status `merged`, tags unioned) instead of creating a second one. The cells must share kind, scope and subject, and a derived restatement is never merged into a human cell. Similarity is the overlap of title and content tokens: 0.3 Jaccard, or 0.9 of the smaller text contained in the larger. Texts under eight tokens need 0.8. A short token counts when it has a digit, so "Q3 plan" and "Q4 plan" stay two cells. `--memory-write-dedup=false` turns this off. `consolidate_memory` finds the same clusters among cells already stored and, with `apply`, retires the restatements as `superseded`, listing them on the keeper.

## Storage and restart

Kubernetes holds authored memory. The in-memory indexes are a cache of it and are rebuilt at start. Derived memory ships as gob snapshots.

| What | Where it lives | After a restart |
|---|---|---|
| Authored items | A `MemoryCell` per item, written by the server when it has a cluster client | Read back from the cache. Nothing is lost |
| Embeddings | The cell's `spec.embedding` | Reused. The index is rebuilt without calling the embedding endpoint again |
| Keyword, trigram and HNSW indexes | Process memory | Rebuilt from the cells at start |
| Items written with no cluster client | Process memory | Lost |
| Sign-in sessions | Cells, with token hashes keyed by a secret shared across replicas | Survive |
| MCP OAuth clients | Cells | Survive |
| MCP access tokens | Signed with an Ed25519 key derived from the OAuth client secrets | Stay valid, on any replica. With no client secret the key is random per process and tokens end at a restart |
| Deployment settings | The `system_configuration` cell, watched live | Survive; edits apply without a restart |
| Maps | A gob snapshot, mounted or carried in a cell | Reloaded from the snapshot |

### Gob snapshots

A map is a pre-built graph saved as `<name>.graph.gob.gz` with its HNSW index. Loading a map embeds nothing in the pod. A snapshot reaches the server by one of three routes:

- **A file.** A snapshot in the directory named by `--mental-maps-dir`. The server walks it and loads every snapshot it finds, and watches it for new files.
- **Carried in a cell.** A cell holds the gob in `data.graph`. The cell syncer decodes it and registers the map, and skips the work when the bytes are unchanged.
- **Pulled from a registry.** A cell holds a digest-pinned OCI reference in `data.graphRef` (plain text, `name@sha256:...`), for a map too large for a cell (about 480 nodes). The server pulls the image, takes its one `*.graph.gob.gz` file and registers the map, once per reference, and retries every 30 seconds while the registry is unreachable. A tag is refused. `--registry-pull-secret` names a `dockerconfigjson` Secret for a private registry. The same key the Triage core reads.

Snapshots must match the server's embedding fingerprint: the model, `--embedding-dims` and `--embedding-max-input-chars`.

## Recall

Search ranks every candidate lexically. When an embedding endpoint is configured it also ranks by meaning, and fuses the two lists by rank. Without an endpoint, search is lexical only.

### Lexical rank

A candidate is admitted when it matches enough of the query: every token of a one- or two-token query, all but one of three or more. A token matches as a substring or within one edit of a word (none for tokens of one or two characters), so a misheard word still finds the item. An item's id is part of what it matches, and a query that is exactly an id returns that item first.

The score is a bonus for the whole query in the title, a graded match per token (title above content, exact above fuzzy), a small trigram overlap, and a penalty for each token missed. Tokens are weighted by how distinctive they are among the candidates, as a smoothed inverse document frequency: a word nearly every item contains counts for little, matched or missed, and a word few contain dominates both. For a query of three or more tokens an item that fails the coverage rule is still admitted when the tokens it did match carry 35% of the query's weight, so "what is wrong with the bigquery export" finds the item that names BigQuery.

### Semantic rank

The query is embedded and compared to the HNSW index by cosine similarity. The default model is `gemini-embedding-001` at 128 dimensions, from Vertex AI (`--gcp-project`) or any OpenAI-compatible endpoint (`--embedding-endpoint`). Input is cut to 8000 characters unless `--embedding-max-input-chars` says otherwise. `--semantic-score-gap` drops results more than a given cosine gap below the top hit.

A cell is embedded as its kind, scope, subject, title and content joined by spaces, cut to 1800 bytes on a character boundary (the batch limit of a local embedder is 2048 tokens, and a longer input is rejected outright). The store knows its embedding dimension and refuses a vector or a query of another length. If a search's whole top window is rejected on lookup, because every hit is a cell deleted or retired on another replica and still indexed here, the search ranks every cell exactly instead, so the cells that qualify are not starved. The embedding and index phases of each semantic search are reported separately as `xray.memory.search.phase.duration`.

### Fusion

The two lists are combined by reciprocal rank, `1/(60+rank)` summed over the lists an item appears in. Ranks are used because cosine similarity and lexical scores are on different scales. An item in one list contributes one term. By default every query fuses both lists. `--search-lexical-shortcircuit` restores the older behaviour of skipping the embedding call when a keyword hit already covers the query.

### Shaping

The fused pool, over-fetched to four times the limit (at most 60), is shaped before it is cut. Near-duplicate facts of the four kinds above collapse to the best-ranked one, which reports how many it absorbed in `collapsed`. The pool is then re-ranked: each item scores its rank times a boost of 1.7 for an active watch item that has not expired, times a mild recency factor that falls from 1 to 0.85 with a seven-day half-life. It reorders close results; it does not override relevance. `--search-recall-shaping=false` returns the fused order. When a cross-encoder is installed it orders the pool in place of the re-rank.

Kind-filtered semantic search scans the items of that kind exactly instead of asking the graph for nearest neighbours and dropping the wrong kinds, which returned nothing for a kind that is a small share of the store. Query embeddings are cached in a bounded LRU, so a repeated search does not call the embedder again.

Searches scope to a subject and also return public items with no subject. The store also has opt-in hooks for one-hop link expansion, source collapse and a cross-encoder rerank; they are Go calls for programs that embed the package, with no flag or chart value.

### Result limits

| Tool | Ceiling | Flag |
|---|---|---|
| search_memory | 10 | `--mcp-search-memory-max-limit` |
| list_memory | 20 | `--mcp-list-memory-max-limit` |
| recent_memory | 20 | `--mcp-recent-memory-max-limit` |

Each ceiling holds whatever limit the caller asks for. `--headroom-content-limit` bounds the characters of each result's content, to control the cost of voice context.

## Interfaces

### MCP tools

Read tools need the scope `xray:memory.read`; `upsert_memory`, `delete_memory`, `refute_memory`, `consolidate_memory`, `expire_memory` and `supersede_memory` need `xray:memory.write`. The shared bearer secret carries both.

| Tool | Arguments | Does |
|---|---|---|
| get_memory | none | Counts of items by kind |
| list_memory | `kind`, `subject`, `limit` | Browse items of a kind |
| search_memory | `query`, `subject`, `kind`, `limit` | Ranked search ([Recall](#recall)) |
| fetch_memory | `ids`, `subject` | Exact retrieval of items, in the order asked |
| recent_memory | `subject`, `limit` | The newest episodic memories of a subject |
| upsert_memory | `id`, `kind`, `scope`, `title`, `content`, `subject`, `confidence`, `event_date`, `tags`, `evidence_refs`, `image_data`, `image_mime` | Create an item, or update the one with that `id`; a restated fact comes back `merged` with the id it landed on |
| delete_memory | `id`, `subject` | Delete an item; a private item needs its own subject |
| refute_memory | `id`, `reason`, `evidence`, `subject` | Retire a wrong item and the derived items that cite it ([provenance](#provenance)) |
| expire_memory | `id`, `subject` | Retire an out-of-date item (status `stale`); it leaves search and lists |
| supersede_memory | `old_id`, `new_id`, `kind`, `scope`, `title`, `content`, `subject`, `confidence`, `tags`, `evidence_refs` | Write a replacement that lists `old_id` in `supersedes` and retire the old item. A derived replacement cannot supersede a human item |
| get_memory_context | `anchor`, `max_depth` | The cells an anchor (an id, a subject or part of a title) reaches over links, by spreading activation: 0.6 per hop times the link weight, up to 3 hops by default and 8 at most. Shared cells only |
| consolidate_memory | `subject`, `apply` | Report near-duplicate facts; with `apply`, retire the restatements |

When a map is loaded, the same endpoint also serves the graph tools, all read scope ([Code graph](#code-graph)).

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "tools/call",
  "params": {
    "name": "upsert_memory",
    "arguments": {
      "kind": "operational_pattern",
      "title": "Checkout retries on 503",
      "content": "The payment client retries a 503 three times with jitter.",
      "subject": "checkout",
      "confidence": "high",
      "tags": ["payments"]
    }
  }
}
```

### Go packages

X-Ray is also a Go library, importable as `github.com/triageagent-dev/x-ray/v2`. Three packages cover the two ways an agent uses it, and they share one interface per kind of memory, so an agent written against the interface runs on either.

| Package | What it is | Implements |
|---|---|---|
| memory | The memory store, run inside the program. Typed items, keyword and trigram index, an HNSW index when given an embedding endpoint, merging of restated facts, recall shaping and provenance. `WithFile` keeps it across restarts, `WithOrigin` says who the program's writes count as | `Memory` |
| codegraph | The code graph, run inside the program. `Build` parses and embeds repositories, `UpdateGraph` re-indexes edited files, `Save` and `Load` use the snapshot format the server reads, and `Handler` offers the graph over MCP. Parsing needs the cgo build; loading and searching a snapshot do not | `CodeGraph` |
| xray | A client for a running X-Ray over MCP, with typed calls for both kinds of memory. `Connect` takes a URL and an optional bearer token; `Call` reaches any tool by name | `Memory`, `CodeGraph` |

| Interface | Methods |
|---|---|
| Memory | `Search`, `List`, `Fetch`, `Get`, `Summary`, `Recent`, `Upsert`, `Delete`, `Refute`, `Expire`, `Supersede`, `Consolidate`, `Context` |
| CodeGraph | `SearchGraph`, `SearchGraphDocs`, `Node`, `Impact`, `Risk`, `GraphStats`, `UpdateGraph` |

A refusal, such as an unknown kind or a missing item, is a `ToolError` in all three, and `IsNotFound` tests for the missing item. The embedded store applies the same rules the server does at its boundary: a kind from the vocabulary, high confidence on a stable fact, a subject on a private item. The credential decides how a write is classed. Over MCP an OAuth token that names a person records `human` and the shared secret `derived`; in process, the program chooses with `WithOrigin`.

To make X-Ray the memory of an agent, it is used in three places: a search before the agent answers, memory tools while it answers, and a write afterwards that cites the items it drew on, so that refuting one retires the rest. A separate adapter module implements the Google ADK memory service on either an embedded store or a client, so X-Ray plugs into the ADK runner and its `load_memory` and `preload_memory` tools. Each user's conversations are stored as private notes under the pair of app and user, and shared memory is added to every search.

The engine that the server's store and the `memory` package both run on is public too, for a program that wraps it in its own host. The host brings its own lock, visibility rule, retention function and kind vocabulary; the packages read no global state and use no Kubernetes client.

| Package | Holds |
|---|---|
| memstore | `Item`, `Store` and `Searcher`; tags; private ownership, which is a `user:<hash>` tag with the cell's `subject` naming the instance that holds the item; event time; near-duplicate detection; the kind vocabulary; `Lexicon`, the vocabulary index on its own |
| memstore/index | The HNSW cell index, the embedder and the query cache |
| memstore/hub | The live event hub. A `Transport` carries events between replicas; without one the hub is local |
| memstore/cellsync | The decisions made when a cell is reconciled into a store: keep it, delete it at its deadline, forget it, embed it or leave it out of the vector index. Also the conversion of a private cell written with the owner in `subject` to the owner tag |
| memstore/memtest | Fixtures for a host's tests |

### REST

The memory routes are read-only. Writes arrive by MCP, by voice, or as objects applied with `kubectl`.

| Route | Auth | Returns |
|---|---|---|
| GET /v1/memory | session | Items, filtered by query, kind and subject |
| GET /v1/memory/summary | session | A light index: id, title, kind and tags, no content |
| GET /v1/memory/{id} | session | One item |
| GET /v1/memory/item | session | One item by `id` query parameter, for ids that contain a slash |
| GET /v1/memory/neighbors | session | The nearest items to `id` by embedding, up to `top` (default 20, at most 50), and the links among them that mark sub-clusters. A pair is linked only when the two are closer to each other than to the centre, so no similarity threshold applies. Shared, active items only |
| GET /v1/memory/events | session | Server-sent events (below) |
| GET /v1/user/notes | session | The signed-in user's private notes |
| GET /v1/categories | none | UI categories from the configuration cell |
| GET /v1/auth/providers, /me, /logout | none, session | Sign-in state |
| GET /v1/version, /healthz | none | Build version and liveness |
| GET /v1/voice | session | WebSocket upgrade for a voice session |

### Live events

`GET /v1/memory/events` carries two event types: a *hit* (the item ids, query and kinds of a recall that just ran) and a *change* (an upsert or delete). A recall publishes a hit when it is made by `search_memory` or `get_memory_context` over MCP, or by the voice search and list tools. Private items are never named: a hit lists the shared items a search returned, and a private change goes only to its owner. A client connected to one replica sees the events of that replica ([Replicas](#replicas)).

### How a recall shows up

The web UI turns a hit into light, the way optogenetics does: a neuron made to carry a light-gated channel fires when blue light reaches it. The kinds in the event name the region, so a blue fibre lights it even when the recalled cell is not one of the nodes drawn. The cells in the region wash blue in the order the light reaches them, nearest the fibre first. The recalled cells fire hardest, with a double flare, three spike rings and an afterglow of a few seconds. A pulse of light then runs from each recalled cell along its links to the memories it connects to. It is laid over the category-coloured flash the graph already draws, and a repeat hit fires again. The region treatment is one fast attack and a smooth decay, about two pulses a second at most, because flicker over a large area is a photosensitivity risk; reduced motion gets one slow wash. The embedded web UI listens to this stream; a Triage deployment carries the same events over a WebSocket.

## Access

Every route under `/v1` needs a session except the sign-in routes, `/v1/version`, `/v1/categories`, `/healthz`, `/manifest.json` and `/sw.js`. `--dev` turns authentication off and is for local use only; every request without a session is then the built-in user `dev`, so voice, personal notes and the UI share one identity.

| Path | How |
|---|---|
| GitHub sign-in | `--github-client-id`, `--github-client-secret`; `--github-org` restricts access to an organisation and `--github-team` to a team |
| Microsoft Entra sign-in | `--azure-client-id`, `--azure-client-secret`, `--azure-tenant-id` |
| Google sign-in | `--google-client-id`, `--google-client-secret`, `--google-domain` (Workspace domain, matched on the ID token `hd` claim) |
| Session | A 24-hour `xray_session` cookie. The same token is accepted as `Authorization: Bearer`, and on a WebSocket upgrade as a subprotocol, because a browser cannot set headers on that handshake |
| Session to another origin | A login may hand its token to a UI on a different host through `?ui=<origin>`, only for origins in `--ui-callback-origins`. Empty by default |
| MCP, shared secret | A Secret named by `--mcp-token-secret`, key `token`. Full scope |
| MCP, OAuth 2.1 | `--mcp-oauth`. The server is its own authorization server: discovery under `/.well-known`, `/register`, `/authorize`, `/token`, with PKCE. Tokens are audience-bound JWTs with the read and write scopes; `--mcp-token-ttl` sets their lifetime. The user must be signed in to the web app first |
| Write access | `writeAllowedUsers` in the configuration cell limits who may hold the write scope |

`--public-url` is the externally reachable base URL. It scopes CORS, the OAuth callbacks and the origin check on the voice WebSocket. `--trusted-proxy-hops` says how many proxies in front of the server append the client address, which decides how `X-Forwarded-For` is read for rate limiting. `--max-request-bytes` caps request bodies.

## Voice

A browser upgrades `GET /v1/voice` to a WebSocket and the server opens a Gemini Live session on its behalf. Two goroutines per session relay audio each way, and tool calls from the model run in parallel and return as tool responses. The server rejects a WebSocket whose `Origin` is not `--public-url`.

Each session reads the `system_configuration` cell at its start, so edits apply to the next session without a restart.

| Setting | Meaning |
|---|---|
| systemPrompt, greeting | What the model is told and how it opens |
| voiceName, voiceModel | Override the `--voice-name` (default `Kore`) and `--voice-model` (default `gemini-3.8-live`) flags |
| enabledTools | The allowlist of tools the model may call |
| writeAllowedUsers | Users whose sessions may write memory |
| maxUserDailyAudioTokens | Per-user audio token cap per UTC day; overrides the flag when non-zero |
| maxItemContentChars | Bound on each memory item's content in tool results |
| affectiveDialog, proactiveAudio | Gemini Live options, native-audio models only |

The voice tools are `search_memory`, `list_memory`, `get_memory`, `fetch_memory`, `recent_memory`, `upsert_memory`, `save_memory`, `delete_memory` and `end_session`. Voice writes are private to the user. When a map is loaded the graph tools are available to the model too.

### Limits

- `--user-daily-audio-token-limit`: audio tokens, in and out, a user may spend per UTC day. Default 500000; 0 is unlimited. At the cap the model is asked to save and say goodbye, and the server closes the session if it has not ended within 45 seconds. The remaining amount is pushed to the browser after each turn.
- `--max-concurrent-voice`: simultaneous sessions across all users. Default 20; 0 is unlimited. A slot is reserved before the session starts, so parallel starts cannot pass the check together.
- `--voice-max-output-tokens`: output cap per turn, 8192 by default, so a runaway turn fails fast.

Voice needs Gemini credentials: `--gcp-project` for Vertex AI, or `--gemini-api-key` for the Google AI API. Without either, the voice route is off.

## Web UI

The web UI is part of the binary. It is a client of the API above, built as a static export and embedded, and the server serves it at `/` on the same origin as the API. API paths, `/healthz`, the PWA files, `/mcp` and the OAuth endpoints go to the server; everything else is the UI. It runs the same locally and in a cluster: `make run` opens it on port 8080 with sign-in off, and the chart serves it through the same Service. `--ui=false` serves the API alone, and a binary built with `UI=0` has no UI at all.

One origin is required. The voice WebSocket URL is built in the browser from its own host, the session cookie has no `Domain`, and the server checks the WebSocket's `Origin` against `--public-url`. Embedding gives it, so `--public-url` is simply the address users reach, and it is also the OAuth callback base.

Each document is served with a `script-src` that names the hashes of its own inline scripts, so no script runs without one; `'unsafe-inline'` is allowed for styles only. Hashed bundles are cached for a year, and HTML is revalidated so an upgrade is seen at once.

The UI speaks this server's own API, `/v1`: voice at `/v1/voice`, live events as server-sent events, per-user notes, no tenants. The same sources build for a Triage deployment, which serves the same surface under `/v2` with a WebSocket event stream and tenants. The code graph has a smaller page of its own at `/ui` on `xray graph serve`.

### Categories

The UI groups items into categories. With none configured it shows the native taxonomy: Concerns (`active_concern`), Patterns (`operational_pattern`), Facts (`stable_fact`), Identity (`system_identity`), Events (`event`) and Notes (`note`). A deployment overrides this by setting `ui.categories` in the configuration cell. The server returns them from `/v1/categories` before sign-in.

| Field | Meaning |
|---|---|
| kind | The kind the category shows |
| tag | Narrows the kind to items with this tag. Tagging is strict: an item without a matching tag falls under "Other" |
| label, id, icon, color | How the category is named and drawn |
| tier, angle | Position on the memory graph: radial distance and direction |
| scope | Limits the category to public or private items |

## Code graph

`xray graph` builds derived memory: a graph of source code, documentation and Kubernetes manifests, embedded for search and saved as a snapshot. Nothing in it is authored, so it can be thrown away and rebuilt. It answers the structural questions that text search cannot: who calls this symbol, what a change reaches, where a decision was written down. It needs no cluster and no database, and it sends nothing off the host except text to the embedding endpoint you point it at.

### Two builds

Only the Tree-sitter parser needs a C toolchain, so the build tag marks the boundary. The static build is the one deployed to a cluster. Asked to parse, it exits with a message pointing at the CGO build.

| Sub-command | Does | Build |
|---|---|---|
| serve | Parse repositories on start, hold the graph in memory, re-index on edit, serve MCP with a small UI | CGO |
| snapshot | Write a snapshot of one or more repositories | CGO |
| dump | Print what the parser extracts from a directory, or read a snapshot | CGO for a directory |
| serve-snapshots | Serve pre-built snapshots | static or CGO |
| servicemap | Load a service catalogue as a map | static or CGO |
| services | Derive a service catalogue from a code map | static or CGO |
| search, impact | Search or compute a blast radius straight over snapshots, with no server | static or CGO |
| update, status, build | Re-index changed files, report, rebuild, against a running server | static or CGO |
| install | Write Claude Code skills and a post-edit hook into a repository and print the MCP snippet | static or CGO |
| keygen | Make an age identity for encrypted snapshots | static or CGO |

### What it indexes

- **Code.** Go with resolved call, import and inherit edges; TypeScript, TSX, JavaScript, Python, Java, Rust, Ruby, C, C++, PHP, C# and Kotlin through a generic tags extractor that stores symbol bodies and call edges.
- **Documentation.** Markdown: a node per file and a node per second-level heading.
- **Manifests.** YAML, Helm templates included: a node per document, with reference edges to images, Secrets and ConfigMaps. It reads files and never contacts a cluster.
- **Infrastructure.** Compose files, Dockerfiles, application config and SQL DDL, which feed the service catalogue that `services` derives.

Node names are qualified by repository, `repo//path::Symbol`, so many repositories share one endpoint without collisions.

The embedder is called twice in the life of a map: over every node when it is built, and over the query string when it is searched. The snapshot between them carries the vectors and the built index, so loading a map is neither a parse nor an embed pass.

### Parse

The walk skips ignored directories and does not descend into symlinked ones unless asked. Call resolution rests on declared evidence only: a bare call in the caller's own package, a `pkg.Func()` in the package the import names, a typed receiver in the package its type came from, a chained receiver followed through declared field and result types. There is no repository-wide name matching, and an ambiguous call yields no edge.

### Embed

A node is not embedded as its source. Documentation nodes embed their prose; a file node becomes a one-line description; a code node leads with its signature and path, then folds in the first 600 characters of its body. The signature keeps exact-name queries strong and the body slice lets a paraphrase reach the function. Vectors are cached by that text, not by node identity, because every rebuild constructs fresh nodes; misses go out in one batch. The defaults are 256 dimensions with Matryoshka truncation and an input cap of 8192 characters, so an oversized node is truncated and not rejected.

### Snapshot

A snapshot is a gzip stream holding two gob values, a header and a body, so a server can register many maps by decoding only the headers.

| Part | Holds | Loaded |
|---|---|---|
| Header | Version, label, source path, build time, embedding fingerprint (model, dimensions, input cap), node and edge counts, routing summary and its source, routing vector | at start, always resident |
| Body | Parsed nodes and edges, every vector verbatim, the built HNSW index, opaque per-node attributes that are round-tripped and never read | on the first hit |

A mismatch in dimensions between the snapshot and the server is a hard rejection, because those vectors are not comparable with a query embedding. A difference in the model name only warns, since an embedding server answers with whatever model it has loaded. When the stored index is absent the loader rebuilds it; when present it is deserialised, which skips the insert loop that dominates a cold load. A snapshot built with `-no-embed` carries no vectors and is searched lexically only.

### Encrypted snapshots

Strings in a gob are plain bytes, so an unencrypted snapshot is a readable dump of every node, and maps travel as OCI images. A snapshot can be written through age instead. The file keeps its name and layout and starts with the age header in place of the gzip magic; readers sniff for either, so one directory can hold both.

The key reads and the recipient writes. A builder needs only the public recipient (`-snapshot-recipient`), so a CI job never holds a secret. A server holds the identity (`-snapshot-key`, or `--mental-maps-snapshot-key` on the memory server). A map read encrypted is re-encrypted to the identity's own public key on every flush, and the flush refuses rather than write plaintext. The identity encrypts nothing else, so a plaintext map and the notes map stay plaintext unless a recipient is also given. An encrypted map found with no matching key is skipped with a warning that names the flag, and the other maps still serve. This protects the file at rest. The running server holds every map in memory and answers in the clear, so access to the server stays the boundary for the data itself.

### Route

There is no merged graph. Each map keeps its own graph and index, with its own HNSW and embedding cache, so ranking never crosses projects. An exact `map` label is a fast path that needs no embedding. Otherwise the map hint and the query are embedded together and cosine-ranked against the summary vector of every resident map; an explicit hint reduces the fan-out to one, and `--mental-maps-route-fanout` sets it otherwise. An idle map is evicted after `--mental-maps-ttl-min` minutes; its small routing vector stays, so it is still reachable and reloads from disk on the next hit. The summary comes from an operator-written description, a README, or the symbol names, in that order, and the header records which.

### Retrieve

Each search makes one query embedding, the only embedder call on the serve path, and then runs two channels.

- **Semantic.** HNSW, fetched at three times the limit and at least 20, so fusion has depth. A kind or repository filter applies after the index returns, so the fetch widens to compensate.
- **Lexical.** Per-word trigram matching behind a coverage gate: a query of one or two tokens must match all of them, a longer one two thirds, rounded up. Trigrams point at the words that contain them and words point at the nodes that use them, so a token is matched once per distinct word and only matching words are expanded to nodes. In code, trigrams discriminate poorly across declarations (every declaration contains `func`) and well across the vocabulary, which is why the index is built over words.

The channels are combined by reciprocal rank fusion with k = 60. Rank is used and not score, because cosine similarity sits in a narrow band while the keyword score is an unbounded integer. Replies are lean: 5 pointers by default and 20 at most (up to 100 when a kind, attribute or ordering selects a set), and `get_graph_node` returns a full body with its relations in both directions.

### Update

The serving process holds the live graph, and `update` is a thin HTTP client onto it, so it works in the static build too. Changed files are re-parsed, and because the cache is keyed by node text only nodes that changed reach the embedder. A static build indexes Markdown and YAML in a batch and reports code files as skipped. A path that resolves to no served repository, through `../` or a symlinked directory the build did not follow, comes back as `unowned` and is never read.

### Graph tools

These are served by `xray graph serve` and `serve-snapshots`, and by the memory server when it loads maps (`--mental-maps-dir`), on the same endpoint as the memory tools. A cell that carries a graph in `data.graph` registers as a map without a restart. Every read tool is annotated read-only, non-destructive and idempotent.

| Tool | Arguments | Does |
|---|---|---|
| search_graph | `query`, `limit`, `map` | Find code and manifests by meaning |
| search_graph_docs | `query`, `limit`, `map` | Find documentation prose by meaning |
| get_graph_node | `qn` | The full body of one node and its relations both ways: `calls`, `called_by`, `imports` |
| get_graph_impact | `seeds`, `depth`, `max_nodes` | The blast radius of changing symbols |
| get_graph_risk | `qn`, `depth`, `max_nodes` | LOW, MEDIUM or HIGH change risk for one symbol |
| get_graph_stats | `map` | Node and edge counts, languages, embedded count, router summary |
| list_skills, get_skill | none, `name` | Operating procedures carried as document nodes in a map |
| update_code_graph | `paths` | Re-index changed files. Refused when the snapshot directory is read-only |

### Agent notes

Three more tools keep short notes of what was asked, answered and decided, so a later session can read them back. They need a server started with `-writable`. Notes go to one map named by `-memory-map` (default `session`), created on first use and never mixed into a code or catalogue map.

| Tool | Arguments | Does |
|---|---|---|
| remember | `text`, `title`, `attrs`, `ttl` | Write one note, embedded and persisted at once. It expires after `-memory-ttl` (7 days) unless `ttl` says otherwise (`30d`, `24h`, `never`). A note about a record a map holds carries `about=<title or handle>` |
| recall | `query`, `limit` | Read notes back, newest first or ranked by `query`. A single-word query returns notes tagged `about` that word first. Each reply says that notes are what people said and not the record |
| forget | `qn` | Remove one note. Refuses anything that is not a note in the notes map |

Expired notes are swept on read and hourly. These notes are separate from `MemoryCell` memory ([Memory model](#memory-model)): they are not typed, scoped or shared through the cluster, and they live in a snapshot file.

### Access

Both graph servers listen on loopback unless told otherwise (`127.0.0.1:8084` for `serve`, `127.0.0.1:8085` for `serve-snapshots`). Every request except `/healthz` passes three checks.

| Check | Rule |
|---|---|
| Host | On a loopback address only `localhost`, `127.0.0.1` and `::1` are accepted; `-allowed-hosts` adds names. This stops a web page from rebinding its own hostname to the loopback address and driving the server from a browser |
| Origin | A browser request from another origin is refused unless `-allowed-origins` lists it. MCP clients, curl and the hook send no Origin and pass |
| Token | `-auth-token-file` (or `$XRAY_AUTH_TOKEN_FILE`) requires `Authorization: Bearer`. Without it nobody is asked for a credential, and a server given a wider `-addr` logs a warning. The browser UI takes the token once as `/ui?token=`, keeps it in an HttpOnly cookie and drops it from the address bar |

The memory server's own access rules ([Access](#access)) apply when it serves maps.

### In a cluster

Two ways to run maps. The memory server takes them as cells that carry a graph or point at one in a registry (`data.graphRef`), or as files in `--mental-maps-dir`; it needs that flag, even for an empty directory, to turn the map registry on. There is one chart. A maps image is a scratch image holding the snapshots under `/maps`; the build target never pushes it.

## Retention

Memory decays by deadline. The only background work in the server is the deletion of expired items, and it makes no model call.

- **Default deadlines.** A newly created `event` gets 30 days, a `recent_change` or `note` 90 days, a `hypothesis` one day and an `unknown` seven, unless the caller sets `staleAfter`. Other kinds never expire by default. The defaults are fixed in code. Restating a hypothesis or unknown (the write merges into the existing cell) renews its deadline.
- **Deletion at the deadline.** An item past `staleAfter` is hard-deleted when its cell is next decided, which is when the deadline falls: the controller requeues the cell for that moment. Every `--retention-sweep-interval` (one hour; 0 disables) a sweep deletes any item the requeue missed, such as one whose cell was never reconciled. Durable and configuration kinds are skipped, and a refuted or superseded item is kept as an audit trail until its own deadline.
- **Episodic memory.** Older than `--episodic-ttl` (7 days) it is dropped from `recent_memory` at read time, and a reaper deletes the cell.

A delete of a cell that another replica already removed counts as done, so the sweep is safe on several replicas.

## Replicas

The chart runs one replica, and one is the supported setting. Cells, sessions and tokens are shared through the cluster. The remaining state is held by the pod.

| State | Across replicas |
|---|---|
| Cells | Shared. Every replica reads the same cache |
| Sign-in sessions, OAuth clients | Shared, through cells and a common HMAC key |
| MCP access tokens | Shared when an OAuth client secret is set: every replica derives the same signing key from it |
| Search indexes | Each pod builds its own from the cells and follows the cluster watch: a cell another replica writes, retires or deletes is indexed or dropped here |
| Live change events | Every pod announces each change it sees through the watch, so a browser sees changes made on any replica |
| Live search-hit events | Per pod. A browser sees the search pulses of the replica it is connected to |
| Items written without a cluster client | Per pod |
| Voice sessions | Bound to the connection, so to one pod |
| Voice quota counters | Held per pod and mirrored to the configuration cell's status, so several replicas do not add up |
| Retention | Runs on every replica; deletes are idempotent |

There is no leader election, because the server has no work that must run once. Search-hit events have publish hooks (`SetHitPublishHook`) and an apply call for remote events, but no transport is wired to them, so those pulses stay on the pod that served the search.

## Configuration

Flags set the baseline. The configuration cell and the chart's `values.yaml` override or supply them as noted. `xray -help` lists every flag.

| Group | Flags |
|---|---|
| Listeners | `--http-address` :8080, `--mcp-address` :8083, `--metrics-address` :9090, `--namespace` xray, `--log-level` info |
| Embeddings | `--embedding-endpoint`, `--embedding-model` gemini-embedding-001, `--embedding-dims` 128, `--embedding-max-input-chars`, `--gcp-project`, `--gcp-location` europe-west4, `--semantic-score-gap` |
| Search | `--search-lexical-shortcircuit`, `--search-recall-shaping` true, `--memory-write-dedup` true, `--headroom-content-limit`, `--mcp-search-memory-max-limit`, `--mcp-list-memory-max-limit`, `--mcp-recent-memory-max-limit` |
| Maps | `--mental-maps-dir`, `--mental-maps-snapshot-key`, `--registry-pull-secret`, `--mental-maps-route-fanout` 1, `--mental-maps-ttl-min` 0 |
| Graph server | `xray graph serve-snapshots`: `-snapshot-dir`, `-addr` 127.0.0.1:8085, `-writable`, `-memory-map` session, `-memory-ttl` 7d, `-skills-map` skills, `-graph-ttl-min` 0, `-route-fanout` 1, `-embedding-endpoint` http://localhost:8090, `-embedding-model` nomic, `-embedding-dims` 256, `-embedding-max-input-chars` 8192, `-snapshot-key`, `-auth-token-file`, `-allowed-hosts`, `-allowed-origins`. `serve` takes `-repo` and listens on 127.0.0.1:8084 |
| Voice | `--gemini-api-key`, `--voice-model`, `--voice-location`, `--voice-name`, `--user-daily-audio-token-limit`, `--max-concurrent-voice`, `--voice-max-output-tokens`, `--voice-affective-dialog`, `--voice-proactive-audio` |
| Sign-in and MCP | `--public-url`, `--github-client-id`, `--github-client-secret`, `--github-org`, `--github-team`, `--azure-client-id`, `--azure-client-secret`, `--azure-tenant-id`, `--google-client-id`, `--google-client-secret`, `--google-domain`, `--ui-callback-origins`, `--mcp-token-secret`, `--mcp-oauth`, `--mcp-resource-url`, `--mcp-token-ttl` 15m, `--dev` |
| Retention | `--episodic-ttl` 168h, `--retention-sweep-interval` 1h |
| Requests | `--max-request-bytes` 1 MiB, `--trusted-proxy-hops` 0 |
| Tracing | `--otel-endpoint`, `--otel-project`, `--otel-token-secret` |

### Chart

| Value | Effect |
|---|---|
| replicaCount | 1 |
| namespace | A namespace dedicated to X-Ray: the server may list and watch every Secret in it |
| publicURL | Passed as `--public-url` |
| args | The flag list given to the container |
| mcp.* | Token Secret name, OAuth on or off, resource URL, token lifetime, optional dedicated hostname |
| dev | Adds `--dev`: sign-in off, any origin accepted. For a first look through a port-forward only; without it the server refuses to start until a sign-in method and `--public-url` are set |
| github.secretName, azure.secretName, google.secretName | The Secret holding the sign-in client id and secret. Empty by default, so a fresh install needs no Secret of its own besides the MCP token |
| imagePullSecrets | Secrets for pulling the images from a private registry |
| ingress, httpRoute | An Ingress, or a Gateway API route, in front of the server |

The chart installs the `MemoryCell` CRD from its `crds` directory on first install. Helm does not upgrade CRDs, so apply a changed CRD by hand.

## FAQ
### Does it need a Kubernetes cluster?
No. Without a kubeconfig the server runs on process memory and warns at start, and items live until the process stops. With a cluster, items persist as cells and survive restarts. A program can also embed the memory store and the code graph as Go packages and need no server at all ([Interfaces](#interfaces)).
### Does it need an embedding model?
No. Without `--embedding-endpoint` or `--gcp-project` search is lexical only: phrase, token and trigram scoring. The semantic list and the HNSW index appear when an embedder is configured.
### What happens when the pod restarts?
The cache reads every `MemoryCell` again and the indexes are rebuilt from them. Embeddings are stored in the cells, so no embedding calls are repeated. Sessions and MCP tokens stay valid. Only items written with no cluster client are lost.
### Why are cells custom resources?
etcd already persists, replicates and audits them, and a cell is visible to `kubectl`, RBAC, GitOps and any other controller. The server needs no database of its own.
### How do I write memory?
With the MCP `upsert_memory` tool, through a voice session, or by applying a `MemoryCell` with `kubectl` or Helm. The REST memory routes are read-only.
### Who consolidates memory?
The agent that uses it. X-Ray stores, indexes and serves items, deletes expired ones, and folds exact restatements of a fact into one cell; it calls no model. An agent that wants a summary reads items with `search_memory` and writes the result with `upsert_memory`. `consolidate_memory` reports and retires near-duplicates, and `refute_memory` retires a wrong item with what was derived from it.
### Can a model overwrite what a person wrote?
No. The server marks a write `human` only when its credential names a person, and refuses a `derived` write onto a `human` cell. A caller cannot claim an origin; the shared MCP secret is always derived.
### Can I run more than one replica?
Cells and their change events follow the cluster across replicas, but voice quota counters and search-hit pulses are per pod ([Replicas](#replicas)). Run one replica until those cross replicas.
### Can I run only the code graph?
Yes. `xray graph serve` needs no cluster, no database and no sign-in, and listens on loopback. It needs an embedding endpoint, because search is by meaning. The memory server is a separate process and nothing in the graph depends on it.
### Why are there two builds?
Parsing source needs the Tree-sitter grammars, which are C. The static build has no C code and is what a cluster runs; it serves snapshots, searches them and keeps notes. The build with the parser adds the sub-commands that read source: `serve`, `snapshot` and `dump` on a directory.
### Where is the web UI?
In the binary. The server serves it at `/`, so `make run` or the chart is all it takes, and `--ui=false` turns it off. MCP, REST and voice do not depend on it. The code graph has a smaller page of its own at `/ui` on `xray graph serve`.
