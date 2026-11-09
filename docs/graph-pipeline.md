# The code graph pipeline: parse, embed, snapshot, serve

A walkthrough of how `xray graph` turns a repository into a queryable graph, and of
where the expensive work happens. For the command reference see the
[code graph guide](graph.md).

## The seam

The embedder appears twice. On the build path it runs over every node in the
repo. On the serve path it runs once, over the query string. The `.gob.gz`
snapshot between them is what makes that asymmetry possible: it carries the
vectors *and* the built ANN index, so loading a map is neither a parse nor an
embed pass.

## Parse

Owner: `internal/codegraph/parser`, `engine/parse.go`, `graph/link.go`.

The walk skips ignored directories and, by default, refuses to descend
symlinked ones. Go gets the rich extractor with resolved `calls`, `imports` and
`inherits` edges; ten other languages (TS/TSX, JS, Python, Java, Rust, Ruby, C,
C++, PHP, C#, Kotlin) go through a generic Tree-sitter `tags` extractor. Markdown
becomes one `Doc` node per file plus a `Section` node per `##` heading. YAML
becomes one `Manifest` node per `---` document, with reference edges to the
images, secrets and configmaps it names.

Call resolution rests on declared evidence only: a bare call in the caller's
own package, a `pkg.Func()` in the package that import names, a typed receiver
in the package its type came from, a chained receiver followed through declared
field and result types. There is no repo-wide name matching. Ambiguity yields
no edge. Nodes are keyed `repo//path::Symbol`.

## Embed

Owner: `internal/codegraph/index/index.go`, `nodeText`.

A node is not embedded as its source. Documentation nodes embed their prose
directly; a `File` node becomes a one-line description; a code node leads with
its signature and path, then folds in a bounded 600-character slice of its
body. The signature leading keeps exact-name queries strong; the body slice is
what lets a paraphrase reach the right function.

The vector is cached by that **text**, not by node identity, because every
rebuild constructs fresh `*Node` objects. Misses go out in one `EmbedBatch`
against any llama.cpp-compatible `/v1/embeddings`, nomic at 256 dimensions
(Matryoshka truncation), with an 8192-byte input cap. Bytes are not tokens, so
a dense node under the cap can still exceed the model's window, and llama.cpp
refuses such an input rather than truncating it; `-embedding-max-input-tokens`
(the window, 512 for nomic-embed-text-v2-moe) cuts it to the longest prefix
that fits, counted by the endpoint's `POST /tokenize`. An endpoint that cannot
count gets the byte-capped text with a warning; a 404 or 405 from `/tokenize`
turns counting off for the life of the process.

## Snapshot

Owner: `internal/codegraph/engine/snapshot.go`, `hnswcodec.go`.

`WriteSnapshot` encodes the header and then the body as two consecutive gob
values into one gzip stream, so a reader can decode the first value and stop.
That is how a server registers dozens of maps without decoding a single graph.

* **Header** (small, always resident): Version, Label, RepoPath, BuiltAt,
  Fingerprint, Nodes, Edges, Summary, SummarySource, SummaryVec.
* **Body** (heavy, loaded on first hit): `Files` (parsed nodes and edges),
  `Cache` (every vector, verbatim), `HNSW` (the built ANN), `Meta` (opaque
  per-node attributes that X-Ray round-trips and never reads).

A `Dims` mismatch in the fingerprint is a hard rejection, because those vectors
are not comparable to a query embedding, and so is a mismatch of either input
cap (`MaxInputChars`, `MaxInputTokens`): a map built before the token window
existed carries 0 and is rebuilt. A model *name* difference only warns:
a llama.cpp sidecar serves whatever GGUF is loaded regardless of its label.

`Body.HNSW` is additive. When absent the loader rebuilds the ANN; when present
it deserializes, skipping the insert loop that is the dominant cold-load cost
(several seconds for a 4.5k-node graph, recurring on every pod start and after
every idle eviction).

**Encryption** (`snapshotkeys.go`). gob keeps strings as plain bytes, so a
snapshot is a readable dump of every node under gzip — and maps travel as OCI
images. `WriteSnapshotWith` can wrap the gzip stream in age (X25519 recipient,
chunked ChaCha20-Poly1305); the file then starts with the age header rather
than the gzip magic, and readers sniff for either, so the name and the image
layout are unchanged. The server holds the identity, builders need only the
public recipient. The key reads, the recipient writes: an identity opens
encrypted maps and re-encrypts them on flush, never downgrading to plaintext,
and leaves plaintext or newly created maps (the notes map) plaintext; only an
explicit recipient encrypts those, so one writable server can serve an
encrypted catalog beside plaintext notes. An encrypted file without a matching
identity is refused with a message naming the flag. This protects the artifact
at rest, not the MCP API.

## Route

Owner: `internal/codegraph/engine/registry.go`.

There is no merged graph. Each map keeps its own graph and index, with its own
HNSW and embedding cache, so ranking never bleeds across projects. An exact
`map=` label is a free fast path with no embedding. Otherwise the map hint and
the query are embedded together and cosine-ranked against every resident
summary vector; an explicit hint collapses fanout to 1.

Idle TTL drops the graph and index. The routing vector stays, so the map is
still reachable and reloads from disk on the next hit. `SummarySource` records
whether the routing text came from an operator-written summary, a README, or
the symbol-name fallback, so the routing input can be inspected.

## Retrieve

Owner: `internal/codegraph/index/index.go`, `lexical.go`.

One `EmbedQuery` per search, the only embedder call on the hot path. Two
channels then run:

* **Semantic**, over HNSW, over-fetched to `max(3k, 20)` so fusion has depth.
  Kind and repo filters apply after the ANN fetch, so the fetch widens to
  compensate.
* **Lexical**, per-word trigram matching behind a `minCoverage` gate on query
  tokens, resolved through the corpus vocabulary rather than through documents.
  Trigrams point at the words containing them and words point at the nodes using
  them, so a token is matched once per candidate WORD and only matching words are
  expanded to nodes. Trigrams have almost no selectivity over documents in code:
  8,830 Go declarations produce only 10,691 distinct grams, and `fun` and `unc`
  appear in 100% of them because every declaration contains `func`. Over the
  28,547-word vocabulary of the same corpus they do discriminate — `fun` is in
  100% of the documents but in a handful of the words.
  Finding the candidate words is the whole cost, and unioning a token's trigram
  lists is the wrong way to do it: a common gram points at thousands of words, so
  the union costs more than the document postings it replaces. Pigeonhole removes
  it — a word clearing `tokThreshold` shares at least `need` of the token's `k`
  trigram positions, so it can lack at most `k-need`, and probing any `k-need+1`
  positions cannot miss it. The probe takes the rarest positions, and the
  thresholds keep it small: 0.90 demands every gram up to `k=9`, so the probe is
  a single posting for tokens shorter than 12 characters and grows by one
  posting per further ten grams (9/10 already clears 0.90); the 0.75 bar for
  short tokens means one posting at `k=3` and two at `k=4`. Words found
  there are verified against all `k` positions, which keeps the pruning exact
  rather than heuristic. For a six-token query on that corpus: 12,723 document
  posting entries before, 1,226 candidate words plus 1,523 word-to-node
  expansions after. It abstains when the query carries no usable tokens rather
  than voting arbitrarily.

They are combined by reciprocal rank fusion at k=60. Rank, not score: cosine
sits in a narrow 0.5 to 0.8 band while the keyword score is an unbounded
integer, so only positions can be meaningfully combined.

Measured on a 12,972-node map, `search_graph` over MCP with novel queries:
p50 29 ms at four words, 36 ms at nine, 35 ms at twenty; p95 44 to 50 ms. Flat
in query length. Query embedding is ~14 ms of that and is the floor; the index
phase is 1 to 2 ms, so the tool's latency is dominated by embedding, not by
retrieval.

The index phase alone, timed across the eight served maps (75 to 12,972 nodes)
by issuing each query twice so the second call hits the embedder's text-keyed
cache, fits `0.66 ms + 0.060 ms per 1,000 nodes`; a 12,972-node map measures
p50 1.4 ms, p95 1.9 ms. Document postings measured `0.65 ms + 0.131 ms per
1,000 nodes` and p50 2.3 ms, p95 5.6 ms on the same harness with one server
running at a time.

Memory is where the vocabulary index pays most. The lexical structures for
8,830 Go declarations hold 50 MB of live heap against 347 MB for document
postings, 7.0x, because per-node trigram caches disappear: a word's gram set is
shared by every node using it instead of being rebuilt and held once per
occurrence. Serving all eight maps (19,130 nodes) the process measures ~410 MB
resident against ~880 MB.

Replies stay lean,
5 pointers by default and 20 at most; `get_graph_node` returns a full body plus
relations in both directions.

## Update

Owner: `internal/codegraph/engine/update.go`.

The serving process holds the live graph, so `xray graph update <file>` is a
thin HTTP client onto it and works in the static build too. Changed files are
re-parsed, and because the cache is keyed by node text only genuinely changed
nodes reach the embedder. A one-file edit lands in roughly 10 to 70 ms.

A CGO-free build indexes the Markdown and YAML in a batch and reports code
files as skipped rather than refusing the whole thing. Paths that resolve to no
served repo come back as `unowned`, so an update that indexed nothing is
visible in the reply.
