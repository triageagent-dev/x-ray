package engine

// Offline graph snapshots let the read-only `serve-snapshots` server
// (Mode B — online long-term memory, a cache in front of RAG) load a fully
// embedded per-project code graph from disk with ZERO embedder calls at startup.
// A snapshot is one file per project holding the parsed per-file Results plus the
// text→vector embedding cache, so loading is parse-free and embed-free; the
// embedder is only ever used to embed the *query* at search time.
//
// The `snapshot` subcommand builds these files (the one and only embed pass).

import (
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"filippo.io/age"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/docs"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/graph"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/parser"
)

// docTitleSampleCap bounds how many Doc titles statsSummary samples for a
// docs-only map's fallback — mirrors the 30-symbol cap the code-map branch
// already uses, for the same reason (a short, cheap-to-embed gist, not an
// index of every document).
const docTitleSampleCap = 30

// snapshotVersion is bumped when the on-disk format changes incompatibly.
// v2 added Nodes/Edges to the header (for /status without loading the body).
const snapshotVersion = 2

// SnapshotVersion is snapshotVersion for callers outside the package that
// construct a Snapshot by hand (the vectors subcommand's test fixture): a
// hand-built header with a stale Version is rejected at read, and a test that
// hardcodes the number breaks silently on the next bump instead of tracking it.
const SnapshotVersion = snapshotVersion

// maxSummaryChars caps a project's router summary before it is embedded — the
// routing vector only needs a gist, and short text keeps the embed cheap.
const maxSummaryChars = 1200

// EmbedFingerprint identifies the embedding configuration a snapshot was built
// with. A snapshot is only valid for a server using the SAME fingerprint — a
// mismatch means the vectors are in a different space and must not be trusted.
type EmbedFingerprint struct {
	Model         string
	Dims          int
	MaxInputChars int
	// MaxInputTokens is the model's input window the text was cut to (0 = no
	// cut). Additive: a snapshot written before it decodes 0, so it compares
	// unequal to a server that cuts, and is rebuilt.
	MaxInputTokens int
}

// SnapshotHeader is the small, cheap-to-read prefix of a snapshot: enough to
// register a project and route to it (Summary + SummaryVec) without decoding the
// whole graph body.
type SnapshotHeader struct {
	Version     int
	Label       string // project / repo namespace label
	RepoPath    string // absolute repo root at build time
	Fingerprint EmbedFingerprint
	BuiltAt     time.Time
	Nodes       int       // graph node count at build time (for /status without loading)
	Edges       int       // graph edge count at build time
	Summary     string    // synthesized project description (router input)
	SummaryVec  []float32 // embedding of Summary — the always-resident routing vector
	// SummarySource records where Summary came from: "explicit" (an operator-
	// written -summary flag), "readme" (the nearest-to-root README), or "stats"
	// (the symbol-name / doc-title fallback, engine/indexer.go's summaryFor).
	// Additive field like Nodes/Edges — an older snapshot decodes it as "" and
	// is reported as such, never misattributed to one of the three real
	// sources. Exists purely for diagnosis: a routing vector you cannot
	// inspect is a routing vector you cannot debug (docs/todo/corpus-map-router
	// measured this blind before this field existed — three of five served
	// maps were routing on a summary built from zero symbol names).
	SummarySource string
	// Facts is the parser.FactsVersion the graph was extracted under. Additive
	// like SummarySource: an older snapshot decodes it as 0. A lower number
	// than the running binary's means facts are missing, not that the file is
	// unreadable — StaleFacts reports it, and nothing rejects the snapshot.
	Facts int
	// NoEmbed records that this snapshot was built with `snapshot -no-embed`:
	// Body.Cache and Body.HNSW are empty on purpose (the embed pass was
	// skipped), and SummaryVec is empty too. Additive like SummarySource/Facts
	// — an older snapshot decodes it as false, correctly, since none of them
	// were ever built this way. NewIndexerFromSnapshot reads it back to mark
	// the loaded Index no-embed (index.Index.SetNoEmbed) rather than silently
	// re-embedding every node on load.
	NoEmbed bool
}

// StaleFacts reports whether a snapshot was extracted before facts this
// binary's extractors emit.
func (h SnapshotHeader) StaleFacts() bool { return h.Facts < parser.FactsVersion }

// SnapshotBody is the heavy part: the parsed graph and its embedding cache.
type SnapshotBody struct {
	Files map[string]parser.Result // relpath -> parsed Nodes+Edges
	Cache map[string][]float32     // nodeText -> vector (the index cache, verbatim)
	// HNSW is the serialized ANN index over Cache's vectors. Optional and
	// additive (older snapshots leave it nil): when present, load deserializes it
	// and SKIPS the rebuild (the dominant cold-load cost); when absent, load
	// rebuilds the ANN from the vectors. No version bump — gob reads a missing
	// field as nil and the load path falls back to rebuild.
	HNSW map[string][]byte
	// Meta is an opaque, schema-free attribute bag per node: QN → arbitrary
	// string key/values. The snapshot format assigns it NO meaning — recall never
	// interprets it; it only round-trips it. Whoever builds the graph owns the
	// schema: a code graph might write {"visibility":"public"}, a service graph
	// {"tier":"1","domain":"Platform","symptoms":"..."}. Keys live as constants in
	// the writer's/reader's code, so adding an attribute is a change
	// there with no format bump here. Optional and additive like HNSW — old
	// snapshots decode it as nil, old readers ignore it, new readers tolerate it. All
	// values are strings by design (a consumer does its own strconv), matching how
	// mmap-cell data already stores typed values as text.
	Meta map[string]map[string]string
}

// Snapshot is the full on-disk payload: header then body, gob-encoded into one
// gzip stream so the header can be read alone.
type Snapshot struct {
	Header SnapshotHeader
	Body   SnapshotBody
}

// WriteSnapshot gob-encodes header then body into a single gzip stream at path.
// Writing them as two consecutive values lets ReadSnapshotHeader decode just the
// header and stop. Plaintext; WriteSnapshotWith adds encryption.
func WriteSnapshot(path string, s *Snapshot) error {
	return WriteSnapshotWith(path, s, SnapshotKeys{})
}

// WriteSnapshotWith is WriteSnapshot with key material. When keys.Encrypts()
// the gzip+gob stream is wrapped in age to keys' recipient (an explicit
// -snapshot-recipient, else the loaded identity's own public key), so the file
// starts with the age header instead of the gzip magic; readers sniff for
// either. With no keys it writes exactly what WriteSnapshot always wrote.
func WriteSnapshotWith(path string, s *Snapshot, keys SnapshotKeys) error {
	return WriteSnapshotFor(path, s, keys, false)
}

// WriteSnapshotFor is WriteSnapshotWith for a file that may be a rewrite of an
// existing snapshot: loadedEncrypted says whether that file was encrypted when
// it was read, and SnapshotKeys.WriteRecipient turns the two into the actual
// recipient (nil = plaintext). Registry.flush and the live server's flush use
// this so a map that arrived encrypted stays encrypted under a key alone.
func WriteSnapshotFor(path string, s *Snapshot, keys SnapshotKeys, loadedEncrypted bool) error {
	return writeSnapshotTo(path, s, keys.WriteRecipient(loadedEncrypted))
}

// writeSnapshotTo writes the snapshot, wrapped in age when rcpt is non-nil.
func writeSnapshotTo(path string, s *Snapshot, rcpt age.Recipient) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Layering, outermost first: file ← age (optional) ← gzip ← gob. age must
	// be closed AFTER gzip so the final gzip trailer is inside the last
	// authenticated chunk.
	var sink io.WriteCloser = f
	if rcpt != nil {
		aw, err := age.Encrypt(f, rcpt)
		if err != nil {
			return fmt.Errorf("age encrypt: %w", err)
		}
		sink = aw
	}
	gz := gzip.NewWriter(sink)
	enc := gob.NewEncoder(gz)
	if err := enc.Encode(&s.Header); err != nil {
		return fmt.Errorf("encode header: %w", err)
	}
	if err := enc.Encode(&s.Body); err != nil {
		return fmt.Errorf("encode body: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("gzip close: %w", err)
	}
	if sink != f {
		if err := sink.Close(); err != nil {
			return fmt.Errorf("age close: %w", err)
		}
	}
	return f.Close()
}

// ReadSnapshotHeader decodes only the header — cheap enough to run over every
// snapshot at server start (register + routing vector, body deferred to load).
// Plaintext files only; ReadSnapshotHeaderWith also opens encrypted ones.
func ReadSnapshotHeader(path string) (*SnapshotHeader, error) {
	return ReadSnapshotHeaderWith(path, SnapshotKeys{})
}

// ReadSnapshotHeaderWith is ReadSnapshotHeader with key material. An encrypted
// file with no matching identity fails with ErrSnapshotEncrypted (wrapped, with
// the flag to pass), never with a gzip or gob decode error.
func ReadSnapshotHeaderWith(path string, keys SnapshotKeys) (*SnapshotHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readSnapshotHeader(f, keys, path)
}

// ReadSnapshotHeaderFromWith is ReadSnapshotHeaderFrom with key material.
func ReadSnapshotHeaderFromWith(r io.Reader, keys SnapshotKeys) (*SnapshotHeader, error) {
	return readSnapshotHeader(r, keys, "snapshot")
}

func readSnapshotHeader(r io.Reader, keys SnapshotKeys, src string) (*SnapshotHeader, error) {
	stream, err := openSnapshotStream(r, keys, src)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(stream)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	var h SnapshotHeader
	if err := gob.NewDecoder(gz).Decode(&h); err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	if h.Version != snapshotVersion {
		return nil, fmt.Errorf("snapshot version %d != supported %d", h.Version, snapshotVersion)
	}
	return &h, nil
}

// ReadSnapshot decodes the full snapshot (header + body) for loading into the
// graph on first resolve / after eviction. Plaintext files only;
// ReadSnapshotWith also opens encrypted ones.
func ReadSnapshot(path string) (*Snapshot, error) {
	return ReadSnapshotWith(path, SnapshotKeys{})
}

// ReadSnapshotWith is ReadSnapshot with key material (see ReadSnapshotHeaderWith
// for the encrypted-without-key behaviour).
func ReadSnapshotWith(path string, keys SnapshotKeys) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readSnapshot(f, keys, path)
}

// ReadSnapshotFromWith is ReadSnapshotFrom with key material.
func ReadSnapshotFromWith(r io.Reader, keys SnapshotKeys) (*Snapshot, error) {
	return readSnapshot(r, keys, "snapshot")
}

func readSnapshot(r io.Reader, keys SnapshotKeys, src string) (*Snapshot, error) {
	stream, err := openSnapshotStream(r, keys, src)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(stream)
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)
	var s Snapshot
	if err := dec.Decode(&s.Header); err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}
	if s.Header.Version != snapshotVersion {
		return nil, fmt.Errorf("snapshot version %d != supported %d", s.Header.Version, snapshotVersion)
	}
	if err := dec.Decode(&s.Body); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	return &s, nil
}

// summaryFor synthesizes a short description of a project for the semantic
// router, and reports where it came from. Precedence: an explicit,
// human-written summary (the snapshot subcommand's -summary flag) always wins
// — it is the one source that actually says what the map is FOR, not just what
// it contains; then the repo README's prose (Section bodies of a top-level
// README); then a stats-derived fallback (symbol names for a code map, Doc
// titles for a docs-only one — see statsSummary) when neither exists.
//
// The stats fallback existing at all is why routing was broken for three of
// five served maps before this: ccmem/skills/commerce have no README, and the
// old stats line was built entirely from Func/Class/Type symbol names — a
// docs-only map has none, so its whole routing vector embedded roughly
// "<label> — code graph (Doc=N Section=M). Symbols: ", which carries no topical
// signal at all. explicit/readme summaries are unaffected by that fix; only a
// map with neither now gets a materially different (and topical) vector.
func summaryFor(g *graph.Graph, label, explicit string) (summary, source string) {
	if s := strings.TrimSpace(explicit); s != "" {
		return capSummary(label + ": " + s), "explicit"
	}
	if s := readmeSummary(g); s != "" {
		return capSummary(label + ": " + s), "readme"
	}
	return capSummary(statsSummary(g, label)), "stats"
}

// readmeSummary concatenates the Section bodies of the nearest-to-root README,
// in document order. Returns "" if the graph has no README sections.
func readmeSummary(g *graph.Graph) string {
	bestPath := ""
	var sections []*graph.Node
	for _, n := range g.Nodes() {
		if n.Kind != docs.KindSection || !isReadme(n.FilePath) {
			continue
		}
		// Prefer the shallowest README (fewest path separators) so a root
		// README wins over a nested one.
		if bestPath == "" || pathDepth(n.FilePath) < pathDepth(bestPath) {
			bestPath = n.FilePath
			sections = sections[:0]
		}
		if n.FilePath == bestPath {
			sections = append(sections, n)
		}
	}
	if len(sections) == 0 {
		return ""
	}
	sort.Slice(sections, func(i, j int) bool { return sections[i].LineStart < sections[j].LineStart })
	var b strings.Builder
	for _, s := range sections {
		body := s.Body
		if body == "" {
			body = s.Content
		}
		b.WriteString(strings.TrimSpace(body))
		b.WriteString("\n")
		if b.Len() >= maxSummaryChars {
			break
		}
	}
	return strings.TrimSpace(b.String())
}

// statsSummary is the no-README (and no -summary) fallback: kinds present plus
// a sample of top-level symbol names, enough to give the router a coarse
// fingerprint — for a CODE map. A docs-only map (no Func/Class/Type symbols at
// all — e.g. ccmem, a pure Markdown corpus) has nothing to sample there, so the
// old unconditional code-graph line embedded as roughly "<label> — code graph
// (Doc=N Section=M). Symbols: " for every such map: same near-empty text
// regardless of what the map actually held, hence indistinguishable routing.
// Falls back to naming the documents themselves (docTitles) when there are no
// symbols — real topical content instead of none.
func statsSummary(g *graph.Graph, label string) string {
	st := g.Stats()
	var kinds []string
	for k, c := range st.ByKind {
		kinds = append(kinds, fmt.Sprintf("%s=%d", k, c))
	}
	sort.Strings(kinds)

	var names []string
	for _, n := range g.Nodes() {
		if n.Kind == "Func" || n.Kind == "Class" || n.Kind == "Type" {
			names = append(names, n.Name)
			if len(names) >= 30 {
				break
			}
		}
	}
	if len(names) > 0 {
		return fmt.Sprintf("%s — code graph (%s). Symbols: %s",
			label, strings.Join(kinds, " "), strings.Join(names, ", "))
	}

	if titles := docTitles(g, docTitleSampleCap); len(titles) > 0 {
		return fmt.Sprintf("%s — document collection (%s). Documents: %s",
			label, strings.Join(kinds, " "), strings.Join(titles, ", "))
	}

	// Neither symbols nor docs (an empty or otherwise-shaped map): keep the
	// old line rather than emitting nothing — it is still a valid, if
	// uninformative, fingerprint.
	return fmt.Sprintf("%s — code graph (%s). Symbols: %s",
		label, strings.Join(kinds, " "), strings.Join(names, ", "))
}

// docTitles samples up to limit Doc node titles as topical content for a
// docs-only map's stats fallback. Ranked by how many sections a doc contains
// (g's "contains" out-edges from the Doc node) as a proxy for which documents
// carry the most weight in the corpus — "most-connected" in the sense the task
// asked for, since a Doc node's only real edges are to its own Section
// children — tie-broken by nearest-to-root path depth (an index/overview doc
// among equally-sized ones) then title, so the ranking is deterministic across
// rebuilds. Titles are de-duplicated (several files can share a generic title
// like "Overview") so the sample isn't spent on repeats.
func docTitles(g *graph.Graph, limit int) []string {
	type scored struct {
		title string
		conns int
		depth int
	}
	var all []scored
	seen := map[string]bool{}
	for _, n := range g.Nodes() {
		if n.Kind != docs.KindDoc {
			continue
		}
		title := strings.TrimSpace(n.Name)
		if title == "" || seen[title] {
			continue
		}
		seen[title] = true
		conns := len(g.Callees(n.QN, parser.EdgeContains))
		all = append(all, scored{title: title, conns: conns, depth: pathDepth(n.FilePath)})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].conns != all[j].conns {
			return all[i].conns > all[j].conns
		}
		if all[i].depth != all[j].depth {
			return all[i].depth < all[j].depth
		}
		return all[i].title < all[j].title
	})
	if len(all) > limit {
		all = all[:limit]
	}
	out := make([]string, len(all))
	for i, s := range all {
		out[i] = s.title
	}
	return out
}

func isReadme(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	return base == "readme.md" || base == "readme.markdown"
}

func pathDepth(path string) int { return strings.Count(filepath.ToSlash(path), "/") }

func capSummary(s string) string {
	if len(s) > maxSummaryChars {
		return s[:maxSummaryChars]
	}
	return s
}

// runSnapshot is the `xray-memory snapshot` subcommand: parse + embed each repo
// ONCE and write one snapshot file per project under -snapshot-dir. This is the
// only embed pass for Mode B; the read-only server never re-embeds nodes.
