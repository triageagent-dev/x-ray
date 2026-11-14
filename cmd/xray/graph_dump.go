// `xray graph dump <target>` — look at a graph without a server.
//
// The target is either a REPOSITORY DIRECTORY, which is parsed on the spot (the
// cheapest way to see what the extractors pull out of a tree — no embedder, no
// index; needs the CGO build because it parses), or a SNAPSHOT FILE
// (<label>.graph.gob.gz), which is read directly (internal/engine only, so it
// works in the static build too). Options pick the view:
//
//	(default)   header line + graph summary: counts by kind, sample nodes/edges
//	-header     the snapshot header alone — fingerprint, counts, routing summary —
//	            without decoding the body, so it stays cheap on a big map
//	-vectors    the embedding cache: one line per vector (-grep/-limit/-n)
//	-routing    the header's routing vector (the embedded summary)
//	-json       the selected view as JSON instead of text
//
// Why the vector view exists: the cache in a snapshot is keyed by the embedded
// TEXT (index.nodeText: signature + path + a bounded body slice), not by node
// identity, because every rebuild constructs fresh nodes. So "what exactly was
// embedded for this node?" and "did the rebuild hit the cache or re-embed?" can
// only be answered by looking at the cache keys themselves — and before this
// that meant a throwaway Go program each time. -header prints the fingerprint,
// which is what to check BEFORE loading a snapshot under a server built at
// different dims: a Dims mismatch is a hard rejection at load, a model-name
// mismatch only warns, and neither is obvious from the file name.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/engine"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/parser"
)

// dumpTextCap bounds the cache-key text on a human-readable line; the key is
// often a 600+ char body slice and one line per vector must stay one line.
const dumpTextCap = 100

// dumpSummaryCap bounds the header summary in -header output.
const dumpSummaryCap = 200

// Mode defaults for -limit when the caller gives none (-1): a summary shows 40
// sample rows, a vector listing 10 vectors. 0 means "all" in either mode.
const (
	dumpSampleLimit  = 40
	dumpVectorsLimit = 10
)

// gzipMagic is what a plaintext snapshot starts with; an encrypted one starts
// with the age header (engine.IsEncryptedSnapshot). Anything else is not a
// snapshot, and saying so beats a codec error from deep inside the decoder.
var gzipMagic = []byte{0x1f, 0x8b}

func runDump(argv []string) error {
	return runDumpTo(os.Stdout, os.Stderr, argv)
}

// dumpOpts are the parsed flags, shared by the repo and snapshot paths.
type dumpOpts struct {
	asJSON  bool
	limit   int // -1 = mode default, 0 = all
	header  bool
	vectors bool
	routing bool
	grep    string
	n       int
}

// runDumpTo is runDump with the streams injected, so tests can read the output
// instead of capturing stdout.
func runDumpTo(stdout, stderr io.Writer, argv []string) error {
	fs := flag.NewFlagSet("dump", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o dumpOpts
	repo := fs.String("repo", "", "repository to parse (same as giving the directory as the target; kept for older scripts)")
	fs.BoolVar(&o.asJSON, "json", false, "emit the selected view as JSON (repo: nodes+edges; snapshot: summary object, header object, or one object per vector)")
	fs.IntVar(&o.limit, "limit", -1, "max rows: sample nodes/edges in a summary (default 40), vectors with -vectors (default 10); 0 = all")
	fs.BoolVar(&o.header, "header", false, "snapshot only: print the header and stop (never decodes the body)")
	fs.BoolVar(&o.vectors, "vectors", false, "snapshot only: list the embedding cache, one vector per line")
	fs.BoolVar(&o.routing, "routing", false, "snapshot only: print the header's routing vector (the embedded summary)")
	fs.StringVar(&o.grep, "grep", "", "with -vectors: case-insensitive substring filter on the embedded text (the cache key)")
	fs.IntVar(&o.n, "n", 8, "with -vectors/-routing/-header: print only the first N components of a vector (0 = whole)")
	keyPath := snapshotKeyFlag(fs)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: xray graph dump <repo-dir | snapshot.graph.gob.gz> [-json] [-limit n] [-header] [-vectors [-grep s] [-n n]] [-routing] [-snapshot-key file]")
		fs.PrintDefaults()
	}
	// The documented form is `dump <target> -flags`, target first, and Go's flag
	// package stops parsing at the first positional argument — so a leading
	// non-flag is lifted out as the target before Parse sees the rest.
	// Flags-first (`dump -header <gob>`) keeps working through fs.Arg(0).
	var target string
	if len(argv) > 0 && !strings.HasPrefix(argv[0], "-") {
		target, argv = argv[0], argv[1:]
	}
	if err := fs.Parse(argv); err != nil {
		return err
	}
	switch {
	case target == "" && fs.NArg() == 1:
		target = fs.Arg(0)
	case fs.NArg() == 0:
	default:
		fs.Usage()
		return fmt.Errorf("expected one target, got %d", fs.NArg()+boolInt(target != ""))
	}
	if target != "" && *repo != "" && target != *repo {
		return fmt.Errorf("both a target (%q) and -repo (%q) were given; use one", target, *repo)
	}
	if target == "" {
		target = *repo
	}
	if target == "" {
		target = "." // bare `dump` keeps meaning "parse the current directory"
	}

	st, err := os.Stat(target)
	if err != nil {
		return err
	}
	if st.IsDir() {
		if o.header || o.vectors || o.routing || o.grep != "" {
			return fmt.Errorf("%q is a repository directory; -header/-vectors/-routing/-grep read a snapshot file", target)
		}
		return dumpRepo(stdout, target, o)
	}
	return dumpSnapshot(stdout, stderr, target, *keyPath, o)
}

// dumpSnapshot is the snapshot-file path: header, vectors, routing or summary.
func dumpSnapshot(stdout, stderr io.Writer, path, keyPath string, o dumpOpts) error {
	if err := sniffSnapshotFile(path); err != nil {
		return err
	}
	snapKeys, err := loadSnapshotKeys(keyPath, "")
	if err != nil {
		return err
	}

	if o.header {
		h, err := engine.ReadSnapshotHeaderWith(path, snapKeys)
		if err != nil {
			return fmt.Errorf("read header %s: %w", path, err)
		}
		// The header alone says nothing about the vector count or the HNSW; a
		// -header run stays header-only on purpose so it is cheap on a big map.
		return printDumpHeader(stdout, h, -1, -1, o.n, o.asJSON)
	}

	snap, err := engine.ReadSnapshotWith(path, snapKeys)
	if err != nil {
		return fmt.Errorf("read snapshot %s: %w", path, err)
	}
	h := &snap.Header
	hnswBytes := 0
	for _, b := range snap.Body.HNSW {
		hnswBytes += len(b)
	}

	switch {
	case o.routing:
		return printRouting(stdout, h.SummaryVec, o.n, o.asJSON)
	case o.vectors:
		return printVectors(stdout, stderr, snap, hnswBytes, o)
	}

	// Default: the same summary the repo path prints, built from the snapshot's
	// parsed results, behind the header line so the two views line up.
	nodes, edges, langs := snapshotGraph(snap)
	if o.asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"header": dumpHeaderJSON(h, len(snap.Body.Cache), hnswBytes, o.n),
			"nodes":  nodes,
			"edges":  edges,
		})
	}
	fmt.Fprintf(stdout, "label=%s model=%s dims=%d nodes=%d vectors=%d hnswBytes=%d\n\n",
		h.Label, h.Fingerprint.Model, h.Fingerprint.Dims, h.Nodes, len(snap.Body.Cache), hnswBytes)
	printSummary(stdout, nodes, edges, len(snap.Body.Files), langs, modeLimit(o.limit, dumpSampleLimit))
	return nil
}

// sniffSnapshotFile rejects a file that is neither gzip nor age up front.
func sniffSnapshotFile(path string) error {
	enc, err := engine.IsEncryptedSnapshot(path)
	if err != nil {
		return err
	}
	if enc {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	head := make([]byte, len(gzipMagic))
	if _, err := io.ReadFull(f, head); err != nil || string(head) != string(gzipMagic) {
		return fmt.Errorf("%s is not a snapshot: neither gzip (a plaintext .graph.gob.gz) nor an age-encrypted one", path)
	}
	return nil
}

// snapshotGraph flattens the snapshot's per-file results into the shape
// printSummary takes. Files are walked in sorted order so the sample rows are
// the same on every run; languages come from the nodes themselves.
func snapshotGraph(snap *engine.Snapshot) (nodes []parser.NodeInfo, edges []parser.EdgeInfo, langs map[string]int) {
	files := make([]string, 0, len(snap.Body.Files))
	for f := range snap.Body.Files {
		files = append(files, f)
	}
	sort.Strings(files)
	langs = map[string]int{}
	for _, f := range files {
		res := snap.Body.Files[f]
		nodes = append(nodes, res.Nodes...)
		edges = append(edges, res.Edges...)
		for _, n := range res.Nodes {
			if n.Language != "" {
				langs[n.Language]++
			}
		}
	}
	return nodes, edges, langs
}

// modeLimit resolves -limit: -1 takes the mode's default, anything else stands.
func modeLimit(limit, def int) int {
	if limit < 0 {
		return def
	}
	return limit
}

// printVectors is the -vectors view: header line, sorted cache keys, trailer.
func printVectors(stdout, stderr io.Writer, snap *engine.Snapshot, hnswBytes int, o dumpOpts) error {
	h := &snap.Header
	cache := snap.Body.Cache
	limit := modeLimit(o.limit, dumpVectorsLimit)

	if !o.asJSON {
		fmt.Fprintf(stdout, "label=%s model=%s dims=%d nodes=%d vectors=%d hnswBytes=%d\n",
			h.Label, h.Fingerprint.Model, h.Fingerprint.Dims, h.Nodes, len(cache), hnswBytes)
	}

	keys := make([]string, 0, len(cache))
	needle := strings.ToLower(o.grep)
	for k := range cache {
		if needle != "" && !strings.Contains(strings.ToLower(k), needle) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic: a map walk would reorder on every run

	enc := json.NewEncoder(stdout)
	shown := 0
	for _, k := range keys {
		if limit > 0 && shown >= limit {
			break
		}
		v := cache[k]
		if h.Fingerprint.Dims > 0 && len(v) != h.Fingerprint.Dims {
			// A vector whose length disagrees with the header is not in the
			// header's space at all; the server would reject the whole file.
			fmt.Fprintf(stderr, "warning: vector has %d dims, header says %d: %.60q\n", len(v), h.Fingerprint.Dims, k)
		}
		if o.asJSON {
			if err := enc.Encode(vectorLine{Text: k, Dims: len(v), Norm: vecNorm(v), Vector: vecHead(v, o.n)}); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(stdout, "--- %d chars: %q\n    norm=%.4f  %v\n", len(k), capText(k, dumpTextCap), vecNorm(v), vecHead(v, o.n))
		}
		shown++
	}
	if !o.asJSON {
		fmt.Fprintf(stdout, "shown=%d of %d (matching %d)\n", shown, len(cache), len(keys))
	}
	return nil
}

// vectorLine is the -vectors -json shape, one per line; -routing -json reuses
// it with an empty text.
type vectorLine struct {
	Text   string    `json:"text,omitempty"`
	Dims   int       `json:"dims"`
	Norm   float64   `json:"norm"`
	Vector []float32 `json:"vector"`
}

func printRouting(w io.Writer, vec []float32, n int, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(w).Encode(vectorLine{Dims: len(vec), Norm: vecNorm(vec), Vector: vecHead(vec, n)})
	}
	fmt.Fprintf(w, "routing vector: dims=%d norm=%.4f\n%v\n", len(vec), vecNorm(vec), vecHead(vec, n))
	return nil
}

// headerJSON is the -header -json / summary "header" shape. Vectors and
// HNSWBytes are -1 when the body was not read (header-only mode).
type headerJSON struct {
	Label          string    `json:"label"`
	Repo           string    `json:"repo"`
	Built          time.Time `json:"built"`
	Model          string    `json:"model"`
	Dims           int       `json:"dims"`
	MaxInputChars  int       `json:"maxInputChars"`
	MaxInputTokens int       `json:"maxInputTokens"`
	Nodes          int       `json:"nodes"`
	Edges          int       `json:"edges"`
	Vectors        int       `json:"vectors"`
	HNSWBytes      int       `json:"hnswBytes"`
	SummarySource  string    `json:"summarySource"`
	Facts          int       `json:"facts"`
	NoEmbed        bool      `json:"noEmbed"`
	Summary        string    `json:"summary"`
	RoutingDims    int       `json:"routingDims"`
	RoutingNorm    float64   `json:"routingNorm"`
	RoutingHead    []float32 `json:"routingHead"`
}

func dumpHeaderJSON(h *engine.SnapshotHeader, vectors, hnswBytes, n int) headerJSON {
	return headerJSON{
		Label: h.Label, Repo: h.RepoPath, Built: h.BuiltAt,
		Model: h.Fingerprint.Model, Dims: h.Fingerprint.Dims,
		MaxInputChars: h.Fingerprint.MaxInputChars, MaxInputTokens: h.Fingerprint.MaxInputTokens,
		Nodes: h.Nodes, Edges: h.Edges, Vectors: vectors, HNSWBytes: hnswBytes,
		SummarySource: h.SummarySource, Facts: h.Facts, NoEmbed: h.NoEmbed, Summary: h.Summary,
		RoutingDims: len(h.SummaryVec), RoutingNorm: vecNorm(h.SummaryVec), RoutingHead: vecHead(h.SummaryVec, n),
	}
}

// printDumpHeader prints the header fields. vectors/hnswBytes < 0 means "not
// read" (header-only mode) and prints as "-".
func printDumpHeader(w io.Writer, h *engine.SnapshotHeader, vectors, hnswBytes, n int, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(dumpHeaderJSON(h, vectors, hnswBytes, n))
	}
	count := func(v int) string {
		if v < 0 {
			return "-"
		}
		return fmt.Sprint(v)
	}
	fmt.Fprintf(w, "label:          %s\n", h.Label)
	fmt.Fprintf(w, "repo:           %s\n", h.RepoPath)
	fmt.Fprintf(w, "built:          %s\n", h.BuiltAt.Format("2006-01-02T15:04:05Z07:00"))
	fmt.Fprintf(w, "model:          %s\n", h.Fingerprint.Model)
	fmt.Fprintf(w, "dims:           %d\n", h.Fingerprint.Dims)
	fmt.Fprintf(w, "maxInputChars:  %d\n", h.Fingerprint.MaxInputChars)
	fmt.Fprintf(w, "maxInputTokens: %d\n", h.Fingerprint.MaxInputTokens)
	fmt.Fprintf(w, "nodes:          %d\n", h.Nodes)
	fmt.Fprintf(w, "edges:          %d\n", h.Edges)
	fmt.Fprintf(w, "vectors:        %s\n", count(vectors))
	fmt.Fprintf(w, "hnswBytes:      %s\n", count(hnswBytes))
	fmt.Fprintf(w, "summarySource:  %s\n", h.SummarySource)
	stale := ""
	if h.StaleFacts() {
		stale = fmt.Sprintf(" (this binary extracts %d: rebuild for the missing facts)", parser.FactsVersion)
	}
	fmt.Fprintf(w, "facts:          %d%s\n", h.Facts, stale)
	noEmbedNote := ""
	if h.NoEmbed {
		noEmbedNote = " (built with `snapshot -no-embed`: search over this map is lexical-only until it is rebuilt without the flag)"
	}
	fmt.Fprintf(w, "noEmbed:        %v%s\n", h.NoEmbed, noEmbedNote)
	fmt.Fprintf(w, "summary:        %q\n", capText(h.Summary, dumpSummaryCap))
	fmt.Fprintf(w, "routingVector:  dims=%d norm=%.4f head=%v\n", len(h.SummaryVec), vecNorm(h.SummaryVec), vecHead(h.SummaryVec, n))
	return nil
}

// printSummary is the text view shared by both targets: counts, then samples.
func printSummary(w io.Writer, nodes []parser.NodeInfo, edges []parser.EdgeInfo, files int, langs map[string]int, limit int) {
	byKind := map[string]int{}
	for _, n := range nodes {
		byKind[n.Kind]++
	}
	byEdge := map[string]int{}
	for _, e := range edges {
		byEdge[e.Kind]++
	}

	fmt.Fprintln(w, "Code Graph Summary")
	fmt.Fprintln(w, "==================")
	fmt.Fprintf(w, "Files parsed: %d\n", files)
	fmt.Fprintf(w, "Nodes:        %d\n", len(nodes))
	fmt.Fprintf(w, "Edges:        %d\n", len(edges))
	fmt.Fprintf(w, "Languages:    %s\n", joinCounts(langs))
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Nodes by kind:", joinCounts(byKind))
	fmt.Fprintln(w, "Edges by kind:", joinCounts(byEdge))

	if limit <= 0 {
		limit = len(nodes) + len(edges) // 0 = all
	}
	fmt.Fprintf(w, "\nSample nodes (first %d):\n", min(limit, len(nodes)))
	for i, n := range nodes {
		if i >= limit {
			break
		}
		fmt.Fprintf(w, "  [%s] %s  (L%d-%d)\n", n.Kind, n.Qualified(), n.LineStart, n.LineEnd)
	}

	fmt.Fprintf(w, "\nSample edges (first %d):\n", min(limit, len(edges)))
	for i, e := range edges {
		if i >= limit {
			break
		}
		fmt.Fprintf(w, "  %s --%s--> %s  (%s:%d)\n", e.Source, e.Kind, e.Target, e.File, e.Line)
	}
}

func joinCounts(m map[string]int) string {
	type kv struct {
		k string
		v int
	}
	var pairs []kv
	for k, v := range m {
		pairs = append(pairs, kv{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].v != pairs[j].v {
			return pairs[i].v > pairs[j].v
		}
		return pairs[i].k < pairs[j].k
	})
	var parts []string
	for _, p := range pairs {
		parts = append(parts, fmt.Sprintf("%s=%d", p.k, p.v))
	}
	return strings.Join(parts, " ")
}

// vecHead returns the first n components (all when n <= 0 or n >= len).
func vecHead(v []float32, n int) []float32 {
	if n <= 0 || n >= len(v) {
		return v
	}
	return v[:n]
}

// vecNorm is the L2 norm; a well-formed embedding is unit length, so anything
// far from 1.0 is worth a second look.
func vecNorm(v []float32) float64 {
	var s float64
	for _, x := range v {
		s += float64(x) * float64(x)
	}
	return math.Sqrt(s)
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// capText truncates s to max runes with an ellipsis marker.
func capText(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}
