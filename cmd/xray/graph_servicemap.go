package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/rs/zerolog"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/engine"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/index"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/parser"
)

// catalogLink is one typed relation to another entry, named by its `name`.
type catalogLink struct {
	Rel string `json:"rel" yaml:"rel"`
	To  string `json:"to" yaml:"to"`
}

// catalogPart is one FIELD of an entry that gets its own node — a form answer,
// an incident timeline entry, a dataset column note.
//
// An entry has ONE vector and ONE preview, and the preview is the head of its
// text. Concatenating fields therefore makes every field past the preview budget
// unreachable for any query, and the single vector is dominated by whichever
// field has the most words. A part becomes a node named <entry>.<part> under the
// entry (ParentName, as docs use for Doc/Section), linked by `contains`, with its
// own text and so its own vector and preview.
type catalogPart struct {
	Name string `json:"name" yaml:"name"`
	Kind string `json:"kind" yaml:"kind"`
	Text string `json:"text" yaml:"text"`
}

// catalogEntry is one record in an entity catalog. Kind, relation names and
// attributes all come from the file, so the same command ingests services,
// people, incidents or datasets without this package knowing which.
//
// The fields under "legacy" are the original service-topology schema. They still
// parse, and normalize folds them onto the generic ones, so catalogs written
// against the old shape keep working unchanged.
type catalogEntry struct {
	Name  string            `json:"name" yaml:"name"`
	Kind  string            `json:"kind" yaml:"kind"`
	Text  string            `json:"text" yaml:"text"`
	Attrs map[string]string `json:"attrs" yaml:"attrs"`
	Links []catalogLink     `json:"links" yaml:"links"`
	Parts []catalogPart     `json:"parts" yaml:"parts"`

	// legacy
	Aliases     []string `json:"aliases" yaml:"aliases"`
	Calls       []string `json:"calls" yaml:"calls"`
	CalledBy    []string `json:"called_by" yaml:"called_by"`
	Domain      string   `json:"domain" yaml:"domain"`
	Team        string   `json:"team" yaml:"team"`
	Tier        int      `json:"tier" yaml:"tier"`
	Description string   `json:"description" yaml:"description"`
	Symptoms    string   `json:"symptoms" yaml:"symptoms"`
}

// legacyKind is the node kind an entry gets when the catalog does not name one.
const legacyKind = "Service"

// normalize resolves an entry to its kind, embedded text and outgoing links,
// preferring the generic fields and falling back to the legacy service ones.
// An explicit Text is used verbatim: a caller that has already written the text
// it wants embedded should not have this package prepend or interleave prose.
func (e catalogEntry) normalize() (kind, text string, links []catalogLink) {
	kind = e.Kind
	if kind == "" {
		kind = legacyKind
	}

	links = e.Links
	if links == nil {
		for _, t := range e.Calls {
			links = append(links, catalogLink{Rel: parser.EdgeCalls, To: t})
		}
	}

	switch {
	case e.Text != "":
		text = e.Text
	case len(e.Attrs) > 0:
		text = e.attrText(kind)
	default:
		text = e.legacyText(kind)
	}
	return kind, text, links
}

// attrText renders arbitrary attributes in sorted key order, so the embedded
// text is deterministic without this package knowing any attribute's meaning.
func (e catalogEntry) attrText(kind string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s.", kind, e.Name)
	keys := make([]string, 0, len(e.Attrs))
	for k := range e.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if v := strings.TrimSpace(e.Attrs[k]); v != "" {
			fmt.Fprintf(&b, " %s: %s.", k, v)
		}
	}
	return b.String()
}

// legacyText is the original service-catalog rendering, kept verbatim so
// existing catalogs embed exactly the text they embedded before.
func (e catalogEntry) legacyText(kind string) string {
	calls := append([]string(nil), e.Calls...)
	calledBy := append([]string(nil), e.CalledBy...)
	sort.Strings(calls)
	sort.Strings(calledBy)

	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", kind, e.Name)
	if e.Domain != "" {
		fmt.Fprintf(&b, " (domain: %s", e.Domain)
		if e.Team != "" {
			fmt.Fprintf(&b, ", team: %s", e.Team)
		}
		b.WriteString(")")
	} else if e.Team != "" {
		fmt.Fprintf(&b, " (team: %s)", e.Team)
	}
	b.WriteString(".")
	if len(e.Aliases) > 0 {
		fmt.Fprintf(&b, " Also known as: %s.", strings.Join(e.Aliases, ", "))
	}
	if e.Description != "" {
		fmt.Fprintf(&b, " %s", strings.TrimSpace(e.Description))
		if !strings.HasSuffix(e.Description, ".") {
			b.WriteString(".")
		}
	}
	if e.Symptoms != "" {
		fmt.Fprintf(&b, " Failure symptoms: %s.", strings.TrimSpace(e.Symptoms))
	}
	if len(calls) > 0 {
		fmt.Fprintf(&b, " Calls: %s.", strings.Join(calls, ", "))
	}
	if len(calledBy) > 0 {
		fmt.Fprintf(&b, " Called by: %s.", strings.Join(calledBy, ", "))
	}
	return b.String()
}

// catalogFile is the top-level YAML/JSON object shape. Both keys are accepted:
// `entries` for the generic shape, `services` for the original one.
type catalogFile struct {
	Entries  []catalogEntry `json:"entries" yaml:"entries"`
	Services []catalogEntry `json:"services" yaml:"services"`
}

// loadCatalog reads an entity catalog from JSON (bare array or {entries|services})
// or YAML, keyed by file extension.
func loadCatalog(path string) ([]catalogEntry, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	pick := func(c catalogFile) []catalogEntry {
		if len(c.Entries) > 0 {
			return c.Entries
		}
		return c.Services
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		var c catalogFile
		if err := yaml.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("parse yaml %q: %w", path, err)
		}
		return pick(c), nil
	default:
		var arr []catalogEntry
		if err := json.Unmarshal(raw, &arr); err == nil && len(arr) > 0 {
			return arr, nil
		}
		var c catalogFile
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("parse json %q: %w", path, err)
		}
		return pick(c), nil
	}
}

// runServiceMap builds a snapshot from a service dependency graph JSON
// (an array of {name, calls[], called_by[]}). Each service becomes a graph node
// whose embedding text describes its call relationships, and each `calls` entry
// becomes a directed "calls" edge — so the resulting <label>.graph.gob.gz loads
// and is served exactly like a code map, but represents a service topology. It
// does no parsing (CGO-free); it only needs an embeddings endpoint. The
// fingerprint (model/dims/max-input-chars) MUST match the serving deployment's
// so the snapshot loads there.
func runServiceMap(argv []string) error {
	fs := flag.NewFlagSet("servicemap", flag.ExitOnError)
	in := fs.String("in", "", "path to the service-graph JSON file (array of {name, calls, called_by}) (required)")
	label := fs.String("label", "", "map label / name (default: input file base name without extension)")
	snapDir := fs.String("snapshot-dir", ".xray-cache", "directory to write <label>.graph.gob.gz into")
	endpoint := fs.String("embedding-endpoint", "http://localhost:8090", "OpenAI-compatible /v1/embeddings endpoint")
	model := fs.String("embedding-model", "nomic", "embedding model name")
	dims := fs.Int("embedding-dims", 256, "embedding output dimensions (Matryoshka truncation)")
	maxInputChars := fs.Int("embedding-max-input-chars", 8192, "hard char cap on a node's text before embedding")
	maxInputTokens := fs.Int("embedding-max-input-tokens", 0, "the model's input window in tokens (0 = unknown); an input over it is cut to the longest prefix that fits, counted by the endpoint's own tokenizer (llama.cpp POST /tokenize). nomic-embed-text-v2-moe reads 512.")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	summary := fs.String("summary", "", `explicit router description for this map — what questions it answers, e.g. "students of the team course: who they are, their experience, tools and expectations". A catalog has no README, so without it the map routes on a stats line ("code graph (Answer=111 Student=37)") that carries no topical signal, and a bare unmapped question never lands here.`)
	keyPath := snapshotKeyFlag(fs)
	recipient := snapshotRecipientFlag(fs)
	_ = fs.Parse(argv)

	if *in == "" {
		return fmt.Errorf("--in is required")
	}
	keys, err := loadSnapshotKeys(*keyPath, *recipient)
	if err != nil {
		return err
	}
	if *label == "" {
		*label = strings.TrimSuffix(filepath.Base(*in), filepath.Ext(*in))
	}

	logger := newLogger(*logLevel)
	ctx := context.Background()

	entries, err := loadCatalog(*in)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return fmt.Errorf("no entries in %q", *in)
	}

	embedder, err := index.NewEmbedder(ctx, *endpoint, *model, *dims, *maxInputChars, *maxInputTokens, logger)
	if err != nil {
		return fmt.Errorf("embedder: %w", err)
	}
	fp := engine.EmbedFingerprint{Model: *model, Dims: *dims, MaxInputChars: *maxInputChars, MaxInputTokens: *maxInputTokens}
	snap, st, err := buildCatalogSnapshot(ctx, entries, *label, *in, *summary, embedder, fp, logger)
	if err != nil {
		return err
	}

	out := filepath.Join(*snapDir, *label+".graph.gob.gz")
	if err := engine.WriteSnapshotWith(out, snap, keys); err != nil {
		return fmt.Errorf("write %q: %w", out, err)
	}
	logger.Info().
		Str("label", *label).Str("path", out).Bool("encrypted", keys.Encrypts()).
		Int("entries", len(entries)).Int("nodes", st.nodes).Int("edges", st.edges).Int("embedded", st.embedded).
		Int("links", st.linked).Int("skippedLinks", st.skippedLinks).
		Str("summarySource", snap.Header.SummarySource).
		Msg("wrote catalog snapshot")
	return nil
}

// catalogBuildStats is what runServiceMap logs about a build.
type catalogBuildStats struct {
	nodes, edges, embedded int
	linked, skippedLinks   int
}

// buildCatalogSnapshot turns catalog entries into an embedded, snapshot-ready
// map. Split from runServiceMap so it can be exercised with a stub embedder:
// the routing summary it stamps is what decides whether an unmapped question
// ever reaches the map, and that must be testable without an embeddings server.
func buildCatalogSnapshot(ctx context.Context, entries []catalogEntry, label, source, summary string, embedder index.Embedder, fp engine.EmbedFingerprint, logger zerolog.Logger) (*engine.Snapshot, catalogBuildStats, error) {
	// Build nodes (one per service) and edges (one per `calls` target). The node
	// Content is the embedding/preview text — describing each service by its
	// dependencies makes the map semantically searchable ("what calls X",
	// "services that depend on mongo", etc.).
	res, linked, skippedLinks := catalogResults(entries, label)

	files := map[string]parser.Result{label + ".catalog.json": res}
	ixr, err := engine.NewIndexerFromResults(ctx, label, source, files, embedder, logger)
	if err != nil {
		return nil, catalogBuildStats{}, fmt.Errorf("index: %w", err)
	}
	// A catalog has no README to fall back on, so with no -summary the header
	// gets the stats line and routing has nothing topical to match. Same idiom
	// as the snapshot subcommand: set it before SnapshotFor derives the header.
	if s := strings.TrimSpace(summary); s != "" {
		ixr.SetExplicitSummary(s)
	}
	snap, err := ixr.SnapshotFor(ctx, label, fp, embedder)
	if err != nil {
		return nil, catalogBuildStats{}, fmt.Errorf("snapshot: %w", err)
	}
	st := ixr.Graph().Stats()
	return snap, catalogBuildStats{nodes: st.Nodes, edges: st.Edges, embedded: st.Embedded, linked: linked, skippedLinks: skippedLinks}, nil
}

// catalogResults converts catalog entries into recall nodes+edges.
//
// Edge endpoints are written as file::Name — the same form NodeInfo.Qualified()
// produces — because graph.Build only repo-prefixes them (tgt = repoQN(repo,
// e.Target)) and does not otherwise qualify catalog relations. Emitting a bare
// name there yields a target no node key matches, so every edge is silently
// dropped by Graph.Dump and by any traversal. Links whose target is not an entry
// in this catalog are skipped and counted rather than written as dangling edges.
func catalogResults(entries []catalogEntry, label string) (parser.Result, int, int) {
	file := label + ".catalog.json"
	known := make(map[string]bool, len(entries))
	for _, e := range entries {
		if e.Name != "" {
			known[e.Name] = true
		}
	}
	qualify := func(name string) string { return file + "::" + name }
	// A part's node name is <entry>.<part>, so a link may target either an entry
	// or one of its parts.
	for _, e := range entries {
		for _, p := range e.Parts {
			if e.Name != "" && p.Name != "" {
				known[e.Name+"."+p.Name] = true
			}
		}
	}

	var res parser.Result
	var linked, skipped int
	for _, e := range entries {
		if e.Name == "" {
			continue
		}
		kind, text, links := e.normalize()
		res.Nodes = append(res.Nodes, parser.NodeInfo{
			Kind:     kind,
			Name:     e.Name,
			FilePath: file,
			Language: strings.ToLower(kind),
			Content:  text,
			Attrs:    e.Attrs,
		})
		// Parts carry no attrs: attrs select ENTITIES, so an attr-filtered search
		// must return the entity, not each of its fields.
		for _, p := range e.Parts {
			if p.Name == "" || strings.TrimSpace(p.Text) == "" {
				continue
			}
			pKind := p.Kind
			if pKind == "" {
				pKind = kind + "Part"
			}
			res.Nodes = append(res.Nodes, parser.NodeInfo{
				Kind:       pKind,
				Name:       p.Name,
				ParentName: e.Name,
				FilePath:   file,
				Language:   strings.ToLower(pKind),
				Content:    p.Text,
			})
			res.Edges = append(res.Edges, parser.EdgeInfo{
				Kind:   parser.EdgeContains,
				Source: qualify(e.Name),
				Target: qualify(e.Name + "." + p.Name),
				File:   file,
			})
			linked++
		}
		for _, l := range links {
			if l.To == "" || !known[l.To] {
				skipped++
				continue
			}
			rel := l.Rel
			if rel == "" {
				rel = parser.EdgeCalls
			}
			res.Edges = append(res.Edges, parser.EdgeInfo{
				Kind:   rel,
				Source: qualify(e.Name),
				Target: qualify(l.To),
				File:   file,
			})
			linked++
		}
	}
	return res, linked, skipped
}
