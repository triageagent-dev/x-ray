//go:build cgo

package parser

import (
	"fmt"

	tree_sitter "github.com/tree-sitter/go-tree-sitter"
)

// Parser holds reusable Tree-sitter parsers, one per language (lazily created).
type Parser struct {
	parsers map[string]*tree_sitter.Parser
}

// New returns a ready-to-use Parser.
func New() *Parser {
	return &Parser{parsers: make(map[string]*tree_sitter.Parser)}
}

// Close releases the C memory behind every cached language parser; the
// tree-sitter bindings do not free it when the Go value is collected. The
// Parser is not usable afterwards.
func (p *Parser) Close() {
	for k, ts := range p.parsers {
		ts.Close()
		delete(p.parsers, k)
	}
}

// parserFor returns a cached parser for the detected language key. The key is
// the extension language (e.g. "tsx" vs "typescript") because those are distinct
// grammars even though they share a tags query / spec.name.
func (p *Parser) parserFor(spec *langSpec, key string) (*tree_sitter.Parser, error) {
	if ts, ok := p.parsers[key]; ok {
		return ts, nil
	}
	ts := tree_sitter.NewParser()
	if err := ts.SetLanguage(spec.language()); err != nil {
		return nil, fmt.Errorf("set language %s: %w", key, err)
	}
	p.parsers[key] = ts
	return ts, nil
}

// ParseFile parses source bytes for the given path. Unknown extensions return an
// empty Result with no error so callers can skip-scan a tree cheaply.
func (p *Parser) ParseFile(path string, source []byte) (Result, error) {
	detected := detectLanguage(path)
	if detected == "" {
		return Result{}, nil
	}
	spec := registry[detected]
	ts, err := p.parserFor(spec, detected)
	if err != nil {
		return Result{}, err
	}
	tree := ts.Parse(source, nil)
	if tree == nil {
		return Result{}, fmt.Errorf("parse %s: nil tree", path)
	}
	defer tree.Close()
	root := tree.RootNode()

	// Use the query/spec name as the language label so .tsx and .ts both report
	// "typescript" and share an embedding/search space.
	lang := spec.name

	res := Result{}
	// File node first; everything CONTAINS-links back to it.
	res.Nodes = append(res.Nodes, NodeInfo{
		Kind:      "File",
		Name:      path,
		FilePath:  path,
		Language:  lang,
		LineStart: 1,
		LineEnd:   int(root.EndPosition().Row) + 1,
	})

	if spec.useTags {
		// Tier 1: generic tags-based extraction.
		extractWithTags(root, source, lang, path, spec, &res)
		return res, nil
	}

	// Tier 2: rich hand-written extractor (Go).
	// Pass 1: collect file scope (imports + locally defined names).
	sc := collectScope(root, spec, source)
	// Pass 2: walk the tree emitting nodes/edges.
	w := &walker{spec: spec, source: source, path: path, scope: sc, res: &res}
	w.walk(root, "", "")
	return res, nil
}

// scope holds the file-level facts needed to resolve call targets.
type scope struct {
	importMap    map[string]string // imported name/alias -> module path
	definedNames map[string]bool   // top-level funcs/types defined in this file
}

type walker struct {
	spec   *langSpec
	source []byte
	path   string
	scope  *scope
	res    *Result

	// varTypes maps an in-scope variable name to what the walk knows of its
	// type for the function body currently being walked (receiver, parameters,
	// simple local bindings). It is saved/restored around each function body so
	// scopes don't leak.
	varTypes map[string]binding
}

// binding is what the walk knows about an in-scope variable: its declared or
// inferred type (ref.Name, with ref.Pkg the import path it came from, and
// ref.Coll for a slice/map/chan of it). ref.Pkg alone is set when the value
// was produced by a call into that package (x := pkg.New(...)) — the type
// stays unknown, but its ORIGIN is not, and origin is enough to keep a method
// call on it from being guessed against the caller's own packages.
type binding struct {
	ref TypeRef
	// A value reached by following chain from another binding's type —
	// `ixr, err := r.load(ctx)`, `m := s.memory` — keeps that provenance
	// instead of a type: rootType/rootPkg are the root variable's binding and
	// chain the steps ("load()", "memory"), so a later ixr.Update() can be
	// followed by the linker on declared result and field types. A value
	// returned by a package-local function — `reg := registryWithRepo(t)` —
	// is rooted in that function instead (rootFunc), and typed by its
	// declared result at link time rather than by a naming convention.
	rootType string
	rootPkg  string
	rootFunc string
	chain    string
}

// walk recurses the AST. enclosingClass/enclosingFunc carry context down so
// methods attach to their receiver type and calls attribute to their caller.
func (w *walker) walk(node *tree_sitter.Node, enclosingClass, enclosingFunc string) {
	count := int(node.NamedChildCount())
	for i := 0; i < count; i++ {
		child := node.NamedChild(uint(i))
		nt := child.Kind()

		switch {
		case w.spec.classTypes[nt]:
			if w.extractClass(child, enclosingClass) {
				continue
			}
		case w.spec.funcTypes[nt]:
			if w.extractFunc(child, enclosingClass, enclosingFunc) {
				continue
			}
		case w.spec.importTypes[nt]:
			w.extractImports(child)
			continue
		case w.spec.callTypes[nt]:
			w.extractCall(child, enclosingClass, enclosingFunc)
			// calls can nest (args contain calls); fall through to recurse
		case nt == "func_literal" && w.spec.name == "go":
			w.walkFuncLiteral(child, enclosingClass, enclosingFunc)
			continue
		default:
			// Track local variable→type bindings inline as we pass over them, so
			// x.method() calls resolve without a second full-body traversal.
			if w.varTypes != nil && w.spec.name == "go" {
				w.noteGoVarType(child, nt)
			}
		}
		w.walk(child, enclosingClass, enclosingFunc)
	}
}

func (w *walker) line(n *tree_sitter.Node) int {
	return int(n.StartPosition().Row) + 1
}

func qualify(file, name, enclosing string) string {
	if enclosing != "" {
		return file + "::" + enclosing + "." + name
	}
	return file + "::" + name
}
