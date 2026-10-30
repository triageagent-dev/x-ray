//go:build !cgo

package parser

// CGO-free stub of the Tree-sitter code parser. The real extractor (walk.go and
// friends) compiles only under CGO because the go-tree-sitter bindings link the
// Tree-sitter C runtime + grammars. This stub lets the rest of the code graph —
// the graph, index, snapshot-load, and MCP read tools, all pure Go — compile and
// run without CGO (the static build).
//
// Under this build, ParseFile yields no nodes: source files are not parsed.
// Markdown and Kubernetes YAML are still handled, because the engine's file
// dispatcher routes those to docs.Parse / k8s.Parse (pure Go) before ever
// reaching the Tree-sitter parser. Code graphs are produced offline by the
// CGO build of this binary and shipped as snapshots; this build only loads them.

// Parser is the CGO-free placeholder for the Tree-sitter parser. It carries no
// state and exists so callers compile identically across builds.
type Parser struct{}

// New returns a no-op parser. It never allocates Tree-sitter resources.
func New() *Parser { return &Parser{} }

// Close is a no-op: this build holds no native parsers.
func (p *Parser) Close() {}

// ParseFile returns an empty Result in the CGO-free build: code parsing requires
// the Tree-sitter runtime, which is unavailable here. The error is always nil so
// callers treat the file as "no code nodes" and move on.
func (p *Parser) ParseFile(path string, source []byte) (Result, error) {
	return Result{}, nil
}
