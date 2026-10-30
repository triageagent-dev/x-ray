//go:build cgo

package engine

// Repo-walking and per-file parse helpers. These invoke the Tree-sitter code
// extractor (parser.ParseFile), so they require CGO and live behind the `cgo`
// build tag together with the live-parse Indexer paths (NewIndexer, Update,
// Reindex). The CGO-free static build never compiles these — it only loads
// pre-built snapshots (see NewIndexerFromSnapshot), which carry already-parsed
// Results and need no parser.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/docs"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/parser"
)

// SplitRepoSpec parses "label=path" or "path" (label defaults to the dir name).
func SplitRepoSpec(spec string) (label, path string) {
	if i := strings.Index(spec, "="); i > 0 {
		return spec[:i], spec[i+1:]
	}
	abs, _ := filepath.Abs(spec)
	return filepath.Base(abs), spec
}

// FilepathWalk walks a repo, skipping ignored dirs and minified bundles, and
// calls fn with each readable file's repo-relative path and bytes. Shared by
// full and incremental indexing so both honour the same skip rules.
//
// followSymlinks controls whether a symlinked directory is descended into.
// filepath.WalkDir never does this on its own — a symlink's DirEntry reports
// IsDir() false even when its target is a directory, so a symlinked dir is
// silently invisible to the walk by default. Default false here too: turning
// it on globally would make every future build's shape depend on wherever a
// repo happens to symlink, and a link into a large tree (or into its own
// parent) could quietly balloon or hang a build that never asked for it. The
// snapshot builder's opt-in -follow-symlinks flag is the only caller that
// sets it true. A symlink to a plain file is read either way — that already
// worked before this parameter existed, since a plain read follows a file
// symlink transparently.
func FilepathWalk(repo string, followSymlinks bool, fn func(rel string, src []byte)) error {
	// The root must exist and be a directory. Unreadable subdirectories are
	// skipped (walkTree), but a missing root is not a repo with no files — it is
	// a wrong path, and a build that "succeeds" with zero nodes on it would
	// write an empty snapshot over a real one.
	info, err := os.Stat(repo)
	if err != nil {
		return fmt.Errorf("repo root: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("repo root %s is not a directory", repo)
	}
	visited := map[string]bool{} // resolved real paths of dirs already descended into via a symlink
	return walkTree(repo, repo, followSymlinks, visited, fn)
}

// walkTree lists dir's entries and recurses, reporting each file's path
// relative to root. dir is the path AS REACHED — it may itself be a symlink,
// or nested under one — and os.ReadDir/os.ReadFile follow that transparently.
// Building each child's path by joining onto dir, rather than a resolved
// target, is what keeps reported paths on the logical (symlink-preserving)
// side rather than the real one.
func walkTree(root, dir string, followSymlinks bool, visited map[string]bool, fn func(rel string, src []byte)) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	for _, d := range entries {
		path := filepath.Join(dir, d.Name())
		if d.Type()&os.ModeSymlink != 0 {
			target, evalErr := filepath.EvalSymlinks(path)
			if evalErr != nil {
				continue // dangling symlink, or a chain EvalSymlinks itself refuses to resolve
			}
			info, statErr := os.Stat(target)
			if statErr == nil && info.IsDir() {
				if !followSymlinks {
					continue // opt-in only, see doc above
				}
				if visited[target] {
					continue // cycle guard: this real directory is already being walked
				}
				visited[target] = true
				if err := walkTree(root, path, followSymlinks, visited, fn); err != nil {
					return err
				}
				continue
			}
			// Symlink to a regular file (or something Stat can't classify as a
			// dir): fall through and read it like any other file below.
		} else if d.IsDir() {
			if skipDir(d.Name()) {
				continue
			}
			if err := walkTree(root, path, followSymlinks, visited, fn); err != nil {
				return err
			}
			continue
		}
		src, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}
		rel, _ := filepath.Rel(root, path)
		if IsMinified(rel, src) {
			continue
		}
		fn(rel, src)
	}
	return nil
}

// ParseRepo walks a repo directory and parses every supported file into a flat
// node/edge list. Shared by the CLI dump path and the live indexer.
func ParseRepo(repo string, followSymlinks bool) (nodes []parser.NodeInfo, edges []parser.EdgeInfo, files int, langs map[string]int, err error) {
	p := parser.New()
	defer p.Close()
	langs = map[string]int{}
	err = FilepathWalk(repo, followSymlinks, func(rel string, src []byte) {
		res := ParseOneFile(p, rel, src)
		if len(res.Nodes) == 0 {
			return
		}
		files++
		for _, n := range res.Nodes {
			if n.Kind != "File" && n.Kind != docs.KindDoc {
				langs[n.Language]++
			}
		}
		nodes = append(nodes, res.Nodes...)
		edges = append(edges, res.Edges...)
	})
	return nodes, edges, files, langs, err
}

// skipDir reports whether a directory should be skipped during a repo walk.
func skipDir(name string) bool {
	switch name {
	case ".git", "node_modules", "vendor", "third_party", "dist", "build", ".next", "out", "bin", ".venv", "__pycache__":
		return true
	}
	if len(name) > 1 && strings.HasPrefix(name, ".") {
		return true
	}
	return false
}

// joinCounts renders a count map as "k=v k=v …" sorted by descending count.
func joinCounts(m map[string]int) string {
	type kv struct {
		k string
		v int
	}
	var pairs []kv
	for k, v := range m {
		pairs = append(pairs, kv{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].v > pairs[j].v })
	var parts []string
	for _, p := range pairs {
		parts = append(parts, fmt.Sprintf("%s=%d", p.k, p.v))
	}
	return strings.Join(parts, " ")
}
