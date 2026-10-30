package engine

// Incremental update, shared by both builds.
//
// Previously this lived behind the `cgo` tag and the CGO-free build refused
// every update outright. That was over-broad: only CODE extraction needs
// Tree-sitter, and Markdown/YAML are handled by pure-Go extractors. A static,
// distroless binary can therefore keep a doc-shaped corpus (Claude Code memory,
// ADRs, manifests) current on its own — no C toolchain, no CGO image.
//
// What the CGO-free build must NOT do is run a code file through ParseOneFile:
// the stub returns zero nodes, and the loop below reads "zero nodes" as "file
// deleted" and drops the file's existing nodes. Skipping is therefore not a
// nicety — it is what prevents a build flag from quietly deleting a graph.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/hub"
	"github.com/triageagent-dev/x-ray/v2/internal/codegraph/parser"
)

// Update re-parses the given paths (absolute or repo-relative), replaces their
// per-file results, rebuilds the graph (which re-resolves dependents), and
// re-embeds only the changed/new nodes.
//
// skipped counts files that are deliberately not extracted: code files in a
// CGO-free binary, and minified bundles in any build. They are left exactly as
// they were in the graph. unowned lists paths
// that resolved to no served repo. Callers should surface both: a caller told
// only "changed: 0" cannot distinguish "your edit produced no nodes" from "this
// build cannot read your file" or "I never found the file you named".
func (ixr *Indexer) Update(ctx context.Context, paths []string) (changed, removed, skipped int, unowned []string, err error) {
	ixr.mu.Lock()
	defer ixr.mu.Unlock()

	p := parser.New()
	defer p.Close()
	touched := false
	var reindexed []string // paths actually changed/removed — see PublishChange below
	for _, raw := range paths {
		rs, rel := ixr.locate(raw)
		if rs == nil {
			unowned = append(unowned, raw)
			ixr.logger.Warn().Str("path", raw).
				Msg("update: path is not inside a served repo; NOT indexed")
			continue
		}
		if !parserAvailable && NeedsCodeParser(rel) {
			skipped++
			ixr.logger.Debug().Str("path", rel).
				Msg("update: code file needs the Tree-sitter parser, absent in this build; left unchanged")
			continue
		}
		abs := filepath.Join(rs.path, rel)
		src, readErr := os.ReadFile(abs)
		if readErr != nil {
			// File deleted/unreadable → drop its nodes.
			if _, had := rs.files[rel]; had {
				delete(rs.files, rel)
				removed++
				touched = true
				reindexed = append(reindexed, repoRelPath(rs.label, rel))
			}
			continue
		}
		if IsMinified(rel, src) {
			skipped++
			ixr.logger.Debug().Str("path", rel).
				Msg("update: minified bundle, not indexed")
			continue
		}
		res := ParseOneFile(p, rel, src)
		if len(res.Nodes) == 0 {
			if _, had := rs.files[rel]; had {
				delete(rs.files, rel)
				removed++
				touched = true
				reindexed = append(reindexed, repoRelPath(rs.label, rel))
			}
			continue
		}
		rs.files[rel] = res
		changed++
		touched = true
		reindexed = append(reindexed, repoRelPath(rs.label, rel))
	}

	if !touched {
		return 0, 0, skipped, unowned, nil
	}

	ixr.buildGraphLocked()
	// Re-embed only nodes missing a vector (new/changed nodes lost theirs on the
	// graph swap); unchanged nodes keep their cached embedding across rebuilds.
	if err := ixr.ix.EmbedMissing(ctx); err != nil {
		return changed, removed, skipped, unowned, fmt.Errorf("re-embed: %w", err)
	}
	ixr.lastBuilt = time.Now()
	// Tell the live "graph in use" stream what actually moved — never the
	// skipped (CGO-free build, minified) or unowned paths above, since those
	// describe no real graph change for a client to react to.
	hub.PublishChange(hub.ChangeEvent{Op: "update", Paths: reindexed})
	return changed, removed, skipped, unowned, nil
}

// repoRelPath namespaces rel by repo label the same way graph node QNs are
// (repo//path), so a change event stays unambiguous when this Indexer serves
// more than one repo and two of them happen to share a relative path.
func repoRelPath(label, rel string) string {
	if label == "" {
		return rel
	}
	return label + "//" + rel
}

// UpdateByPaths routes each changed file to its owning project, re-indexes it in
// that project's worked copy, and marks the project dirty so it is flushed to
// disk before eviction / on shutdown. Paths that resolve to no snapshotted repo
// are returned in unowned rather than dropped: a caller that mistypes a path, or
// names a file this server does not serve, must not be told "changed: 0" and
// left to read that as "already up to date".
func (r *Registry) UpdateByPaths(ctx context.Context, paths []string) (changed, removed, skipped int, unowned []string, err error) {
	if !r.writable {
		return 0, 0, 0, nil, fmt.Errorf("snapshot dir is read-only (served from an immutable OCI image); rebuild snapshots with `xray-memory snapshot` and re-push, or run with -writable on a writable dir")
	}
	byEntry := map[*projectEntry][]string{}
	for _, p := range paths {
		e, abs := r.resolve(p)
		if e == nil {
			unowned = append(unowned, p)
			r.logger.Warn().Str("path", p).
				Msg("update: path resolved to no snapshotted repo; NOT indexed")
			continue
		}
		byEntry[e] = append(byEntry[e], abs)
	}

	for e, ps := range byEntry {
		if _, lerr := r.load(ctx, e); lerr != nil { // ensure resident before updating
			return changed, removed, skipped, unowned, lerr
		}
		e.mu.Lock()
		ixr := e.ixr
		e.mu.Unlock()
		if ixr == nil {
			continue
		}
		c, rm, sk, un, uerr := ixr.Update(ctx, ps)
		changed += c
		removed += rm
		skipped += sk
		unowned = append(unowned, un...)
		if uerr != nil {
			return changed, removed, skipped, unowned, fmt.Errorf("update %s: %w", e.label, uerr)
		}
		// Only a real mutation dirties the entry; a fully-skipped batch must not
		// trigger a flush that would rewrite the snapshot for no change.
		if c > 0 || rm > 0 {
			e.mu.Lock()
			e.dirty = true
			e.lastAccess = time.Now()
			e.mu.Unlock()
		}
	}
	return changed, removed, skipped, unowned, nil
}
