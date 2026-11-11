// Package webui carries the X-Ray web UI inside the binary and serves it.
//
// The UI is a Next.js static export (ui/). "make ui" builds it and stages the
// result in web/, which //go:embed compiles into the binary; web/.gitkeep keeps
// the directive compiling on a checkout where the UI has not been built, and
// Assets reports that case so the server simply serves no UI.
package webui

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"io/fs"
	"net/http"
	"path"
	"sort"
	"strings"
)

//go:embed all:web
var embedded embed.FS

// Assets returns the embedded export, or false when the binary was built without
// one.
func Assets() (fs.FS, bool) {
	root, err := fs.Sub(embedded, "web")
	if err != nil {
		return nil, false
	}
	if fi, err := fs.Stat(root, "index.html"); err != nil || fi.IsDir() {
		return nil, false
	}
	return root, true
}

// cspCommon is everything that does not depend on the document being served.
//
//   - blob: in script-src and worker-src is required: the voice hook compiles
//     its PCM capture AudioWorklet from a Blob URL.
//   - data: in img-src is required: memory items can carry a thumbnail as a
//     data URI.
//   - 'unsafe-inline' survives only in style-src: React inline styles and
//     next/font's injected <style> both need it, and neither is a script.
const cspCommon = "default-src 'self'; " +
	"base-uri 'self'; " +
	"object-src 'none'; " +
	"frame-ancestors 'none'; " +
	"form-action 'self'; " +
	"img-src 'self' data: blob:; " +
	"font-src 'self' data:; " +
	"style-src 'self' 'unsafe-inline'; " +
	"worker-src 'self' blob:; " +
	"connect-src 'self'; " +
	"manifest-src 'self'; " +
	"media-src 'self' blob:"

// Handler serves the export with Next.js resolution order: the exact path, then
// <path>.html (export route files, such as /login and /k/events), then
// <path>/index.html, then index.html for a client-side route that has no file of
// its own. Each HTML document is served with a script-src naming exactly its own
// inline scripts, since a static export cannot carry a per-request nonce.
func Handler(root fs.FS) http.Handler {
	csp := newCSPSet(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Clean collapses any "..", and TrimPrefix makes the path fs-relative. An
		// embedded FS cannot escape to the host filesystem regardless, but keeping
		// the path well-formed avoids relying on that as the only guard.
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		for _, candidate := range []string{name, name + ".html", path.Join(name, "index.html")} {
			if fi, err := fs.Stat(root, candidate); err == nil && !fi.IsDir() {
				serveFile(w, r, root, candidate, csp)
				return
			}
		}
		// A client-side route: serve the shell and let the router resolve it. It
		// is 200, not 404, because the URL is real to the app with no file behind it.
		serveFile(w, r, root, "index.html", csp)
	})
}

// serveFile writes the caching and document headers appropriate to what is
// being served. The immutable header is set here and not on the /_next/static/
// prefix before the lookup: a miss under that prefix falls through to
// index.html, and a year-long immutable header on that response would let a
// caller pin HTML into a shared cache at a key of their choosing.
func serveFile(w http.ResponseWriter, r *http.Request, root fs.FS, name string, csp *cspSet) {
	switch {
	case strings.HasPrefix(name, "_next/static/"):
		// Content-hashed bundles: the name changes whenever the bytes do.
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	case strings.HasSuffix(name, ".html"):
		// Route files keep their names across releases, so they are revalidated or
		// an upgrade is invisible to a returning browser.
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Content-Security-Policy", csp.policy(name))
	default:
		w.Header().Set("Cache-Control", "public, max-age=300")
	}
	http.ServeFileFS(w, r, root, name)
}

// cspSet holds the finished policy for each exported HTML file.
type cspSet struct{ byFile map[string]string }

// newCSPSet hashes the inline scripts of every HTML file in the export.
func newCSPSet(root fs.FS) *cspSet {
	s := &cspSet{byFile: make(map[string]string)}
	_ = fs.WalkDir(root, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".html") {
			return nil //nolint:nilerr // one unreadable file must not stop the walk
		}
		if b, err := fs.ReadFile(root, p); err == nil {
			s.byFile[p] = policyFor(inlineScripts(b))
		}
		return nil
	})
	return s
}

// policy returns the CSP for a named HTML file, or one with no inline hashes for
// anything not scanned.
func (s *cspSet) policy(name string) string {
	if p, ok := s.byFile[name]; ok {
		return p
	}
	return policyFor(nil)
}

func policyFor(scripts [][]byte) string {
	hashes := make([]string, 0, len(scripts))
	seen := make(map[string]bool, len(scripts))
	for _, body := range scripts {
		sum := sha256.Sum256(body)
		h := "'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'"
		if !seen[h] {
			seen[h] = true
			hashes = append(hashes, h)
		}
	}
	sort.Strings(hashes)
	var b strings.Builder
	b.WriteString(cspCommon)
	b.WriteString("; script-src 'self' blob:")
	for _, h := range hashes {
		b.WriteString(" ")
		b.WriteString(h)
	}
	return b.String()
}

// inlineScripts returns the body of every <script> element that has no src
// attribute: the ones a hash-based CSP has to name. Next.js emits several per
// exported page to carry the hydration payload.
func inlineScripts(doc []byte) [][]byte {
	var out [][]byte
	rest := doc
	for {
		i := bytes.Index(rest, []byte("<script"))
		if i < 0 {
			return out
		}
		rest = rest[i:]
		j := bytes.IndexByte(rest, '>')
		if j < 0 {
			return out
		}
		open, body := rest[:j+1], rest[j+1:]
		k := bytes.Index(body, []byte("</script>"))
		if k < 0 {
			return out
		}
		if !bytes.Contains(open, []byte("src=")) {
			out = append(out, body[:k])
		}
		rest = body[k:]
	}
}

// BuiltFor reports whether the export was built for a server mounted at base
// (make build BASE_PATH=…): its pages load their scripts from base + "/_next/".
// An export without index.html has nothing to check.
func BuiltFor(root fs.FS, base string) bool {
	index, err := fs.ReadFile(root, "index.html")
	if err != nil {
		return true
	}
	return bytes.Contains(index, []byte(`"`+base+`/_next/`))
}
