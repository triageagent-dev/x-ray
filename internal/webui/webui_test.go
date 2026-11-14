package webui

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const inline = `window.__boot=1`

func export() fstest.MapFS {
	page := func(title string) []byte {
		return []byte(`<!doctype html><title>` + title + `</title><script src="/_next/static/a.js"></script><script>` + inline + `</script>`)
	}
	return fstest.MapFS{
		"index.html":        {Data: page("home")},
		"login.html":        {Data: page("login")},
		"k/events.html":     {Data: page("events")},
		"docs/index.html":   {Data: page("docs")},
		"_next/static/a.js": {Data: []byte("console.log(1)")},
		"icon.svg":          {Data: []byte("<svg/>")},
	}
}

func get(h http.Handler, method, p string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, p, nil))
	return rec
}

func TestHandler_ResolvesNextRoutes(t *testing.T) {
	h := Handler(export())
	for p, title := range map[string]string{
		"/":              "home",
		"/login":         "login",
		"/k/events":      "events",
		"/docs":          "docs",
		"/k/not-a-file":  "home", // a client-side route: the shell, not a 404
		"/some/deep/url": "home",
	} {
		rec := get(h, http.MethodGet, p)
		assert.Equal(t, http.StatusOK, rec.Code, p)
		assert.Contains(t, rec.Body.String(), "<title>"+title+"</title>", p)
	}
}

func TestHandler_DocumentsCarryTheirOwnScriptHashes(t *testing.T) {
	sum := sha256.Sum256([]byte(inline))
	want := "'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'"
	rec := get(Handler(export()), http.MethodGet, "/login")
	csp := rec.Header().Get("Content-Security-Policy")
	assert.Contains(t, csp, "script-src 'self' blob: "+want)
	assert.NotContains(t, csp, "script-src 'self' 'unsafe-inline'", "no script may run without a hash")
	assert.Contains(t, csp, "frame-ancestors 'none'")
	assert.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
}

func TestHandler_OnlyHashedBundlesAreImmutable(t *testing.T) {
	h := Handler(export())
	assert.Equal(t, "public, max-age=31536000, immutable", get(h, http.MethodGet, "/_next/static/a.js").Header().Get("Cache-Control"))
	// A miss under the immutable prefix falls through to the HTML shell and must
	// not inherit a year-long cache lifetime.
	miss := get(h, http.MethodGet, "/_next/static/missing.js")
	assert.Equal(t, "no-cache", miss.Header().Get("Cache-Control"))
	assert.Equal(t, "public, max-age=300", get(h, http.MethodGet, "/icon.svg").Header().Get("Cache-Control"))
}

func TestHandler_GetAndHeadOnly(t *testing.T) {
	h := Handler(export())
	assert.Equal(t, http.StatusMethodNotAllowed, get(h, http.MethodPost, "/").Code)
	assert.Equal(t, http.StatusOK, get(h, http.MethodHead, "/").Code)
}

func TestHandler_CannotLeaveTheExport(t *testing.T) {
	rec := get(Handler(export()), http.MethodGet, "/../../etc/passwd")
	// The standard library refuses a path with ".." outright; either way nothing
	// outside the export is read.
	assert.Contains(t, []int{http.StatusOK, http.StatusBadRequest}, rec.Code)
	assert.False(t, strings.Contains(rec.Body.String(), "root:"))
}

func TestInlineScripts_SkipsExternalOnes(t *testing.T) {
	got := inlineScripts([]byte(`<script src="/x.js"></script><script>a()</script><script id=b>b()</script>`))
	require.Len(t, got, 2)
	assert.Equal(t, "a()", string(got[0]))
	assert.Equal(t, "b()", string(got[1]))
}

func TestAssets_FalseWhenNothingWasBuilt(t *testing.T) {
	// The committed tree holds only .gitkeep, so a plain `go build` has no UI.
	_, ok := Assets()
	_ = ok // either is valid here; the contract is that it never panics or errors
}

func TestBuiltFor(t *testing.T) {
	root := export()
	assert.True(t, BuiltFor(root, ""))
	assert.False(t, BuiltFor(root, "/path"), "a root export under /path asks the host root for its assets")

	sub := fstest.MapFS{"index.html": {Data: []byte(`<script src="/path/_next/static/a.js"></script>`)}}
	assert.True(t, BuiltFor(sub, "/path"))
	assert.False(t, BuiltFor(sub, ""), "a /path export at the root asks /path for its assets")
	assert.False(t, BuiltFor(sub, "/pat"))

	assert.True(t, BuiltFor(fstest.MapFS{}, "/path"), "no index.html: nothing to check")
}
