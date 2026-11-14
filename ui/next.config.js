/** @type {import('next').NextConfig} */
const nextConfig = {
  images: { unoptimized: true },
}

if (process.env.NEXT_BUILD_EXPORT === 'true') {
  nextConfig.output = 'export'
  nextConfig.trailingSlash = false
  // Mount the app under a sub-path of the API's own origin (e.g. /ui) instead
  // of at its root, so the two can share a hostname without the UI sitting in
  // front of the API. Same origin is what actually matters — the session
  // cookie, the /v1/voice Origin check and the /v1/memory/events SSE stream
  // all key off it — and a path costs nothing there.
  //
  // basePath is BAKED IN: it rewrites every asset and route URL in the emitted
  // HTML, so an export built for one path cannot be served at another. The
  // server's BASE_PATH must match, and it checks that it does at startup.
  //
  // Note what is deliberately NOT prefixed: lib/api.ts builds root-relative
  // /v1 URLs, which is what keeps API calls on the origin ROOT where the API
  // itself answers them — the UI is a peer of the API here, not a proxy in
  // front of it. When the server itself is mounted under the path, the build
  // also sets NEXT_PUBLIC_API_URL to it (make build BASE_PATH=/path).
  if (process.env.NEXT_BASE_PATH) {
    nextConfig.basePath = process.env.NEXT_BASE_PATH
  }
} else {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080'
  nextConfig.rewrites = async () => [
    { source: '/v1/:path*', destination: `${apiUrl}/v1/:path*` },
    { source: '/manifest.json', destination: `${apiUrl}/manifest.json` },
    { source: '/sw.js', destination: `${apiUrl}/sw.js` },
  ]
}

// Hand the baked prefix to the app itself. Next rewrites the URLs it emits
// (its own assets, next/link routes), but not the ones the app writes
// literally — an <img src>, a metadata icon — and under a sub-path those are
// requested from the origin ROOT, which belongs to the API. lib/utils' asset()
// prefixes them with this; reading it back off nextConfig keeps it from
// drifting out of step with basePath.
nextConfig.env = { NEXT_PUBLIC_BASE_PATH: nextConfig.basePath || '' }

module.exports = nextConfig
