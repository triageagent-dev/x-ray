# ADR-064: Serving Under a Path on a Shared Host

**Status:** Implemented

## Context

X-Ray assumed it owned its host. Redirects went to `/` and `/login`, the session
cookie was scoped to `/`, the OAuth state cookie to `/v1/auth/`, and the UI called
the API at `/v1`. Behind a proxy that routes one path of a shared hostname to X-Ray
(`https://gw.example.com/path/*`), every one of those lands on the neighbouring
services, and the session cookie is sent to them. The UI could already be exported
under a sub-path (`NEXT_BASE_PATH`), but only with the API left at the origin root.

## Decision

The path of `--public-url` is the base path (`https://gw.example.com/path` → `/path`).
There is no second setting that could disagree with it.

- The server strips the base path from requests under it and serves requests
  outside it as before, so the kubelet probes and in-cluster clients keep working.
  The bare base path redirects to its slash form. The proxy forwards the path
  unchanged; no rewrite rule is needed.
- Login, logout and MCP `/authorize` redirects, the session cookie and the state
  cookie are all under the base path, so the cookies stay with X-Ray on a shared host.
- The PWA manifest and service worker are scoped to the base path.
- Under a base path the server also serves the OAuth metadata at the host root
  with the path inserted (`/.well-known/oauth-authorization-server/path`,
  `/.well-known/oauth-protected-resource/path/mcp`), where MCP clients look first
  (RFC 8414 §3.1, RFC 9728 §3.1). The proxy routes those two paths to X-Ray.
- The UI is built for one base path: `make build BASE_PATH=/path` sets
  `NEXT_BASE_PATH` and `NEXT_PUBLIC_API_URL` to the same value. A path-valued
  `NEXT_PUBLIC_API_URL` means "same origin, under this path": the API, the voice
  WebSocket, the manifest and the service worker are addressed through it, and no
  cross-origin login hand-off is requested.
- CORS and the voice Origin check take the scheme and host of `--public-url`, which
  is all a browser sends as `Origin`.

## Consequences

- One hostname can carry X-Ray next to other services. It is still one origin:
  the cookie path keeps the session cookie from being sent to the neighbours, but
  a script on a neighbouring page can call X-Ray with the user's cookie. Sharing a
  host means trusting the services on it; a dedicated host is the separation.
- An image is bound to the base path its UI was built for; a server with another
  `--public-url` path serves a UI whose assets are not found, and warns at startup.
- With no path in `--public-url` the behaviour is unchanged, and an existing UI
  build (no `NEXT_PUBLIC_API_URL`, or an origin) emits the same URLs as before.
