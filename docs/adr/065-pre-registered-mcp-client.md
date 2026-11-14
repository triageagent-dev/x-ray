# ADR-065: Pre-registered Public MCP Client

**Status:** Implemented

## Context

Open dynamic client registration (RFC 7591) accepts only loopback and custom-scheme
redirect URIs, so a registration from anywhere cannot point authorization codes at a
public host. That fits Claude Code, which listens on localhost. A Claude.ai custom
connector does not: it posts the code to its own https callback, and its form takes a
client ID for a server that does not want open registration from the internet.

## Decision

`--mcp-static-client-id` and `--mcp-static-client-redirect-uris` pre-register one public
client (PKCE, no secret) at startup.

- Its redirect URIs match exactly, must be https (http only on loopback), and carry no
  fragment. A bad value stops the server at startup.
- It lives apart from the registration store: open registration cannot overwrite it, it is
  never evicted, and it is not persisted, because the flags restore it on every start.
- Open registration and its rules are unchanged; the static client does not widen them.
- The user approves first. The client's code goes to an https callback shared by every user of
  that service, so a link that walked a signed-in user through `/authorize` could hand their code
  to someone else's flow there. For a pre-registered client `/authorize` shows a page naming the
  user, the client and the callback host; its Allow link carries an HMAC over the browser session
  and the exact request (client, redirect URI, PKCE challenge, state, resource, scope), valid five
  minutes. Loopback clients are unchanged: their code never leaves the user's machine.
- `/token` refuses a code presented with another `client_id` or `redirect_uri` than it was issued
  for (RFC 6749 §4.1.3).

## Consequences

- A Claude.ai connector works with the client ID entered and the secret left empty, and the
  registration endpoint need not be reachable from Claude's cloud.
- A user still needs a session and a place on `writeAllowedUsers` to be issued a token.

## Residual risk

The consent page cannot tell a user's own connector flow from one an attacker started and
sent as a link: someone starts a flow at the shared callback service, and a user who opens
its `/authorize` link and clicks Allow gives that flow their token. What limits it: the page
names the user, the client and the callback host; the redirect URIs are fixed and matched
exactly, so a code goes only to the registered callback; the Allow link is bound to the browser
session and the exact request and expires in five minutes; and a token is issued only to a user
on `writeAllowedUsers`. What it does not stop is a user approving a request they did not start,
so the page's line "Approve only if you started this connection yourself just now" is the
remaining control. Each approval is logged with the user and client (`mcp authorize: code issued`).
