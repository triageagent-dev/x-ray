# ADR-066: Access Granted by the Deployment, Then by Identity-Provider Groups

**Status:** Phase 1 implemented (principal lists); phase 2 (groups) not built

## Context

With Google sign-in (ADR-063) anyone in the Workspace domain can sign in, and the admin interface
(MCP tokens, voice writes; ADR-036) is granted by `writeAllowedUsers`, a hand-kept list of user
hashes on the configuration cell. GitHub sign-in can defer the decision to the identity provider
(`--github-org`, `--github-team`, ADR-022), and Entra to its tenant; Google has no equivalent. So
access to an instance is administered by whoever edits the cell, is not tied to the person's
lifecycle, and leaves no trail in the directory.

## Decision

**Phase 1, principal lists (implemented).** `--access-users` and `--access-admins` (or
`ACCESS_USERS`, `ACCESS_ADMINS`) take comma-separated principals from the deployment's
configuration, never from code. They work for every sign-in provider:

- `--access-users`: who may sign in. Empty (and no admins): anyone the provider admits.
- `--access-admins`: who gets the admin interface (an MCP token, voice writes); they may also sign in.
- A principal has one of three forms, each keyed on what the session already carries:
  - `google:<address>`: the address, ASCII only and lower-cased, in `--google-domain` (the lists
    need it: sign-in then admits only that domain's managed accounts, the `hd` claim; without it a
    consumer Google account with a verified look-alike address would match). `--google-domain` takes one
    domain, so an address in a secondary domain cannot be listed. A non-ASCII address never matches a
    list (and signs in as before when no list is set). A Google session's
    name is the full address so the list can match it; reassigning an address inside Workspace
    moves its access with it.
  - `github:<numeric user id>`: digits only. A GitHub login can be renamed and the old one
    re-registered by someone else; the numeric id never is. The session id is `github:<id>`.
  - `azure:<object id>`: the Entra object id, a GUID, lower-cased. A UPN can be changed or reused;
    the object id cannot. The session id is `azure:<oid>`. Object ids are tenant-scoped, so
    an `azure:` principal relies on `--azure-tenant-id` to confine sign-in to that tenant.
- A principal matches only a session of its own provider: an Entra UPN can equal a Google address.
  Anything else (a bare address, an unknown prefix, a malformed id) fails start-up with an error
  naming the entry and the three forms.
- With either list set, only listed principals sign in, for every provider; with neither, nothing
  changes and the provider's own gate decides. The org, team and tenant checks still run first.
- Checked at each provider's sign-in callback (403 `not granted access to this instance`, no
  session), in the session resolver (every request, every carrier: cookie, Bearer, WebSocket
  subprotocol; a stale unlisted cookie does not shadow a valid Bearer, and `/v1/auth/me` no longer
  reports a removed user), at `/authorize` and for voice writes. `writeAllowedUsers` stays and is
  unioned in for the admin interface.
- An issued MCP token is not re-checked and there is no refresh token, so a principal removed from a
  list keeps a working MCP token until it expires (`--mcp-token-ttl`, 15 minutes by default).
- Start-up refuses a principal for a provider that has no client id (`--google-client-id`,
  `--github-client-id`, `--azure-client-id`), and logs only the list sizes, never the principals.
- The principal is the grant key, never what a model is told: a voice prompt gets the local part
  of a Google address, or the login.

**Phase 2, groups (not built).** Two groups per instance would replace the lists, read from the
identity provider and keyed per provider: a GitHub team, the Entra `groups` claim, and Workspace
groups through a narrow Cloud Identity reader (Admin SDK `members.hasMember`) run as the server's
own identity with the Groups Reader role limited to these groups. Group owners would then grant
and revoke access with the provider's audit log, at the cost of one provider call per user per
cache interval. Checks would fail closed, with a short last-good cache; `writeAllowedUsers` and
the lists would stay as break-glass and fallback. It needs its own design before it is built.

## Consequences

- Access changes are a reviewed configuration change instead of an edit to the
  configuration cell, and a GitHub or Entra deployment can now restrict who signs in beyond its
  org or tenant. A Google session from before the change, named by the address's local part,
  matches no list and signs in again. The callbacks of GitHub and Entra call hard-coded provider
  URLs, so the new 403 is covered by the matching unit tests of the grant check, not by a
  callback test.

These lists decide access to one instance: who signs in and who is an admin. They are not the core's tenant visibility and grants, which X-Ray does not take (ADR-053), so an instance remains one access boundary.

## Alternatives

- User hashes in the release values: declarative and reviewed, but still a hand-kept list with no
  link to the directory.
- GitHub logins or Entra UPNs as principals: readable, but renameable and reusable.
