# ADR-067: MCP Member Access

**Status:** Implemented

## Context

MCP is admin-only. Read access is privileged because a private cell (voice session memory and
notes, stored `scope=private`) is gated only by the `subject` argument the caller supplies. A token
holder who passes another person's hash reads that person's notes. So `/authorize` issues tokens
to admins only: the `writeAllowedUsers` list, or a principal in `--access-admins` (ADR-066).

Employees who are not admins cannot use x-ray from Claude.ai, although voice already lets every
signed-in user keep private notes. Only `upsert_memory`, which writes shared cells, needs the
admin list there (ADR-037).

## Decision

`--mcp-member-access` (default off), with `--mcp-oauth`.

- Admin: unchanged. The `writeAllowedUsers` list, an `--access-admins` principal or the static token gets
  `xray:memory.read xray:memory.write`.
- Member: any user who reaches `/authorize` with a valid web session and is not an admin. The
  session resolver has already applied the provider gates and the access lists, so the
  rule has no provider branch. A member gets `xray:memory.read xray:memory.write.own`. The token
  lifetime is unchanged and the user is still asked to approve a pre-registered client.
- `xray:memory.write.own` is advertised in both discovery documents and in the 401 challenge only
  when the flag is on.
- A token without `xray:memory.write` is bound to its owner, the hash of the token's subject. The
  server does not trust a `subject` argument from a bound token:
  - `fetch_memory`, `recent_memory`, `list_memory`, `search_memory`: a non-empty `subject` is
    replaced by the owner hash. An empty one stays empty, which is shared cells only.
  - `get_memory` counts shared cells and the owner's own private cells.
  - `get_memory_context` returns shared cells only. The store already does this; a test pins it.
  - `upsert_memory`: scope defaults to private, `public` is refused, the subject is the owner.
    A member cannot choose an id: an `id` must name the member's own private cell. A missing
    id, a shared cell and another person's cell all answer `not found`, so the call does not
    show which ids exist. A member holds at most 1000 private cells in any status
    (`memberPrivateLimit`); a create or a supersede past it returns `private note limit reached`.
    Expired, refuted and superseded cells still count; only `delete_memory` frees room. Voice is
    unchanged.
  - `delete_memory`, `refute_memory`, `expire_memory`: only the owner's own private cells. A
    shared target is refused with `forbidden`; another person's private cell answers `not found`,
    like a missing id. A member's `refute_memory` retires only the named cell: it does not cascade
    to cells that cite it, since those may belong to someone else (admin refute still cascades).
  - `supersede_memory`: the old cell must be the owner's private cell, and the new cell is private
    and owned by the owner. A `new_id` must name the owner's own private cell, else `not found`.
  - `consolidate_memory` stays admin only, because it writes shared cells.

## Consequences

- Members read shared knowledge and keep their own notes from Claude.ai. A note a member writes
  is the same cell the voice path reads for that user.
- A member cannot read, change or delete anyone else's private cell, and cannot write or retire a
  shared cell.
- With the flag off, `/authorize` and the discovery documents behave as before, and a member
  token that was already issued is refused at `/mcp` (403 `member access is off`) until the flag
  is on again.
- Members can call the code-map read tools (`search_graph`, `search_graph_docs`, `get_graph_node`,
  `get_graph_impact`, `get_graph_risk`, `get_graph_stats`). Voice gives them to every signed-in
  user already, and `update_code_graph` is not registered on MCP.
- A read-only token (no write scope of either kind) is now owner-bound too.
- A member's private space is bounded by the 1000-cell limit and the size limits of every write;
  the per-IP throttle applies as well.

Owner binding relies on a private cell's subject naming a person, the owner's user hash, as in ADR-028 and ADR-037; a deployment that uses subject as a tenant key should leave the flag off. An admin token is not owner-bound and still reads any subject it passes, which this ADR does not change.

## Alternatives

- Keep MCP admin-only. Safe, but employees cannot use x-ray from Claude.ai at all.
- A read-only token for public cells only. It gives no way to keep notes, and it needs a second
  tool surface to hide the `subject` argument.
