# ADR-035: UI Category Taxonomy — Tag Sub-Views over a Generic Backend

**Status:** Implemented (all 4 items)
**Date:** 2026-05-31
## Context

The memory store is deliberately generic: every cell is a raw `kind`
(`stable_fact`, `recent_change`, `system_identity`, `event`, `note`) plus a flat
`tags` list and a `scope`. The X-Ray UI, however, presents memory by **domain
category** — Projects, Team, Blog, News, Events, Notes. The mapping from the
generic backend to those categories had drifted into **four divergent hardcoded
label/color maps** (`memory-card`, `mobile-item-overlay`, `mobile-swipe-layout`,
`memory-item-panel`) plus an ad-hoc `system_identity→contributor` kind rewrite in
the graph. Categories resolved by `kind` alone, so `system_identity` was one
bucket — there was no Projects-vs-Team split, and the domain labels weren't shown
on desktop.

This supersedes the **reset-out CoALA `class`/`type` approach** (the
`feat/coala-taxonomy` branch name is historical). CoALA proposed first-class
backend taxonomy fields; that was abandoned. The decision here is the opposite:
**domain typing is a UI concern — a mapping from `(kind, tag)` to a display
category — and the backend stays a standard MCP/REST store.**

## Decision

A single source of truth, `ui/lib/category-config.ts`, defines a
**category-keyed** model. A `Category` is
`{ id, label, color, kind, tag?, tier, angle, icon, scope? }`.

- **A category narrows a `kind`, optionally by a `tag`.** Multiple categories may
  share a kind, split by tag. `system_identity` resolves to two distinct
  categories: **Projects** (`+project`) and **Team** (`+contributor`).
- **Resolution** (`categoryOf(item)`): tag-specific categories for the item's
  kind are checked first (first tag match wins); a bare-kind category (no `tag`)
  is the fallback. `agent_note` is aliased to the `note`/Notes category
  (`KIND_ALIASES`) so the synthesized per-user note kind resolves consistently.
  Anything unmatched falls to `UNKNOWN`.
- **Strict matching.** Projects/Team are *both* tagged, so an untagged
  `system_identity` cell has no bare-kind fallback and resolves to "Other". The
  tag is mandatory and gates UI visibility — enforced in the upsert skills and
  CLAUDE.md's memory matrix; malformed cells are fixed at the data source, not
  absorbed by a permissive fallback.
- **Domain labels are active by default.** Sidebar nav, the Voronoi graph
  (regions/colors/watermark labels/dots/synapses), `/k/[id]` listings, the item
  page back-link, and mobile tabs/cards all group, color, and **route by category
  id** (`/k/projects`, `/k/team`, …) — not raw kind — with a per-category
  lucide icon. `generateStaticParams` emits the category ids as the static routes.
- **Graph placement** (`tier`/`angle`) is a property of the category, so
  Projects and Team — which share a kind — occupy distinct radial sectors
  (the six categories are spaced 60° apart; see the Voronoi fix in 1.18.9).

No backend change: the cells already carry the tags (since then the graph preview's
cap and its counts are per category on the server, so a kind split by tag is capped
and counted per category: `/v1/memory/summary` `totalsByCategory`; and a hit event carries the tags of
each shared item, so a recall pulses the tagged region it belongs to). Privacy is unaffected —
Notes are private (`scope`-gated, served via `/v1/user/notes`), orthogonal to the
category mapping (ADR-037).

## Consequences

- **One mapping, not four.** The scattered label/color maps and the graph's
  `system_identity→contributor` rewrite are gone; every surface reads
  `category-config.ts`.
- **Projects/Team split** works everywhere by `(kind, tag)`, including the graph.
- **Strict tagging is a contract.** Untagged `system_identity` cells are
  invisible from Projects/Team and graph-only as "Other" — acceptable because the
  upsert paths enforce the tag; the trade-off is documented, not silently
  permissive.
- **Now per-deployment, not just per-build (item 4).** `MemoryCellSpec.UI.Categories`
  (`api/v1alpha1`) carries the mapping on the `system_configuration` cell; a
  public, pre-auth `GET /v1/categories` endpoint serves it (the graph loads
  before login); `ui/lib/category-config.ts` fetches it once at app load and
  replaces the in-repo default in place, with the default as fallback when
  unset. Same image/binary now serves different taxonomies per deployment —
  e.g. triage-xray's Concerns/Patterns/Facts/Identity/Events/Notes vs.
  xray-xray's own Blog/News/Projects/Team/Events/Notes.

## Status of the todo items

1. **Tag sub-views (Projects/Team)** — Done.
2. **Domain labels active** — Done (collapsed into the category `label`s; no
   separate skin toggle).
3. **Per-category icons** — Done (lucide icon per category in nav and cards).
4. **Runtime config from a `system_configuration` cell** — Done. See
   `internal/xray/configwatcher.go` (parsing), `internal/xray/server.go`
   (`/v1/categories`), `ui/lib/category-config.ts` +
   `ui/hooks/use-categories.ts` (fetch + reactive consumption).

## Addendum (2026-07-04): default is the native agent-memory taxonomy; only real customizations go in config

The original ADR treated Blog/News/Projects/Team as *the* taxonomy and the
in-repo default. That conflated one deployment's customization with the
product baseline. X-Ray is a **generic** memory product; X-Ray's
Blog/News/Projects/Team is a *customization* of the raw ADR-020 kinds. So:

- `DEFAULT_CATEGORIES` (`ui/lib/category-config.ts`) is the **native
  agent-memory taxonomy** — the raw ADR-020 kinds with clean labels:
  Concerns (`active_concern`), Patterns (`operational_pattern`), Facts
  (`stable_fact`), Identity (`system_identity`), Events (`event`), Notes
  (`note`). This is what the UI shows when a deployment sets no
  `ui.categories`.
- **A deployment only sets `ui.categories` to override the default with
  something different — the default is never repeated in config.** A memory-
  for-agents deployment (triage-xray) inherits the default and sets no
  categories of its own (it sets `ui.categories: []` only because the base
  `values.yaml` is xray-xray's own config, whose Blog/News override a
  layered deployment would otherwise inherit — the empty list clears it back
  to the code default). X-Ray's Blog/News/Projects/Team is the one genuine
  override (`charts/xray-xray/values.yaml`).
- The `CategoryConfig` CRD fields (except `kind`) are now optional; the UI
  fills `id`/`angle`/`tier`/`color`/`icon` with sensible defaults
  (`loadRuntimeCategories`), so an override entry can be as terse as
  `{kind: stable_fact, label: Blog, icon: BookOpen}` — no repeating the
  mechanical fields.

## Addendum (2026-10-29): regions for voice

The voice tool `get_memory` also returns the regions, in the configured order and keyed as the
graph keys them, so a voice agent can say how many items a region holds and what is in it. Items
cut from one source document may share a `doc:<id>` tag; per region `get_memory` then counts the
documents and their sections and names up to 50 documents, from section titles written as
`<document>: <heading>`. Without such tags a region reports its item count only. Shared items only,
as for the rest of `get_memory`. MCP `get_memory` is unchanged.

## Tests

- Go: `TestServer_Categories_Empty`, `TestServer_Categories_FromConfig`
  (`internal/xray/server_test.go`).
- UI: verified via `tsc --noEmit`, `vitest run`, and the static-export
  `next build` (prerenders the union of both deployments' category ids —
  `ALL_KNOWN_CATEGORY_IDS` in `category-config.ts`).
