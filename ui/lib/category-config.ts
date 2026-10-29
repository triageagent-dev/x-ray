// Category model for the memory UI.
//
// The BACKEND stays a generic store: every cell is a raw memory `kind`
// (ADR-020: active_concern, operational_pattern, stable_fact, system_identity,
// event, note, plus watch_item/hypothesis/unknown/recent_change) plus `tags`
// and a `scope`. Display typing is a UI concern: a mapping from `kind` to a
// display CATEGORY.
//
// THE X-RAY STANDARD: there are exactly 12 categories, fixed — one per core
// memory kind — defined once in DEFAULT_CATEGORIES below (Concerns, Patterns,
// Precursors, Watch, Facts, Identity, Policy, Maps, Analytics, Events, Claims,
// Notes). A deployment customizes by RELABELING these 12 via its
// system_configuration cell (values.yaml `ui.categories`): each override entry
// gives a `kind` + `label`, and the runtime fetch swaps the label. It does NOT
// add, remove, split, or re-kind categories — the id set is always these 12. A
// deployment that sets no `ui.categories` shows the default labels as-is.
//
// (A category MAY still narrow a kind by a `tag` — `categoryOf` checks
// tag-specific categories before the bare-kind fallback — but the standard
// taxonomy doesn't use tag splits.)
//
// Placement on the graph (tier/angle) and presentation (label/color/icon) are
// properties of the category, not the raw kind.
//
// RUNTIME CONFIG (ADR-035 item 4): labels are per-deployment, not per-build.
// At app load, loadRuntimeCategories() fetches /v1/categories (public,
// pre-auth — the graph loads before login) and, if the deployment defines any,
// REPLACES the in-repo default below in place so every existing import of
// CATEGORIES sees the update. Components that need to re-render on that change
// should read via the useCategories() hook in @/hooks/use-categories (React
// external-store subscription) rather than assuming CATEGORIES is static; the
// exported functions (categoryOf/categoryById/categoryKey) always read the
// live list, so they need no changes at call sites. This module is deliberately
// framework-agnostic (no "use client", no React import) so Server Components
// (e.g. generateStaticParams) can import ALL_KNOWN_CATEGORY_IDS from it.

import type { LucideIcon } from "lucide-react"
import {
  Newspaper, BookOpen, FolderOpen, Users, Calendar, StickyNote, Sparkles,
  AlertTriangle, Repeat, Server, Eye, Waypoints, Scale, Radar, BarChart3, Flag,
} from "lucide-react"
import { apiFetch, api } from "@/lib/api"

export type Category = {
  /** Stable category id — the route segment (e.g. /k/projects) and group key. */
  id: string
  /** Display label. */
  label: string
  /** Node/section color. */
  color: string
  /** The raw backend memory kind this category draws from. */
  kind: string
  /**
   * Optional tag that narrows the kind. When set, an item matches only if it
   * carries this tag. When absent, this is the bare-kind fallback for `kind`.
   */
  tag?: string
  /** Radial tier 0..1 (smaller = closer to centre). */
  tier: number
  /** Fixed angular sector (radians) so graph regions never overlap. */
  angle: number
  /** Nav/card icon (lucide-react). */
  icon: LucideIcon
  /** private = isolated/personal (scope=private); public = shared. */
  scope?: "public" | "private"
}

// ── The 12 categories (the X-Ray standard taxonomy) ─────────────────────────
// One per core ADR-020 memory kind, with clean labels. This is the WHOLE set —
// a deployment relabels these via ui.categories but never adds or removes one.
// A deployment that sets no ui.categories shows these labels as-is.
//
// The twelve are spaced 360/12 = 30° apart around the circle so no two graph
// regions overlap (angle = index * 2π/12).
//
// Colors are all Tailwind *-400 shades so no node reads louder than another,
// and the ORDER below is also the graph's sector order — so each category's
// color is chosen to sit far in hue from its two neighbours:
//   - `watch` (fuchsia) sits next to Concerns/Patterns — a watch item is the
//     ops signal that hasn't become a concern yet.
//   - `policy` (violet) sits just past Identity: a policy is how a system is
//     required to behave, the governance sibling of what the system IS. Tiered
//     innermost with Facts — both durable ground truth rather than live signal.
//   - `precursors` (lime) sits between Patterns and Watch: a precursor pattern
//     is the learned signature that runs AHEAD of an incident, so it belongs in
//     the ops-signal arc. Lime fills the wide amber→green gap in the palette.
//   - `analytics` (cyan) sits between Maps and Events: counters are derived
//     aggregates over the event stream, the other derived view beside Maps.
//   - `claims` (pink) sits between Events and Notes, at the ephemeral end — a
//     claim is a short-lived coordination lock, the most transient cell there
//     is, hence the outermost tier.
const DEFAULT_CATEGORIES: Category[] = [
  { id: "concerns",   label: "Concerns",   color: "#f87171", kind: "active_concern",      tier: 0.60, angle: 0,                   icon: AlertTriangle },
  { id: "patterns",   label: "Patterns",   color: "#fb923c", kind: "operational_pattern", tier: 0.65, angle: 2 * Math.PI / 12,    icon: Repeat },
  { id: "precursors", label: "Precursors", color: "#a3e635", kind: "precursor_pattern",   tier: 0.66, angle: 4 * Math.PI / 12,    icon: Radar },
  { id: "watch",      label: "Watch",      color: "#e879f9", kind: "watch_item",          tier: 0.68, angle: 6 * Math.PI / 12,    icon: Eye },
  { id: "facts",      label: "Facts",      color: "#4ade80", kind: "stable_fact",         tier: 0.55, angle: 8 * Math.PI / 12,    icon: BookOpen },
  { id: "identity",   label: "Identity",   color: "#38bdf8", kind: "system_identity",     tier: 0.60, angle: 10 * Math.PI / 12,   icon: Server },
  { id: "policy",     label: "Policy",     color: "#a78bfa", kind: "policy",              tier: 0.50, angle: 12 * Math.PI / 12,   icon: Scale },
  { id: "maps",       label: "Maps",       color: "#818cf8", kind: "mental_map",          tier: 0.70, angle: 14 * Math.PI / 12,   icon: Waypoints },
  { id: "analytics",  label: "Analytics",  color: "#22d3ee", kind: "analytics_counter",   tier: 0.72, angle: 16 * Math.PI / 12,   icon: BarChart3 },
  { id: "events",     label: "Events",     color: "#fbbf24", kind: "event",               tier: 0.75, angle: 18 * Math.PI / 12,   icon: Calendar },
  { id: "claims",     label: "Claims",     color: "#f472b6", kind: "incident_claim",      tier: 0.78, angle: 20 * Math.PI / 12,   icon: Flag },
  { id: "notes",      label: "Notes",      color: "#2dd4bf", kind: "note",                tier: 0.40, angle: 22 * Math.PI / 12,   icon: StickyNote, scope: "private" },
]

export const UNKNOWN_CATEGORY: Category = {
  id: "unknown", label: "Other", color: "#94a3b8", kind: "unknown", tier: 0.85, angle: 0, icon: Sparkles,
}

// lucide-react icons selectable by name from a system_configuration cell.
// Unrecognized/missing names fall back to Sparkles.
const ICONS_BY_NAME: Record<string, LucideIcon> = {
  Newspaper, BookOpen, FolderOpen, Users, Calendar, StickyNote, Sparkles,
  AlertTriangle, Repeat, Server, Eye, Waypoints, Scale, Radar, BarChart3, Flag,
}

// Default rotating palette + icon set, used to fill color/icon for a runtime
// category that doesn't specify them (see loadRuntimeCategories). Indexed by
// the category's position so adjacent categories get distinct colors/icons.
const DEFAULT_PALETTE = ["#f87171", "#fb923c", "#fbbf24", "#a3e635", "#4ade80", "#2dd4bf", "#22d3ee", "#38bdf8", "#818cf8", "#a78bfa", "#e879f9", "#f472b6"]
const DEFAULT_ICON_ROTATION: LucideIcon[] = [AlertTriangle, Repeat, Radar, BookOpen, Server, Eye, BarChart3, Calendar, StickyNote, Flag, Scale, Sparkles]
const DEFAULT_TIER = 0.6

// ── Graph preview cap ────────────────────────────────────────────────────────
// The memory graph is a bounded PREVIEW: at most this many nodes per category
// are fetched/drawn, so a large store (thousands of cells) stays cheap to
// render while every category is still represented. The true store size is
// shown separately (the summary `total`), and SEARCH always queries the full
// store — this cap only limits the initial graph, never search.
//
// Pure UI constant — a display concern, so it lives in the frontend only (no
// CRD, no server config, no endpoint). Change it by editing here and rebuilding.
export const GRAPH_NODES_PER_KIND = 25
// The mobile graph draws this many nodes in total, not per kind.
export const MOBILE_GRAPH_NODES = 25

// slugify turns a label into a stable, URL-safe category id (the /k/<id>
// route segment). Mirrors the server-side derivation; kept here since the
// frontend fills id when the CR omits it.
function slugify(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "category"
}

// ── Mutable "current" category list ──────────────────────────────────────────
// CATEGORIES keeps the SAME array reference for the lifetime of the app —
// existing `import { CATEGORIES }` bindings observe in-place updates via
// splice, not reassignment. Derived lookup maps and subscribers are rebuilt
// whenever the contents change (setCategories / loadRuntimeCategories).
export const CATEGORIES: Category[] = [...DEFAULT_CATEGORIES]

let BY_ID: Record<string, Category> = {}
let TAGGED_BY_KIND: Record<string, Category[]> = {}
let BARE_BY_KIND: Record<string, Category> = {}

function rebuildIndexes() {
  BY_ID = Object.fromEntries(CATEGORIES.map(c => [c.id, c]))
  TAGGED_BY_KIND = {}
  BARE_BY_KIND = {}
  for (const c of CATEGORIES) {
    if (c.tag) (TAGGED_BY_KIND[c.kind] ??= []).push(c)
    else BARE_BY_KIND[c.kind] ??= c
  }
}
rebuildIndexes()

const subscribers = new Set<() => void>()

/** Replace the current category list in place and notify subscribers. */
function setCategories(next: Category[]) {
  CATEGORIES.splice(0, CATEGORIES.length, ...next)
  rebuildIndexes()
  subscribers.forEach(fn => fn())
}

/**
 * Shape returned by GET /v1/categories — see api/v1alpha1 CategoryConfig.
 * Only `kind` is required; everything else is filled with a sensible default
 * by loadRuntimeCategories, so a deployment's config can be as terse as
 * `{kind: "active_concern", label: "Concerns"}` without repeating the
 * mechanical fields (id, angle, tier) or picking a color/icon.
 */
export type RuntimeCategoryConfig = {
  kind: string
  label?: string
  id?: string
  color?: string
  tag?: string
  tier?: number
  angle?: number
  icon?: string
  scope?: "public" | "private"
}

/**
 * Fetch the per-deployment category taxonomy from /v1/categories (public,
 * pre-auth) and, if the deployment defines any, replace CATEGORIES in place.
 * Fills defaults for any field the deployment left unset: `label` from `kind`,
 * `id` from a slug of the label, `angle` evenly spaced around the circle by
 * position, `tier` a constant, and `color`/`icon` from rotating palettes keyed
 * by position (so adjacent categories look distinct). An explicit value always
 * wins over its default. An empty/missing list or a failed fetch silently
 * keeps the in-repo default — call once at app startup (see
 * CategoryConfigLoader in app/layout).
 */
export async function loadRuntimeCategories(): Promise<void> {
  try {
    const base = process.env.NEXT_PUBLIC_API_URL ?? ""
    const r = await apiFetch(`${api}/categories`)
    if (!r.ok) return
    const data = await r.json() as { categories?: RuntimeCategoryConfig[] }
    if (!data.categories || data.categories.length === 0) return
    const n = data.categories.length
    setCategories(data.categories.map((c, i) => {
      const label = c.label || c.kind.replace(/_/g, " ").replace(/\b\w/g, m => m.toUpperCase())
      return {
        id: c.id || slugify(label),
        label,
        color: c.color || DEFAULT_PALETTE[i % DEFAULT_PALETTE.length],
        kind: c.kind,
        tag: c.tag || undefined,
        tier: c.tier ?? DEFAULT_TIER,
        // Evenly space categories around the full circle by position.
        angle: c.angle ?? (i * 2 * Math.PI) / n,
        icon: c.icon ? (ICONS_BY_NAME[c.icon] ?? Sparkles) : DEFAULT_ICON_ROTATION[i % DEFAULT_ICON_ROTATION.length],
        scope: c.scope,
      }
    }))
  } catch {
    // Network error / no backend (e.g. static preview) — keep defaults.
  }
}

/**
 * Registers a listener called whenever CATEGORIES is replaced (e.g. by
 * loadRuntimeCategories()). Returns an unsubscribe function. Framework-
 * agnostic — kept here (not "use client") so this module stays importable
 * from Server Components; see hooks/use-categories.ts for the React hook
 * built on top of this.
 */
export function subscribeCategories(onChange: () => void): () => void {
  subscribers.add(onChange)
  return () => subscribers.delete(onChange)
}

/** The item shape needed to resolve a category: a kind plus optional tags. */
export type Categorizable = { kind: string; tags?: string[] }

/** Resolve an item to its display category by (kind, tag). */
export function categoryOf(item: Categorizable): Category {
  const tagged = TAGGED_BY_KIND[item.kind]
  if (tagged) {
    const tags = item.tags ?? []
    const hit = tagged.find(c => tags.includes(c.tag!))
    if (hit) return hit
  }
  const bare = BARE_BY_KIND[item.kind]
  if (bare) return bare
  return { ...UNKNOWN_CATEGORY, id: item.kind, label: item.kind.replace(/_/g, " ") }
}

/** Look up a category by its id (route segment). */
export function categoryById(id: string): Category | undefined {
  return BY_ID[id]
}

/** Stable grouping key for an item — its resolved category id. */
export function categoryKey(item: Categorizable): string {
  return categoryOf(item).id
}

/**
 * The category ids — exactly the 12 defaults, fixed. There are only ever these
 * 12 categories; a deployment customizes by RELABELING them (Facts → "Blog"),
 * not by adding/splitting/re-kinding — so the id set never changes and this is
 * just the default ids. Used by generateStaticParams (build-time static
 * export) to pre-render the /k/<id> routes.
 */
export const ALL_KNOWN_CATEGORY_IDS: string[] = DEFAULT_CATEGORIES.map(c => c.id)
