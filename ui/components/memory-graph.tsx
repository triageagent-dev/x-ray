"use client"

import { useEffect, useRef, useState, useCallback, useMemo } from "react"
import { useRouter } from "next/navigation"
import * as d3 from "d3"
import { Delaunay } from "d3-delaunay"
import { BrainCog, RefreshCw } from "lucide-react"
import { fetchMemorySummary, fetchMemorySearch, subscribeMemoryEvents, type MemoryItemSummary } from "@/lib/api"
import { cn } from "@/lib/utils"
import { fireOptogenetic, GRAPH_LAYER_CLASS } from "@/lib/opto"
import { categoryOf as catOf, categoryKey as catKey, GRAPH_NODES_PER_KIND, type Category } from "@/lib/category-config"

// Graph placement/color/label come from the UI category, resolved by (kind,
// tag). Items group and lay out by category id, so Projects and Team (which
// share the kind system_identity) occupy distinct graph regions.

const HIT_FADE_MS = 20000      // how long a hit highlight lingers before fully fading out
// Region-flash intensity — the PRIMARY region treatment: the whole hit
// category's Voronoi cells jump to this and fade back over the full
// HIT_FADE_MS, mirroring the direct-cell fade (0.85 → base over 20s). Kept
// below the direct-hit 0.85 so a rendered direct hit still reads
// distinguishably stronger than its surrounding region.
const REGION_FLASH_OPACITY = 0.7
// The graph is a bounded PREVIEW: it fetches at most GRAPH_NODES_PER_KIND
// items PER category so a large store stays cheap. The true store size comes
// from the summary `total`, and search always queries the full store
// (server-first) regardless of this preview.
//
// Hard cap on D3 SVG nodes, to keep a large store from freezing the browser.
// Sized to what the fetch above can actually return, so no category is
// starved of nodes by the budget: the server caps ListSummaryPerKind at
// GRAPH_NODES_PER_KIND items PER KIND, and kind is a free string on the
// backend — beyond the 12 categories in category-config.ts, a handful of
// legacy/fallback kinds (recent_change, hypothesis, unknown) still surface as
// their own round-robin bucket via UNKNOWN_CATEGORY. ~15 known kinds total.
// NOTES_HEADROOM leaves room for extraItems (personal notes/episodic
// memories), which merge on top of the fetch outside the per-kind cap.
const KNOWN_KIND_COUNT = 15
const NOTES_HEADROOM = 50
const MAX_RENDER_NODES = GRAPH_NODES_PER_KIND * KNOWN_KIND_COUNT + NOTES_HEADROOM

function hashStr(s: string): number {
  let h = 0
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) >>> 0
  return h
}
function lcg(seed: number): number {
  return ((seed * 1664525 + 1013904223) >>> 0) / 0x100000000
}

function itemSeeds(items: MemoryItemSummary[], width: number, height: number): [number, number][] {
  const cx = width / 2, cy = height / 2
  const maxR = Math.min(width, height) * 0.46

  const byCat: Record<string, MemoryItemSummary[]> = {}
  for (const item of items) { const k = catKey(item); byCat[k] ??= []; byCat[k].push(item) }

  const arcSlice = Math.PI / 3  // 60° sector per category (matches the 60°-spaced angles)

  return items.map((item) => {
    const cat = catOf(item)
    const tier = cat.tier
    const h = hashStr(item.id)
    const r = maxR * (tier + (lcg(h) - 0.5) * 0.18)
    const catItems = byCat[catKey(item)] ?? [item]
    const n = catItems.length
    const itemIdx = catItems.indexOf(item)
    const baseAngle = cat.angle
    const spread = n === 1 ? 0 : (itemIdx / (n - 1) - 0.5) * arcSlice * 0.80
    const angle = baseAngle + spread + (lcg(h + 7) - 0.5) * 0.08
    return [
      Math.max(8, Math.min(width - 8, cx + Math.cos(angle) * r)),
      Math.max(8, Math.min(height - 8, cy + Math.sin(angle) * r)),
    ] as [number, number]
  })
}

interface Tooltip { x: number; y: number; item: MemoryItemSummary }

interface Props {
  className?: string
  query?: string
  highlightIds?: Set<string>
  // Per-hit-id kind, parallel to highlightIds' contents (lib/api.ts
  // hitKindMap builds this from a MemoryHitEvent's kinds array). Absent when
  // the server hasn't sent kinds (older server) or the current highlightIds
  // aren't from a live hit (e.g. a "latest news" fallback) — the region
  // flash below simply doesn't fire in that case, degrading to today's
  // cell-only behaviour.
  hitKinds?: Map<string, string>
  hitKey?: number
  extraItems?: MemoryItemSummary[]
  onSelect?: (id: string, screenX: number, screenY: number, kind?: string) => void
  // Mobile passes false: the watermark labels would be redundant noise on a
  // small screen.
  showKindLabels?: boolean
  // Mobile passes true: a node tap shows the tooltip instead of opening a
  // detail panel/route (touch devices have no hover to preview with).
  tapShowsTooltip?: boolean
  // Upper bound on drawn nodes across ALL categories, replacing the default
  // per-category budget. Mobile passes MOBILE_GRAPH_NODES: every node is an SVG
  // element with its own animation, which a phone cannot keep up with in the
  // hundreds. Category labels still carry the true totals.
  maxNodes?: number
}

export function MemoryGraph({ className, query, highlightIds, hitKinds, hitKey, extraItems, onSelect, showKindLabels = true, tapShowsTooltip = false, maxNodes }: Props) {
  const nodeBudget = maxNodes ?? MAX_RENDER_NODES
  const router = useRouter()
  const svgRef = useRef<SVGSVGElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [items, setItems] = useState<MemoryItemSummary[]>([])
  const [totalItems, setTotalItems] = useState(0)
  // True per-kind counts over the full store (the graph draws only a capped
  // preview per category, so category labels use these real totals, not the
  // number of drawn nodes).
  const [totalsByKind, setTotalsByKind] = useState<Record<string, number>>({})
  const [serverResults, setServerResults] = useState<MemoryItemSummary[]>([])
  const [searching, setSearching] = useState(false)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [tooltip, setTooltip] = useState<Tooltip | null>(null)
  const hitTimestamps = useRef<Map<string, number>>(new Map())
  const prevHighlightIds = useRef<Set<string>>(new Set())
  const [drawVersion, setDrawVersion] = useState(0)
  const searchDebounce = useRef<ReturnType<typeof setTimeout> | null>(null)

  const load = useCallback(async () => {
    setLoading(true); setError(null)
    try {
      // Bounded preview: up to N active cells per category. total is the true
      // full store size (not the preview count).
      const { items: data, total, totalsByKind: byKind } = await fetchMemorySummary({ perKind: GRAPH_NODES_PER_KIND })
      setTotalItems(total)
      setTotalsByKind(byKind)
      setItems(data.filter(it => it.status === "active" || it.status === "indexed"))
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load")
    } finally { setLoading(false) }
  }, [])

  useEffect(() => { load() }, [load])

  // Server-first search: every query hits the backend, which searches the FULL
  // store — not just the bounded graph preview (GRAPH_NODES_PER_KIND/category)
  // held in `items`. A client-side filter over the preview would miss anything
  // outside it, so search must always go server-side.
  useEffect(() => {
    if (searchDebounce.current) clearTimeout(searchDebounce.current)
    if (!query?.trim()) { setServerResults([]); return }
    searchDebounce.current = setTimeout(async () => {
      setSearching(true)
      try {
        // Search the full store with per-kind fairness so a dominant kind
        // (e.g. 'event') can't crowd out matches in smaller categories. Limit is
        // generous (categories × per-kind) so every category with hits appears.
        const results = await fetchMemorySearch(query.trim(), 300, GRAPH_NODES_PER_KIND)
        setServerResults(results.map(it => ({
          id: it.id, kind: it.kind, scope: it.scope, subject: it.subject,
          title: it.title, tags: it.tags, status: it.status,
          updatedAt: it.updatedAt, hitCount: 0,
        })))
      } catch { setServerResults([]) }
      finally { setSearching(false) }
    }, 300)
  }, [query])

  // Coalesce SSE change events instead of applying each one straight to state:
  // every setItems() triggers the full voronoi rebuild effect below (teardown +
  // rebuild of every cell/label/synapse), which is expensive. A burst of saves
  // (e.g. an agent writing several facts back-to-back) previously meant one
  // full rebuild per event; batching on a trailing timer turns a burst into a
  // single rebuild instead of blocking the main thread N times in a row.
  useEffect(() => {
    const pending = new Map<string, { op: "upsert"; item: MemoryItemSummary } | { op: "delete" }>()
    let timer: ReturnType<typeof setTimeout> | null = null
    const FLUSH_MS = 400

    const flush = () => {
      timer = null
      if (pending.size === 0) return
      const ops = Array.from(pending.entries())
      pending.clear()
      setItems(prev => {
        const byId = new Map(prev.map(i => [i.id, i] as const))
        let delta = 0
        for (const [id, op] of ops) {
          if (op.op === "upsert") {
            if (!byId.has(id)) delta++
            byId.set(id, op.item)
          } else if (byId.delete(id)) {
            delta--
          }
        }
        if (delta !== 0) setTotalItems(t => Math.max(0, t + delta))
        return Array.from(byId.values())
      })
    }

    const unsubscribe = subscribeMemoryEvents(
      () => {},
      (evt) => {
        // The shared graph shows public cells only — private items (notes,
        // episodic) are personal and come in via extraItems, not here. Without
        // this guard a live save_memory(kind=episodic) upsert would add a private
        // cell to the graph (a grey "Other" region) that vanishes on refresh,
        // since the initial /v1/memory/summary fetch already excludes private
        // items server-side. Mirror that exclusion for the live path.
        if (evt.op === "upsert" && evt.item && evt.item.scope !== "private") {
          const item: MemoryItemSummary = {
            id: evt.item.id, kind: evt.item.kind, scope: evt.item.scope,
            subject: evt.item.subject, title: evt.item.title, tags: evt.item.tags,
            status: evt.item.status, updatedAt: evt.item.updatedAt, hitCount: 0,
          }
          pending.set(item.id, { op: "upsert", item })
        } else if (evt.op === "delete" && evt.id) {
          pending.set(evt.id, { op: "delete" })
        } else {
          return
        }
        if (!timer) timer = setTimeout(flush, FLUSH_MS)
      }
    )
    return () => {
      unsubscribe()
      if (timer) clearTimeout(timer)
    }
  }, [])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setTooltip(null) }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [])

  // Last hitKey the flash effect below acted on. Held in a ref, and consumed
  // INSIDE that effect rather than by a separate reset effect, because the
  // two ran in the wrong order for a repeated hit:
  //
  //   render N   a hit arrives → highlightIds gets a new Set identity, hitKey
  //              is still the old value. The flash effect runs with prev
  //              still holding the previous ids, so an id hit AGAIN produces
  //              newHits = [] and nothing flashes.
  //   render N+1 the [hits] effect's setHitKey lands, the old reset effect
  //              cleared prev — but the flash effect never re-ran, because
  //              its deps (highlightIds identity) had not changed again.
  //
  // So a repeat hit on the same id was silently swallowed. Invisible for
  // search, whose result ids vary every query, and total for any surface
  // whose id set is effectively constant (the same cell hit repeatedly) —
  // it flashed once and never again.
  const lastHitKey = useRef<number | undefined>(undefined)

  // When highlightIds changes, flash newly-hit cells then fade them out via D3 transitions
  useEffect(() => {
    if (!highlightIds || !svgRef.current) return
    const svg = d3.select(svgRef.current)
    const now = Date.now()

    // A fresh hitKey means a real hit frame arrived, so every id in it is
    // "new" for flashing purposes even if the same id flashed a moment ago —
    // that is what makes a repeated hit visible. prevHighlightIds still
    // suppresses the other trigger for this effect: a re-render that changed
    // highlightIds for some reason other than an incoming hit, where
    // re-flashing would be noise.
    const freshHit = hitKey !== undefined && hitKey !== lastHitKey.current
    lastHitKey.current = hitKey

    const newHits: string[] = []
    highlightIds.forEach(id => {
      if (freshHit || !prevHighlightIds.current.has(id)) newHits.push(id)
    })
    prevHighlightIds.current = new Set(highlightIds)

    // Region flash: resolve each new hit's category — even when its own cell
    // isn't rendered on the bounded per-category preview — and pulse the
    // whole region once per DISTINCT category, so N hits landing in the same
    // region (e.g. several `event` hits in one batch) read as one pulse
    // instead of N stacked transitions saturating to a solid block. Kind is
    // resolved from hitKinds (server-provided, kind-only — no per-item tags
    // available here, see lib/api.ts hitKindMap's doc comment) rather than
    // from any locally-known item, so this fires identically whether or not
    // the hit is actually drawn.
    const hitCategoryIds = new Set<string>()
    newHits.forEach(id => {
      const kind = hitKinds?.get(id)
      if (kind) hitCategoryIds.add(catKey({ kind }))
    })

    // Opt-in tracing for "the hit arrived but nothing lit up". Off unless the
    // viewer sets localStorage.debugHits — a highlight that silently does
    // nothing is otherwise indistinguishable from a hit that never arrived,
    // and every layer below this one (publish, SSE frame) can already be
    // verified from outside the browser. This is the one link that cannot.
    // Logs what arrived, what it resolved to, and crucially how many DOM
    // nodes each region selector actually matched — zero matches means the
    // flash had nothing to target.
    if (typeof window !== "undefined" && window.localStorage?.getItem("debugHits")) {
      const counts: Record<string, { cells: number; dots: number; label: number }> = {}
      hitCategoryIds.forEach(catId => {
        counts[catId] = {
          cells: svg.selectAll(`[data-category-id="${catId}"]`).size(),
          dots: svg.selectAll(`[data-dot-category-id="${catId}"]`).size(),
          label: svg.selectAll(`[data-label-category-id="${catId}"]`).size(),
        }
      })
      console.log("[memory-hit]", {
        hitKey,
        freshHit,
        ids: Array.from(highlightIds),
        newHits,
        kinds: hitKinds ? Array.from(hitKinds.entries()) : undefined,
        categories: Array.from(hitCategoryIds),
        domMatches: counts,
        directCellsFound: newHits.filter(id => !svg.select(`[data-id="${id}"]`).empty()).length,
      })
    }

    newHits.forEach(id => {
      hitTimestamps.current.set(id, now)

      const cell = svg.select<SVGPathElement>(`[data-id="${id}"]`)
      if (!cell.empty()) {
        const color = cell.attr("data-color")
        const base = parseFloat(cell.attr("data-base-opacity") ?? "0.28")
        cell.interrupt()
          .attr("fill-opacity", 0.85)
          .attr("stroke-width", 3)
          .attr("filter", `drop-shadow(0 0 18px ${color}cc) drop-shadow(0 0 6px ${color}88)`)
          .transition().duration(HIT_FADE_MS).ease(d3.easeCubicOut)
          .attr("fill-opacity", base)
          .attr("stroke-width", 1)
          .attr("filter", "none")
      }

      const dot = svg.select<SVGCircleElement>(`[data-dot-id="${id}"]`)
      if (!dot.empty()) {
        const node = dot.node()!
        node.classList.remove("memory-dot-hit")
        void node.getBoundingClientRect()
        node.classList.add("memory-dot-hit")
      }
    })

    // Shared exclusion rule for every region-treatment selection below: an
    // element identified by `idAttr` is excluded from the region pulse if it
    // was itself one of the ids just flashed directly above — the direct
    // treatment already covers it, and re-applying the weaker region pulse on
    // top would either fight the direct transition (cells share the default
    // unnamed d3 transition) or visibly downgrade the direct-hit dot the
    // instant the region loop runs. One rule, reused for cells and dots
    // rather than re-deriving "is this id in newHits" per selection.
    const isDirectHit = (el: Element, idAttr: string) => newHits.includes(el.getAttribute(idAttr) ?? "")

    // Apply the region pulse to every rendered cell of each hit category,
    // EXCEPT the ids just flashed above directly. The directly-hit node(s)
    // keep the strong pop; the rest of the region gets the weaker pulse, so a
    // hit that IS rendered still reads as visually stronger than its
    // surrounding region. This cell wash is the lingering region signal —
    // the whole category area lights up and slowly dims over the same
    // HIT_FADE_MS the direct hit uses (see REGION_FLASH_OPACITY above).
    hitCategoryIds.forEach(catId => {
      const region = svg.selectAll<SVGPathElement, unknown>(`[data-category-id="${catId}"]`)
        .filter(function () { return !isDirectHit(this, "data-id") })
      region.each(function () {
        const cellSel = d3.select(this)
        const color = cellSel.attr("data-color")
        const base = parseFloat(cellSel.attr("data-base-opacity") ?? "0.28")
        cellSel.interrupt()
          .attr("fill-opacity", Math.max(base, REGION_FLASH_OPACITY))
          .attr("filter", `drop-shadow(0 0 12px ${color}88)`)
          .transition().duration(HIT_FADE_MS).ease(d3.easeCubicOut)
          .attr("fill-opacity", base)
          .attr("filter", "none")
      })

      // A short attention spike layered on top of the slow cell wash: a
      // radius+opacity pulse (region-dot-hit, globals.css — same animation
      // family as the direct hit's memory-dot-hit, smaller swing, shorter
      // duration) on every rendered dot in the hit category, drawing the eye
      // to the region whose cells are now fading. Class toggle uses remove →
      // forced reflow → add (same dance as the direct-hit dot above) so the
      // CSS animation restarts on every hit instead of only playing once.
      const regionDots = svg.selectAll<SVGCircleElement, unknown>(`[data-dot-category-id="${catId}"]`)
        .filter(function () { return !isDirectHit(this, "data-dot-id") })
      regionDots.each(function () {
        this.classList.remove("memory-dot-region-hit")
        void this.getBoundingClientRect()
        this.classList.add("memory-dot-region-hit")
      })

      // Watermark label pulse — subtle (see label-region-hit in globals.css);
      // on a region with few or no rendered dots the label may be the only
      // thing on screen there. Same restart dance, applied to the whole
      // label group (title + count) as one unit.
      const labelGroup = svg.select<SVGGElement>(`[data-label-category-id="${catId}"]`)
      if (!labelGroup.empty()) {
        const node = labelGroup.node()!
        node.classList.remove("memory-label-hit")
        void node.getBoundingClientRect()
        node.classList.add("memory-label-hit")
      }
    })

    // Optogenetic recall: a blue fibre lights the recalled region, the cells
    // fire in the order the light reaches them, the exact recalled cells fire
    // hardest, and the signal travels along their synapses (lib/opto.ts). It is
    // a transient overlay on top of the category-coloured flash above.
    if (newHits.length > 0 || hitCategoryIds.size > 0) {
      fireOptogenetic(svgRef.current, { categoryIds: hitCategoryIds, itemIds: newHits })
    }
  }, [highlightIds, hitKinds, hitKey, drawVersion])

  // Merge personal notes (extraItems) into the base list, deduplicating by id.
  const allItems = useMemo(() => {
    if (!extraItems?.length) return items
    const known = new Set(items.map(i => i.id))
    const notes: MemoryItemSummary[] = extraItems
      .filter(n => !known.has(n.id))
      .map(n => ({ id: n.id, kind: n.kind, scope: n.scope, subject: n.subject, title: n.title, tags: n.tags, status: "active", updatedAt: n.updatedAt, hitCount: 0 }))
    return [...items, ...notes]
  }, [items, extraItems])

  const filteredItems = useMemo(() => {
    // No query → the bounded graph preview. With a query → the server results
    // (full-store, server-first) UNION any preview matches, so already-loaded
    // nodes that also match aren't dropped and results outside the preview
    // still appear. A client-side filter alone would silently scope search to
    // the preview.
    let base: MemoryItemSummary[]
    if (!query?.trim()) {
      base = allItems
    } else {
      const q = query.trim().toLowerCase()
      const clientHits = allItems.filter(it =>
        it.id.toLowerCase().includes(q) ||
        it.title.toLowerCase().includes(q) ||
        it.kind.includes(q) ||
        (it.subject?.toLowerCase().includes(q)) ||
        (it.tags?.some(t => t.toLowerCase().includes(q)))
      )
      const byId = new Map<string, MemoryItemSummary>()
      for (const it of [...serverResults, ...clientHits]) byId.set(it.id, it)
      base = Array.from(byId.values())
    }
    // Apply the render budget FAIRLY ACROSS CATEGORIES.
    //
    // This used to be one global sort by hitCount followed by a slice, which
    // can drop entire categories. hitCount is the wrong tiebreak for that:
    // counts are per-replica and in-memory, so after any restart every cell
    // is 0 and the stable sort simply preserves fetch order, cutting
    // whichever categories land in the tail. Worse, it is self-reinforcing —
    // a cell that is never rendered can never be highlighted, so its
    // hitCount stays 0 and it is dropped again next time. A quiet category
    // (few cells, or a kind whose cells just haven't been hit yet) could be
    // starved out entirely and never light up.
    //
    // Round-robin instead: sort within each category by hitCount, then take
    // one per category per pass. Every category with items keeps at least
    // one node, so every category can light up, while busy categories still
    // get proportionally more of the budget.
    if (base.length <= nodeBudget) return base
    const byCat = new Map<string, MemoryItemSummary[]>()
    for (const it of base) {
      const k = catKey(it)
      const bucket = byCat.get(k)
      if (bucket) bucket.push(it)
      else byCat.set(k, [it])
    }
    const buckets = Array.from(byCat.values())
    for (const bucket of buckets) {
      bucket.sort((a, b) => (b.hitCount ?? 0) - (a.hitCount ?? 0))
    }
    const budgeted: MemoryItemSummary[] = []
    for (let round = 0; budgeted.length < nodeBudget; round++) {
      let placed = false
      for (const bucket of buckets) {
        if (round >= bucket.length) continue
        budgeted.push(bucket[round])
        placed = true
        if (budgeted.length >= nodeBudget) break
      }
      if (!placed) break
    }
    return budgeted
  }, [allItems, serverResults, query, nodeBudget])

  useEffect(() => {
    const container = containerRef.current
    if (!container || filteredItems.length === 0) return
    const width = container.clientWidth || 800
    const height = container.clientHeight || 600

    const svg = d3.select(svgRef.current)
    svg.selectAll("*").remove()
    svg.attr("viewBox", `0 0 ${width} ${height}`)
    // SVG was wiped — reset so the hit effect re-applies current highlightIds
    prevHighlightIds.current = new Set()
    setDrawVersion(v => v + 1)

    const defs = svg.append("defs")
    const gradDefs = [
      { id: "fade-l", x1: "0%", y1: "0%", x2: "100%", y2: "0%" },
      { id: "fade-r", x1: "100%", y1: "0%", x2: "0%", y2: "0%" },
      { id: "fade-t", x1: "0%", y1: "0%", x2: "0%", y2: "100%" },
      { id: "fade-b", x1: "0%", y1: "100%", x2: "0%", y2: "0%" },
    ]
    gradDefs.forEach(({ id, x1, y1, x2, y2 }) => {
      const g = defs.append("linearGradient").attr("id", id)
        .attr("x1", x1).attr("y1", y1).attr("x2", x2).attr("y2", y2)
      g.append("stop").attr("offset", "0%").attr("stop-color", "black").attr("stop-opacity", 1)
      g.append("stop").attr("offset", "22%").attr("stop-color", "black").attr("stop-opacity", 0)
      g.append("stop").attr("offset", "100%").attr("stop-color", "black").attr("stop-opacity", 0)
    })
    const mask = defs.append("mask").attr("id", "rmask")
    mask.append("rect").attr("width", width).attr("height", height).attr("fill", "white")
    ;["fade-l", "fade-r", "fade-t", "fade-b"].forEach(id => {
      mask.append("rect").attr("width", width).attr("height", height)
        .attr("fill", `url(#${id})`).attr("fill-opacity", 1)
        .style("mix-blend-mode", "multiply")
    })

    const points = itemSeeds(filteredItems, width, height)
    const delaunay = Delaunay.from(points)
    const voronoi = delaunay.voronoi([0, 0, width, height])

    const root = svg.append("g")
    const g = root.append("g").attr("mask", "url(#rmask)").attr("class", GRAPH_LAYER_CLASS)

    // Voronoi cells — category-colored fill; hits animated via the highlightIds effect above
    filteredItems.forEach((item, i) => {
      const color = catOf(item).color
      const isNews = item.kind === "recent_change"
      const baseOpacity = isNews ? 0.55 : 0.28
      g.append("path")
        .attr("d", voronoi.renderCell(i))
        .attr("data-id", item.id)
        .attr("data-category-id", catOf(item).id)
        .attr("data-color", color)
        .attr("data-base-opacity", baseOpacity)
        .attr("fill", color)
        .attr("fill-opacity", baseOpacity)
        .attr("stroke", isNews ? color : "rgba(255,255,255,0.22)")
        .attr("stroke-width", isNews ? 2 : 1)
        .attr("filter", isNews ? `drop-shadow(0 0 8px ${color}88)` : "none")
        .attr("cursor", "pointer")
        .on("mouseenter", function (ev: MouseEvent) {
          d3.select(this).attr("fill-opacity", 0.72).attr("stroke", "rgba(255,255,255,0.6)")
          const rect = svgRef.current!.getBoundingClientRect()
          setTooltip({ item, x: ev.clientX - rect.left, y: ev.clientY - rect.top })
          ev.stopPropagation()
        })
        .on("mousemove", (ev: MouseEvent) => {
          const rect = svgRef.current!.getBoundingClientRect()
          setTooltip(prev => prev ? { ...prev, x: ev.clientX - rect.left, y: ev.clientY - rect.top } : null)
        })
        .on("mouseleave", function () {
          d3.select(this)
            .attr("fill-opacity", baseOpacity)
            .attr("stroke", isNews ? color : "rgba(255,255,255,0.22)")
          setTooltip(null)
        })
        .on("click", (ev: MouseEvent) => {
          if (tapShowsTooltip) {
            const rect = svgRef.current!.getBoundingClientRect()
            setTooltip(prev =>
              prev?.item.id === item.id
                ? null  // tap again to dismiss
                : { item, x: ev.clientX - rect.left, y: ev.clientY - rect.top }
            )
            return
          }
          setTooltip(null)
          if (onSelect) onSelect(item.id, ev.clientX, ev.clientY, item.kind)
          else router.push(`/item?id=${item.id}`)
        })
    })

    // Category group watermark labels — one per category, centered on the
    // bounding box of all cells in that category.
    const catGroups: Record<string, { cat: Category; items: MemoryItemSummary[]; indices: number[] }> = {}
    filteredItems.forEach((item, i) => {
      const cat = catOf(item)
      catGroups[cat.id] ??= { cat, items: [], indices: [] }
      catGroups[cat.id].items.push(item)
      catGroups[cat.id].indices.push(i)
    })
    for (const { cat, items: kitems, indices } of Object.values(catGroups)) {
      const color = cat.color
      const label = cat.label.toUpperCase()
      // Label with the category's TRUE total (full store), not the count of
      // drawn nodes — the graph only renders a capped preview per category, so
      // kitems.length would misreport a large category as the cap (e.g. "25").
      // Sum the real per-kind totals for the kinds this category actually holds.
      // During a search there's no full-store total for the matched subset, so
      // fall back to the drawn count (which is the match count).
      let count = kitems.length
      if (!query?.trim() && Object.keys(totalsByKind).length > 0) {
        const kinds = new Set(kitems.map(it => it.kind))
        let sum = 0
        kinds.forEach(k => { sum += totalsByKind[k] ?? 0 })
        if (sum > 0) count = sum
      }

      // Collect all polygon vertices, clamped to the visible canvas
      const pad = 24
      let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity
      for (const idx of indices) {
        const poly = voronoi.cellPolygon(idx)
        if (!poly) continue
        for (const [vx, vy] of poly) {
          const cx2 = Math.max(pad, Math.min(width - pad, vx))
          const cy2 = Math.max(pad, Math.min(height - pad, vy))
          if (cx2 < minX) minX = cx2
          if (cx2 > maxX) maxX = cx2
          if (cy2 < minY) minY = cy2
          if (cy2 > maxY) maxY = cy2
        }
      }
      const lx = (minX + maxX) / 2
      const ly = (minY + maxY) / 2
      const regionW = maxX - minX
      const regionH = maxY - minY

      // Scale font to fit within the region width, clamped to a sensible range
      const charW = 0.6  // rough em-width per char at 1px font-size
      const fitsW = regionW / (label.length * charW * 1.15)
      const fitsH = regionH * 0.35
      const fontSize = Math.max(28, Math.min(120, Math.min(fitsW, fitsH)))

      // Both texts share a group so a region hit can pulse them together as
      // one unit (memory-label-hit, see the highlightIds effect + globals.css)
      // — tagged with its own attribute rather than reusing data-category-id
      // (which stays cell-path-only) so existing cell-treatment selectors
      // don't start matching label nodes too.
      const labelGroup = g.append("g").attr("data-label-category-id", cat.id)

      // Watermark — suppressed on mobile (chips already name each kind)
      if (showKindLabels) {
        labelGroup.append("text")
          .attr("x", lx).attr("y", ly)
          .attr("text-anchor", "middle")
          .attr("dominant-baseline", "middle")
          .attr("font-size", fontSize).attr("font-weight", "900")
          .attr("letter-spacing", "0.10em")
          .attr("fill", color).attr("fill-opacity", 0.05)
          .attr("stroke", color).attr("stroke-opacity", 0.28).attr("stroke-width", 1)
          .attr("pointer-events", "none")
          .text(label)
      }

      // Count below
      labelGroup.append("text")
        .attr("x", lx).attr("y", showKindLabels ? ly + fontSize * 0.65 : ly)
        .attr("text-anchor", "middle")
        .attr("dominant-baseline", "middle")
        .attr("font-size", Math.max(13, fontSize * 0.20)).attr("font-weight", "700")
        .attr("letter-spacing", "0.08em")
        .attr("fill", color).attr("fill-opacity", 0.55)
        .attr("pointer-events", "none")
        .text(showKindLabels ? `${count} ${count === 1 ? "item" : "items"}` : String(count))
    }

    // Synapses — faint lines between nodes that share at least one tag
    const synapseGroup = g.append("g").attr("pointer-events", "none")
    for (let i = 0; i < filteredItems.length; i++) {
      const tagsI = new Set(filteredItems[i].tags ?? [])
      if (tagsI.size === 0) continue
      for (let j = i + 1; j < filteredItems.length; j++) {
        const tagsJ = filteredItems[j].tags ?? []
        const shared = tagsJ.filter(t => tagsI.has(t)).length
        if (shared < 2) continue
        const [x1, y1] = points[i]
        const [x2, y2] = points[j]
        const dist = Math.hypot(x2 - x1, y2 - y1)
        if (dist > Math.min(width, height) * 0.55) continue  // skip very long cross-graph edges
        const opacity = Math.min(0.35, 0.10 + shared * 0.08)
        const colorI = catOf(filteredItems[i]).color
        const gradId = `syn-${i}-${j}`
        const grad = defs.append("linearGradient").attr("id", gradId)
          .attr("x1", x1).attr("y1", y1).attr("x2", x2).attr("y2", y2)
          .attr("gradientUnits", "userSpaceOnUse")
        grad.append("stop").attr("offset", "0%").attr("stop-color", colorI)
        grad.append("stop").attr("offset", "100%").attr("stop-color", catOf(filteredItems[j]).color)
        synapseGroup.append("line")
          .attr("x1", x1).attr("y1", y1).attr("x2", x2).attr("y2", y2)
          .attr("stroke", `url(#${gradId})`)
          .attr("stroke-width", 1 + shared * 0.4)
          .attr("stroke-opacity", opacity)
          .attr("filter", `drop-shadow(0 0 3px ${colorI}66)`)
      }
    }

    // Item dots
    filteredItems.forEach((item, i) => {
      const color = catOf(item).color
      const [px, py] = points[i]
      g.append("circle")
        .attr("cx", px).attr("cy", py)
        .attr("r", 5)
        .attr("data-dot-id", item.id)
        .attr("data-dot-category-id", catOf(item).id)
        .attr("fill", color)
        .attr("fill-opacity", 0.35)
        .attr("stroke", color)
        .attr("stroke-width", 1.5)
        .attr("pointer-events", "none")
    })

  }, [filteredItems, router, showKindLabels, tapShowsTooltip, totalsByKind, query])

  if (loading) return (
    <div className={cn("flex items-center justify-center h-full text-muted-foreground", className)}>
      <RefreshCw className="h-4 w-4 animate-spin mr-2" /> Loading memory…
    </div>
  )
  if (error) return (
    <div className={cn("flex items-center justify-center h-full text-destructive text-sm", className)}>{error}</div>
  )
  if (items.length === 0) return (
    <div className={cn("flex flex-col items-center justify-center h-full text-muted-foreground gap-2", className)}>
      <BrainCog className="h-8 w-8 opacity-30" />
      <p className="text-sm">No memory items yet.</p>
    </div>
  )

  return (
    <div ref={containerRef} className={cn("relative w-full h-full", className)}>
      <svg ref={svgRef} className="w-full h-full cursor-grab active:cursor-grabbing" />

      {/* Radial fade overlay */}
      <div className="absolute inset-0 pointer-events-none" style={{
        background: "radial-gradient(ellipse at center, transparent 52%, hsl(var(--background)) 88%)"
      }} />



      {/* Tooltip — tappable in tap mode to open the item */}
      {tooltip && (
        <div
          className={cn(
            "absolute z-50 max-w-[260px] rounded-xl border border-white/10 bg-slate-900/40 backdrop-blur-2xl p-3 shadow-2xl text-sm",
            tapShowsTooltip && onSelect ? "pointer-events-auto cursor-pointer active:scale-[0.98] transition-transform" : "pointer-events-none"
          )}
          style={{
            left: Math.min(tooltip.x + 14, (containerRef.current?.clientWidth ?? 500) - 280),
            top: Math.max(4, Math.min(tooltip.y - 8, (containerRef.current?.clientHeight ?? 400) - 150)),
          }}
          onClick={tapShowsTooltip && onSelect ? () => { const t = tooltip; setTooltip(null); onSelect(t.item.id, t.x, t.y, t.item.kind) } : undefined}
        >
          <div className="flex items-center gap-2 mb-1.5">
            <span className="h-2.5 w-2.5 rounded-full flex-shrink-0" style={{ background: catOf(tooltip.item).color }} />
            <span className="font-semibold leading-tight line-clamp-2">{tooltip.item.title}</span>
          </div>
          <div className="flex flex-wrap gap-1 text-[10px] mb-2">
            <span className="rounded-full border border-border px-1.5 py-0.5">{catOf(tooltip.item).label}</span>
            {tooltip.item.subject && tooltip.item.scope !== "private" && <span className="rounded-full border border-border px-1.5 py-0.5">{tooltip.item.subject}</span>}
            {tooltip.item.tags?.slice(0, 3).map(t => (
              <span key={t} className="rounded-full border border-border px-1.5 py-0.5 text-muted-foreground">{t}</span>
            ))}
          </div>
          <p className="mt-1 text-[10px] text-primary/70">{tapShowsTooltip ? "Tap to open →" : "Click to open panel →"}</p>
        </div>
      )}
    </div>
  )
}
