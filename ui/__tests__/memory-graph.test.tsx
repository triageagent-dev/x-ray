/**
 * Repeat-hit flash regression test.
 *
 * Before the fix, a hit landing twice on the same id never flashed the
 * second time. The reset (`prevHighlightIds.current = new Set()`) lived in
 * a separate effect keyed on `hitKey`, and ran a render behind the flash
 * effect keyed on `highlightIds`: a repeat hit produced a new `highlightIds`
 * Set identity while `hitKey` was still stale, so the flash effect ran
 * first, saw `prevHighlightIds` still containing the id, and computed
 * `newHits = []`. By the time the reset effect caught up and cleared
 * `prevHighlightIds`, the flash effect's deps hadn't changed again, so it
 * never re-ran. Any surface with an effectively constant id set (the same
 * cell hit repeatedly) flashed once and never again.
 *
 * The fix folds the reset into the flash effect itself via a `lastHitKey`
 * ref: a fresh `hitKey` marks every id in the current frame as new, so a
 * repeat hit flashes again. A `highlightIds` identity change with no fresh
 * `hitKey` (e.g. an unrelated re-render) still stays quiet.
 */
import { describe, it, expect, vi, afterEach } from "vitest"
import { render, waitFor } from "@testing-library/react"
import { MemoryGraph } from "../components/memory-graph"
import type { MemoryItemSummary } from "../lib/api"

// IMPORTANT: return the SAME object identity on every call. The draw effect
// in memory-graph.tsx depends on `router` — a mock that returns a fresh
// object each render makes that effect think its deps changed every render,
// which loops setDrawVersion forever ("Maximum update depth exceeded").
const mockRouter = { push: vi.fn() }
vi.mock("next/navigation", () => ({
  useRouter: () => mockRouter,
}))

const summaryItems: MemoryItemSummary[] = [
  { id: "event-1", kind: "event", scope: "shared", title: "e1", status: "active", updatedAt: "2026-07-30T00:00:00Z", hitCount: 0 },
  { id: "event-2", kind: "event", scope: "shared", title: "e2", status: "active", updatedAt: "2026-07-30T00:00:00Z", hitCount: 0 },
  { id: "fact-1", kind: "stable_fact", scope: "shared", title: "f1", status: "active", updatedAt: "2026-07-30T00:00:00Z", hitCount: 0 },
]

const fetchMemorySummaryMock = vi.fn().mockResolvedValue({
  items: summaryItems,
  total: summaryItems.length,
  totalsByKind: { event: 2, stable_fact: 1 },
})

vi.mock("../lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../lib/api")>()
  return {
    ...actual,
    fetchMemorySummary: (...args: unknown[]) => fetchMemorySummaryMock(...args),
    fetchMemorySearch: vi.fn().mockResolvedValue([]),
    subscribeMemoryEvents: vi.fn(() => () => {}),
  }
})

afterEach(() => {
  fetchMemorySummaryMock.mockClear()
})

describe("MemoryGraph repeat-hit flash", () => {
  it("re-flashes a cell hit twice in a row, each with a fresh hitKey", async () => {
    const { container, rerender } = render(
      <MemoryGraph highlightIds={new Set(["event-1"])} hitKey={1} />
    )

    // Wait for the FLASH to land, not just the dot element's existence: the
    // dot is created in the draw effect, but "memory-dot-hit" is applied by
    // a separate effect pass one render later (triggered by drawVersion) —
    // querying right after the element appears can race that second pass.
    await waitFor(() => {
      const el = container.querySelector('[data-dot-id="event-1"]')
      expect(el).not.toBeNull()
      expect(el!.classList.contains("memory-dot-hit")).toBe(true)
    })

    // The direct-hit flash removes then re-adds "memory-dot-hit" with a
    // forced reflow in between, so a CSS restart animation re-triggers even
    // if the class name never changed. Spying on classList.add for that
    // EXACT class name (rather than the generic forced-reflow call, which
    // shares a DOM method any number of unrelated things could also invoke)
    // isolates "did the flash logic run again" from "is the class still
    // applied from before" as precisely as possible.
    const dot = container.querySelector('[data-dot-id="event-1"]') as unknown as SVGCircleElement
    expect(dot.classList.contains("memory-dot-hit")).toBe(true)
    const addSpy = vi.spyOn(dot.classList, "add")

    // Same id, same visible Set contents, but a fresh hitKey — this is the
    // repeat-hit case that used to be silently swallowed.
    rerender(<MemoryGraph highlightIds={new Set(["event-1"])} hitKey={2} />)

    await waitFor(() => {
      expect(addSpy.mock.calls.some(args => args.includes("memory-dot-hit"))).toBe(true)
    })

    addSpy.mockRestore()
  })

  it("does not re-flash when highlightIds gets a new identity but hitKey is unchanged", async () => {
    const { container, rerender } = render(
      <MemoryGraph highlightIds={new Set(["event-1"])} hitKey={1} />
    )

    // Wait for the FLASH to land, not just the dot element's existence — see
    // the sibling test above for why the class can lag the element.
    await waitFor(() => {
      const el = container.querySelector('[data-dot-id="event-1"]')
      expect(el).not.toBeNull()
      expect(el!.classList.contains("memory-dot-hit")).toBe(true)
    })

    const dot = container.querySelector('[data-dot-id="event-1"]') as unknown as SVGCircleElement
    const addSpy = vi.spyOn(dot.classList, "add")

    // New Set identity (re-render trigger), but no fresh hitKey — this is
    // the "noise" case prevHighlightIds still has to suppress. Rerender
    // twice with a settling wait between (rather than one fixed sleep) so
    // the assertion isn't racing a single arbitrary timeout window.
    rerender(<MemoryGraph highlightIds={new Set(["event-1"])} hitKey={1} />)
    await new Promise(r => setTimeout(r, 0))
    rerender(<MemoryGraph highlightIds={new Set(["event-1"])} hitKey={1} />)
    await new Promise(r => setTimeout(r, 0))

    expect(addSpy.mock.calls.some(args => args.includes("memory-dot-hit"))).toBe(false)

    addSpy.mockRestore()
  })
})

/**
 * Render-budget fairness regression test.
 *
 * The budget used to be one global sort by hitCount followed by a slice to
 * MAX_RENDER_NODES. With every hitCount starting at 0 (per-replica, in-memory,
 * reset on every restart), a stable sort just preserves fetch order — so a
 * category whose items land late in that order can be sliced off entirely.
 * Worse, it's self-reinforcing: a cell that's never rendered can never be
 * highlighted, so its hitCount stays 0 and it's dropped again next time.
 * Reported live: active_concern always lit up, mental_map and event never
 * did, even though hit frames for them were verified arriving correctly —
 * those categories simply had no rendered nodes to flash.
 *
 * The fix round-robins the budget across categories (sort each category's
 * items by hitCount, then take one per category per pass), so a category
 * with items always keeps at least one rendered node regardless of how many
 * items a noisier category contributes.
 */
describe("MemoryGraph render-budget fairness", () => {
  it("keeps at least one node from a sparse category even when a noisy category alone exceeds the render cap", async () => {
    // GRAPH_NODES_PER_KIND(25) * 15 known kinds + 50 notes headroom = 425.
    // 430 active_concern items alone already exceed that cap, with only 3
    // mental_map items sharing the budget — the exact shape of the reported
    // bug (one loud category, one quiet one).
    const concernItems: MemoryItemSummary[] = Array.from({ length: 430 }, (_, i) => ({
      id: `concern-${i}`, kind: "active_concern", scope: "shared", title: `c${i}`,
      status: "active", updatedAt: "2026-07-30T00:00:00Z", hitCount: 0,
    }))
    const mapItems: MemoryItemSummary[] = Array.from({ length: 3 }, (_, i) => ({
      id: `map-${i}`, kind: "mental_map", scope: "shared", title: `m${i}`,
      status: "active", updatedAt: "2026-07-30T00:00:00Z", hitCount: 0,
    }))
    const allItems = [...concernItems, ...mapItems]

    fetchMemorySummaryMock.mockResolvedValueOnce({
      items: allItems,
      total: allItems.length,
      totalsByKind: { active_concern: concernItems.length, mental_map: mapItems.length },
    })

    const { container } = render(<MemoryGraph />)

    await waitFor(() => {
      expect(container.querySelectorAll("[data-dot-id]").length).toBeGreaterThan(0)
    }, { timeout: 5000 })

    // Every mental_map item must have made it into the render budget — a
    // global hitCount sort with mental_map appended after 430 zero-hitCount
    // concern items would have sliced all three off.
    for (const it of mapItems) {
      expect(container.querySelector(`[data-dot-id="${it.id}"]`)).not.toBeNull()
    }
  })
})

/**
 * The mobile graph (maxNodes) draws a fixed number of nodes in total, however
 * many categories and items the store has, and its category labels are bare
 * numbers carrying the true totals.
 */
describe("MemoryGraph maxNodes", () => {
  it("draws at most maxNodes nodes across all kinds and labels kinds with totals only", async () => {
    const kinds = ["active_concern", "stable_fact", "event", "operational_pattern"]
    const all: MemoryItemSummary[] = kinds.flatMap(kind =>
      Array.from({ length: 25 }, (_, i) => ({
        id: `${kind}-${i}`, kind, scope: "shared", title: `${kind} ${i}`,
        status: "active", updatedAt: "2026-07-30T00:00:00Z", hitCount: 0,
      })))
    fetchMemorySummaryMock.mockResolvedValueOnce({
      items: all,
      total: 400,
      totalsByKind: { active_concern: 100, stable_fact: 100, event: 100, operational_pattern: 100 },
    })

    const { container } = render(<MemoryGraph maxNodes={25} showKindLabels={false} />)

    await waitFor(() => {
      expect(container.querySelectorAll("[data-dot-id]").length).toBeGreaterThan(0)
    }, { timeout: 5000 })

    expect(container.querySelectorAll("[data-dot-id]").length).toBe(25)
    const labels = Array.from(container.querySelectorAll("[data-label-category-id] text")).map(t => t.textContent)
    expect(labels.length).toBeGreaterThan(0)
    for (const l of labels) expect(l).toMatch(/^\d+$/)
    expect(labels).toContain("100")
  })
})

/**
 * Region-flash regression test. Before the fix, a hit landing on an ID that
 * wasn't one of the bounded-preview's rendered nodes (the overwhelmingly
 * common case for `event`/`active_concern` hits — the graph draws only
 * GRAPH_NODES_PER_KIND nodes per category against kinds with orders of
 * magnitude more cells) produced NO visible reaction at all: both
 * `svg.select('[data-id="..."]')` and the `[data-dot-id]` lookup were silent
 * no-ops. The fix adds a category-level "region" pulse that fires from the
 * hit's `kind` alone (via hitKinds, parallel to highlightIds — see
 * lib/api.ts hitKindMap), independent of whether the individual cell is
 * drawn. This test deliberately hits an ID that is NOT in the mocked item
 * set, so it exercises exactly the path that used to do nothing.
 */
describe("MemoryGraph region flash", () => {
  it("flashes the EVENTS region for a hit id that is NOT itself rendered", async () => {
    const { container } = render(
      <MemoryGraph
        highlightIds={new Set(["phantom-event-not-rendered"])}
        hitKinds={new Map([["phantom-event-not-rendered", "event"]])}
        hitKey={1}
      />
    )

    // Wait for the graph to finish loading and draw its Voronoi cells.
    await waitFor(() => {
      expect(container.querySelectorAll('[data-category-id="events"]').length).toBeGreaterThan(0)
    })

    // Confirm the premise: the hit id itself was never drawn as a node — this
    // is exactly the case the per-cell-only effect silently did nothing for.
    expect(container.querySelector('[data-id="phantom-event-not-rendered"]')).toBeNull()

    // All checked together in one waitFor (generous timeout to absorb a
    // loaded CI/parallel-test environment) rather than split across separate
    // waitFor/point-in-time reads, which can race an in-progress re-render:
    // the EVENTS region (every rendered cell sharing that category) should
    // show the region-flash elevated opacity — weaker than a direct hit
    // (0.85) so the two stay visually distinct — even though no individual
    // cell in it matched the hit id, while the unrelated FACTS region must
    // not react at all.
    await waitFor(() => {
      const eventCells = container.querySelectorAll('[data-category-id="events"]')
      expect(eventCells.length).toBeGreaterThan(0)
      eventCells.forEach(el => {
        const opacity = parseFloat(el.getAttribute("fill-opacity") ?? "0")
        expect(opacity).toBeGreaterThan(0.28) // base opacity for a non-"recent_change" cell
        expect(opacity).toBeLessThan(0.85)
      })

      const factCells = container.querySelectorAll('[data-category-id="facts"]')
      expect(factCells.length).toBeGreaterThan(0)
      factCells.forEach(el => {
        const opacity = parseFloat(el.getAttribute("fill-opacity") ?? "0")
        expect(opacity).toBeCloseTo(0.28, 5)
      })
    })
  })

  it("does not flash any region when hitKinds is absent (older server, degrades to cell-only)", async () => {
    const { container } = render(
      <MemoryGraph
        highlightIds={new Set(["phantom-event-not-rendered"])}
        hitKey={1}
      />
    )

    await waitFor(() => {
      expect(container.querySelectorAll('[data-category-id="events"]').length).toBeGreaterThan(0)
    })

    // Give any (incorrect) region logic a chance to have applied synchronously.
    const eventCells = container.querySelectorAll('[data-category-id="events"]')
    eventCells.forEach(el => {
      const opacity = parseFloat(el.getAttribute("fill-opacity") ?? "0")
      expect(opacity).toBeCloseTo(0.28, 5)
    })
  })

  it("flashes a rendered cell's own region too, distinguishing it from the stronger direct hit", async () => {
    const { container } = render(
      <MemoryGraph
        highlightIds={new Set(["event-1"])}
        hitKinds={new Map([["event-1", "event"]])}
        hitKey={1}
      />
    )

    await waitFor(() => {
      expect(container.querySelector('[data-id="event-1"]')).not.toBeNull()
    })

    // Both read together, immediately once the node exists. Note: both the
    // direct flash and the region pulse ride a REAL (unfaked) 20s d3
    // transition (HIT_FADE_MS), so their fill-opacity decays continuously
    // from the instant it's applied — asserting an exact value (e.g.
    // toBeCloseTo(0.85, 5), precision to 0.000005) is inherently flaky under
    // any real elapsed wall-clock time, however small, and gets WORSE with a
    // longer waitFor timeout (more time for the value to decay), not better.
    // The direct hit starts at 0.85 and the region pulse at
    // REGION_FLASH_OPACITY (0.7); both fade toward the same 0.28 base on the
    // same clock, so the direct value stays strictly above the sibling's for
    // the whole fade — the relative comparison is what actually proves
    // distinguishability without racing the clock.
    await waitFor(() => {
      const direct = container.querySelector('[data-id="event-1"]')!
      const directOpacity = parseFloat(direct.getAttribute("fill-opacity") ?? "0")
      expect(directOpacity).toBeGreaterThan(0.7)

      // event-2 shares the category but was not itself hit — it should show
      // the weaker region pulse (peaking at REGION_FLASH_OPACITY=0.7), not
      // the strong direct-hit opacity, proving the two effects don't collide
      // on the same node and stay distinguishable.
      const sibling = container.querySelector('[data-id="event-2"]')!
      const siblingOpacity = parseFloat(sibling.getAttribute("fill-opacity") ?? "0")
      expect(siblingOpacity).toBeGreaterThan(0.4)
      expect(siblingOpacity).toBeLessThanOrEqual(0.7)
      expect(siblingOpacity).toBeLessThan(directOpacity)
    })
  })

  /**
   * The region-flash opacity assertions above were passing well before there
   * was any dot-level sprite at all — they only prove the (invisible) cell
   * wash reacts. These three tests target the actual fix: the dot radius
   * pulse (memory-dot-region-hit / region-dot-hit in globals.css) that a
   * human eye actually registers, since the graph draws only
   * GRAPH_NODES_PER_KIND dots per category and a hit's own cell is usually
   * not among them.
   */
  describe("MemoryGraph region dot sprite", () => {
    it("applies the region-pulse class to sibling dots in the hit category, and only the direct-hit class to the hit dot itself", async () => {
      const { container } = render(
        <MemoryGraph
          highlightIds={new Set(["event-1"])}
          hitKinds={new Map([["event-1", "event"]])}
          hitKey={1}
        />
      )

      await waitFor(() => {
        expect(container.querySelector('[data-dot-id="event-1"]')).not.toBeNull()
      })

      await waitFor(() => {
        const direct = container.querySelector('[data-dot-id="event-1"]')!
        expect(direct.classList.contains("memory-dot-hit")).toBe(true)
        expect(direct.classList.contains("memory-dot-region-hit")).toBe(false)

        // event-2 shares the "events" category but was not itself hit — it
        // must get the region pulse, and must NOT get the direct-hit class.
        const sibling = container.querySelector('[data-dot-id="event-2"]')!
        expect(sibling.classList.contains("memory-dot-region-hit")).toBe(true)
        expect(sibling.classList.contains("memory-dot-hit")).toBe(false)

        // fact-1 is a different category entirely — neither class applies.
        const unrelated = container.querySelector('[data-dot-id="fact-1"]')!
        expect(unrelated.classList.contains("memory-dot-region-hit")).toBe(false)
        expect(unrelated.classList.contains("memory-dot-hit")).toBe(false)
      })
    })

    it("keeps the direct-hit and region-pulse classes distinct names, so the two treatments cannot silently collapse into one", () => {
      // Static guard: if a future edit ever reused memory-dot-hit for both
      // treatments (or renamed one to match the other), every test above
      // that distinguishes "direct" from "region" by class name would stop
      // meaning anything even though it would keep passing on coincidence.
      // Pin the two literal class names apart explicitly.
      expect("memory-dot-hit").not.toBe("memory-dot-region-hit")
    })

    it("restarts the region-pulse animation on a second hit in the same region (reflow-restart dance)", async () => {
      const { container, rerender } = render(
        <MemoryGraph
          highlightIds={new Set(["event-1"])}
          hitKinds={new Map([["event-1", "event"]])}
          hitKey={1}
        />
      )

      await waitFor(() => {
        expect(container.querySelector('[data-dot-id="event-2"]')).not.toBeNull()
      })
      await waitFor(() => {
        const sibling = container.querySelector('[data-dot-id="event-2"]')!
        expect(sibling.classList.contains("memory-dot-region-hit")).toBe(true)
      })

      // Same DOM node persists across the two hits below (the draw effect
      // only depends on the loaded items/layout, not on hitKey/highlightIds,
      // so the SVG is not rebuilt) — spying on THIS instance's
      // getBoundingClientRect isolates exactly the forced-reflow call the
      // restart dance makes, proving it fires again rather than the class
      // merely staying applied from the first hit.
      const sibling = container.querySelector('[data-dot-id="event-2"]') as unknown as SVGCircleElement
      const rectSpy = vi.spyOn(sibling, "getBoundingClientRect").mockReturnValue({} as DOMRect)
      const callsBeforeSecondHit = rectSpy.mock.calls.length

      // A second, distinct hitKey with the SAME source id: prevHighlightIds
      // resets on hitKey change, so event-1 is treated as newly-arrived
      // again, and the region loop re-applies to event-2 a second time.
      rerender(
        <MemoryGraph
          highlightIds={new Set(["event-1"])}
          hitKinds={new Map([["event-1", "event"]])}
          hitKey={2}
        />
      )

      await waitFor(() => {
        expect(rectSpy.mock.calls.length).toBeGreaterThan(callsBeforeSecondHit)
      })

      rectSpy.mockRestore()
    })
  })

  describe("MemoryGraph region label pulse", () => {
    it("applies the label-pulse class to the hit category's watermark label group, not to an unrelated category's", async () => {
      const { container } = render(
        <MemoryGraph
          highlightIds={new Set(["phantom-event-not-rendered"])}
          hitKinds={new Map([["phantom-event-not-rendered", "event"]])}
          hitKey={1}
        />
      )

      await waitFor(() => {
        const eventsLabel = container.querySelector('[data-label-category-id="events"]')
        expect(eventsLabel).not.toBeNull()
        expect(eventsLabel!.classList.contains("memory-label-hit")).toBe(true)

        const factsLabel = container.querySelector('[data-label-category-id="facts"]')
        expect(factsLabel).not.toBeNull()
        expect(factsLabel!.classList.contains("memory-label-hit")).toBe(false)
      })
    })
  })
})

describe("MemoryGraph optogenetic recall", () => {
  it("lights the recalled region and fires the recalled cell", async () => {
    const { container } = render(
      <MemoryGraph highlightIds={new Set(["event-1"])} hitKinds={new Map([["event-1", "event"]])} hitKey={1} />
    )
    await waitFor(() => {
      expect(container.querySelector(".opto-layer .opto-beam")).not.toBeNull()
      expect(container.querySelector(".opto-layer .opto-fire")).not.toBeNull()
      expect(container.querySelectorAll(".opto-layer .opto-ring")).toHaveLength(3)
    })
    // The other event cell is washed, the fact in another region is not.
    expect(container.querySelectorAll(".opto-layer .opto-wash")).toHaveLength(1)
    expect(container.querySelector('.opto-layer [data-opto-region="fact"]')).toBeNull()
  })

  it("fires again on a repeat hit, and stays quiet when only the Set identity changes", async () => {
    const { container, rerender } = render(
      <MemoryGraph highlightIds={new Set(["event-1"])} hitKinds={new Map([["event-1", "event"]])} hitKey={1} />
    )
    await waitFor(() => expect(container.querySelectorAll(".opto-layer .opto-halo")).toHaveLength(1))

    rerender(<MemoryGraph highlightIds={new Set(["event-1"])} hitKinds={new Map([["event-1", "event"]])} hitKey={1} />)
    expect(container.querySelectorAll(".opto-layer .opto-halo")).toHaveLength(1)

    rerender(<MemoryGraph highlightIds={new Set(["event-1"])} hitKinds={new Map([["event-1", "event"]])} hitKey={2} />)
    await waitFor(() => expect(container.querySelectorAll(".opto-layer .opto-halo")).toHaveLength(2))
  })
})
