/**
 * Region pulse for a hit in a category split off its kind by tag (ADR-035).
 *
 * The hit event carried ids and kinds only, so the graph resolved a hit by kind
 * and pulsed the kind's bare category: with "policy" split into A and B, a hit
 * on an A policy lit the bare "Rest" region. The event now also carries each
 * item's tags (hitTagMap), and the pulse lands in A.
 */
import { describe, it, expect, vi, beforeAll } from "vitest"
import { render, waitFor } from "@testing-library/react"
import type { MemoryItemSummary } from "../lib/api"

// Same identity on every call: the draw effect depends on the router.
const mockRouter = { push: vi.fn() }
vi.mock("next/navigation", () => ({ useRouter: () => mockRouter }))

const item = (id: string, tag?: string): MemoryItemSummary => ({
  id, kind: "policy", scope: "shared", title: id, tags: tag ? [tag] : [], status: "active",
  updatedAt: "2026-10-30T00:00:00Z", hitCount: 0,
})
const summary = {
  items: [item("a-1", "area:a"), item("a-2", "area:a"), item("b-1", "area:b"), item("b-2", "area:b"), item("r-1"), item("r-2")],
  total: 6,
  totalsByKind: { policy: 6 },
  totalsByCategory: { "policy|area:a": 2, "policy|area:b": 2, policy: 2 },
}

vi.mock("../lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../lib/api")>()
  return {
    ...actual,
    fetchMemorySummary: () => Promise.resolve(summary),
    fetchMemorySearch: vi.fn().mockResolvedValue([]),
    subscribeMemoryEvents: vi.fn(() => () => {}),
  }
})

import { MemoryGraph } from "../components/memory-graph"
import { loadRuntimeCategories } from "../lib/category-config"

beforeAll(async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
    categories: [
      { id: "a", kind: "policy", tag: "area:a", label: "A", tier: 0.6, angle: 1 },
      { id: "b", kind: "policy", tag: "area:b", label: "B", tier: 0.6, angle: 3 },
      { id: "rest", kind: "policy", label: "Rest", tier: 0.3, angle: 5 },
    ],
  }), { status: 200, headers: { "Content-Type": "application/json" } })))
  await loadRuntimeCategories()
  vi.unstubAllGlobals()
})

const opacities = (container: HTMLElement, cat: string) =>
  Array.from(container.querySelectorAll(`[data-category-id="${cat}"]`)).map(el => parseFloat(el.getAttribute("fill-opacity") ?? "0"))

describe("MemoryGraph region pulse with hit tags", () => {
  it("pulses the tagged category the hit belongs to, not the kind's bare one", async () => {
    const { container } = render(
      <MemoryGraph
        highlightIds={new Set(["a-99"])}
        hitKinds={new Map([["a-99", "policy"]])}
        hitTags={new Map([["a-99", ["area:a"]]])}
        hitKey={1}
      />
    )
    await waitFor(() => {
      const a = opacities(container, "a")
      expect(a.length).toBeGreaterThan(0)
      a.forEach(o => { expect(o).toBeGreaterThan(0.28); expect(o).toBeLessThan(0.85) })
      opacities(container, "b").forEach(o => expect(o).toBeCloseTo(0.28, 5))
      opacities(container, "rest").forEach(o => expect(o).toBeCloseTo(0.28, 5))
    })
  })

  it("falls back to the kind's bare category without tags (an older server)", async () => {
    const { container } = render(
      <MemoryGraph
        highlightIds={new Set(["a-99"])}
        hitKinds={new Map([["a-99", "policy"]])}
        hitKey={1}
      />
    )
    await waitFor(() => {
      const rest = opacities(container, "rest")
      expect(rest.length).toBeGreaterThan(0)
      rest.forEach(o => expect(o).toBeGreaterThan(0.28))
      opacities(container, "a").forEach(o => expect(o).toBeCloseTo(0.28, 5))
    })
  })
})
