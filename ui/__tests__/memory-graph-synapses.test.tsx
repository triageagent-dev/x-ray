/**
 * Synapse lines stay, but at rest they are faint and carry no glow, so a dense
 * store does not bury its regions; the lines of the node under the pointer take
 * the full style and return to rest when the pointer leaves.
 */
import { describe, it, expect, vi } from "vitest"
import { render, waitFor, fireEvent } from "@testing-library/react"
import type { MemoryItemSummary } from "../lib/api"

// Same identity on every call: the draw effect depends on the router.
const mockRouter = { push: vi.fn() }
vi.mock("next/navigation", () => ({ useRouter: () => mockRouter }))

const item = (id: string, tags: string[]): MemoryItemSummary => ({
  id, kind: "stable_fact", scope: "shared", title: id, tags, status: "active",
  updatedAt: "2026-10-30T00:00:00Z", hitCount: 0,
})
// x-1 and x-2 share two tags (one line); y-1 shares two with neither.
const summary = {
  items: [item("x-1", ["area:x", "topic:x"]), item("x-2", ["area:x", "topic:x"]), item("y-1", ["area:y", "topic:y"]), item("y-2", ["area:y", "topic:y"])],
  total: 4,
  totalsByKind: { stable_fact: 4 },
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

const lineOf = (c: HTMLElement, a: string, b: string) => c.querySelector(`line[data-a="${a}"][data-b="${b}"]`)

describe("MemoryGraph synapse lines", () => {
  it("draws every line faint at rest and brightens a node's lines on hover", async () => {
    const { container } = render(<MemoryGraph />)
    await waitFor(() => expect(lineOf(container, "x-1", "x-2")).not.toBeNull())
    const x = lineOf(container, "x-1", "x-2")!
    const y = lineOf(container, "y-1", "y-2")!
    expect(parseFloat(x.getAttribute("stroke-opacity")!)).toBeLessThanOrEqual(0.12)
    expect(x.getAttribute("filter")).toBeNull()

    fireEvent.mouseEnter(container.querySelector('[data-id="x-1"]')!)
    expect(parseFloat(x.getAttribute("stroke-opacity")!)).toBeGreaterThan(0.2)
    expect(x.getAttribute("filter")).toContain("drop-shadow")
    expect(parseFloat(y.getAttribute("stroke-opacity")!)).toBeLessThanOrEqual(0.12)

    fireEvent.mouseLeave(container.querySelector('[data-id="x-1"]')!)
    expect(parseFloat(x.getAttribute("stroke-opacity")!)).toBeLessThanOrEqual(0.12)
    expect(x.getAttribute("filter")).toBeNull()

    // The other end of the same line lights it too.
    fireEvent.mouseEnter(container.querySelector('[data-id="x-2"]')!)
    expect(parseFloat(x.getAttribute("stroke-opacity")!)).toBeGreaterThan(0.2)
    fireEvent.mouseLeave(container.querySelector('[data-id="x-2"]')!)
  })

  it("keeps the hover working for an id a CSS selector cannot hold", async () => {
    summary.items.push(item('q"1', ["area:q", "topic:q"]), item('q"2', ["area:q", "topic:q"]))
    const { container } = render(<MemoryGraph />)
    const cell = await waitFor(() => {
      const el = Array.from(container.querySelectorAll("[data-id]")).find(e => e.getAttribute("data-id") === 'q"1')
      expect(el).toBeTruthy()
      return el!
    })
    expect(() => fireEvent.mouseEnter(cell)).not.toThrow()
    const lit = Array.from(container.querySelectorAll("line")).some(l => parseFloat(l.getAttribute("stroke-opacity") ?? "0") > 0.2)
    expect(lit).toBe(true)
    summary.items.splice(-2, 2)
  })
})
