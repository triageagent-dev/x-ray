import { describe, it, expect } from "vitest"
import { hitTagMap } from "../lib/api"

describe("hitTagMap", () => {
  it("maps each hit id to its tags, a null entry to no tags", () => {
    const m = hitTagMap({ itemIds: ["a", "b"], query: "", kinds: ["policy", "note"], tags: [["area:a"], null] })
    expect(m?.get("a")).toEqual(["area:a"])
    expect(m?.get("b")).toEqual([])
  })

  it("returns undefined without tags or with fewer tags than ids (an older server)", () => {
    expect(hitTagMap({ itemIds: ["a"], query: "" })).toBeUndefined()
    expect(hitTagMap({ itemIds: ["a", "b"], query: "", tags: [["area:a"]] })).toBeUndefined()
    expect(hitTagMap(null)).toBeUndefined()
  })
})
