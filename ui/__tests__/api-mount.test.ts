import { describe, it, expect } from "vitest"
import { apiMount } from "../lib/utils"

describe("apiMount", () => {
  it("is the path when the API shares this origin under a prefix", () => {
    expect(apiMount("/path")).toBe("/path")
    expect(apiMount("/path/")).toBe("/path")
    expect(apiMount("/a/b")).toBe("/a/b")
  })

  it("is empty at the origin root and for an API on another origin", () => {
    expect(apiMount(undefined)).toBe("")
    expect(apiMount("")).toBe("")
    expect(apiMount("http://localhost:8080")).toBe("")
    expect(apiMount("https://api.example")).toBe("")
    expect(apiMount("//api.example")).toBe("")
  })
})
