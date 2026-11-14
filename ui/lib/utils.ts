import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * Reduce an untrusted redirect target to a path on THIS origin, falling back
 * to "/".
 *
 * A `startsWith("/")` test is the usual version of this check and it is
 * broken: `//evil.com` and `/\evil.com` both pass it, and a browser resolves
 * both as absolute URLs to another host. Parsing against our own origin and
 * comparing the result is the only form that holds.
 */
export function sameOriginPath(target: string | null | undefined, origin: string): string {
  if (!target) return "/"
  try {
    const url = new URL(target, origin)
    if (url.origin !== new URL(origin).origin) return "/"
    return url.pathname + url.search + url.hash
  } catch {
    return "/"
  }
}

/**
 * Build stamp of THIS UI, inlined at export time from the Makefile's VERSION
 * (the same value ko tags the image with), so the page can show which UI
 * build is live next to the core's own version. "dev" outside `make build`.
 */
export const UI_VERSION = process.env.NEXT_PUBLIC_UI_VERSION || "dev"

/**
 * Prefix a URL for a file in `public/` with the export's BASE_PATH.
 *
 * Next.js bakes `basePath` into every URL it emits — `/_next/*`, `next/link`
 * routes — but leaves alone the ones written literally here. Under a sub-path
 * those literals point at the origin ROOT, which is the API's, not ours (the
 * same asymmetry that keeps `lib/api.ts` building root-relative `/v2` URLs),
 * so `/logo.svg` is asked of the API and 404s. Every literal reference to a
 * bundled asset goes through this.
 */
export const BASE_PATH = process.env.NEXT_PUBLIC_BASE_PATH || ""

export function asset(path: string): string {
  return BASE_PATH + path
}

/**
 * The path the API is mounted under on this origin, when the build sets
 * NEXT_PUBLIC_API_URL to a path ("/path": the server and this export share that
 * prefix behind a path-routing proxy). "" when the API is at the origin root or
 * on another origin. Things the server serves itself (WebSockets, the PWA
 * manifest and service worker) are addressed through it.
 */
export function apiMount(apiUrl: string | undefined): string {
  const v = apiUrl ?? ""
  return v.startsWith("/") && !v.startsWith("//") ? v.replace(/\/+$/, "") : ""
}

export const API_MOUNT = apiMount(process.env.NEXT_PUBLIC_API_URL)
