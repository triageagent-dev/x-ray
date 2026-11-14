"use client"

import { useState, useMemo, useCallback, useEffect, useRef } from "react"
import { MemoryGraph } from "@/components/memory-graph"
import { MemoryItemPanel, PANEL_W, PANEL_H_MAX } from "@/components/memory-item-panel"
import { useMemoryList, useMemoryHits, useMetrics } from "@/hooks/use-memory"
import { StatsHud } from "@/components/stats-hud"
import { useVoiceSession } from "@/hooks/use-voice-session"
import { useLiveQuota, type LiveQuota } from "@/lib/quota-store"
import { useAuth } from "@/hooks/use-auth"
import { apiFetch, fetchUserNotes, subscribeMemoryEvents, hitKindMap, hitTagMap, type MemoryItemSummary, api } from "@/lib/api"
import { useTenant } from "@/components/tenant-provider"
import { Search, LogOut } from "lucide-react"
import { cn, UI_VERSION } from "@/lib/utils"

let panelCounter = 0

interface PinnedPanel {
  key: number
  id: string
  kind?: string
  x: number
  y: number
  minimized: boolean
}

interface StoredPanel { id: string; kind?: string; x: number; y: number; minimized: boolean }

function pinnedStorageKey(username: string) {
  return `xray.pinned.${username}`
}

function loadStoredPanels(username: string): StoredPanel[] {
  try {
    const raw = localStorage.getItem(pinnedStorageKey(username))
    if (!raw) return []
    const parsed = JSON.parse(raw)
    // Support legacy format (plain string array)
    if (Array.isArray(parsed) && parsed.length && typeof parsed[0] === "string") {
      return (parsed as string[]).map(id => ({ id, x: 0, y: 0, minimized: false }))
    }
    return parsed as StoredPanel[]
  } catch { return [] }
}

function saveStoredPanels(username: string, panels: PinnedPanel[]) {
  try {
    const data: StoredPanel[] = panels.map(p => ({ id: p.id, kind: p.kind, x: p.x, y: p.y, minimized: p.minimized }))
    localStorage.setItem(pinnedStorageKey(username), JSON.stringify(data))
  } catch {}
}

type QuotaLike = { used?: number; limit: number; remaining?: number }

// pickFreshestQuota returns whichever quota reports the higher cumulative `used`
// (mirrors stats-hud.tsx): the live WS push leads the 30s poll, so the larger
// `used` is the more current. On a fresh load there is no live value and the
// poll wins.
function pickFreshestQuota(live: LiveQuota | null, polled?: QuotaLike): QuotaLike | undefined {
  if (!live) return polled
  if (!polled) return live
  if (live.limit === 0 || polled.limit === 0) return live // unlimited — used is moot
  return (live.used ?? 0) >= (polled.used ?? 0) ? live : polled
}

function clampPos(x: number, y: number): { x: number; y: number } {
  const vw = window.innerWidth
  const vh = window.innerHeight
  return {
    x: Math.max(16, Math.min(vw - PANEL_W - 16, x)),
    y: Math.max(16, Math.min(vh - PANEL_H_MAX - 16, y)),
  }
}

function anchorPos(clickX: number, clickY: number): { x: number; y: number } {
  const w = PANEL_W
  const vw = window.innerWidth
  const vh = window.innerHeight
  let x = clickX + 20
  let y = clickY - 60
  if (x + w > vw - 16) x = clickX - w - 20
  if (x < 16) x = 16
  if (y + PANEL_H_MAX > vh - 16) y = vh - PANEL_H_MAX - 16
  if (y < 16) y = 16
  return { x, y }
}

export default function MemoryIndexPage() {
  const [query, setQuery] = useState("")
  const [active, setActive] = useState<{ id: string; kind?: string; x: number; y: number } | null>(null)
  const [pinned, setPinned] = useState<PinnedPanel[]>([])
  const [topKey, setTopKey] = useState<number | null>(null)

  const [version, setVersion] = useState<string | null>(null)
  const [voiceEnabled, setVoiceEnabled] = useState(false)
  const pttActiveRef = useRef(false)
  const limitReachedRef = useRef(false)
  const searchRef = useRef<HTMLInputElement>(null)
  const { hits } = useMemoryHits()
  const { items: newsItems } = useMemoryList({ kind: "recent_change" })

  // Just the IDs — backend fetches content via tool if user asks
  const pinnedIds = useMemo(() => pinned.map(p => p.id), [pinned])
  const pinnedIdSet = useMemo(() => new Set(pinnedIds), [pinnedIds])

  const { status, bubbles } = useVoiceSession({
    enabled: voiceEnabled,
    pinnedIds,
    onDone: useCallback(() => setVoiceEnabled(false), []),
  })

  const liveQuota = useLiveQuota()
  const polledMetrics = useMetrics(30_000)
  const quota = pickFreshestQuota(liveQuota, polledMetrics?.quota)
  const limitReached = !!quota && quota.limit > 0 &&
    (quota.remaining ?? (quota.limit - (quota.used ?? 0))) <= 0

  useEffect(() => {
    apiFetch(`${api}/version`).then(r => r.json()).then(d => setVersion(d.version)).catch(() => {})
  }, [])

  // Keep the keydown handler (deps []) reading the latest limit state via a ref.
  useEffect(() => { limitReachedRef.current = limitReached }, [limitReached])

  const longPressTimer = useRef<ReturnType<typeof setTimeout> | null>(null)
  useEffect(() => {
    const isTyping = () => {
      const el = document.activeElement
      return el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement
    }
    const onDown = (e: KeyboardEvent) => {
      if (e.code !== "Space" || e.repeat || isTyping()) return
      e.preventDefault()
      if (limitReachedRef.current) return // daily audio limit reached — no new session
      if (!pttActiveRef.current && !longPressTimer.current) {
        longPressTimer.current = setTimeout(() => {
          longPressTimer.current = null
          pttActiveRef.current = true
          setVoiceEnabled(true)
        }, 1000)
      }
    }
    const onUp = (e: KeyboardEvent) => {
      if (e.code !== "Space" || isTyping()) return
      e.preventDefault()
      if (longPressTimer.current) { clearTimeout(longPressTimer.current); longPressTimer.current = null }
      pttActiveRef.current = false
    }
    window.addEventListener("keydown", onDown)
    window.addEventListener("keyup", onUp)
    return () => {
      window.removeEventListener("keydown", onDown)
      window.removeEventListener("keyup", onUp)
      if (longPressTimer.current) { clearTimeout(longPressTimer.current); longPressTimer.current = null }
    }
  }, [])

  const { username, userID } = useAuth()
  const [userMenuOpen, setUserMenuOpen] = useState(false)
  const { tenants, tenant, setTenant, canSwitch } = useTenant()
  const [hitKey, setHitKey] = useState(0)
  const [userNotes, setUserNotes] = useState<MemoryItemSummary[]>([])
  const pinnedRestoredRef = useRef<string | null>(null)

  // Fetch personal agent notes when logged in; map to summary shape for graph.
  useEffect(() => {
    if (!username || username === "anonymous") { setUserNotes([]); return }
    fetchUserNotes().then(items =>
      setUserNotes(items.map(it => ({
        id: it.id, kind: it.kind, scope: it.scope, subject: it.subject,
        title: it.title, tags: it.tags, status: it.status,
        updatedAt: it.updatedAt, hitCount: 0,
      })))
    ).catch(() => {})
  }, [username])

  // Re-sync agent notes on SSE change events so new notes appear immediately.
  useEffect(() => {
    if (!username || username === "anonymous") return
    return subscribeMemoryEvents(
      () => {},
      (evt) => {
        // Personal notes arrive on this user's own SSE channel (server-side
        // owner-scoped delivery), so no client-side subject check is needed.
        if (evt.op === "upsert" && evt.item?.kind === "note") {
          const n = evt.item
          setUserNotes(prev => {
            const idx = prev.findIndex(i => i.id === n.id)
            const summary: MemoryItemSummary = {
              id: n.id, kind: n.kind, scope: n.scope, subject: n.subject,
              title: n.title, tags: n.tags, status: n.status,
              updatedAt: n.updatedAt, hitCount: 0,
            }
            if (idx >= 0) { const next = [...prev]; next[idx] = summary; return next }
            return [...prev, summary]
          })
        } else if (evt.op === "delete" && evt.id) {
          setUserNotes(prev => prev.filter(i => i.id !== evt.id))
        }
      }
    )
  }, [username, userID])

  // Restore pinned panels from localStorage once per login (not on every render).
  useEffect(() => {
    if (!username || username === "anonymous") return
    if (pinnedRestoredRef.current === username) return
    pinnedRestoredRef.current = username
    const stored = loadStoredPanels(username)
    if (!stored.length) return
    setPinned(stored.map((p, i) => {
      const key = ++panelCounter
      // If position was never saved (legacy zeros), fall back to grid layout
      const x = p.x || 24 + (i % 3) * (PANEL_W + 16)
      const y = p.y || 80 + Math.floor(i / 3) * 220
      return { key, id: p.id, kind: p.kind, x, y, minimized: p.minimized ?? false }
    }))
  }, [username])

  // Persist pinned panels (id, position, minimized) whenever pinned changes.
  useEffect(() => {
    if (!username || username === "anonymous") return
    if (pinnedRestoredRef.current !== username) return  // don't persist before restore
    saveStoredPanels(username, pinned)
  }, [username, pinned])

  useEffect(() => {
    if (!hits?.itemIds?.length) return
    setHitKey(k => k + 1)
  }, [hits])

  const highlightIds = useMemo(() => {
    if (hits?.itemIds?.length) return new Set(hits.itemIds)
    const latest = [...newsItems]
      .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
      .slice(0, 3)
      .map(it => it.id)
    return new Set(latest)
  }, [hits, newsItems])

  // Per-hit kind — undefined when hits is the "latest news" fallback above
  // (no live hit) or the server hasn't sent kinds; MemoryGraph degrades to
  // cell-only flashing in that case.
  const hitKinds = useMemo(() => hitKindMap(hits), [hits])
  const hitTags = useMemo(() => hitTagMap(hits), [hits])

  const handleSelect = useCallback((id: string, screenX: number, screenY: number, kind?: string) => {
    setActive(prev => prev?.id === id ? null : { id, kind, ...anchorPos(screenX, screenY) })
  }, [])

  const handlePin = useCallback((itemId: string, x: number, y: number, kind?: string) => {
    setPinned(prev => {
      if (prev.some(p => p.id === itemId)) return prev  // already pinned
      const key = ++panelCounter
      return [...prev, { key, id: itemId, kind, x, y, minimized: false }]
    })
    setActive(null)
  }, [])

  const handleActiveClose = useCallback(() => setActive(null), [])

  const handlePinnedClose = useCallback((key: number) => {
    setPinned(prev => prev.filter(p => p.key !== key))
  }, [])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return
      setActive(null)
      setQuery("")
      searchRef.current?.blur()
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [])

  const isActive = status === "active"
  const isConnecting = status === "connecting"

  return (
    <div className="relative h-full">
      <MemoryGraph
        query={query}
        highlightIds={highlightIds}
        hitKinds={hitKinds}
        hitTags={hitTags}
        hitKey={hitKey}
        extraItems={userNotes}
        onSelect={handleSelect}
        className="absolute inset-0"
      />

      <div className="absolute top-4 left-4 right-4 z-20 flex items-center justify-between gap-4">
        <span className={cn(
          "text-[15px] font-bold tracking-[0.3em] uppercase select-none bg-clip-text",
          isActive
            ? "text-red-400 animate-[xray-breathe_4s_ease-in-out_infinite]"
            : isConnecting
            ? "text-primary/50 animate-pulse"
            : "text-transparent animate-[xray-shimmer_12s_linear_infinite] [background-image:linear-gradient(90deg,#6d56c1,#1e7a9e,#1f8a60,#a03070,#6d56c1)] [background-size:300%_auto] opacity-60"
        )}>
          X-Ray
        </span>

        <div className="flex items-center gap-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground" />
            <input
              ref={searchRef}
              className="w-64 rounded-xl border border-border/50 bg-background/60 backdrop-blur-md pl-8 pr-4 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/40 focus:bg-background/80"
              placeholder="Search memories…"
              value={query}
              onChange={e => setQuery(e.target.value)}
            />
          </div>
          {username && (
            <div className="relative">
              <button
                onClick={() => setUserMenuOpen(v => !v)}
                className="flex items-center justify-center w-8 h-8 rounded-full text-[12px] font-semibold text-white/70 uppercase animate-[xray-shimmer_12s_linear_infinite] [background-image:linear-gradient(90deg,#6d56c1,#1e7a9e,#1f8a60,#a03070,#6d56c1)] [background-size:300%_auto] opacity-40 hover:opacity-70 transition-opacity"
                title={username}
              >
                {username[0]}
              </button>
              {userMenuOpen && (
                <>
                  <div className="fixed inset-0 z-10" onClick={() => setUserMenuOpen(false)} />
                  <div className="absolute top-full right-0 mt-1 z-20 min-w-[160px] rounded-xl border border-white/10 bg-zinc-900/90 backdrop-blur-md shadow-xl overflow-hidden">
                    <div className="px-3 py-2 text-[11px] text-white/30 truncate border-b border-white/5">{username}</div>
                    {canSwitch && (
                      <div className="px-3 py-2 border-b border-white/5">
                        <label className="block text-[10px] uppercase tracking-wide text-white/25 mb-1">Tenant</label>
                        <select
                          value={tenant}
                          onChange={(e) => setTenant(e.target.value)}
                          className="w-full bg-white/5 border border-white/10 rounded-md px-2 py-1 text-[12px] text-white/80 outline-none focus:border-white/25"
                        >
                          {tenants.map((t) => (
                            <option key={t} value={t} className="bg-zinc-900">{t}</option>
                          ))}
                        </select>
                      </div>
                    )}
                    <a
                      href={`${api}/auth/logout`}
                      className="flex items-center gap-2 px-3 py-2.5 text-[12px] text-white/60 hover:bg-white/5 hover:text-white transition-colors"
                    >
                      <LogOut size={13} />
                      Sign out
                    </a>
                  </div>
                </>
              )}
            </div>
          )}
        </div>
      </div>

      {(isActive || isConnecting) && bubbles.length > 0 && (
        <div className="absolute bottom-12 left-1/2 -translate-x-1/2 z-20 w-full max-w-lg px-4 flex flex-col-reverse gap-1.5 pointer-events-none">
          {bubbles.map(b => (
            <div key={b.id} className={b.role === "user" ? "flex justify-end" : "flex justify-start"}>
              <span className={
                b.role === "user"
                  ? "rounded-2xl rounded-br-sm bg-red-500/90 px-3 py-2 text-xs text-white leading-snug backdrop-blur-sm"
                  : "rounded-2xl rounded-bl-sm bg-background/80 px-3 py-2 text-xs text-foreground leading-snug backdrop-blur-sm border border-border/40"
              }>
                {b.text}
              </span>
            </div>
          ))}
        </div>
      )}

      {active && (
        <MemoryItemPanel
          key={`active-${active.id}`}
          id={active.id}
          kind={active.kind}
          initialX={active.x}
          initialY={active.y}
          pinnedIds={pinnedIdSet}
          onPin={handlePin}
          onClose={handleActiveClose}
        />
      )}

      {[...pinned].sort((a, b) => a.key === topKey ? 1 : b.key === topKey ? -1 : 0).map(p => (
        <MemoryItemPanel
          key={p.key}
          id={p.id}
          kind={p.kind}
          initialX={p.x}
          initialY={p.y}
          initialMinimized={p.minimized}
          pinned
          onFocus={() => setTopKey(p.key)}
          onMove={(x, y) => setPinned(prev => prev.map(q => q.key === p.key ? { ...q, x, y } : q))}
          onMinimizedChange={m => setPinned(prev => prev.map(q => q.key === p.key ? { ...q, minimized: m } : q))}
          onClose={() => handlePinnedClose(p.key)}
        />
      ))}

      {voiceEnabled ? (
        <button
          onClick={() => setVoiceEnabled(false)}
          className="absolute bottom-3 left-1/2 -translate-x-1/2 z-20 text-[11px] text-white/30 hover:text-white/60 transition-colors select-none tracking-widest uppercase cursor-pointer"
        >
          say goodbye to finish
        </button>
      ) : (
        <span className="absolute bottom-3 left-1/2 -translate-x-1/2 z-20 text-[11px] text-white select-none pointer-events-none tracking-widest uppercase animate-[ptt-hint-shimmer_12s_ease-in-out_infinite]">
          {limitReached ? "voice limit reached" : "hold space to talk x-ray"}
        </span>
      )}

      {/* The HUD and the version stamp share one bottom-right column, so the
          stamp grows upward from the bottom edge instead of sliding under the
          HUD. */}
      <div className="absolute bottom-3 right-4 z-20 flex flex-col items-end gap-1 max-w-[45%] pointer-events-none">
        <StatsHud />
        {/* Two builds, a row each: the core's version is fetched from
            /v2/version, the UI's is baked into the export — so a UI redeploy is
            visible here even when the core did not change. One per line rather
            than one run, because a `git describe` version is long enough that
            the pair reads as a single unreadable string on one row. */}
        <div className="text-[11px] text-muted-foreground/40 font-mono select-none pointer-events-none text-right leading-[1.6] [overflow-wrap:anywhere]">
          <div>ui {UI_VERSION}</div>
          {version && <div>api {version}</div>}
        </div>
      </div>
    </div>
  )
}
