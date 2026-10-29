"use client"

import { useState, useRef, useCallback, useEffect, useMemo } from "react"
import { Sparkles, LogOut, Search, X } from "lucide-react"
import { cn } from "@/lib/utils"
import { useMemoryList, useMemoryHits, useMetrics, useMemorySearch } from "@/hooks/use-memory"
import { useLiveQuota } from "@/lib/quota-store"
import { useVoiceSession } from "@/hooks/use-voice-session"
import { useAuth } from "@/hooks/use-auth"
import { MemoryCard } from "@/components/memory-card"
import { MemoryGraph } from "@/components/memory-graph"
import { MobileItemOverlay } from "@/components/mobile-item-overlay"
import { categoryById, categoryOf, MOBILE_GRAPH_NODES } from "@/lib/category-config"
import { useCategories } from "@/hooks/use-categories"
import { fetchUserNotes, subscribeMemoryEvents, hitKindMap, type MemoryItem, type MemoryItemSummary, api } from "@/lib/api"

// ─── Tab definitions ──────────────────────────────────────────────────────────
// Content tabs are the UI categories (icons/labels/colors from category-config);
// the centered "home" tab is the AI button (graph view + voice on long-press).
// Categories are arranged with home injected in the middle.
type Tab = { id: string; label: string; Icon: typeof Sparkles; color: string }

const HOME_TAB: Tab = { id: "home", label: "AI", Icon: Sparkles, color: "#a78bfa" }

// The full taxonomy (category-config.ts) has 12 categories — plenty for a
// desktop sidebar, but a bottom tab bar on a phone screen has room for only a
// handful before labels start clipping. Mobile shows a fixed subset; every
// other kind is still reachable by tapping its node on the Home graph, just
// without a dedicated browsing tab. Order here is left-to-right around the
// injected AI/home button. "notes" stays in the set — personal notes have no
// other mobile entry point.
const MOBILE_CATEGORY_IDS = ["concerns", "events", "facts", "notes"]

// Builds the tab list (home tab injected in the middle) from the current
// per-deployment category taxonomy (ADR-035 item 4), narrowed to the mobile
// subset above — ids are stable across relabeling, so this still tracks a
// deployment's custom labels/colors/icons, just not a custom category set.
function buildTabs(categories: ReturnType<typeof useCategories>): Tab[] {
  const byId = new Map(categories.map(c => [c.id, c]))
  const catTabs: Tab[] = MOBILE_CATEGORY_IDS
    .map(id => byId.get(id))
    .filter((c): c is NonNullable<typeof c> => !!c)
    .map(c => ({ id: c.id, label: c.label, Icon: c.icon, color: c.color }))
  const mid = Math.ceil(catTabs.length / 2)
  return [...catTabs.slice(0, mid), HOME_TAB, ...catTabs.slice(mid)]
}

interface SectionProps {
  onOpen: (id: string, kind?: string) => void
  query?: string
}

// Filter items by a free-text query over id, title, subject, content and tags.
function matchQuery(items: MemoryItem[], query?: string): MemoryItem[] {
  const q = query?.trim().toLowerCase()
  if (!q) return items
  return items.filter(it =>
    it.id.toLowerCase().includes(q) ||
    it.title.toLowerCase().includes(q) ||
    it.subject?.toLowerCase().includes(q) ||
    it.content?.toLowerCase().includes(q) ||
    it.tags?.some(t => t.toLowerCase().includes(q))
  )
}

// Merges backend search hits with local matches, deduped by id — server
// results first so a local match (already fully loaded, e.g. carrying live
// SSE updates) wins the id collision. Mirrors memory-graph.tsx's
// `filteredItems` merge pattern.
function mergeById(remote: MemoryItem[], local: MemoryItem[]): MemoryItem[] {
  const byId = new Map<string, MemoryItem>()
  for (const it of [...remote, ...local]) byId.set(it.id, it)
  return Array.from(byId.values())
}

// ─── Section content components ───────────────────────────────────────────────

// Generic category list: fetch by the category's backend kind, narrow to the
// category by (kind, tag) via the resolver, then sort newest-first. Events and
// Notes need special handling (date grouping / per-user fetch) and stay below.
function CategorySection({ categoryId, onOpen, query }: SectionProps & { categoryId: string }) {
  const category = categoryById(categoryId)
  const { items, loading } = useMemoryList({ kind: category?.kind })
  const { hits } = useMemoryHits()
  const hitSet = useMemo(() => new Set(hits?.itemIds ?? []), [hits])
  // Server-first search (mirrors memory-graph.tsx / kind-client.tsx):
  // useMemoryList above only holds a capped local fetch, so this reaches the
  // full store for whatever the local list doesn't already have.
  const { results: searchResults } = useMemorySearch(query ?? "", category?.kind)
  const filtered = useMemo(() => {
    const inCat = category ? items.filter(it => categoryOf(it).id === category.id) : items
    const sorted = [...inCat].sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
    const localHits = matchQuery(sorted, query)
    if (!query?.trim()) return localHits
    const remoteHits = category ? searchResults.filter(it => categoryOf(it).id === category.id) : searchResults
    return mergeById(remoteHits, localHits)
  }, [items, category, query, searchResults])
  return <CardList items={filtered} loading={loading} hitSet={hitSet} empty={`No ${(category?.label ?? "items").toLowerCase()} yet.`} onOpen={onOpen} />
}

function NotesSection({ onOpen, query }: SectionProps) {
  const { username } = useAuth()
  const { hits } = useMemoryHits()
  const hitSet = useMemo(() => new Set(hits?.itemIds ?? []), [hits])
  const [items, setItems] = useState<MemoryItem[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    if (!username || username === "anonymous") { setItems([]); setLoading(false); return }
    let cancelled = false
    fetchUserNotes()
      .then(notes => { if (!cancelled) setItems(notes) })
      .catch(() => { if (!cancelled) setItems([]) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [username])

  const sorted = useMemo(
    () => matchQuery([...items].sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime()), query),
    [items, query]
  )
  return <CardList items={sorted} loading={loading} hitSet={hitSet} empty="No personal notes yet." onOpen={onOpen} />
}

function groupByMonth(items: MemoryItem[]): [string, MemoryItem[]][] {
  const map = new Map<string, MemoryItem[]>()
  for (const it of items) {
    const d = it.eventDate ? new Date(it.eventDate) : null
    const key = d ? d.toLocaleDateString("en-US", { month: "long", year: "numeric" }) : "No date"
    if (!map.has(key)) map.set(key, [])
    map.get(key)!.push(it)
  }
  return Array.from(map.entries())
}

function EventsSection({ onOpen, query }: SectionProps) {
  const { items, loading } = useMemoryList({ kind: "event" })
  const { hits } = useMemoryHits()
  const hitSet = useMemo(() => new Set(hits?.itemIds ?? []), [hits])
  // Server-first search — see CategorySection's comment above; the local
  // `items` list is a capped fetch of kind=event cells, so a query (e.g. a
  // full event id) can match a cell outside it.
  const { results: searchResults } = useMemorySearch(query ?? "", "event")
  const sorted = useMemo(() => {
    const localHits = matchQuery([...items], query)
    const merged = query?.trim() ? mergeById(searchResults, localHits) : localHits
    return merged.sort((a, b) => {
      const da = a.eventDate ? new Date(a.eventDate).getTime() : 0
      const db = b.eventDate ? new Date(b.eventDate).getTime() : 0
      return da - db
    })
  }, [items, query, searchResults])
  const groups = useMemo(() => groupByMonth(sorted), [sorted])

  if (loading) return <Spinner />
  if (sorted.length === 0) return <Empty text={query ? "No matching events." : "No events yet."} />

  return (
    <div className="space-y-6 p-4 pt-3 pb-24">
      {groups.map(([month, groupItems]) => (
        <section key={month}>
          <h2 className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground mb-2 pl-1">{month}</h2>
          <div className="flex flex-col gap-2">
            {groupItems.map(item => (
              <MemoryCard key={item.id} item={item} highlighted={hitSet.has(item.id)} onOpen={onOpen} />
            ))}
          </div>
        </section>
      ))}
    </div>
  )
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

function Spinner() {
  return (
    <div className="flex items-center justify-center py-24">
      <div className="animate-spin rounded-full h-7 w-7 border-b-2 border-primary/40" />
    </div>
  )
}

function Empty({ text }: { text: string }) {
  return <p className="text-center text-muted-foreground py-24 text-sm">{text}</p>
}

function CardList({ items, loading, hitSet, empty, onOpen }: {
  items: MemoryItem[]
  loading: boolean
  hitSet: Set<string>
  empty: string
  onOpen: (id: string, kind?: string) => void
}) {
  if (loading) return <Spinner />
  if (items.length === 0) return <Empty text={empty} />
  return (
    <div className="flex flex-col gap-2 p-4 pt-3 pb-24">
      {items.map(item => (
        <MemoryCard key={item.id} item={item} highlighted={hitSet.has(item.id)} onOpen={onOpen} />
      ))}
    </div>
  )
}

// ─── Graph "Home" view ────────────────────────────────────────────────────
// The Voronoi graph centered as the landing screen. Tap a node to peek at it
// via a tooltip, then tap the tooltip to open the item; section browsing lives
// in the bottom tab bar.

function HomeSection({ onOpenItem, query }: { onOpenItem: (id: string, kind?: string) => void; query?: string }) {
  const { username, userID } = useAuth()
  const { hits } = useMemoryHits()
  const [userNotes, setUserNotes] = useState<MemoryItemSummary[]>([])

  const highlightIds = useMemo(
    () => (hits?.itemIds?.length ? new Set(hits.itemIds) : undefined),
    [hits]
  )
  // Per-hit kind — undefined when there's no live hit or the server hasn't
  // sent kinds; MemoryGraph degrades to cell-only flashing in that case.
  const hitKinds = useMemo(() => hitKindMap(hits), [hits])

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

  useEffect(() => {
    if (!username || username === "anonymous") return
    return subscribeMemoryEvents(
      () => {},
      (evt) => {
        // Notes arrive on this user's own SSE channel (owner-scoped delivery), so
        // no subject check is needed. Backend kind is "note".
        if (evt.op === "upsert" && evt.item?.kind === "note") {
          const n = evt.item
          setUserNotes(prev => {
            const idx = prev.findIndex(i => i.id === n.id)
            const s: MemoryItemSummary = {
              id: n.id, kind: n.kind, scope: n.scope, subject: n.subject,
              title: n.title, tags: n.tags, status: n.status, updatedAt: n.updatedAt, hitCount: 0,
            }
            if (idx >= 0) { const next = [...prev]; next[idx] = s; return next }
            return [...prev, s]
          })
        } else if (evt.op === "delete" && evt.id) {
          setUserNotes(prev => prev.filter(i => i.id !== evt.id))
        }
      }
    )
  }, [username, userID])

  return (
    <div className="relative h-full w-full">
      <MemoryGraph
        query={query}
        highlightIds={highlightIds}
        hitKinds={hitKinds}
        extraItems={userNotes}
        tapShowsTooltip
        onSelect={(id, _x, _y, kind) => onOpenItem(id, kind)}
        showKindLabels={false}
        maxNodes={MOBILE_GRAPH_NODES}
        className="absolute inset-0"
      />
    </div>
  )
}

// ─── Main swipe layout ────────────────────────────────────────────────────────

const SWIPE_THRESHOLD = 50

export function MobileSwipeLayout() {
  const categories = useCategories()
  const TABS = useMemo(() => buildTabs(categories), [categories])
  const HOME_IDX = useMemo(() => TABS.findIndex(t => t.id === "home"), [TABS])
  const [active, setActive] = useState<number>(HOME_IDX)
  const [dragging, setDragging] = useState(false)
  const [dragDelta, setDragDelta] = useState(0)
  const touchStart = useRef<{ x: number; y: number } | null>(null)
  const axisLock = useRef<"h" | "v" | null>(null)
  const [voiceEnabled, setVoiceEnabled] = useState(false)
  const [userMenuOpen, setUserMenuOpen] = useState(false)
  const [overlay, setOverlay] = useState<{ id: string; kind?: string } | null>(null)
  const [query, setQuery] = useState("")
  const { username } = useAuth()
  const { status, bubbles } = useVoiceSession({ enabled: voiceEnabled })
  const voiceActive = status === "active"
  const voiceConnecting = status === "connecting"
  const longPressTimer = useRef<ReturnType<typeof setTimeout> | null>(null)

  // Remaining voice token quota for the user menu. Prefer the live per-turn push
  // (fresher) over the polled metrics, taking whichever reports more usage.
  const metrics = useMetrics(30_000)
  const liveQuota = useLiveQuota()
  const voiceRemainingPct = useMemo(() => {
    const polled = metrics?.quota
    const q = liveQuota && polled
      ? (liveQuota.used ?? 0) >= (polled.used ?? 0) ? liveQuota : polled
      : liveQuota ?? polled
    if (!q || q.limit === 0) return null  // null = unlimited or unknown
    const remaining = q.remaining ?? (q.limit - (q.used ?? 0))
    return Math.max(0, Math.min(100, Math.round((remaining / q.limit) * 100)))
  }, [metrics, liveQuota])

  const homeIdx = HOME_IDX
  const onHome = active === homeIdx

  const goTo = useCallback((idx: number) => {
    setActive(Math.max(0, Math.min(TABS.length - 1, idx)))
    setQuery("")  // search is scoped per-tab; clear when switching
  }, [TABS])

  const openItem = useCallback((id: string, kind?: string) => {
    setOverlay({ id, kind })
  }, [])

  // AI center button: tap = graph Home (default onClick), long-press = voice.
  const onAITabTouchStart = useCallback(() => {
    longPressTimer.current = setTimeout(() => {
      setVoiceEnabled(v => {
        if (!v) goTo(homeIdx)
        return !v
      })
      longPressTimer.current = null
    }, 500)
  }, [goTo, homeIdx])

  const onAITabTouchEnd = useCallback(() => {
    if (longPressTimer.current) {
      clearTimeout(longPressTimer.current)
      longPressTimer.current = null
    }
  }, [])

  const onTouchStart = useCallback((e: React.TouchEvent) => {
    touchStart.current = { x: e.touches[0].clientX, y: e.touches[0].clientY }
    axisLock.current = null
    setDragging(true)
    setDragDelta(0)
  }, [])

  const onTouchMove = useCallback((e: React.TouchEvent) => {
    if (touchStart.current === null) return
    const dx = e.touches[0].clientX - touchStart.current.x
    const dy = e.touches[0].clientY - touchStart.current.y
    // Lock to an axis once movement is meaningful; ignore vertical drags so
    // scrolling a list never flips tabs.
    if (axisLock.current === null && (Math.abs(dx) > 8 || Math.abs(dy) > 8)) {
      axisLock.current = Math.abs(dx) > Math.abs(dy) ? "h" : "v"
    }
    if (axisLock.current === "h") setDragDelta(dx)
  }, [])

  const onTouchEnd = useCallback(() => {
    if (axisLock.current === "h" && Math.abs(dragDelta) >= SWIPE_THRESHOLD) {
      goTo(active + (dragDelta < 0 ? 1 : -1))
    }
    setDragging(false)
    setDragDelta(0)
    touchStart.current = null
    axisLock.current = null
  }, [dragDelta, active, goTo])

  const activeColor = TABS[active].color

  return (
    <div className="flex flex-col h-dvh overflow-hidden bg-background">
      {/* Header: brand left, user icon right. The active section is shown by
          the bottom tab bar, so no center page title here. */}
      <div className="flex-none flex items-center justify-between px-4 pt-3 pb-2 border-b border-border/30">
        <span className="text-[13px] font-bold tracking-[0.3em] uppercase select-none text-transparent bg-clip-text animate-[xray-shimmer_12s_linear_infinite] [background-image:linear-gradient(90deg,#6d56c1,#1e7a9e,#1f8a60,#a03070,#6d56c1)] [background-size:300%_auto] opacity-60">
          X-Ray
        </span>
        {username && (
          <div className="relative">
            <button
              onClick={() => setUserMenuOpen((v) => !v)}
              className="flex items-center justify-center w-7 h-7 rounded-full text-[11px] font-semibold text-white/70 uppercase animate-[xray-shimmer_12s_linear_infinite] [background-image:linear-gradient(90deg,#6d56c1,#1e7a9e,#1f8a60,#a03070,#6d56c1)] [background-size:300%_auto] opacity-40 hover:opacity-70 transition-opacity"
            >
              {username[0]}
            </button>
            {userMenuOpen && (
              <>
                <div className="fixed inset-0 z-10" onClick={() => setUserMenuOpen(false)} />
                <div className="absolute top-full right-0 mt-1 z-20 min-w-[170px] rounded-lg border border-white/10 bg-zinc-900 shadow-xl overflow-hidden">
                  <div className="px-3 py-2 text-[10px] text-white/30 truncate border-b border-white/5">{username}</div>
                  <div className="px-3 py-2.5 border-b border-white/5">
                    <div className="flex items-center justify-between text-[10px] mb-1">
                      <span className="text-white/30 uppercase tracking-wide">Voice quota</span>
                      <span className="text-white/60 tabular-nums">
                        {voiceRemainingPct === null ? "∞" : `${voiceRemainingPct}%`}
                      </span>
                    </div>
                    {voiceRemainingPct !== null && (
                      <div className="h-1 rounded-full bg-white/10 overflow-hidden">
                        <div
                          className="h-full rounded-full transition-all"
                          style={{
                            width: `${voiceRemainingPct}%`,
                            background: voiceRemainingPct <= 10 ? "#f87171" : voiceRemainingPct <= 30 ? "#fbbf24" : "#4ade80",
                          }}
                        />
                      </div>
                    )}
                  </div>
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

      {/* Context-aware search: all memories on Home, this category on a list tab */}
      <div className="flex-none px-3 py-2 border-b border-border/30">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
          <input
            value={query}
            onChange={e => setQuery(e.target.value)}
            placeholder={onHome ? "Search all memories…" : `Search ${TABS[active].label}…`}
            className="w-full rounded-xl border border-border/50 bg-background/60 pl-9 pr-9 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/40 focus:bg-background/80"
          />
          {query && (
            <button
              onClick={() => setQuery("")}
              className="absolute right-2.5 top-1/2 -translate-y-1/2 text-muted-foreground active:text-foreground"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>
      </div>

      {/* Swipeable content area */}
      <div
        className="flex-1 overflow-hidden relative"
        onTouchStart={onTouchStart}
        onTouchMove={onTouchMove}
        onTouchEnd={onTouchEnd}
      >
        {/* Each panel is absolutely sized to the container (inset-0) and slid into
            place by its offset from the active tab. No wide track / vw widths, so
            it can never overflow the screen (standalone PWA safe). */}
        {TABS.map(({ id }, idx) => {
          const q = idx === active ? query : ""
          const offset = `calc(${(idx - active) * 100}% + ${dragDelta}px)`
          return (
            <div
              key={id}
              className={cn(
                "absolute inset-0",
                id === "home" ? "overflow-hidden" : "overflow-y-auto overscroll-contain",
              )}
              style={{
                transform: `translateX(${offset})`,
                transition: dragging ? "none" : "transform 0.28s cubic-bezier(0.4,0,0.2,1)",
              }}
            >
              {id === "home" ? (
                <HomeSection onOpenItem={openItem} query={q} />
              ) : id === "events" ? (
                <EventsSection onOpen={openItem} query={q} />
              ) : id === "notes" ? (
                <NotesSection onOpen={openItem} query={q} />
              ) : (
                <CategorySection categoryId={id} onOpen={openItem} query={q} />
              )}
            </div>
          )
        })}

        {/* Voice bubbles — messenger-style, stack bottom to top, each auto-disappears */}
        {(voiceActive || voiceConnecting) && bubbles.length > 0 && (
          <div className="absolute bottom-14 left-1/2 -translate-x-1/2 z-20 w-full max-w-sm px-4 flex flex-col-reverse gap-1.5 pointer-events-none">
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

        {/* Voice hint — only on content tabs (chips occupy this zone on Map) and
            only when idle, so users discover hold-to-talk */}
        {onHome && !voiceActive && !voiceConnecting && (
          <div className="absolute bottom-7 left-1/2 -translate-x-1/2 z-10 pointer-events-none">
            <span className="text-[10px] tracking-widest uppercase text-white/35 animate-[ptt-hint-shimmer_12s_ease-in-out_infinite] select-none">
              hold AI to talk
            </span>
          </div>
        )}

        {/* Swipe indicator dots */}
        <div className="absolute bottom-2 left-1/2 -translate-x-1/2 flex gap-1.5 pointer-events-none">
          {TABS.map((tab, i) => (
            <span
              key={tab.id}
              className="rounded-full transition-all duration-200"
              style={{
                width: i === active ? 16 : 4,
                height: 4,
                background: i === active ? activeColor : "rgba(255,255,255,0.2)",
              }}
            />
          ))}
        </div>
      </div>

      {/* Bottom tab bar */}
      <nav
        className="flex-none flex items-stretch border-t border-border/40 bg-background/90 backdrop-blur-md"
        style={{ paddingBottom: "env(safe-area-inset-bottom, 0px)" }}
      >
        {TABS.map(({ id, label, Icon, color }, idx) => {
          const isActive = idx === active
          const isAI = id === "home"

          // The AI button stands out (shimmer icon) and carries the voice
          // long-press; voice state recolors/animates it.
          if (isAI) {
            return (
              <button
                key={id}
                onClick={() => goTo(idx)}
                onTouchStart={onAITabTouchStart}
                onTouchEnd={onAITabTouchEnd}
                onTouchMove={onAITabTouchEnd}
                onContextMenu={e => e.preventDefault()}
                className="flex-1 flex items-center justify-center py-2.5 select-none"
              >
                <span className={cn(
                  "flex items-center justify-center w-11 h-11 rounded-full transition-all",
                  voiceActive
                    ? "bg-red-500/20 animate-[xray-breathe_4s_ease-in-out_infinite]"
                    : voiceConnecting
                    ? "bg-primary/20 animate-pulse"
                    : "animate-[xray-shimmer_12s_linear_infinite] [background-image:linear-gradient(90deg,#6d56c1,#1e7a9e,#1f8a60,#a03070,#6d56c1)] [background-size:300%_auto]",
                  isActive ? "opacity-100" : "opacity-70 active:opacity-90",
                )}>
                  <Icon
                    size={22}
                    strokeWidth={2.2}
                    style={{ color: voiceActive ? "#f87171" : voiceConnecting ? "#a78bfa" : "#fff" }}
                  />
                </span>
              </button>
            )
          }

          return (
            <button
              key={id}
              onClick={() => goTo(idx)}
              onContextMenu={e => e.preventDefault()}
              className={cn(
                "flex-1 flex flex-col items-center justify-center gap-0.5 py-2.5 transition-opacity select-none",
                isActive ? "opacity-100" : "opacity-40 active:opacity-60"
              )}
            >
              <Icon
                size={20}
                style={{ color: isActive ? color : undefined }}
                strokeWidth={isActive ? 2.5 : 1.5}
              />
              <span
                className="text-[9px] font-semibold tracking-wider uppercase"
                style={{ color: isActive ? color : undefined }}
              >
                {label}
              </span>
            </button>
          )
        })}
      </nav>

      {/* Item detail overlay — stays in-shell, no route navigation */}
      {overlay && (
        <MobileItemOverlay id={overlay.id} kind={overlay.kind} onClose={() => setOverlay(null)} />
      )}
    </div>
  )
}
