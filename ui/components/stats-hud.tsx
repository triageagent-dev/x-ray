"use client"

import { useMetrics } from "@/hooks/use-memory"
import { useLiveQuota, type LiveQuota } from "@/lib/quota-store"
import { cn } from "@/lib/utils"

type QuotaLike = { used?: number; limit: number; remaining?: number }

// pickFreshestQuota returns whichever quota reports the higher cumulative `used`.
// Both sources track the same per-day counter; the larger is the more current
// (the polled value lags the live push by up to the poll interval).
function pickFreshestQuota(live: LiveQuota | null, polled?: QuotaLike): QuotaLike | undefined {
  if (!live) return polled
  if (!polled) return live
  if (live.limit === 0 || polled.limit === 0) return live // unlimited — used is moot
  return (live.used ?? 0) >= (polled.used ?? 0) ? live : polled
}

export function StatsHud({ className }: { className?: string }) {
  const m = useMetrics(30_000)
  const liveQuota = useLiveQuota()
  if (!m) return null

  // The live per-turn push (over the voice WS) and the 30s polled metrics read
  // the same cumulative daily counter. The polled value lags by up to 30s, so
  // right after a session ends it can briefly report a stale (lower) number.
  // Take the larger of the two so the count only ever moves forward within a
  // day; on a fresh page load there is no live value and the poll wins.
  const quota = pickFreshestQuota(liveQuota, m.quota)

  const rows: [string, string | number][] = [
    ["memory", m.items],
  ]
  if (m.voice) {
    rows.push(["voice sessions", m.voice.totalCalls])
    if (m.voice.activeCalls > 0) rows.push(["voice active", m.voice.activeCalls])
  }
  if (quota) {
    // The daily cap meters AUDIO tokens (ADR-042) — audio-in + audio-out, the
    // billable cost driver — so label it as such; the in/out-audio rows below show
    // the components.
    rows.push(["audio quota", quota.limit === 0
      ? "unlimited"
      : `${(quota.used ?? 0).toLocaleString()} / ${quota.limit.toLocaleString()}`])
  }
  // Per-modality session breakdown — only the live push carries it. Audio is the
  // dominant cost driver, so show in/out audio vs text separately when present.
  const bd = liveQuota?.breakdown
  if (bd) {
    if (bd.inputAudio > 0) rows.push(["in audio", bd.inputAudio.toLocaleString()])
    if (bd.inputText > 0) rows.push(["in text", bd.inputText.toLocaleString()])
    if (bd.outputAudio > 0) rows.push(["out audio", bd.outputAudio.toLocaleString()])
    if (bd.outputText > 0) rows.push(["out text", bd.outputText.toLocaleString()])
    // Always show cached (even 0) — it's the provider-side prompt-cache monitor.
    // A persistent 0 means implicit caching isn't discounting the stable prefix;
    // hiding it would make "off" indistinguishable from "not reported".
    rows.push(["cached", bd.cached.toLocaleString()])
  }
  if (m.search) {
    rows.push(["searches", m.search.totalSearches])
    if (m.search.keywordHits > 0) rows.push(["kw hits", m.search.keywordHits])
    if (m.search.kwTrigramHits > 0) rows.push(["trigram hits", m.search.kwTrigramHits])
    if (m.search.kwVoiceHits > 0) rows.push(["kw voice hits", m.search.kwVoiceHits])
    if (m.search.kwVoiceMisses > 0) rows.push(["kw voice miss", m.search.kwVoiceMisses])
    if (m.search.semanticCalls > 0) rows.push(["semantic", m.search.semanticCalls])
    if (m.search.semanticFallbacks > 0) rows.push(["sem fallbacks", m.search.semanticFallbacks])
  }
  if (m.embed) {
    if (m.embed.embedCalls > 0) rows.push(["embeddings", m.embed.embedCalls])
    if (m.embed.cacheHits > 0) rows.push(["embed cached", m.embed.cacheHits])
  }
  if (m.semanticCache && m.semanticCache.hits > 0) rows.push(["sem cache hits", m.semanticCache.hits.toLocaleString()])
  if (m.rag) {
    if (m.rag.retrievals > 0) rows.push(["rag retrievals", m.rag.retrievals.toLocaleString()])
    if (m.rag.hits > 0) rows.push(["rag hits", m.rag.hits.toLocaleString()])
  }
  if (m.classifier) {
    const c = m.classifier
    if (c.signal > 0) rows.push(["signal", c.signal.toLocaleString()])
    if (c.noise > 0) rows.push(["noise", c.noise.toLocaleString()])
    if (c.incident > 0) rows.push(["incident", c.incident.toLocaleString()])
    if (c.unknown > 0) rows.push(["unclassified", c.unknown.toLocaleString()])
    if (c.embeddingCalls > 0) rows.push(["classifier embeds", c.embeddingCalls.toLocaleString()])
  }
  if (m.guardrail) {
    if (m.guardrail.noiseDropped > 0) rows.push(["noise dropped", m.guardrail.noiseDropped.toLocaleString()])
    if (m.guardrail.infraPassed > 0) rows.push(["infra passed", m.guardrail.infraPassed.toLocaleString()])
  }
  if (m.llm) {
    if (m.llm.calls > 0) rows.push(["llm calls", m.llm.calls.toLocaleString()])
    if (m.llm.agentCalls > 0) rows.push(["agent calls", m.llm.agentCalls.toLocaleString()])
    if (m.llm.promptTokens > 0) rows.push(["prompt tokens", m.llm.promptTokens.toLocaleString()])
    if (m.llm.completionTokens > 0) rows.push(["completion tokens", m.llm.completionTokens.toLocaleString()])
  }
  // Instance-wide daily LLM budget — distinct from the per-user audio quota
  // above, which meters this caller's voice spend only.
  if (m.llmBudget && m.llmBudget.limit > 0) {
    rows.push(["llm budget", `${m.llmBudget.tokensToday.toLocaleString()} / ${m.llmBudget.limit.toLocaleString()}`])
  }
  if (m.tools) {
    for (const [name, t] of Object.entries(m.tools)) {
      if (t.calls > 0) rows.push([name, `${t.calls}× ${Math.round(t.avg_us)}µs`])
    }
  }

  return (
    <div className={cn(
      "pointer-events-none select-none tabular-nums",
      // The size grows with the viewport, from 11 px on a laptop to 20 px on a
      // large monitor (about 12 px at 1440 px wide, 16 px at 2560 px); a fixed
      // 10 px was too small to read on a big screen.
      "text-[clamp(11px,0.4vw+6px,20px)] leading-[1.6] text-right font-mono",
      "text-foreground/20",
      className
    )}>
      {rows.map(([label, val]) => (
        <div key={label}>
          <span className="text-foreground/10">{label} </span>
          <span>{val}</span>
        </div>
      ))}
    </div>
  )
}
