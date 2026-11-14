"use client"

import { useCallback, useEffect, useRef, useState } from "react"

import { setLiveQuota, type LiveQuota } from "@/lib/quota-store"
import { endpoints, wsProtocols, getApiTenant } from "@/lib/api"
import { API_MOUNT } from "@/lib/utils"

export type VoiceStatus = "idle" | "connecting" | "active" | "error"

export interface VoiceBubble {
  id: string
  role: "user" | "agent"
  text: string
}

interface ServerMessage {
  audio?: string
  transcript?: string
  inputTranscript?: string
  interrupt?: boolean
  error?: string
  done?: boolean
  quota?: LiveQuota
}

const WORKLET_CODE = `
class PcmCaptureProcessor extends AudioWorkletProcessor {
  process(inputs) {
    const ch = inputs[0]?.[0]
    if (ch) this.port.postMessage(ch)
    return true
  }
}
registerProcessor('pcm-capture', PcmCaptureProcessor)
`

const BUBBLE_TTL = 8000
let bubbleCounter = 0

/** Joins transcript chunks: a space between words, none before punctuation or after a space. */
export function joinTranscript(prev: string, chunk: string): string {
  if (!prev) return chunk
  if (/\s$/.test(prev) || /^[\s.,;:!?)\]'"”’%]/.test(chunk)) return prev + chunk
  return prev + " " + chunk
}

/** Hosts where a plaintext WebSocket never leaves the machine. */
function isLocalHost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]" || hostname === "::1"
}

export function useVoiceSession({ enabled, onDone, pinnedIds }: { enabled: boolean; onDone?: () => void; pinnedIds?: string[] }) {
  const [status, setStatus] = useState<VoiceStatus>("idle")
  const [bubbles, setBubbles] = useState<VoiceBubble[]>([])
  const [error, setError] = useState<string | null>(null)

  const setBubblesRef = useRef(setBubbles)
  setBubblesRef.current = setBubbles

  // Stable refs — never change identity, safe to use inside useEffect without deps.
  //
  // One bubble per speaker turn. The model streams its words (and the user's) as
  // small chunks; a bubble per chunk gave a column of fragments that vanished
  // before they could be read. A chunk now extends the speaker's current bubble,
  // and a change of speaker (or an interruption) starts the next one.
  const currentRef = useRef<{ id: string; role: "user" | "agent"; text: string; timer: ReturnType<typeof setTimeout> | null } | null>(null)

  const addChunk = useRef((role: "user" | "agent", chunk: string) => {
    const piece = chunk.trim()
    if (!piece) return
    let cur = currentRef.current
    if (cur && cur.role === role) {
      cur.text = joinTranscript(cur.text, piece)
      const { id, text } = cur
      setBubblesRef.current(prev => prev.map(b => (b.id === id ? { ...b, text } : b)))
    } else {
      const id = `vb-${++bubbleCounter}`
      cur = { id, role, text: piece, timer: null }
      currentRef.current = cur
      setBubblesRef.current(prev => [...prev, { id, role, text: piece }])
    }
    // The bubble leaves a while after its LAST chunk, not its first.
    if (cur.timer) clearTimeout(cur.timer)
    const id = cur.id
    cur.timer = setTimeout(() => {
      setBubblesRef.current(prev => prev.filter(b => b.id !== id))
      if (currentRef.current?.id === id) currentRef.current = null
    }, BUBBLE_TTL)
  }).current

  const endTurn = useRef(() => { currentRef.current = null }).current

  const onDoneRef = useRef(onDone)
  onDoneRef.current = onDone

  // Snapshot pinned IDs at session-start only — not a dep so mid-session
  // pin changes never tear down and restart the WebSocket.
  const pinnedIdsRef = useRef(pinnedIds)

  const wsRef = useRef<WebSocket | null>(null)
  const stopMicRef = useRef<(() => void) | null>(null)
  const captureCtxRef = useRef<AudioContext | null>(null)
  const playCtxRef = useRef<AudioContext | null>(null)
  const workletNodeRef = useRef<AudioWorkletNode | null>(null)
  const sourceRef = useRef<MediaStreamAudioSourceNode | null>(null)
  const mediaStreamRef = useRef<MediaStream | null>(null)
  const activeSrcRef = useRef<AudioBufferSourceNode[]>([])
  const nextPlayTimeRef = useRef(0)

  const flushAudio = useCallback(() => {
    activeSrcRef.current.forEach(s => { try { s.stop() } catch {} })
    activeSrcRef.current = []
    nextPlayTimeRef.current = 0
  }, [])

  useEffect(() => {
    if (!enabled) return

    setStatus("connecting")
    setError(null)
    setBubbles([])

    let ws: WebSocket
    let captureCtx: AudioContext
    let playCtx: AudioContext

    ;(async () => {
      try {
        const stream = await navigator.mediaDevices.getUserMedia({
          audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true },
        })
        mediaStreamRef.current = stream

        captureCtx = new AudioContext({ sampleRate: 16000 })
        captureCtxRef.current = captureCtx

        const blob = new Blob([WORKLET_CODE], { type: "application/javascript" })
        const blobUrl = URL.createObjectURL(blob)
        await captureCtx.audioWorklet.addModule(blobUrl)
        URL.revokeObjectURL(blobUrl)

        const secure = window.location.protocol === "https:"
        // A plain-ws upgrade puts the bearer token on the wire in cleartext —
        // and plain-http is exactly the case that USES the token rather than
        // the Secure cookie. Localhost is exempt: the traffic never leaves the
        // machine, and it is the documented development setup.
        if (!secure && !isLocalHost(window.location.hostname)) {
          throw new Error("Refusing to open an unencrypted voice connection — serve this UI over HTTPS.")
        }
        const wsUrl = new URL(`${secure ? "wss:" : "ws:"}//${window.location.host}${API_MOUNT}${endpoints.voice}`)
        // Pin the tenant explicitly. The core's binding ladder treats ?tenant=
        // as the highest-precedence pin below a meeting token, so without it a
        // multi-tenant user's session falls through to the unbound operator
        // branch instead of binding to the tenant they are looking at.
        const tenant = getApiTenant()
        if (tenant) wsUrl.searchParams.set("tenant", tenant)
        pinnedIdsRef.current = pinnedIds
        if (pinnedIdsRef.current?.length) wsUrl.searchParams.set("pinned", pinnedIdsRef.current.join(","))
        ws = new WebSocket(wsUrl.toString(), wsProtocols())
        wsRef.current = ws

        await new Promise<void>((resolve, reject) => {
          ws.onopen = () => resolve()
          ws.onerror = () => reject(new Error("WebSocket connection failed"))
          ws.onclose = () => reject(new Error("WebSocket closed before open"))
        })

        setStatus("active")

        // Keepalive ping every 20s — prevents gateway idle timeout from dropping
        // the WebSocket during quiet moments between voice turns.
        const keepalive = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ ping: true }))
        }, 20000)
        const clearKeepalive = () => clearInterval(keepalive)
        ws.addEventListener("close", clearKeepalive)
        ws.addEventListener("error", clearKeepalive)

        playCtx = new AudioContext({ sampleRate: 24000 })
        playCtxRef.current = playCtx

        const source = captureCtx.createMediaStreamSource(stream)
        sourceRef.current = source
        const worklet = new AudioWorkletNode(captureCtx, "pcm-capture")
        workletNodeRef.current = worklet

        worklet.port.onmessage = (e: MessageEvent<Float32Array>) => {
          if (ws.readyState !== WebSocket.OPEN) return
          const f32 = e.data
          const i16 = new Int16Array(f32.length)
          for (let i = 0; i < f32.length; i++) {
            i16[i] = Math.max(-32768, Math.min(32767, f32[i] * 32768))
          }
          const bytes = new Uint8Array(i16.buffer)
          let bin = ""
          for (let i = 0; i < bytes.byteLength; i++) bin += String.fromCharCode(bytes[i])
          ws.send(JSON.stringify({ audio: btoa(bin) }))
        }

        source.connect(worklet)

        // Expose mic-stop so PTT release can signal end-of-speech without
        // tearing down the session — Gemini responds, then end_session hangs up.
        stopMicRef.current = () => {
          worklet.port.onmessage = null
          try { source.disconnect(worklet) } catch {}
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ audioStreamEnd: true }))
          }
        }

        ws.onmessage = (ev) => {
          // Guarded like the SSE dispatcher in lib/api.ts: a malformed frame
          // would otherwise throw out of the handler and take the session
          // down mid-call.
          let msg: ServerMessage
          try {
            msg = JSON.parse(ev.data)
          } catch {
            return
          }
          if (msg.done) { setStatus("idle"); onDoneRef.current?.(); return }
          if (msg.error) { setError(msg.error); return }
          if (msg.interrupt) { flushAudio(); endTurn() }
          if (msg.audio) {
            const bin = atob(msg.audio)
            const bytes = new Uint8Array(bin.length)
            for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i)
            const i16 = new Int16Array(bytes.buffer)
            const f32 = new Float32Array(i16.length)
            for (let i = 0; i < i16.length; i++) f32[i] = i16[i] / 32768
            const buf = playCtx.createBuffer(1, f32.length, playCtx.sampleRate)
            buf.getChannelData(0).set(f32)
            const src = playCtx.createBufferSource()
            src.buffer = buf
            src.connect(playCtx.destination)
            activeSrcRef.current.push(src)
            const startAt = Math.max(nextPlayTimeRef.current, playCtx.currentTime)
            src.start(startAt)
            nextPlayTimeRef.current = startAt + buf.duration
            src.onended = () => { activeSrcRef.current = activeSrcRef.current.filter(s => s !== src) }
          }
          if (msg.transcript) addChunk("agent", msg.transcript)
          if (msg.inputTranscript) addChunk("user", msg.inputTranscript)
          if (msg.quota) {
            setLiveQuota(msg.quota)
            // The page-level gate only refuses to START a session. Without
            // this, a session already running keeps streaming billed audio
            // past the budget it just reported as spent. The upstream is the
            // real enforcement point; this stops paying for a call the user
            // can no longer use.
            if (msg.quota.limit > 0 && msg.quota.remaining <= 0) {
              setError("Token budget reached — ending the session.")
              try { ws.close(1000, "quota exhausted") } catch { /* already closing */ }
              setStatus("idle")
              onDoneRef.current?.()
              return
            }
          }
        }

        ws.onerror = () => setError("Connection error")
        ws.onclose = () => setStatus("idle")
      } catch (err) {
        const msg = err instanceof Error ? err.message : "Microphone access denied"
        setError(msg)
        setStatus("error")
      }
    })()

    return () => {
      // Keep the last pushed quota rather than nulling it. The polled metrics
      // refresh only every 30s, so clearing here would fall back to a stale
      // mid-session poll value for up to 30s (observed as the token count
      // dropping then recovering). The final per-turn push is the most accurate
      // number and matches what the CR persists; the next poll keeps it fresh.
      stopMicRef.current = null
      workletNodeRef.current?.disconnect()
      workletNodeRef.current = null
      sourceRef.current?.disconnect()
      sourceRef.current = null
      mediaStreamRef.current?.getTracks().forEach(t => t.stop())
      mediaStreamRef.current = null
      activeSrcRef.current.forEach(s => { try { s.stop() } catch {} })
      activeSrcRef.current = []
      nextPlayTimeRef.current = 0
      captureCtxRef.current?.close()
      captureCtxRef.current = null
      playCtxRef.current?.close()
      playCtxRef.current = null
      wsRef.current?.close()
      wsRef.current = null
    }
  }, [enabled, flushAudio])

  // Push updated pinned IDs to the server whenever they change mid-session.
  useEffect(() => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN || !pinnedIds?.length) return
    ws.send(JSON.stringify({ pinned: pinnedIds }))
  }, [pinnedIds])

  const stopMic = useCallback(() => { stopMicRef.current?.() }, [])

  return { status, bubbles, error, stopMic }
}
