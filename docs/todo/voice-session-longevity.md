# TODO: Voice Session Longevity (GoAway, Resumption, Context Compression)

**Status:** Open — ADR-039 (Accepted); Phases 1 and 2 done 2026-11-09 (see ADR-039 "As built: capabilities 1 and 2")

Implements [ADR-039](../adr/039-voice-session-longevity.md). Three independent,
flag-gated capabilities so Gemini Live voice sessions survive the ~15-min audio
cap and ~10-min connection recycle instead of dropping silently.

All SDK fields referenced below are confirmed present in
`google.golang.org/genai@v1.47.0`:
`LiveServerMessage.GoAway` (`LiveServerGoAway.TimeLeft`, types.go:6055/6059),
`LiveServerMessage.SessionResumptionUpdate` (`.NewHandle`, types.go:6100/6102),
`LiveConnectConfig.SessionResumption` (`SessionResumptionConfig{Handle, Transparent}`, types.go:6424/6193),
`LiveConnectConfig.ContextWindowCompression` (`ContextWindowCompressionConfig{TriggerTokens, SlidingWindow{TargetTokens}}`, types.go:6434/6216),
`LiveConnectConfig.EnableAffectiveDialog` (*bool, types.go:6412),
`LiveConnectConfig.Proactivity` (`ProactivityConfig{ProactiveAudio *bool}`, types.go:6438/6229).

## Problem

`handleGeminiMessage` (`internal/xray/voice.go:681`) never inspects
`msg.GoAway` or `msg.SessionResumptionUpdate`, and the `Live.Connect` config
(`voice.go:479`) sets neither resumption nor compression. A conversation past
~15 min (audio cap) or ~10 min (connection lifetime) drops abruptly:
`readFromGemini` (`voice.go:671`) gets a receive error and the session tears
down — no browser warning, no recovery, no trace event.

## Plan (single commit per the project-workflow checklist)

### Phase 1 — GoAway awareness (always on, near-free) — Done (log + span event; no metric, no `SessionEnding` message)
1. **Handle `msg.GoAway`** in `handleGeminiMessage`: warn-log with
   `TimeLeft`; emit span event + metric `voice.goaway{time_left_ms}`.
2. **Browser notification:** add `SessionEnding *struct{Reason string; TimeLeftMs int64}`
   to `VoiceServerMessage`; write it on GoAway so the UI can show "reconnecting…".
   - Test: `TestVoiceSession_GoAwayNotifiesBrowser`,
     `TestVoiceSession_GoAwayRecordsMetric`.

### Phase 2 — Session resumption (flag `--voice-session-resumption`, default false) — Done
> The delicate phase. Teardown is trigger-on-first-error (`run`, `voice.go:587`):
> any non-nil `errCh` value runs `cancel()` + `live.Close()` + end. So reconnect
> must happen *inside* the read loop without surfacing the error, and must
> serialize the `s.live` swap against concurrent sends. See ADR-039 "Reconnect
> mechanics" + "Risks specific to reconnect".
3. **Extract `buildLiveConfig()`** from the `Live.Connect` literal (`voice.go:479`)
   so both the initial connect and `reconnect()` share it. Conditionally set
   `SessionResumption: &genai.SessionResumptionConfig{Handle: s.resumptionHandle}`
   when the resolved flag/CR is true.
4. **Store handle:** add `resumptionHandle string` on `VoiceSession`, guarded by
   the existing `liveMu`; update on each `msg.SessionResumptionUpdate.NewHandle`.
5. **Reconnect (`reconnect(ctx)`):** under `s.liveMu` — `s.live.Close()`,
   `Live.Connect` via `buildLiveConfig()`, assign `s.live`. Fail closed after one
   attempt (no retry storm).
6. **Read-loop guard (`readFromGemini`, `voice.go:663`):** on `Receive` error, if
   `canResume()` then `reconnect()` + `continue`; else return the error (today's
   path). **Proactive trigger:** on `msg.GoAway`, reconnect before the socket
   closes — prefer reconnecting at imminent `TurnComplete`, not mid-audio.
7. **Send-side guard (`readFromBrowser`, `voice.go:602`):** a `Send*` error while
   resumable must be recoverable (drop frame / retry after reconnect), not
   returned — symmetric to the read guard, or it trips teardown.
   - Test: `TestVoiceSession_StoresResumptionHandle`,
     `TestVoiceSession_ReconnectUsesStoredHandle`,
     `TestVoiceSession_ReconnectFailsClosedAfterOneAttempt`,
     `TestVoiceSession_RecoverableReceiveErrorDoesNotTearDown`,
     `TestVoiceSession_TokenCountersSurviveReconnect`,
     `TestVoiceManager_ResumptionCRLOverridesFlag`.

### Phase 3 — Context window compression (flags `--voice-context-trigger-tokens`, `--voice-context-target-tokens`, default 0)
8. **Set** `ContextWindowCompression` with `TriggerTokens` / `SlidingWindow.TargetTokens`
   in `buildLiveConfig()` when trigger > 0. Validate trigger ∈ [5_000, 128_000]
   at startup; target < trigger.
   - Test: `TestVoiceManager_ContextCompressionDisabledWhenZero`,
     `TestVoiceManager_ContextCompressionValidatesTriggerRange`,
     `TestConfigWatcher_ParsesContextTokens`.

### Phase 4 — Conversation quality (flags `--voice-affective-dialog`, `--voice-proactive-audio`, default false) — *native-audio only*
> Persona behavior, not plumbing. Both are single `*bool`s on the same config
> literal; cheap once Phases 1–3 exist. Native-audio only — guard on `liveModel`.
> Warrants a listen-test before enabling. See ADR-039 capabilities 4 & 5.
A. **Affective dialog:** in `buildLiveConfig()`, set
   `EnableAffectiveDialog = ptr(true)` when enabled **and**
   `isNativeAudioModel(s.liveModel)`; else skip-with-warn.
B. **Proactive audio:** set `Proactivity = &genai.ProactivityConfig{ProactiveAudio: ptr(true)}`
   under the same model guard. Orthogonal to VAD (`voice.go:490`).
   - Test: `TestVoiceManager_AffectiveDialogDisabledByDefault`,
     `TestVoiceManager_AffectiveDialogSkippedOnNonNativeModel`,
     `TestVoiceManager_ProactiveAudioConfiguredWhenEnabled`,
     `TestVoiceManager_QualityFlagsCROverridesFlag`.

### Cross-cutting wiring (all phases)
9. `api/v1alpha1/memorycell_types.go`: `SessionResumption *bool`,
   `ContextTriggerTokens int`, `ContextTargetTokens int`, `AffectiveDialog *bool`,
   `ProactiveAudio *bool` on `VoiceSessionConfig`.
10. `internal/xray/configwatcher.go`: parse the five CR fields.
11. `cmd/xray/main.go`: five flags → `NewVoiceManager` (same call site as
    `--headroom-content-limit`).
12. `internal/xray/voice.go` (`VoiceManager`/`ServeHTTP`): hold defaults,
    resolve CR-wins per session (mirror `maxUserDailyTokens` / `maxItemContentChars`);
    extend `newVoiceSession` signature (currently ends `headroomLimit, logger`).
13. `charts/xray-xray/`: `voice.sessionResumption`,
    `voice.contextTriggerTokens`, `voice.contextTargetTokens`,
    `voice.affectiveDialog`, `voice.proactiveAudio` + deployment flags
    + CR template fields.
14. UI (`ui/`): handle `SessionEnding` (toast / reconnecting indicator). Optional —
    resumption works without it.

### Close-out (project-workflow)
- `make test && make lint` clean.
- CHANGELOG entry (bold lead per phase).
- ADR-039 header → Implemented + commit; this file → Done + commit.
- `docs/adr/README.md` + `docs/todo/README.md` rows: status, commit, tests.

## Notes / decisions
- **Out of scope:** asynchronous (`NON_BLOCKING`) tool calls. X-Ray's
  first-party tools are sub-30 ms in-process; blocking dispatch is fine. Revisit
  only if slow remote MCP tools (`buildRemoteMCPTools`) become a primary path.
- **Cost:** longevity does not bypass spend control — ADR-031 per-user daily
  token quota still caps it; compaction lowers per-turn context cost on long runs.
- **Backward compatible:** all knobs default off/zero → behavior identical to
  today except the added GoAway log + browser notify (purely additive).
- Phase 1 is independently shippable and worthwhile on its own (makes a silent
  failure observable); Phases 2–3 can land in the same commit or follow.
