# Todo

| File | Title | Status | Tests |
|------|-------|--------|-------|
| [async-a2a-voice.md](async-a2a-voice.md) | Async A2A in Voice Sessions | Todo | — |
| [floating-panel-followup.md](floating-panel-followup.md) | Floating Panel Follow-up | Todo — ADR-025 | — |
| [github-oauth.md](github-oauth.md) | GitHub OAuth (org-restricted) | Todo — ADR-022 | — |
| [051-mcp-general-agentic-memory.md](051-mcp-general-agentic-memory.md) | MCP Tools as General Agentic Memory (align with voice tools, hard result caps, multi-tenant subject scope) | Proposed — ADR-051 | — |
| [memory-search-and-controller.md](memory-search-and-controller.md) | Memory Search Optimisation + Proper Controller | Todo | — |
| [voice-session-evals.md](voice-session-evals.md) | Voice Session Evals (Phoenix + Vertex AI) | Todo | — |
| [voice-session-longevity.md](voice-session-longevity.md) | Voice Session Longevity & Quality (GoAway, Resumption, Compression, Affective Dialog, Proactive Audio) | Open — ADR-039; Phases 1, 2 done | `TestVoiceSession_GoAwayResumesWithHeldHandle`, `TestVoiceSession_ReceiveErrorResumesWithHeldHandle`, `TestVoiceSession_ResumeAttemptsAreBounded`, `TestVoiceSession_TeardownSendsCloseFrame` (full list in ADR-039), `TestVoiceSession_GoAwayWhileUserSpeaksResumesAfterTurnWithoutLoss`, `TestVoiceSession_GoAwayAfterLongSilenceResumesAtOnceWithoutReplay`, `TestVoiceSession_GoAwayAfterInterruptionResumesAtOnce`, `TestVoiceSession_GoAwayAfterTranscribedSpeechWaitsForTheTurn`, `TestVoiceSession_DropMidUtteranceReplaysAfterConsumedIndex`, `TestVoiceSession_ReplayBufferTrimmedByConsumedIndex`, `TestVoiceSession_ReplayBufferIsBounded`, `TestVoiceSession_ReplaySkipsLeadingSilenceAndKeepsThePace`, `TestIsQuiet`, `TestVoiceSession_PacedSendStopsOnCancel` |
