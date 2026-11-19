package xray

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
	"github.com/triageagent-dev/x-ray/v2/pkg/meshsync"
)

// Telemetry is what the server counts and persists about itself, apart from the
// memory it serves: search and voice counters, each user's daily voice-token
// spend, tool latencies, and the debounced writer that keeps them, the live
// sessions and the registered OAuth clients in the system_configuration cell.
type Telemetry struct {
	logger   zerolog.Logger
	counters searchCounters

	toolMu    sync.Mutex
	toolStats map[string]*toolCallStat

	usageMu sync.Mutex
	usage   map[string]*userDaily // userHash → current-UTC-day token spend

	// statsDirty is set by markStatsDirty whenever a counter, session, or usage
	// row changes, and cleared by the background flusher after it writes the CR.
	// This decouples the hot request path (search, voice, login) from the etcd
	// write: callers flip a flag instead of doing a Get+Status().Update each, so
	// N concurrent searches collapse to at most one CR write per flush tick
	// instead of N. See ADR-047 (debounced stats flush).
	statsDirty atomic.Bool

	mu        sync.RWMutex
	reader    client.Reader // non-nil when controller-runtime manager is available
	writer    client.Client // non-nil when controller-runtime manager is available
	namespace string

	// fleet is set when this replica shares its counters with the others (ADR-068);
	// nil when it counts alone.
	fleet *fleetLedger
	// leader says whether this replica writes the stats to the cell; nil means the
	// only replica. Followers keep their counts, which the leader hears over the
	// mesh, and leave the write to it.
	leader func() bool
	// seeded is set once the persisted counters have been taken back. With a fleet
	// they are taken once: a later echo of the cell holds totals that already
	// include what the peers sent, and applying it again would count them twice.
	seeded atomic.Bool

	// count and embedMetrics are read when the stats are flushed: how many
	// memory cells there are, and the embedder's call and cache-hit counters.
	count        func() int
	embedMetrics func() (calls, cacheHits int64)
}

func newTelemetry(logger zerolog.Logger) *Telemetry {
	return &Telemetry{
		logger:    logger,
		toolStats: make(map[string]*toolCallStat),
		usage:     make(map[string]*userDaily),
		count:     func() int { return 0 },
	}
}

// setBackend wires the cluster client the stats flusher reads and writes with.
func (s *Telemetry) setBackend(r client.Reader, ns string) {
	s.mu.Lock()
	s.reader = r
	s.namespace = ns
	s.mu.Unlock()
}

func (s *Telemetry) setWriter(c client.Client) {
	s.mu.Lock()
	s.writer = c
	s.mu.Unlock()
}

// SearchMetrics tracks search operation counters since process start.
type SearchMetrics struct {
	TotalSearches     int64 `json:"totalSearches"`
	KeywordHits       int64 `json:"keywordHits"`
	KWVoiceHits       int64 `json:"kwVoiceHits"`
	KWVoiceMisses     int64 `json:"kwVoiceMisses"`
	KWTrigramHits     int64 `json:"kwTrigramHits"`
	SemanticCalls     int64 `json:"semanticCalls"`
	SemanticFallbacks int64 `json:"semanticFallbacks"`
}

// fleetCounter is a count of the whole service. local is what this replica
// counted itself, and fleet adds what the other replicas counted, so a read
// answers for every replica and only local is sent to them.
type fleetCounter struct{ local, fleet atomic.Int64 }

func (c *fleetCounter) Add(n int64)   { c.local.Add(n); c.fleet.Add(n) }
func (c *fleetCounter) Load() int64   { return c.fleet.Load() }
func (c *fleetCounter) Store(n int64) { c.fleet.Store(n) }

type searchCounters struct {
	total         fleetCounter
	kwHits        fleetCounter
	kwVoiceHits   fleetCounter
	kwVoiceMisses fleetCounter
	kwTrigramHits fleetCounter
	semCalls      fleetCounter
	semFallbacks  fleetCounter
	voiceTotal    fleetCounter
	voiceActive   atomic.Int64 // sessions open on this replica: a level, not a count
}

// toolCallStat tracks invocation count and cumulative duration for one tool.
type toolCallStat struct {
	calls   int64
	totalNs int64
}

// userDaily tracks one authenticated user's Gemini Live token spend for a UTC day.
type userDaily struct {
	day    string // UTC calendar day, "YYYY-MM-DD"
	tokens int64
}

// utcDay returns the current UTC calendar day as "YYYY-MM-DD".
func utcDay() string { return time.Now().UTC().Format("2006-01-02") }

// RestoreStats loads persisted counters from a RuntimeStats snapshot (e.g. read by ConfigWatcher at startup).
// voiceActive is intentionally not restored — it is session-local and always starts at 0.
func (s *Telemetry) RestoreStats(st *mcell.RuntimeStats) {
	if st == nil {
		return
	}
	// With a fleet the counters and the token spend are taken back once, at start:
	// what a later echo of the cell holds already includes what the peers sent.
	if s.fleet == nil || !s.seeded.Swap(true) {
		s.restoreCounters(st)
	}
	s.restoreShared(st)
}

// restoreCounters takes the persisted counters and daily token spend back.
func (s *Telemetry) restoreCounters(st *mcell.RuntimeStats) {
	s.counters.total.Store(st.SearchTotal)
	s.counters.kwHits.Store(st.KeywordHits)
	s.counters.kwVoiceHits.Store(st.KWVoiceHits)
	s.counters.kwVoiceMisses.Store(st.KWVoiceMisses)
	s.counters.kwTrigramHits.Store(st.KWTrigramHits)
	s.counters.semCalls.Store(st.SemanticCalls)
	s.counters.semFallbacks.Store(st.SemanticFallbacks)
	s.counters.voiceTotal.Store(st.VoiceTotal)

	// Rehydrate per-user daily token spend for the current UTC day. This runs on
	// every system_configuration watch event, not just at startup — and flushStats
	// writes that CR, so each flush echoes back here. We must NOT clobber the live
	// in-memory map: a stale or self-echoed CR snapshot would otherwise lower a
	// counter the running session has already advanced (observed as the HUD
	// dropping mid-count then recovering). Merge by max so restore only ever fills
	// in or raises a user's count, never lowers it; a genuine restart starts from
	// an empty map and rehydrates fully.
	today := utcDay()
	s.usageMu.Lock()
	for _, ud := range st.UserDaily {
		if ud.UserHash == "" || ud.Day != today {
			continue
		}
		if u, ok := s.usage[ud.UserHash]; ok && u.day == today {
			if ud.Tokens > u.tokens {
				u.tokens = ud.Tokens
			}
			continue
		}
		s.usage[ud.UserHash] = &userDaily{day: ud.Day, tokens: ud.Tokens}
	}
	s.usageMu.Unlock()
}

// restoreShared takes the sessions and the OAuth client registrations back; it
// runs on every echo of the cell, and never lowers or replaces what is held.
func (s *Telemetry) restoreShared(st *mcell.RuntimeStats) {
	// Rehydrate persisted sessions so logins survive a restart and propagate across
	// replicas via this same watch event. restoreSessions merges in (never clobbers
	// a live entry) and prunes expired rows, mirroring the UserDaily merge above.
	if r := sessions.restoreSessions(st.Sessions); r.added > 0 {
		s.logger.Info().Int("sessions", r.added).Int("rows", len(st.Sessions)).Msg("restored persisted sessions from CR")
	} else if r.undecryptable > 0 {
		// Only a genuine decrypt failure is worth a warning — a key mismatch means
		// these sessions can never restore (e.g. rows written under a different
		// OAuth-secret-derived key). added==0 with only alreadyHeld rows is the
		// normal self-echo from our own flush and is silent.
		s.logger.Warn().Int("undecryptable", r.undecryptable).Int("rows", len(st.Sessions)).Msg("CR session rows could not be decrypted (key mismatch) — those logins will require re-auth")
	}

	// Rehydrate MCP OAuth client registrations (ADR-049) so a client registered
	// before a restart (e.g. Claude Code) stays known — otherwise /authorize rejects
	// its cached client_id and forces a re-register.
	if n := oauthClients.restore(st.OAuthClients); n > 0 {
		s.logger.Info().Int("clients", n).Msg("restored persisted MCP OAuth clients from CR")
	}
}

// AddUserTokens accumulates Gemini Live token spend for an authenticated user on
// the current UTC day. Empty userHash (unauthenticated/dev sessions) is ignored —
// there is no stable key to meter. Rolls the counter over when the UTC day changes.
func (s *Telemetry) AddUserTokens(userHash string, n int64) {
	if userHash == "" || n <= 0 {
		return
	}
	s.addTokens(userHash, n)
	if f := s.fleet; f != nil {
		f.publish([]meshsync.Record{f.sync.Own(meshsync.Record{Family: famSpend, Name: userHash, Start: dayStartOf(time.Now()), Vals: []int64{n}})})
	}
}

// addTokens adds n to the person's count of today, whoever spent them.
func (s *Telemetry) addTokens(userHash string, n int64) {
	today := utcDay()
	s.usageMu.Lock()
	u, ok := s.usage[userHash]
	if !ok {
		u = &userDaily{day: today}
		s.usage[userHash] = u
	}
	if u.day != today {
		u.day = today
		u.tokens = 0
	}
	u.tokens += n
	s.usageMu.Unlock()
}

// UserTokensToday returns the user's token spend for the current UTC day, or 0 if
// the stored counter is from a previous day (not yet rolled over) or the user is
// unknown.
func (s *Telemetry) UserTokensToday(userHash string) int64 {
	if userHash == "" {
		return 0
	}
	today := utcDay()
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	u, ok := s.usage[userHash]
	if !ok || u.day != today {
		return 0
	}
	return u.tokens
}

// snapshotUserDaily returns the per-user token rows for the current UTC day for
// persistence to CR status, pruning rows from past days from the in-memory map so
// it stays bounded by active users. Returns nil when there is nothing to persist.
func (s *Telemetry) snapshotUserDaily() []mcell.UserDailyTokens {
	today := utcDay()
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	var rows []mcell.UserDailyTokens
	for hash, u := range s.usage {
		if u.day != today {
			delete(s.usage, hash)
			continue
		}
		rows = append(rows, mcell.UserDailyTokens{UserHash: hash, Day: u.day, Tokens: u.tokens})
	}
	return rows
}

// TrackToolCall records a tool invocation and its execution duration.
func (s *Telemetry) TrackToolCall(name string, d time.Duration) {
	s.toolMu.Lock()
	st, ok := s.toolStats[name]
	if !ok {
		st = &toolCallStat{}
		s.toolStats[name] = st
	}
	st.calls++
	st.totalNs += d.Nanoseconds()
	s.toolMu.Unlock()
}

// ToolMetrics returns per-tool call counts and average latency in milliseconds.
func (s *Telemetry) ToolMetrics() map[string]any {
	s.toolMu.Lock()
	out := make(map[string]any, len(s.toolStats))
	for name, st := range s.toolStats {
		avgUs := float64(0)
		if st.calls > 0 {
			avgUs = float64(st.totalNs) / float64(st.calls) / 1e3
		}
		out[name] = map[string]any{
			"calls":  st.calls,
			"avg_us": avgUs,
		}
	}
	s.toolMu.Unlock()
	return out
}

// TrackKWTrigramHit records that the trigram index contributed at least one candidate to a search result.
func (s *Telemetry) TrackKWTrigramHit() { s.counters.kwTrigramHits.Add(1) }

// TrackKWVoiceHit records that the keyword-first path in the voice search_memory tool
// returned enough results to skip semantic search.
func (s *Telemetry) TrackKWVoiceHit() { s.counters.kwVoiceHits.Add(1) }

// TrackKWVoiceMiss records that keyword-first returned too few results and fell through
// to semantic search.
func (s *Telemetry) TrackKWVoiceMiss() { s.counters.kwVoiceMisses.Add(1) }

// Metrics returns a snapshot of search operation counters.
func (s *Telemetry) Metrics() SearchMetrics {
	return SearchMetrics{
		TotalSearches:     s.counters.total.Load(),
		KeywordHits:       s.counters.kwHits.Load(),
		KWVoiceHits:       s.counters.kwVoiceHits.Load(),
		KWVoiceMisses:     s.counters.kwVoiceMisses.Load(),
		KWTrigramHits:     s.counters.kwTrigramHits.Load(),
		SemanticCalls:     s.counters.semCalls.Load(),
		SemanticFallbacks: s.counters.semFallbacks.Load(),
	}
}

// VoiceMetrics returns a snapshot of voice session counters.
func (s *Telemetry) VoiceMetrics() map[string]any {
	return map[string]any{
		"totalCalls":  s.counters.voiceTotal.Load(),
		"activeCalls": s.counters.voiceActive.Load(),
	}
}

// TrackVoiceCall records a new voice session start and marks stats dirty.
func (s *Telemetry) TrackVoiceCall() {
	s.counters.voiceTotal.Add(1)
	s.counters.voiceActive.Add(1)
	s.markStatsDirty()
}

// TrackVoiceEnd records a voice session end and marks stats dirty.
func (s *Telemetry) TrackVoiceEnd() {
	s.counters.voiceActive.Add(-1)
	s.markStatsDirty()
}

// markStatsDirty marks the persisted stats/session snapshot as stale without
// touching the apiserver. The background flusher (StartStatsFlusher) coalesces
// these into at most one CR write per tick. Cheap and non-blocking — safe to call
// on the hot path (every search, voice event, login/logout).
func (s *Telemetry) markStatsDirty() { s.statsDirty.Store(true) }

// FlushStats marks stats dirty so the next flush tick persists them. Exported so
// the session store can request a flush on login/logout (wired via
// SetSessionPersistence). It no longer writes synchronously — see ADR-047
// (debounced stats flush). The change lands on the next StartStatsFlusher tick.
func (s *Telemetry) FlushStats() { s.markStatsDirty() }

// StartStatsFlusher runs the single CR-status writer: every `every` it writes the
// current counters + sessions to the system_configuration CR, but only when
// something changed since the last write (markStatsDirty). This replaces the old
// per-request synchronous flush, so search/voice/login throughput is no longer
// gated on an etcd round-trip and N concurrent requests cause at most one write
// per tick. On ctx cancellation it does a final flush so the last state is durable
// across a graceful shutdown. Call once at startup after SetWriter. A zero or
// negative interval disables the loop (in-memory only).
func (s *Telemetry) StartStatsFlusher(ctx context.Context, every time.Duration) {
	if every <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				// Final flush so a login/voice/search in the last window survives.
				if s.leads() && s.statsDirty.Swap(false) {
					s.flushStats()
				}
				return
			case <-t.C:
				// Only write when something changed — idle pods make no etcd writes —
				// and only the leader writes: the others' counts, sessions and clients
				// reach it over the mesh (ADR-068).
				if s.leads() && s.statsDirty.Swap(false) {
					s.flushStats()
				}
			}
		}
	}()
}

// flushStats writes runtime counters to the status of the system_configuration CR.
func (s *Telemetry) flushStats() {
	s.mu.RLock()
	w := s.writer
	ns := s.namespace
	r := s.reader
	s.mu.RUnlock()
	if w == nil || r == nil {
		return
	}

	var embedCalls, embedCacheHits int64
	if s.embedMetrics != nil {
		embedCalls, embedCacheHits = s.embedMetrics()
	}

	stats := &mcell.RuntimeStats{
		MemoryCells:       int64(s.count()),
		VoiceTotal:        s.counters.voiceTotal.Load(),
		VoiceActive:       s.counters.voiceActive.Load(),
		SearchTotal:       s.counters.total.Load(),
		KeywordHits:       s.counters.kwHits.Load(),
		KWVoiceHits:       s.counters.kwVoiceHits.Load(),
		KWVoiceMisses:     s.counters.kwVoiceMisses.Load(),
		KWTrigramHits:     s.counters.kwTrigramHits.Load(),
		SemanticCalls:     s.counters.semCalls.Load(),
		SemanticFallbacks: s.counters.semFallbacks.Load(),
		EmbedCalls:        embedCalls,
		EmbedCacheHits:    embedCacheHits,
		UpdatedAt:         time.Now().UTC().Format(time.RFC3339),
		UserDaily:         s.snapshotUserDaily(),
		Sessions:          sessions.snapshotSessions(),
		OAuthClients:      oauthClients.snapshot(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cell := &mcell.MemoryCell{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: ns, Name: SystemConfigCRName}, cell); err != nil {
		return
	}
	cell = cell.DeepCopy()
	if s.fleet != nil {
		// A leader that has just taken over holds counts from before the one it
		// follows wrote its last: never write less than the cell has.
		mergeWithPersisted(stats, cell.Status.Stats)
	}
	cell.Status.Stats = stats
	if err := w.Status().Update(ctx, cell); err != nil {
		s.logger.Warn().Err(err).Msg("flushStats: status update failed")
		return
	}
	if totals := mergeSavedTotals(cell.Spec.Data[meshTotalsKey].Bytes, s.meshTotals(time.Now()), time.Now()); totals != "" && totals != cell.Spec.Data[meshTotalsKey].Bytes {
		if cell.Spec.Data == nil {
			cell.Spec.Data = map[string]mcell.DataEntry{}
		}
		cell.Spec.Data[meshTotalsKey] = mcell.DataEntry{Bytes: totals, Mime: "application/json"}
		if err := w.Update(ctx, cell); err != nil && !apierrors.IsConflict(err) {
			s.logger.Warn().Err(err).Msg("flushStats: mesh totals update failed")
		}
	}
}

// mergeWithPersisted raises each counter and each person's spend of today in out
// to what prev holds, when prev holds more.
func mergeWithPersisted(out, prev *mcell.RuntimeStats) {
	if prev == nil {
		return
	}
	out.VoiceTotal = max(out.VoiceTotal, prev.VoiceTotal)
	out.SearchTotal = max(out.SearchTotal, prev.SearchTotal)
	out.KeywordHits = max(out.KeywordHits, prev.KeywordHits)
	out.KWVoiceHits = max(out.KWVoiceHits, prev.KWVoiceHits)
	out.KWVoiceMisses = max(out.KWVoiceMisses, prev.KWVoiceMisses)
	out.KWTrigramHits = max(out.KWTrigramHits, prev.KWTrigramHits)
	out.SemanticCalls = max(out.SemanticCalls, prev.SemanticCalls)
	out.SemanticFallbacks = max(out.SemanticFallbacks, prev.SemanticFallbacks)
	today := utcDay()
	byUser := make(map[string]int, len(out.UserDaily))
	for i, u := range out.UserDaily {
		byUser[u.UserHash] = i
	}
	for _, u := range prev.UserDaily {
		if u.Day != today || u.UserHash == "" {
			continue
		}
		if i, ok := byUser[u.UserHash]; ok {
			out.UserDaily[i].Tokens = max(out.UserDaily[i].Tokens, u.Tokens)
		} else {
			out.UserDaily = append(out.UserDaily, u)
		}
	}
}
