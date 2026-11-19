package xray

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/triageagent-dev/x-ray/v2/pkg/meshsync"
)

// The counters and each person's daily token spend are numbers of the whole
// service, not of one replica (ADR-068). Each replica counts its own work, sends
// its running totals to the others (pkg/meshsync), and adds what a total grows
// by, so a read answers for every replica and a daily token cap holds across
// them.

const (
	famSpend = "spend" // a person's token spend on one UTC day: Name is the user hash, Start the day
	famCount = "count" // a lifetime counter: Name is the counter's name

	// meshTotalsKey is the spec.data entry of the system_configuration cell that
	// holds what this replica has applied from each sender, so a restart knows what
	// the persisted totals already contain.
	meshTotalsKey = "mesh-totals"

	// spendKeep is how long a day's rows are kept and resent: the day, and a day
	// of margin for a replica whose clock is behind.
	spendKeep = 25 * time.Hour

	// counterPublishEvery is how often a replica sends what it counted since the
	// last send. A token charge is sent at once; a counter is read for display.
	counterPublishEvery = 5 * time.Second
)

// fleetLedger is one replica's side of the exchange.
type fleetLedger struct {
	self    string
	sync    *meshsync.Sync
	publish func([]meshsync.Record)

	mu   sync.Mutex
	sent map[string]int64 // counter name → this replica's own count already sent
}

func dayStartOf(t time.Time) int64 { return t.UTC().Truncate(24 * time.Hour).Unix() }

// namedCounters are the lifetime counters that are shared, by the name they go by
// on the mesh.
func (s *Telemetry) namedCounters() map[string]*fleetCounter {
	return map[string]*fleetCounter{
		"search_total":       &s.counters.total,
		"keyword_hits":       &s.counters.kwHits,
		"kw_voice_hits":      &s.counters.kwVoiceHits,
		"kw_voice_misses":    &s.counters.kwVoiceMisses,
		"kw_trigram_hits":    &s.counters.kwTrigramHits,
		"semantic_calls":     &s.counters.semCalls,
		"semantic_fallbacks": &s.counters.semFallbacks,
		"voice_total":        &s.counters.voiceTotal,
	}
}

// joinFleet makes this replica share its counters and token spend as self,
// sending its records with publish. Call before the replica serves.
func (s *Telemetry) joinFleet(self string, publish func([]meshsync.Record)) {
	s.fleet = &fleetLedger{self: self, sync: meshsync.New(), publish: publish, sent: make(map[string]int64)}
}

// SetLeader sets whether this replica writes the stats to the cell.
func (s *Telemetry) SetLeader(is func() bool) { s.leader = is }

func (s *Telemetry) leads() bool { return s.leader == nil || s.leader() }

// publishCounters sends what this replica counted since it last did.
func (s *Telemetry) publishCounters() {
	f := s.fleet
	if f == nil {
		return
	}
	var recs []meshsync.Record
	f.mu.Lock()
	for name, c := range s.namedCounters() {
		cur := c.local.Load()
		if d := cur - f.sent[name]; d > 0 {
			recs = append(recs, f.sync.Own(meshsync.Record{Family: famCount, Name: name, Vals: []int64{d}}))
			f.sent[name] = cur
		}
	}
	f.mu.Unlock()
	if len(recs) > 0 {
		f.publish(recs)
	}
}

// ledgerState is this replica's own rows, to resend: the counters, and today's
// spend.
func (s *Telemetry) ledgerState(now time.Time) []meshsync.Record {
	if s.fleet == nil {
		return nil
	}
	return s.fleet.sync.State(now, spendKeep, spendKeep)
}

// applyLedger folds the totals peers sent into this replica's counts.
func (s *Telemetry) applyLedger(recs []meshsync.Record, now time.Time) {
	f := s.fleet
	if f == nil {
		return
	}
	named := s.namedCounters()
	for _, r := range recs {
		switch r.Family {
		case famSpend:
			if r.Name == "" || r.Start != dayStartOf(now) {
				continue // another day than this replica's own: it would misstate a counter keyed by day
			}
			if d := f.sync.Take(r, r.Start > dayStartOf(f.sync.Started()), now); d != nil {
				s.addTokens(r.Name, d[0])
			}
		case famCount:
			c, ok := named[r.Name]
			if !ok {
				continue
			}
			if d := f.sync.Take(r, false, now); d != nil {
				c.fleet.Add(d[0])
			}
		}
	}
	s.markStatsDirty()
}

// meshTotals is what this replica has applied from each sender, encoded for the
// cell; "" when there is nothing to save.
func (s *Telemetry) meshTotals(now time.Time) string {
	if s.fleet == nil {
		return ""
	}
	rows := s.fleet.sync.Save(s.fleet.self, now, spendKeep)
	if len(rows) == 0 {
		return ""
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return ""
	}
	return string(raw)
}

// restoreMeshTotals takes back the sender totals the persisted counters were
// written with, once, before they are seeded. Called when the cell has been read,
// with "" when it holds none: a sender it does not know is then counted in full,
// because nothing persisted contains it. Without a read (no cell) the first total
// from a peer already running is taken as counted, within meshsync.BaselineWindow.
func (s *Telemetry) restoreMeshTotals(encoded string, now time.Time) {
	if s.fleet == nil {
		return
	}
	var rows []meshsync.Saved
	if encoded != "" {
		if err := json.Unmarshal([]byte(encoded), &rows); err != nil {
			s.logger.Warn().Err(err).Msg("mesh totals on the cell do not decode; every peer is counted in full")
			rows = nil
		}
	}
	s.fleet.sync.Load(rows, s.fleet.self, now)
}

// mergeSavedTotals unions the totals on the cell with this replica's own, so a
// leader that took over does not drop what the one before it had applied.
func mergeSavedTotals(persisted, own string, now time.Time) string {
	var a, b []meshsync.Saved
	if persisted != "" {
		_ = json.Unmarshal([]byte(persisted), &a)
	}
	if own != "" {
		_ = json.Unmarshal([]byte(own), &b)
	}
	out := meshsync.MergeSaved(a, b, now, spendKeep)
	if len(out) == 0 {
		return ""
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return ""
	}
	return string(raw)
}
