// Package meshsync keeps the numbers a service counts converging across its
// replicas. Every ledger (counters, hourly usage, daily spend) uses it the same
// way: a replica adds what it counts to its own running
// totals, sends the totals with its epoch, and a receiver applies only what a
// total adds over the last total it saw from that sender and epoch. A lost
// update is made good by the next one, a late one adds nothing, and every
// replica periodically resends its totals so a counter that went quiet
// converges too.
package meshsync

import (
	"sync"
	"time"
)

// BaselineWindow is how long after a replica starts that the first totals it
// hears from a peer which was already running are taken as already counted. A
// replica restores its ledgers from the persisted blobs, which hold the
// fleet's merged totals, so applying those totals again would count them
// twice. Two resends fit in the window. It is a guess, used only when the
// persisted ledger carries no sender totals (Load was not called): a peer first
// heard after the window is counted twice, and what a peer counted before the
// first contact is lost.
const BaselineWindow = 75 * time.Second

// ForgetWindow is how long after a replica forgets a tenant that it ignores the
// rows peers still send for it. Every replica forgets a purged tenant when it
// learns of the purge, and until then a peer that has not yet resends totals
// the purge made void; applied, they would bring the tenant back.
const ForgetWindow = 2 * time.Minute

// ResendInterval is how often a replica resends its own totals.
const ResendInterval = 30 * time.Second

// EpochSilence is how long a sender's epoch may go unheard before a record of
// an older epoch is taken for a new start. Within it a lower epoch is a late
// frame of the sender's previous life; past it, a sender that is still sending
// has restarted with a clock that went back, and would otherwise be ignored for
// as long as this replica runs. Three resends missed in a row is not a sender
// that is up.
const EpochSilence = 3 * ResendInterval

// Record is one row of one ledger as it crosses the mesh: the sender's own
// running totals for it, one per field of the row. Family names the ledger;
// Tenant (a grouping key, empty when a ledger has none), Name and Start key the
// row inside it (Start is the unix time of the
// bucket or day, zero for a lifetime row). Epoch names the sender's Sync, so
// a sender that restarted starts afresh.
type Record struct {
	Family string  `json:"f"`
	Tenant string  `json:"t,omitempty"`
	Name   string  `json:"n,omitempty"`
	Start  int64   `json:"s,omitempty"`
	Epoch  int64   `json:"e"`
	Vals   []int64 `json:"v"`
	// Origin is the sending replica, set on receipt from the frame; never on
	// the wire.
	Origin string `json:"-"`
}

type rowKey struct {
	family, tenant, name string
	start                int64
}

func (r Record) key() rowKey { return rowKey{r.Family, r.Tenant, r.Name, r.Start} }

// peer is what has been applied from one sender's Sync instance.
type peer struct {
	epoch    int64
	fromZero bool // the peer started after this replica: everything it counted is new here
	totals   map[rowKey][]int64
	lastSeen time.Time // dropped when stale: pods get new names on every rollout
}

// Sync is one replica's side of the exchange. Safe for concurrent use.
type Sync struct {
	epoch int64
	// exact is set by Load: the persisted ledger came with the sender totals it
	// holds, so a sender's total is measured against those and the baseline
	// window below is not used.
	exact bool

	mu        sync.Mutex
	own       map[rowKey][]int64
	peers     map[string]*peer
	forgotten map[tenantKey]time.Time // when each tenant's rows were dropped, see Forget
}

type tenantKey struct{ family, tenant string }

// New starts a Sync; its creation time names it on the mesh.
func New() *Sync { return &Sync{epoch: time.Now().UnixNano()} }

// Started is when this Sync was created.
func (y *Sync) Started() time.Time { return time.Unix(0, y.epoch) }

// Own adds r.Vals to this replica's own totals for r's row and returns the
// record to send, carrying the totals.
func (y *Sync) Own(r Record) Record {
	y.mu.Lock()
	defer y.mu.Unlock()
	if y.own == nil {
		y.own = make(map[rowKey][]int64)
	}
	k := r.key()
	tot := grown(y.own[k], len(r.Vals))
	y.own[k] = tot
	for i, v := range r.Vals {
		tot[i] += v
	}
	return Record{Family: r.Family, Tenant: r.Tenant, Name: r.Name, Start: r.Start, Epoch: y.epoch, Vals: append([]int64(nil), tot...)}
}

// grown returns tot with room for n values: a row keeps its shape across
// builds, but a build that adds a value to a family must not index past the
// shorter slice a peer or an earlier own row stored.
func grown(tot []int64, n int) []int64 {
	if len(tot) >= n {
		return tot
	}
	return append(tot, make([]int64, n-len(tot))...)
}

// Forget drops the rows of one grouping key (tenant) of one ledger family: this
// replica's own totals, which it would resend for ever, and what it has taken
// from each peer. For ForgetWindow it then ignores the rows peers send for it.
func (y *Sync) Forget(family, tenant string, now time.Time) {
	y.mu.Lock()
	defer y.mu.Unlock()
	drop := func(m map[rowKey][]int64) {
		for k := range m {
			if k.family == family && k.tenant == tenant {
				delete(m, k)
			}
		}
	}
	drop(y.own)
	for _, p := range y.peers {
		drop(p.totals)
	}
	if y.forgotten == nil {
		y.forgotten = make(map[tenantKey]time.Time)
	}
	y.forgotten[tenantKey{family, tenant}] = now
}

// Take returns what r adds, field by field, over the last totals seen from the
// same sender and epoch, and records r; nil when it adds nothing, as for a
// record of an epoch older than the one stored for a sender that is still
// heard (EpochSilence). A row first
// seen inside the baseline window from a peer that was running before this
// replica is taken as already counted, unless fresh says the row belongs to a
// bucket or day younger than this replica.
func (y *Sync) Take(r Record, fresh bool, now time.Time) []int64 {
	y.mu.Lock()
	defer y.mu.Unlock()
	if at, ok := y.forgotten[tenantKey{r.Family, r.Tenant}]; ok {
		if now.Sub(at) < ForgetWindow {
			return nil
		}
		delete(y.forgotten, tenantKey{r.Family, r.Tenant})
	}
	if y.peers == nil {
		y.peers = make(map[string]*peer)
	}
	p := y.peers[r.Origin]
	if p != nil && r.Epoch < p.epoch && now.Sub(p.lastSeen) < EpochSilence {
		// A late frame of a sender's previous life: its totals were superseded
		// by the restart, and applying it would reset the peer and count the
		// new epoch's totals again.
		return nil
	}
	if p == nil || p.epoch != r.Epoch {
		p = &peer{epoch: r.Epoch, fromZero: r.Epoch > y.epoch, totals: make(map[rowKey][]int64)}
		y.peers[r.Origin] = p
	}
	p.lastSeen = now
	k := r.key()
	prev, seen := p.totals[k]
	prev = grown(prev, len(r.Vals))
	p.totals[k] = prev
	diff := make([]int64, len(r.Vals))
	grew := false
	for i, v := range r.Vals {
		if v > prev[i] {
			diff[i] = v - prev[i]
			prev[i] = v
			grew = true
		}
	}
	if !grew || (!y.exact && !seen && !p.fromZero && !fresh && now.Before(y.Started().Add(BaselineWindow))) {
		return nil
	}
	return diff
}

// State returns this replica's own rows to resend: every lifetime row and the
// bucketed rows younger than resend. It also drops the bucketed rows older
// than keep, its own and its peers', and peers not heard from for as long.
func (y *Sync) State(now time.Time, keep, resend time.Duration) []Record {
	y.mu.Lock()
	defer y.mu.Unlock()
	cutoff, recent := now.Add(-keep).Unix(), now.Add(-resend).Unix()
	prune := func(m map[rowKey][]int64) {
		for k := range m {
			if k.start != 0 && k.start < cutoff {
				delete(m, k)
			}
		}
	}
	prune(y.own)
	for origin, p := range y.peers {
		if p.lastSeen.Unix() < cutoff {
			delete(y.peers, origin)
			continue
		}
		prune(p.totals)
	}
	out := make([]Record, 0, len(y.own))
	for k, v := range y.own {
		if k.start == 0 || k.start >= recent {
			out = append(out, Record{Family: k.family, Tenant: k.tenant, Name: k.name, Start: k.start, Epoch: y.epoch, Vals: append([]int64(nil), v...)})
		}
	}
	return out
}
