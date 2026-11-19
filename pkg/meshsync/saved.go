package meshsync

import (
	"cmp"
	"slices"
	"time"
)

// Saved is one row of the totals a replica has applied from one sender, as it
// is persisted next to the merged ledger. The ledger's persisted totals hold
// the fleet's counts up to the moment they were written; Saved says, sender by
// sender, how much of each sender's running total those counts contain. A
// replica that restores both knows exactly what a peer's next total adds, so
// it needs no guess about what is "already counted".
type Saved struct {
	Origin string  `json:"o"`
	Family string  `json:"f"`
	Tenant string  `json:"t,omitempty"`
	Name   string  `json:"n,omitempty"`
	Start  int64   `json:"s,omitempty"`
	Epoch  int64   `json:"e"`
	Vals   []int64 `json:"v"`
}

type savedKey struct {
	origin string
	row    rowKey
}

func (s Saved) key() savedKey {
	return savedKey{s.Origin, rowKey{s.Family, s.Tenant, s.Name, s.Start}}
}

// Save returns what this replica has applied from each peer, and its own
// running totals under the name self, for the rows that can still arrive: every
// lifetime row and the bucketed rows younger than horizon (the longest a sender
// keeps resending a bucket).
func (y *Sync) Save(self string, now time.Time, horizon time.Duration) []Saved {
	y.mu.Lock()
	defer y.mu.Unlock()
	cutoff := now.Add(-horizon).Unix()
	var out []Saved
	add := func(origin string, epoch int64, m map[rowKey][]int64) {
		for k, v := range m {
			if k.start == 0 || k.start >= cutoff {
				out = append(out, Saved{Origin: origin, Family: k.family, Tenant: k.tenant, Name: k.name, Start: k.start, Epoch: epoch, Vals: append([]int64(nil), v...)})
			}
		}
	}
	add(self, y.epoch, y.own)
	for origin, p := range y.peers {
		add(origin, p.epoch, p.totals)
	}
	return out
}

// Load takes the sender totals a persisted ledger was written with, once, at
// start, before the restored totals are seeded. From then on a total from a
// sender is measured against what the persisted ledger holds of it, however
// late it arrives, and a sender the persisted ledger does not know is counted
// in full. A sender already heard from live keeps its live state, and the rows
// saved under self (the replica's own earlier run) are ignored.
func (y *Sync) Load(saved []Saved, self string, now time.Time) {
	y.mu.Lock()
	defer y.mu.Unlock()
	y.exact = true
	if y.peers == nil {
		y.peers = make(map[string]*peer)
	}
	live := make(map[string]bool, len(y.peers))
	for origin := range y.peers {
		live[origin] = true
	}
	newest := make(map[string]int64)
	for _, s := range saved {
		newest[s.Origin] = max(newest[s.Origin], s.Epoch)
	}
	loaded := make(map[string]*peer)
	for _, s := range saved {
		if s.Origin == "" || s.Origin == self || live[s.Origin] {
			continue
		}
		p := loaded[s.Origin]
		if p == nil {
			p = &peer{epoch: newest[s.Origin], fromZero: newest[s.Origin] > y.epoch, totals: make(map[rowKey][]int64), lastSeen: now}
			loaded[s.Origin] = p
			y.peers[s.Origin] = p
		}
		if p.epoch != s.Epoch {
			continue
		}
		p.totals[rowKey{s.Family, s.Tenant, s.Name, s.Start}] = append([]int64(nil), s.Vals...)
	}
}

// MergeSaved unions two saved sets, dropping the bucketed rows older than
// horizon. For a sender and row the newer epoch wins outright (the older run
// of that sender is over), and the same epoch takes the larger total of each
// field, as the ledgers' own persisted totals are merged. The result is sorted.
func MergeSaved(a, b []Saved, now time.Time, horizon time.Duration) []Saved {
	cutoff := now.Add(-horizon).Unix()
	byKey := make(map[savedKey]Saved, len(a)+len(b))
	for _, set := range [][]Saved{a, b} {
		for _, s := range set {
			if s.Start != 0 && s.Start < cutoff {
				continue
			}
			k := s.key()
			cur, ok := byKey[k]
			switch {
			case !ok || s.Epoch > cur.Epoch:
				s.Vals = append([]int64(nil), s.Vals...)
				byKey[k] = s
			case s.Epoch == cur.Epoch:
				if len(s.Vals) > len(cur.Vals) {
					cur.Vals = append(cur.Vals, make([]int64, len(s.Vals)-len(cur.Vals))...)
				}
				for i, v := range s.Vals {
					cur.Vals[i] = max(cur.Vals[i], v)
				}
				byKey[k] = cur
			}
		}
	}
	out := make([]Saved, 0, len(byKey))
	for _, s := range byKey {
		out = append(out, s)
	}
	slices.SortFunc(out, func(x, y Saved) int {
		return cmp.Or(cmp.Compare(x.Origin, y.Origin), cmp.Compare(x.Family, y.Family), cmp.Compare(x.Tenant, y.Tenant), cmp.Compare(x.Name, y.Name), cmp.Compare(x.Start, y.Start))
	})
	return out
}
