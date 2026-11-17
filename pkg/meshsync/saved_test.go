package meshsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// persisted is what a replica that wrote the ledger last saw: peer "p" (epoch
// pe) had counted 100 of the row, and the writer "w" itself 7.
func persisted(pe, we int64) []Saved {
	return []Saved{
		{Origin: "p", Family: "f", Name: "n", Epoch: pe, Vals: []int64{100}},
		{Origin: "w", Family: "f", Name: "n", Epoch: we, Vals: []int64{7}},
	}
}

// A replica that restores the sender totals measures a peer exactly: it counts
// what the peer added since the ledger was written, however late the first
// contact is, where the baseline window counted all of it (after the window) or
// none of it (inside the window).
func TestSync_LoadedSenderTotalsMakeTheFirstContactExact(t *testing.T) {
	older := time.Now().Add(-time.Hour).UnixNano()
	late := time.Now().Add(BaselineWindow + time.Minute)

	b := New()
	b.Load(persisted(older, older), "b", time.Now())
	assert.Equal(t, []int64{30}, b.Take(rec("p", older, 130), false, late), "late contact: only what the peer added since the save")
	assert.Equal(t, []int64{3}, b.Take(rec("w", older, 10), false, late), "the writer's own total is measured too")

	b = New()
	b.Load(persisted(older, older), "b", time.Now())
	assert.Equal(t, []int64{30}, b.Take(rec("p", older, 130), false, time.Now()), "contact inside the window: what the peer counted since the save is not lost")
	assert.Nil(t, b.Take(rec("p", older, 130), false, time.Now()), "a repeated total adds nothing")
}

func TestSync_LoadedSenderTotals_UnknownSendersAreCountedInFull(t *testing.T) {
	older := time.Now().Add(-time.Hour).UnixNano()
	b := New()
	b.Load(persisted(older, older), "b", time.Now())

	assert.Equal(t, []int64{9}, b.Take(rec("q", older, 9), false, time.Now()), "a sender the saved ledger never heard from")
	assert.Equal(t, []int64{4}, b.Take(Record{Family: "f", Name: "other", Epoch: older, Origin: "p", Vals: []int64{4}}, false, time.Now()), "a row of a known sender that was not saved")
	assert.Equal(t, []int64{5}, b.Take(rec("p", older+1, 5), false, time.Now()), "a restarted peer counts from zero")
}

func TestSync_LoadKeepsWhatWasHeardLiveAndSkipsItsOwnOldRun(t *testing.T) {
	older := time.Now().Add(-time.Hour).UnixNano()
	b := New()
	assert.Nil(t, b.Take(rec("p", older, 120), false, time.Now()), "inside the window, before the load: taken as counted")
	b.Load(persisted(older, older), "w", time.Now())
	assert.Equal(t, []int64{5}, b.Take(rec("p", older, 125), false, time.Now()), "the live total stays the base")
	assert.NotContains(t, b.peers, "w", "rows saved under the replica's own name are its earlier run")
}

// Without Load the window still applies, so a ledger written by an older build
// keeps working.
func TestSync_WithoutLoadTheWindowStillApplies(t *testing.T) {
	older := time.Now().Add(-time.Hour).UnixNano()
	b := New()
	assert.Nil(t, b.Take(rec("p", older, 100), false, time.Now()))
}

func TestSync_SaveWhatWasApplied(t *testing.T) {
	older := time.Now().Add(-time.Hour).UnixNano()
	now := time.Now()
	b := New()
	b.Own(Record{Family: "f", Name: "n", Vals: []int64{7}})
	b.Own(Record{Family: "f", Name: "old", Start: now.Add(-5 * time.Hour).Unix(), Vals: []int64{1}})
	b.Take(rec("p", older, 100), true, now)

	got := b.Save("b", now, 2*time.Hour)
	require.Len(t, got, 2, "the old bucket is beyond the horizon")
	by := map[string]Saved{}
	for _, s := range got {
		by[s.Origin] = s
	}
	assert.Equal(t, []int64{7}, by["b"].Vals)
	assert.Equal(t, b.epoch, by["b"].Epoch)
	assert.Equal(t, []int64{100}, by["p"].Vals)
	assert.Equal(t, older, by["p"].Epoch)
}

func TestMergeSaved(t *testing.T) {
	now := time.Now()
	a := []Saved{
		{Origin: "p", Family: "f", Name: "n", Epoch: 5, Vals: []int64{100, 4}},
		{Origin: "q", Family: "f", Name: "n", Epoch: 5, Vals: []int64{1}},
		{Origin: "p", Family: "f", Name: "b", Start: now.Add(-9 * time.Hour).Unix(), Epoch: 5, Vals: []int64{1}},
	}
	b := []Saved{
		{Origin: "p", Family: "f", Name: "n", Epoch: 5, Vals: []int64{90, 9}},
		{Origin: "q", Family: "f", Name: "n", Epoch: 6, Vals: []int64{2}},
	}
	got := MergeSaved(a, b, now, 2*time.Hour)
	require.Len(t, got, 2, "the old bucket is dropped")
	assert.Equal(t, Saved{Origin: "p", Family: "f", Name: "n", Epoch: 5, Vals: []int64{100, 9}}, got[0], "the same epoch takes the larger of each field")
	assert.Equal(t, Saved{Origin: "q", Family: "f", Name: "n", Epoch: 6, Vals: []int64{2}}, got[1], "the newer epoch wins outright")
	assert.Equal(t, []int64{100, 4}, a[0].Vals, "the inputs are not changed")
}
