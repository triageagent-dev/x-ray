package meshsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pair is a sender and a receiver whose Sync started an hour earlier, so the
// sender counts as started after the receiver.
func pair() (a, b *Sync) {
	b = New()
	b.epoch = time.Now().Add(-time.Hour).UnixNano()
	return New(), b
}

func rec(origin string, epoch int64, vals ...int64) Record {
	return Record{Family: "f", Name: "n", Epoch: epoch, Origin: origin, Vals: vals}
}

func TestSync_ALostRecordIsMadeGoodByTheNext(t *testing.T) {
	a, b := pair()
	var sent []Record
	for i := 0; i < 3; i++ {
		sent = append(sent, a.Own(Record{Family: "f", Name: "n", Vals: []int64{1, 10}}))
	}
	for _, i := range []int{0, 2} { // the second is lost
		sent[i].Origin = "a"
		got := b.Take(sent[i], false, time.Now())
		require.NotNil(t, got)
	}
	// 1+10 first, then the total 3,30 adds 2,20: the lost record is counted.
	sent[1].Origin = "a"
	assert.Nil(t, b.Take(sent[1], false, time.Now()), "a late record adds nothing")
	assert.Nil(t, b.Take(sent[2], false, time.Now()), "a repeated record adds nothing")
}

// A family that gains a value between builds: the peer's and the own row keep
// the values they had and take the new one.
func TestSync_ARowThatGrewKeepsItsValuesAndTakesTheNewOne(t *testing.T) {
	a, b := pair()
	a.Own(Record{Family: "f", Name: "n", Vals: []int64{1}})
	sent := a.Own(Record{Family: "f", Name: "n", Vals: []int64{1, 10}})
	assert.Equal(t, []int64{2, 10}, sent.Vals)

	short := rec("a", sent.Epoch, 1)
	require.NotNil(t, b.Take(short, false, time.Now()))
	got := b.Take(rec("a", sent.Epoch, 2, 10), false, time.Now())
	assert.Equal(t, []int64{1, 10}, got)
	assert.Nil(t, b.Take(rec("a", sent.Epoch, 2), false, time.Now()), "a shorter row adds nothing")
}

func TestSync_TheFirstTotalsOfAPeerOlderThanUsAreAlreadyCounted(t *testing.T) {
	b := New()
	older := time.Now().Add(-time.Hour).UnixNano()
	assert.Nil(t, b.Take(rec("a", older, 40), false, time.Now()), "a total from before this replica started is in the restored blob")
	assert.Equal(t, []int64{3}, b.Take(rec("a", older, 43), false, time.Now()), "what it adds afterwards is new")

	newer := time.Now().Add(time.Second).UnixNano()
	assert.Equal(t, []int64{5}, b.Take(rec("c", newer, 5), false, time.Now()), "a peer that started after this replica counted only new work")

	assert.Equal(t, []int64{7}, b.Take(rec("d", older, 7), true, time.Now()), "a row younger than this replica is new whatever the peer's age")
}

func TestSync_APeerRestartStartsItsTotalsAfresh(t *testing.T) {
	_, b := pair()
	first := time.Now().UnixNano()
	assert.Equal(t, []int64{10}, b.Take(rec("a", first, 10), false, time.Now()))
	assert.Equal(t, []int64{2}, b.Take(rec("a", first+1, 2), false, time.Now()), "the restarted peer counts from zero again")
}

func TestSync_StateResendsRecentRowsAndDropsOldOnesAndSilentPeers(t *testing.T) {
	a, b := pair()
	now := time.Now()
	a.Own(Record{Family: "f", Name: "life", Vals: []int64{1}})
	a.Own(Record{Family: "f", Name: "old", Start: now.Add(-3 * time.Hour).Unix(), Vals: []int64{1}})
	a.Own(Record{Family: "f", Name: "new", Start: now.Add(-time.Hour).Unix(), Vals: []int64{1}})

	names := func(rs []Record) (out []string) {
		for _, r := range rs {
			out = append(out, r.Name)
		}
		return out
	}
	assert.ElementsMatch(t, []string{"life", "new"}, names(a.State(now, 24*time.Hour, 2*time.Hour)), "an old bucket is kept but not resent")
	assert.Len(t, a.own, 3)
	assert.ElementsMatch(t, []string{"life"}, names(a.State(now.Add(25*time.Hour), 24*time.Hour, 2*time.Hour)))
	assert.Len(t, a.own, 1, "buckets older than keep are dropped")

	b.Take(rec("gone", time.Now().UnixNano(), 1), false, now)
	b.State(now, 24*time.Hour, 2*time.Hour)
	assert.Contains(t, b.peers, "gone")
	b.State(now.Add(25*time.Hour), 24*time.Hour, 2*time.Hour)
	assert.NotContains(t, b.peers, "gone", "a peer silent for a day is dropped")
}

// Forget drops a tenant's own rows and the totals taken from peers, ignores
// what peers still send for it for ForgetWindow, and takes the tenant's rows
// again after that.
func TestForget_DropsOwnRowsAndIgnoresPeersForAWindow(t *testing.T) {
	a := New()
	now := time.Now()
	a.Own(Record{Family: "f", Tenant: "gone", Name: "n", Vals: []int64{4}})
	a.Own(Record{Family: "f", Tenant: "kept", Name: "n", Vals: []int64{2}})
	peer := Record{Family: "f", Tenant: "gone", Name: "n", Epoch: a.epoch + 1, Vals: []int64{9}, Origin: "p"}
	require.NotNil(t, a.Take(peer, true, now))

	a.Forget("f", "gone", now)
	for _, r := range a.State(now, time.Hour, time.Hour) {
		assert.NotEqual(t, "gone", r.Tenant)
	}
	require.Len(t, a.State(now, time.Hour, time.Hour), 1, "another tenant's row is kept")

	peer.Vals = []int64{10}
	assert.Nil(t, a.Take(peer, true, now.Add(time.Minute)), "a peer's row inside the window is ignored")
	peer.Vals = []int64{11}
	assert.Equal(t, []int64{11}, a.Take(peer, true, now.Add(ForgetWindow+time.Second)), "after the window the tenant is counted again, from zero")
}

// A frame of a sender's previous epoch that arrives after the new epoch's first
// one adds nothing and leaves the stored totals alone, so the next frame of the
// new epoch is not counted again.
func TestSync_ALateFrameOfAnOlderEpochIsIgnored(t *testing.T) {
	b := New()
	older := time.Now().Add(-2 * time.Hour).UnixNano()
	newer := time.Now().Add(time.Second).UnixNano()
	require.Equal(t, []int64{5}, b.Take(rec("a", newer, 5), false, time.Now()))

	assert.Nil(t, b.Take(rec("a", older, 40), false, time.Now()), "a record of the previous epoch adds nothing")
	assert.Equal(t, []int64{2}, b.Take(rec("a", newer, 7), false, time.Now()), "the new epoch's totals were not reset")
}

// A sender that restarted with a clock that went back sends a lower epoch than
// the one stored. While the stored epoch is still heard that is a late frame
// and is ignored; once it has been silent for EpochSilence, the lower epoch is
// the sender's new start and its totals count.
func TestSync_ALowerEpochFromASilentSenderIsANewStart(t *testing.T) {
	b := New()
	now := time.Now()
	stored := now.Add(time.Hour).UnixNano()
	restarted := now.UnixNano()
	require.Equal(t, []int64{5}, b.Take(rec("a", stored, 5), false, now))

	late := now.Add(EpochSilence - time.Second)
	assert.Nil(t, b.Take(rec("a", restarted, 3), false, late), "inside the silence window a lower epoch is a late frame")

	back := now.Add(EpochSilence + time.Second)
	assert.Equal(t, []int64{3}, b.Take(rec("a", restarted, 3), false, back), "a sender silent that long and now sending a lower epoch has restarted")
	assert.Equal(t, []int64{2}, b.Take(rec("a", restarted, 5), false, back.Add(time.Second)), "its totals go on from the new epoch")
}
