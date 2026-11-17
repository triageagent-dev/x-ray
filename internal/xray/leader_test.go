package xray

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// Running more than one replica (ADR-068): every replica mirrors every cell into
// its own store, but only the leader writes back to a cell it reads, and only the
// leader deletes at a deadline. These tests pin both halves.

func publicNote(id string) *mcell.MemoryCell {
	c := &mcell.MemoryCell{}
	c.Name = cellResourceName(id)
	c.Namespace = "ns"
	c.Spec = mcell.MemoryCellSpec{ID: id, Kind: "note", Scope: "public", Title: "T", Content: "body", Status: "active"}
	return c
}

func leaderSyncer(t *testing.T, leader func() bool, cells ...*mcell.MemoryCell) (*CellSyncer, *persistCounters, client.Client) {
	t.Helper()
	b := fake.NewClientBuilder().WithStatusSubresource(&mcell.MemoryCell{})
	for _, c := range cells {
		b = b.WithObjects(c)
	}
	tracked, counters := trackedClient(t, b)
	s := NewCellSyncer(tracked, NewMemoryStore(), zerolog.Nop())
	if leader != nil {
		s.SetLeader(leader)
	}
	return s, counters, tracked
}

func TestCellSyncer_FollowerMirrorsButDoesNotWriteBack(t *testing.T) {
	for _, tc := range []struct {
		name   string
		leader func() bool
		writes int64
	}{
		{"no election, the only replica", nil, 1},
		{"leader", func() bool { return true }, 1},
		{"follower", func() bool { return false }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cell := publicNote("n1")
			s, counters, _ := leaderSyncer(t, tc.leader, cell)

			require.NoError(t, s.SyncCell(context.Background(), cell))

			assert.NotNil(t, s.memory.GetItem("n1"), "every replica mirrors the cell into its own store")
			assert.Equal(t, tc.writes, counters.statusUpdates.Load(), "the indexed status is the leader's write")
		})
	}
}

func TestCellSyncer_FollowerLeavesTheExpiredCellForTheLeader(t *testing.T) {
	expired := func() *mcell.MemoryCell {
		c := publicNote("old")
		past := metav1.NewTime(time.Now().Add(-time.Hour))
		c.Spec.StaleAfter = &past
		return c
	}
	get := func(c client.Client) error {
		return c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: cellResourceName("old")}, &mcell.MemoryCell{})
	}

	follower, _, fc := leaderSyncer(t, func() bool { return false }, expired())
	require.NoError(t, follower.SyncCell(context.Background(), expired()))
	assert.NoError(t, get(fc), "a follower does not delete the cell")
	assert.Nil(t, follower.memory.GetItem("old"), "it drops the item from its own store")

	leader, _, lc := leaderSyncer(t, func() bool { return true }, expired())
	require.NoError(t, leader.SyncCell(context.Background(), expired()))
	assert.Error(t, get(lc), "the leader deletes the cell")
}

func TestCellSyncer_RetentionSweepRunsOnTheLeaderOnly(t *testing.T) {
	expired := func() *mcell.MemoryCell {
		c := publicNote("old")
		past := metav1.NewTime(time.Now().Add(-time.Hour))
		c.Spec.StaleAfter = &past
		return c
	}
	gone := func(c client.Client) bool {
		return c.Get(context.Background(), client.ObjectKey{Namespace: "ns", Name: cellResourceName("old")}, &mcell.MemoryCell{}) != nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	follower, _, fc := leaderSyncer(t, func() bool { return false }, expired())
	follower.StartRetentionSweep(ctx, "ns", 10*time.Millisecond)
	leader, _, lc := leaderSyncer(t, func() bool { return true }, expired())
	leader.StartRetentionSweep(ctx, "ns", 10*time.Millisecond)

	require.Eventually(t, func() bool { return gone(lc) }, 2*time.Second, 5*time.Millisecond, "the leader sweeps")
	assert.False(t, gone(fc), "a follower's sweep leaves the cell")
}

func TestEpisodicReaper_RunsOnTheLeaderOnly(t *testing.T) {
	reaped := func(leader bool) bool {
		s, _, _ := leaderSyncer(t, func() bool { return leader })
		mem := s.memory
		mem.SetEpisodicTTL(10 * time.Minute)
		id := mkEpisodic(t, mem, stableUserID("azure:user-a"), "stale", 60)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		mem.StartEpisodicReaper(ctx, s, 10*time.Millisecond)
		time.Sleep(200 * time.Millisecond)
		return mem.GetItem(id) == nil
	}
	assert.True(t, reaped(true), "the leader reaps")
	assert.False(t, reaped(false), "a follower leaves it")
}
