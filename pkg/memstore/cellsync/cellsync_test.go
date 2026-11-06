package cellsync

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	memoryv1 "github.com/triageagent-dev/x-ray/v2/api/v1"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore/index"
)

var now = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

func cell(mut func(*memoryv1.MemoryCell)) *memoryv1.MemoryCell {
	c := &memoryv1.MemoryCell{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
		Spec:       memoryv1.MemoryCellSpec{ID: "c1", Kind: "note", Scope: "public", Subject: "inst", Title: "t", Content: "c"},
	}
	if mut != nil {
		mut(c)
	}
	return c
}

func at(d time.Duration) *metav1.Time { t := metav1.NewTime(now.Add(d)); return &t }

func isConfig(c *memoryv1.MemoryCell) bool   { return c.Spec.Kind == "config" }
func neverEmbed(c *memoryv1.MemoryCell) bool { return c.Labels["embed"] == "none" }

func opts() Options {
	return Options{Subject: "inst", ConfigKind: isConfig, NeverEmbed: neverEmbed}
}

func TestOwnedBy(t *testing.T) {
	assert.True(t, OwnedBy(cell(nil), ""), "with no subject configured every cell is owned")
	assert.True(t, OwnedBy(cell(nil), "inst"))
	assert.False(t, OwnedBy(cell(nil), "other"), "a cell of another instance sharing the namespace is not ours")
}

func TestDecide_DeletesAnOwnedCellAtItsDeadline(t *testing.T) {
	d := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.StaleAfter = at(-time.Minute) }), now, opts())
	assert.True(t, d.Delete)
	assert.False(t, d.Index)
	assert.Zero(t, d.Requeue)

	for _, status := range []string{"stale", "superseded"} {
		d = Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Status = status }), now, opts())
		assert.True(t, d.Delete, status)
	}
}

func TestDecide_KeepRetiredLeavesARetiredCellAndStillDeletesAtTheDeadline(t *testing.T) {
	o := opts()
	o.KeepRetired = true
	for _, status := range []string{"stale", "superseded"} {
		d := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Status = status; c.Spec.Embedding = "v"; c.Status.Indexed = true }), now, o)
		assert.False(t, d.Delete, status)
		assert.False(t, d.Index, "a kept retired cell stays out of the vector index: "+status)
	}
	d := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Status = "stale"; c.Spec.StaleAfter = at(-time.Minute) }), now, o)
	assert.True(t, d.Delete, "a deadline that has passed still deletes")
}

func TestDecide_NeverDeletesAForeignOrConfigCell(t *testing.T) {
	past := func(c *memoryv1.MemoryCell) { c.Spec.StaleAfter = at(-time.Hour) }
	foreign := Decide(cell(func(c *memoryv1.MemoryCell) { past(c); c.Spec.Subject = "other" }), now, opts())
	assert.False(t, foreign.Delete, "another instance's cell is never deleted by this one")

	cfg := Decide(cell(func(c *memoryv1.MemoryCell) { past(c); c.Spec.Kind = "config" }), now, opts())
	assert.False(t, cfg.Delete, "a config cell that only looks expired is the host's source of truth")
	assert.False(t, cfg.Index, "and is never embedded")
}

func TestDecide_ArmsARequeueAtTheDeadlineOnEveryPath(t *testing.T) {
	future := func(c *memoryv1.MemoryCell) { c.Spec.StaleAfter = at(90 * time.Second) }
	paths := map[string]func(*memoryv1.MemoryCell){
		"embeddable": future,
		"private":    func(c *memoryv1.MemoryCell) { future(c); c.Spec.Scope = "private" },
		"never":      func(c *memoryv1.MemoryCell) { future(c); c.Labels = map[string]string{"embed": "none"} },
		"seeded":     func(c *memoryv1.MemoryCell) { future(c); c.Spec.Embedding = "x"; c.Status.Indexed = true },
	}
	for name, mut := range paths {
		d := Decide(cell(mut), now, opts())
		assert.Equal(t, 90*time.Second, d.Requeue, name)
		assert.False(t, d.Delete, name)
	}
	assert.Zero(t, Decide(cell(nil), now, opts()).Requeue, "no deadline, no requeue")
	assert.Zero(t, Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.StaleAfter = at(-time.Second); c.Spec.Subject = "other" }), now, opts()).Requeue, "a deadline already past arms nothing")
}

func TestDecide_IndexesEveryEmbeddableCellWhetherOrNotItWasIndexedBefore(t *testing.T) {
	assert.True(t, Decide(cell(nil), now, opts()).Index, "a cell with no embedding is embedded")
	assert.True(t, Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Embedding = "v" }), now, opts()).Index, "a persisted embedding not yet seeded is seeded")
	assert.True(t, Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Embedding = "v"; c.Status.Indexed = true }), now, opts()).Index,
		"an indexed cell is seeded again: the vector index lives in the process and a restart empties it")
	assert.False(t, Decide(cell(func(c *memoryv1.MemoryCell) { c.Labels = map[string]string{"embed": "none"} }), now, opts()).Index, "the host's never-embed cells")
}

func TestDecide_NeverIndexesAnotherInstancesCell(t *testing.T) {
	o := Options{Subject: "me"}
	for name, mut := range map[string]func(*memoryv1.MemoryCell){
		"unembedded": func(c *memoryv1.MemoryCell) { c.Spec.Subject = "other" },
		"embedded": func(c *memoryv1.MemoryCell) {
			c.Spec.Subject = "other"
			c.Spec.Embedding = "v"
			c.Status.Indexed = true
		},
	} {
		d := Decide(cell(mut), now, o)
		assert.False(t, d.Index, "another instance's cell is neither embedded nor written: "+name)
		assert.False(t, d.Delete, name)
	}
	assert.True(t, Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Subject = "me" }), now, o).Index, "this instance's own cell is")
}

func TestDecide_IndexesOnlyActiveCells(t *testing.T) {
	o := opts()
	o.KeepRetired = true
	for status, want := range map[string]bool{"": true, "active": true, "indexed": true, "draft": false, "stale": false, "superseded": false} {
		d := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Status = status }), now, o)
		assert.Equal(t, want, d.Index, "status %q", status)
	}
}

func TestDecide_IndexesACellUntilItsDeadlineAtTheGivenNow(t *testing.T) {
	before := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.StaleAfter = at(time.Hour) }), now, opts())
	assert.True(t, before.Index, "the deadline is after now: active")
	assert.Equal(t, time.Hour, before.Requeue)

	after := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.StaleAfter = at(-time.Hour) }), now, opts())
	assert.False(t, after.Index, "the deadline is before now: not indexed")
	assert.True(t, after.Delete)
}

func TestDecide_APrivateCellIsNeverEmbeddedAndLosesAStoredEmbedding(t *testing.T) {
	plain := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Scope = memstore.ScopePrivate }), now, opts())
	assert.False(t, plain.Index)
	assert.False(t, plain.ClearEmbedding, "nothing to clear")

	withVec := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Scope = memstore.ScopePrivate; c.Spec.Embedding = "v" }), now, opts())
	assert.True(t, withVec.ClearEmbedding)
	assert.False(t, withVec.Index)

	indexed := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Scope = memstore.ScopePrivate; c.Status.Indexed = true }), now, opts())
	assert.True(t, indexed.ClearEmbedding, "an indexed status on a private cell is cleared too")
}

func TestDecide_NilHooksMeanTheFeatureIsAbsent(t *testing.T) {
	d := Decide(cell(func(c *memoryv1.MemoryCell) { c.Spec.Kind = "config" }), now, Options{})
	assert.True(t, d.Index, "without a ConfigKind hook every kind is a memory kind")
}

func TestShouldSeed(t *testing.T) {
	assert.True(t, ShouldSeed(cell(nil), opts()))
	assert.False(t, ShouldSeed(cell(func(c *memoryv1.MemoryCell) { c.Spec.Subject = "other" }), opts()), "foreign cells stay out of this instance's search")
	assert.False(t, ShouldSeed(cell(func(c *memoryv1.MemoryCell) { c.Labels = map[string]string{"embed": "none"} }), opts()))
	assert.False(t, ShouldSeed(cell(func(c *memoryv1.MemoryCell) { c.Spec.Scope = "private" }), opts()), "a restart between a flip to private and the next reconcile must not seed a stale vector")
}

// ── private owner migration ──────────────────────────────────────────

func TestMigratePrivateOwner(t *testing.T) {
	legacy := cell(func(c *memoryv1.MemoryCell) {
		c.Spec.Scope = memstore.ScopePrivate
		c.Spec.Subject = "alice-hash"
		c.Spec.Tags = []string{"topic/x"}
	})
	assert.True(t, MigratePrivateOwner(legacy, "inst"))
	assert.Equal(t, "inst", legacy.Spec.Subject, "Subject becomes the instance identity")
	assert.Equal(t, []string{"topic/x", "user:alice-hash"}, legacy.Spec.Tags, "the old subject becomes the owner tag")

	assert.False(t, MigratePrivateOwner(legacy, "inst"), "idempotent: a second pass changes nothing")

	owned := cell(func(c *memoryv1.MemoryCell) {
		c.Spec.Scope = memstore.ScopePrivate
		c.Spec.Subject = "inst"
		c.Spec.Tags = []string{"user:bob-hash"}
	})
	assert.False(t, MigratePrivateOwner(owned, "inst"))

	public := cell(func(c *memoryv1.MemoryCell) { c.Spec.Subject = "some-topic" })
	assert.False(t, MigratePrivateOwner(public, "inst"), "only private cells carry an owner")
	assert.Equal(t, "some-topic", public.Spec.Subject)

	ownerless := cell(func(c *memoryv1.MemoryCell) { c.Spec.Scope = memstore.ScopePrivate; c.Spec.Subject = "" })
	assert.False(t, MigratePrivateOwner(ownerless, "inst"), "no legacy subject, no owner to record: nobody can read it, as before")

	// With no instance identity configured the cell keeps an empty Subject.
	bare := cell(func(c *memoryv1.MemoryCell) { c.Spec.Scope = memstore.ScopePrivate; c.Spec.Subject = "carol-hash" })
	assert.True(t, MigratePrivateOwner(bare, ""))
	assert.Equal(t, "", bare.Spec.Subject)
	assert.Equal(t, memstore.VisibleToUser(memstore.Item{MemoryCellSpec: bare.Spec}, "carol-hash"), true)
	assert.Equal(t, memstore.VisibleToUser(memstore.Item{MemoryCellSpec: bare.Spec}, "alice-hash"), false)
}

// ── listing and forgetting ───────────────────────────────────────────

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, memoryv1.AddToScheme(s))
	return s
}

func TestListByKind_ReturnsThisInstancesCellsOfOneKind(t *testing.T) {
	mk := func(name, kind, subject string) *memoryv1.MemoryCell {
		c := cell(func(c *memoryv1.MemoryCell) {
			c.Name = name
			c.Spec.ID, c.Spec.Kind, c.Spec.Subject = name, kind, subject
			c.Labels = map[string]string{KindLabel: kind}
		})
		return c
	}
	other := mk("elsewhere", "counter", "x")
	other.Namespace = "other-ns"
	cl := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		mk("a", "counter", "inst"), mk("b", "counter", "inst"), mk("c", "counter", "foreign"), mk("d", "pattern", "inst"), other,
	).Build()

	got, err := ListByKind(context.Background(), cl, "ns", "counter", "inst")
	require.NoError(t, err)
	var ids []string
	for _, it := range got {
		ids = append(ids, it.ID)
	}
	assert.ElementsMatch(t, []string{"a", "b"}, ids, "the kind, the namespace and the instance's own cells")

	all, err := ListByKind(context.Background(), cl, "ns", "counter", "")
	require.NoError(t, err)
	assert.Len(t, all, 3, "no subject means every instance's")
}

func TestForget_RemovesAnItemFromTheStoreItsIndexesAndTheVectorIndex(t *testing.T) {
	store := memstore.NewStore()
	it, msg := store.Upsert(memstore.Item{MemoryCellSpec: memoryv1.MemoryCellSpec{ID: "gone", Kind: "note", Scope: "public", Title: "needle", Content: "text", Status: "active"}})
	require.Empty(t, msg)
	cells := index.NewCellStore(nil, zerolog.Nop())
	require.True(t, cells.SeedFromEmbedding(index.IndexEntry{ID: it.ID}, []float32{1, 0, 0, 0}))
	require.EqualValues(t, 1, cells.CollectionSize())

	Forget(store, cells, "gone", "note")
	assert.False(t, store.Indexed("gone"))
	assert.Zero(t, cells.CollectionSize())
	_, ok := store.Get("gone")
	assert.True(t, ok, "Expire keeps the stale record; it is just out of every index")
	got, _ := store.Get("gone")
	assert.Equal(t, "stale", got.Status)

	Forget(store, nil, "gone", "note") // no vector index: the store side still runs
	Forget(nil, nil, "x", "note")      // nothing to forget in
}
