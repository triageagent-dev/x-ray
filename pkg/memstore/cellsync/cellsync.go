// Package cellsync holds the decisions a controller makes when it reconciles a
// MemoryCell into a memory store: whether the cell is deleted at its deadline,
// whether it is embedded, seeded or left out of the vector index, when to look
// at it again. They are functions of the cell, so the host's controller, which
// has its own config kinds and clients, calls them instead of carrying the
// logic. The package keeps no state and writes nothing.
package cellsync

import (
	"context"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	memoryv1 "github.com/triageagent-dev/x-ray/v2/api/v1"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore/index"
)

// KindLabel is the label every managed cell carries with its kind, so a cell of
// one kind can be listed without decoding the rest.
const KindLabel = "xray.triageagent.dev/kind"

// Options are the host's seams. Every field may be nil.
type Options struct {
	// Subject is the identity of this instance. With one set, only cells whose
	// Spec.Subject matches are owned; cells of another instance sharing the
	// namespace are neither ingested nor deleted. With none set, every cell is
	// owned.
	Subject string
	// ConfigKind reports cells that configure the host rather than hold memory.
	// They are never embedded and never deleted at a deadline: a config cell that
	// only looks expired may be the host's only source of its configuration.
	ConfigKind func(cell *memoryv1.MemoryCell) bool
	// NeverEmbed reports cells that stay durable and listable without entering
	// the vector index (a label, a coordination record).
	NeverEmbed func(cell *memoryv1.MemoryCell) bool
	// KeepRetired keeps a cell whose status is stale or superseded instead of
	// deleting it: a host that treats retirement as an audit trail (a refuted cell
	// with the record of why) leaves the cell and drops it from the indexes
	// only. A cell past its deadline is deleted either way.
	KeepRetired bool
}

// Decision is what to do with one cell.
type Decision struct {
	// Delete: the deadline has passed, or the cell is stale or superseded (unless
	// KeepRetired), and this instance owns it and it is not a config cell. Delete the cell and call
	// Forget; nothing else applies.
	Delete bool
	// Requeue is when to look at the cell again: the time until its deadline, or
	// zero. It is set on every path that does not delete, so a deadline is never
	// missed because the cell took an early return.
	Requeue time.Duration
	// Index: the cell belongs in the vector index. Seed it from the embedding it
	// carries when that embedding is current, or embed it
	// (index.CellStore.SeedOrEmbed). It is set for every public cell this
	// instance owns whose item is active (an empty or "indexed" status reads as
	// active, as a mirror of the cell reads it), unless it is a config or
	// never-embed cell, whether or not the cell carries an embedding or an
	// indexed status: the vector index lives in the process, so a restart or a
	// changed text needs the cell again, and seeding a cell whose embedding is
	// current writes nothing. A cell of another instance is never indexed, so
	// two controllers never write each other's embeddings; a retired or
	// otherwise inactive cell is never indexed, since a mirror drops it from the
	// vector index.
	Index bool
	// ClearEmbedding: a private cell holds an embedding or an indexed status.
	// Remove it from the vector index, clear Spec.Embedding, its
	// index.FingerprintAnnotation and Status.Indexed, so a flip back to public
	// re-embeds fresh.
	ClearEmbedding bool
}

// OwnedBy reports whether a cell belongs to the instance named subject.
func OwnedBy(cell *memoryv1.MemoryCell, subject string) bool {
	return subject == "" || cell.Spec.Subject == subject
}

// Decide says what to do with cell at now.
func Decide(cell *memoryv1.MemoryCell, now time.Time, o Options) Decision {
	isConfig := o.ConfigKind != nil && o.ConfigKind(cell)
	owned := OwnedBy(cell, o.Subject)

	deadline := cell.Spec.StaleAfter != nil && !cell.Spec.StaleAfter.IsZero()
	expired := deadline && now.After(cell.Spec.StaleAfter.Time)
	retired := !o.KeepRetired && (cell.Spec.Status == "stale" || cell.Spec.Status == "superseded")
	if (expired || retired) && owned && !isConfig {
		return Decision{Delete: true}
	}

	d := Decision{}
	if deadline {
		if ttl := cell.Spec.StaleAfter.Time.Sub(now); ttl > 0 {
			d.Requeue = ttl
		}
	}
	if isConfig {
		d.Requeue = 0
		return d
	}
	if cell.Spec.Scope == memstore.ScopePrivate {
		d.ClearEmbedding = cell.Status.Indexed || cell.Spec.Embedding != ""
		return d
	}
	if o.NeverEmbed != nil && o.NeverEmbed(cell) {
		return d
	}
	d.Index = owned && !expired && activeStatus(cell.Spec.Status)
	return d
}

// activeStatus reports whether a cell's status is active, reading an empty
// status or the in-process status "indexed" as active, the way a host's mirror
// of the cell reads it. With "not expired", checked by Decide at its own now,
// it is memstore.Item.IsActive: a mirror drops an inactive item from the vector
// index, so seeding one would put it back on every reconcile.
func activeStatus(status string) bool {
	return status == "" || status == "active" || status == "indexed"
}

// ShouldSeed is the per-cell gate of a startup bulk seed of the vector index. It
// is a separate path from the reconcile, so the same exclusions apply: foreign
// cells stay out of this instance's search, never-embed cells stay out of the
// index, and a private cell is never seeded, which closes the window in which a
// cell that has just flipped to private still carries an embedding the next
// reconcile has not yet cleared.
func ShouldSeed(cell *memoryv1.MemoryCell, o Options) bool {
	if !OwnedBy(cell, o.Subject) || cell.Spec.Scope == memstore.ScopePrivate {
		return false
	}
	return o.NeverEmbed == nil || !o.NeverEmbed(cell)
}

// MigratePrivateOwner moves a private cell written under the older rule, where
// Subject named the owner, to the current one: the owner becomes a user: tag and
// Subject the identity of this instance. It reports whether it changed the cell;
// the caller writes it back. A second call changes nothing, a public cell is not
// touched, and a private cell with no Subject has no owner to record.
func MigratePrivateOwner(cell *memoryv1.MemoryCell, subject string) bool {
	if cell.Spec.Scope != memstore.ScopePrivate {
		return false
	}
	item := memstore.Item{MemoryCellSpec: cell.Spec}
	if memstore.PrivateOwner(item) != "" || cell.Spec.Subject == "" {
		return false
	}
	owner := cell.Spec.Subject
	cell.Spec.Tags = append(cell.Spec.Tags, memstore.PrivateOwnerTagPrefix+owner)
	cell.Spec.Subject = subject
	return true
}

// ListByKind lists the cells of one kind in namespace as items. With a subject
// it returns only that instance's cells. It is the read path for kinds a host
// never seeds into the working set, which have no other way back into the
// process after a restart.
func ListByKind(ctx context.Context, c client.Reader, namespace, kind, subject string) ([]memstore.Item, error) {
	var list memoryv1.MemoryCellList
	if err := c.List(ctx, &list, client.InNamespace(namespace), client.MatchingLabels{KindLabel: kind}); err != nil {
		return nil, err
	}
	items := make([]memstore.Item, 0, len(list.Items))
	for i := range list.Items {
		spec := list.Items[i].Spec
		if subject != "" && spec.Subject != subject {
			continue
		}
		items = append(items, memstore.Item{MemoryCellSpec: spec})
	}
	return items, nil
}

// Forget takes a cell that was deleted, by this host or by anyone else, out of
// every in-process index: the store's lexical indexes (the item is marked stale
// and leaves search) and the vector index. Either may be nil. kind is the cell's
// kind, for a host that publishes the deletion.
func Forget(store *memstore.Store, cells *index.CellStore, id, kind string) {
	_ = kind
	if strings.TrimSpace(id) == "" {
		return
	}
	if store != nil {
		store.Expire(id)
	}
	if cells != nil {
		_ = cells.Delete(context.Background(), id)
	}
}
