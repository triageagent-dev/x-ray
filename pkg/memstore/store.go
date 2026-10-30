package memstore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	memoryv1 "github.com/triageagent-dev/x-ray/v2/api/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Locker is what the store needs of its host's lock. *sync.RWMutex satisfies it.
type Locker interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()
}

// Config is what the Store asks of its host. Every field may be nil or zero.
type Config struct {
	// Retention gives the days a knowledge item of a partition is kept (0 = no
	// deadline), for the StaleAfter stamped on a write of a knowledge kind. It
	// runs before the store lock is taken.
	Retention func(partition string) int
	// Validate returns why item may not be written, or "": the kind-specific
	// rules the host owns, on top of the store's own. It runs before the store
	// lock is taken.
	Validate func(item Item) string
	// Before amends an item about to be stored, after any merge and before the
	// write, with the store lock held (a host that links an item to what its
	// evidence names does it here).
	Before func(item *Item)
	// Read amends items about to be returned, with the lock held (a host that
	// corrects a status note against live state does it here).
	Read func(items []Item)
	// Dirty and Signal fire with the lock held after every store or delete: the
	// host rebuilds what it derives from the items and wakes its watchers.
	Dirty  func()
	Signal func()
	// Quiet reports an id whose changes are not worth publishing, such as a note
	// rewritten every few seconds. It runs after the lock is released.
	Quiet func(id string) bool
	// Published fires, outside the lock, once per published change.
	Published func()

	// Vocabulary is the host's kinds; the zero value restricts nothing.
	Vocabulary Vocabulary
	// DefaultTTL gives a kind a deadline when an item of it is created without
	// one: decay by default. A kind not listed never expires on its own. An
	// update that carries no deadline keeps the stored one; it never restamps or
	// clears it implicitly, so renewing is the writer's explicit act. With a nil
	// map the store keeps its older rule, where every write of a hypothesis or
	// unknown without a deadline is stamped afresh.
	DefaultTTL map[string]time.Duration
	// RenewOnMerge makes a restatement that merges into an existing item of a
	// DefaultTTL kind re-arm that item's deadline; without it the merged item
	// keeps the deadline it had.
	RenewOnMerge bool
	// Subject is the identity of the instance that holds these items. A private
	// item's Subject is that identity, never its owner (the owner is the user:
	// tag).
	Subject string
	// IDPrefix starts the id of an item written without one. Default "memory-".
	IDPrefix string
	// NewID, when set, supplies the id of an item written without one instead of
	// IDPrefix and a timestamp.
	NewID func() string
	// SkipIDText, when set, names the ids that are left out of an item's search
	// text: an id the host minted says nothing about the item, so a query that
	// happens to resemble it must not find the item. A chosen id is still searched.
	SkipIDText func(id string) bool
	// Logger receives the store's log lines. The zero Logger discards them.
	Logger zerolog.Logger
}

// Store is the in-memory set of memory items with its lexical indexes. It shares
// its host's lock: the host may read the items and its own state together under
// one mutex, and the hooks in Config run inside it.
type Store struct {
	mu    Locker
	cfg   Config
	cells map[string]Item
	kw    *trigramIndex
	vocab *vocabIndex

	changeHook func(op string, item *Item, id string, kind string)
	hitHook    func(itemIDs []string, query, kind string, kinds []string)
}

// StoreOption configures NewStore.
type StoreOption func(*Store)

// WithLocker makes the store use the host's lock instead of a private one.
func WithLocker(l Locker) StoreOption { return func(s *Store) { s.mu = l } }

// WithConfig sets the host seams.
func WithConfig(cfg Config) StoreOption { return func(s *Store) { s.cfg = cfg } }

// NewStore builds an empty store.
func NewStore(opts ...StoreOption) *Store {
	s := &Store{cells: make(map[string]Item), kw: NewTrigramIndex(), vocab: newVocabIndex()}
	for _, o := range opts {
		o(s)
	}
	if s.mu == nil {
		s.mu = &sync.RWMutex{}
	}
	if s.cfg.IDPrefix == "" {
		s.cfg.IDPrefix = "memory-"
	}
	return s
}

// newID is the id of an item written without one.
func (s *Store) newID(now time.Time) string {
	if s.cfg.NewID != nil {
		return s.cfg.NewID()
	}
	return s.cfg.IDPrefix + now.UTC().Format("20060102150405.000000000")
}

// Subject is the instance identity the store was configured with.
func (s *Store) Subject() string { return s.cfg.Subject }

// Vocabulary is the kind vocabulary the store was configured with.
func (s *Store) Vocabulary() Vocabulary { return s.cfg.Vocabulary }

func (s *Store) dirty() {
	if s.cfg.Dirty != nil {
		s.cfg.Dirty()
	}
}

func (s *Store) signal() {
	if s.cfg.Signal != nil {
		s.cfg.Signal()
	}
}

func (s *Store) before(item *Item) {
	if s.cfg.Before != nil {
		s.cfg.Before(item)
	}
}

func (s *Store) read(items []Item) []Item {
	if s.cfg.Read != nil {
		s.cfg.Read(items)
	}
	return items
}

func (s *Store) readOne(item *Item) {
	if s.cfg.Read != nil {
		one := []Item{*item}
		s.cfg.Read(one)
		*item = one[0]
	}
}

// validate runs the store's own rules, then the host's.
func (s *Store) validate(item Item) string {
	if msg := s.ValidateBase(item); msg != "" {
		return msg
	}
	if s.cfg.Validate != nil {
		return s.cfg.Validate(item)
	}
	return ""
}

// coerce lets the host rewrite an item's kind before it is validated.
func (s *Store) coerce(item *Item) {
	if s.cfg.Vocabulary.Coerce != nil {
		s.cfg.Vocabulary.Coerce(item)
	}
}

// StampKnowledgeRetention stamps a computed StaleAfter onto an item that has
// none. A non-zero StaleAfter is never overwritten. stable_fact and
// operational_pattern get retention(partition) days, the partition read from the
// item's tenant/ tag so a per-partition rule applies. A hypothesis or unknown
// without a deadline gets the compiled default instead of being refused by the
// base rules. Other kinds are untouched.
func StampKnowledgeRetention(item *Item, retention func(partition string) int) {
	if item.StaleAfter != nil && !item.StaleAfter.IsZero() {
		return // the caller set one deliberately
	}
	switch item.Kind {
	case KindHypothesis:
		t := metav1.NewTime(time.Now().Add(DefaultHypothesisStaleAfter))
		item.StaleAfter = &t
		return
	case KindUnknown:
		t := metav1.NewTime(time.Now().Add(DefaultUnknownStaleAfter))
		item.StaleAfter = &t
		return
	case KindStableFact, KindOperationalPattern:
	default:
		return
	}
	if retention == nil {
		return
	}
	days := retention(PartitionFromTags(item.Tags))
	if days <= 0 {
		return
	}
	t := metav1.NewTime(time.Now().AddDate(0, 0, days))
	item.StaleAfter = &t
}

// stampDeadline gives a new item its deadline: DefaultTTL for its kind, else the
// knowledge retention rule. byDefault reports that the deadline came from
// DefaultTTL, so an update can put the stored one back.
func (s *Store) stampDeadline(item *Item) (byDefault bool) {
	if item.StaleAfter != nil && !item.StaleAfter.IsZero() {
		return false
	}
	if ttl, ok := s.cfg.DefaultTTL[item.Kind]; ok {
		t := metav1.NewTime(time.Now().Add(ttl))
		item.StaleAfter = &t
		return true
	}
	StampKnowledgeRetention(item, s.cfg.Retention)
	return false
}

// Items returns all public memory items sorted by UpdatedAt descending, never
// a private item (IsPrivateItem) regardless of caller. Items past their
// stale_after deadline are lazily marked stale. ForUser is the only reader that
// can return a private item, to its owner; AllItems is the internal exception.
func (s *Store) Items() []Item {
	s.expireStaleItemsIfNeeded()
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]Item, 0, len(s.cells))
	for _, item := range s.cells {
		if !VisibleToUser(item, "") {
			continue
		}
		items = append(items, item.Clone())
	}
	sortMemoryItems(items)
	return s.read(items)
}

// AllItems returns every active memory item, private ones included. It is not
// a general reader: it serves the sync that persists items as cells, which must
// keep writing a private item's cell back or the sync deletes it. Never
// wire it to a read surface a caller can reach.
func (s *Store) AllItems() []Item {
	s.expireStaleItemsIfNeeded()
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.read(cloneMemoryItems(s.cells))
}

// Filtered returns memory items filtered by status, kind, scope, subject and
// confidence; empty filters match all. Items past their stale_after deadline
// are lazily marked stale.
//
// The subject filter matches the item's literal Subject or a service/<name> tag
// equal to subject, so get_memory_items(subject="auth") finds items that carry
// the service in a tag .
func (s *Store) Filtered(status, kind, scope, subject, confidence string, limit int) []Item {
	return s.filteredForUser("", status, kind, scope, subject, confidence, limit)
}

// ForUser is Filtered plus userHash's own private items: a private item is
// included only when its PrivateOwner tag equals userHash. It is the only
// reader that returns a private item, and only to its owner (no admin bypass);
// an empty userHash behaves like Filtered.
func (s *Store) ForUser(userHash, status, kind, scope, subject, confidence string, limit int) []Item {
	return s.filteredForUser(userHash, status, kind, scope, subject, confidence, limit)
}

// filteredForUser implements Filtered and ForUser. The visibility gate runs
// in the same pass as the filters, before the limit, so limit counts items the
// caller can see.
func (s *Store) filteredForUser(userHash, status, kind, scope, subject, confidence string, limit int) []Item {
	s.expireStaleItemsIfNeeded()

	s.mu.RLock()
	defer s.mu.RUnlock()
	filtered := make([]Item, 0, len(s.cells))
	for _, item := range s.cells {
		if status != "" && item.Status != status {
			continue
		}
		if kind != "" && item.Kind != kind {
			continue
		}
		if scope != "" && item.Scope != scope {
			continue
		}
		if subject != "" && !matchesSubjectFilter(item, subject) {
			continue
		}
		if confidence != "" && item.Confidence != confidence {
			continue
		}
		if !VisibleToUser(item, userHash) {
			continue
		}
		filtered = append(filtered, item.Clone())
	}
	sortMemoryItems(filtered)
	if limit > 0 && len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return s.read(filtered)
}

// matchesSubjectFilter reports whether item matches a subject filter value,
// checking the literal Subject field first, then falling back to a
// service/<name> tag (see Filtered's doc comment).
func matchesSubjectFilter(item Item, subject string) bool {
	if item.Subject == subject {
		return true
	}
	v, ok := ServiceTagValue(item)
	return ok && v == subject
}

// Upsert adds or updates a single memory item.
// Returns an error string if validation fails; item is unchanged in that case.
func (s *Store) Upsert(item Item) (Item, string) {
	s.coerce(&item)
	byDefault := s.stampDeadline(&item)
	if msg := s.validate(item); msg != "" {
		return Item{}, msg
	}

	s.mu.Lock()
	now := time.Now()
	nowMeta := metav1.NewTime(now)
	if strings.TrimSpace(item.ID) == "" {
		item.ID = s.newID(now)
	}
	existing, hasExisting := s.cells[item.ID]
	origin, originErr := resolveOrigin(existing.Origin, hasExisting, item.Origin)
	if originErr != "" {
		s.mu.Unlock()
		return Item{}, originErr
	}
	item.Origin = origin
	if byDefault && hasExisting {
		// A default deadline is for a new item; an update that names none keeps
		// the stored one.
		item.StaleAfter = existing.StaleAfter
	}
	if hasExisting {
		if item.CreatedAt.IsZero() {
			item.CreatedAt = existing.CreatedAt
		}
		// Keep the existing Links: callers mutate the graph through AddLink. Skipped
		// when the caller sets LinksSet: a state-note refresh supplies its own
		// authoritative link set, possibly empty.
		if !item.LinksSet && len(item.Links) == 0 && len(existing.Links) > 0 {
			item.Links = existing.Links
		}
	} else if item.CreatedAt.IsZero() {
		item.CreatedAt = nowMeta
	}
	item.UpdatedAt = nowMeta
	if strings.TrimSpace(item.Status) == "" {
		item.Status = "active"
	}
	s.before(&item)
	item = s.storeLocked(item) // Title/Content/ID may have just changed — refresh the search cache before storing
	s.mu.Unlock()
	s.notifyChange("upsert", &item, "", item.Kind)
	return item, ""
}

// storeLocked stores item, refreshes its lexical cache, marks dirty, indexes
// it and signals watchers: the sequence Upsert and UpsertDedup's merge path
// share. It returns item with the cache populated, since item is passed by
// value. The caller holds s.mu, unlocks, then calls notifyChange itself.
func (s *Store) storeLocked(item Item) Item {
	s.buildCache(&item)
	s.cells[item.ID] = item.Clone()
	s.dirty()
	s.indexLocked(item)
	s.signal()
	return item
}

// memoryDedupEligibleKinds are the model-authored knowledge kinds
// UpsertDedup's near-duplicate check applies to; other kinds insert or update
// as Upsert does. "mindmap" is absent: it has a stable ID per partition and
// service and is always upserted in place.
var memoryDedupEligibleKinds = map[string]bool{
	KindStableFact:         true,
	KindOperationalPattern: true,
	KindWatchItem:          true,
	KindHypothesis:         true,
}

// mergeMemoryTags unions existing's tags (kept first, in order) with
// incoming's, dropping duplicates.
func mergeMemoryTags(existing, incoming []string) []string {
	seen := make(map[string]struct{}, len(existing)+len(incoming))
	out := make([]string, 0, len(existing)+len(incoming))
	for _, t := range existing {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	for _, t := range incoming {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// UpsertDedup is the write gate for model-authored knowledge cells: a caller
// restating an existing fact under a fresh ID lands on the same cell instead of
// minting a copy.
//
// It delegates to Upsert unchanged (mergedInto == "") when item.ID names an
// existing cell or the kind is not in memoryDedupEligibleKinds. Hydration and
// machine-state note paths call Upsert directly and never dedup.
//
// Otherwise, under the lock, it scores item against every active cell of the
// same Kind (FindNearDuplicate) and on a hit updates that cell in place: ID,
// CreatedAt, Links, Supersedes and CellOriginated are kept, every other field
// comes from item, Tags is the union (existing first) and UpdatedAt is bumped.
// No hit inserts item as Upsert would.
func (s *Store) UpsertDedup(item Item) (result Item, mergedInto string, validationErr string) {
	s.coerce(&item)
	// The delegations to Upsert below take the item as the caller gave it, so
	// Upsert can tell a deadline it stamps itself from one the caller set.
	orig := item
	byDefault := s.stampDeadline(&item)
	if msg := s.validate(item); msg != "" {
		return Item{}, "", msg
	}

	if !memoryDedupEligibleKinds[item.Kind] {
		result, validationErr = s.Upsert(orig)
		return result, "", validationErr
	}

	s.mu.Lock()
	if strings.TrimSpace(item.ID) != "" {
		if _, exists := s.cells[item.ID]; exists {
			s.mu.Unlock()
			result, validationErr = s.Upsert(orig)
			return result, "", validationErr
		}
	}

	// Collecting candidates is O(total cells), since there is no kind-keyed index;
	// the expensive scoring runs only over candidates of item's Kind.
	var candidates []Item
	for _, c := range s.cells {
		if c.Kind == item.Kind {
			candidates = append(candidates, c)
		}
	}
	match, score, found := FindNearDuplicate(candidates, item)
	if !found {
		s.mu.Unlock()
		result, validationErr = s.Upsert(orig)
		return result, "", validationErr
	}

	origin, originErr := resolveOrigin(match.Origin, true, item.Origin)
	if originErr != "" {
		s.mu.Unlock()
		return Item{}, "", originErr
	}
	merged := item.Clone()
	merged.Origin = origin
	merged.ID = match.ID
	merged.CreatedAt = match.CreatedAt
	merged.Links = match.Links
	merged.Supersedes = match.Supersedes
	merged.CellOriginated = match.CellOriginated
	merged.Tags = mergeMemoryTags(match.Tags, item.Tags)
	if strings.TrimSpace(merged.Status) == "" {
		merged.Status = "active"
	}
	merged.UpdatedAt = metav1.NewTime(time.Now())
	if byDefault && !s.cfg.RenewOnMerge {
		merged.StaleAfter = match.StaleAfter
	}

	s.before(&merged)
	merged = s.storeLocked(merged)
	s.mu.Unlock()
	s.notifyChange("upsert", &merged, "", merged.Kind)

	s.cfg.Logger.Info().
		Str("op", "merge").
		Str("incoming_id", item.ID).
		Str("merged_into", match.ID).
		Str("kind", merged.Kind).
		Float64("score", score).
		Msg("memory: near-duplicate merged")

	return merged, match.ID, ""
}

// Get returns a copy of one memory item by ID, passed through the Read
// hook.
func (s *Store) Get(id string) (Item, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, ok := s.cells[id]
	if !ok {
		return Item{}, false
	}
	clone := item.Clone()
	s.readOne(&clone)
	return clone, true
}

// GetFold is Get plus a case-insensitive fallback, so a query whose casing
// differs from the stored ID still resolves. The O(1) exact lookup always runs;
// the O(n) fold scan runs only when foldScan is true, which keeps the
// classifier's per-event hot path at the plain lookup.
func (s *Store) GetFold(id string, foldScan bool) (Item, bool) {
	if id == "" {
		return Item{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if item, ok := s.cells[id]; ok {
		clone := item.Clone()
		s.readOne(&clone)
		return clone, true
	}
	if !foldScan {
		return Item{}, false
	}
	for _, item := range s.cells {
		if strings.EqualFold(item.ID, id) {
			clone := item.Clone()
			s.readOne(&clone)
			return clone, true
		}
	}
	return Item{}, false
}

// memorySpecsEqualIgnoringEmbedding compares two cell specs with Embedding
// blanked: the vector already lives in HNSW and the cell sync preserves the
// CR's embedding on update, so an embedding-only difference is not state drift.
func memorySpecsEqualIgnoringEmbedding(a, b memoryv1.MemoryCellSpec) bool {
	a.Embedding, b.Embedding = "", ""
	return equality.Semantic.DeepEqual(a, b)
}

// Mirror reflects a persisted MemoryCell spec into the in-memory map without
// acting as a mutation: no UpdatedAt stamp, and no notification when nothing
// changed. Routing cell reconciles through Upsert would feed the mutation-watch
// sync loop its own writes. A cell older than the in-memory item never
// clobbers it, since that is a pending local change. It returns true when
// in-memory state changed (a real external edit).
func (s *Store) Mirror(item Item) bool {
	if strings.TrimSpace(item.ID) == "" {
		return false
	}
	if strings.TrimSpace(item.Status) == "" {
		item.Status = "active"
	}
	s.mu.Lock()
	// A cell written before origins existed, or edited by an operator, has none:
	// keep the in-memory origin, else derive it as a restore does.
	if existing, ok := s.cells[item.ID]; ok && item.Origin == "" && existing.Origin != "" {
		item.Origin = existing.Origin
	} else {
		item.Origin = RestoredOrigin(item)
	}
	if existing, ok := s.cells[item.ID]; ok {
		if memorySpecsEqualIgnoringEmbedding(existing.MemoryCellSpec, item.MemoryCellSpec) {
			s.mu.Unlock()
			return false
		}
		if existing.UpdatedAt.Time.After(item.UpdatedAt.Time) {
			s.mu.Unlock()
			return false
		}
	}
	s.buildCache(&item) // real external edit (checked above) — Title/Content/ID may have changed
	s.cells[item.ID] = item.Clone()
	s.dirty()
	s.indexLocked(item)
	s.signal()
	s.mu.Unlock()
	s.notifyChange("upsert", &item, "", item.Kind)
	return true
}

// ThoughtLinkRejection reports why AddLink would reject link, or "". It
// covers only the checks that depend on the caller's values (empty ToID,
// invalid Relation, self-link), not the state-dependent ones (missing cell,
// cell at MaxModelLinks). AddLink calls it so the write path and callers such
// as a handler that validates a link before writing it agree.
func ThoughtLinkRejection(fromID string, link memoryv1.ThoughtLink) string {
	// An empty ToID is never a valid edge; a payload with the wrong JSON key
	// would otherwise append a dangling link.
	if link.ToID == "" {
		return "link has an empty toId"
	}
	// The relation enum is closed: an unrecognized relation is rejected outright,
	// with no partial acceptance.
	if !validThoughtLinkRelations[link.Relation] {
		return fmt.Sprintf("relation %q is not valid; must be one of: %s", link.Relation, strings.Join(ValidThoughtLinkRelationsOrdered, ", "))
	}
	// A cell cannot associate with itself.
	if fromID == link.ToID {
		return "link is a self-link: fromID equals toId (" + fromID + ")"
	}
	return ""
}

// AddLink appends a ThoughtLink to the cell fromID. It returns false if the
// cell does not exist, the link fails ThoughtLinkRejection, or the cell is at
// MaxModelLinks and the link does not out-weigh the lowest-weight one. A link
// with the same ToID and Relation replaces the old one and is exempt from the
// cap.
func (s *Store) AddLink(fromID string, link memoryv1.ThoughtLink) bool {
	if ThoughtLinkRejection(fromID, link) != "" {
		return false
	}
	s.mu.Lock()
	item, ok := s.cells[fromID]
	if !ok {
		s.mu.Unlock()
		return false
	}
	// Replace a link with the same ToID+Relation; exempt from the cap since the
	// slice does not grow.
	replaced := false
	for i, l := range item.Links {
		if l.ToID == link.ToID && l.Relation == link.Relation {
			item.Links[i] = link
			replaced = true
			break
		}
	}
	if !replaced {
		if len(item.Links) >= MaxModelLinks {
			// Evict the lowest-weight link, ties broken by the largest ToID so the choice
			// is deterministic across replicas.
			evictIdx := 0
			for i := 1; i < len(item.Links); i++ {
				cur := item.Links[evictIdx]
				cand := item.Links[i]
				if cand.Weight < cur.Weight || (cand.Weight == cur.Weight && cand.ToID > cur.ToID) {
					evictIdx = i
				}
			}
			if link.Weight <= item.Links[evictIdx].Weight {
				s.mu.Unlock()
				return false
			}
			item.Links[evictIdx] = link
		} else {
			item.Links = append(item.Links, link)
		}
	}
	item.UpdatedAt = metav1.NewTime(time.Now())
	s.cells[fromID] = item
	s.dirty()
	s.signal()
	s.mu.Unlock()
	return true
}

// PruneInvalidLinks drops structurally invalid ThoughtLinks from every cell:
// an empty ToID or a self-link. AddLink rejects both at write time, so this
// removes only data from before that guard or from another write path.
//
// It does not check that a link's target exists: data cells a host keeps
// are absent from this map by design, so that would delete legitimate edges. It
// dirties and signals only when a link was removed, or every cell would be
// rewritten on each sweep. It returns the number of links removed.
func (s *Store) PruneInvalidLinks() int {
	s.mu.Lock()
	removed := 0
	for id, item := range s.cells {
		if len(item.Links) == 0 {
			continue
		}
		kept := make([]memoryv1.ThoughtLink, 0, len(item.Links))
		for _, l := range item.Links {
			if l.ToID == "" || l.ToID == id {
				removed++
				continue
			}
			kept = append(kept, l)
		}
		if len(kept) == len(item.Links) {
			continue
		}
		if len(kept) == 0 {
			item.Links = nil
		} else {
			item.Links = kept
		}
		s.cells[id] = item
	}
	if removed > 0 {
		s.dirty()
		s.signal()
	}
	s.mu.Unlock()
	return removed
}

// Delete removes a single memory item by ID.
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	// Captured before the delete: a delete carries no item, and notifyChange
	// needs the kind.
	item, ok := s.cells[id]
	if !ok {
		s.mu.Unlock()
		return false
	}
	delete(s.cells, id)
	s.dirty()
	if s.kw != nil {
		s.kw.Remove(id)
	}
	if s.vocab != nil {
		s.vocab.Remove(id)
	}
	s.signal()
	s.mu.Unlock()
	s.notifyChange("delete", nil, id, item.Kind)
	return true
}

// Expire marks a memory item as stale.
func (s *Store) Expire(id string) bool {
	s.mu.Lock()
	item, ok := s.cells[id]
	if !ok {
		s.mu.Unlock()
		return false
	}
	item.Status = "stale"
	item.UpdatedAt = metav1.NewTime(time.Now())
	s.cells[id] = item
	s.dirty()
	// Stale items are excluded from search; drop them from the lexical index
	// too.
	if s.kw != nil {
		s.kw.Remove(id)
	}
	if s.vocab != nil {
		s.vocab.Remove(id)
	}
	s.signal()
	s.mu.Unlock()
	// Expiry is a soft delete, but the change feed only has upsert and delete, so
	// it is published as a delete.
	s.notifyChange("delete", nil, id, item.Kind)
	return true
}

// RefutedRecord is the JSON held under RefutedDataKey.
type RefutedRecord struct {
	Reason   string   `json:"reason"`
	Evidence []string `json:"evidence"`
	At       string   `json:"at"`
}

// Refute refutes a cell: it records data.refuted {reason, evidence, at},
// expires the cell through Expire, then expires every active derived cell that
// cites it, one hop only (an evidenceRef naming the id, with or without a cell:
// prefix, or a link to it). Observed, human and verified cells are never
// cascaded. It returns whether the cell existed and the ids expired as a
// consequence.
func (s *Store) Refute(id, reason string, evidence []string, now time.Time) (refuted bool, cascaded []string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return false, nil
	}
	s.mu.Lock()
	item, ok := s.cells[id]
	if !ok {
		s.mu.Unlock()
		return false, nil
	}
	if evidence == nil {
		evidence = []string{}
	}
	raw, err := json.Marshal(RefutedRecord{Reason: reason, Evidence: evidence, At: now.UTC().Format(time.RFC3339)})
	if err == nil {
		data := make(map[string]memoryv1.DataEntry, len(item.Data)+1)
		for k, v := range item.Data {
			data[k] = v
		}
		data[RefutedDataKey] = memoryv1.DataEntry{Bytes: base64.StdEncoding.EncodeToString(raw), Mime: "application/json"}
		item.Data = data
		s.cells[id] = item
	}
	var hits []string
	for cid, c := range s.cells {
		if cid == id || !c.IsActive() || c.Origin != OriginDerived {
			continue
		}
		if citesCell(c, id) {
			hits = append(hits, cid)
		}
	}
	s.mu.Unlock()

	s.Expire(id)
	sort.Strings(hits)
	for _, cid := range hits {
		if s.Expire(cid) {
			cascaded = append(cascaded, cid)
		}
	}
	return true, cascaded
}

// citesCell reports whether c names id in its evidence refs (with or without
// a cell: prefix) or links to it.
func citesCell(c Item, id string) bool {
	for _, r := range c.EvidenceRefs {
		if r == id || strings.TrimPrefix(r, "cell:") == id {
			return true
		}
	}
	for _, l := range c.Links {
		if l.ToID == id {
			return true
		}
	}
	return false
}

// PruneExcept hard-deletes, without notifying, every item that is
// cell-originated (loaded from an existing cell) or soft-deleted (stale or
// superseded) and whose id is not in keep. A host calls it with a fresh list of
// its own cell ids. It closes two gaps: a cell-originated item whose cell was
// deleted without this instance seeing the delete, and expired or superseded
// items that would otherwise pile up for the life of the process. Active items
// authored in memory are never touched. This is read-model garbage collection,
// not a state change. It returns the number pruned.
func (s *Store) PruneExcept(keep map[string]struct{}) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	pruned := 0
	for id, item := range s.cells {
		if !item.CellOriginated && item.Status != "stale" && item.Status != "superseded" {
			continue
		}
		if _, ok := keep[id]; ok {
			continue
		}
		delete(s.cells, id)
		if s.kw != nil {
			s.kw.Remove(id)
		}
		if s.vocab != nil {
			s.vocab.Remove(id)
		}
		pruned++
	}
	return pruned
}

// Supersede marks the old item as superseded and inserts the
// new item with its Supersedes field pointing at the old ID.
func (s *Store) Supersede(oldID string, newItem Item) (Item, string) {
	s.coerce(&newItem)
	s.stampDeadline(&newItem)
	if msg := s.validate(newItem); msg != "" {
		return Item{}, msg
	}

	s.mu.Lock()
	old, ok := s.cells[oldID]
	if !ok {
		s.mu.Unlock()
		return Item{}, "not_found"
	}
	// Resolve the origin first: a refused write leaves both cells as they were.
	prev, hasPrev := s.cells[newItem.ID]
	if strings.TrimSpace(newItem.ID) == "" {
		hasPrev = false
	}
	origin, originErr := resolveOrigin(prev.Origin, hasPrev, newItem.Origin)
	if originErr == "" && old.Origin == OriginHuman && origin == OriginDerived {
		originErr = errDerivedReplacesHuman
	}
	if originErr != "" {
		s.mu.Unlock()
		return Item{}, originErr
	}
	newItem.Origin = origin
	now := time.Now()
	nowMeta := metav1.NewTime(now)
	old.Status = "superseded"
	old.UpdatedAt = nowMeta
	s.cells[oldID] = old
	if s.kw != nil {
		s.kw.Remove(oldID)
	}
	if s.vocab != nil {
		s.vocab.Remove(oldID)
	}

	if strings.TrimSpace(newItem.ID) == "" {
		newItem.ID = s.newID(now)
	}
	newItem.CreatedAt = nowMeta
	newItem.UpdatedAt = nowMeta
	if strings.TrimSpace(newItem.Status) == "" {
		newItem.Status = "active"
	}
	if !ContainsString(newItem.Supersedes, oldID) {
		newItem.Supersedes = append(newItem.Supersedes, oldID)
	}
	s.buildCache(&newItem) // fresh item — Title/Content/ID are whatever the caller supplied
	s.cells[newItem.ID] = newItem.Clone()
	s.dirty()
	s.indexLocked(newItem)
	s.signal()
	s.mu.Unlock()
	// Emit both halves: the old cell leaves the live view (delete, kind captured
	// before the status flip) and the new one enters it.
	s.notifyChange("delete", nil, oldID, old.Kind)
	s.notifyChange("upsert", &newItem, "", newItem.Kind)
	return newItem, ""
}

// ScoredItem is an item with the activation a traversal gave it.
type ScoredItem struct {
	Item  Item
	Score float32
}

// Traverse spreads activation over the ThoughtLink graph from the anchor IDs,
// decaying by decayFactor per hop. It returns items by activation score
// descending, excluding the anchors and any private item, since the traversal
// has no caller identity to check an owner tag against. A maxDepth outside
// 1..8 means 8.
func (s *Store) Traverse(anchorIDs []string, maxDepth int, decayFactor float32) []Item {
	scored := s.TraverseScored(anchorIDs, maxDepth, decayFactor)
	out := make([]Item, len(scored))
	for i, r := range scored {
		out[i] = r.Item
	}
	return out
}

// TraverseScored is Traverse that also returns the activation of each item.
// Each hop multiplies the score by decayFactor and the link's weight, an item
// keeps the best score any path gave it, and ties order by ID.
func (s *Store) TraverseScored(anchorIDs []string, maxDepth int, decayFactor float32) []ScoredItem {
	if maxDepth <= 0 || maxDepth > 8 {
		maxDepth = 8
	}
	if decayFactor <= 0 || decayFactor > 1 {
		decayFactor = 0.6
	}

	type activation struct {
		score float32
		depth int
	}

	activated := make(map[string]activation)
	for _, id := range anchorIDs {
		activated[id] = activation{score: 1.0, depth: 0}
	}

	s.mu.RLock()
	// BFS over ThoughtLinks with activation decay.
	queue := make([]string, len(anchorIDs))
	copy(queue, anchorIDs)
	for len(queue) > 0 {
		fromID := queue[0]
		queue = queue[1:]
		cur := activated[fromID]
		if cur.depth >= maxDepth {
			continue
		}
		item, ok := s.cells[fromID]
		if !ok {
			continue
		}
		for _, link := range item.Links {
			if link.ToID == "" {
				continue
			}
			newScore := cur.score * decayFactor * link.Weight
			if existing, seen := activated[link.ToID]; seen {
				if newScore <= existing.score {
					continue
				}
			}
			activated[link.ToID] = activation{score: newScore, depth: cur.depth + 1}
			queue = append(queue, link.ToID)
		}
	}

	anchorSet := make(map[string]bool, len(anchorIDs))
	for _, id := range anchorIDs {
		anchorSet[id] = true
	}

	var results []ScoredItem
	for id, act := range activated {
		if anchorSet[id] {
			continue
		}
		item, ok := s.cells[id]
		if !ok || !item.IsActive() || IsPrivateItem(item) {
			continue
		}
		results = append(results, ScoredItem{Item: item.Clone(), Score: act.score})
	}
	s.mu.RUnlock()

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].Item.ID < results[j].Item.ID
	})
	return results
}

// ResolveAnchors maps an anchor string to ids of shared, active items: an exact
// id first (case-insensitively), then every item whose Subject is the anchor,
// then every item whose Subject or Title contains it. Private items never
// resolve.
func (s *Store) ResolveAnchors(anchor string) []string {
	anchor = strings.ToLower(strings.TrimSpace(anchor))
	if anchor == "" {
		return nil
	}
	pool := s.Filtered("active", "", "", "", "", 0)
	for _, it := range pool {
		if strings.ToLower(it.ID) == anchor {
			return []string{it.ID}
		}
	}
	var ids []string
	for _, it := range pool {
		if strings.ToLower(it.Subject) == anchor {
			ids = append(ids, it.ID)
		}
	}
	if len(ids) > 0 {
		sort.Strings(ids)
		return ids
	}
	for _, it := range pool {
		if strings.Contains(strings.ToLower(it.Subject), anchor) || strings.Contains(strings.ToLower(it.Title), anchor) {
			ids = append(ids, it.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// Context returns what an anchor activates: the anchor items themselves and
// everything reachable from them over ThoughtLinks, by spreading activation. Each
// hop multiplies the score by 0.6 and the link's weight; at most maxDepth hops
// are followed (default 3, at most 8). Only shared, active items take part.
func (s *Store) Context(anchor string, maxDepth int) (anchors []Item, activated []ScoredItem) {
	if maxDepth <= 0 {
		maxDepth = 3
	}
	ids := s.ResolveAnchors(anchor)
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if it, ok := s.Get(id); ok {
			anchors = append(anchors, it)
		}
	}
	return anchors, s.TraverseScored(ids, maxDepth, 0.6)
}

// Recent returns the active items matching kind, scope and subject (empty
// matches all) newest first by creation time, with the update time breaking
// ties, capped to limit. It is the recall path for memory selected by recency,
// the opposite of ranking by relevance. skip, when set, drops items the caller
// treats as expired. Only the 500 most recently updated candidates are sorted.
func (s *Store) Recent(kind, scope, subject string, limit int, skip func(Item) bool) []Item {
	if limit <= 0 {
		return nil
	}
	all := s.Filtered("active", kind, scope, subject, "", recentCandidateCap)
	if skip != nil {
		kept := all[:0]
		for _, it := range all {
			if !skip(it) {
				kept = append(kept, it)
			}
		}
		all = kept
	}
	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(&all[j].CreatedAt) {
			return all[i].CreatedAt.After(all[j].CreatedAt.Time)
		}
		if !all[i].UpdatedAt.Equal(&all[j].UpdatedAt) {
			return all[i].UpdatedAt.After(all[j].UpdatedAt.Time)
		}
		return all[i].ID < all[j].ID
	})
	if len(all) > limit {
		all = all[:limit]
	}
	return all
}

// recentCandidateCap bounds the candidate set Recent sorts. Filtered orders by
// update time, so a generous superset is taken and then sorted by creation.
const recentCandidateCap = 500

// Restore replaces the in-memory items with persisted ones from MemoryCell
// CRs at startup and rebuilds the lexical indexes.
func (s *Store) Restore(items []Item) {
	s.mu.Lock()
	s.cells = make(map[string]Item, len(items))
	s.kw = NewTrigramIndex()
	s.vocab = newVocabIndex()
	for _, item := range items {
		item.Origin = RestoredOrigin(item)
		s.buildCache(&item) // restored from CR — no prior in-process cache to reuse
		s.cells[item.ID] = item.Clone()
		s.indexLocked(item)
	}
	s.kw.compact()
	s.vocab.compact()
	s.dirty()
	s.signal()
	s.mu.Unlock()
}

// indexLocked (re)indexes item in the trigram index and word vocabulary, or
// removes it from both when inactive or private (a private item is never a
// candidate for the shared index). Every write path calls it with the
// post-update state. The caller holds s.mu. It is in-memory only and never
// writes a cell.
func (s *Store) indexLocked(item Item) {
	if !item.IsActive() || IsPrivateItem(item) {
		if s.kw != nil {
			s.kw.Remove(item.ID)
		}
		if s.vocab != nil {
			s.vocab.Remove(item.ID)
		}
		return
	}
	// idHaystack folds the item's ID into the indexed text, so an id-shaped or
	// partial-id query trigram-matches it too.
	if s.kw != nil {
		s.kw.Index(item.ID, item.Title, s.haystackOf(item))
	}
	if s.vocab != nil {
		// vocabWordsForItem reads the lexical cache, which every write path builds
		// before calling indexLocked; lowerFieldsFor falls back if it is not built.
		words, joins := vocabWordsForItem(item)
		s.vocab.Index(item.ID, words, joins)
	}
}

// expireStaleItemsIfNeeded checks under RLock whether any active item has
// passed its StaleAfter deadline, acquiring the exclusive lock only when at
// least one item needs to be marked stale.
func (s *Store) expireStaleItemsIfNeeded() {
	now := time.Now()
	s.mu.RLock()
	hasExpired := false
	for _, item := range s.cells {
		if item.Status == "active" && item.StaleAfter != nil && !item.StaleAfter.IsZero() && now.After(item.StaleAfter.Time) {
			hasExpired = true
			break
		}
	}
	s.mu.RUnlock()
	if !hasExpired {
		return
	}
	s.mu.Lock()
	s.expireStaleItemsLocked()
	s.mu.Unlock()
}

// expireStaleItemsLocked marks active items past StaleAfter as stale. The
// caller holds the write lock.
func (s *Store) expireStaleItemsLocked() {
	now := time.Now()
	changed := false
	for id, item := range s.cells {
		if item.Status == "active" && item.StaleAfter != nil && !item.StaleAfter.IsZero() && now.After(item.StaleAfter.Time) {
			item.Status = "stale"
			item.UpdatedAt = metav1.NewTime(now)
			s.cells[id] = item
			if s.kw != nil {
				s.kw.Remove(id)
			}
			if s.vocab != nil {
				s.vocab.Remove(id)
			}
			changed = true
		}
	}
	if changed {
		s.dirty()
	}
}

func ContainsString(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

func sortMemoryItems(out []Item) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt.Equal(&out[j].UpdatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt.Time)
	})
}

func cloneMemoryItems(items map[string]Item) []Item {
	out := make([]Item, 0, len(items))
	for _, item := range items {
		out = append(out, item.Clone())
	}
	sortMemoryItems(out)
	return out
}

// KeywordSearchScored runs a keyword search over active memory items and
// reports whether the top hit is a confident lexical answer: it covers every
// query token (all but one for 3+ tokens), even if per-token miss penalties
// lower its raw score. HybridSearch uses it to decide whether fusing in the
// semantic round trip is worth it.
func (s *Store) KeywordSearchScored(query, kind, subject string, limit int) (items []Item, confident bool) {
	items, confident, _ = s.KeywordSearchScoredWithTrigramHit(query, kind, subject, limit)
	return items, confident
}

// KeywordSearchScoredWithTrigramHit is KeywordSearchScored plus whether the
// trigram index contributed a candidate (see KeywordSearchWithTrigramHit).
func (s *Store) KeywordSearchScoredWithTrigramHit(query, kind, subject string, limit int) (items []Item, confident bool, trigramHit bool) {
	items, trigramHit, toks, weights := s.keywordSearch("", query, kind, subject, limit, 0)
	if len(items) == 0 {
		return items, false, trigramHit
	}
	if len(toks) == 0 {
		return items, true, trigramHit
	}
	nt, ntf, nc, ncf := lowerFieldsFor(items[0])
	matched, _ := softMatchCoverageFields(nt, ntf, nc, ncf, toks)
	need := len(toks)
	if len(toks) >= 3 {
		need = len(toks) - 1
	}
	if matched < need {
		return items, false, trigramHit
	}
	// Covering enough tokens is not enough: the tokens matched must also carry
	// most of the query's weight, or an item that matches only the ordinary words
	// and misses the one that names the thing asked about would pass.
	mass := weightedMatchFraction(nt, ntf, nc, ncf, toks, weights)
	return items, mass >= lexConfidentMass, trigramHit
}

// lexConfidentMass is the share of the query's weight the top lexical item's
// matched tokens must account for before the lexical answer is called confident.
const lexConfidentMass = 0.75

// KeywordSearch scans active memory items by lexical match on title+content,
// gated by trigram-index candidates and ranked by lexScore.
func (s *Store) KeywordSearch(query, kind, subject string, limit int) []Item {
	items, _ := s.KeywordSearchWithTrigramHit(query, kind, subject, limit)
	return items
}

// KeywordSearchWithTrigramHit is KeywordSearch plus whether the trigram
// index's candidate set contributed at least one of the final ranked hits (the
// signal behind a trigram-hit counter).
func (s *Store) KeywordSearchWithTrigramHit(query, kind, subject string, limit int) ([]Item, bool) {
	items, trigramHit, _, _ := s.keywordSearch("", query, kind, subject, limit, 0)
	return items, trigramHit
}

// KeywordSearchForUser is KeywordSearch that also considers the private items
// owned by userHash, which no index holds, so they are scored by the lexical
// match alone. With no userHash it is KeywordSearch.
func (s *Store) KeywordSearchForUser(userHash, query, kind, subject string, limit int) []Item {
	items, _, _, _ := s.keywordSearch(userHash, query, kind, subject, limit, 0)
	return items
}

// KeywordSearchPerKind is KeywordSearch with per-kind fairness: at most perKind
// results of any one kind, so a dominant kind cannot fill the limit and starve
// the smaller categories. Relevance order within a kind is kept and limit still
// caps the total. It suits a search that spans every kind over a skewed store.
func (s *Store) KeywordSearchPerKind(query, kind, subject string, limit, perKind int) []Item {
	items, _, _, _ := s.keywordSearch("", query, kind, subject, limit, perKind)
	return items
}

// keywordSearch is KeywordSearchWithTrigramHit that also returns the query
// tokens and the distinctiveness weights the ranking used.
func (s *Store) keywordSearch(userHash, query, kind, subject string, limit, perKind int) (items []Item, trigramHit bool, toks []string, weights map[string]float64) {
	return s.keywordSearchOpt(userHash, query, kind, subject, limit, perKind, false)
}

// keywordSearchOpt is keywordSearch that, with ownOnly, ranks only the private
// items userHash owns.
func (s *Store) keywordSearchOpt(userHash, query, kind, subject string, limit, perKind int, ownOnly bool) (items []Item, trigramHit bool, toks []string, weights map[string]float64) {
	q := NormalizeQuery(query)
	toks = KwTokens(q)

	s.mu.RLock()
	cands := make([]Item, 0, len(s.cells))
	var own []Item // the caller's own private items, which no index holds
	for _, it := range s.cells {
		if !it.IsActive() {
			continue
		}
		// A private item is a lexical search candidate only for its owner. The
		// shared search names no caller, so it never sees one; KeywordSearchForUser
		// does.
		private := IsPrivateItem(it)
		if private && !VisibleToUser(it, userHash) {
			continue
		}
		if ownOnly && !private {
			continue
		}
		if kind != "" && it.Kind != kind {
			continue
		}
		if kind == "" && s.cfg.Vocabulary.IsNonSearchable(it.Kind) {
			continue
		}
		if subject != "" && !strings.EqualFold(it.Subject, subject) {
			continue
		}
		if private {
			own = append(own, it.Clone())
			continue
		}
		cands = append(cands, it.Clone())
	}
	idx := s.kw
	vocab := s.vocab
	s.mu.RUnlock()

	// Distinctiveness weights are computed once per query, against the corpus
	// size before the vocabulary restriction below, which is a prefilter over the
	// same corpus, not a smaller one (see tokenWeights).
	weights = tokenWeights(toks, vocab, len(cands)+len(own))

	// Restrict the scan to items the vocabulary says could match a query token.
	// An item outside the set would score matched=0 and fail the coverage gate
	// anyway, so results do not change (see vocabIndex.candidateIDsForTokens).
	if vocab != nil && len(toks) > 0 && len(cands) > 0 {
		restrict := vocab.CandidateIDsForTokens(toks)
		if restrict != nil {
			filtered := cands[:0]
			for _, it := range cands {
				if _, ok := restrict[it.ID]; ok {
					filtered = append(filtered, it)
				}
			}
			cands = filtered
		}
	}

	cands = append(cands, own...)

	var trigramCands map[string]struct{}
	if q != "" && idx != nil {
		if ids := idx.Search(q); len(ids) > 0 {
			trigramCands = make(map[string]struct{}, len(ids))
			for _, id := range ids {
				trigramCands[id] = struct{}{}
			}
		}
	}

	items, trigramHit = rankAndLimitPerKind(q, toks, cands, idx, trigramCands, weights, limit, perKind)
	return items, trigramHit, toks, weights
}

// MarkSuperseded retires id's cell as superseded by byID, in place. Unlike
// Supersede it inserts no new cell: byID must already name an existing cell
// (ConsolidateMemory's cluster keeper). The link is recorded on the keeper's
// Supersedes, so a reader of byID finds every cell it absorbed. id's cell
// stays in the map with status superseded and leaves the lexical indexes. It
// returns false, writing nothing, when id or byID is empty, equal, or unknown.
func (s *Store) MarkSuperseded(id, byID string) bool {
	if id == "" || byID == "" || id == byID {
		return false
	}

	s.mu.Lock()
	old, ok := s.cells[id]
	if !ok {
		s.mu.Unlock()
		return false
	}
	keeper, ok := s.cells[byID]
	if !ok {
		s.mu.Unlock()
		return false
	}

	nowMeta := metav1.NewTime(time.Now())

	old.Status = "superseded"
	old.UpdatedAt = nowMeta
	s.cells[id] = old
	if s.kw != nil {
		s.kw.Remove(id)
	}
	if s.vocab != nil {
		s.vocab.Remove(id)
	}

	if !ContainsString(keeper.Supersedes, id) {
		keeper.Supersedes = append(keeper.Supersedes, id)
		keeper.UpdatedAt = nowMeta
		s.cells[byID] = keeper
	}

	s.dirty()
	s.signal()
	s.mu.Unlock()

	// Same two notifications as Supersede: the retired cell leaves the live view
	// (delete) and the keeper's changed Supersedes is republished (upsert).
	s.notifyChange("delete", nil, id, old.Kind)
	keeperClone := keeper.Clone()
	s.notifyChange("upsert", &keeperClone, "", keeperClone.Kind)
	return true
}

// DefaultHypothesisStaleAfter and DefaultUnknownStaleAfter are the deadlines
// stamped onto a model-authored hypothesis or unknown that carries none: the
// a day and a week.
const (
	DefaultHypothesisStaleAfter = 24 * time.Hour
	DefaultUnknownStaleAfter    = 7 * 24 * time.Hour
)

// validThoughtLinkRelations is the closed set of ThoughtLink.Relation values
// AddLink accepts; it mirrors the vocabulary on memoryv1.ThoughtLink.Relation.
// A relation outside it can still be written directly through Item.LinksSet
// (a host's own deterministic links), so the enum applies only at the
// model-authored boundary.
var validThoughtLinkRelations = map[string]bool{
	"triggers":        true,
	"caused_by":       true,
	"correlates_with": true,
	"precedes":        true,
	"explains":        true,
	"contradicts":     true,
}

// ValidThoughtLinkRelationsOrdered mirrors validThoughtLinkRelations in a
// fixed order for rejection messages, since map order is undefined.
var ValidThoughtLinkRelationsOrdered = []string{
	"triggers", "caused_by", "correlates_with", "precedes", "explains", "contradicts",
}

// MaxModelLinks caps the ThoughtLinks one cell accumulates through AddLink.
// Past the cap a link is admitted only if its Weight out-weighs the
// lowest-weight link, which is evicted. Replacing an existing (ToID, Relation)
// pair is exempt.
const MaxModelLinks = 12

// RefutedDataKey is the spec.data entry Refute writes (a RefutedRecord as
// base64 JSON).
const RefutedDataKey = "refuted"

// ValidateBase enforces the writing rules that hold for every item and every
// host; the host's own rules (Config.Validate, Vocabulary) come on top. It
// returns a message describing the violation, or "" when the item is valid.
func (s *Store) ValidateBase(item Item) string { return ValidateBase(s.cfg.Vocabulary, item) }

// ValidateBase is the Store method of the same name for a caller that has a
// vocabulary but no store.
func ValidateBase(v Vocabulary, item Item) string {
	if strings.TrimSpace(item.Kind) == "" {
		return "kind is required"
	}
	if v.IsRejected(item.Kind) {
		return "kind " + item.Kind + " cannot be written by a model-authored upsert"
	}
	if strings.TrimSpace(item.Content) == "" {
		return "content is required"
	}
	if strings.TrimSpace(item.Scope) == "" {
		return "scope is required"
	}
	if item.Origin != "" && !validOrigin(item.Origin) {
		return "origin must be one of observed, derived, human, verified"
	}
	// A stable fact is one the writer is sure of.
	if item.Kind == KindStableFact && item.Confidence != "high" && !v.FactConfidenceOptional {
		return "stable_fact items must have confidence=high"
	}
	// A hypothesis, an unknown and a mind map decay: they must say when.
	if (item.Kind == KindHypothesis || item.Kind == KindUnknown || item.Kind == "mindmap") && (item.StaleAfter == nil || item.StaleAfter.IsZero()) {
		return "hypothesis, unknown and mindmap items must have stale_after set"
	}
	return ""
}

// SetChangeHook installs (or clears with nil) the callback run after a real
// memory item mutation, the bridge to the SSE "memory in use" stream. op is
// "upsert" (item set, id empty) or "delete" (item nil, id set); kind is the
// item's category, which for a delete is the only way it reaches the hook. It
// never fires on Mirror's no-op branch, and AddLink's link-only edits are not
// published.
func (s *Store) SetChangeHook(fn func(op string, item *Item, id string, kind string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changeHook = fn
}

// notifyChange fires the change hook (if any) with a defensive clone of item,
// after the lock is released. Changes to ids the host calls quiet (the
// machine-refreshed state notes, rewritten every few seconds) are dropped:
// the stream carries only information about memory actually changing.
func (s *Store) notifyChange(op string, item *Item, id string, kind string) {
	targetID := id
	if item != nil {
		targetID = item.ID
		kind = item.Kind
	}
	if s.cfg.Quiet != nil && s.cfg.Quiet(targetID) {
		return
	}
	if s.cfg.Published != nil {
		s.cfg.Published()
	}
	s.mu.RLock()
	hook := s.changeHook
	s.mu.RUnlock()
	if hook == nil {
		return
	}
	if item != nil {
		clone := item.Clone()
		item = &clone
	}
	hook(op, item, id, kind)
}

// SetHitHook installs (or clears with nil) the callback invoked when a memory
// search surfaces results that reach a downstream consumer: itemIDs are the
// ids actually used, query the search string, kind the kind filter applied,
// and kinds each item's own kind, parallel to itemIDs (nil when the caller has
// none).
func (s *Store) SetHitHook(fn func(itemIDs []string, query, kind string, kinds []string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hitHook = fn
}

// NotifyHit fires the hit hook with a defensive copy of itemIDs (and kinds,
// when its length matches itemIDs). An empty hit is not an event; a kinds
// slice of the wrong length is dropped rather than forwarded misaligned.
func (s *Store) NotifyHit(itemIDs []string, query, kind string, kinds []string) {
	s.mu.RLock()
	hook := s.hitHook
	s.mu.RUnlock()
	if hook == nil || len(itemIDs) == 0 {
		return
	}
	ids := append([]string(nil), itemIDs...)
	var ks []string
	if len(kinds) == len(itemIDs) {
		ks = append([]string(nil), kinds...)
	}
	hook(ids, query, kind, ks)
}

// CloneInto copies the items into a new store guarded by mu, for a deep copy of
// the host's state. A cfg with no vocabulary inherits the source's, and the
// copy's lexical indexes are built from the copied items.
//
// CloneInto takes no lock on the source: the caller holds the source's lock,
// read or write, for the whole call. That is what lets a host that shares one
// mutex between the store and its other state clone both under it without
// deadlocking on a second acquisition. A caller that does not share the lock and
// clones while writers run must take the store's own lock around the call (see
// WithLocker); an unguarded concurrent clone is a data race.
func (s *Store) CloneInto(l Locker, cfg Config) *Store {
	if cfg.Vocabulary.isZero() {
		cfg.Vocabulary = s.cfg.Vocabulary
	}
	out := NewStore(WithLocker(l), WithConfig(cfg))
	for k, v := range s.cells {
		c := v.Clone()
		out.cells[k] = c
		out.indexLocked(c)
	}
	return out
}

// ItemsLocked is a copy of every item, private ones included. The caller holds
// the lock (the host derives a summary of the whole set from it).
func (s *Store) ItemsLocked() []Item { return cloneMemoryItems(s.cells) }

// CountActive is the number of active items, private ones included.
func (s *Store) CountActive() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, it := range s.cells {
		if it.IsActive() {
			n++
		}
	}
	return n
}

// LenLocked is the number of items. The caller holds the lock.
func (s *Store) LenLocked() int { return len(s.cells) }

// Mutate applies fn to the item with id under the lock. When fn reports a
// change the item's UpdatedAt is stamped and the host told to rebuild what it
// derives. It reports whether the item exists. For a host-owned amendment of a
// single field (an item's evidence refs or tags); lexical
// fields must not be changed through it.
func (s *Store) Mutate(id string, fn func(item *Item) (changed bool)) (found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.cells[id]
	if !ok {
		return false
	}
	if fn(&item) {
		item.UpdatedAt = metav1.NewTime(time.Now())
		s.cells[id] = item
		s.dirty()
		s.signal()
	}
	return true
}

// Removed names an item taken out of the store.
type Removed struct{ ID, Kind string }

// ForgetLocked deletes every item match accepts, with its index entries, and
// returns what it removed. The caller holds the lock and, once it has released
// it, hands the result to PublishRemoved.
func (s *Store) ForgetLocked(match func(Item) bool) []Removed {
	var gone []Removed
	for id, it := range s.cells {
		if !match(it) {
			continue
		}
		delete(s.cells, id)
		s.kw.Remove(id)
		s.vocab.Remove(id)
		gone = append(gone, Removed{ID: id, Kind: it.Kind})
	}
	return gone
}

// PublishRemoved announces items ForgetLocked removed, outside the lock.
func (s *Store) PublishRemoved(gone []Removed) {
	for _, d := range gone {
		s.notifyChange("delete", nil, d.ID, d.Kind)
	}
}

// Put stores item exactly as given: no validation, no origin or timestamp
// stamping and no merge with an existing item, but with its lexical cache and
// index entries built. For a caller that owns every field of the item (the
// host's seeding of state from elsewhere).
func (s *Store) Put(item Item) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buildCache(&item)
	s.cells[item.ID] = item.Clone()
	s.dirty()
	s.indexLocked(item)
	s.signal()
}

// Indexed reports whether id has entries in the lexical index.
func (s *Store) Indexed(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.kw.HasItem(id)
}

// ExactMatches returns the active, non-private items whose whole title, or the
// value of whose service/ tag, equals query once both are normalised: the items
// a caller who typed a name means. kind and subject narrow the match as they do
// in a keyword search; empty means any. The newest come first.
func (s *Store) ExactMatches(query, kind, subject string) []Item {
	q := NormalizeQuery(query)
	if q == "" {
		return nil
	}
	s.mu.RLock()
	var out []Item
	for _, it := range s.cells {
		if !it.IsActive() || IsPrivateItem(it) {
			continue
		}
		if kind != "" && it.Kind != kind {
			continue
		}
		if kind == "" && s.cfg.Vocabulary.IsNonSearchable(it.Kind) {
			continue
		}
		if subject != "" && !strings.EqualFold(it.Subject, subject) {
			continue
		}
		title := it.lex.titleNorm
		if !it.lex.built {
			title = NormalizeQuery(it.Title)
		}
		if title == q {
			out = append(out, it.Clone())
			continue
		}
		if svc, ok := ServiceTagValue(it); ok && NormalizeQuery(svc) == q {
			out = append(out, it.Clone())
		}
	}
	s.mu.RUnlock()
	sortMemoryItems(out)
	return out
}

// haystackOf is the text an item is searched by besides its title: its content,
// widened with its id unless the host says the id is not worth searching.
func (s *Store) haystackOf(it Item) string {
	if s.cfg.SkipIDText != nil && s.cfg.SkipIDText(it.ID) {
		return it.Content
	}
	return idHaystack(it)
}

// buildCache (re)computes the item's lexical cache from the haystack the store
// searches it by.
func (s *Store) buildCache(item *Item) { buildLexicalCacheText(item, s.haystackOf(*item)) }

// KeywordSearchOwn is KeywordSearch over the private items owned by userHash and
// nothing else, so shared items that score higher cannot push them out of the
// limit. With no userHash it returns nil.
func (s *Store) KeywordSearchOwn(userHash, query, kind string, limit int) []Item {
	if userHash == "" {
		return nil
	}
	items, _, _, _ := s.keywordSearchOpt(userHash, query, kind, "", limit, 0, true)
	return items
}
