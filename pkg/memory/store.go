package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
	ixray "github.com/triageagent-dev/x-ray/v2/internal/xray"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore/index"
	"github.com/triageagent-dev/x-ray/v2/pkg/xray"
)

// Store is an in-process X-Ray memory store. It is safe for concurrent use.
type Store struct {
	m      *ixray.MemoryStore
	origin string
	path   string
	saveMu sync.Mutex
}

var _ xray.Memory = (*Store)(nil)

type config struct {
	endpoint      string
	model         string
	dims          int
	maxInputChars int
	path          string
	origin        string
	dedup         bool
	shaping       bool
}

// Option configures New.
type Option func(*config)

// WithEmbeddings turns on semantic search. endpoint is an OpenAI-compatible
// /v1/embeddings server (llama.cpp, Ollama, vLLM, LM Studio or a hosted one),
// model its model name and dims the vector size to truncate to (256 suits
// nomic). Items are embedded when written, so a write waits for the endpoint.
func WithEmbeddings(endpoint, model string, dims int) Option {
	return func(c *config) { c.endpoint, c.model, c.dims = endpoint, model, dims }
}

// WithMaxInputChars caps the text sent to the embedder per item. The default is
// 8192, which fits a 2048-token model.
func WithMaxInputChars(n int) Option { return func(c *config) { c.maxInputChars = n } }

// WithFile keeps the store in a JSON file: loaded on New, rewritten after every
// change. The file holds every item, private ones included, so give it the
// permissions you would give the data (it is written 0600).
func WithFile(path string) Option { return func(c *config) { c.path = path } }

// WithOrigin sets who the program's writes are attributed to. The default is
// xray.OriginDerived: written by a model. Use xray.OriginHuman for a store that
// holds what a person told the agent, or xray.OriginObserved for facts the code
// measured. A derived write never replaces a human item.
func WithOrigin(origin string) Option { return func(c *config) { c.origin = origin } }

// WithWriteDedup turns merging of restated facts on write on or off (default on).
func WithWriteDedup(on bool) Option { return func(c *config) { c.dedup = on } }

// WithRecallShaping turns collapsing of near-duplicates in search results on or
// off (default on).
func WithRecallShaping(on bool) Option { return func(c *config) { c.shaping = on } }

// New creates a store. With WithFile it loads the file if it exists.
func New(ctx context.Context, opts ...Option) (*Store, error) {
	cfg := config{origin: xray.OriginDerived, dedup: true, shaping: true, maxInputChars: 8192}
	for _, o := range opts {
		o(&cfg)
	}
	switch cfg.origin {
	case xray.OriginObserved, xray.OriginDerived, xray.OriginHuman, xray.OriginVerified:
	default:
		return nil, fmt.Errorf("memory: unknown origin %q", cfg.origin)
	}

	var m *ixray.MemoryStore
	if cfg.endpoint != "" {
		if cfg.dims <= 0 {
			cfg.dims = 256
		}
		emb, err := index.NewEmbedder(ctx, cfg.endpoint, cfg.model, "", "", cfg.dims, cfg.maxInputChars, 0, zerolog.Nop())
		if err != nil {
			return nil, fmt.Errorf("memory: embedder: %w", err)
		}
		m = ixray.NewMemoryStoreWithEmbedder(emb, zerolog.Nop())
	} else {
		m = ixray.NewMemoryStore()
	}
	m.SetWriteDedup(cfg.dedup)
	m.SetRecallShaping(cfg.shaping)

	s := &Store{m: m, origin: cfg.origin, path: cfg.path}
	if err := s.load(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Close writes the file, if there is one. The store stays usable.
func (s *Store) Close() error { return s.save() }

func (s *Store) load(ctx context.Context) error {
	if s.path == "" {
		return nil
	}
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("memory: read %s: %w", s.path, err)
	}
	var items []*ixray.MemoryItem
	if err := json.Unmarshal(raw, &items); err != nil {
		return fmt.Errorf("memory: decode %s: %w", s.path, err)
	}
	for _, it := range items {
		s.m.Restore(ctx, it)
	}
	return nil
}

// save writes the file atomically: a crash leaves the previous file intact.
func (s *Store) save() error {
	if s.path == "" {
		return nil
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	raw, err := json.Marshal(s.m.ExportAll())
	if err != nil {
		return fmt.Errorf("memory: encode: %w", err)
	}
	if dir := filepath.Dir(s.path); dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("memory: %w", err)
		}
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".xray-memory-*")
	if err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("memory: %w", err)
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("memory: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	return os.Rename(tmp.Name(), s.path)
}

func (s *Store) ctx(ctx context.Context) context.Context { return ixray.WithOrigin(ctx, s.origin) }

func refuse(tool, msg string) error { return &xray.ToolError{Tool: tool, Message: msg} }

func toItem(it *ixray.MemoryItem) xray.Item {
	out := xray.Item{
		ID: it.ID, Kind: it.Kind, Scope: it.Scope, Subject: it.Subject,
		Title: it.Title, Content: it.Content, Confidence: it.Confidence, Tags: it.Tags,
		Status: it.Status, EventDate: it.EventDate, StaleAfter: it.StaleAfter,
		CreatedAt: it.CreatedAt, UpdatedAt: it.UpdatedAt, Origin: it.Origin,
		EvidenceRefs: it.EvidenceRefs, Supersedes: it.Supersedes, Via: it.Via, Collapsed: it.Collapsed,
	}
	for _, l := range it.Links {
		out.Links = append(out.Links, xray.Link{ToID: l.ToID, Relation: l.Relation, Weight: l.Weight, EvidenceRefs: l.EvidenceRefs})
	}
	return out
}

func toItems(items []*ixray.MemoryItem) []xray.Item {
	out := make([]xray.Item, 0, len(items))
	for _, it := range items {
		out = append(out, toItem(it))
	}
	return out
}

// visible reports whether the caller, naming subject, may see the item.
func visible(it *ixray.MemoryItem, subject string) bool {
	return it != nil && (it.Scope != xray.ScopePrivate || it.Subject == subject)
}

// Search ranks items by relevance to query. Without a subject only shared items
// are searched; a subject also reaches that subject's private items.
func (s *Store) Search(ctx context.Context, query string, opts ...xray.QueryOption) ([]xray.Item, error) {
	q := xray.ResolveQuery(opts...)
	if msg := ixray.ValidateSearchKind(q.Kind); msg != "" {
		return nil, refuse("search_memory", msg)
	}
	if q.Limit <= 0 {
		q.Limit = ixray.DefaultMCPSearchMemoryMaxLimit
	}
	return toItems(s.m.Search(s.ctx(ctx), query, q.Kind, q.Subject, q.Limit)), nil
}

// List browses items by kind and subject, unranked.
func (s *Store) List(_ context.Context, opts ...xray.QueryOption) ([]xray.Item, error) {
	q := xray.ResolveQuery(opts...)
	if msg := ixray.ValidateSearchKind(q.Kind); msg != "" {
		return nil, refuse("list_memory", msg)
	}
	if q.Limit <= 0 {
		q.Limit = ixray.DefaultMCPListMemoryMaxLimit
	}
	items := s.m.FilterItems(q.Subject, q.Kind)
	if len(items) > q.Limit {
		items = items[:q.Limit]
	}
	return toItems(items), nil
}

// Fetch returns the items with the given ids, in that order. A missing id, or a
// private item the subject does not own, is left out.
func (s *Store) Fetch(_ context.Context, ids []string, opts ...xray.QueryOption) ([]xray.Item, error) {
	q := xray.ResolveQuery(opts...)
	out := make([]xray.Item, 0, len(ids))
	for _, id := range ids {
		if it := s.m.GetItem(id); visible(it, q.Subject) {
			out = append(out, toItem(it))
		}
	}
	return out, nil
}

// Get returns one item, or an error satisfying xray.IsNotFound.
func (s *Store) Get(ctx context.Context, id string, opts ...xray.QueryOption) (*xray.Item, error) {
	items, err := s.Fetch(ctx, []string{id}, opts...)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, refuse("fetch_memory", "not found")
	}
	return &items[0], nil
}

// Summary counts the shared items by kind.
func (s *Store) Summary(context.Context) (*xray.Summary, error) {
	items := s.m.AllItems("")
	out := &xray.Summary{Total: len(items), ByKind: map[string]int{}}
	for _, it := range items {
		out.ByKind[it.Kind]++
	}
	return out, nil
}

// Recent returns the newest episodic memories of a subject, newest first.
func (s *Store) Recent(_ context.Context, opts ...xray.QueryOption) ([]xray.Episode, error) {
	q := xray.ResolveQuery(opts...)
	if q.Limit <= 0 {
		q.Limit = ixray.DefaultMCPRecentMemoryMaxLimit
	}
	items := s.m.ListRecent("episodic", xray.ScopePrivate, q.Subject, q.Limit)
	out := make([]xray.Episode, 0, len(items))
	for _, it := range items {
		out = append(out, xray.Episode{ID: it.ID, Title: it.Title, Content: it.Content, CreatedAt: it.CreatedAt.UTC().Format(time.RFC3339)})
	}
	return out, nil
}

func dataFor(s *Store, w xray.Write) map[string]mcell.DataEntry {
	if len(w.Image) > 0 {
		mime := w.ImageMIME
		if mime == "" {
			mime = "image/jpeg"
		}
		return map[string]mcell.DataEntry{"image": {Bytes: encodeBase64(w.Image), Mime: mime}}
	}
	if w.ID != "" {
		if existing := s.m.GetItem(w.ID); existing != nil && existing.Data != nil {
			return existing.Data // an update that names no image keeps the one it has
		}
	}
	return nil
}

func checkWrite(tool string, w xray.Write) error {
	if msg := ixray.ValidateWrite(w.Kind, w.Confidence); msg != "" {
		return refuse(tool, msg)
	}
	if w.Scope == xray.ScopePrivate && w.Subject == "" {
		return refuse(tool, "subject is required when scope=private")
	}
	if w.EventDate != "" && !memstore.ValidEventDate(w.EventDate) {
		return refuse(tool, "event_date must be RFC3339 (e.g. 2026-06-15T09:00:00Z) or YYYY-MM-DD")
	}
	return nil
}

// Upsert creates an item, or updates the one named by w.ID. A write that
// restates a fact already held comes back with Status "merged".
func (s *Store) Upsert(ctx context.Context, w xray.Write) (*xray.WriteResult, error) {
	if err := checkWrite("upsert_memory", w); err != nil {
		return nil, err
	}
	var opts []ixray.UpsertOption
	if w.EvidenceRefs != nil {
		opts = append(opts, ixray.WithEvidenceRefs(w.EvidenceRefs))
	}
	res, errMsg := s.m.Upsert(s.ctx(ctx), w.ID, w.Kind, w.Scope, w.Title, w.Content, w.Subject, w.Confidence, w.EventDate, dataFor(s, w), w.Tags, opts...)
	if errMsg != "" {
		return nil, refuse("upsert_memory", errMsg)
	}
	out := &xray.WriteResult{ID: res.ID, Kind: w.Kind, Status: res.Status, Title: w.Title}
	if res.Status == "merged" {
		out.MergedInto = res.ID
	}
	return out, s.save()
}

// Delete removes an item. A private item needs its subject.
func (s *Store) Delete(_ context.Context, id string, opts ...xray.QueryOption) error {
	q := xray.ResolveQuery(opts...)
	it := s.m.GetItem(id)
	if it == nil {
		return refuse("delete_memory", "not found")
	}
	if !visible(it, q.Subject) {
		return refuse("delete_memory", "forbidden: private item belongs to another subject")
	}
	s.m.Delete(id)
	return s.save()
}

// Refute retires an item that is wrong, records the reason and evidence on it,
// and retires every active derived item that cites it.
func (s *Store) Refute(ctx context.Context, id, reason string, evidence []string, opts ...xray.QueryOption) (*xray.RefuteResult, error) {
	if reason == "" {
		return nil, errors.New("xray: refute needs a reason")
	}
	q := xray.ResolveQuery(opts...)
	if !visible(s.m.GetItem(id), q.Subject) {
		return nil, refuse("refute_memory", "not found")
	}
	changed, _ := s.m.Refute(s.ctx(ctx), id, reason, evidence)
	out := &xray.RefuteResult{ID: id, Status: "stale", Cascaded: []string{}}
	for _, c := range changed[1:] {
		out.Cascaded = append(out.Cascaded, c.ID)
	}
	return out, s.save()
}

// Expire retires an item that is out of date. Use Refute when it is wrong.
func (s *Store) Expire(_ context.Context, id string, opts ...xray.QueryOption) error {
	q := xray.ResolveQuery(opts...)
	if !visible(s.m.GetItem(id), q.Subject) {
		return refuse("expire_memory", "not found")
	}
	s.m.Expire(id)
	return s.save()
}

// Supersede writes w as the replacement of oldID and retires the old item. A
// derived replacement cannot supersede a human item.
func (s *Store) Supersede(ctx context.Context, oldID string, w xray.Write) (*xray.SupersedeResult, error) {
	if err := checkWrite("supersede_memory", w); err != nil {
		return nil, err
	}
	if !visible(s.m.GetItem(oldID), w.Subject) {
		return nil, refuse("supersede_memory", "not found")
	}
	var opts []ixray.UpsertOption
	if w.EvidenceRefs != nil {
		opts = append(opts, ixray.WithEvidenceRefs(w.EvidenceRefs))
	}
	changed, errMsg := s.m.Supersede(s.ctx(ctx), oldID, w.ID, w.Kind, w.Scope, w.Title, w.Content, w.Subject, w.Confidence, w.Tags, opts...)
	if errMsg != "" {
		return nil, refuse("supersede_memory", errMsg)
	}
	return &xray.SupersedeResult{ID: changed[0].ID, Superseded: oldID, Status: "created"}, s.save()
}

// Consolidate reports near-duplicate facts. With apply it retires the
// restatements. WithSubject restricts the sweep to one subject.
func (s *Store) Consolidate(_ context.Context, apply bool, opts ...xray.QueryOption) (*xray.ConsolidationReport, error) {
	q := xray.ResolveQuery(opts...)
	rep, _ := s.m.ConsolidateMemory(q.Subject, apply)
	out := &xray.ConsolidationReport{Scanned: rep.Scanned, Superseded: rep.Superseded, Applied: rep.Applied}
	for _, g := range rep.Groups {
		out.Groups = append(out.Groups, xray.ConsolidationGroup{Kind: g.Kind, Keep: g.Keep, KeepTitle: g.KeepTitle, Superseded: g.Superseded, Scores: g.Scores})
	}
	if apply {
		return out, s.save()
	}
	return out, nil
}

// Context follows links from an anchor (an id, a subject or part of a title)
// with decaying activation, and returns the anchor items and the neighbourhood
// they reach. maxDepth 0 uses 3, and at most 8 is followed.
func (s *Store) Context(_ context.Context, anchor string, maxDepth int) ([]xray.ContextItem, error) {
	if maxDepth <= 0 {
		maxDepth = 3
	}
	anchors, activated := s.m.Context(anchor, maxDepth)
	out := make([]xray.ContextItem, 0, len(anchors)+len(activated))
	for _, it := range anchors {
		out = append(out, xray.ContextItem{ID: it.ID, Kind: it.Kind, Subject: it.Subject, Title: it.Title, Content: it.Content, Confidence: it.Confidence, IsAnchor: true})
	}
	for _, a := range activated {
		it := a.Item
		out = append(out, xray.ContextItem{ID: it.ID, Kind: it.Kind, Subject: it.Subject, Title: it.Title, Content: it.Content, Confidence: it.Confidence, Score: a.Score})
	}
	return out, nil
}
