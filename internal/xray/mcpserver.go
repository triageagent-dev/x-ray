package xray

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/rs/zerolog"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
	msnap "github.com/triageagent-dev/x-ray/v2/internal/codegraph/mcpserver"
	"github.com/triageagent-dev/x-ray/v2/pkg/memstore"
)

// writeScopeTools require scopeWrite; everything else requires scopeRead.
var writeScopeTools = map[string]bool{
	"upsert_memory":      true,
	"delete_memory":      true,
	"refute_memory":      true,
	"consolidate_memory": true,
	"expire_memory":      true,
	"supersede_memory":   true,
}

// memberWriteTools are the write tools a token holding scopeWriteOwn may call.
// consolidate_memory is not among them: it writes shared cells.
var memberWriteTools = map[string]bool{
	"upsert_memory":    true,
	"delete_memory":    true,
	"refute_memory":    true,
	"expire_memory":    true,
	"supersede_memory": true,
}

type memberOwnerKey struct{}

// withMemberOwner binds a request to one owner hash: a member token never acts
// on another person's private cells, whatever subject the caller passes (ADR 067).
func withMemberOwner(ctx context.Context, ownerHash string) context.Context {
	return context.WithValue(ctx, memberOwnerKey{}, ownerHash)
}

// memberOwner returns the owner hash of an owner-bound request.
func memberOwner(ctx context.Context) (string, bool) {
	h, ok := ctx.Value(memberOwnerKey{}).(string)
	return h, ok
}

// boundSubject is the subject a read uses: an owner-bound caller's non-empty
// subject becomes their own hash, an empty one stays empty (shared cells only).
func boundSubject(ctx context.Context, subject string) string {
	if owner, ok := memberOwner(ctx); ok && subject != "" {
		return owner
	}
	return subject
}

// memberPrivateLimit caps the private cells, in any status, one member may hold.
// Only delete_memory frees room: expired and superseded cells still count.
const memberPrivateLimit = 1000

// ownsPrivate reports whether it is a private cell owned by owner.
func ownsPrivate(it *MemoryItem, owner string) bool {
	return it != nil && isPrivateScope(it.Scope) && it.Subject == owner
}

const (
	errMembersPrivateOnly = `{"error":"members can write private notes only"}`
	errMemberSharedTarget = `{"error":"forbidden","reason":"members can change their own private notes only"}`
)

// DefaultMCPListMemoryMaxLimit / DefaultMCPSearchMemoryMaxLimit are the
// fallback hard result-count ceilings for list_memory/search_memory when the
// caller (main.go) doesn't override them via --mcp-list-memory-max-limit /
// --mcp-search-memory-max-limit. Applied regardless of what limit the MCP
// caller requests — a misbehaving or adversarial client (the limit is fully
// caller-controlled otherwise) cannot force a full-store dump (ADR-051).
const (
	DefaultMCPListMemoryMaxLimit   = 20
	DefaultMCPSearchMemoryMaxLimit = 10
	DefaultMCPRecentMemoryMaxLimit = 20
)

// fullKindVocabulary documents every ADR-020 memory kind for MCP tool
// descriptions — not just the 5 kinds X-Ray's own content happens to
// use, so external agents know the SRE-flavored kinds are
// valid too.
const fullKindVocabulary = "stable_fact, recent_change, system_identity, event, note, active_concern, operational_pattern, watch_item, hypothesis, unknown"

// NewMCPServer returns an HTTP handler exposing memory tools.
// syncer and namespace are optional: if syncer is non-nil, upsert/delete also
// persist changes to AwarenessMemoryCell CRs in the cluster.
// listMemoryMaxLimit/searchMemoryMaxLimit/recentMemoryMaxLimit are hard
// result-count ceilings for list_memory/search_memory/recent_memory
// (ADR-051); pass <= 0 to use the Default* constants.
//
// Authentication accepts two credential types, tried in order:
//  1. an X-Ray-issued OAuth JWT (when auth != nil) — carries per-user scopes;
//  2. the static shared Bearer secret from getToken() — treated as full scope.
//
// At least one must be configured; otherwise the endpoint fails closed (503).
func NewMCPServer(memory *MemoryStore, logger zerolog.Logger, getToken func() string, auth *mcpAuthorizer, syncer *CellSyncer, namespace string, memMapsDeps *msnap.Deps, listMemoryMaxLimit, searchMemoryMaxLimit, recentMemoryMaxLimit int) http.Handler {
	if listMemoryMaxLimit <= 0 {
		listMemoryMaxLimit = DefaultMCPListMemoryMaxLimit
	}
	if searchMemoryMaxLimit <= 0 {
		searchMemoryMaxLimit = DefaultMCPSearchMemoryMaxLimit
	}
	if recentMemoryMaxLimit <= 0 {
		recentMemoryMaxLimit = DefaultMCPRecentMemoryMaxLimit
	}
	s := server.NewMCPServer("xray-mcp", "1.0.0")

	// get_memory — orientation snapshot: counts by kind and the memory summary
	// text. No items returned. Same shape as the voice tool of the same name
	// (ADR-051).
	s.AddTool(mcp.NewTool("get_memory",
		mcp.WithDescription("Get an orientation summary of stored memory: item counts by kind and the memory summary text. Call this for a quick overview before deciding whether to list or search."),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		items := memory.AllItems("")
		if owner, ok := memberOwner(ctx); ok {
			// shared cells plus the caller's own private ones, never another's
			items = append(items, memory.List("", scopePrivate, owner, 0)...)
		}
		counts := map[string]int{}
		for _, it := range items {
			counts[it.Kind]++
		}
		b, _ := json.Marshal(map[string]any{
			"total":   len(items),
			"by_kind": counts,
		})
		logger.Debug().Int("total", len(items)).Msg("mcp get_memory")
		return mcp.NewToolResultText(string(b)), nil
	})

	// fetch_memory — exact retrieval of one or more items by id, in the order
	// requested (ADR-051). Replaces get_memory_context's id-mode (the
	// subject-listing mode folds into list_memory, redundant otherwise) and
	// voice's get_pinned_items (batched fetch of several known ids). A missing
	// id or a private item whose subject doesn't match the caller's supplied
	// subject is silently omitted from the result, not an error — so one bad
	// id in a batch doesn't fail the whole call, and ownership isn't leaked by
	// an error message that confirms existence.
	s.AddTool(mcp.NewTool("fetch_memory",
		mcp.WithDescription("Fetch the full content of one or more memory items by ID, in the order given. Use this when you already know the exact id(s) — from a prior list_memory/search_memory result, or a caller-tracked set (e.g. pinned items). A missing id or a private item you don't own is simply absent from the result, not an error."),
		mcp.WithArray("ids", mcp.Description("Item IDs to fetch"), mcp.Required()),
		mcp.WithString("subject", mcp.Description("Your own subject — required to fetch a private item you own; omitted or mismatched private items are skipped")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		subject, _ := args["subject"].(string)
		subject = boundSubject(ctx, subject)
		_, bound := memberOwner(ctx)
		var ids []string
		if raw, ok := args["ids"].([]any); ok {
			for _, v := range raw {
				if s, ok := v.(string); ok && s != "" {
					ids = append(ids, s)
				}
			}
		}
		items := make([]*MemoryItem, 0, len(ids))
		for _, id := range ids {
			it := memory.GetItem(id)
			if it == nil {
				continue
			}
			if isPrivateScope(it.Scope) && (it.Subject != subject || (bound && subject == "")) {
				continue
			}
			items = append(items, it)
		}
		logger.Debug().Int("requested", len(ids)).Int("found", len(items)).Msg("mcp fetch_memory")
		b, _ := json.MarshalIndent(map[string]any{"count": len(items), "items": items}, "", "  ")
		return mcp.NewToolResultText(string(b)), nil
	})

	// recent_memory — episodic items, newest-first by creation time (ADR-051).
	// A genuinely different shape from list_memory (ordering matters, kind is
	// implicitly episodic), not just a filter variant. Replaces voice's
	// get_recent_memory; now available on MCP too.
	s.AddTool(mcp.NewTool("recent_memory",
		mcp.WithDescription("Recall the most recent episodic memories (what happened in past sessions/conversations), newest first. Call this early when continuing a prior session or asked 'where were we'."),
		mcp.WithString("subject", mcp.Description("Your own subject — episodic memories are private, scoped to whoever wrote them")),
		mcp.WithNumber("limit", mcp.Description(fmt.Sprintf("How many recent memories to return (default %d, hard-capped at %d)", recentMemoryMaxLimit, recentMemoryMaxLimit))),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		subject, _ := args["subject"].(string)
		subject = boundSubject(ctx, subject)
		limit := recentMemoryMaxLimit
		if l, ok := args["limit"].(float64); ok && l > 0 {
			limit = int(l)
		}
		if limit > recentMemoryMaxLimit {
			limit = recentMemoryMaxLimit
		}
		items := memory.ListRecent("episodic", scopePrivate, subject, limit)
		type row struct {
			ID        string `json:"id"`
			Title     string `json:"title"`
			Content   string `json:"content"`
			CreatedAt string `json:"created_at"`
		}
		rows := make([]row, 0, len(items))
		for _, it := range items {
			rows = append(rows, row{ID: it.ID, Title: it.Title, Content: it.Content, CreatedAt: it.CreatedAt.UTC().Format(time.RFC3339)})
		}
		logger.Debug().Str("subject", subject).Int("results", len(rows)).Msg("mcp recent_memory")
		b, _ := json.Marshal(map[string]any{"count": len(rows), "items": rows})
		return mcp.NewToolResultText(string(b)), nil
	})

	s.AddTool(mcp.NewTool("list_memory",
		mcp.WithDescription("Browse memory items by structural filter (kind/subject), unranked. Supports kind-only and scope-only queries (e.g. scope=private with a subject). system_configuration is always excluded. For a private multi-tenant working set, pass subject as your own stable agent/user name — items written with the same subject and scope=private are only visible to callers who pass that subject."),
		mcp.WithString("kind", mcp.Description("Filter by kind: "+fullKindVocabulary)),
		mcp.WithString("subject", mcp.Description("Optional subject filter — pass your own name for private/working memory scoped to you")),
		mcp.WithNumber("limit", mcp.Description(fmt.Sprintf("Max results (default %d, hard-capped at %d)", listMemoryMaxLimit, listMemoryMaxLimit))),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		kind, _ := args["kind"].(string)
		subject, _ := args["subject"].(string)
		subject = boundSubject(ctx, subject)
		if msg := ValidateSearchKind(kind); msg != "" {
			return mcp.NewToolResultText(errJSON(msg)), nil
		}
		limit := listMemoryMaxLimit
		if l, ok := args["limit"].(float64); ok && l > 0 {
			limit = int(l)
		}
		if limit > listMemoryMaxLimit {
			limit = listMemoryMaxLimit
		}
		items := memory.FilterItems(subject, kind)
		if _, bound := memberOwner(ctx); bound && subject == "" {
			// an empty subject is the shared view: no private cell at all
			shared := items[:0]
			for _, it := range items {
				if !isPrivateItem(it.Kind, it.Scope) {
					shared = append(shared, it)
				}
			}
			items = shared
		}
		if len(items) > limit {
			items = items[:limit]
		}
		logger.Debug().Str("subject", subject).Str("kind", kind).Int("results", len(items)).Msg("mcp list_memory")
		b, _ := json.MarshalIndent(map[string]any{"subject": subject, "kind": kind, "count": len(items), "items": items}, "", "  ")
		return mcp.NewToolResultText(string(b)), nil
	})

	// search_memory — search the memory store by query. Named to match the voice
	// tool of the same name.
	searchMemory := func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		query, _ := args["query"].(string)
		subject, _ := args["subject"].(string)
		subject = boundSubject(ctx, subject)
		kind, _ := args["kind"].(string)
		// Private cells are tenant-isolated by subject (ADR-028/037): the store
		// hides them whenever subject is empty, so an unscoped search returns only
		// public knowledge. Setting subject scopes to one owner's cells.
		if msg := ValidateSearchKind(kind); msg != "" {
			return mcp.NewToolResultText(errJSON(msg)), nil
		}
		limit := searchMemoryMaxLimit
		if l, ok := args["limit"].(float64); ok && l > 0 {
			limit = int(l)
		}
		if limit > searchMemoryMaxLimit {
			limit = searchMemoryMaxLimit
		}
		items := memory.Search(ctx, query, kind, subject, limit)
		// A recall lights the recalled cells and their region in a connected UI
		// (a hit event). Only shared items are named: the stream reaches every
		// client, and a private item's id is not theirs to see.
		hitIDs, hitKinds := make([]string, 0, len(items)), make([]string, 0, len(items))
		for _, it := range items {
			if !isPrivateItem(it.Kind, it.Scope) {
				hitIDs, hitKinds = append(hitIDs, it.ID), append(hitKinds, it.Kind)
			}
		}
		if len(hitIDs) > 0 {
			memory.PublishHit(hitIDs, kind, hitKinds)
		}
		logger.Debug().Str("subject", subject).Str("query", query).Str("kind", kind).Int("results", len(items)).Msg("mcp search_memory")
		rendered := annotateEventTimes(items)
		b, _ := json.MarshalIndent(map[string]any{"query": query, "subject": subject, "count": len(rendered), "items": rendered}, "", "  ")
		return mcp.NewToolResultText(string(b)), nil
	}
	searchMemoryOpts := []mcp.ToolOption{
		mcp.WithDescription("Search the memory store by relevance to a query. Leave subject empty to search shared (public) knowledge across all subjects. Private cells (scope=private, e.g. personal notes or another agent's private working memory) are tenant-isolated and only returned when you set subject to their owner — pass your own stable agent/user name to reach your own private items."),
		mcp.WithString("query", mcp.Description("Search query"), mcp.Required()),
		mcp.WithString("subject", mcp.Description("Optional subject filter; REQUIRED to retrieve a private cell (set it to the owner)")),
		mcp.WithString("kind", mcp.Description("Filter by kind: "+fullKindVocabulary)),
		mcp.WithNumber("limit", mcp.Description(fmt.Sprintf("Max results (default %d, hard-capped at %d)", searchMemoryMaxLimit, searchMemoryMaxLimit))),
	}
	s.AddTool(mcp.NewTool("search_memory", searchMemoryOpts...), searchMemory)

	s.AddTool(mcp.NewTool("upsert_memory",
		mcp.WithDescription("Create or update a memory item. Pass id to update an existing item; omit to create new. For private, per-agent working memory shared across your own sessions, use scope=private with a stable subject (e.g. your agent's own name) — only callers passing that same subject can read it back via list_memory/search_memory."),
		mcp.WithString("id", mcp.Description("Existing item ID to update; omit to create a new item")),
		mcp.WithString("kind", mcp.Description("One of: "+fullKindVocabulary), mcp.Required()),
		mcp.WithString("scope", mcp.Description("Scope: 'public' (shared knowledge, visible to everyone including the X-Ray UI) or 'private' (isolated to subject — pass a stable subject like an agent's own name for private working memory)"), mcp.Required()),
		mcp.WithString("title", mcp.Description("Short title for the item"), mcp.Required()),
		mcp.WithString("content", mcp.Description("Full content/body in Markdown. Supports **bold**, _italic_, [links](url), ![images](url), headings, lists, blockquotes, code blocks."), mcp.Required()),
		mcp.WithString("subject", mcp.Description("Author or subject tag")),
		mcp.WithString("confidence", mcp.Description("Confidence level: high, medium, low")),
		mcp.WithString("event_date", mcp.Description("When this item happened: RFC3339 timestamp or a bare YYYY-MM-DD date (e.g. 2026-06-15T09:00:00Z or 2026-06-15). Rendered back to readers as an event_time annotation on search results.")),
		mcp.WithArray("tags", mcp.Description("Optional list of string tags")),
		mcp.WithArray("evidence_refs", mcp.Description("Optional ids of the cells or events this item was drawn from. refute_memory follows them: a model-written cell that cites a refuted one is retired with it.")),
		mcp.WithString("image_data", mcp.Description("Base64-encoded image bytes for Data[\"image\"] (thumbnail)")),
		mcp.WithString("image_mime", mcp.Description("MIME type of image_data, e.g. image/jpeg (default image/jpeg)")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		id, _ := args["id"].(string)
		kind, _ := args["kind"].(string)
		scope, _ := args["scope"].(string)
		title, _ := args["title"].(string)
		content, _ := args["content"].(string)
		subject, _ := args["subject"].(string)
		confidence, _ := args["confidence"].(string)
		eventDate, _ := args["event_date"].(string)
		imageData, _ := args["image_data"].(string)
		imageMime, _ := args["image_mime"].(string)
		tags := stringList(args["tags"])
		evidence := stringList(args["evidence_refs"])
		if owner, ok := memberOwner(ctx); ok {
			// A member writes private notes of their own: scope defaults to
			// private, shared is refused, the owner is the caller.
			if scope == "" {
				scope = scopePrivate
			}
			if scope != scopePrivate {
				return mcp.NewToolResultText(errMembersPrivateOnly), nil
			}
			subject = owner
			// A member cannot pick an id: it must name the member's own private
			// cell. A missing id and someone else's cell answer the same, so the
			// call is no oracle for which ids exist (ADR-067).
			if id != "" {
				if !ownsPrivate(memory.GetItem(id), owner) {
					return mcp.NewToolResultText(`{"error":"not found"}`), nil
				}
			} else if memory.CountPrivateOwned(owner) >= memberPrivateLimit {
				return mcp.NewToolResultText(`{"error":"private note limit reached"}`), nil
			}
		}
		if msg := ValidateWrite(kind, confidence); msg != "" {
			return mcp.NewToolResultText(errJSON(msg)), nil
		}
		// A private cell must have an owner — reject an ownerless private write,
		// which would otherwise be unreachable and (if the guard regressed) leak.
		if scope == scopePrivate && subject == "" {
			return mcp.NewToolResultText(`{"error":"subject is required when scope=private"}`), nil
		}
		// event_date is optional but, once given, must be a format
		// eventTimeAnnotation can render back out on read (RFC3339 or a bare
		// YYYY-MM-DD date) — reject anything else now rather than storing a
		// value that silently never annotates.
		if eventDate != "" {
			if _, ok := memstore.ParseEventDate(eventDate); !ok {
				return mcp.NewToolResultText(`{"error":"event_date must be RFC3339 (e.g. 2026-06-15T09:00:00Z) or YYYY-MM-DD"}`), nil
			}
		}
		var data map[string]mcell.DataEntry
		if imageData != "" {
			if imageMime == "" {
				imageMime = "image/jpeg"
			}
			data = map[string]mcell.DataEntry{
				"image": {Bytes: imageData, Mime: imageMime},
			}
		} else if id != "" {
			// Preserve existing data (e.g. image) when not supplying new image_data.
			if existing := memory.GetItem(id); existing != nil && existing.Data != nil {
				data = existing.Data
			}
		}
		var opts []UpsertOption
		if evidence != nil {
			opts = append(opts, WithEvidenceRefs(evidence))
		}
		result, errMsg := memory.Upsert(ctx, id, kind, scope, title, content, subject, confidence, eventDate, data, tags, opts...)
		if errMsg != "" {
			b, _ := json.Marshal(map[string]string{"error": errMsg})
			return mcp.NewToolResultText(string(b)), nil
		}
		logger.Info().Str("id", result.ID).Str("status", result.Status).Str("kind", kind).Str("title", title).Msg("mcp upsert_memory")
		if syncer != nil && result.Item != nil {
			it := result.Item
			go func() {
				if err := syncer.Persist(context.Background(), it, namespace); err != nil {
					logger.Warn().Err(err).Str("id", result.ID).Msg("persist cell CR failed")
				}
			}()
		}
		out := map[string]any{"id": result.ID, "status": result.Status, "kind": kind, "title": title}
		if result.Status == "merged" {
			out["merged_into"] = result.ID
		}
		b, _ := json.Marshal(out)
		return mcp.NewToolResultText(string(b)), nil
	})

	persistAll := func(items []*MemoryItem) {
		if syncer == nil {
			return
		}
		go func() {
			for _, it := range items {
				if err := syncer.Persist(context.Background(), it, namespace); err != nil {
					logger.Warn().Err(err).Str("id", it.ID).Msg("persist cell CR failed")
				}
			}
		}()
	}

	s.AddTool(mcp.NewTool("refute_memory",
		mcp.WithDescription("Refute a memory item that is wrong, not merely out of date: it is retired (status stale), the reason and evidence are recorded on the cell (data.refuted), and every active model-written cell that cites it, by evidence ref or link, one hop away, is retired with it. A private item can only be refuted by passing the subject that owns it."),
		mcp.WithString("id", mcp.Description("Memory item ID"), mcp.Required()),
		mcp.WithString("reason", mcp.Description("Why the item is wrong"), mcp.Required()),
		mcp.WithArray("evidence", mcp.Description("Evidence that refutes it: event ids or cell ids")),
		mcp.WithString("subject", mcp.Description("Required to refute a private item — must match the item's own subject")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		id, _ := args["id"].(string)
		reason, _ := args["reason"].(string)
		subject, _ := args["subject"].(string)
		owner, bound := memberOwner(ctx)
		if bound {
			subject = owner
		}
		if strings.TrimSpace(reason) == "" {
			return mcp.NewToolResultText(`{"error":"reason is required"}`), nil
		}
		item := memory.GetItem(id)
		if item == nil || (isPrivateScope(item.Scope) && item.Subject != subject) {
			return mcp.NewToolResultText(`{"error":"not found"}`), nil
		}
		if bound && !isPrivateScope(item.Scope) {
			return mcp.NewToolResultText(errMemberSharedTarget), nil
		}
		var changed []*MemoryItem
		if bound {
			// no cascade: a member's refutation never reaches a cell they do not own
			changed, _ = memory.RefuteOnly(ctx, id, reason, stringList(args["evidence"]))
		} else {
			changed, _ = memory.Refute(ctx, id, reason, stringList(args["evidence"]))
		}
		persistAll(changed)
		cascaded := make([]string, 0, len(changed))
		for _, c := range changed[min(1, len(changed)):] {
			cascaded = append(cascaded, c.ID)
		}
		logger.Info().Str("id", id).Int("cascaded", len(cascaded)).Msg("mcp refute_memory")
		b, _ := json.Marshal(map[string]any{"id": id, "status": "stale", "cascaded": cascaded})
		return mcp.NewToolResultText(string(b)), nil
	})

	s.AddTool(mcp.NewTool("consolidate_memory",
		mcp.WithDescription("Find near-duplicate stable_fact, operational_pattern, watch_item and hypothesis cells (public ones, optionally of one subject) and report the clusters: which cell to keep and which restatements it absorbs. Reports only unless apply=true, which retires the restatements (status superseded) and lists them on the keeper's supersedes."),
		mcp.WithString("subject", mcp.Description("Optional subject to restrict the sweep to")),
		mcp.WithBoolean("apply", mcp.Description("Retire the duplicates (default false: report only)")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		subject, _ := args["subject"].(string)
		apply, _ := args["apply"].(bool)
		report, changed := memory.ConsolidateMemory(subject, apply)
		persistAll(changed)
		logger.Info().Int("groups", len(report.Groups)).Bool("apply", apply).Msg("mcp consolidate_memory")
		b, _ := json.MarshalIndent(report, "", "  ")
		return mcp.NewToolResultText(string(b)), nil
	})

	s.AddTool(mcp.NewTool("expire_memory",
		mcp.WithDescription("Retire a memory item that is out of date: it is kept with status stale and no longer appears in search or lists. Use refute_memory instead when the item is wrong. A private item can only be expired by passing the subject that owns it."),
		mcp.WithString("id", mcp.Description("Memory item ID"), mcp.Required()),
		mcp.WithString("subject", mcp.Description("Required to expire a private item — must match the item's own subject")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		id, _ := args["id"].(string)
		subject, _ := args["subject"].(string)
		owner, bound := memberOwner(ctx)
		if bound {
			subject = owner
		}
		item := memory.GetItem(id)
		if item == nil || (isPrivateScope(item.Scope) && item.Subject != subject) {
			return mcp.NewToolResultText(`{"error":"not found"}`), nil
		}
		if bound && !isPrivateScope(item.Scope) {
			return mcp.NewToolResultText(errMemberSharedTarget), nil
		}
		persistAll([]*MemoryItem{memory.Expire(id)})
		logger.Info().Str("id", id).Msg("mcp expire_memory")
		b, _ := json.Marshal(map[string]any{"id": id, "status": "stale"})
		return mcp.NewToolResultText(string(b)), nil
	})

	s.AddTool(mcp.NewTool("supersede_memory",
		mcp.WithDescription("Replace a memory item with a new one: the replacement is written and lists the old id in supersedes, and the old item is retired (status superseded). Use when new evidence invalidates an item. A model-written replacement cannot supersede a human-written item."),
		mcp.WithString("old_id", mcp.Description("ID of the item to supersede"), mcp.Required()),
		mcp.WithString("new_id", mcp.Description("Optional ID for the replacement; generated when omitted")),
		mcp.WithString("kind", mcp.Description("One of: "+fullKindVocabulary), mcp.Required()),
		mcp.WithString("scope", mcp.Description("'public' or 'private'"), mcp.Required()),
		mcp.WithString("title", mcp.Description("Short title"), mcp.Required()),
		mcp.WithString("content", mcp.Description("Content of the replacement, in Markdown"), mcp.Required()),
		mcp.WithString("subject", mcp.Description("Author or subject tag; required for a private replacement, and to supersede a private item")),
		mcp.WithString("confidence", mcp.Description("Confidence level: high, medium, low")),
		mcp.WithArray("tags", mcp.Description("Optional list of string tags")),
		mcp.WithArray("evidence_refs", mcp.Description("Optional ids of the cells or events the replacement was drawn from")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		str := func(k string) string { v, _ := args[k].(string); return v }
		oldID, kind, scope, subject, confidence := str("old_id"), str("kind"), str("scope"), str("subject"), str("confidence")
		owner, bound := memberOwner(ctx)
		if bound {
			if scope == "" {
				scope = scopePrivate
			}
			if scope != scopePrivate {
				return mcp.NewToolResultText(errMembersPrivateOnly), nil
			}
			subject = owner
			if nid := str("new_id"); nid != "" && !ownsPrivate(memory.GetItem(nid), owner) {
				return mcp.NewToolResultText(errJSON("not found")), nil
			}
			if nid := str("new_id"); nid == "" && memory.CountPrivateOwned(owner) >= memberPrivateLimit {
				return mcp.NewToolResultText(errJSON("private note limit reached")), nil
			}
		}
		if msg := ValidateWrite(kind, confidence); msg != "" {
			return mcp.NewToolResultText(errJSON(msg)), nil
		}
		if scope == scopePrivate && subject == "" {
			return mcp.NewToolResultText(errJSON("subject is required when scope=private")), nil
		}
		old := memory.GetItem(oldID)
		if old == nil || (isPrivateScope(old.Scope) && old.Subject != subject) {
			return mcp.NewToolResultText(errJSON("not found")), nil
		}
		if bound && !isPrivateScope(old.Scope) {
			return mcp.NewToolResultText(errMemberSharedTarget), nil
		}
		var opts []UpsertOption
		if ev := stringList(args["evidence_refs"]); ev != nil {
			opts = append(opts, WithEvidenceRefs(ev))
		}
		changed, errMsg := memory.Supersede(ctx, oldID, str("new_id"), kind, scope, str("title"), str("content"), subject, confidence, stringList(args["tags"]), opts...)
		if errMsg != "" {
			return mcp.NewToolResultText(errJSON(errMsg)), nil
		}
		persistAll(changed)
		logger.Info().Str("old", oldID).Str("new", changed[0].ID).Msg("mcp supersede_memory")
		b, _ := json.Marshal(map[string]any{"id": changed[0].ID, "superseded": oldID, "status": "created"})
		return mcp.NewToolResultText(string(b)), nil
	})

	s.AddTool(mcp.NewTool("get_memory_context",
		mcp.WithDescription("Retrieve the cells around an anchor by following their links: starts from the cells that match the anchor, follows ThoughtLink edges with decaying activation, and returns the anchor cells and the activated neighbourhood by relevance. More focused than list_memory when investigating one subject. Only shared (public) cells take part."),
		mcp.WithString("anchor", mcp.Description("A cell ID, a subject, or part of a title to start from"), mcp.Required()),
		mcp.WithNumber("max_depth", mcp.Description("Link hops to follow (default 3, at most 8)")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		anchor, _ := args["anchor"].(string)
		depth := 3
		if d, ok := args["max_depth"].(float64); ok && d > 0 {
			depth = int(d)
		}
		anchors, activated := memory.Context(anchor, depth)
		if len(anchors) == 0 {
			b, _ := json.Marshal(map[string]any{"anchor": anchor, "items": []any{}, "message": "no cells matched anchor"})
			return mcp.NewToolResultText(string(b)), nil
		}
		type row struct {
			ID         string  `json:"id"`
			Kind       string  `json:"kind"`
			Subject    string  `json:"subject,omitempty"`
			Title      string  `json:"title"`
			Content    string  `json:"content"`
			Confidence string  `json:"confidence,omitempty"`
			IsAnchor   bool    `json:"is_anchor,omitempty"`
			Score      float32 `json:"score,omitempty"`
		}
		var rows []row
		var ids, kinds []string
		for _, it := range anchors {
			rows = append(rows, row{it.ID, it.Kind, it.Subject, it.Title, it.Content, it.Confidence, true, 0})
			ids, kinds = append(ids, it.ID), append(kinds, it.Kind)
		}
		for _, a := range activated {
			it := a.Item
			rows = append(rows, row{it.ID, it.Kind, it.Subject, it.Title, it.Content, it.Confidence, false, a.Score})
			ids, kinds = append(ids, it.ID), append(kinds, it.Kind)
		}
		memory.PublishHit(ids, "", kinds)
		logger.Debug().Str("anchor", anchor).Int("anchors", len(anchors)).Int("activated", len(activated)).Msg("mcp get_memory_context")
		b, _ := json.MarshalIndent(map[string]any{"anchor": anchor, "count": len(rows), "items": rows}, "", "  ")
		return mcp.NewToolResultText(string(b)), nil
	})

	s.AddTool(mcp.NewTool("delete_memory",
		mcp.WithDescription("Delete a memory item by ID. A private item (scope=private) can only be deleted by passing the subject that owns it — matching the tenant-isolation rule used by list_memory/search_memory/upsert_memory."),
		mcp.WithString("id", mcp.Description("Memory item ID to delete"), mcp.Required()),
		mcp.WithString("subject", mcp.Description("Required to delete a private item — must match the item's own subject")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		args := req.GetArguments()
		id, _ := args["id"].(string)
		subject, _ := args["subject"].(string)
		owner, bound := memberOwner(ctx)
		if bound {
			subject = owner
		}
		item := memory.GetItem(id)
		if item == nil || (bound && isPrivateScope(item.Scope) && item.Subject != subject) {
			// for a member, another person's cell answers like a missing one
			return mcp.NewToolResultText(`{"error":"not found","id":"` + id + `"}`), nil
		}
		if bound && !isPrivateScope(item.Scope) {
			return mcp.NewToolResultText(errMemberSharedTarget), nil
		}
		// A private item is only deletable by the subject that owns it — the same
		// tenant-isolation rule search_memory/list_memory/upsert_memory apply
		// (ADR-028/037/051). MCP has no implicit caller identity (unlike voice's
		// userHash), so the caller must pass subject explicitly.
		if isPrivateScope(item.Scope) && item.Subject != subject {
			return mcp.NewToolResultText(`{"error":"forbidden","reason":"private item belongs to another subject"}`), nil
		}
		memory.Delete(id)
		if syncer != nil {
			go func() {
				if err := syncer.Remove(context.Background(), id, namespace); err != nil {
					logger.Warn().Err(err).Str("id", id).Msg("remove cell CR failed")
				}
			}()
		}
		logger.Info().Str("id", id).Msg("mcp delete_memory")
		b, _ := json.Marshal(map[string]any{"status": "deleted", "id": id})
		return mcp.NewToolResultText(string(b)), nil
	})

	logger.Info().Msg("mcp: memory tools registered on /mcp (get_memory, list_memory, search_memory, fetch_memory, recent_memory, upsert_memory, delete_memory, refute_memory, consolidate_memory, expire_memory, supersede_memory, get_memory_context)")

	// Code graph (ADR-046): when a snapshot is configured, expose its six read
	// tools (search_graph, search_graph_docs, get_graph_node,
	// get_graph_impact, get_graph_stats) on this same auth-gated /mcp endpoint, so
	// external agents reach memory + code through one credential. All read-scope;
	// update_code_graph is not registered (the binary is CGO-free, no parser).
	if memMapsDeps != nil {
		msnap.RegisterReadTools(s, *memMapsDeps)
		logger.Info().Msg("mcp: recall read tools registered on /mcp (search_graph, search_graph_docs, get_graph_node, get_graph_impact, get_graph_stats)")
	}

	h := server.NewStreamableHTTPServer(s)
	// Throttle MCP calls per client IP — tool calls drive memory writes and
	// (via search) embedding spend. Generous burst, ~5 req/s sustained.
	limiter := newKeyedRateLimiter(200*time.Millisecond, 20)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		staticTok := getToken()
		// Fail closed: with NEITHER an OAuth signing key NOR a static secret
		// configured, the endpoint must never serve — it exposes memory
		// write/delete.
		if auth == nil && staticTok == "" {
			logger.Warn().Msg("mcp request rejected: no credential configured (fail-closed)")
			http.Error(w, `{"error":"mcp auth not configured"}`, http.StatusServiceUnavailable)
			return
		}

		cred := bearerToken(r)
		var scopes []string
		var sub string
		authed := false
		// A write is human when the credential names a person (an OAuth JWT with
		// a subject), derived otherwise: the shared secret identifies nobody.
		origin := OriginDerived
		// 1. OAuth JWT (per-user scopes).
		if auth != nil && cred != "" {
			if s, sc, ok := auth.validateMCPToken(cred); ok {
				sub, scopes, authed = s, sc, true
				if sub != "" {
					origin = OriginHuman
				}
			}
		}
		// 2. Static shared secret (full scope). Constant-time compare.
		if !authed && staticTok != "" && cred != "" &&
			subtle.ConstantTimeCompare([]byte(cred), []byte(staticTok)) == 1 {
			scopes, authed = allMCPScopes, true
		}
		if !authed {
			challenge(w, auth)
			return
		}

		// Per-tool scope enforcement. Peek at the JSON-RPC body to find a
		// tools/call name; non-call requests (initialize, tools/list) need only
		// read scope. Body is restored for the downstream handler.
		tool, body := peekToolCall(r)
		need := scopeRead
		allowed := hasScope(scopes, need)
		if writeScopeTools[tool] {
			need = scopeWrite
			allowed = hasScope(scopes, scopeWrite) || (memberWriteTools[tool] && hasScope(scopes, scopeWriteOwn))
		}
		// A token without the full write scope is a member's: it is bound to its
		// own hash. A bound token that names no subject has no owner: refuse it.
		bound := !hasScope(scopes, scopeWrite)
		if bound && sub == "" {
			allowed = false
		}
		// Turning --mcp-member-access off cuts member tokens already issued.
		if bound && hasScope(scopes, scopeWriteOwn) && (auth == nil || !auth.memberAccess) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error":"member access is off"}`)) //nolint:errcheck
			return
		}
		if !allowed {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error":"insufficient_scope","scope":"` + need + `"}`)) //nolint:errcheck
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		r = r.WithContext(withWriteOrigin(r.Context(), origin))
		if bound {
			r = r.WithContext(withMemberOwner(r.Context(), stableUserID(sub)))
		}

		if !limiter.allow(clientIP(r.Header.Get("X-Forwarded-For"), r.Header.Get("X-Real-IP"), r.RemoteAddr)) {
			http.Error(w, `{"error":"rate limited"}`, http.StatusTooManyRequests)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// bearerToken extracts the credential from an Authorization: Bearer header.
// The scheme is case-insensitive per RFC 6750/7235.
func bearerToken(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if len(auth) < 8 || !strings.EqualFold(auth[:7], "Bearer ") {
		return ""
	}
	return auth[7:]
}

// challenge writes a 401. When OAuth is configured it includes the RFC 9728
// WWW-Authenticate pointer so MCP clients can discover the authorization server;
// otherwise it keeps the bare 401 used by static-secret-only deployments.
func challenge(w http.ResponseWriter, auth *mcpAuthorizer) {
	if auth != nil {
		w.Header().Set("WWW-Authenticate",
			`Bearer realm="mcp", resource_metadata="`+auth.issuer+
				`/.well-known/oauth-protected-resource", scope="`+
				strings.Join(auth.advertisedScopes(), " ")+`"`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte(`{"error":"unauthorized"}`)) //nolint:errcheck
}

// peekToolCall reads the request body and, if it is a tools/call request,
// returns the tool name. It always returns the full body so the caller can
// restore r.Body for the downstream handler.
func peekToolCall(r *http.Request) (tool string, body []byte) {
	if r.Body == nil {
		return "", nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return "", body
	}
	var rpc struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	if json.Unmarshal(body, &rpc) == nil && rpc.Method == "tools/call" {
		return rpc.Params.Name, body
	}
	return "", body
}

// stringList reads a JSON array argument as strings, skipping non-strings. It
// returns nil when the argument is absent or not an array.
func stringList(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// errJSON renders an MCP tool error body.
func errJSON(msg string) string {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return string(b)
}
