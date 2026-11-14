package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ThoughtLink is a directed associative edge between two memory cells.
// Links capture causal/correlative chains between cells, enabling graph traversal
// at retrieval time.
type ThoughtLink struct {
	// ToID is the target cell ID.
	ToID string `json:"toId"`
	// Relation describes the type of association.
	// Valid values: triggers, caused_by, correlates_with, precedes, explains, contradicts
	Relation string `json:"relation"`
	// Weight is the model-assigned confidence for this link (0.0–1.0).
	Weight float32 `json:"weight"`
	// EvidenceRefs optionally cites the incidents or events that support this link.
	// +optional
	EvidenceRefs []string `json:"evidenceRefs,omitempty"`
}

// DataEntry is a named blob stored in MemoryCellSpec.Data.
type DataEntry struct {
	// Bytes is the base64-encoded content of the blob.
	Bytes string `json:"bytes"`
	// Mime is the MIME type (e.g. "image/jpeg"). Optional.
	// +optional
	Mime string `json:"mime,omitempty"`
}

// MemoryCellSpec is the spec of a single memory cell.
// Mirrors MemoryItemSpec exactly plus an Embedding field that is set
// by the reconciler after the first embed call.
type MemoryCellSpec struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	// Scope is the privacy axis: "public" (shared — visible on the graph and in
	// user-facing search; the default) or "private" (hidden from everyone but
	// its owner). The owner of a private cell is the person named by its
	// "user:<hash>" tag; Subject is the identity of the instance that holds the
	// cell, never its owner. system_configuration cells are always treated as
	// private regardless of scope.
	Scope        string        `json:"scope,omitempty"`
	Subject      string        `json:"subject,omitempty"`
	Title        string        `json:"title"`
	Content      string        `json:"content"`
	Confidence   string        `json:"confidence"`
	Status       string        `json:"status"`
	EvidenceRefs []string      `json:"evidenceRefs,omitempty"`
	Tags         []string      `json:"tags,omitempty"`
	Supersedes   []string      `json:"supersedes,omitempty"`
	Links        []ThoughtLink `json:"links,omitempty"`
	CreatedAt    metav1.Time   `json:"createdAt"`
	UpdatedAt    metav1.Time   `json:"updatedAt"`
	StaleAfter   *metav1.Time  `json:"staleAfter,omitempty"`

	// Origin is the cell's provenance (ADR-053): observed (written by code
	// from facts), derived (written by a model, a prior claim and never
	// evidence), human (written by an identified person; outranks derived) or
	// verified (derived, then passed a check). A cell without one is derived.
	// +kubebuilder:validation:Enum=observed;derived;human;verified
	// +optional
	Origin string `json:"origin,omitempty"`

	// EventDate is the date/time of the event, for kind=event cells.
	// RFC3339 format. Only meaningful when Kind == "event".
	// +optional
	EventDate string `json:"eventDate,omitempty"`

	// Embedding holds the base64-encoded little-endian float32 vector.
	// Written by the reconciler; never set by callers.
	// +optional
	Embedding string `json:"embedding,omitempty"`

	// Data holds arbitrary named blobs (e.g. "image") keyed by name.
	// +optional
	Data map[string]DataEntry `json:"data,omitempty"`

	// VoiceConfig holds voice session parameters. Only used when Kind == "system_configuration".
	// +optional
	VoiceConfig *VoiceSessionConfig `json:"voiceConfig,omitempty"`

	// MCP holds remote MCP server connections. Only used when Kind == "system_configuration".
	// +optional
	MCP *MCPConfig `json:"mcp,omitempty"`

	// UI holds UI display configuration (category taxonomy). Only used when
	// Kind == "system_configuration".
	// +optional
	UI *UIConfig `json:"ui,omitempty"`
}

// MCPSecretRef selects a key from a Kubernetes Secret.
type MCPSecretRef struct {
	// Name is the name of the Secret.
	Name string `json:"name"`
	// Key is the key within the Secret whose value is used as the Bearer token.
	// +optional
	Key string `json:"key,omitempty"`
}

// MCPServerConfig describes a remote MCP server whose tools are exposed to the voice model.
type MCPServerConfig struct {
	// Name is a short identifier used for logging (e.g. "my-agent").
	Name string `json:"name"`
	// URL is the base URL of the MCP server (e.g. "https://triage.internal/mcp").
	URL string `json:"url"`
	// TokenSecret references a Kubernetes Secret containing the Bearer token.
	// The key defaults to "token" if not specified.
	// +optional
	TokenSecret *MCPSecretRef `json:"tokenSecret,omitempty"`
	// Tools is an optional allowlist of tool names to register from this server.
	// If empty, all tools are registered.
	// +optional
	Tools []string `json:"tools,omitempty"`
}

// MCPConfig holds MCP server connections for this configuration cell.
type MCPConfig struct {
	// Servers lists remote MCP servers whose tools are fetched at session start
	// and registered as native voice tools.
	// +optional
	Servers []MCPServerConfig `json:"servers,omitempty"`
}

// CategoryConfig defines one UI display category — a domain-facing grouping
// (e.g. "Projects", "Active Concerns") layered over the generic (kind, tag)
// backend (ADR-035). Served pre-auth via /v1/categories so the X-Ray UI can
// render a per-deployment taxonomy without a build-time fork.
// Only Kind is required. Every other field is optional and filled with a
// sensible default by the UI at load time (ID from a slug of the label, Label
// from the kind, Angle evenly spaced by position, Tier a constant, Color/Icon
// from rotating palettes) — so a category can be as terse as `{kind:
// active_concern, label: Concerns}`. An explicit value always wins over its
// default. See ui/lib/category-config.ts loadRuntimeCategories.
type CategoryConfig struct {
	// Kind is the raw backend memory kind this category draws from.
	Kind string `json:"kind"`
	// Label is the display label (e.g. "Projects"). Defaults to a title-cased
	// form of Kind when empty.
	// +optional
	Label string `json:"label,omitempty"`
	// ID is the stable category id — the route segment (/k/<id>) and group key.
	// Defaults to a slug of Label when empty.
	// +optional
	ID string `json:"id,omitempty"`
	// Color is the node/section color (hex, e.g. "#818cf8"). Defaults to a
	// rotating palette entry when empty.
	// +optional
	Color string `json:"color,omitempty"`
	// Tag optionally narrows Kind — an item matches only if it carries this tag.
	// Empty means this is the bare-kind fallback for Kind.
	// +optional
	Tag string `json:"tag,omitempty"`
	// Tier is the radial tier 0..1 (smaller = closer to centre) on the Voronoi
	// graph. Defaults to a constant when unset.
	// +optional
	Tier float64 `json:"tier,omitempty"`
	// Angle is the fixed angular sector in radians so graph regions don't
	// overlap. Defaults to an even spacing around the circle by position when
	// unset.
	// +optional
	Angle float64 `json:"angle,omitempty"`
	// Icon is a lucide-react icon name (e.g. "FolderOpen"). Defaults to a
	// rotating icon set when empty or unrecognized.
	// +optional
	Icon string `json:"icon,omitempty"`
	// Scope marks a category private (isolated/personal) vs public (shared).
	// Empty means public.
	// +optional
	Scope string `json:"scope,omitempty"`
}

// UIConfig holds UI display configuration for this configuration cell.
type UIConfig struct {
	// Categories is the ordered list of display categories. Empty means the UI
	// falls back to its in-repo default taxonomy.
	// +optional
	Categories []CategoryConfig `json:"categories,omitempty"`
}

// VoiceSessionConfig holds the typed voice session parameters for a system_configuration cell.
// Replaces the tag-encoded "voice:<name>", "model:<id>", "greeting:<text>", and tool allowlist.
type VoiceSessionConfig struct {
	// SystemPrompt is the system instruction sent to Gemini Live at session start.
	// Falls back to Content if empty.
	// +optional
	SystemPrompt string `json:"systemPrompt,omitempty"`

	// Greeting is the opening line sent as the first user-turn after connect.
	// +optional
	Greeting string `json:"greeting,omitempty"`

	// VoiceName is the Gemini prebuilt voice (e.g. "Kore").
	// +optional
	VoiceName string `json:"voiceName,omitempty"`

	// VoiceModel is the Gemini Live model ID.
	// +optional
	VoiceModel string `json:"voiceModel,omitempty"`

	// EnabledTools is the allowlist of voice tool names. Empty means all tools.
	// +optional
	EnabledTools []string `json:"enabledTools,omitempty"`

	// WriteAllowedUsers is the list of usernames (GitHub login or UPN short form)
	// permitted to invoke memory write operations (upsert, delete) via voice.
	// Empty means write tools are disabled for all users.
	// +optional
	WriteAllowedUsers []string `json:"writeAllowedUsers,omitempty"`

	// MaxUserDailyAudioTokens caps the Gemini Live AUDIO tokens (audio input +
	// audio output) an authenticated user may spend per UTC calendar day. Audio is
	// ~all of voice cost and equals the Live Audio billing SKUs, so the cap is
	// visible and reconcilable. 0 means unlimited. Overrides the
	// --user-daily-audio-token-limit flag when non-zero. See ADR-042.
	// +optional
	MaxUserDailyAudioTokens int64 `json:"maxUserDailyAudioTokens,omitempty"`

	// MaxItemContentChars caps each search-result item's content (after
	// markdown-noise stripping) before the tool response is serialised, to
	// bound LLM context cost. 0 means "not set in CR" — the caller falls back
	// to the --headroom-content-limit flag. See ADR-038.
	// +optional
	MaxItemContentChars int `json:"maxItemContentChars,omitempty"`

	// AffectiveDialog enables Gemini Live affective dialog: the model detects
	// emotional tone and adapts its responses. Native-audio only. nil means
	// "not set in CR" — the caller falls back to the --voice-affective-dialog
	// flag; non-nil overrides it.
	// +optional
	AffectiveDialog *bool `json:"affectiveDialog,omitempty"`

	// ProactiveAudio enables Gemini Live proactive audio: the model may stay
	// silent on out-of-context / background speech instead of replying. Native-
	// audio only. nil means "not set in CR" — the caller falls back to the
	// --voice-proactive-audio flag; non-nil overrides it.
	// +optional
	ProactiveAudio *bool `json:"proactiveAudio,omitempty"`

	// SessionResumption keeps a voice session alive across Gemini Live connection
	// recycles: the server reconnects upstream with the latest resumption handle
	// on a GoAway or a dropped connection, and the browser stays connected. nil
	// means "not set in CR" — the caller falls back to the
	// --voice-session-resumption flag; non-nil overrides it. See ADR-039.
	// +optional
	SessionResumption *bool `json:"sessionResumption,omitempty"`

	// ContextTriggerTokens turns on Gemini Live's sliding-window context
	// compression: once the session's context passes this many tokens the server
	// drops the oldest user-turn-aligned prefix (the system instruction is never
	// dropped). Must be 5000-128000. 0 means "not set in CR" — the caller falls
	// back to the --voice-context-trigger-tokens flag. See ADR-039.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ContextTriggerTokens int `json:"contextTriggerTokens,omitempty"`

	// ContextTargetTokens is the size the window is cut to when compression runs;
	// it applies only together with ContextTriggerTokens and must be below it.
	// 0 leaves the choice to the server (half the trigger). See ADR-039.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ContextTargetTokens int `json:"contextTargetTokens,omitempty"`
}

// MemoryCellStatus reports reconciler observations.
type MemoryCellStatus struct {
	// Indexed is true when the cell is present in the HNSW graph.
	// +optional
	Indexed bool `json:"indexed,omitempty"`
	// LastIndexedAt is the timestamp of the last successful embed+index.
	// +optional
	LastIndexedAt *metav1.Time `json:"lastIndexedAt,omitempty"`
	// Conditions holds standard Kubernetes condition list.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Stats holds runtime counters for system_configuration cells.
	// +optional
	Stats *RuntimeStats `json:"stats,omitempty"`
}

// RuntimeStats holds live server counters stored on the system_configuration cell status.
type RuntimeStats struct {
	MemoryCells       int64  `json:"memoryCells"`
	VoiceTotal        int64  `json:"voiceTotal"`
	VoiceActive       int64  `json:"voiceActive"`
	SearchTotal       int64  `json:"searchTotal"`
	KeywordHits       int64  `json:"keywordHits"`
	KWVoiceHits       int64  `json:"kwVoiceHits"`
	KWVoiceMisses     int64  `json:"kwVoiceMisses"`
	KWTrigramHits     int64  `json:"kwTrigramHits"`
	SemanticCalls     int64  `json:"semanticCalls"`
	SemanticFallbacks int64  `json:"semanticFallbacks"`
	EmbedCalls        int64  `json:"embedCalls"`
	EmbedCacheHits    int64  `json:"embedCacheHits"`
	UpdatedAt         string `json:"updatedAt"`
	// UserDaily holds per-user token spend for the current UTC day. Rows for
	// past days are pruned on flush so status size stays bounded by active users.
	// +optional
	UserDaily []UserDailyTokens `json:"userDaily,omitempty"`

	// Sessions holds the authenticated session set, persisted so logins survive a
	// pod restart and are shared across replicas via the same informer that loads
	// this CR. Expired rows are pruned on flush so size stays bounded by active
	// users (one row per live session). Neither the raw bearer token nor cleartext
	// identity is stored — only TokenHMAC and an encrypted identity blob — so a
	// reader with `get mcell` RBAC can neither impersonate nor see who is logged in.
	// See ADR-047.
	// +optional
	Sessions []UserSession `json:"sessions,omitempty"`

	// OAuthClients holds the MCP OAuth dynamic-client registrations (RFC 7591),
	// persisted so a client registered before a restart (e.g. Claude Code) is still
	// known afterward and across replicas — otherwise /authorize rejects its cached
	// client_id with "redirect_uri not registered". Public clients (PKCE, no secret),
	// so only the client_id and its redirect_uris are stored; nothing secret. Pruned
	// to a bounded set on flush. See ADR-049.
	// +optional
	OAuthClients []OAuthClientReg `json:"oauthClients,omitempty"`
}

// OAuthClientReg is one MCP OAuth dynamic-client registration persisted to
// system_configuration CR status so it survives restarts and is visible to every
// replica. Public client (PKCE, no secret) — only the client_id and the allowed
// redirect_uris are stored, which are not secret.
type OAuthClientReg struct {
	// ClientID is the issued public client identifier.
	ClientID string `json:"clientId"`
	// RedirectURIs is the allowlist the /authorize redirect_uri is validated against.
	RedirectURIs []string `json:"redirectUris"`
	// IssuedAt is the registration time (RFC3339), used as the eviction key.
	IssuedAt string `json:"issuedAt"`
}

// UserSession is one authenticated session, persisted to system_configuration CR
// status so it survives restarts and is visible to every replica. It deliberately
// stores NO cleartext identity: a reader with `get mcell` RBAC sees only opaque
// hashes/ciphertext, never a login name or SSO ID. TokenHMAC is HMAC-SHA256(token,
// serverKey) — the raw token is never stored; validate() hashes the incoming cookie
// and compares. IdentityEnc carries the display name + provider ID encrypted under
// the server key, so validate() can return real identity after a restart (write-
// gating and metering need it) without exposing PII at rest. ExpiresAt is the prune
// key (analogous to UserDailyTokens.Day): rows past their expiry are dropped on flush.
type UserSession struct {
	// UserHash is SHA-256(userID)[:16] — raw SSO IDs are never stored. It keys the
	// row so re-login replaces in place rather than accumulating.
	UserHash string `json:"userHash"`
	// TokenHMAC is HMAC-SHA256(token, serverKey), hex-encoded. Not a usable credential.
	TokenHMAC string `json:"tokenHmac"`
	// IdentityEnc is AES-256-GCM(username "\x00" userID) under a key derived from the
	// server secret, hex-encoded (nonce-prefixed). Opaque at rest; decrypted on
	// restore to recover the live username/userID. Never contains cleartext PII.
	IdentityEnc string `json:"identityEnc"`
	// ExpiresAt is the session expiry in RFC3339. Prune key on flush.
	ExpiresAt string `json:"expiresAt"`
}

// UserDailyTokens records one authenticated user's Gemini Live token spend for a
// single UTC calendar day. UserHash is SHA-256(rawUserID)[:16] — raw SSO IDs are
// never stored.
type UserDailyTokens struct {
	UserHash string `json:"userHash"`
	Day      string `json:"day"` // UTC calendar day, "YYYY-MM-DD"
	Tokens   int64  `json:"tokens"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=memcell;mcell
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.kind`
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.spec.status`
// +kubebuilder:printcolumn:name="Scope",type=string,JSONPath=`.spec.scope`
// +kubebuilder:printcolumn:name="Indexed",type=boolean,JSONPath=`.status.indexed`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// MemoryCell represents a single memory cell in the X-Ray distributed memory system.
// Each fact, hypothesis, concern, or watch item is an independent Kubernetes object
// whose content is embedded by the reconciler into the in-process HNSW graph for
// semantic search.
type MemoryCell struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MemoryCellSpec   `json:"spec,omitempty"`
	Status MemoryCellStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// MemoryCellList contains a list of MemoryCell.
type MemoryCellList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MemoryCell `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MemoryCell{}, &MemoryCellList{})
}
