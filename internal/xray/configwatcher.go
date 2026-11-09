package xray

import (
	"sync"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// SecretStore holds Secret data watched via the controller-runtime cache.
// It is safe for concurrent use.
type SecretStore struct {
	mu   sync.RWMutex
	data map[string]map[string][]byte // namespace/name -> key -> value
}

func NewSecretStore() *SecretStore {
	return &SecretStore{data: make(map[string]map[string][]byte)}
}

func (s *SecretStore) Set(namespace, name string, data map[string][]byte) {
	key := namespace + "/" + name
	s.mu.Lock()
	s.data[key] = data
	s.mu.Unlock()
}

func (s *SecretStore) Delete(namespace, name string) {
	key := namespace + "/" + name
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

// Get returns the value for a key in a Secret. Returns empty string if not found.
func (s *SecretStore) Get(namespace, name, key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if d, ok := s.data[namespace+"/"+name]; ok {
		return string(d[key])
	}
	return ""
}

// SecretEventHandlerFuncs returns informer handlers that keep the store in sync.
func (s *SecretStore) SecretEventHandlerFuncs() toolscache.ResourceEventHandlerFuncs {
	set := func(obj any) {
		sec, ok := obj.(*corev1.Secret)
		if !ok {
			return
		}
		s.Set(sec.Namespace, sec.Name, sec.Data)
	}
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc:    set,
		UpdateFunc: func(_, obj any) { set(obj) },
		DeleteFunc: func(obj any) {
			// Unwrap tombstone delivered by the informer on missed watch events.
			if d, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = d.Obj
			}
			if sec, ok := obj.(*corev1.Secret); ok {
				s.Delete(sec.Namespace, sec.Name)
			}
		},
	}
}

const SystemConfigCRName = "xray-system-configuration"

// VoiceSessionConfig is the parsed, typed voice session configuration
// loaded from the system_configuration MemoryCell CR.
type VoiceSessionConfig struct {
	SystemPrompt      string
	Greeting          string
	VoiceName         string
	VoiceModel        string
	EnabledTools      []string
	MCPServers        []MCPServerRef
	WriteAllowedUsers []string // usernames permitted to invoke write tools via voice
	// MaxUserDailyAudioTokens caps per-user Gemini Live AUDIO token spend (audio-in
	// + audio-out) per UTC day. 0 means "not set in CR" — the caller falls back to
	// the --user-daily-audio-token-limit flag. See ADR-042.
	MaxUserDailyAudioTokens int64
	// MaxItemContentChars caps each search-result item's content (headroom
	// LossyCompress budget). 0 means "not set in CR" — the caller falls back to the
	// --headroom-content-limit flag. See ADR-038.
	MaxItemContentChars int
	// AffectiveDialog / ProactiveAudio are native-audio quality toggles. nil means
	// "not set in CR" — the caller falls back to the corresponding flag.
	AffectiveDialog *bool
	ProactiveAudio  *bool
	// SessionResumption resumes the upstream Gemini Live session on a GoAway or a
	// dropped connection (ADR-039). nil means "not set in CR" — the caller falls
	// back to the --voice-session-resumption flag.
	SessionResumption *bool
	// Categories is the UI display category taxonomy (ADR-035 item 4). Empty
	// means the UI falls back to its in-repo default taxonomy.
	Categories []CategoryConfig
}

// CategoryConfig is the parsed, typed form of mcell.CategoryConfig — a UI
// display category served pre-auth via /v1/categories. JSON tags match the
// frontend's RuntimeCategoryConfig shape (ui/lib/category-config.ts).
// An unset tier or angle is left out: the frontend fills its defaults with ??,
// which keeps a 0 and would draw every such region at the centre.
type CategoryConfig struct {
	ID    string  `json:"id"`
	Label string  `json:"label"`
	Color string  `json:"color"`
	Kind  string  `json:"kind"`
	Tag   string  `json:"tag,omitempty"`
	Tier  float64 `json:"tier,omitempty"`
	Angle float64 `json:"angle,omitempty"`
	Icon  string  `json:"icon,omitempty"`
	Scope string  `json:"scope,omitempty"`
}

// MCPServerRef describes a remote MCP server to connect to at session start.
// Token is resolved at session start from the SecretStore, not stored in the CR.
type MCPServerRef struct {
	Name            string
	URL             string
	TokenSecretName string   // k8s Secret name
	TokenSecretKey  string   // key within the Secret; defaults to "token"
	Token           string   // resolved from SecretStore at session start; never persisted
	Tools           []string // allowlist of tool names; empty = all
}

// ConfigWatcher watches the single system_configuration MemoryCell CR by name
// and keeps a parsed VoiceSessionConfig atomically up to date.
// It is the only place that reads voice config — no List, no tag parsing.
type ConfigWatcher struct {
	val    atomic.Pointer[VoiceSessionConfig] //nolint:govet
	memory *MemoryStore
}

// NewConfigWatcher creates a ConfigWatcher that restores persisted stats into
// memory when the system_configuration CR is first loaded.
func NewConfigWatcher(memory *MemoryStore) *ConfigWatcher {
	return &ConfigWatcher{memory: memory}
}

// Get returns the current config. Never nil — returns an empty config if not yet loaded.
func (w *ConfigWatcher) Get() *VoiceSessionConfig {
	if v := w.val.Load(); v != nil {
		return v
	}
	return &VoiceSessionConfig{}
}

func (w *ConfigWatcher) update(obj any) {
	cell, ok := obj.(*mcell.MemoryCell)
	if !ok || cell.Name != SystemConfigCRName || cell.Spec.Kind != "system_configuration" {
		return
	}
	cfg := parseVoiceConfig(cell)
	w.val.Store(cfg)
	if w.memory != nil {
		w.memory.RestoreStats(cell.Status.Stats)
	}
}

func (w *ConfigWatcher) delete(obj any) {
	// Unwrap tombstone delivered by the informer on missed watch events.
	if d, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	cell, ok := obj.(*mcell.MemoryCell)
	if !ok || cell.Name != SystemConfigCRName || cell.Spec.Kind != "system_configuration" {
		return
	}
	w.val.Store(&VoiceSessionConfig{})
}

// EventHandlerFuncs returns handler funcs to register on the MemoryCell informer.
func (w *ConfigWatcher) EventHandlerFuncs() toolscache.ResourceEventHandlerFuncs {
	return toolscache.ResourceEventHandlerFuncs{
		AddFunc:    w.update,
		UpdateFunc: func(_, obj any) { w.update(obj) },
		DeleteFunc: w.delete,
	}
}

func parseVoiceConfig(cell *mcell.MemoryCell) *VoiceSessionConfig {
	cfg := &VoiceSessionConfig{}
	if vc := cell.Spec.VoiceConfig; vc != nil {
		cfg.SystemPrompt = vc.SystemPrompt
		cfg.Greeting = vc.Greeting
		cfg.VoiceName = vc.VoiceName
		cfg.VoiceModel = vc.VoiceModel
		if len(vc.EnabledTools) > 0 {
			cfg.EnabledTools = append([]string(nil), vc.EnabledTools...)
		}
		if len(vc.WriteAllowedUsers) > 0 {
			cfg.WriteAllowedUsers = append([]string(nil), vc.WriteAllowedUsers...)
		}
		cfg.MaxUserDailyAudioTokens = vc.MaxUserDailyAudioTokens
		cfg.MaxItemContentChars = vc.MaxItemContentChars
		cfg.AffectiveDialog = vc.AffectiveDialog
		cfg.ProactiveAudio = vc.ProactiveAudio
		cfg.SessionResumption = vc.SessionResumption
	}
	if mc := cell.Spec.MCP; mc != nil {
		for _, s := range mc.Servers {
			ref := MCPServerRef{Name: s.Name, URL: s.URL}
			if s.TokenSecret != nil {
				ref.TokenSecretName = s.TokenSecret.Name
				ref.TokenSecretKey = s.TokenSecret.Key
				if ref.TokenSecretKey == "" {
					ref.TokenSecretKey = "token"
				}
			}
			if len(s.Tools) > 0 {
				ref.Tools = append([]string(nil), s.Tools...)
			}
			cfg.MCPServers = append(cfg.MCPServers, ref)
		}
	}
	if ui := cell.Spec.UI; ui != nil {
		for _, c := range ui.Categories {
			cfg.Categories = append(cfg.Categories, CategoryConfig{
				ID:    c.ID,
				Label: c.Label,
				Color: c.Color,
				Kind:  c.Kind,
				Tag:   c.Tag,
				Tier:  c.Tier,
				Angle: c.Angle,
				Icon:  c.Icon,
				Scope: c.Scope,
			})
		}
	}
	return cfg
}
