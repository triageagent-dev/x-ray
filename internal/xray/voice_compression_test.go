package xray

import (
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genai"

	mcell "github.com/triageagent-dev/x-ray/v2/api/v1"
)

// ── Context window compression (ADR-039 capability 3) ─────────────────────────

func TestValidateContextCompression(t *testing.T) {
	cases := []struct {
		name            string
		trigger, target int
		ok              bool
	}{
		{"off", 0, 0, true},
		{"trigger only", 32000, 0, true},
		{"trigger and target", 32000, 16000, true},
		{"floor", 5000, 0, true},
		{"ceiling", 128000, 64000, true},
		{"below the floor", 4999, 0, false},
		{"above the ceiling", 128001, 0, false},
		{"target with no trigger", 0, 16000, false},
		{"target equal to the trigger", 32000, 32000, false},
		{"target above the trigger", 32000, 40000, false},
		{"negative trigger", -1, 0, false},
		{"negative target", 32000, -1, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateContextCompression(c.trigger, c.target)
			if c.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestContextCompressionConfig(t *testing.T) {
	assert.Nil(t, contextCompressionConfig(0, 0), "trigger 0 is off")

	cfg := contextCompressionConfig(32000, 0)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.TriggerTokens)
	assert.EqualValues(t, 32000, *cfg.TriggerTokens)
	require.NotNil(t, cfg.SlidingWindow, "a sliding window is always asked for")
	assert.Nil(t, cfg.SlidingWindow.TargetTokens, "no target leaves the choice to the server")

	cfg = contextCompressionConfig(32000, 16000)
	require.NotNil(t, cfg.SlidingWindow.TargetTokens)
	assert.EqualValues(t, 16000, *cfg.SlidingWindow.TargetTokens)
}

func TestVoiceManager_ContextCompressionDisabledWhenZero(t *testing.T) {
	vm := newVoiceManagerWithConfig(&VoiceSessionConfig{})
	assert.Nil(t, vm.contextCompression(zerolog.Nop()), "no flag and nothing in the CR: compression stays off")
}

func TestVoiceManager_ContextCompressionFlagsApplyWhenCRUnset(t *testing.T) {
	vm := newVoiceManagerWithConfig(&VoiceSessionConfig{})
	vm.ctxTriggerTokens, vm.ctxTargetTokens = 20000, 10000
	cfg := vm.contextCompression(zerolog.Nop())
	require.NotNil(t, cfg)
	assert.EqualValues(t, 20000, *cfg.TriggerTokens)
	assert.EqualValues(t, 10000, *cfg.SlidingWindow.TargetTokens)
}

func TestVoiceManager_ContextCompressionCROverridesFlags(t *testing.T) {
	// The CR trigger wins, and the CR target goes with it: the flag target is not
	// mixed into a CR pair.
	vm := newVoiceManagerWithConfig(&VoiceSessionConfig{ContextTriggerTokens: 40000})
	vm.ctxTriggerTokens, vm.ctxTargetTokens = 20000, 10000
	cfg := vm.contextCompression(zerolog.Nop())
	require.NotNil(t, cfg)
	assert.EqualValues(t, 40000, *cfg.TriggerTokens)
	assert.Nil(t, cfg.SlidingWindow.TargetTokens, "the CR left the target to the server")

	vm = newVoiceManagerWithConfig(&VoiceSessionConfig{ContextTriggerTokens: 40000, ContextTargetTokens: 8000})
	vm.ctxTriggerTokens = 20000
	cfg = vm.contextCompression(zerolog.Nop())
	assert.EqualValues(t, 8000, *cfg.SlidingWindow.TargetTokens)

	// A CR target alone has no trigger to apply to; the flags stand.
	vm = newVoiceManagerWithConfig(&VoiceSessionConfig{ContextTargetTokens: 8000})
	vm.ctxTriggerTokens, vm.ctxTargetTokens = 20000, 10000
	cfg = vm.contextCompression(zerolog.Nop())
	assert.EqualValues(t, 20000, *cfg.TriggerTokens)
	assert.EqualValues(t, 10000, *cfg.SlidingWindow.TargetTokens)

	// The CR can turn compression on when the flags leave it off.
	vm = newVoiceManagerWithConfig(&VoiceSessionConfig{ContextTriggerTokens: 32000})
	assert.NotNil(t, vm.contextCompression(zerolog.Nop()))
}

func TestVoiceManager_ContextCompressionInvalidCRFallsBackToFlags(t *testing.T) {
	for _, bad := range []VoiceSessionConfig{
		{ContextTriggerTokens: 100},                               // below the floor
		{ContextTriggerTokens: 200000},                            // above the ceiling
		{ContextTriggerTokens: 32000, ContextTargetTokens: 32000}, // target not below the trigger
	} {
		vm := newVoiceManagerWithConfig(&bad)
		assert.Nil(t, vm.contextCompression(zerolog.Nop()), "invalid CR %+v with the flags off leaves compression off", bad)

		vm = newVoiceManagerWithConfig(&bad)
		vm.ctxTriggerTokens = 20000
		cfg := vm.contextCompression(zerolog.Nop())
		require.NotNil(t, cfg, "invalid CR %+v falls back to the flags", bad)
		assert.EqualValues(t, 20000, *cfg.TriggerTokens)
	}
}

func TestConfigWatcher_ParsesContextTokens(t *testing.T) {
	w := &ConfigWatcher{}
	cell := &mcell.MemoryCell{}
	cell.Name = SystemConfigCRName
	cell.Spec.Kind = "system_configuration"
	cell.Spec.VoiceConfig = &mcell.VoiceSessionConfig{ContextTriggerTokens: 32000, ContextTargetTokens: 16000}
	w.update(cell)
	assert.Equal(t, 32000, w.Get().ContextTriggerTokens)
	assert.Equal(t, 16000, w.Get().ContextTargetTokens)

	cell.Spec.VoiceConfig = &mcell.VoiceSessionConfig{}
	w.update(cell)
	assert.Zero(t, w.Get().ContextTriggerTokens, "unset in the CR stays 0 so the flag applies")
	assert.Zero(t, w.Get().ContextTargetTokens)
}

// A resumed connection is opened with the same config as the first, compression
// included: a session that compressed its context must not come back without it.
func TestVoiceSession_LiveConfigKeepsCompressionOnResume(t *testing.T) {
	compression := contextCompressionConfig(32000, 16000)
	base := &genai.LiveConnectConfig{ContextWindowCompression: compression}
	s := &VoiceSession{liveConfig: base, resumption: true}

	first := s.liveConfigFor("")
	resumed := s.liveConfigFor("handle-1")
	assert.Same(t, compression, first.ContextWindowCompression)
	assert.Same(t, compression, resumed.ContextWindowCompression)
	require.NotNil(t, resumed.SessionResumption)
	assert.Equal(t, "handle-1", resumed.SessionResumption.Handle)
}
