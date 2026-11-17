package leaderlease

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"k8s.io/client-go/rest"
)

// TestNew_IdentityFallsBackToHostname verifies POD_NAME wins when set, and a
// non-empty identity is still derived (via os.Hostname) when it isn't — a lone
// dev/local process must still get SOME stable identity to campaign with.
func TestNew_IdentityFallsBackToHostname(t *testing.T) {
	t.Setenv("POD_NAME", "")
	e := New(Config{RestConfig: &rest.Config{}, Namespace: "xray", Logger: zerolog.Nop()})
	if e.identity == "" {
		hostname, err := os.Hostname()
		if err == nil && hostname != "" {
			t.Fatal("identity empty even though os.Hostname() succeeded")
		}
		// Environment has no resolvable hostname either — nothing more to assert.
		return
	}

	t.Setenv("POD_NAME", "xray-abc123")
	e2 := New(Config{RestConfig: &rest.Config{}, Namespace: "xray", Logger: zerolog.Nop()})
	if e2.identity != "xray-abc123" {
		t.Errorf("identity = %q, want POD_NAME value %q", e2.identity, "xray-abc123")
	}
}

// TestElector_IsLeader_DefaultsFalse verifies a freshly constructed Elector
// (Start never called, e.g. tests that don't stand up envtest) reports
// not-leader — a replica must not assume leadership before winning a real
// campaign against the API server.
func TestElector_IsLeader_DefaultsFalse(t *testing.T) {
	e := New(Config{RestConfig: &rest.Config{}, Namespace: "xray", Logger: zerolog.Nop()})
	if e.IsLeader() {
		t.Error("IsLeader() = true before Start ever ran, want false")
	}
}

// TestElector_NeedLeaderElection_AlwaysFalse pins the design decision documented
// in the package doc: this lease is independent of controller-runtime's own
// manager-level election, so it must run on every replica regardless of
// --leader-elect.
func TestElector_NeedLeaderElection_AlwaysFalse(t *testing.T) {
	e := New(Config{RestConfig: &rest.Config{}, Namespace: "xray", Logger: zerolog.Nop()})
	if e.NeedLeaderElection() {
		t.Error("NeedLeaderElection() = true, want false (must run independently of manager election)")
	}
}

// TestElector_Callbacks_Plumbing verifies SetCallbacks wires exactly the
// hooks passed onto the fields Start's OnStartedLeading/OnStoppedLeading
// closures read . A full end-to-end test would need a real
// (or fake) apiserver to drive an actual leaderelection.LeaderElector
// campaign through Start — this package has no such harness (every existing
// test above deliberately avoids contacting a real API server), so this
// pins the callback PLUMBING only: SetCallbacks stores the exact functions
// given, and they are callable multiple times (every acquisition/loss
// cycle — see Start's re-campaign loop).
func TestElector_Callbacks_Plumbing(t *testing.T) {
	e := New(Config{RestConfig: &rest.Config{}, Namespace: "xray", Logger: zerolog.Nop()})

	if e.onStarted != nil || e.onStopped != nil {
		t.Fatal("a freshly constructed Elector must have nil callbacks until SetCallbacks is called")
	}

	var startedCount, stoppedCount int
	e.SetCallbacks(func() { startedCount++ }, func() { stoppedCount++ })

	if e.onStarted == nil || e.onStopped == nil {
		t.Fatal("SetCallbacks did not wire the provided hooks")
	}
	e.onStarted()
	e.onStarted()
	e.onStopped()
	if startedCount != 2 || stoppedCount != 1 {
		t.Fatalf("started=%d stopped=%d, want 2/1 — the wired hooks must be exactly the functions passed, callable on every acquisition/loss cycle", startedCount, stoppedCount)
	}
}

// TestElector_Callbacks_NilIsSafe verifies SetCallbacks accepts nil for
// either or both hooks (the documented "half the transition is silently
// skipped" behavior).
func TestElector_Callbacks_NilIsSafe(t *testing.T) {
	e := New(Config{RestConfig: &rest.Config{}, Namespace: "xray", Logger: zerolog.Nop()})
	e.SetCallbacks(nil, nil)
	if e.onStarted != nil || e.onStopped != nil {
		t.Fatal("SetCallbacks(nil, nil) must leave both hooks nil")
	}
}

// TestElector_StartHonorsCancelledContext verifies Start returns promptly
// without ever contacting a (nonexistent) API server when the context is
// already cancelled — bounds the test's own runtime and proves the loop's
// ctx.Err() guard is checked before campaigning.
func TestElector_StartHonorsCancelledContext(t *testing.T) {
	e := New(Config{RestConfig: &rest.Config{Host: "http://127.0.0.1:0"}, Namespace: "xray", Logger: zerolog.Nop()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before Start ever checks it
	if err := e.Start(ctx); err != nil {
		t.Errorf("Start() with an already-cancelled context returned %v, want nil", err)
	}
	if e.IsLeader() {
		t.Error("IsLeader() = true after a cancelled-context Start, want false")
	}
}

// An elector with no identity cannot hold the lease: Start says so once, does
// not campaign (so never reports leader), and returns when ctx ends.
func TestElector_StartWithoutIdentityLogsOnceAndNeverLeads(t *testing.T) {
	var logs bytes.Buffer
	e := &Elector{cfg: Config{RestConfig: &rest.Config{}, Namespace: "xray", Logger: zerolog.New(&logs)}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx) }()

	time.Sleep(3 * RetryPeriod / 2) // longer than a retry: a retrying Start would log again
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return when ctx ended")
	}
	if n := strings.Count(logs.String(), "\n"); n != 1 {
		t.Errorf("logged %d lines, want 1: %s", n, logs.String())
	}
	if e.IsLeader() {
		t.Error("an elector with no identity reports leader")
	}
}

// The Lease is named by Config.Name; an empty name takes the default, so two
// services that elect for different work never share one by accident.
func TestElector_LeaseName(t *testing.T) {
	if n := New(Config{Name: "reaper"}).name(); n != "reaper" {
		t.Errorf("name = %q, want reaper", n)
	}
	if n := New(Config{}).name(); n != DefaultLeaseName {
		t.Errorf("default name = %q, want %q", n, DefaultLeaseName)
	}
	if n := (&Elector{}).name(); n != DefaultLeaseName {
		t.Errorf("name of an Elector built without New = %q, want %q", n, DefaultLeaseName)
	}
}
