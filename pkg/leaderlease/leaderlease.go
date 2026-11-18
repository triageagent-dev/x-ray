// Package leaderlease elects one leader among a service's replicas for the work
// that must run once, not once per replica: sweeps, reapers, the writer of a
// shared cell. Without it N replicas each run the same periodic job on the same
// schedule, multiplying its cost and its writes by N.
//
// The lease is deliberately separate from controller-runtime's manager-level
// leader election (ctrl.Options.LeaderElection). That election, when enabled,
// stops every follower's reconcilers entirely, and with them the follower's
// watches and HTTP servers, blackholing whatever ingress is pinned to that
// replica. Only the singleton work needs single-writer semantics; serving must
// keep running identically on every replica. Hence Elector.NeedLeaderElection()
// returns false: it is not gated on the manager's own election and campaigns on
// every replica regardless of it.
package leaderlease

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// DefaultLeaseName is the Lease object name campaigned for when Config.Name is
// empty. A different resource from controller-runtime's own LeaderElectionID.
const DefaultLeaseName = "xray-leader"

// Standard client-go leader-election timings (matches controller-runtime's own
// defaults): a lease is considered stale after LeaseDuration with no renew, a
// holder gives up if it can't renew within RenewDeadline, and a non-holder
// retries acquisition every RetryPeriod.
const (
	LeaseDuration = 15 * time.Second
	RenewDeadline = 10 * time.Second
	RetryPeriod   = 2 * time.Second
)

// Config configures an Elector.
type Config struct {
	// RestConfig authenticates to the API server (e.g. mgr.GetConfig()). Required.
	RestConfig *rest.Config
	// Namespace the Lease object lives in, normally the release namespace.
	// Required.
	Namespace string
	// Name is the Lease object's name; DefaultLeaseName when empty. Services that
	// elect independently, for different work, use different names.
	Name string
	// Logger receives leadership transition events. Zero value (zerolog.Logger{})
	// is a valid no-op-ish logger.
	Logger zerolog.Logger
}

// Elector wraps client-go's leaderelection into a manager.Runnable plus a
// cheap, lock-free IsLeader() poll for the leader-only work's periodic-tick gate.
type Elector struct {
	cfg       Config
	identity  string
	leader    atomic.Bool
	onStarted func()
	onStopped func()
	// coord replaces the client built from RestConfig, for tests.
	coord coordinationv1client.CoordinationV1Interface
}

// New builds an Elector. It does not contact the API server until Start runs.
// Identity is POD_NAME (set by the Deployment's downward API), falling back to
// os.Hostname() for local/dev runs where POD_NAME is unset — a lone process
// still gets a stable, unique-enough identity and simply wins immediately
// (the agent does not wait on the lease; it only gates leader-only jobs).
func New(cfg Config) *Elector {
	identity := strings.TrimSpace(os.Getenv("POD_NAME"))
	if identity == "" {
		if h, err := os.Hostname(); err == nil {
			identity = h
		}
	}
	if cfg.Name == "" {
		cfg.Name = DefaultLeaseName
	}
	return &Elector{cfg: cfg, identity: identity}
}

// IsLeader reports whether this replica currently holds the lease.
// Safe for concurrent use. False until Start has won its first campaign — a
// replica that hasn't heard from the API server yet must not assume it's the
// (possibly sole) leader.
func (e *Elector) IsLeader() bool {
	return e.leader.Load()
}

// SetCallbacks wires optional hooks fired on every leadership acquisition
// (onStarted) and loss (onStopped), for work that must adopt state on taking
// over. Must be called before Start. Nil-safe: either or both may be nil, in which case that half
// of the transition is silently skipped, so a deployment that never wires
// callbacks behaves exactly as before this method existed. Fired AFTER the
// atomic leader flag flips (leader.Store already ran), and on EVERY campaign
// cycle, not just the first: client-go's LeaderElector.Run only covers ONE
// campaign, and Start's own re-campaign loop (see its doc comment) means
// these fire again on every future acquisition/loss, giving continuous
// coverage across a lease handoff, not just process start.
func (e *Elector) SetCallbacks(onStarted, onStopped func()) {
	e.onStarted = onStarted
	e.onStopped = onStopped
}

// name is the Lease object's name, for an Elector built without New.
func (e *Elector) name() string {
	if e.cfg.Name == "" {
		return DefaultLeaseName
	}
	return e.cfg.Name
}

// NeedLeaderElection implements manager.LeaderElectionRunnable, always
// returning false — see the package doc for why this must run independently
// of controller-runtime's own (usually disabled) manager-level election.
func (e *Elector) NeedLeaderElection() bool {
	return false
}

// Start implements manager.Runnable: campaigns for the lease in a loop until
// ctx is done, re-campaigning immediately after losing. client-go's
// LeaderElector.Run blocks only for the duration of ONE campaign (it returns
// once leadership is lost or ctx ends, it does not re-campaign itself), so the
// looping here is what gives a lost-then-regained lease continuous coverage —
// e.g. after this replica's own transient API-server hiccup, or after the
// previous leader pod is deleted.
func (e *Elector) Start(ctx context.Context) error {
	if e.identity == "" {
		// client-go refuses a lock with no identity, so campaigning would fail
		// the same way every round. This replica never leads; the others still
		// can.
		e.cfg.Logger.Error().Msg("leaderlease: no identity (POD_NAME unset and no hostname), not campaigning for the lease")
		<-ctx.Done()
		return nil
	}
	coordClient := e.coord
	if coordClient == nil {
		c, err := coordinationv1client.NewForConfig(e.cfg.RestConfig)
		if err != nil {
			return fmt.Errorf("leaderlease: build coordination client: %w", err)
		}
		coordClient = c
	}
	// A replica that is stopping starts no more leader-only work. Work it already
	// started (a flush, a backfill) may still run for the seconds until the process
	// exits, next to the new leader's: a backfill writes the same vectors, and the
	// stats never go below what the cell holds.
	go func() {
		<-ctx.Done()
		e.leader.Store(false)
	}()
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      e.name(),
			Namespace: e.cfg.Namespace,
		},
		Client: coordClient,
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: e.identity,
		},
	}

	for ctx.Err() == nil {
		elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
			Lock:          lock,
			LeaseDuration: LeaseDuration,
			RenewDeadline: RenewDeadline,
			RetryPeriod:   RetryPeriod,
			// Hand the lease back when ctx ends (the pod is stopping), so another
			// replica takes over within a RetryPeriod instead of waiting out
			// LeaseDuration.
			ReleaseOnCancel: true,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(context.Context) {
					e.leader.Store(true)
					e.cfg.Logger.Info().Str("identity", e.identity).Str("lease", e.name()).
						Msg("leaderlease: acquired lease")
					if e.onStarted != nil {
						e.onStarted()
					}
				},
				OnStoppedLeading: func() {
					e.leader.Store(false)
					e.cfg.Logger.Info().Str("identity", e.identity).Str("lease", e.name()).
						Msg("leaderlease: lost lease")
					if e.onStopped != nil {
						e.onStopped()
					}
				},
			},
		})
		if err != nil {
			e.cfg.Logger.Error().Err(err).Msg("leaderlease: build elector failed, retrying")
			select {
			case <-time.After(RetryPeriod):
				continue
			case <-ctx.Done():
				return nil
			}
		}
		elector.Run(ctx) // blocks for one campaign: until this replica loses the lease or ctx ends
	}
	return nil
}
