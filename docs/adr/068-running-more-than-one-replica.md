# ADR-068: Running More Than One Replica

**Status:** Phase 1 implemented; phases 2 and 3 not built

## Context

X-Ray runs as one pod (`replicaCount: 1`). Above one replica it misbehaves, because every replica
treats the shared `system_configuration` cell as its own:

- **Daily token caps leak.** Each replica writes its own absolute per-user count to the cell and
  restores by taking the larger value, so two replicas serving one person enforce the cap against
  the larger of two counts, not their sum.
- **Counters overwrite each other.** Search and voice totals are written whole by whichever replica
  flushes last; a conflicting write is dropped.
- **Periodic work runs once per replica.** The cell-sync self-writes and the retention and episodic
  reapers duplicate writes and collide on the same cells.
- **MCP authorization codes are per replica.** A code issued by `/authorize` on one pod is unknown to
  `/token` on another. (Tokens are signed with a shared key and validate anywhere, ADR-050.)
- **The chart cannot ask for it.** The Role has no `leases` permission.

A sibling service that serves many replicas already solves each of these with two small pieces: a
Kubernetes Lease for work that must run once, and counters that converge by exchanging each
replica's own running totals. Both are generic, so they are published here as packages the
sibling imports in turn, instead of being copied twice.

## Decision

**Two public packages, extracted unchanged in behaviour and wire format.**

- `pkg/leaderlease`: an `Elector` that campaigns for a named Lease (`Config.Name`) on every replica,
  with a lock-free `IsLeader()` and start/stop callbacks. It is deliberately independent of
  controller-runtime's own election, which would stop a follower's watches and HTTP servers; only
  the singleton work is gated, serving stays identical on every replica.
- `pkg/meshsync`: a `Sync` that exchanges each replica's own running totals with an epoch. A
  receiver applies only what a total adds over the last one it saw from that sender, so a lost
  update is made good by the next, a late one adds nothing, and a restarted sender starts afresh.
  `Save` and `Load` persist what was applied from each sender next to the merged ledger, so a
  restart counts nothing twice.

The packages carry the tests they had where they were written, so a service that swaps its internal
copy for these has the same behaviour pinned.

**Phases for X-Ray.**

1. **Implemented.** The packages, and `--leader-elect` (`--leader-lease-name` names the Lease). With it on,
   the leader alone runs the retention sweep and the episodic reaper, deletes a cell at its deadline,
   and makes the writes a reconcile does back to a cell it only reads (private-owner migration, the
   stored embedding and the indexed status). Every replica still mirrors every cell into its own store
   and index, so search is complete on each; a follower that has to embed a cell does so for its own
   index and leaves the write-back to the leader. A replica that takes the lease re-runs the startup
   backfill, which skipped its writes while it was a follower. The chart turns the flag on by itself above
   one replica (`leaderElect` forces it on with one), gives the pods their name as the Lease identity and
   adds the `leases` permission. Requests a person makes (an upsert, a delete) are written by the replica
   that serves them, as before. The stats flush is **not** gated: it also carries the live sessions and
   registered OAuth clients, which another replica restores from the cell, so a flush only the leader made
   would lose a login made on a follower. It moves with the counters in phase 2.
2. A small mesh (peers found through a headless Service, mutual TLS) that carries one ledger frame,
   `meshsync` for the per-user daily token spend so a cap holds across replicas, and a frame for the
   MCP authorization codes.
3. A `policy` cell for the limits (daily caps, retention), watched live like the voice
   configuration.

## Consequences

- A single replica behaves as before: with one pod the lease is won at once and nothing else changes.
- Followers keep serving search, voice and MCP; only periodic work waits for the leader, and a failover is
  bounded by the lease timings (15 s duration, 10 s renew, 2 s retry).
- The sibling service can drop its internal copies after a release of X-Ray that contains the packages.
  Its import path changes and its lease name is passed as `Config.Name`, so the Lease object and the
  mesh frames are the ones it has today.
