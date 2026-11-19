# ADR-068: Running More Than One Replica

**Status:** Phases 1 and 2 implemented; phase 3 not built

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
2. **Implemented.** A mesh, `pkg/mesh`: each replica finds its peers through a headless Service, dials every
   one over mutual TLS (a self-signed certificate the chart generates and keeps across upgrades, named for the
   mesh because peers are dialed by pod IP), and sends small typed frames. It is best effort: a frame sent
   while a peer is away is lost, so what travels is level state that is resent on connect and every 30 s, or can
   be read back from the cell. `--mesh-tls-dir` turns it on; the chart does so above one replica. A replica
   logs a peer it dials when the connection is made and when it is lost. Over it go:
   - **Counters and each person's daily token spend**, with `pkg/meshsync`. A replica counts its own work and
     sends its running totals; a receiver adds only what a total grew by, so a resend adds nothing, a lost
     frame is made good by the next, and a restarted replica starts afresh. A token charge is sent at once and
     a counter within 5 s, so a daily cap is checked against the sum for the whole service. What was applied
     from each sender is saved on the cell (`spec.data["mesh-totals"]`) next to the totals, so a restart knows
     what the persisted totals already hold and counts nothing twice.
   - **Logins and OAuth client registrations.** A login on one replica reaches the others as the same sealed row
     the cell holds, a logout as a drop, so the next request may land on any replica without waiting for a flush.
   - **MCP authorization codes.** `/authorize` answers once the peers acknowledge the code, because the client's
     next call is `/token` and the Service may route it to any replica; a redeemed code is dropped everywhere.
   - **Sign-ins.** A sign-in started on one replica is completed by asking that replica, which its state names, as
     for codes; the state key is derived from the OAuth secret. If it cannot be asked, the person signs in again.
   With the mesh carrying everything the stats flush held, the flush is leader-only: the leader writes the
   counters, the sessions and the clients, never less than the cell already has, and a replica that takes the
   lease marks the stats dirty so it writes at once. The persisted counters are taken back once at start; with a
   mesh a later echo of the cell would count what the peers sent twice.
**Authorization codes are single-use across replicas.** A code is redeemed once, but the same code can reach two
replicas at once (a client that retries, or two requests sent on purpose), each holding a copy. The replica that
issued the code decides: a replica that holds a copy asks the issuer, over the mesh, whether it is still
unredeemed, and the issuer answers under its lock, so of any number of requests one gets it. If the issuer cannot
be asked (it is gone, or not connected) the answer is 503 `temporarily_unavailable` and no token; the client
authorizes again.

**Who a peer is.** Peers authenticate with mutual TLS: a connection needs a certificate the mesh CA signed, in
both directions, over TLS 1.3. The chart generates one certificate for all pods, so it proves that a caller holds
the mesh Secret, not which pod it is. Three things narrow it:
- A connection is accepted only from an address the headless Service resolves to, so holding the Secret is not
  enough; the caller must be a pod behind the Service.
- An authorization code carries the subject and scopes of the token it becomes, so its frame carries a MAC under
  a key derived from the OAuth signing secret every replica shares. A peer without that secret cannot plant a code.
  Sessions are sealed under a key from the same secret, so a forged login does not decrypt.
- The Secret is in the release namespace, where the service account can already read the OAuth secrets, so run
  X-Ray in a namespace of its own (as the chart's RBAC note says) and limit the mesh port to the pods of the
  release with a NetworkPolicy.

What a peer is trusted for, once connected: the counters and token spend it reports, and the name it gives itself.
A caller that holds the Secret and is behind the Service can overstate a person's spend. It cannot mint a token or
a login.

3. A `policy` cell for the limits (daily caps, retention), watched live like the voice
   configuration.

## Consequences

- A single replica behaves as before: with one pod the lease is won at once, the mesh is off, and nothing else changes.
- Without the mesh (no `--mesh-tls-dir`) above one replica, each replica still counts alone, as before.
- A rolling update between a build with the mesh and one without leaves each side counting its own work until
  the roll finishes; frames carry a protocol version and a frame of another version is dropped.
- Followers keep serving search, voice and MCP; only periodic work waits for the leader, and a failover is
  bounded by the lease timings (15 s duration, 10 s renew, 2 s retry).
- The sibling service can drop its internal copies after a release of X-Ray that contains the packages.
  Its import path changes and its lease name is passed as `Config.Name`, so the Lease object and the
  mesh frames are the ones it has today.
