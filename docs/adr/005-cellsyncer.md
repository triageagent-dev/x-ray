# ADR-005: CellSyncer — CR to Memory Sync

**Status:** Implemented  
## Role

`CellSyncer` (`internal/xray/cellsync.go`) is the bridge between `AwarenessMemoryCell` Kubernetes CRs and the runtime `MemoryStore`. It runs in two directions:

- **CR → Memory**: at startup and on CR changes (via controller-runtime).
- **Memory → CR**: when MCP `upsert_memory` or `delete_memory` is called (via `Persist()` / `Remove()`).

## Startup: `SeedAll(namespace)`

Called once at startup. Lists all `AwarenessMemoryCell` CRs in the namespace. For each:
- Skips if `spec.status == "stale"` or `"superseded"`.
- Calls `syncCell()`.

## `syncCell(cell)`

1. If `spec.embedding` is present and valid: calls `memory.SeedFromDoc()` with the decoded vector — **no embedder API call**, fast.
2. If embedding is missing or invalid and embedder is configured: calls `cells.Index()` to embed, patches `spec.embedding` and `status.indexed=true` back to the CR, then calls `memory.seedItemDirect()`.
3. If no embedder (keyword mode): calls `memory.seedItemDirect()` — adds to items map, no HNSW indexing.

## Runtime: `Reconcile(req)`

Registered as a controller-runtime reconciler for `AwarenessMemoryCell`. Triggered on create/update/delete. Calls `syncCell()` for the changed CR. Retries after 30s on error.

## `Persist(it, namespace)`

Writes a `MemoryItem` to the cluster as an `AwarenessMemoryCell` CR.
- CR name: `cellResourceName(id)` = sanitised ID + `-` + 8-char SHA1 suffix.
- On create: sets labels `triageagent.dev/managed=true` and `triageagent.dev/kind=<kind>`. Sets `status.indexed=true` and `status.lastIndexedAt`.
- On update: preserves existing `spec.embedding` so the reconciler won't re-embed unnecessarily. Updates `spec.updatedAt`.
- Runs in a goroutine from MCP handler — fire and forget with warn log on error.

## `Remove(id, namespace)`

Deletes the CR by computing `cellResourceName(id)`. No-op if CR not found.

## CR name format

`cellResourceName(id)`:
- Lowercases, replaces non-alphanumeric with `-`, trims leading/trailing dashes.
- Appends `-` + first 8 chars of SHA1(original id).
- Max base length: 244 chars (leaving room for suffix within 253-char k8s name limit).
- Example: `aa6556af6f2e1486` → `aa6556af6f2e1486-5bd2fd3a`.

## Graceful degradation

If no k8s client is available (local dev, no kubeconfig): `CellSyncer` is nil. MCP still works — items are in-memory only, not persisted to CRs. `NewMCPServer()` accepts `syncer=nil` and simply skips `Persist()`/`Remove()` calls.

## Amendments

### 2026-11-05 — Every cell is seeded; a stored vector carries its fingerprint

`syncCell` (now `syncCellDecision`, on `cellsync.Decide`) seeds every public cell this instance owns whose item is active, the rule `MirrorCell` uses to keep a cell in the vector index, unless it is a config cell, at startup and on every reconcile. From 2.3.0 until this change it skipped a cell that already carried an embedding and `status.indexed=true`; the vector index lives in the process, so after a restart exactly those cells were missing from semantic search. A cell of another instance (`cellsync.Options.Subject`) is never indexed or written, so two controllers sharing a namespace never write each other's embeddings.

A stored vector is reused only while it still stands for the cell's text. The annotation `xray.triageagent.dev/embedding-fingerprint` (`index.CellStore.EmbeddingFingerprint`) holds the lowercase hex SHA-256 of the embedding model, its output dimension and the text sent to the embedder, a dot, and the first 16 hex digits of the SHA-256 of `spec.embedding`, and it is written in the same `Update` as `spec.embedding`. The second part binds the annotation to the vector it was written with, so a vector another writer replaced without the annotation is not taken as current; it detects a mismatch, it does not authenticate the writer. Step 1 above now reads: a stored embedding whose annotation equals its current fingerprint, and which decodes to the index dimension, is seeded with no embedder call; the only write is stamping `status.indexed` once when it is not yet set. Everything else (no embedding; a changed text, model, dimension or vector; an embedding written before fingerprints) takes step 2, which writes the embedding and the annotation together. If that embed fails and the cell carries a stale vector of the store's known dimension, the stale vector is seeded so the cell stays searchable, nothing is written, and the reconcile retries. It is an annotation, not a spec or status field, because deployments keep the chart's CRDs (Helm does not upgrade them) and the API server prunes a field its schema does not know, which would re-embed the cell on every sync.

`Persist` keeps the existing embedding on an update only while the stored annotation equals the fingerprint of the new text and that embedding (a change of tags or confidence keeps it); a changed text clears both, so the reconciler embeds the new text. A private or `system_configuration` cell loses the annotation with its embedding. Reconciling an unchanged cell writes nothing, which keeps the reconciler and etcd out of a write loop. `SeedAll` runs beside the controller's first pass, so at the first start a cell can be embedded twice and one of the two writes lose on a conflict; the next reconcile finds the cell current and writes nothing.

Tests: `TestSeedAll_LoadsAStoredVectorWhoseFingerprintMatchesWithNoEmbedAndNoWrite`, `TestSeedAll_AStoredVectorWithoutAFingerprintIsReEmbeddedOnce`, `TestSeedAll_ConvergesWhenItRunsBesideTheFirstReconciles`, `TestReconcile_AnUnchangedIndexedCellWritesNothing`, `TestReconcile_ReEmbedsAStoredVectorWhoseTextChanged`, `TestPersist_ChangedTextDropsTheStoredEmbeddingAndItsFingerprint`, `TestSyncCell_APrivateCellLosesItsEmbeddingAndFingerprint`, `TestSyncCell_ASystemConfigurationCellLosesItsEmbeddingAndFingerprint`, `TestSyncCell_IndexesExactlyTheCellsMirrorCellKeepsSearchable`, `TestSyncCell_AVectorReplacedUnderItsAnnotationIsReEmbedded`, `TestSyncCell_AFailingEmbedderKeepsALegacyCellSearchableAndWritesNothing`, `TestDecide_IndexesEveryEmbeddableCellWhetherOrNotItWasIndexedBefore`, `TestDecide_NeverIndexesAnotherInstancesCell`, `TestDecide_IndexesOnlyActiveCells`, `TestDecide_IndexesACellUntilItsDeadlineAtTheGivenNow`, `TestSeedOrEmbed_AVectorReplacedUnderItsAnnotationIsReEmbedded`, `TestSeedOrEmbed_AFailedEmbedSeedsAStaleStoredVectorAndReportsTheError`, `TestEmbeddingFingerprint_FollowsTheTextTheModelTheDimensionAndTheVector`.
