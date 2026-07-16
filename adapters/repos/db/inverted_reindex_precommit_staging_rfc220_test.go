//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// This file pins the pre-commit staging protocol for
// https://github.com/weaviate/0-weaviate-issues/issues/220.
//
// # The defect it guards against
//
// A semantic (tokenization-changing) migration used to COMMIT its swap
// per shard inside [ShardReindexTaskGeneric.runtimeSwap] — pointer flip,
// old→backup rename, tidied.mig, and the trim that DELETES the OLD
// backup — all before the cluster-wide ack barrier had seen a single
// ack. A sibling unit failing afterwards flipped the task to FAILED and
// skipped the schema flip, leaving every already-committed shard
// permanently inverted (bucket=NEW, schema=OLD) with its rollback data
// already destroyed.
//
// # The protocol under test
//
// STAGE (per shard, pre-ack): pointer flip + old→backup rename +
// swapped.mig + staged.mig. Nothing destroyed.
// COMMIT (task-level, post-barrier + schema flip): OnMigrationComplete +
// tidied.mig + trim ([ShardReindexTaskGeneric.CommitSwapOnShard]).
// ROLLBACK (task FAILED/CANCELLED): pointer restored to the preserved
// OLD backup + unswapped.mig ([ShardReindexTaskGeneric.RollbackSwapOnShard]).
// RESTART inside the window: resolved by the schema-backed verdict in
// [FinalizeCompletedMigrationsWithVerdict].

// TestReindexInversion_SiblingSwapFailureLeavesNoRollback_RFC220 is the
// original pin, now a passing regression test: after unit A stages its
// swap and unit B (a sibling replica of the same property) fails, unit
// A's OLD-tokenized backup dir MUST still exist so the task can be
// rolled back to OLD tokenization.
//
// RED on the pre-staging code (verified 2026-07-16: trimOlderGenerations
// Locked deleted the backup inside runtimeSwap, before the barrier could
// observe B's failure). GREEN with pre-commit staging: the destructive
// trim is deferred to CommitSwapOnShard, which only ever runs on the
// all-success verdict.
func TestReindexInversion_SiblingSwapFailureLeavesNoRollback_RFC220(t *testing.T) {
	ctx := testCtx()
	const propName = "title"

	// Two independent shards model two replicas (units) of the same
	// property in one distributed reindex task. Each drives its own
	// runtimeSwap; the ack barrier that gates the cluster-wide schema
	// flip only fires after both have returned.
	shardA, taskA, propsA, rtA := prepShardToSwapBoundaryRFC220(t, ctx, "InversionUnitA_"+uuid.NewString()[:8])
	shardB, taskB, propsB, rtB := prepShardToSwapBoundaryRFC220(t, ctx, "InversionUnitB_"+uuid.NewString()[:8])
	defer shardA.Shutdown(ctx)
	defer shardB.Shutdown(ctx)

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)

	// --- Unit A: the sibling that stages its swap first. ---
	require.NoError(t, taskA.runtimeSwap(ctx, taskA.logger, shardA, rtA, propsA),
		"unit A swap should succeed (models the sub-task that stages first)")

	require.Equal(t, lsmkv.StrategyInverted, shardA.store.Bucket(searchBucketName).Strategy(),
		"unit A must have flipped its canonical bucket pointer to the NEW-tokenized data")
	require.True(t, rtA.IsSwapped(), "unit A must be marked swapped")
	require.True(t, rtA.IsStaged(),
		"staged mode: unit A's swap must be recorded as STAGED (awaiting the task verdict), not committed")
	require.False(t, rtA.IsTidied(),
		"staged mode: tidied.mig is the task-level COMMIT record and must not exist before the barrier verdict")
	stratA := taskA.strategy.(*testMigrationStrategy)
	require.False(t, stratA.migrationCompleted,
		"staged mode: OnMigrationComplete is part of the COMMIT and must not run at stage time")

	backupDirA := filepath.Join(shardA.pathLSM(), taskA.backupBucketName(propName))

	// --- Unit B: the sibling whose swap fails AFTER A staged. ---
	// A clean sub-task failure: the injected body returns an error without
	// flipping any pointer, modelling the mid-flight faults of #218 (working
	// dir wiped) / #213 (Shutdown ctx not propagated) and the future triggers
	// listed in #220 (disk full, OOM-kill, merger panic, hardware fault).
	taskB.processOneSwapPropFn = func(_ context.Context, _ *lsmkv.Store, _ reindexTracker, _ int, name string) (*lsmkv.Bucket, error) {
		return nil, fmt.Errorf("injected mid-flight sub-task failure on prop %q (RFC #220 trigger)", name)
	}
	errB := taskB.runtimeSwap(ctx, taskB.logger, shardB, rtB, propsB)
	require.Error(t, errB, "unit B swap must fail to model the sibling sub-task failure")
	require.False(t, rtB.IsSwapped(), "unit B must NOT be marked swapped")

	// The ack barrier now observes unit B's failure → the task transitions
	// to FAILED → flipSemanticMigrationSchema is skipped. The ONLY thing
	// that can prevent a permanent inversion on unit A is rolling it back
	// to OLD — which requires A's OLD-tokenized backup to still exist.
	require.True(t, dirExists(backupDirA),
		"RFC #220 pin: after a sibling unit's swap failed, unit A's OLD-tokenized backup dir "+
			"(%s) MUST still exist so the task can be rolled back to OLD tokenization. Pre-fix, "+
			"trimOlderGenerationsLocked deleted it inside runtimeSwap — BEFORE the ack barrier "+
			"could observe the failure — making the schema↔bucket inversion permanent.", backupDirA)
}

// TestReindexStagedSwap_SiblingFailureRollsBackStagedUnits is the
// multi-shard rollback case: units A and B stage their swaps, unit C
// fails, the task verdict is FAILED — RollbackSwapOnShard (what
// OnTaskCompleted's FAILED branch drives per staged unit) must restore
// the OLD data under the canonical bucket name on A and B without a
// restart, and record the deferred on-disk restore durably.
func TestReindexStagedSwap_SiblingFailureRollsBackStagedUnits(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)

	shardA, taskA, propsA, rtA := prepShardToSwapBoundaryRFC220(t, ctx, "RollbackUnitA_"+uuid.NewString()[:8])
	shardB, taskB, propsB, rtB := prepShardToSwapBoundaryRFC220(t, ctx, "RollbackUnitB_"+uuid.NewString()[:8])
	shardC, taskC, propsC, rtC := prepShardToSwapBoundaryRFC220(t, ctx, "RollbackUnitC_"+uuid.NewString()[:8])
	defer shardA.Shutdown(ctx)
	defer shardB.Shutdown(ctx)
	defer shardC.Shutdown(ctx)

	// A and B stage successfully.
	require.NoError(t, taskA.runtimeSwap(ctx, taskA.logger, shardA, rtA, propsA))
	require.NoError(t, taskB.runtimeSwap(ctx, taskB.logger, shardB, rtB, propsB))
	require.True(t, rtA.IsStaged())
	require.True(t, rtB.IsStaged())

	// C fails mid-flight.
	taskC.processOneSwapPropFn = func(_ context.Context, _ *lsmkv.Store, _ reindexTracker, _ int, name string) (*lsmkv.Bucket, error) {
		return nil, fmt.Errorf("injected failure on %q", name)
	}
	require.Error(t, taskC.runtimeSwap(ctx, taskC.logger, shardC, rtC, propsC))

	// Task verdict: FAILED. Roll back every staged unit.
	require.NoError(t, taskA.RollbackSwapOnShard(ctx, shardA))
	require.NoError(t, taskB.RollbackSwapOnShard(ctx, shardB))

	for name, fixture := range map[string]struct {
		shard *Shard
		task  *ShardReindexTaskGeneric
	}{
		"unitA": {shardA, taskA},
		"unitB": {shardB, taskB},
	} {
		shard, task := fixture.shard, fixture.task
		rt, err := task.newReindexTracker(shard.pathLSM())
		require.NoError(t, err)

		require.Equal(t, lsmkv.StrategyMapCollection, shard.store.Bucket(searchBucketName).Strategy(),
			"%s: canonical bucket must serve the OLD (map) data again after rollback", name)
		require.False(t, rt.IsSwapped(), "%s: swapped sentinel must be cleared", name)
		require.False(t, rt.IsSwappedProp(propName), "%s: per-prop swapped sentinel must be cleared", name)
		require.False(t, rt.IsStaged(), "%s: staged sentinel must be retired", name)
		require.True(t, rt.IsUnswapped(),
			"%s: unswapped.mig must record the deferred on-disk restore durably", name)

		backupDir := filepath.Join(shard.pathLSM(), task.backupBucketName(propName))
		ingestDir := filepath.Join(shard.pathLSM(), task.ingestBucketName(propName))
		require.True(t, dirExists(backupDir),
			"%s: the OLD data (live under the backup-named dir until restart) must survive", name)
		require.False(t, dirExists(ingestDir),
			"%s: the discarded NEW ingest dir must be removed", name)
	}

	// Idempotency: a re-fired terminal callback must be a no-op.
	require.NoError(t, taskA.RollbackSwapOnShard(ctx, shardA))

	// The unit whose own swap failed has nothing staged; rollback is a no-op.
	require.NoError(t, taskC.RollbackSwapOnShard(ctx, shardC))
	rtC2, err := taskC.newReindexTracker(shardC.pathLSM())
	require.NoError(t, err)
	require.False(t, rtC2.IsUnswapped(),
		"unitC never staged anything; rollback must not fabricate an unswapped record")
}

// TestReindexStagedSwap_CommitSingleShardDegenerate is the single-shard
// (single-unit) case: stage via the production RunSwapOnShard entry
// point, then commit via CommitSwapOnShard — the end state must equal
// the legacy inline commit (tidied, OLD backup trimmed, canonical=NEW,
// OnMigrationComplete ran), just split across the barrier.
func TestReindexStagedSwap_CommitSingleShardDegenerate(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)

	shard, task, _, _ := prepShardToSwapBoundaryRFC220(t, ctx, "CommitSingle_"+uuid.NewString()[:8])
	defer shard.Shutdown(ctx)

	// Stage through the production DTM entry point (exercises the
	// staged dispatch + the verdict-window double-write arming).
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsStaged(), "swap must be staged, not committed")
	require.False(t, rt.IsTidied())
	migStrategy := task.strategy.(*testMigrationStrategy)
	require.False(t, migStrategy.migrationCompleted, "OnMigrationComplete must wait for the commit")
	task.callbackDisableFuncsMu.Lock()
	armed := len(task.callbackDisableFuncs)
	task.callbackDisableFuncsMu.Unlock()
	require.NotZero(t, armed,
		"the staged window must arm double-writes into the OLD backup so a rollback loses no writes")

	backupDir := filepath.Join(shard.pathLSM(), task.backupBucketName(propName))
	require.True(t, dirExists(backupDir), "OLD backup must survive the stage")

	// Cluster verdict: COMMIT (all acks success, schema flip durable).
	require.NoError(t, task.CommitSwapOnShard(ctx, shard))

	require.Equal(t, lsmkv.StrategyInverted, shard.store.Bucket(searchBucketName).Strategy())
	require.True(t, rt.IsTidied(), "commit must write the tidied sentinel")
	require.False(t, rt.IsStaged(), "commit must retire the staged sentinel")
	require.True(t, migStrategy.migrationCompleted, "commit must run OnMigrationComplete")
	require.False(t, dirExists(backupDir), "commit must trim the OLD backup")

	// Idempotency: a re-fired completion callback must be a no-op.
	require.NoError(t, task.CommitSwapOnShard(ctx, shard))
}

// TestReindexStagedSwap_RestartDuringStagedWindow_TaskCommits covers the
// crash-in-window residual: the node dies after staging; the task
// meanwhile went FINISHED (all acks landed, another node committed the
// cluster-wide schema flip). At the next boot the schema-backed verdict
// says "flipped" and FinalizeCompletedMigrationsWithVerdict must
// complete the COMMIT: promote the staged NEW data to the canonical
// name, drop the OLD backup and the tracker.
func TestReindexStagedSwap_RestartDuringStagedWindow_TaskCommits(t *testing.T) {
	ctx := testCtx()
	const propName = "title"

	shard, task, props, rt := prepShardToSwapBoundaryRFC220(t, ctx, "RestartCommit_"+uuid.NewString()[:8])
	className := shard.Index().Config.ClassName.String()
	require.NoError(t, task.runtimeSwap(ctx, task.logger, shard, rt, props))
	require.True(t, rt.IsStaged())

	lsmPath := shard.pathLSM()
	writeStagedRecoveryPayloadRFC220(t, task, lsmPath, className)

	// Crash: shut the shard down mid-window. Tag the NEW and OLD dirs so
	// the post-"restart" promotion direction is observable.
	require.NoError(t, shard.Shutdown(ctx))
	ingestDir := filepath.Join(lsmPath, task.ingestBucketName(propName))
	backupDir := filepath.Join(lsmPath, task.backupBucketName(propName))
	mainDir := filepath.Join(lsmPath, task.strategy.SourceBucketName(propName))
	tagDirRFC220(t, ingestDir, "NEW")
	tagDirRFC220(t, backupDir, "OLD")

	// Boot: the schema already reflects the target — verdict COMMIT.
	FinalizeCompletedMigrationsWithVerdict(lsmPath, task.logger,
		func(payload *ReindexTaskPayload) (bool, bool) { return true, true })

	require.True(t, dirExists(mainDir), "canonical dir must exist after the boot-time commit")
	require.Equal(t, "NEW", readDirTagRFC220(t, mainDir),
		"the STAGED (NEW) data must have been promoted to the canonical name")
	require.False(t, dirExists(backupDir), "the OLD backup must be dropped on commit")
	require.False(t, dirExists(ingestDir), "the ingest dir was renamed away")
	require.False(t, dirExists(filepath.Join(lsmPath, ".migrations", task.strategy.MigrationDirName())),
		"the tracker dir must be removed after the boot-time commit")
}

// TestReindexStagedSwap_RestartDuringStagedWindow_TaskPendingOrFailed is
// the other crash direction: the node dies after staging and the task
// did NOT commit (still SWAPPING, or FAILED). The schema still shows the
// source state, so the boot verdict is "not flipped" —
// FinalizeCompletedMigrationsWithVerdict must restore the OLD data to
// the canonical name and revert the tracker to the merged state (so a
// still-live task's DTM re-drive can re-stage), destroying nothing.
func TestReindexStagedSwap_RestartDuringStagedWindow_TaskPendingOrFailed(t *testing.T) {
	ctx := testCtx()
	const propName = "title"

	shard, task, props, rt := prepShardToSwapBoundaryRFC220(t, ctx, "RestartPending_"+uuid.NewString()[:8])
	className := shard.Index().Config.ClassName.String()
	require.NoError(t, task.runtimeSwap(ctx, task.logger, shard, rt, props))

	lsmPath := shard.pathLSM()
	writeStagedRecoveryPayloadRFC220(t, task, lsmPath, className)
	require.NoError(t, shard.Shutdown(ctx))
	ingestDir := filepath.Join(lsmPath, task.ingestBucketName(propName))
	backupDir := filepath.Join(lsmPath, task.backupBucketName(propName))
	mainDir := filepath.Join(lsmPath, task.strategy.SourceBucketName(propName))
	tagDirRFC220(t, ingestDir, "NEW")
	tagDirRFC220(t, backupDir, "OLD")

	assertRestoredToMerged := func(pass string) {
		require.True(t, dirExists(mainDir), "%s: canonical dir must exist after the boot-time restore", pass)
		require.Equal(t, "OLD", readDirTagRFC220(t, mainDir),
			"%s: the OLD data must be restored to the canonical name while the verdict is not COMMIT", pass)
		require.True(t, dirExists(ingestDir),
			"%s: the staged NEW data must be kept for a later re-drive (nothing is destroyed)", pass)
		rt2, err := task.newReindexTracker(lsmPath)
		require.NoError(t, err)
		require.True(t, rt2.IsMerged(), "%s: tracker must be reverted to the merged state", pass)
		require.False(t, rt2.IsSwapped(), "%s: swapped sentinel must be cleared", pass)
		require.False(t, rt2.IsSwappedProp(propName), "%s: per-prop swapped sentinel must be cleared", pass)
		require.False(t, rt2.IsStaged(), "%s: staged sentinel must be cleared", pass)
	}

	FinalizeCompletedMigrationsWithVerdict(lsmPath, task.logger,
		func(payload *ReindexTaskPayload) (bool, bool) { return false, true })
	assertRestoredToMerged("first pass")

	// Idempotency: a second boot in the same state (the task is still
	// undecided) must not promote, delete, or otherwise change anything.
	FinalizeCompletedMigrationsWithVerdict(lsmPath, task.logger,
		func(payload *ReindexTaskPayload) (bool, bool) { return false, true })
	assertRestoredToMerged("second pass")

	// The nil-verdict legacy entry point must behave like "not flipped"
	// (conservative restore) rather than promoting — its inputs give no
	// grounds for destroying either side.
	FinalizeCompletedMigrations(lsmPath, task.logger)
	assertRestoredToMerged("nil-verdict pass")
}

// TestReindexStagedSwap_RestartAfterInProcessRollback covers the
// deferred half of RollbackSwapOnShard: the in-memory rollback ran (task
// FAILED while the process was up), then the node restarted. The boot
// pass must complete the on-disk restore — OLD back under the canonical
// name, discarded sidecars and the tracker removed.
func TestReindexStagedSwap_RestartAfterInProcessRollback(t *testing.T) {
	ctx := testCtx()
	const propName = "title"

	shard, task, props, rt := prepShardToSwapBoundaryRFC220(t, ctx, "RestartRollback_"+uuid.NewString()[:8])
	require.NoError(t, task.runtimeSwap(ctx, task.logger, shard, rt, props))
	require.NoError(t, task.RollbackSwapOnShard(ctx, shard))

	lsmPath := shard.pathLSM()
	backupDir := filepath.Join(lsmPath, task.backupBucketName(propName))
	mainDir := filepath.Join(lsmPath, task.strategy.SourceBucketName(propName))
	require.NoError(t, shard.Shutdown(ctx))
	tagDirRFC220(t, backupDir, "OLD")

	FinalizeCompletedMigrations(lsmPath, task.logger)

	require.True(t, dirExists(mainDir), "canonical dir must exist after the rollback restore")
	require.Equal(t, "OLD", readDirTagRFC220(t, mainDir),
		"the preserved OLD data must be restored to the canonical name")
	require.False(t, dirExists(backupDir), "the backup-named dir was renamed to canonical")
	require.False(t, dirExists(filepath.Join(lsmPath, ".migrations", task.strategy.MigrationDirName())),
		"the tracker of the rolled-back migration must be removed")
}

// TestSemanticMigrationSchemaFlipped pins the schema-backed verdict the
// boot reconciler uses; it must stay in lockstep with
// flipSemanticMigrationSchema's mutators.
func TestSemanticMigrationSchemaFlipped(t *testing.T) {
	trueVal := true
	falseVal := false
	cls := func(mutate func(*models.Class)) *models.Class {
		c := &models.Class{
			Class: "C",
			Properties: []*models.Property{{
				Name:            "p",
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &falseVal,
				IndexSearchable: &falseVal,
			}},
			InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: false},
		}
		if mutate != nil {
			mutate(c)
		}
		return c
	}

	cases := []struct {
		name        string
		cls         *models.Class
		payload     ReindexTaskPayload
		wantFlipped bool
		wantOK      bool
	}{
		{
			name:        "change-tokenization not flipped",
			cls:         cls(nil),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeChangeTokenization, Properties: []string{"p"}, TargetTokenization: models.PropertyTokenizationField},
			wantFlipped: false, wantOK: true,
		},
		{
			name:        "change-tokenization flipped",
			cls:         cls(func(c *models.Class) { c.Properties[0].Tokenization = models.PropertyTokenizationField }),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeChangeTokenization, Properties: []string{"p"}, TargetTokenization: models.PropertyTokenizationField},
			wantFlipped: true, wantOK: true,
		},
		{
			name:        "change-tokenization-filterable flipped",
			cls:         cls(func(c *models.Class) { c.Properties[0].Tokenization = models.PropertyTokenizationField }),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeChangeTokenizationFilterable, Properties: []string{"p"}, TargetTokenization: models.PropertyTokenizationField},
			wantFlipped: true, wantOK: true,
		},
		{
			name:        "change-tokenization missing property",
			cls:         cls(nil),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeChangeTokenization, Properties: []string{"gone"}, TargetTokenization: models.PropertyTokenizationField},
			wantFlipped: false, wantOK: false,
		},
		{
			name:        "change-tokenization empty target is unanswerable",
			cls:         cls(nil),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeChangeTokenization, Properties: []string{"p"}},
			wantFlipped: false, wantOK: false,
		},
		{
			name:        "enable-filterable not flipped",
			cls:         cls(nil),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable, Properties: []string{"p"}},
			wantFlipped: false, wantOK: true,
		},
		{
			name:        "enable-filterable flipped",
			cls:         cls(func(c *models.Class) { c.Properties[0].IndexFilterable = &trueVal }),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable, Properties: []string{"p"}},
			wantFlipped: true, wantOK: true,
		},
		{
			name: "enable-searchable needs flag AND tokenization",
			cls:  cls(func(c *models.Class) { c.Properties[0].IndexSearchable = &trueVal }),
			payload: ReindexTaskPayload{
				MigrationType: ReindexTypeEnableSearchable, Properties: []string{"p"},
				TargetTokenization: models.PropertyTokenizationField,
			},
			wantFlipped: false, wantOK: true,
		},
		{
			name: "enable-searchable flipped",
			cls: cls(func(c *models.Class) {
				c.Properties[0].IndexSearchable = &trueVal
				c.Properties[0].Tokenization = models.PropertyTokenizationField
			}),
			payload: ReindexTaskPayload{
				MigrationType: ReindexTypeEnableSearchable, Properties: []string{"p"},
				TargetTokenization: models.PropertyTokenizationField,
			},
			wantFlipped: true, wantOK: true,
		},
		{
			name:        "change-algorithm not flipped",
			cls:         cls(nil),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeChangeAlgorithm, Properties: []string{"p"}},
			wantFlipped: false, wantOK: true,
		},
		{
			name:        "change-algorithm flipped",
			cls:         cls(func(c *models.Class) { c.InvertedIndexConfig.UsingBlockMaxWAND = true }),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeChangeAlgorithm, Properties: []string{"p"}},
			wantFlipped: true, wantOK: true,
		},
		{
			name:        "format-only type is unanswerable",
			cls:         cls(nil),
			payload:     ReindexTaskPayload{MigrationType: ReindexTypeRebuildSearchable, Properties: []string{"p"}},
			wantFlipped: false, wantOK: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flipped, ok := semanticMigrationSchemaFlipped(tc.cls, &tc.payload)
			require.Equal(t, tc.wantOK, ok, "ok")
			require.Equal(t, tc.wantFlipped, flipped, "flipped")
		})
	}
}

// prepShardToSwapBoundaryRFC220 builds a fresh single-property shard,
// ingests a handful of objects, and drives the generic reindex task in
// STAGED mode (as the semantic-task constructors configure it) through
// iteration + runtimePrepare so the next step is runtimeSwap's Phase 2a.
// Mirrors the drive-to-swap-boundary setup in
// TestRecoveryConvergence_MidPropSwap_Loop.
func prepShardToSwapBoundaryRFC220(
	t *testing.T, ctx context.Context, className string,
) (*Shard, *ShardReindexTaskGeneric, []string, reindexTracker) {
	t.Helper()
	class := newTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)

	objs := make([]*storobj.Object, 8)
	for i := range objs {
		objs[i] = createTestObjectWithText(className, "hello world document "+uuid.NewString())
		require.NoError(t, shard.PutObject(ctx, objs[i]))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

	// Drive to the IsMerged sentinel (the runtimePrepare boundary) via the
	// shared dispatch-matrix inline-path helper, instead of re-inlining the
	// skipSwapOnFinish + OnAfterLsmInit(Async) drive loop that already
	// exists (near-verbatim) in several sibling reindex test files.
	dispatchMatrixDriveToMerged(t, ctx, shard, task, dispatchMatrixPathInline)

	// Semantic migrations run in staged mode (the constructors in
	// inverted_reindexer_{searchable,filterable}_retokenize.go etc. set
	// this); the drive-to-merged above ran with the flag off so the
	// staged gate in OnBeforeLsmInit doesn't interfere.
	task.config.stagedSwapCommit = true

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	props, err := task.readPropsToReindex(rt)
	require.NoError(t, err)

	return shard, task, props, rt
}

// writeStagedRecoveryPayloadRFC220 persists the payload.mig recovery
// record the DTM flow writes before the swap; the boot reconciler reads
// it as the verdict input for a staged gen.
func writeStagedRecoveryPayloadRFC220(t *testing.T, task *ShardReindexTaskGeneric, lsmPath, className string) {
	t.Helper()
	rec := reindexRecoveryRecord{
		TaskID:      "rfc220-test-task",
		TaskVersion: 1,
		UnitID:      "unit-0",
		Payload: ReindexTaskPayload{
			MigrationType: ReindexTypeChangeAlgorithm,
			Collection:    className,
			Properties:    []string{"title"},
		},
	}
	encoded, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, task.SaveRecoveryPayload(lsmPath, encoded))
}

// tagDirRFC220 drops a marker file into dir so a later rename chain can
// be asserted by content rather than by name.
func tagDirRFC220(t *testing.T, dir, tag string) {
	t.Helper()
	require.True(t, dirExists(dir), "cannot tag non-existent dir %s", dir)
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".rfc220_tag"), []byte(tag), 0o644))
}

// readDirTagRFC220 reads the marker dropped by tagDirRFC220.
func readDirTagRFC220(t *testing.T, dir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, ".rfc220_tag"))
	require.NoError(t, err)
	return string(data)
}
