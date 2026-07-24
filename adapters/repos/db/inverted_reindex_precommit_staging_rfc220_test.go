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

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// This file pins the pre-commit staging protocol for
// weaviate/0-weaviate-issues#220: a semantic migration's swap must not
// destroy a shard's OLD backup before the cluster-wide ack barrier
// confirms every unit succeeded, or a sibling failure leaves a permanent
// bucket↔schema inversion with no way to roll back.
//
// Protocol: STAGE (pointer flip + backup rename; nothing destroyed) →
// COMMIT ([ShardReindexTaskGeneric.CommitSwapOnShard], post-barrier) or
// ROLLBACK ([ShardReindexTaskGeneric.RollbackSwapOnShard], on
// FAILED/CANCELLED) → crash recovery via
// [FinalizeCompletedMigrationsWithVerdict].

// TestReindexInversion_SiblingSwapFailureLeavesNoRollback_RFC220 pins
// weaviate/0-weaviate-issues#220: after unit A stages its swap and
// sibling unit B fails, A's OLD-tokenized backup dir must still exist so
// the task can be rolled back to OLD tokenization.
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

// TestReindexStagedSwap_SiblingFailureRollsBackStagedUnits: units A and
// B stage, unit C fails — RollbackSwapOnShard must restore A and B's
// OLD data under the canonical name without a restart.
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

// TestReindexStagedSwap_PartialStageFailureRollsBackSwappedProps: a
// multi-property task flips its first prop then fails on the second
// (torn Phase 2a, no staged.mig) — rollback must restore exactly the
// props that had already flipped.
func TestReindexStagedSwap_PartialStageFailureRollsBackSwappedProps(t *testing.T) {
	ctx := testCtx()
	props := []string{"alpha", "beta"}
	className := "PartialStage_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, props)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for i := 0; i < 8; i++ {
		obj := &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					"alpha": "hello alpha " + uuid.NewString(),
					"beta":  "hello beta " + uuid.NewString(),
				},
			},
		}
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)
	dispatchMatrixDriveToMerged(t, ctx, shard, task, dispatchMatrixPathInline)
	task.config.stagedSwapCommit = true

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	taskProps, err := task.readPropsToReindex(rt)
	require.NoError(t, err)
	require.Len(t, taskProps, 2)

	// First prop flips, second fails — a torn Phase 2a.
	origSwapFn := task.processOneSwapPropFn
	var swappedProp string
	task.processOneSwapPropFn = func(ctx context.Context, store *lsmkv.Store, rt reindexTracker, propIdx int, propName string) (*lsmkv.Bucket, error) {
		if swappedProp != "" {
			return nil, fmt.Errorf("injected mid-Phase-2a failure on prop %q", propName)
		}
		swappedProp = propName
		return origSwapFn(ctx, store, rt, propIdx, propName)
	}
	require.Error(t, task.runtimeSwap(ctx, task.logger, shard, rt, taskProps))
	require.NotEmpty(t, swappedProp, "exactly one prop must have flipped before the failure")
	require.True(t, rt.IsSwappedProp(swappedProp))
	require.False(t, rt.IsStaged(), "a torn swap must not be recorded as fully staged")

	// Task verdict: FAILED. The rollback must restore the flipped prop.
	require.NoError(t, task.RollbackSwapOnShard(ctx, shard))

	for _, propName := range taskProps {
		bucketName := helpers.BucketSearchableFromPropNameLSM(propName)
		require.Equal(t, lsmkv.StrategyMapCollection, shard.store.Bucket(bucketName).Strategy(),
			"prop %q must serve the OLD (map) data after the partial-stage rollback", propName)
		require.False(t, rt.IsSwappedProp(propName))
	}
	require.True(t, rt.IsUnswapped(), "the deferred on-disk restore must be recorded")
}

// TestReindexStagedSwap_CommitSingleShardDegenerate: single-unit stage
// (via RunSwapOnShard) then commit (via CommitSwapOnShard) must reach
// the same end state as the legacy inline commit, just split across the
// barrier.
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

// TestReindexStagedSwap_RestartDuringStagedWindow_TaskCommits: the node
// dies after staging while the task reaches FINISHED elsewhere (schema
// flip already committed). At the next boot the schema-backed verdict
// says "flipped", and FinalizeCompletedMigrationsWithVerdict must
// complete the commit.
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

// TestReindexStagedSwap_RestartDuringStagedWindow_TaskPendingOrFailed:
// the node dies after staging and the task did not commit — the boot
// verdict is "not flipped", so FinalizeCompletedMigrationsWithVerdict
// must restore OLD and revert the tracker to merged, destroying nothing.
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

// TestReindexStagedSwap_RestartAfterInProcessRollback: the in-memory
// rollback ran (task FAILED while the process was up), then the node
// restarted — the boot pass must complete the on-disk restore (OLD
// under the canonical name, sidecars and tracker removed).
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

// TestReindexStagedSwap_RealLaggingFollowerOrdering_ReResolvesOnFlip drives
// the REAL lagging-follower ordering QA finding 2 named — NOT the
// injected-verdict shortcut. A node stages + acks its swap, then crashes
// before the cluster-wide schema flip applies on it. It boots against the
// still-PRE-flip schema, so the production shard-init verdict
// ([FinalizeCompletedMigrationsWithVerdict] reading the real getSchema) is a
// false-negative and reverts the staged swap to merged (serving OLD under the
// soon-to-be-flipped schema). No DTM re-drives it (Noop reindexer). Then the
// flip UPDATE_PROPERTY entry applies — modelled by flipping the class the
// getSchema returns — and the real schema-apply hook
// [Shard.updatePropertyBuckets] fires [Shard.reResolveStagedSwapsOnSchemaFlip],
// which must converge the node to NEW WITHOUT a restart, using the real
// resolution functions (RunSwapOnShard + CommitSwapOnShard).
func TestReindexStagedSwap_RealLaggingFollowerOrdering_ReResolvesOnFlip(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25
	className := "ReResolveOnFlip_" + uuid.NewString()[:8]
	// Word tokenization is the PRE-flip schema; the migration retokenizes to
	// field.
	class := newTestClassWithProps(className, []string{propName})
	canonicalBucket := helpers.BucketFromPropNameLSM(propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	shardName := shard.Name()
	lsmPath := shard.pathLSM()

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	oldFP := fingerprintRoaringSetBucket(t, shard.store.Bucket(canonicalBucket))

	// Stage the swap through the real filterable-retokenize path (staged
	// mode). After this the canonical bucket serves the NEW field-tokenized
	// data and the OLD data survives as the backup, awaiting the verdict.
	stageTask := NewRuntimeFilterableRetokenizeTask(idx.logger, propName,
		models.PropertyTokenizationField, className, className, 1)
	require.NoError(t, stageTask.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, stageTask.RunPrepareOnShard(ctx, shard))
	require.NoError(t, stageTask.RunSwapOnShard(ctx, shard))

	rt, err := stageTask.newReindexTracker(lsmPath)
	require.NoError(t, err)
	require.True(t, rt.IsStaged(), "swap must be staged (awaiting the cluster-wide verdict)")
	require.False(t, rt.IsTidied(), "staged swap must not be committed yet")
	newFP := fingerprintRoaringSetBucket(t, shard.store.Bucket(canonicalBucket))
	require.NotEqual(t, oldFP, newFP,
		"field tokenization must produce a different inverted fingerprint than word")

	// Persist the payload.mig recovery record the boot reconciler and the
	// flip-apply re-resolve both consult.
	rec := reindexRecoveryRecord{
		TaskID: "reresolve-test", TaskVersion: 1, UnitID: "unit-0",
		Payload: ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenizationFilterable,
			Collection:         className,
			Properties:         []string{propName},
			TargetTokenization: models.PropertyTokenizationField,
		},
	}
	encoded, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, stageTask.SaveRecoveryPayload(lsmPath, encoded))

	// --- Crash before the flip applied on this node. ---
	require.NoError(t, shard.Shutdown(ctx))

	// --- Boot with the still-PRE-flip schema. Shard init consults the
	// schema-backed verdict; it is a false-negative and reverts to merged. A
	// Noop reindexer guarantees NOTHING re-drives the swap — the only
	// convergence driver under test is the flip-apply re-resolve. ---
	require.Equal(t, models.PropertyTokenizationWord, class.Properties[0].Tokenization,
		"precondition: the schema must still be pre-flip at boot")
	idx.shardReindexer = NewShardReindexerV3Noop()
	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)

	rt2, err := stageTask.newReindexTracker(lsmPath)
	require.NoError(t, err)
	require.True(t, rt2.IsMerged(), "boot must revert the staged swap to merged")
	require.False(t, rt2.IsStaged(), "staged sentinel must be cleared on revert")
	require.False(t, rt2.IsTidied(), "revert must not commit")
	require.Equal(t, oldFP, fingerprintRoaringSetBucket(t, shard2.store.Bucket(canonicalBucket)),
		"the bug state: the node serves OLD data under the soon-to-be-flipped schema")

	// --- The flip UPDATE_PROPERTY entry now applies on this node. The
	// fakeSchemaGetter returns the class pointer, so flipping its tokenization
	// IS the applied schema flip. ---
	class.Properties[0].Tokenization = models.PropertyTokenizationField

	// --- Drive the REAL schema-apply hook (updatePropertyBuckets), which
	// fires the corrective re-resolve. ---
	eg := enterrors.NewErrorGroupWrapper(idx.logger)
	shard2.updatePropertyBuckets(ctx, eg, class.Properties[0])
	require.NoError(t, eg.Wait())

	rt3, err := stageTask.newReindexTracker(lsmPath)
	require.NoError(t, err)
	require.True(t, rt3.IsTidied(), "the flip-apply re-resolve must commit the staged swap (tidied)")
	require.False(t, rt3.IsStaged(), "the staged sentinel must be retired on commit")
	require.Equal(t, newFP, fingerprintRoaringSetBucket(t, shard2.store.Bucket(canonicalBucket)),
		"convergence: the canonical bucket serves NEW (field-tokenized) data WITHOUT a restart")
	backupDir := filepath.Join(lsmPath, stageTask.backupBucketName(propName))
	require.False(t, dirExists(backupDir), "commit must trim the OLD backup")

	// Idempotency: a re-fired flip-apply must be a no-op.
	eg2 := enterrors.NewErrorGroupWrapper(idx.logger)
	shard2.updatePropertyBuckets(ctx, eg2, class.Properties[0])
	require.NoError(t, eg2.Wait())
	require.Equal(t, newFP, fingerprintRoaringSetBucket(t, shard2.store.Bucket(canonicalBucket)),
		"a second flip-apply must not disturb the converged bucket")

	require.NoError(t, shard2.Shutdown(ctx))
}

// TestReindexStagedSwap_ReResolve_PreservesRevertWindowWrites is the
// weaviate/weaviate#12211 verification (QA finding 4). It proves that a
// write accepted DURING the revert-to-merged window survives the flip-apply
// re-resolve — i.e. the re-drive is convergent, not a promotion of a stale
// frozen NEW bucket.
//
// The mechanism (all pre-existing machinery, no new code): on restart the
// recovery reindexer fires OnAfterLsmInit, whose merged-state branch re-arms
// the ingest double-write callbacks (task_generic.go:1480 — no staged gate).
// So while the reverted node serves OLD, every write ALSO lands in the NEW
// ingest bucket under the target tokenization. The flip-apply re-resolve then
// promotes that ingest bucket, which already contains the window writes. This
// is why "revert-to-merged collapses the write window and the re-drive is
// convergent": 12211's residual closes without a dedicated write overlay.
func TestReindexStagedSwap_ReResolve_PreservesRevertWindowWrites(t *testing.T) {
	ctx := testCtx()
	const propName = "title"
	const numObjects = 25
	// A single-token value so word and field tokenization agree on the term,
	// keeping the assertion about capture (not tokenization) unambiguous.
	const windowTerm = "quokka"
	className := "ReResolveWindowWrite_" + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{propName})
	canonicalBucket := helpers.BucketFromPropNameLSM(propName)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	shardName := shard.Name()
	lsmPath := shard.pathLSM()

	for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Stage the swap.
	stageTask := NewRuntimeFilterableRetokenizeTask(idx.logger, propName,
		models.PropertyTokenizationField, className, className, 1)
	require.NoError(t, stageTask.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, stageTask.RunPrepareOnShard(ctx, shard))
	require.NoError(t, stageTask.RunSwapOnShard(ctx, shard))

	rec := reindexRecoveryRecord{
		TaskID: "reresolve-window-test", TaskVersion: 1, UnitID: "unit-0",
		Payload: ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenizationFilterable,
			Collection:         className,
			Properties:         []string{propName},
			TargetTokenization: models.PropertyTokenizationField,
		},
	}
	encoded, err := json.Marshal(rec)
	require.NoError(t, err)
	require.NoError(t, stageTask.SaveRecoveryPayload(lsmPath, encoded))

	// --- Crash before the flip applied. ---
	require.NoError(t, shard.Shutdown(ctx))

	// --- Boot: revert to merged against the pre-flip schema. Model the
	// production recovery reindexer, which fires OnAfterLsmInit to re-install
	// the ingest double-write callbacks. ---
	idx.shardReindexer = NewShardReindexerV3Noop()
	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err)
	shard2 := shd2.(*Shard)
	idx.shards.Store(shardName, shd2)

	rt2, err := stageTask.newReindexTracker(lsmPath)
	require.NoError(t, err)
	require.True(t, rt2.IsMerged(), "boot must revert the staged swap to merged")

	// The recovery reindexer's OnAfterLsmInit re-arms the ingest double-writes
	// for the merged tracker (production restart behavior).
	rearmTask := NewRuntimeFilterableRetokenizeTask(idx.logger, propName,
		models.PropertyTokenizationField, className, className, 1)
	require.NoError(t, rearmTask.OnAfterLsmInit(ctx, shard2))

	// --- A write accepted DURING the revert window. It lands in the OLD
	// canonical bucket AND double-writes into the NEW ingest bucket. ---
	windowObj := createTestObjectWithText(className, windowTerm)
	require.NoError(t, shard2.PutObject(ctx, windowObj))

	// --- Flip applies; the re-resolve promotes the ingest bucket. ---
	class.Properties[0].Tokenization = models.PropertyTokenizationField
	eg := enterrors.NewErrorGroupWrapper(idx.logger)
	shard2.updatePropertyBuckets(ctx, eg, class.Properties[0])
	require.NoError(t, eg.Wait())

	rt3, err := stageTask.newReindexTracker(lsmPath)
	require.NoError(t, err)
	require.True(t, rt3.IsTidied(), "the flip-apply re-resolve must commit the swap")

	// Convergence including the window write: the term of the object written
	// during the revert window MUST be present in the promoted NEW bucket.
	convergedFP := fingerprintRoaringSetBucket(t, shard2.store.Bucket(canonicalBucket))
	require.Contains(t, convergedFP, windowTerm,
		"weaviate/weaviate#12211: a write accepted during the revert window must survive "+
			"the flip-apply re-resolve — the ingest double-write captured it into NEW and "+
			"the re-resolve promoted NEW, so no restart-residual write overlay is needed")
	require.NotEmpty(t, convergedFP[windowTerm],
		"the window write's term must have a non-empty postings list in the converged NEW bucket")

	require.NoError(t, shard2.Shutdown(ctx))
}
