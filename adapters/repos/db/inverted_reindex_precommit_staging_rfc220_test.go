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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestReindexInversion_SiblingSwapFailureLeavesNoRollback_RFC220 is the RED
// pinning test for https://github.com/weaviate/0-weaviate-issues/issues/220.
//
// # Mechanism it pins
//
// A semantic (tokenization-changing) migration commits its swap PER SHARD
// inside [ShardReindexTaskGeneric.runtimeSwap], with no gating on the
// cluster-wide ack barrier that decides whether the schema flip commits:
//
//	Phase 2a  store.SwapBucketPointer(mainName, ingestName)   // canonical → NEW, in-memory
//	Phase 2b  os.Rename(oldMainDir, backupDir)                // OLD data preserved as backup
//	Phase 2c  trimOlderGenerationsLocked(...)                 // DELETES the OLD backup dir
//
// The ack barrier only runs AFTER every unit's runtimeSwap has already
// returned. So the sequence for a two-replica property is:
//
//	unit A: runtimeSwap succeeds  → canonical=NEW, OLD backup TRIMMED (gone)
//	unit B: runtimeSwap fails     → its swap sub-task errors
//	barrier: sees B failed        → task → FAILED → flipSemanticMigrationSchema SKIPPED
//	result: schema=OLD, unit A bucket=NEW  →  bucket↔schema inversion on A
//
// Post-hoc rollback of unit A is PHYSICALLY IMPOSSIBLE: Phase 2c already
// deleted A's OLD-tokenized backup dir during the same runtimeSwap, before
// the barrier could observe B's failure. The in-memory tokenization overlay
// masks the inversion until the next restart; then it is permanent.
//
// # Why this is the load-bearing assertion
//
// A correct pre-commit-staging design (the fix proposed in #220) guarantees
// the invariant: *if ANY unit's swap fails, every already-committed unit must
// still hold its OLD-tokenized backup, so the whole task can be rolled back to
// OLD.* The single fact that violates that invariant here is that unit A's OLD
// backup dir is gone. The test asserts the dir MUST survive a sibling failure.
//
//   - RED on current code: trimOlderGenerationsLocked ran inside runtimeSwap,
//     so the backup dir is already gone → assertion fails.
//   - GREEN under the minimal slice: deferring the destructive Phase-2c trim
//     until the ack barrier confirms all-success keeps the OLD data on disk, so
//     a rollback is at least physically possible.
//
// The in-memory pointer flip (Phase 2a) is NOT undone by that minimal slice —
// staging the flip itself + wiring the actual rollback and its recovery
// sentinels is the larger refactor tracked by the RFC. This test pins only the
// destructive, irreversible part: destroying the OLD data before the barrier.
func TestReindexInversion_SiblingSwapFailureLeavesNoRollback_RFC220(t *testing.T) {
	ctx := testCtx()
	const propName = "title"

	// Two independent shards model two replicas (units) of the same property
	// in one distributed reindex task. Each drives its own runtimeSwap; the
	// ack barrier that gates the cluster-wide schema flip only fires after
	// both have returned.
	shardA, taskA, propsA, rtA := prepShardToSwapBoundaryRFC220(t, ctx, "InversionUnitA_"+uuid.NewString()[:8])
	shardB, taskB, propsB, rtB := prepShardToSwapBoundaryRFC220(t, ctx, "InversionUnitB_"+uuid.NewString()[:8])
	defer shardA.Shutdown(ctx)
	defer shardB.Shutdown(ctx)

	searchBucketName := helpers.BucketSearchableFromPropNameLSM(propName)

	// --- Unit A: the sibling that commits its swap fully (flip + trim). ---
	require.NoError(t, taskA.runtimeSwap(ctx, taskA.logger, shardA, rtA, propsA),
		"unit A swap should succeed (models the sub-task that commits first)")

	require.Equal(t, lsmkv.StrategyInverted, shardA.store.Bucket(searchBucketName).Strategy(),
		"unit A must have flipped its canonical bucket pointer to the NEW-tokenized data")
	require.True(t, rtA.IsSwapped(), "unit A must be marked swapped (committed)")

	backupDirA := filepath.Join(shardA.pathLSM(), taskA.backupBucketName(propName))

	// --- Unit B: the sibling whose swap fails AFTER A committed. ---
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

	// The ack barrier now observes unit B's failure → the task transitions to
	// FAILED → flipSemanticMigrationSchema is skipped → the cluster schema
	// stays at OLD tokenization while unit A's bucket already serves NEW. That
	// is the inversion. The ONLY thing that could undo it is rolling unit A
	// back to OLD — which requires A's OLD-tokenized backup to still exist.

	require.True(t, dirExists(backupDirA),
		"RFC #220 pin: after a sibling unit's swap failed, unit A's OLD-tokenized backup dir "+
			"(%s) MUST still exist so the task can be rolled back to OLD tokenization. On current "+
			"code trimOlderGenerationsLocked deleted it inside runtimeSwap — BEFORE the ack barrier "+
			"could observe the failure — so the schema↔bucket inversion on unit A is permanent and "+
			"unrollbackable. Fix: defer the destructive Phase-2c trim until the barrier confirms "+
			"every unit succeeded (pre-commit staging).", backupDirA)
}

// prepShardToSwapBoundaryRFC220 builds a fresh single-property shard, ingests a
// handful of objects, and drives the generic reindex task through iteration +
// runtimePrepare so the next step is runtimeSwap's Phase 2a. Mirrors the
// drive-to-swap-boundary setup in TestRecoveryConvergence_MidPropSwap_Loop.
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

	// skipSwapOnFinish stops the async loop at IsReindexed() so we drive
	// runtimePrepare + runtimeSwap explicitly.
	task.skipSwapOnFinish.Store(true)
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	props, err := task.readPropsToReindex(rt)
	require.NoError(t, err)
	require.NoError(t, task.runtimePrepare(ctx, task.logger, shard, rt, props))

	return shard, task, props, rt
}
