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
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins the QA base-sync carry-check finding (weaviate/weaviate#12221,
// 2026-07-17 13:04 comment): OnAfterLsmInit's rt.IsSwapped()&&!rt.IsTidied()
// crash-recovery branch (inverted_reindex_task_generic.go, "swapped, not
// tidied. starting backup buckets") re-registers the backup-window
// double-write callback, but the recovery-only shim
// (shardReindexerV3RecoveryOnly.RunBeforeLsmInit, reindex_recovery.go:392)
// deliberately skips OnBeforeLsmInit, and the SAME task instance is later
// reused by ReindexProvider.OnGroupCompleted's RunSwapOnShard call
// (reindex_provider.go:93-96) - whose rt.IsSwapped() dispatch branch
// (tidyBackupBuckets + finalizeMigrationAfterRecovery) never routes through
// runtimeSwap, the ONLY place that calls disableCallbacks(). The
// registration survives the migration's completion and keeps firing
// trackMigratingPropLength for every subsequent write to the migrating
// prop, alongside the live SetPropertyLengths path once the cluster-wide
// schema flip lands - double-tallying every post-recovery write until the
// shard restarts again.

// synthesizeSwappedNotTidied drives a real EnableSearchable migration to
// completion, then removes the on-disk tidied sentinel to synthetically
// reproduce a crash between markSwapped and markTidied inside the ORIGINAL
// runtimeSwap call - same technique as
// TestRecoveryConvergence_EnableSearchable_FromEachState's
// "IsSwapped_synthetic_tidied_removed" case.
func synthesizeSwappedNotTidied(t *testing.T, shard *Shard, task *ShardReindexTaskGeneric) {
	t.Helper()
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	ftr := rt.(*fileReindexTracker)
	require.NoError(t, os.Remove(
		filepath.Join(ftr.config.migrationPath, ftr.config.filenameTidied),
	))
}

func TestReindex_EnableSearchable_CrashRecoveryMidTidy_LeakedCallbackDoubleTalliesPostFlipWrite(t *testing.T) {
	const numObjects = 20
	ctx := testCtx()

	className := "EnableSearchableCrashMidTidy_" + uuid.NewString()[:8]
	vFalse, vTrue := false, true
	// Filterable=true (live index) so AnalyzeObject doesn't gate the prop out
	// of the double-write callback machinery for from-scratch writes - same
	// shape as newSidecarBackfillSearchableCallbackFixture.
	class := newSidecarBackfillTextClass(className, &vTrue, &vFalse)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	objects := sidecarBackfillTextObjects(className, numObjects, 0)
	for _, obj := range objects {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, wrapped := newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted)

	synthesizeSwappedNotTidied(t, shard, task)

	// Simulated restart: a fresh task instance, callback registration state
	// starts empty. shardReindexerV3RecoveryOnly.RunBeforeLsmInit is a
	// documented no-op for DTM-driven semantic migrations, so only
	// OnAfterLsmInit runs here - never OnBeforeLsmInit.
	task2, wrapped2 := newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task2.OnAfterLsmInit(ctx, shard))

	rt2, err := task2.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt2.IsSwapped(), "sanity: must still report swapped")
	require.False(t, rt2.IsTidied(), "sanity: must still be inside the crash-recovery window this bug targets")

	// The real recovery dispatch: ReindexProvider.OnGroupCompleted calls
	// RunSwapOnShard on the SAME (recovered) task instance.
	require.NoError(t, task2.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped2.migrationCompleted)

	rt3, err := task2.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt3.IsTidied(), "sanity: recovery dispatch must have completed tidying")

	// Simulate the cluster-wide schema flip ReindexProvider.OnTaskCompleted
	// performs once every node's OnGroupCompleted has acked: the live schema
	// now reports the migrating prop as searchable, so SetPropertyLengths
	// starts tracking it on the ordinary (non-migration) write path.
	class.Properties[0].IndexSearchable = &vTrue

	preSum, preCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)
	require.EqualValues(t, numObjects, preCount,
		"sanity: the recovery dispatch's recompute must have tallied every pre-existing object exactly once")

	// One write landing strictly after recovery finalized AND after the
	// schema flip - the exact shape a rolling upgrade would produce between
	// this node's ack and the cluster-wide RAFT schema commit.
	postFlip := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:    strfmt.UUID(uuid.NewString()),
			Class: className,
			Properties: map[string]interface{}{
				sidecarBackfillTextProp: "alpha bravo charlie", // 3 words
			},
		},
	}
	// The leaked callback's postings leg is a MORE severe manifestation of
	// the same root cause than the tally alone: it still resolves the
	// backup bucket by NAME (resolveDoubleWriteBucket finds it non-nil in
	// the store's bucket map - trimOlderGenerationsLocked only removes the
	// ON-DISK directory, never unregisters the in-memory bucket), so the
	// mirror write dereferences a bucket whose backing segment files are
	// already gone. Pre-fix this fails the user's write outright, not just
	// the tally.
	require.NoErrorf(t, shard.PutObject(ctx, postFlip),
		"BUG regression (weaviate/0-weaviate-issues#322 base-sync finding C): a write landing after crash-recovery "+
			"mid-tidy finalization and after the cluster schema flip must not fail - the leaked backup-window "+
			"double-write callback (registered by OnAfterLsmInit's IsSwapped&&!IsTidied branch, never disarmed "+
			"because the recovery dispatch in RunSwapOnShard doesn't route through runtimeSwap's defer) tries to "+
			"mirror postings into a backup bucket whose on-disk segment files trimOlderGenerationsLocked already removed")

	postSum, postCount, _, err := shard.GetPropertyLengthTracker().PropertyTally(sidecarBackfillTextProp)
	require.NoError(t, err)

	assert.Equalf(t, preCount+1, postCount,
		"BUG regression (weaviate/0-weaviate-issues#322 base-sync finding C): a write landing after crash-recovery "+
			"mid-tidy finalization and after the cluster schema flip must tally BM25 COUNT exactly ONCE - the leaked "+
			"backup-window double-write callback (registered by OnAfterLsmInit's IsSwapped&&!IsTidied branch, never "+
			"disarmed because the recovery dispatch in RunSwapOnShard doesn't route through runtimeSwap's defer) "+
			"double-counts alongside the live SetPropertyLengths path - got %d, want %d", postCount, preCount+1)
	assert.Equalf(t, preSum+3, postSum,
		"BUG regression (weaviate/0-weaviate-issues#322 base-sync finding C): BM25 SUM must increase by exactly the "+
			"new object's 3-word contribution, not double it - got %d, want %d", postSum, preSum+3)
}
