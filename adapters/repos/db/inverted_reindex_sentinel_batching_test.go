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
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins the split that keeps Phase 2a inside its wall-clock budget:
// markSwappedPropUnsynced must NOT fsync, and syncSentinelDir must.
//
// Both halves are observed through the same unreadable-dir technique used by
// the other durability tests: 0o311 is write+execute, so the sentinel write
// still succeeds while diskio.Fsync's O_RDONLY open fails with EACCES. If
// someone makes the per-prop write durable again the first case fails (the
// cost returns to the timed loop); if someone drops the batched fsync the
// second case fails (the durability is silently gone).
func TestFileReindexTracker_SentinelWriteIsBatchedNotPerProp(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test cannot run as root")
	}

	t.Run("per-prop write does not fsync", func(t *testing.T) {
		tr := newTestReindexTracker(t)
		require.NoError(t, os.Chmod(tr.config.migrationPath, 0o311))
		t.Cleanup(func() { _ = os.Chmod(tr.config.migrationPath, 0o777) })

		// An unreadable parent is fatal to an fsync but not to the write,
		// so success here is what proves no fsync happened.
		require.NoError(t, tr.markSwappedPropUnsynced("title"),
			"the per-prop sentinel write must not fsync — that cost belongs in syncSentinelDir")
		require.True(t, tr.IsSwappedProp("title"), "the sentinel must still land")
	})

	t.Run("batched sync fsyncs the migration dir", func(t *testing.T) {
		tr := newTestReindexTracker(t)
		require.NoError(t, tr.markSwappedPropUnsynced("title"))
		require.NoError(t, os.Chmod(tr.config.migrationPath, 0o311))
		t.Cleanup(func() { _ = os.Chmod(tr.config.migrationPath, 0o777) })

		err := tr.syncSentinelDir()

		require.Error(t, err, "syncSentinelDir must fsync the migration dir, not no-op")
		require.ErrorIs(t, err, fs.ErrPermission)

		var pathErr *fs.PathError
		require.ErrorAs(t, err, &pathErr)
		require.Equal(t, tr.config.migrationPath, pathErr.Path)
	})
}

// Pins that runtimeSwap actually runs the batched fsync. The tracker-level
// test above proves syncSentinelDir fsyncs; this proves the Phase 2a loop
// calls it, so deleting the call from runtimeSwap cannot pass unnoticed.
func TestRuntimeSwap_Phase2a_BatchedSentinelFsyncRuns(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based test cannot run as root")
	}

	ctx := testCtx()
	className := "TestPhase2aBatchedFsync"
	propNames := []string{"title", "description"}
	class := newTestClassWithProps(className, propNames)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeMultiPropConvergenceObjects(t, 5, className, propNames) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
	task := newTestTask(idx.logger, strategy)

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

	// Strip read permission only once prepare is done, so the sentinel
	// writes inside the loop still succeed and the trailing fsync is the
	// only thing that can fail.
	migPath := rt.(*fileReindexTracker).config.migrationPath
	require.NoError(t, os.Chmod(migPath, 0o311))
	t.Cleanup(func() { _ = os.Chmod(migPath, 0o777) })

	err = task.runtimeSwap(ctx, task.logger, shard, rt, props)

	require.Error(t, err, "runtimeSwap must fsync the sentinel dir after Phase 2a and propagate the failure")
	require.ErrorIs(t, err, fs.ErrPermission)
	require.Contains(t, err.Error(), "syncing per-prop swap sentinels")

	// The sentinels themselves landed — only their durability step failed.
	for _, p := range props {
		require.True(t, rt.IsSwappedProp(p),
			"prop %q sentinel must be written before the batched fsync", p)
	}

	// The ordering invariant that makes batching safe: the aggregate must
	// never be durable while the per-prop sentinels are not. Moving the
	// batched fsync after markSwapped would break exactly this.
	require.False(t, rt.IsSwapped(),
		"the aggregate sentinel must not be set when the batched fsync failed")
}
