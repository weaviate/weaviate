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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// The zero-total pending retry runs on a flag alone, and the backup release sweep
// reaches every loaded shard, so it can arrive uninvited. The orphan audit pauses
// compaction directly and the store's pause is a shared boolean, so an unguarded
// retry restarts compaction underneath the audit's file removals.
//
// The observable for "the retry did not run" is the vector callback control: it is
// activated by the same completeResumeLocked error group as store.ResumeCompaction,
// so it staying inactive means neither ran. (The store's own pause state is not
// reachable from this package.)
func TestPendingResumeRetryDefersToReindexInFlight(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	// Leave the shard with a pending physical resume, exactly as a failed release
	// does: unregistering makes the resume's Activate fail with ErrorCallbackNotFound.
	require.NoError(t, shard.HaltForTransfer(ctx, "test:opA", false, 0))
	require.NoError(t, shard.cycleCallbacks.vectorCombinedCallbacksCtrl.Unregister(ctx))
	require.Error(t, shard.resumeMaintenanceCycles(ctx, "test:opA"))

	shard.haltForTransferMux.Lock()
	require.True(t, shard.maintenanceResumePending, "pre-condition: the failure must be recorded")
	require.Zero(t, shard.haltTotalLocked(), "pre-condition: the owner bookkeeping is already cleared")
	shard.haltForTransferMux.Unlock()

	// Heal the failing leg but leave it deactivated, so any retry that DOES run is
	// visible as an activation rather than a second failure.
	rebindVectorCallbacksCtrl(t, shard)

	// The audit window: DTM reports a live reindex task and the audit holds the
	// store's compaction pause.
	index.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{
		{index.Config.ClassName.String(), shard.name}: true,
	}))
	require.NoError(t, shard.store.PauseCompaction(ctx))

	// A foreign backup's release sweeps every loaded shard, this one included.
	require.NoError(t, index.ReleaseBackup(ctx, backup.NewOp("foreign")))

	shard.haltForTransferMux.Lock()
	require.True(t, shard.maintenanceResumePending,
		"the deferred retry must keep its flag so a later attempt still runs it")
	shard.haltForTransferMux.Unlock()
	require.False(t, shard.cycleCallbacks.vectorCombinedCallbacksCtrl.IsActive(),
		"the retry must not resume maintenance while the audit holds the store pause")

	// The audit finishes: it drops its own pause, and DTM no longer lists the task.
	require.NoError(t, shard.store.ResumeCompaction(ctx))
	index.db.SetShardReindexActivityLookup(makeActivityBuilder(map[[2]string]bool{}))

	require.NoError(t, shard.resumeMaintenanceCycles(ctx, "test:opA"))

	shard.haltForTransferMux.Lock()
	defer shard.haltForTransferMux.Unlock()
	require.False(t, shard.maintenanceResumePending,
		"once the window closes the next attempt must complete the retry")
	require.True(t, shard.cycleCallbacks.vectorCombinedCallbacksCtrl.IsActive(),
		"the completed retry must actually re-run the physical resume")
}
