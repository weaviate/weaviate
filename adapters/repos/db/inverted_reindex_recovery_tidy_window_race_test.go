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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

// tidyWindowRaceMarkerWord is outside sidecarBackfillTextObjects' 5-word
// dictionary, so an injected object's posting is unambiguous.
const tidyWindowRaceMarkerWord = "yankee"

// TestReindex_EnableSearchable_RecoveryTidyWindowRace_DisarmBeforeTidy pins
// weaviate/weaviate#12221 finding N1: a write racing RunSwapOnShard's
// IsSwapped()&&!IsTidied() recovery branch, landing in the window between
// tidyBackupBuckets removing the backup bucket's on-disk directory and
// finalizeMigrationAfterRecovery's deferred disableCallbacks() call, must
// not fail.
//
// Mechanism: the backup-window double-write callback is re-registered by
// OnAfterLsmInit on exactly this crash-recovery path (IsSwapped()&&
// !IsTidied()). processOneTidyProp is a bare os.RemoveAll - it never
// unregisters the bucket from the store - so a write racing the removal
// still resolves a live *Bucket via resolveDoubleWriteBucket whose backing
// segment files were just deleted, and the WAL append fails outright.
//
// Driven via the processOneTidyPropFn seam: the hook fires the racing
// write immediately after the production per-prop tidy body has removed
// that prop's backup directory - the exact window the fix (disarming
// callbacks before tidyBackupBuckets runs, in the IsSwapped branch only)
// closes.
func TestReindex_EnableSearchable_RecoveryTidyWindowRace_DisarmBeforeTidy(t *testing.T) {
	const numObjects = 20
	ctx := testCtx()

	shard, idx, task, wrapped, _ := newBackfilledEnableSearchableFixture(t, ctx, "EnableSearchableTidyWindowRace", numObjects)
	className := shard.Index().Config.ClassName.String()

	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted)

	// Synthesize a crash between markSwapped and markTidied inside the
	// ORIGINAL runtimeSwap call - same technique as
	// synthesizeSwappedNotTidied's other caller
	// (inverted_reindex_crash_recovery_mid_tidy_tally_test.go).
	synthesizeSwappedNotTidied(t, shard, task)

	// Simulated restart: a fresh task instance. OnAfterLsmInit sees
	// IsSwapped()&&!IsTidied() and re-registers the backup-window
	// double-write callback - the ONLY callback armed for the rest of this
	// test.
	task2, wrapped2 := newEnableSearchableTask(t, idx, className, sidecarBackfillTextProp, models.PropertyTokenizationWord)
	require.NoError(t, task2.OnAfterLsmInit(ctx, shard))

	rt2, err := task2.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt2.IsSwapped(), "sanity: must still report swapped")
	require.False(t, rt2.IsTidied(), "sanity: must still be inside the crash-recovery window this bug targets")

	racer := newSidecarMarkerObject(className, sidecarBackfillTextProp, tidyWindowRaceMarkerWord)
	racerID := racer.ID()
	raceInjected := false
	var raceWriteErr error
	origTidy := task2.processOneTidyProp
	task2.processOneTidyPropFn = func(propIdx int, propName, lsmPath string) error {
		if err := origTidy(propIdx, propName, lsmPath); err != nil {
			return err
		}
		if !raceInjected {
			raceInjected = true
			// The backup bucket's on-disk directory for this prop is now
			// gone. Pre-fix, the backup-window callback re-registered
			// above is still armed at this point - RunSwapOnShard's
			// IsSwapped branch hasn't disarmed it yet. A write racing
			// here exercises exactly that window. The live schema still
			// reports this prop as non-searchable (the cluster-wide flip
			// hasn't landed), so a successful write is NOT expected to
			// produce a searchable posting - only to not fail outright,
			// which is the N1 claim under test.
			raceWriteErr = shard.PutObject(ctx, racer)
		}
		return nil
	}

	// The real recovery dispatch: ReindexProvider.OnGroupCompleted calls
	// RunSwapOnShard on the SAME (recovered) task instance.
	require.NoError(t, task2.RunSwapOnShard(ctx, shard))
	require.True(t, wrapped2.migrationCompleted)
	require.True(t, raceInjected, "tidy-window race injection hook must have fired")

	require.NoErrorf(t, raceWriteErr,
		"regression (weaviate/weaviate#12221 finding N1): a write racing RunSwapOnShard's IsSwapped recovery "+
			"branch in the window between tidyBackupBuckets removing the backup bucket's on-disk directory and "+
			"finalizeMigrationAfterRecovery's deferred disarm must not fail - the backup-window callback must be "+
			"disarmed BEFORE tidyBackupBuckets runs in this branch, not after")

	// The write must have genuinely landed (not silently dropped) - fetch
	// it back from the objects bucket to confirm end-to-end success, not
	// just the absence of an error return.
	stored, err := shard.ObjectByID(ctx, racerID, nil, additional.Properties{})
	require.NoError(t, err)
	require.NotNilf(t, stored, "the racing write's object must be retrievable after recovery completes")
}
