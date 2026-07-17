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

package inverted

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Pins weaviate/0-weaviate-issues#322's base-sync finding 1 - window 4 in
// db.Shard.recomputeSearchableTallyForProp's godoc: ResetProperty runs
// BEFORE FlushAndSwitch there, so a writer whose objects-bucket Put lands
// before the flush (the rescan will independently find and re-track it) but
// whose OWN double-write-callback TrackProperty call fires strictly AFTER
// ResetProperty has already run is counted TWICE. Deterministic,
// channel-synchronized - same shape as
// TestJsonShardMetaData_UnTrackProperty_ChecksPresenceBeforeMutating in
// new_prop_length_tracker_untrack_toctou_test.go.
//
// This is a KNOWN, UNFIXED residual, not a regression to guard green: a
// clean in-scope fix would need a per-property epoch ResetProperty bumps
// and the callback's TrackProperty call checks against, plumbed through the
// general (non-migration) write path - out of scope for a migration-local
// change. Skipped rather than asserted; un-skip once that causality
// mechanism (or an equivalent fix) lands. Converges on any LATER recompute
// re-entry, since that invocation's own ResetProperty wipes the inflated
// value and its rescan re-derives the correct one from disk - it does not
// self-heal without one.
func TestJsonShardMetaData_TrackAfterResetDuringRescanDoubleCounts(t *testing.T) {
	t.Skip("RED pin for weaviate/0-weaviate-issues#322's recompute double-count race (window 4 in " +
		"db.Shard.recomputeSearchableTallyForProp's godoc): a writer whose TrackProperty call for the " +
		"double-write callback (window 1) fires strictly AFTER a concurrent recompute's ResetProperty, while " +
		"that same object's objects-bucket Put landed before the recompute's FlushAndSwitch (so the rescan " +
		"independently re-tracks it too), is counted TWICE - the reset can't discard an increment that hasn't " +
		"happened yet, and the tracker has no causality marker to recognize the two TrackProperty calls as " +
		"describing the same underlying write. No clean in-scope fix: closing this needs a per-property epoch " +
		"plumbed through the general, non-migration write path, not just the migration-local recompute. Silent " +
		"BM25 sum/count inflation, not data loss - converges on any LATER recompute re-entry (that invocation's " +
		"own ResetProperty wipes the inflated value; its rescan re-derives the correct one from disk). Red when " +
		"un-skipped: Count=2 where the correct value is 1, deterministic via channel synchronization.")

	tracker := newTOCTOUTestTracker(t, "trackAfterReset")

	resetDone := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	var callbackTrackErr error

	// Goroutine A: the post-swap recompute. ResetProperty runs first (matches
	// recomputeSearchableTallyForProp's actual statement order - see that
	// function's godoc window 4), then the rescan re-derives the racing
	// object's contribution from disk, independent of goroutine B.
	go func() {
		defer wg.Done()
		tracker.ResetProperty("title")
		close(resetDone)
		// The rescan loop finds the racing object on disk (its objects-bucket
		// Put landed before the recompute's FlushAndSwitch) and tracks it
		// exactly once, same as every other pre-existing object.
		require.NoError(t, tracker.TrackProperty("title", 7))
	}()

	// Goroutine B: the racing write's OWN double-write-callback TrackProperty
	// call (trackMigratingPropLength in production) - fires strictly AFTER
	// ResetProperty, so the reset does not discard it.
	go func() {
		defer wg.Done()
		<-resetDone
		callbackTrackErr = tracker.TrackProperty("title", 7)
	}()

	wg.Wait()
	require.NoError(t, callbackTrackErr, "sanity: TrackProperty itself must not error")

	sum, count, _, err := tracker.PropertyTally("title")
	require.NoError(t, err)

	// Correct behavior: the racing object is counted exactly once (either by
	// the rescan or by its own callback, not both) - Count=1, Sum=7.
	require.Equal(t, 1, count,
		"BUG (gh#322 recompute double-count residual, pinned not fixed - see the skip message above): the racing "+
			"object's TrackProperty call landing after ResetProperty must not double-count against the rescan's own "+
			"independent re-derivation - got Count=%d, want 1", count)
	require.Equal(t, 7, sum,
		"BUG (gh#322 recompute double-count residual, pinned not fixed - see the skip message above): Sum must "+
			"reflect the racing object's contribution exactly once, not twice - got Sum=%d, want 7", sum)
}
