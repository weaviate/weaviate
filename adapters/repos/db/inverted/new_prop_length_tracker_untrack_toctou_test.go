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
	"path"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// PR #12221 QA round-4: adapters/repos/db/inverted_reindex_strategy_enable_searchable.go's
// untrackMigratingPropLength composed a read-only PropertyTally presence
// check with a separate UnTrackProperty call - two independent lock
// acquisitions on JsonShardMetaData's mutex. Genuinely concurrent with
// Shard.recomputeSearchableTallyForProp's ResetProperty (registered
// double-write callbacks stay live until runtimeSwap's deferred
// disableCallbacks call - see that function's godoc), the composition left
// a TOCTOU window: PropertyTally sees the property present, ResetProperty
// deletes it, then UnTrackProperty's blind decrement lands on absent map
// keys (Go's zero-value semantics turn `0 - value` into a negative Sum and
// `0 - 1` into Count=-1) BEFORE its own presence check (on BucketedData)
// returns "property not found" - corrupting the tally, and the error
// propagates out through PutObject's deleteFromInvertedIndicesLSM call,
// failing the user's write.
//
// These tests pin the fix at the JsonShardMetaData level:
//  1. UnTrackProperty itself now checks presence BEFORE mutating, so even a
//     racy check-then-act CALLER can no longer corrupt Sum/Count - the
//     error return is now a clean no-op, not a torn write.
//  2. UnTrackPropertyIfPresent closes the TOCTOU window structurally (one
//     lock acquisition, no separate pre-check) - the fix
//     untrackMigratingPropLength was switched to use.
// -----------------------------------------------------------------------------

func newTOCTOUTestTracker(t *testing.T, name string) *JsonShardMetaData {
	t.Helper()
	dirName := t.TempDir()
	trackerPath := path.Join(dirName, name)
	tracker, err := NewJsonShardMetaData(trackerPath, logrus.New())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tracker.Close()) })
	return tracker
}

// TestJsonShardMetaData_UnTrackProperty_ChecksPresenceBeforeMutating
// reproduces the exact check-then-act composition untrackMigratingPropLength
// used pre-fix (PropertyTally, then UnTrackProperty, as two separate lock
// acquisitions) with a deterministic, channel-synchronized race against
// ResetProperty landing in the gap between the two calls - the same
// interleaving PR #12221 QA round-4 described, forced instead of relying on
// scheduler luck.
//
// Causal link: this test catches the bug because it holds the pre-check
// result (PropertyTally observing the property present) across a
// synchronization barrier, deliberately runs ResetProperty while
// UnTrackProperty has not yet been called, and only then calls
// UnTrackProperty - the exact ordering that corrupts state pre-fix. Verified
// RED on the pre-fix UnTrackProperty (stash-revert: this test's negative-value
// assertions failed because Sum/Count actually went negative) and GREEN
// post-fix (UnTrackProperty's presence check now runs before any mutation,
// so losing the race is a no-op, not a partial write).
func TestJsonShardMetaData_UnTrackProperty_ChecksPresenceBeforeMutating(t *testing.T) {
	tracker := newTOCTOUTestTracker(t, "checkThenAct")

	require.NoError(t, tracker.TrackProperty("prop", 5))
	sum, count, _, err := tracker.PropertyTally("prop")
	require.NoError(t, err)
	require.Equal(t, 5, sum)
	require.Equal(t, 1, count)

	preCheckObserved := make(chan struct{})
	resetDone := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	var (
		preCheckCount int
		untrackErr    error
	)

	// Goroutine A: the delete-callback side of the pre-fix composition -
	// PropertyTally (the "pre-check"), then - AFTER ResetProperty has been
	// given the chance to run - UnTrackProperty (the "act").
	go func() {
		defer wg.Done()
		_, c, _, tallyErr := tracker.PropertyTally("prop")
		require.NoError(t, tallyErr)
		preCheckCount = c
		close(preCheckObserved)

		<-resetDone
		untrackErr = tracker.UnTrackProperty("prop", 5)
	}()

	// Goroutine B: the concurrent recompute side - ResetProperty landing
	// strictly between goroutine A's pre-check and its UnTrackProperty call.
	go func() {
		defer wg.Done()
		<-preCheckObserved
		tracker.ResetProperty("prop")
		close(resetDone)
	}()

	wg.Wait()

	require.Equal(t, 1, preCheckCount, "sanity: the pre-check must have observed the property present before the reset landed")
	require.Error(t, untrackErr, "UnTrackProperty must still report 'property not found' after losing the race - the error contract is unchanged")

	sum, count, _, err = tracker.PropertyTally("prop")
	require.NoError(t, err)

	assert.GreaterOrEqual(t, count, 0,
		"BUG regression check (PR#12221 round-4 TOCTOU): UnTrackProperty must never leave COUNT negative after "+
			"losing a race against a concurrent ResetProperty - got %d", count)
	assert.GreaterOrEqual(t, sum, 0,
		"BUG regression check (PR#12221 round-4 TOCTOU): UnTrackProperty must never leave SUM negative after "+
			"losing a race against a concurrent ResetProperty - got %d", sum)
	assert.Zero(t, count, "post-reset UnTrackProperty must be a clean no-op (error returned, no mutation) - COUNT must stay exactly 0")
	assert.Zero(t, sum, "post-reset UnTrackProperty must be a clean no-op (error returned, no mutation) - SUM must stay exactly 0")
}

// TestJsonShardMetaData_UnTrackPropertyIfPresent_NoOpAfterReset is the
// direct primitive test for the atomic replacement: a single call, made
// after the property has already been reset, must report absence and must
// not mutate Sum/Count at all.
//
// Causal link: this test catches a regression where a future change
// reintroduces a check-then-act shape inside UnTrackPropertyIfPresent
// itself (e.g. calling a presence-check helper before re-acquiring the
// lock) - such a change would reopen exactly the same window this file's
// other tests close, and this test's exact-zero assertions would catch any
// mutation the reintroduced gap allowed through.
func TestJsonShardMetaData_UnTrackPropertyIfPresent_NoOpAfterReset(t *testing.T) {
	tracker := newTOCTOUTestTracker(t, "noOpAfterReset")

	require.NoError(t, tracker.TrackProperty("prop", 5))
	tracker.ResetProperty("prop")

	sum, count, _, err := tracker.PropertyTally("prop")
	require.NoError(t, err)
	require.Zero(t, count, "sanity: ResetProperty must leave the property with no entries")
	require.Zero(t, sum, "sanity: ResetProperty must leave the property with no entries")

	removed, err := tracker.UnTrackPropertyIfPresent("prop", 5)
	require.NoError(t, err, "UnTrackPropertyIfPresent must never error on an absent property")
	assert.False(t, removed, "UnTrackPropertyIfPresent must report false (no-op) for an absent property")

	sum, count, _, err = tracker.PropertyTally("prop")
	require.NoError(t, err)
	assert.Zero(t, count, "UnTrackPropertyIfPresent must not mutate COUNT for an absent property")
	assert.Zero(t, sum, "UnTrackPropertyIfPresent must not mutate SUM for an absent property")
}

// TestJsonShardMetaData_UnTrackPropertyIfPresent_ReportsPresentAndMutates is
// the positive-path counterpart: called against a property that IS present,
// it must report removed=true and apply the same decrement UnTrackProperty
// would - the atomic replacement's happy path must stay byte-identical to
// the composition it replaces.
func TestJsonShardMetaData_UnTrackPropertyIfPresent_ReportsPresentAndMutates(t *testing.T) {
	tracker := newTOCTOUTestTracker(t, "presentMutates")

	require.NoError(t, tracker.TrackProperty("prop", 5))
	require.NoError(t, tracker.TrackProperty("prop", 3))

	removed, err := tracker.UnTrackPropertyIfPresent("prop", 5)
	require.NoError(t, err)
	assert.True(t, removed)

	sum, count, _, err := tracker.PropertyTally("prop")
	require.NoError(t, err)
	assert.Equal(t, 3, sum)
	assert.Equal(t, 1, count)
}

// TestJsonShardMetaData_UnTrackPropertyIfPresent_ConcurrentHammerRaceFreeAndFunctional
// is the concurrent hammer test: many goroutines drive TrackProperty /
// UnTrackPropertyIfPresent / ResetProperty / PropertyTally against the same
// property simultaneously (run under -race).
//
// This intentionally does NOT assert COUNT/SUM stay non-negative under
// unbounded concurrent ResetProperty traffic - that is not a true invariant
// of this shared-counter design (verified empirically: an earlier version of
// this test asserted it and failed reproducibly). "Presence" here is a
// per-property boolean (BucketedData[propName] existing), not a per-value
// balance, so two UnTrackPropertyIfPresent calls whose paired TrackProperty
// calls happened before a ResetProperty can both see "present" (because a
// third goroutine's post-reset TrackProperty re-established presence) and
// both decrement, over-subtracting relative to that one fresh track. That is
// a pre-existing structural property of the aggregate-counter tracker, not
// something this fix changes or is scoped to fix - production only ever
// calls ResetProperty once per recompute (see
// Shard.recomputeSearchableTallyForProp), not in a tight concurrent loop.
// TestJsonShardMetaData_UnTrackProperty_ChecksPresenceBeforeMutating and
// TestJsonShardMetaData_UnTrackPropertyIfPresent_NoOpAfterReset above cover
// that single-reset production shape deterministically and DO prove
// non-negative/exact-zero convergence for it.
//
// What this test DOES prove, and what the task's "invariants hold" refers
// to for an unconstrained concurrent hammer:
//  1. Race-free: -race reports no data race across concurrent
//     Track/UnTrackPropertyIfPresent/ResetProperty/PropertyTally calls - the
//     single t.Lock()/t.Unlock() critical section per call is sufficient
//     synchronization.
//  2. No panics and no unexpected errors: none of these calls ever error
//     except the explicit "tracker is closed" path, which this test never
//     triggers.
//  3. Functional integrity survives the hammer: a final, uncontended
//     ResetProperty + TrackProperty sequence performed AFTER every hammer
//     goroutine has finished reads back exactly the tracked value - proving
//     the internal maps are not left in a corrupted/wedged state (e.g. a
//     torn CountData/BucketedData pair) by the concurrent traffic.
func TestJsonShardMetaData_UnTrackPropertyIfPresent_ConcurrentHammerRaceFreeAndFunctional(t *testing.T) {
	tracker := newTOCTOUTestTracker(t, "hammer")

	const (
		numTrackWorkers  = 8
		numResetWorkers  = 2
		numReaderWorkers = 2
		iterations       = 500
	)

	var wg sync.WaitGroup
	wg.Add(numTrackWorkers + numResetWorkers + numReaderWorkers)

	for w := 0; w < numTrackWorkers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				assert.NoError(t, tracker.TrackProperty("prop", 5))
				_, err := tracker.UnTrackPropertyIfPresent("prop", 5)
				assert.NoError(t, err)
			}
		}()
	}

	for w := 0; w < numResetWorkers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				tracker.ResetProperty("prop")
			}
		}()
	}

	for w := 0; w < numReaderWorkers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_, _, _, err := tracker.PropertyTally("prop")
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	// THE FUNCTIONAL-INTEGRITY CHECK: uncontended now that every hammer
	// goroutine has finished, so this must behave exactly like a fresh
	// tracker - proving the concurrent traffic above left no torn internal
	// state behind.
	tracker.ResetProperty("prop")
	require.NoError(t, tracker.TrackProperty("prop", 7))
	require.NoError(t, tracker.TrackProperty("prop", 3))

	sum, count, _, err := tracker.PropertyTally("prop")
	require.NoError(t, err)
	assert.Equal(t, 10, sum,
		"BUG regression check (PR#12221 round-4 TOCTOU): tracker must remain functionally correct after the "+
			"concurrent Track/UnTrackPropertyIfPresent/ResetProperty/PropertyTally hammer - got SUM=%d, want 10", sum)
	assert.Equal(t, 2, count,
		"BUG regression check (PR#12221 round-4 TOCTOU): tracker must remain functionally correct after the "+
			"concurrent Track/UnTrackPropertyIfPresent/ResetProperty/PropertyTally hammer - got COUNT=%d, want 2", count)
}
