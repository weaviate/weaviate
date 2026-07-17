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

// Pins the TOCTOU where a concurrent ResetProperty could land between
// UnTrackProperty's presence check and its mutation, corrupting Sum/Count.

func newTOCTOUTestTracker(t *testing.T, name string) *JsonShardMetaData {
	t.Helper()
	dirName := t.TempDir()
	trackerPath := path.Join(dirName, name)
	tracker, err := NewJsonShardMetaData(trackerPath, logrus.New())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tracker.Close()) })
	return tracker
}

// Deterministic, channel-synchronized repro of the check-then-act race:
// PropertyTally observes the property present, then ResetProperty is forced
// to land before UnTrackProperty runs.
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

	go func() {
		defer wg.Done()
		_, c, _, tallyErr := tracker.PropertyTally("prop")
		require.NoError(t, tallyErr)
		preCheckCount = c
		close(preCheckObserved)

		<-resetDone
		untrackErr = tracker.UnTrackProperty("prop", 5)
	}()

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
		"regression (UnTrackProperty TOCTOU): UnTrackProperty must never leave COUNT negative after "+
			"losing a race against a concurrent ResetProperty - got %d", count)
	assert.GreaterOrEqual(t, sum, 0,
		"regression (UnTrackProperty TOCTOU): UnTrackProperty must never leave SUM negative after "+
			"losing a race against a concurrent ResetProperty - got %d", sum)
	assert.Zero(t, count, "post-reset UnTrackProperty must be a clean no-op (error returned, no mutation) - COUNT must stay exactly 0")
	assert.Zero(t, sum, "post-reset UnTrackProperty must be a clean no-op (error returned, no mutation) - SUM must stay exactly 0")
}

// A call made after the property has already been reset must report
// absence and must not mutate Sum/Count.
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

// Positive-path counterpart: called against a present property, it must
// report removed=true and apply the same decrement UnTrackProperty would.
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

// Concurrent hammer: many goroutines drive
// Track/UnTrackPropertyIfPresent/ResetProperty/PropertyTally against the
// same property (run under -race). Does not assert COUNT/SUM stay
// non-negative under unbounded concurrent ResetProperty traffic - that is
// not an invariant of this aggregate-counter design (two racing
// UnTrackPropertyIfPresent calls can both observe "present" and
// over-subtract relative to one fresh track); production only ever calls
// ResetProperty once per recompute, which the deterministic tests above
// cover exactly.
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

	// Uncontended now: proves the hammer left no torn internal state.
	tracker.ResetProperty("prop")
	require.NoError(t, tracker.TrackProperty("prop", 7))
	require.NoError(t, tracker.TrackProperty("prop", 3))

	sum, count, _, err := tracker.PropertyTally("prop")
	require.NoError(t, err)
	assert.Equal(t, 10, sum,
		"regression (UnTrackProperty TOCTOU): tracker must remain functionally correct after the "+
			"concurrent Track/UnTrackPropertyIfPresent/ResetProperty/PropertyTally hammer - got SUM=%d, want 10", sum)
	assert.Equal(t, 2, count,
		"regression (UnTrackProperty TOCTOU): tracker must remain functionally correct after the "+
			"concurrent Track/UnTrackPropertyIfPresent/ResetProperty/PropertyTally hammer - got COUNT=%d, want 2", count)
}
