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

package cyclemanager

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_CycleTickersBackOffWhenIdle(t *testing.T) {
	type testCase struct {
		name      string
		newTicker func() CycleTicker
		intervals func() CycleIntervals
	}

	testCases := []testCase{
		{
			name:      "compaction",
			newTicker: func() CycleTicker { return CompactionCycleTicker(true) },
			intervals: CompactionCycleIntervals,
		},
		{
			name:      "memtable flush",
			newTicker: func() CycleTicker { return MemtableFlushCycleTicker(true) },
			intervals: MemtableFlushCycleIntervals,
		},
		{
			name:      "geo commit logger",
			newTicker: func() CycleTicker { return GeoCommitLoggerCycleTicker(true) },
			intervals: GeoCommitLoggerCycleIntervals,
		},
		{
			name:      "hnsw commit logger",
			newTicker: func() CycleTicker { return HnswCommitLoggerCycleTicker(true) },
			intervals: HnswCommitLoggerCycleIntervals,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expected := collectIntervals(tc.intervals())
			require.Greater(t, len(expected), 1)
			min := expected[0]
			max := expected[len(expected)-1]

			synctest.Test(t, func(t *testing.T) {
				ticker := tc.newTicker()
				ticker.Start()
				defer ticker.Stop()

				// each idle cycle advances the interval up to max
				for _, want := range expected {
					start := time.Now()
					time.Sleep(want)
					synctest.Wait()

					tick := <-ticker.C()
					assert.Equal(t, want, tick.Sub(start))

					ticker.CycleExecuted(false)
				}

				// further idle cycles stay at max
				start := time.Now()
				time.Sleep(max)
				synctest.Wait()

				tick := <-ticker.C()
				assert.Equal(t, max, tick.Sub(start))

				// work resets to min
				ticker.CycleExecuted(true)
				start = time.Now()
				time.Sleep(min)
				synctest.Wait()

				tick = <-ticker.C()
				assert.Equal(t, min, tick.Sub(start))
			})
		})
	}
}

func Test_CycleManagerBacksOffWhenIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var wakes int
		busy := false

		cycleCallback := func(shouldAbort ShouldAbortCallback) bool {
			mu.Lock()
			defer mu.Unlock()
			wakes++
			return busy
		}

		cm := NewManager("test", MemtableFlushCycleTicker(true), cycleCallback, logger)
		cm.Start()

		// idle: 100ms . 258ms .. 574ms .... 1.206s ........ 2.471s
		// ................ 5s, then every 5s => 16 wakes in a minute
		// (a fixed 100ms ticker would wake ~600 times)
		time.Sleep(time.Minute)
		synctest.Wait()

		mu.Lock()
		idleWakes := wakes
		busy = true
		mu.Unlock()

		assert.Greater(t, idleWakes, 5)
		assert.Less(t, idleWakes, 30)

		// work: one tick at the current (max) interval picks the work up,
		// then the ticker snaps back to the 100ms floor
		time.Sleep(memtableFlushMaxInterval + time.Second)
		synctest.Wait()

		mu.Lock()
		busyWakes := wakes - idleWakes
		mu.Unlock()

		assert.GreaterOrEqual(t, busyWakes, 8)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, cm.StopAndWait(ctx))
	})
}

// collectIntervals advances given intervals until the value no longer
// changes, returning the full min-to-max series
func collectIntervals(intervals CycleIntervals) []time.Duration {
	out := []time.Duration{intervals.Get()}
	for {
		intervals.Advance()
		next := intervals.Get()
		if next == out[len(out)-1] {
			return out
		}
		out = append(out, next)
	}
}
