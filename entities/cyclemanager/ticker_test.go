//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_FixedIntervalTicker(t *testing.T) {
	t.Run("channel is empty before started", func(t *testing.T) {
		interval := 10 * time.Millisecond
		ticker := NewFixedTicker(10 * time.Millisecond)

		assert.Len(t, ticker.C(), 0)

		ticker.Start()
		time.Sleep(2 * interval)

		assert.Len(t, ticker.C(), 1)
	})

	t.Run("interval is fixed", func(t *testing.T) {
		interval := 50 * time.Millisecond
		tolerance := 25 * time.Millisecond

		ticker := NewFixedTicker(interval)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()
		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, interval, tolerance)
		assertTimeDiffEquals(t, val2, val3, interval, tolerance)
		assertTimeDiffEquals(t, val3, val4, interval, tolerance)
		assertTimeDiffEquals(t, t0, t1, interval, tolerance)
		assertTimeDiffEquals(t, t1, t2, interval, tolerance)
		assertTimeDiffEquals(t, t2, t3, interval, tolerance)
		assertTimeDiffEquals(t, t3, t4, interval, tolerance)
	})

	t.Run("interval does not change on CycleExecuted call", func(t *testing.T) {
		interval := 50 * time.Millisecond
		tolerance := 25 * time.Millisecond

		ticker := NewFixedTicker(interval)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.CycleExecuted(false)

		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.CycleExecuted(true)

		val5 := <-ticker.C()
		t5 := time.Now()
		val6 := <-ticker.C()
		t6 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, interval, tolerance)
		assertTimeDiffEquals(t, val2, val3, interval, tolerance)
		assertTimeDiffEquals(t, val3, val4, interval, tolerance)
		assertTimeDiffEquals(t, val4, val5, interval, tolerance)
		assertTimeDiffEquals(t, val5, val6, interval, tolerance)
		assertTimeDiffEquals(t, t0, t1, interval, tolerance)
		assertTimeDiffEquals(t, t1, t2, interval, tolerance)
		assertTimeDiffEquals(t, t2, t3, interval, tolerance)
		assertTimeDiffEquals(t, t3, t4, interval, tolerance)
		assertTimeDiffEquals(t, t4, t5, interval, tolerance)
		assertTimeDiffEquals(t, t5, t6, interval, tolerance)
	})

	t.Run("no ticks after stop", func(t *testing.T) {
		interval := 50 * time.Millisecond
		tolerance := 25 * time.Millisecond

		ticker := NewFixedTicker(interval)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.Stop()

		tickOccurred := false
		ctx, cancel := context.WithTimeout(context.Background(), 2*interval)
		defer cancel()

		select {
		case <-ticker.C():
			tickOccurred = true
		case <-ctx.Done():
			tickOccurred = false
		}

		assert.False(t, tickOccurred)

		assertTimeDiffEquals(t, val1, val2, interval, tolerance)
		assertTimeDiffEquals(t, t0, t1, interval, tolerance)
		assertTimeDiffEquals(t, t1, t2, interval, tolerance)
	})

	t.Run("ticker starts again", func(t *testing.T) {
		interval := 50 * time.Millisecond
		tolerance := 25 * time.Millisecond

		ticker := NewFixedTicker(interval)
		ticker.Start()

		t01 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.Stop()
		ticker.Start()

		t02 := time.Now()
		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, interval, tolerance)
		assertTimeDiffEquals(t, val3, val4, interval, tolerance)
		assertTimeDiffEquals(t, t01, t1, interval, tolerance)
		assertTimeDiffEquals(t, t1, t2, interval, tolerance)
		assertTimeDiffEquals(t, t02, t3, interval, tolerance)
		assertTimeDiffEquals(t, t3, t4, interval, tolerance)
	})

	t.Run("ticker does not run with <= 0 interval", func(t *testing.T) {
		interval := time.Duration(0)

		ticker := NewFixedTicker(interval)
		ticker.Start()

		tickOccurred := false
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		select {
		case <-ticker.C():
			tickOccurred = true
		case <-ctx.Done():
			tickOccurred = false
		}

		assert.False(t, tickOccurred)

		ticker.Stop()
	})
}

func Test_SeriesTicker(t *testing.T) {
	t.Run("channel is empty before started", func(t *testing.T) {
		intervals := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
		ticker := NewSeriesTicker(intervals)

		assert.Len(t, ticker.C(), 0)

		ticker.Start()
		time.Sleep(2 * intervals[0])

		assert.Len(t, ticker.C(), 1)
	})

	t.Run("interval is fixed between CycleExecuted calls, advances on false, resets on true", func(t *testing.T) {
		intervals := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 150 * time.Millisecond}
		tolerance := 25 * time.Millisecond

		ticker := NewSeriesTicker(intervals)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.CycleExecuted(false)

		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.CycleExecuted(false)

		val5 := <-ticker.C()
		t5 := time.Now()
		val6 := <-ticker.C()
		t6 := time.Now()

		ticker.CycleExecuted(false)

		val7 := <-ticker.C()
		t7 := time.Now()
		val8 := <-ticker.C()
		t8 := time.Now()

		ticker.CycleExecuted(true)

		val9 := <-ticker.C()
		t9 := time.Now()
		val10 := <-ticker.C()
		t10 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, intervals[0], tolerance)
		assertTimeDiffEquals(t, val2, val3, intervals[1], tolerance)
		assertTimeDiffEquals(t, val3, val4, intervals[1], tolerance)
		assertTimeDiffEquals(t, val4, val5, intervals[2], tolerance)
		assertTimeDiffEquals(t, val5, val6, intervals[2], tolerance)
		assertTimeDiffEquals(t, val6, val7, intervals[2], tolerance)
		assertTimeDiffEquals(t, val7, val8, intervals[2], tolerance)
		assertTimeDiffEquals(t, val8, val9, intervals[0], tolerance)
		assertTimeDiffEquals(t, val9, val10, intervals[0], tolerance)
		assertTimeDiffEquals(t, t0, t1, intervals[0], tolerance)
		assertTimeDiffEquals(t, t1, t2, intervals[0], tolerance)
		assertTimeDiffEquals(t, t2, t3, intervals[1], tolerance)
		assertTimeDiffEquals(t, t3, t4, intervals[1], tolerance)
		assertTimeDiffEquals(t, t4, t5, intervals[2], tolerance)
		assertTimeDiffEquals(t, t5, t6, intervals[2], tolerance)
		assertTimeDiffEquals(t, t6, t7, intervals[2], tolerance)
		assertTimeDiffEquals(t, t7, t8, intervals[2], tolerance)
		assertTimeDiffEquals(t, t8, t9, intervals[0], tolerance)
		assertTimeDiffEquals(t, t9, t10, intervals[0], tolerance)
	})

	t.Run("no ticks after stop", func(t *testing.T) {
		intervals := []time.Duration{50 * time.Millisecond}
		tolerance := 25 * time.Millisecond

		ticker := NewSeriesTicker(intervals)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.Stop()

		tickOccurred := false
		ctx, cancel := context.WithTimeout(context.Background(), 2*intervals[0])
		defer cancel()

		select {
		case <-ticker.C():
			tickOccurred = true
		case <-ctx.Done():
			tickOccurred = false
		}

		assert.False(t, tickOccurred)

		assertTimeDiffEquals(t, val1, val2, intervals[0], tolerance)
		assertTimeDiffEquals(t, t0, t1, intervals[0], tolerance)
		assertTimeDiffEquals(t, t1, t2, intervals[0], tolerance)
	})

	t.Run("ticker starts again", func(t *testing.T) {
		intervals := []time.Duration{50 * time.Millisecond}
		tolerance := 25 * time.Millisecond

		ticker := NewSeriesTicker(intervals)
		ticker.Start()

		t01 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.Stop()
		ticker.Start()

		t02 := time.Now()
		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, intervals[0], tolerance)
		assertTimeDiffEquals(t, val3, val4, intervals[0], tolerance)
		assertTimeDiffEquals(t, t01, t1, intervals[0], tolerance)
		assertTimeDiffEquals(t, t1, t2, intervals[0], tolerance)
		assertTimeDiffEquals(t, t02, t3, intervals[0], tolerance)
		assertTimeDiffEquals(t, t3, t4, intervals[0], tolerance)
	})

	t.Run("ticker does not run with invalid params", func(t *testing.T) {
		run := func(t *testing.T, ticker CycleTicker) {
			ticker.Start()

			tickOccurred := false
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			select {
			case <-ticker.C():
				tickOccurred = true
			case <-ctx.Done():
				tickOccurred = false
			}

			assert.False(t, tickOccurred)

			ticker.Stop()
		}

		t.Run("any interval <= 0", func(t *testing.T) {
			ticker := NewSeriesTicker([]time.Duration{50 * time.Millisecond, 0})

			run(t, ticker)
		})

		t.Run("no intervals", func(t *testing.T) {
			ticker := NewSeriesTicker([]time.Duration{})

			run(t, ticker)
		})
	})
}

func Test_LinearTicker(t *testing.T) {
	t.Run("channel is empty before started", func(t *testing.T) {
		minInterval := 10 * time.Millisecond
		maxInterval := 50 * time.Millisecond
		steps := uint(2)
		ticker := NewLinearTicker(minInterval, maxInterval, steps)

		assert.Len(t, ticker.C(), 0)

		ticker.Start()
		time.Sleep(2 * minInterval)

		assert.Len(t, ticker.C(), 1)
	})

	t.Run("interval is fixed between CycleExecuted calls, advances on false, resets on true", func(t *testing.T) {
		ms50 := 50 * time.Millisecond
		ms75 := 75 * time.Millisecond
		ms100 := 100 * time.Millisecond
		tolerance := 25 * time.Millisecond

		minInterval := ms50
		maxInterval := ms100
		steps := uint(2)

		ticker := NewLinearTicker(minInterval, maxInterval, steps)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.CycleExecuted(false)

		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.CycleExecuted(false)

		val5 := <-ticker.C()
		t5 := time.Now()
		val6 := <-ticker.C()
		t6 := time.Now()

		ticker.CycleExecuted(false)

		val7 := <-ticker.C()
		t7 := time.Now()
		val8 := <-ticker.C()
		t8 := time.Now()

		ticker.CycleExecuted(true)

		val9 := <-ticker.C()
		t9 := time.Now()
		val10 := <-ticker.C()
		t10 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, ms50, tolerance)
		assertTimeDiffEquals(t, val2, val3, ms75, tolerance)
		assertTimeDiffEquals(t, val3, val4, ms75, tolerance)
		assertTimeDiffEquals(t, val4, val5, ms100, tolerance)
		assertTimeDiffEquals(t, val5, val6, ms100, tolerance)
		assertTimeDiffEquals(t, val6, val7, ms100, tolerance)
		assertTimeDiffEquals(t, val7, val8, ms100, tolerance)
		assertTimeDiffEquals(t, val8, val9, ms50, tolerance)
		assertTimeDiffEquals(t, val9, val10, ms50, tolerance)
		assertTimeDiffEquals(t, t0, t1, ms50, tolerance)
		assertTimeDiffEquals(t, t1, t2, ms50, tolerance)
		assertTimeDiffEquals(t, t2, t3, ms75, tolerance)
		assertTimeDiffEquals(t, t3, t4, ms75, tolerance)
		assertTimeDiffEquals(t, t4, t5, ms100, tolerance)
		assertTimeDiffEquals(t, t5, t6, ms100, tolerance)
		assertTimeDiffEquals(t, t6, t7, ms100, tolerance)
		assertTimeDiffEquals(t, t7, t8, ms100, tolerance)
		assertTimeDiffEquals(t, t8, t9, ms50, tolerance)
		assertTimeDiffEquals(t, t9, t10, ms50, tolerance)
	})

	t.Run("no ticks after stop", func(t *testing.T) {
		minInterval := 50 * time.Millisecond
		maxInterval := 100 * time.Millisecond
		steps := uint(2)
		tolerance := 10 * time.Millisecond

		ticker := NewLinearTicker(minInterval, maxInterval, steps)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.Stop()

		tickOccurred := false
		ctx, cancel := context.WithTimeout(context.Background(), 2*minInterval)
		defer cancel()

		select {
		case <-ticker.C():
			tickOccurred = true
		case <-ctx.Done():
			tickOccurred = false
		}

		assert.False(t, tickOccurred)

		assertTimeDiffEquals(t, val1, val2, minInterval, tolerance)
		assertTimeDiffEquals(t, t0, t1, minInterval, tolerance)
		assertTimeDiffEquals(t, t1, t2, minInterval, tolerance)
	})

	t.Run("ticker starts again", func(t *testing.T) {
		minInterval := 50 * time.Millisecond
		maxInterval := 100 * time.Millisecond
		steps := uint(2)
		tolerance := 25 * time.Millisecond

		ticker := NewLinearTicker(minInterval, maxInterval, steps)
		ticker.Start()

		t01 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.Stop()
		ticker.Start()

		t02 := time.Now()
		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, minInterval, tolerance)
		assertTimeDiffEquals(t, val3, val4, minInterval, tolerance)
		assertTimeDiffEquals(t, t01, t1, minInterval, tolerance)
		assertTimeDiffEquals(t, t1, t2, minInterval, tolerance)
		assertTimeDiffEquals(t, t02, t3, minInterval, tolerance)
		assertTimeDiffEquals(t, t3, t4, minInterval, tolerance)
	})

	t.Run("ticker does not run with invalid params", func(t *testing.T) {
		run := func(t *testing.T, ticker CycleTicker) {
			ticker.Start()

			tickOccurred := false
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			select {
			case <-ticker.C():
				tickOccurred = true
			case <-ctx.Done():
				tickOccurred = false
			}

			assert.False(t, tickOccurred)

			ticker.Stop()
		}

		t.Run("minInterval <= 0", func(t *testing.T) {
			ticker := NewLinearTicker(0, 100*time.Millisecond, 1)

			run(t, ticker)
		})

		t.Run("maxInterval <= 0", func(t *testing.T) {
			ticker := NewLinearTicker(50*time.Millisecond, 0, 1)

			run(t, ticker)
		})

		t.Run("steps = 0", func(t *testing.T) {
			ticker := NewLinearTicker(50*time.Millisecond, 100*time.Millisecond, 0)

			run(t, ticker)
		})

		t.Run("minInterval > maxInterval", func(t *testing.T) {
			ticker := NewLinearTicker(100*time.Millisecond, 50*time.Millisecond, 0)

			run(t, ticker)
		})
	})
}

func Test_ExpTicker(t *testing.T) {
	t.Run("channel is empty before started", func(t *testing.T) {
		minInterval := 10 * time.Millisecond
		maxInterval := 20 * time.Millisecond
		base := uint(2)
		steps := uint(2)
		ticker := NewExpTicker(minInterval, maxInterval, base, steps)

		assert.Len(t, ticker.C(), 0)

		ticker.Start()
		time.Sleep(2 * minInterval)

		assert.Len(t, ticker.C(), 1)
	})

	t.Run("interval is fixed between CycleExecuted calls, advances on false, resets on true", func(t *testing.T) {
		ms25 := 25 * time.Millisecond
		ms50 := 50 * time.Millisecond
		ms100 := 100 * time.Millisecond
		tolerance := 25 * time.Millisecond

		minInterval := ms25
		maxInterval := ms100
		base := uint(2)
		steps := uint(2)

		ticker := NewExpTicker(minInterval, maxInterval, base, steps)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.CycleExecuted(false)

		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.CycleExecuted(false)

		val5 := <-ticker.C()
		t5 := time.Now()
		val6 := <-ticker.C()
		t6 := time.Now()

		ticker.CycleExecuted(false)

		val7 := <-ticker.C()
		t7 := time.Now()
		val8 := <-ticker.C()
		t8 := time.Now()

		ticker.CycleExecuted(true)

		val9 := <-ticker.C()
		t9 := time.Now()
		val10 := <-ticker.C()
		t10 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, ms25, tolerance)
		assertTimeDiffEquals(t, val2, val3, ms50, tolerance)
		assertTimeDiffEquals(t, val3, val4, ms50, tolerance)
		assertTimeDiffEquals(t, val4, val5, ms100, tolerance)
		assertTimeDiffEquals(t, val5, val6, ms100, tolerance)
		assertTimeDiffEquals(t, val6, val7, ms100, tolerance)
		assertTimeDiffEquals(t, val7, val8, ms100, tolerance)
		assertTimeDiffEquals(t, val8, val9, ms25, tolerance)
		assertTimeDiffEquals(t, val9, val10, ms25, tolerance)
		assertTimeDiffEquals(t, t0, t1, ms25, tolerance)
		assertTimeDiffEquals(t, t1, t2, ms25, tolerance)
		assertTimeDiffEquals(t, t2, t3, ms50, tolerance)
		assertTimeDiffEquals(t, t3, t4, ms50, tolerance)
		assertTimeDiffEquals(t, t4, t5, ms100, tolerance)
		assertTimeDiffEquals(t, t5, t6, ms100, tolerance)
		assertTimeDiffEquals(t, t6, t7, ms100, tolerance)
		assertTimeDiffEquals(t, t7, t8, ms100, tolerance)
		assertTimeDiffEquals(t, t8, t9, ms25, tolerance)
		assertTimeDiffEquals(t, t9, t10, ms25, tolerance)
	})

	t.Run("no ticks after stop", func(t *testing.T) {
		minInterval := 25 * time.Millisecond
		maxInterval := 100 * time.Millisecond
		base := uint(2)
		steps := uint(2)
		tolerance := 25 * time.Millisecond

		ticker := NewExpTicker(minInterval, maxInterval, base, steps)
		ticker.Start()

		t0 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.Stop()

		tickOccurred := false
		ctx, cancel := context.WithTimeout(context.Background(), 2*minInterval)
		defer cancel()

		select {
		case <-ticker.C():
			tickOccurred = true
		case <-ctx.Done():
			tickOccurred = false
		}

		assert.False(t, tickOccurred)

		assertTimeDiffEquals(t, val1, val2, minInterval, tolerance)
		assertTimeDiffEquals(t, t0, t1, minInterval, tolerance)
		assertTimeDiffEquals(t, t1, t2, minInterval, tolerance)
	})

	t.Run("ticker starts again", func(t *testing.T) {
		minInterval := 25 * time.Millisecond
		maxInterval := 100 * time.Millisecond
		base := uint(2)
		steps := uint(2)
		tolerance := 25 * time.Millisecond

		ticker := NewExpTicker(minInterval, maxInterval, base, steps)
		ticker.Start()

		t01 := time.Now()
		val1 := <-ticker.C()
		t1 := time.Now()
		val2 := <-ticker.C()
		t2 := time.Now()

		ticker.Stop()
		ticker.Start()

		t02 := time.Now()
		val3 := <-ticker.C()
		t3 := time.Now()
		val4 := <-ticker.C()
		t4 := time.Now()

		ticker.Stop()

		assertTimeDiffEquals(t, val1, val2, minInterval, tolerance)
		assertTimeDiffEquals(t, val3, val4, minInterval, tolerance)
		assertTimeDiffEquals(t, t01, t1, minInterval, tolerance)
		assertTimeDiffEquals(t, t1, t2, minInterval, tolerance)
		assertTimeDiffEquals(t, t02, t3, minInterval, tolerance)
		assertTimeDiffEquals(t, t3, t4, minInterval, tolerance)
	})

	t.Run("ticker does not run with invalid params", func(t *testing.T) {
		run := func(t *testing.T, ticker CycleTicker) {
			ticker.Start()

			tickOccurred := false
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			select {
			case <-ticker.C():
				tickOccurred = true
			case <-ctx.Done():
				tickOccurred = false
			}

			assert.False(t, tickOccurred)

			ticker.Stop()
		}

		t.Run("minInterval <= 0", func(t *testing.T) {
			ticker := NewExpTicker(0, 100*time.Millisecond, 2, 2)

			run(t, ticker)
		})

		t.Run("maxInterval <= 0", func(t *testing.T) {
			ticker := NewExpTicker(100*time.Millisecond, 0, 2, 2)

			run(t, ticker)
		})

		t.Run("base == 0", func(t *testing.T) {
			ticker := NewExpTicker(25*time.Millisecond, 100*time.Millisecond, 0, 2)

			run(t, ticker)
		})

		t.Run("steps = 0", func(t *testing.T) {
			ticker := NewExpTicker(25*time.Millisecond, 100*time.Millisecond, 2, 0)

			run(t, ticker)
		})

		t.Run("minInterval > maxInterval", func(t *testing.T) {
			ticker := NewExpTicker(100*time.Millisecond, 25*time.Millisecond, 2, 2)

			run(t, ticker)
		})
	})
}

func Test_LinearToIntervals(t *testing.T) {
	type testCase struct {
		name        string
		minInterval time.Duration
		maxInterval time.Duration
		steps       uint
		expected    []time.Duration
	}

	testCases := []testCase{
		{
			name:        "100 => 5000; steps 2",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			steps:       2,
			expected: []time.Duration{
				100_000_000,
				2_550_000_000,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; steps 3",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			steps:       3,
			expected: []time.Duration{
				100_000_000,
				1_733_333_333,
				3_366_666_666,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; steps 4",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			steps:       4,
			expected: []time.Duration{
				100_000_000,
				1_325_000_000,
				2_550_000_000,
				3_775_000_000,
				5_000_000_000,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := linearToIntervals(tc.minInterval, tc.maxInterval, tc.steps)

			assert.ElementsMatch(t, res, tc.expected)
		})
	}
}

func Test_ExpToIntervals(t *testing.T) {
	type testCase struct {
		name        string
		minInterval time.Duration
		maxInterval time.Duration
		base        uint
		steps       uint
		expected    []time.Duration
	}

	testCases := []testCase{
		{
			name:        "100 => 5000; base 2; steps 2",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        2,
			steps:       2,
			expected: []time.Duration{
				100_000_000,
				1_733_333_333,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 2; steps 3",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        2,
			steps:       3,
			expected: []time.Duration{
				100_000_000,
				800_000_000,
				2_200_000_000,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 2; steps 4",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        2,
			steps:       4,
			expected: []time.Duration{
				100_000_000,
				426_666_666,
				1_080_000_000,
				2_386_666_666,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 3; steps 2",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        3,
			steps:       2,
			expected: []time.Duration{
				100_000_000,
				1_325_000_000,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 3; steps 3",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        3,
			steps:       3,
			expected: []time.Duration{
				100_000_000,
				476_923_076,
				1_607_692_307,
				5_000_000_000,
			},
		},
		{
			name:        "100 => 5000; base 3; steps 4",
			minInterval: 100 * time.Millisecond,
			maxInterval: 5 * time.Second,
			base:        3,
			steps:       4,
			expected: []time.Duration{
				100_000_000,
				222_500_000,
				590_000_000,
				1_692_500_000,
				5_000_000_000,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res := expToIntervals(tc.minInterval, tc.maxInterval, tc.base, tc.steps)

			assert.ElementsMatch(t, res, tc.expected)
		})
	}
}

func assertTimeDiffEquals(t *testing.T, time1, time2 time.Time, expected time.Duration, tolerance time.Duration) {
	diff := time2.Sub(time1)
	assert.GreaterOrEqual(t, diff, expected-tolerance)
	assert.LessOrEqual(t, diff, expected+tolerance)
}
