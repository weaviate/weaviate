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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCycleCallback_Parallel(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("no callbacks", func(t *testing.T) {
		var executed bool

		callbacks := NewCallbackGroup("id", logger, 2)

		executed = callbacks.CycleCallback(shouldNotAbort)

		assert.False(t, executed)
	})

	t.Run("2 executable callbacks", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
	})

	t.Run("2 non-executable callbacks", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(10 * time.Millisecond)
			executedCounter1++
			return false
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(10 * time.Millisecond)
			executedCounter2++
			return false
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 10*time.Millisecond)
	})

	t.Run("3 executable callbacks, not all executed due to should abort", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter3++
			return true
		}
		// each due callback triggers one shouldAbort check in its worker before
		// running; with 3 due callbacks and abort tripping after the 2nd check,
		// the callback picked up third is skipped, so some run but not all 3.
		shouldAbortCounter := uint32(0)
		shouldAbort := func() bool {
			return atomic.AddUint32(&shouldAbortCounter, 1) > 2
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)
		callbacks.Register("c3", callback3)

		start := time.Now()
		executed = callbacks.CycleCallback(shouldAbort)
		d = time.Since(start)

		assert.True(t, executed)
		totalExecuted := executedCounter1 + executedCounter2 + executedCounter3
		assert.Greater(t, totalExecuted, 0)
		assert.Less(t, totalExecuted, 3)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("register new while executing runs it on the next tick", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter3++
			return true
		}
		executedCounter4 := 0
		callback4 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter4++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)
		callbacks.Register("c3", callback3)

		// register 4th callback while the others are executing:
		// 1st and 2nd are processed first (50ms), 3rd waits for a free routine.
		// The tick dispatches the callbacks due when it started (c1, c2, c3), so
		// the 4th, registered afterwards, is not part of this tick.
		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		callbacks.Register("c4", callback4)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.Equal(t, 1, executedCounter3)
		assert.Equal(t, 0, executedCounter4)
		assert.GreaterOrEqual(t, d, 100*time.Millisecond)

		// the next tick picks up the 4th callback
		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, 1, executedCounter4)
	})

	t.Run("idle tick executes nothing", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			executedCounter2++
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", callback1, WithIntervals(NewFixedIntervals(time.Hour)))
		callbacks.Register("c2", callback2, WithIntervals(NewFixedIntervals(time.Hour)))

		// 1st tick: both callbacks due (registration allows immediate execution)
		executed := callbacks.CycleCallback(shouldNotAbort)
		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)

		// 2nd tick: intervals not elapsed, nothing due
		executed = callbacks.CycleCallback(shouldNotAbort)
		assert.False(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
	})

	t.Run("idle tick prunes ids of unregistered callbacks", func(t *testing.T) {
		callback := func(shouldAbort ShouldAbortCallback) bool { return true }

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback)
		ctrl2 := callbacks.Register("c2", callback)

		require.NoError(t, ctrl1.Unregister(context.Background()))
		require.NoError(t, ctrl2.Unregister(context.Background()))

		executed := callbacks.CycleCallback(shouldNotAbort)

		assert.False(t, executed)
		assert.Empty(t, callbacks.(*cycleCallbackGroup).dueHeap)
	})

	t.Run("run with intervals", func(T *testing.T) {
		ticker := NewFixedTicker(10 * time.Millisecond)
		intervals2 := NewSeriesIntervals([]time.Duration{
			10 * time.Millisecond, 30 * time.Millisecond, 50 * time.Millisecond,
		})
		intervals3 := NewFixedIntervals(60 * time.Millisecond)
		now := time.Now()

		executionTimes1 := []time.Duration{}
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			executionTimes1 = append(executionTimes1, time.Since(now))
			return true
		}
		executionCounter2 := 0
		executionTimes2 := []time.Duration{}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			executionCounter2++
			executionTimes2 = append(executionTimes2, time.Since(now))
			// reports executed every 3 calls, should result in 10, 30, 50, 50, 10, 30, 50, 50, ... intervals
			return executionCounter2%4 == 0
		}
		executionTimes3 := []time.Duration{}
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			executionTimes3 = append(executionTimes3, time.Since(now))
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 2)
		// should be called on every tick, with 10 intervals
		callbacks.Register("c1", callback1)
		// should be called with 10, 30, 50, 50, 10, 30, 50, 50, ... intervals
		callbacks.Register("c2", callback2, WithIntervals(intervals2))
		// should be called with 60, 60, ... intervals
		callbacks.Register("c3", callback3, WithIntervals(intervals3))

		cm := NewManager("test", ticker, callbacks.CycleCallback, logger)
		cm.Start()
		time.Sleep(400 * time.Millisecond)
		cm.StopAndWait(context.Background())

		// within 400 ms c1 should be called at least 30x
		require.GreaterOrEqual(t, len(executionTimes1), 30)
		// 1st call on 1st tick after 10ms
		sumDuration := time.Duration(10)
		for i := 0; i < 30; i++ {
			assert.GreaterOrEqual(t, executionTimes1[i], sumDuration)
			sumDuration += 10 * time.Millisecond
		}

		// within 400 ms c2 should be called at least 8x
		require.GreaterOrEqual(t, len(executionTimes2), 8)
		// 1st call on 1st tick after 10ms
		sumDuration = time.Duration(0)
		for i := 0; i < 8; i++ {
			assert.GreaterOrEqual(t, executionTimes2[i], sumDuration)
			switch (i + 1) % 4 {
			case 0:
				sumDuration += 10 * time.Millisecond
			case 1:
				sumDuration += 30 * time.Millisecond
			case 2, 3:
				sumDuration += 50 * time.Millisecond
			}
		}

		// within 400 ms c3 should be called at least 6x
		require.GreaterOrEqual(t, len(executionTimes3), 6)
		// 1st call on 1st tick after 10ms
		sumDuration = time.Duration(0)
		for i := 0; i < 6; i++ {
			assert.GreaterOrEqual(t, executionTimes3[i], sumDuration)
			sumDuration += 60 * time.Millisecond
		}
	})
}

func TestCycleCallback_Parallel_Unregister(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("1 executable callback, 1 unregistered", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c1", callback)
		require.Nil(t, ctrl.Unregister(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 2 unregistered", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		require.Nil(t, ctrl1.Unregister(ctx))
		require.Nil(t, ctrl2.Unregister(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 1 unregistered", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)
		require.Nil(t, ctrl1.Unregister(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.True(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("4 executable callbacks, all unregistered at different time", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter3++
			return true
		}
		executedCounter4 := 0
		callback4 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter4++
			return true
		}
		var executed1 bool
		var executed2 bool
		var executed3 bool
		var executed4 bool
		var d1 time.Duration
		var d2 time.Duration
		var d3 time.Duration
		var d4 time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		ctrl3 := callbacks.Register("c3", callback3)
		ctrl4 := callbacks.Register("c4", callback4)
		require.Nil(t, ctrl3.Unregister(ctx))

		start := time.Now()
		executed1 = callbacks.CycleCallback(shouldNotAbort)
		d1 = time.Since(start)

		require.Nil(t, ctrl1.Unregister(ctx))

		start = time.Now()
		executed2 = callbacks.CycleCallback(shouldNotAbort)
		d2 = time.Since(start)

		require.Nil(t, ctrl4.Unregister(ctx))

		start = time.Now()
		executed3 = callbacks.CycleCallback(shouldNotAbort)
		d3 = time.Since(start)

		require.Nil(t, ctrl2.Unregister(ctx))

		start = time.Now()
		executed4 = callbacks.CycleCallback(shouldNotAbort)
		d4 = time.Since(start)

		assert.True(t, executed1)
		assert.True(t, executed2)
		assert.True(t, executed3)
		assert.False(t, executed4)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 3, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.Equal(t, 2, executedCounter4)
		assert.GreaterOrEqual(t, d1, 50*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 25*time.Millisecond)
		assert.GreaterOrEqual(t, d3, 25*time.Millisecond)
		assert.GreaterOrEqual(t, d4, 0*time.Millisecond)
	})

	t.Run("unregister is waiting till the end of execution", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl.Unregister(ctx))
		du := time.Since(start)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 40*time.Millisecond)
	})

	t.Run("unregister fails due to context timeout", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed1 bool
		var executed2 bool
		var d1 time.Duration
		var d2 time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed1 = callbacks.CycleCallback(shouldNotAbort)
			d1 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()
		require.NotNil(t, ctrl.Unregister(ctxTimeout))
		du := time.Since(start)
		<-chFinished

		go func() {
			start := time.Now()
			executed2 = callbacks.CycleCallback(shouldNotAbort)
			d2 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chFinished

		assert.True(t, executed1)
		assert.True(t, executed2)
		assert.Equal(t, 2, executedCounter)
		assert.GreaterOrEqual(t, d1, 50*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 30*time.Millisecond)
	})

	t.Run("unregister 3rd and 4th while executing", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter3++
			return true
		}
		executedCounter4 := 0
		callback4 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter4++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)
		ctrl3 := callbacks.Register("c3", callback3)
		ctrl4 := callbacks.Register("c4", callback4)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl3.Unregister(ctx))
		require.Nil(t, ctrl4.Unregister(ctx))
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.Equal(t, 0, executedCounter3)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
	})

	t.Run("unregister while running", func(t *testing.T) {
		counter1 := 0
		counter2 := 0
		max := 25

		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			for {
				if shouldAbort() {
					return false
				}

				time.Sleep(10 * time.Millisecond)
				counter1++

				// 10ms * 25 = 250ms
				if counter1 > max {
					return true
				}
			}
		}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			for {
				if shouldAbort() {
					return false
				}

				time.Sleep(10 * time.Millisecond)
				counter2++

				// 10ms * 25 = 250ms
				if counter2 > max {
					return true
				}
			}
		}

		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(50 * time.Millisecond)
		require.NoError(t, ctrl1.Unregister(ctx))
		require.NoError(t, ctrl2.Unregister(ctx))
		<-chFinished

		assert.False(t, executed)
		assert.LessOrEqual(t, counter1, max)
		assert.LessOrEqual(t, counter2, max)
		assert.LessOrEqual(t, d, 200*time.Millisecond)
	})
}

func TestCycleCallback_Parallel_Deactivate(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("1 executable callback, 1 deactivated", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c1", callback)
		require.Nil(t, ctrl.Deactivate(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 2 deactivated", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		require.Nil(t, ctrl1.Deactivate(ctx))
		require.Nil(t, ctrl2.Deactivate(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 1 deactivated", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)
		require.Nil(t, ctrl1.Deactivate(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.True(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("4 executable callbacks, all deactivated at different time", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter3++
			return true
		}
		executedCounter4 := 0
		callback4 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter4++
			return true
		}
		var executed1 bool
		var executed2 bool
		var executed3 bool
		var executed4 bool
		var d1 time.Duration
		var d2 time.Duration
		var d3 time.Duration
		var d4 time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		ctrl3 := callbacks.Register("c3", callback3)
		ctrl4 := callbacks.Register("c4", callback4)
		require.Nil(t, ctrl3.Deactivate(ctx))

		start := time.Now()
		executed1 = callbacks.CycleCallback(shouldNotAbort)
		d1 = time.Since(start)

		require.Nil(t, ctrl1.Deactivate(ctx))

		start = time.Now()
		executed2 = callbacks.CycleCallback(shouldNotAbort)
		d2 = time.Since(start)

		require.Nil(t, ctrl4.Deactivate(ctx))

		start = time.Now()
		executed3 = callbacks.CycleCallback(shouldNotAbort)
		d3 = time.Since(start)

		require.Nil(t, ctrl2.Deactivate(ctx))

		start = time.Now()
		executed4 = callbacks.CycleCallback(shouldNotAbort)
		d4 = time.Since(start)

		assert.True(t, executed1)
		assert.True(t, executed2)
		assert.True(t, executed3)
		assert.False(t, executed4)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 3, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.Equal(t, 2, executedCounter4)
		assert.GreaterOrEqual(t, d1, 50*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 25*time.Millisecond)
		assert.GreaterOrEqual(t, d3, 25*time.Millisecond)
		assert.GreaterOrEqual(t, d4, 0*time.Millisecond)
	})

	t.Run("deactivate is waiting till the end of execution", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl.Deactivate(ctx))
		du := time.Since(start)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 40*time.Millisecond)
	})

	t.Run("deactivate fails due to context timeout", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed1 bool
		var executed2 bool
		var d1 time.Duration
		var d2 time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed1 = callbacks.CycleCallback(shouldNotAbort)
			d1 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()
		require.NotNil(t, ctrl.Deactivate(ctxTimeout))
		du := time.Since(start)
		<-chFinished

		go func() {
			start := time.Now()
			executed2 = callbacks.CycleCallback(shouldNotAbort)
			d2 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chFinished

		assert.True(t, executed1)
		assert.True(t, executed2)
		assert.Equal(t, 2, executedCounter)
		assert.GreaterOrEqual(t, d1, 50*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 30*time.Millisecond)
	})

	t.Run("deactivate 3rd and 4th while executing", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter3++
			return true
		}
		executedCounter4 := 0
		callback4 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter4++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)
		ctrl3 := callbacks.Register("c3", callback3)
		ctrl4 := callbacks.Register("c4", callback4)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl3.Deactivate(ctx))
		require.Nil(t, ctrl4.Deactivate(ctx))
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.Equal(t, 0, executedCounter3)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
	})

	t.Run("deactivate while running", func(t *testing.T) {
		counter1 := 0
		counter2 := 0
		max := 25

		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			for {
				if shouldAbort() {
					return false
				}

				time.Sleep(10 * time.Millisecond)
				counter1++

				// 10ms * 25 = 250ms
				if counter1 > max {
					return true
				}
			}
		}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			for {
				if shouldAbort() {
					return false
				}

				time.Sleep(10 * time.Millisecond)
				counter2++

				// 10ms * 25 = 250ms
				if counter2 > max {
					return true
				}
			}
		}

		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(50 * time.Millisecond)
		require.NoError(t, ctrl1.Deactivate(ctx))
		require.NoError(t, ctrl2.Deactivate(ctx))
		<-chFinished

		assert.False(t, executed)
		assert.LessOrEqual(t, counter1, max)
		assert.LessOrEqual(t, counter2, max)
		assert.LessOrEqual(t, d, 200*time.Millisecond)

		t.Run("does not abort after activated back again", func(t *testing.T) {
			require.NoError(t, ctrl1.Activate())
			require.NoError(t, ctrl2.Activate())

			counter1 = 0
			counter2 = 0
			max = 10

			go func() {
				start := time.Now()
				executed = callbacks.CycleCallback(shouldNotAbort)
				d = time.Since(start)
				chFinished <- struct{}{}
			}()
			<-chFinished

			assert.True(t, executed)
			assert.Greater(t, counter1, max)
			assert.Greater(t, counter2, max)
			assert.GreaterOrEqual(t, d, 100*time.Millisecond)
		})
	})
}

func TestCycleCallback_Sequential(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("no callbacks", func(t *testing.T) {
		var executed bool

		callbacks := NewCallbackGroup("id", logger, 1)

		executed = callbacks.CycleCallback(shouldNotAbort)

		assert.False(t, executed)
	})

	t.Run("2 executable callbacks", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 75*time.Millisecond)
	})

	t.Run("2 non-executable callbacks", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(10 * time.Millisecond)
			executedCounter1++
			return false
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(10 * time.Millisecond)
			executedCounter2++
			return false
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 10*time.Millisecond)
	})

	t.Run("2 executable callbacks, not executed due to should abort", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		shouldAbortCounter := 0
		shouldAbort := func() bool {
			shouldAbortCounter++
			return shouldAbortCounter > 1
		}

		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)

		start := time.Now()
		executed = callbacks.CycleCallback(shouldAbort)
		d = time.Since(start)

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("register new while executing runs it on the next tick", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter2++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		callbacks.Register("c1", callback1)

		// register 2nd callback while the 1st is executing. The tick dispatches
		// the callbacks due when it started (c1), so the 2nd is not part of it.
		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		callbacks.Register("c2", callback2)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)

		// the next tick picks up the 2nd callback
		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, 1, executedCounter2)
	})

	t.Run("run with intervals", func(T *testing.T) {
		ticker := NewFixedTicker(10 * time.Millisecond)
		intervals2 := NewSeriesIntervals([]time.Duration{
			10 * time.Millisecond, 30 * time.Millisecond, 50 * time.Millisecond,
		})
		intervals3 := NewFixedIntervals(60 * time.Millisecond)
		now := time.Now()

		executionTimes1 := []time.Duration{}
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			executionTimes1 = append(executionTimes1, time.Since(now))
			return true
		}
		executionCounter2 := 0
		executionTimes2 := []time.Duration{}
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			executionCounter2++
			executionTimes2 = append(executionTimes2, time.Since(now))
			// reports executed every 3 calls, should result in 10, 30, 50, 50, 10, 30, 50, 50, ... intervals
			return executionCounter2%4 == 0
		}
		executionTimes3 := []time.Duration{}
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			executionTimes3 = append(executionTimes3, time.Since(now))
			return true
		}

		callbacks := NewCallbackGroup("id", logger, 1)
		// should be called on every tick, with 10 intervals
		callbacks.Register("c1", callback1)
		// should be called with 10, 30, 50, 50, 10, 30, 50, 50, ... intervals
		callbacks.Register("c2", callback2, WithIntervals(intervals2))
		// should be called with 60, 60, ... intervals
		callbacks.Register("c3", callback3, WithIntervals(intervals3))

		cm := NewManager("test", ticker, callbacks.CycleCallback, logger)
		cm.Start()
		time.Sleep(400 * time.Millisecond)
		cm.StopAndWait(context.Background())

		// within 400 ms c1 should be called at least 30x
		require.GreaterOrEqual(t, len(executionTimes1), 30)
		// 1st call on 1st tick after 10ms
		sumDuration := time.Duration(10)
		for i := 0; i < 30; i++ {
			assert.GreaterOrEqual(t, executionTimes1[i], sumDuration)
			sumDuration += 10 * time.Millisecond
		}

		// within 400 ms c2 should be called at least 8x
		require.GreaterOrEqual(t, len(executionTimes2), 8)
		// 1st call on 1st tick after 10ms
		sumDuration = time.Duration(0)
		for i := 0; i < 8; i++ {
			assert.GreaterOrEqual(t, executionTimes2[i], sumDuration)
			switch (i + 1) % 4 {
			case 0:
				sumDuration += 10 * time.Millisecond
			case 1:
				sumDuration += 30 * time.Millisecond
			case 2, 3:
				sumDuration += 50 * time.Millisecond
			}
		}

		// within 400 ms c3 should be called at least 6x
		require.GreaterOrEqual(t, len(executionTimes3), 6)
		// 1st call on 1st tick after 10ms
		sumDuration = time.Duration(0)
		for i := 0; i < 6; i++ {
			assert.GreaterOrEqual(t, executionTimes3[i], sumDuration)
			sumDuration += 60 * time.Millisecond
		}
	})
}

func TestCycleCallback_Sequential_Unregister(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("1 executable callback, 1 unregistered", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("c1", callback)
		require.Nil(t, ctrl.Unregister(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 2 unregistered", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		require.Nil(t, ctrl1.Unregister(ctx))
		require.Nil(t, ctrl2.Unregister(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 1 unregistered", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)
		require.Nil(t, ctrl1.Unregister(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.True(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("4 executable callbacks, all unregistered at different time", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter3++
			return true
		}
		executedCounter4 := 0
		callback4 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter4++
			return true
		}
		var executed1 bool
		var executed2 bool
		var executed3 bool
		var executed4 bool
		var d1 time.Duration
		var d2 time.Duration
		var d3 time.Duration
		var d4 time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		ctrl3 := callbacks.Register("c3", callback3)
		ctrl4 := callbacks.Register("c4", callback4)
		require.Nil(t, ctrl3.Unregister(ctx))

		start := time.Now()
		executed1 = callbacks.CycleCallback(shouldNotAbort)
		d1 = time.Since(start)

		require.Nil(t, ctrl1.Unregister(ctx))

		start = time.Now()
		executed2 = callbacks.CycleCallback(shouldNotAbort)
		d2 = time.Since(start)

		require.Nil(t, ctrl4.Unregister(ctx))

		start = time.Now()
		executed3 = callbacks.CycleCallback(shouldNotAbort)
		d3 = time.Since(start)

		require.Nil(t, ctrl2.Unregister(ctx))

		start = time.Now()
		executed4 = callbacks.CycleCallback(shouldNotAbort)
		d4 = time.Since(start)

		assert.True(t, executed1)
		assert.True(t, executed2)
		assert.True(t, executed3)
		assert.False(t, executed4)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 3, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.Equal(t, 2, executedCounter4)
		assert.GreaterOrEqual(t, d1, 75*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 50*time.Millisecond)
		assert.GreaterOrEqual(t, d3, 25*time.Millisecond)
		assert.GreaterOrEqual(t, d4, 0*time.Millisecond)
	})

	t.Run("unregister is waiting till the end of execution", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl.Unregister(ctx))
		du := time.Since(start)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 40*time.Millisecond)
	})

	t.Run("unregister fails due to context timeout", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed1 bool
		var executed2 bool
		var d1 time.Duration
		var d2 time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed1 = callbacks.CycleCallback(shouldNotAbort)
			d1 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()
		require.NotNil(t, ctrl.Unregister(ctxTimeout))
		du := time.Since(start)
		<-chFinished

		go func() {
			start := time.Now()
			executed2 = callbacks.CycleCallback(shouldNotAbort)
			d2 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chFinished

		assert.True(t, executed1)
		assert.True(t, executed2)
		assert.Equal(t, 2, executedCounter)
		assert.GreaterOrEqual(t, d1, 50*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 30*time.Millisecond)
	})

	t.Run("unregister 2nd and 3rd while executing", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter3++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		ctrl3 := callbacks.Register("c3", callback3)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl2.Unregister(ctx))
		require.Nil(t, ctrl3.Unregister(ctx))
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
	})

	t.Run("unregister while running", func(t *testing.T) {
		counter := 0
		max := 25
		callback := func(shouldAbort ShouldAbortCallback) bool {
			for {
				if shouldAbort() {
					return false
				}

				time.Sleep(10 * time.Millisecond)
				counter++

				// 10ms * 25 = 250ms
				if counter > max {
					return true
				}
			}
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(50 * time.Millisecond)
		require.NoError(t, ctrl.Unregister(ctx))
		<-chFinished

		assert.False(t, executed)
		assert.LessOrEqual(t, counter, max)
		assert.LessOrEqual(t, d, 200*time.Millisecond)
	})
}

func TestCycleCallback_Parallel_DueDispatch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("only due callbacks execute among many not-due", func(t *testing.T) {
		const total = 200
		var executed int64

		callbacks := NewCallbackGroup("id", logger, 4)
		// long interval: due on the first tick (registration allows immediate
		// execution), not due afterwards
		for i := 0; i < total; i++ {
			callbacks.Register(fmt.Sprintf("c%d", i),
				func(shouldAbort ShouldAbortCallback) bool {
					atomic.AddInt64(&executed, 1)
					return true
				}, WithIntervals(NewFixedIntervals(time.Hour)))
		}

		// 1st tick: all due
		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(total), atomic.LoadInt64(&executed))

		// 2nd tick: none due, nothing runs
		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(total), atomic.LoadInt64(&executed))
	})

	t.Run("concurrent register and unregister during cycles is race free", func(t *testing.T) {
		callback := func(shouldAbort ShouldAbortCallback) bool { return true }

		callbacks := NewCallbackGroup("id", logger, 4)
		const stable = 8
		for i := 0; i < stable; i++ {
			callbacks.Register(fmt.Sprintf("stable%d", i), callback)
		}

		stop := make(chan struct{})
		wg := new(sync.WaitGroup)
		// churn: continuously register then unregister while cycles run
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctrl := callbacks.Register("churn", callback)
				assert.NoError(t, ctrl.Unregister(context.Background()))
			}
		}()

		for i := 0; i < 200; i++ {
			callbacks.CycleCallback(shouldNotAbort)
		}
		close(stop)
		wg.Wait()

		// final tick prunes ids of the churned callbacks
		callbacks.CycleCallback(shouldNotAbort)

		group := callbacks.(*cycleCallbackGroup)
		group.Lock()
		defer group.Unlock()
		assert.Len(t, group.callbacks, stable)
		assert.Len(t, group.dueHeap, stable)
	})
}

// BenchmarkCycleCallback_Parallel measures per-tick scheduling overhead of an
// index-level group. The two regimes mirror the backoff decision in
// index_cyclecallbacks.go: MT registers each shard entry with its own interval
// (WithIntervals) and on a steady-state tick only a few shards are due; ST
// registers entries with no interval (the ticker backs off instead), so every
// entry is due whenever the group fires.
func BenchmarkCycleCallback_Parallel(b *testing.B) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }
	noop := func(shouldAbort ShouldAbortCallback) bool { return true }

	cases := []struct {
		name        string
		multiTenant bool
		registered  int
		due         int // only meaningful for MT; ST is always all-due
	}{
		{name: "MT/registered=5000/due=0", multiTenant: true, registered: 5000, due: 0},
		{name: "MT/registered=5000/due=5", multiTenant: true, registered: 5000, due: 5},
		{name: "MT/registered=5000/due=200", multiTenant: true, registered: 5000, due: 200},
		{name: "MT/registered=5000/due=1000", multiTenant: true, registered: 5000, due: 1000},
		{name: "MT/registered=5000/due=5000", multiTenant: true, registered: 5000, due: 5000},
		{name: "MT/registered=1000/due=5", multiTenant: true, registered: 1000, due: 5},
		{name: "MT/registered=10000/due=5", multiTenant: true, registered: 10000, due: 5},
		{name: "ST/registered=1000", registered: 1000},
		{name: "ST/registered=5000", registered: 5000},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			callbacks := NewCallbackGroup("id", logger, 4)
			for i := 0; i < tc.registered; i++ {
				switch {
				case !tc.multiTenant:
					// ST: no per-entry interval, so always due when the group fires
					callbacks.Register(fmt.Sprintf("st%d", i), noop)
				case i < tc.due:
					// MT due entry: interval already elapsed, due on every tick
					callbacks.Register(fmt.Sprintf("due%d", i), noop,
						WithIntervals(NewFixedIntervals(time.Nanosecond)))
				default:
					// MT backed-off entry: long interval; priming below records
					// started=now so it stays not-due for the rest of the benchmark
					callbacks.Register(fmt.Sprintf("idle%d", i), noop,
						WithIntervals(NewFixedIntervals(time.Hour)))
				}
			}
			callbacks.CycleCallback(shouldNotAbort)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				callbacks.CycleCallback(shouldNotAbort)
			}
		})
	}
}

func TestCycleCallback_Sequential_Deactivate(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("1 executable callback, 1 deactivated", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("c1", callback)
		require.Nil(t, ctrl.Deactivate(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 2 deactivated", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		require.Nil(t, ctrl1.Deactivate(ctx))
		require.Nil(t, ctrl2.Deactivate(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 1 deactivated", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", callback1)
		callbacks.Register("c2", callback2)
		require.Nil(t, ctrl1.Deactivate(ctx))

		start := time.Now()
		executed = callbacks.CycleCallback(shouldNotAbort)
		d = time.Since(start)

		assert.True(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("4 executable callbacks, all deactivated at different time", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter3++
			return true
		}
		executedCounter4 := 0
		callback4 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter4++
			return true
		}
		var executed1 bool
		var executed2 bool
		var executed3 bool
		var executed4 bool
		var d1 time.Duration
		var d2 time.Duration
		var d3 time.Duration
		var d4 time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		ctrl3 := callbacks.Register("c3", callback3)
		ctrl4 := callbacks.Register("c4", callback4)
		require.Nil(t, ctrl3.Deactivate(ctx))

		start := time.Now()
		executed1 = callbacks.CycleCallback(shouldNotAbort)
		d1 = time.Since(start)

		require.Nil(t, ctrl1.Deactivate(ctx))

		start = time.Now()
		executed2 = callbacks.CycleCallback(shouldNotAbort)
		d2 = time.Since(start)

		require.Nil(t, ctrl4.Deactivate(ctx))

		start = time.Now()
		executed3 = callbacks.CycleCallback(shouldNotAbort)
		d3 = time.Since(start)

		require.Nil(t, ctrl2.Deactivate(ctx))

		start = time.Now()
		executed4 = callbacks.CycleCallback(shouldNotAbort)
		d4 = time.Since(start)

		assert.True(t, executed1)
		assert.True(t, executed2)
		assert.True(t, executed3)
		assert.False(t, executed4)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 3, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.Equal(t, 2, executedCounter4)
		assert.GreaterOrEqual(t, d1, 75*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 50*time.Millisecond)
		assert.GreaterOrEqual(t, d3, 25*time.Millisecond)
		assert.GreaterOrEqual(t, d4, 0*time.Millisecond)
	})

	t.Run("deactivate is waiting till the end of execution", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl.Deactivate(ctx))
		du := time.Since(start)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 40*time.Millisecond)
	})

	t.Run("deactivate fails due to context timeout", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed1 bool
		var executed2 bool
		var d1 time.Duration
		var d2 time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed1 = callbacks.CycleCallback(shouldNotAbort)
			d1 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()
		require.NotNil(t, ctrl.Deactivate(ctxTimeout))
		du := time.Since(start)
		<-chFinished

		go func() {
			start := time.Now()
			executed2 = callbacks.CycleCallback(shouldNotAbort)
			d2 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chFinished

		assert.True(t, executed1)
		assert.True(t, executed2)
		assert.Equal(t, 2, executedCounter)
		assert.GreaterOrEqual(t, d1, 50*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 30*time.Millisecond)
	})

	t.Run("deactivate 2nd and 3rd while executing", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter3++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		callbacks.Register("c1", callback1)
		ctrl2 := callbacks.Register("c2", callback2)
		ctrl3 := callbacks.Register("c3", callback3)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl2.Deactivate(ctx))
		require.Nil(t, ctrl3.Deactivate(ctx))
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
	})

	t.Run("deactivate while running", func(t *testing.T) {
		counter := 0
		max := 25
		callback := func(shouldAbort ShouldAbortCallback) bool {
			for {
				if shouldAbort() {
					return false
				}

				time.Sleep(10 * time.Millisecond)
				counter++

				// 10ms * 25 = 250ms
				if counter > max {
					return true
				}
			}
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("c", callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(50 * time.Millisecond)
		require.NoError(t, ctrl.Deactivate(ctx))
		<-chFinished

		assert.False(t, executed)
		assert.LessOrEqual(t, counter, max)
		assert.LessOrEqual(t, d, 200*time.Millisecond)

		t.Run("does not abort after activated back again", func(t *testing.T) {
			require.NoError(t, ctrl.Activate())

			counter = 0
			max = 10

			go func() {
				start := time.Now()
				executed = callbacks.CycleCallback(shouldNotAbort)
				d = time.Since(start)
				chFinished <- struct{}{}
			}()
			<-chFinished

			assert.True(t, executed)
			assert.Greater(t, counter, max)
			assert.GreaterOrEqual(t, d, 100*time.Millisecond)
		})
	})
}

// TestCycleCallback_DueHeap covers the due-heap scheduling behaviors in both
// dispatch modes: nil-interval "always due" entries, running only the due
// subset, the single-membership invariant across deactivate/activate, panic
// containment, and restoring drained-but-un-run entries on abort.
func TestCycleCallback_DueHeap(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	modes := []struct {
		name     string
		routines int
	}{
		{"sequential", 1},
		{"parallel", 4},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			t.Run("always-due entry runs every tick", func(t *testing.T) {
				var counter int64
				callbacks := NewCallbackGroup("id", logger, mode.routines)
				callbacks.Register("always", func(shouldAbort ShouldAbortCallback) bool {
					atomic.AddInt64(&counter, 1)
					return true
				})

				// nil interval means always due; each tick runs it exactly once,
				// never re-popped within the same tick
				for tick := int64(1); tick <= 3; tick++ {
					assert.True(t, callbacks.CycleCallback(shouldNotAbort))
					assert.Equal(t, tick, atomic.LoadInt64(&counter))
				}
			})

			t.Run("only due entries run", func(t *testing.T) {
				var alwaysCounter, longCounter int64
				callbacks := NewCallbackGroup("id", logger, mode.routines)
				callbacks.Register("always", func(shouldAbort ShouldAbortCallback) bool {
					atomic.AddInt64(&alwaysCounter, 1)
					return true
				})
				callbacks.Register("long", func(shouldAbort ShouldAbortCallback) bool {
					atomic.AddInt64(&longCounter, 1)
					return true
				}, WithIntervals(NewFixedIntervals(time.Hour)))

				// 1st tick: both due (registration allows immediate execution)
				assert.True(t, callbacks.CycleCallback(shouldNotAbort))
				assert.Equal(t, int64(1), atomic.LoadInt64(&alwaysCounter))
				assert.Equal(t, int64(1), atomic.LoadInt64(&longCounter))

				// 2nd tick: only the always-due entry runs, long interval not elapsed
				assert.True(t, callbacks.CycleCallback(shouldNotAbort))
				assert.Equal(t, int64(2), atomic.LoadInt64(&alwaysCounter))
				assert.Equal(t, int64(1), atomic.LoadInt64(&longCounter))
			})

			t.Run("activate after deactivate runs once", func(t *testing.T) {
				var counter int64
				callbacks := NewCallbackGroup("id", logger, mode.routines)
				ctrl := callbacks.Register("c", func(shouldAbort ShouldAbortCallback) bool {
					atomic.AddInt64(&counter, 1)
					return true
				})
				require.NoError(t, ctrl.Deactivate(context.Background()))
				require.NoError(t, ctrl.Activate())

				assert.True(t, callbacks.CycleCallback(shouldNotAbort))
				assert.Equal(t, int64(1), atomic.LoadInt64(&counter))

				// exactly one heap entry afterwards - the activate push did not
				// duplicate it
				group := callbacks.(*cycleCallbackGroup)
				group.Lock()
				heapLen := group.dueHeap.Len()
				group.Unlock()
				assert.Equal(t, 1, heapLen)
			})

			t.Run("panicking callback is contained and rescheduled", func(t *testing.T) {
				const siblings = 4
				var okCounter, panicCounter int64
				callbacks := NewCallbackGroup("id", logger, mode.routines)
				for i := 0; i < siblings; i++ {
					callbacks.Register(fmt.Sprintf("ok%d", i), func(shouldAbort ShouldAbortCallback) bool {
						atomic.AddInt64(&okCounter, 1)
						return true
					})
				}
				callbacks.Register("panic", func(shouldAbort ShouldAbortCallback) bool {
					atomic.AddInt64(&panicCounter, 1)
					panic("boom")
				})

				// 1st tick: siblings all run, the panic is contained
				callbacks.CycleCallback(shouldNotAbort)
				assert.Equal(t, int64(siblings), atomic.LoadInt64(&okCounter))
				assert.Equal(t, int64(1), atomic.LoadInt64(&panicCounter))

				// 2nd tick: the panicking entry was rescheduled, so it runs again
				callbacks.CycleCallback(shouldNotAbort)
				assert.Equal(t, int64(2), atomic.LoadInt64(&panicCounter))
			})

			t.Run("abort restores un-run drained entries", func(t *testing.T) {
				const total = 5
				var counter int64
				callbacks := NewCallbackGroup("id", logger, mode.routines)
				for i := 0; i < total; i++ {
					callbacks.Register(fmt.Sprintf("c%d", i), func(shouldAbort ShouldAbortCallback) bool {
						atomic.AddInt64(&counter, 1)
						return true
					})
				}

				// abort the whole tick: every drained entry is restored, none run
				assert.False(t, callbacks.CycleCallback(func() bool { return true }))
				assert.Equal(t, int64(0), atomic.LoadInt64(&counter))

				group := callbacks.(*cycleCallbackGroup)
				group.Lock()
				heapLen := group.dueHeap.Len()
				group.Unlock()
				assert.Equal(t, total, heapLen) // nothing orphaned outside the heap

				// next tick runs exactly the restored entries
				assert.True(t, callbacks.CycleCallback(shouldNotAbort))
				assert.Equal(t, int64(total), atomic.LoadInt64(&counter))
			})

			t.Run("abort then deactivate does not resurrect", func(t *testing.T) {
				const total = 4
				counters := make([]int64, total)
				callbacks := NewCallbackGroup("id", logger, mode.routines)
				ctrls := make([]CycleCallbackCtrl, total)
				for i := 0; i < total; i++ {
					idx := i
					ctrls[i] = callbacks.Register(fmt.Sprintf("c%d", i), func(shouldAbort ShouldAbortCallback) bool {
						atomic.AddInt64(&counters[idx], 1)
						return true
					})
				}

				// abort tick: all entries restored to the heap, none run
				assert.False(t, callbacks.CycleCallback(func() bool { return true }))

				// deactivate one restored entry
				require.NoError(t, ctrls[0].Deactivate(context.Background()))

				// next tick runs every entry but the deactivated one
				assert.True(t, callbacks.CycleCallback(shouldNotAbort))
				assert.Equal(t, int64(0), atomic.LoadInt64(&counters[0]))
				for i := 1; i < total; i++ {
					assert.Equal(t, int64(1), atomic.LoadInt64(&counters[i]))
				}
			})
		})
	}

	t.Run("activate during execution does not duplicate the heap entry", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("blocking", func(shouldAbort ShouldAbortCallback) bool {
			started <- struct{}{}
			<-release
			return true
		})

		// run a tick concurrently: it drains the entry (heapIndex -1) and blocks
		// inside the callback, holding it in flight
		done := make(chan bool, 1)
		go func() {
			done <- callbacks.CycleCallback(shouldNotAbort)
		}()
		<-started

		// activate the in-flight entry: activate re-queues it (its heapIndex -1
		// guard passes), so the tick's repushDue must not push it a second time
		require.NoError(t, ctrl.Activate())
		close(release)
		<-done

		group := callbacks.(*cycleCallbackGroup)
		group.Lock()
		heapLen := group.dueHeap.Len()
		group.Unlock()
		assert.Equal(t, 1, heapLen)
	})

	t.Run("sequential abort after first execution restores the remainder", func(t *testing.T) {
		const total = 4
		var counter int64
		callbacks := NewCallbackGroup("id", logger, 1)
		for i := 0; i < total; i++ {
			callbacks.Register(fmt.Sprintf("c%d", i), func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&counter, 1)
				return true
			})
		}

		// abort once the first callback has run; the sequential loop stops
		// executing but restores every remaining drained entry
		shouldAbort := func() bool { return atomic.LoadInt64(&counter) >= 1 }
		assert.True(t, callbacks.CycleCallback(shouldAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&counter))

		group := callbacks.(*cycleCallbackGroup)
		group.Lock()
		heapLen := group.dueHeap.Len()
		group.Unlock()
		assert.Equal(t, total, heapLen) // executed one re-pushed, remainder restored

		// next tick runs all four again
		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1+total), atomic.LoadInt64(&counter))
	})
}
