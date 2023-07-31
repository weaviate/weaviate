//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import (
	"context"
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

		callbacks := NewCycleCallbacks("id", logger, 2)

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

		callbacks := NewCycleCallbacks("id", logger, 2)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)

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

		callbacks := NewCycleCallbacks("id", logger, 2)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)

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

		callbacks := NewCycleCallbacks("id", logger, 2)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)

		start := time.Now()
		executed = callbacks.CycleCallback(shouldAbort)
		d = time.Since(start)

		assert.True(t, executed)
		// only one routine should fail, unknown which one
		assert.Equal(t, 1, executedCounter1+executedCounter2)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("register new while executing", func(t *testing.T) {
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)
		callbacks.Register("c3", true, callback3)

		// register 4th callback while other are executed,
		//
		// while 1st and 2nd are being processed (50ms),
		// 3rd is waiting for available routine (without 3rd callback loop would be finished)
		// 4th is registered (25ms) to be called next along with 3rd
		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		callbacks.Register("c4", true, callback4)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.Equal(t, 1, executedCounter3)
		assert.Equal(t, 1, executedCounter4)
		assert.GreaterOrEqual(t, d, 100*time.Millisecond)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl := callbacks.Register("c1", true, callback)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl1 := callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl1 := callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl1 := callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
		ctrl3 := callbacks.Register("c3", true, callback3)
		ctrl4 := callbacks.Register("c4", true, callback4)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl := callbacks.Register("c", true, callback)

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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl := callbacks.Register("c", true, callback)

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

		callbacks := NewCycleCallbacks("id", logger, 2)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)
		ctrl3 := callbacks.Register("c3", true, callback3)
		ctrl4 := callbacks.Register("c4", true, callback4)

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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl := callbacks.Register("c1", true, callback)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl1 := callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl1 := callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl1 := callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
		ctrl3 := callbacks.Register("c3", true, callback3)
		ctrl4 := callbacks.Register("c4", true, callback4)
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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl := callbacks.Register("c", true, callback)

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

		callbacks := NewCycleCallbacks("id", logger, 2)
		ctrl := callbacks.Register("c", true, callback)

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

		callbacks := NewCycleCallbacks("id", logger, 2)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)
		ctrl3 := callbacks.Register("c3", true, callback3)
		ctrl4 := callbacks.Register("c4", true, callback4)

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
}

func TestCycleCallback_Sequential(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("no callbacks", func(t *testing.T) {
		var executed bool

		callbacks := NewCycleCallbacks("id", logger, 1)

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

		callbacks := NewCycleCallbacks("id", logger, 1)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)

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

		callbacks := NewCycleCallbacks("id", logger, 1)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)

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

		callbacks := NewCycleCallbacks("id", logger, 1)
		callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)

		start := time.Now()
		executed = callbacks.CycleCallback(shouldAbort)
		d = time.Since(start)

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("register new while executing", func(t *testing.T) {
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		callbacks.Register("c1", true, callback1)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.CycleCallback(shouldNotAbort)
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		callbacks.Register("c2", true, callback2)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 100*time.Millisecond)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl := callbacks.Register("c1", true, callback)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl1 := callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl1 := callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl1 := callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
		ctrl3 := callbacks.Register("c3", true, callback3)
		ctrl4 := callbacks.Register("c4", true, callback4)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl := callbacks.Register("c", true, callback)

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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl := callbacks.Register("c", true, callback)

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

		callbacks := NewCycleCallbacks("id", logger, 1)
		callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
		ctrl3 := callbacks.Register("c3", true, callback3)

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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl := callbacks.Register("c1", true, callback)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl1 := callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl1 := callbacks.Register("c1", true, callback1)
		callbacks.Register("c2", true, callback2)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl1 := callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
		ctrl3 := callbacks.Register("c3", true, callback3)
		ctrl4 := callbacks.Register("c4", true, callback4)
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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl := callbacks.Register("c", true, callback)

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

		callbacks := NewCycleCallbacks("id", logger, 1)
		ctrl := callbacks.Register("c", true, callback)

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

		callbacks := NewCycleCallbacks("id", logger, 1)
		callbacks.Register("c1", true, callback1)
		ctrl2 := callbacks.Register("c2", true, callback2)
		ctrl3 := callbacks.Register("c3", true, callback3)

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
}
