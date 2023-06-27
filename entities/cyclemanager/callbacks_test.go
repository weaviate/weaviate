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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallbacks_Multi(t *testing.T) {
	ctx := context.Background()

	t.Run("no callbacks", func(t *testing.T) {
		ch := make(chan struct{}, 1)
		var executed bool

		callbacks := newMultiCallbacks()

		go func() {
			executed = callbacks.execute(func() bool { return false })
			ch <- struct{}{}
		}()
		<-ch

		assert.False(t, executed)
	})

	t.Run("2 executable callbacks", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		ch := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := newMultiCallbacks()
		callbacks.register(callback1)
		callbacks.register(callback2)

		go func() {
			start := time.Now()
			executed = callbacks.execute(func() bool { return false })
			d = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 75*time.Millisecond)
	})

	t.Run("2 non-executable callbacks", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(10 * time.Millisecond)
			executedCounter1++
			return false
		}
		executedCounter2 := 0
		callback2 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(10 * time.Millisecond)
			executedCounter2++
			return false
		}
		ch := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := newMultiCallbacks()
		callbacks.register(callback1)
		callbacks.register(callback2)

		go func() {
			start := time.Now()
			executed = callbacks.execute(func() bool { return false })
			d = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		assert.False(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 20*time.Millisecond)
	})

	t.Run("1 executable callback, 1 unregistered", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		ch := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := newMultiCallbacks()
		unregister := callbacks.register(callback1)
		require.Nil(t, unregister(ctx))

		go func() {
			start := time.Now()
			executed = callbacks.execute(func() bool { return false })
			d = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 2 unregistered", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		ch := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := newMultiCallbacks()
		unregister1 := callbacks.register(callback1)
		unregister2 := callbacks.register(callback2)
		require.Nil(t, unregister1(ctx))
		require.Nil(t, unregister2(ctx))

		go func() {
			start := time.Now()
			executed = callbacks.execute(func() bool { return false })
			d = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		assert.False(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d, 0*time.Millisecond)
	})

	t.Run("2 executable callbacks, 1 unregistered", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		ch := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := newMultiCallbacks()
		unregister1 := callbacks.register(callback1)
		callbacks.register(callback2)
		require.Nil(t, unregister1(ctx))

		go func() {
			start := time.Now()
			executed = callbacks.execute(func() bool { return false })
			d = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		assert.True(t, executed)
		assert.Equal(t, 0, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 25*time.Millisecond)
	})

	t.Run("4 executable callbacks, all unregistered at different time", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter3++
			return true
		}
		executedCounter4 := 0
		callback4 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter4++
			return true
		}
		ch := make(chan struct{}, 1)
		var executed1 bool
		var executed2 bool
		var executed3 bool
		var executed4 bool
		var d1 time.Duration
		var d2 time.Duration
		var d3 time.Duration
		var d4 time.Duration

		callbacks := newMultiCallbacks()
		unregister1 := callbacks.register(callback1)
		unregister2 := callbacks.register(callback2)
		unregister3 := callbacks.register(callback3)
		unregister4 := callbacks.register(callback4)
		require.Nil(t, unregister3(ctx))

		go func() {
			start := time.Now()
			executed1 = callbacks.execute(func() bool { return false })
			d1 = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		require.Nil(t, unregister1(ctx))

		go func() {
			start := time.Now()
			executed2 = callbacks.execute(func() bool { return false })
			d2 = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		require.Nil(t, unregister4(ctx))

		go func() {
			start := time.Now()
			executed3 = callbacks.execute(func() bool { return false })
			d3 = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		require.Nil(t, unregister2(ctx))

		go func() {
			start := time.Now()
			executed4 = callbacks.execute(func() bool { return false })
			d4 = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

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

	t.Run("2 executable callbacks, not executed due to should break", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(25 * time.Millisecond)
			executedCounter2++
			return true
		}
		shouldBreakCounter := 0
		shouldBreak := func() bool {
			shouldBreakCounter++
			return shouldBreakCounter > 1
		}

		ch := make(chan struct{}, 1)
		var executed1 bool
		var executed2 bool
		var d1 time.Duration
		var d2 time.Duration

		callbacks := newMultiCallbacks()
		callbacks.register(callback1)
		callbacks.register(callback2)

		go func() {
			start := time.Now()
			executed1 = callbacks.execute(shouldBreak)
			d1 = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch
		go func() {
			start := time.Now()
			executed2 = callbacks.execute(shouldBreak)
			d2 = time.Since(start)
			ch <- struct{}{}
		}()
		<-ch

		assert.True(t, executed1)
		assert.False(t, executed2)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.GreaterOrEqual(t, d1, 25*time.Millisecond)
		assert.GreaterOrEqual(t, d2, 0*time.Millisecond)
	})

	t.Run("unregister is waiting till the end of execution", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := newMultiCallbacks()
		unregister := callbacks.register(callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.execute(func() bool { return false })
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, unregister(ctx))
		du := time.Since(start)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
		assert.GreaterOrEqual(t, du, 50*time.Millisecond)
	})

	t.Run("unregister fails due to context timeout", func(t *testing.T) {
		executedCounter := 0
		callback := func(shouldBreak ShouldBreakFunc) bool {
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

		callbacks := newMultiCallbacks()
		unregister := callbacks.register(callback)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed1 = callbacks.execute(func() bool { return false })
			d1 = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		require.NotNil(t, unregister(ctxTimeout))
		du := time.Since(start)
		<-chFinished

		go func() {
			start := time.Now()
			executed2 = callbacks.execute(func() bool { return false })
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

		cancel()
	})

	t.Run("register new while executing", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter2++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := newMultiCallbacks()
		callbacks.register(callback1)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.execute(func() bool { return false })
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		callbacks.register(callback2)
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 1, executedCounter2)
		assert.GreaterOrEqual(t, d, 100*time.Millisecond)
	})

	t.Run("unregister 2nd and 3rd while executing", func(t *testing.T) {
		executedCounter1 := 0
		callback1 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter1++
			return true
		}
		executedCounter2 := 0
		callback2 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter2++
			return true
		}
		executedCounter3 := 0
		callback3 := func(shouldBreak ShouldBreakFunc) bool {
			time.Sleep(50 * time.Millisecond)
			executedCounter3++
			return true
		}
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)
		var executed bool
		var d time.Duration

		callbacks := newMultiCallbacks()
		callbacks.register(callback1)
		unregister2 := callbacks.register(callback2)
		unregister3 := callbacks.register(callback3)

		go func() {
			chStarted <- struct{}{}
			start := time.Now()
			executed = callbacks.execute(func() bool { return false })
			d = time.Since(start)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, unregister2(ctx))
		require.Nil(t, unregister3(ctx))
		<-chFinished

		assert.True(t, executed)
		assert.Equal(t, 1, executedCounter1)
		assert.Equal(t, 0, executedCounter2)
		assert.Equal(t, 0, executedCounter3)
		assert.GreaterOrEqual(t, d, 50*time.Millisecond)
	})
}
