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
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// Black-box behavioral tests
// ----------------------------------------------------------------------------

func TestCycleCallback_Parallel(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("no callbacks", func(t *testing.T) {
		callbacks := NewCallbackGroup("id", logger, 2)
		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
	})

	t.Run("2 executable callbacks", func(t *testing.T) {
		var c1, c2 int64

		synctest.Test(t, func(t *testing.T) {
			callbacks := NewCallbackGroup("id", logger, 2)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				time.Sleep(50 * time.Millisecond)
				atomic.AddInt64(&c1, 1)
				return true
			})
			callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				time.Sleep(25 * time.Millisecond)
				atomic.AddInt64(&c2, 1)
				return true
			})

			start := time.Now()
			executed := callbacks.CycleCallback(shouldNotAbort)
			d := time.Since(start)

			assert.True(t, executed)
			assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
			assert.GreaterOrEqual(t, d, 50*time.Millisecond)
		})
	})

	t.Run("2 non-executable callbacks", func(t *testing.T) {
		var c1, c2 int64

		synctest.Test(t, func(t *testing.T) {
			callbacks := NewCallbackGroup("id", logger, 2)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt64(&c1, 1)
				return false
			})
			callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt64(&c2, 1)
				return false
			})

			start := time.Now()
			executed := callbacks.CycleCallback(shouldNotAbort)
			d := time.Since(start)

			assert.False(t, executed)
			assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
			assert.GreaterOrEqual(t, d, 10*time.Millisecond)
		})
	})

	t.Run("abort skips some callbacks", func(t *testing.T) {
		var c1, c2, c3 int64
		shouldAbortCtr := uint32(0)
		shouldAbort := func() bool {
			return atomic.AddUint32(&shouldAbortCtr, 1) > 2
		}
		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		callbacks.Register("c3", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c3, 1)
			return true
		})

		executed := callbacks.CycleCallback(shouldAbort)

		assert.True(t, executed)
		total := atomic.LoadInt64(&c1) + atomic.LoadInt64(&c2) + atomic.LoadInt64(&c3)
		assert.Greater(t, total, int64(0))
		assert.Less(t, total, int64(3))
	})

	t.Run("register new while executing runs it on next tick", func(t *testing.T) {
		var c1, c2, c3, c4 int64
		chFinished := make(chan struct{}, 1)

		synctest.Test(t, func(t *testing.T) {
			// tickRunning is closed by c1 when it starts executing.
			// release is closed by the test to let c1 proceed. This
			// channel rendezvous is a true happens-before guarantee that
			// Register("c4") is called while the tick is in-progress.
			tickRunning := make(chan struct{})
			release := make(chan struct{})

			callbacks := NewCallbackGroup("id", logger, 2)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				close(tickRunning)
				<-release
				atomic.AddInt64(&c1, 1)
				return true
			})
			callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c2, 1)
				return true
			})
			callbacks.Register("c3", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c3, 1)
				return true
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-tickRunning
			// c1 is confirmed mid-execution (blocked on release). Register
			// c4 now: it arrives after drainDue snapshotted the due set for
			// this tick, so it will not be included in the current tick.
			callbacks.Register("c4", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c4, 1)
				return true
			})
			close(release)
			<-chFinished

			assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
			assert.Equal(t, int64(1), atomic.LoadInt64(&c3))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c4))

			assert.True(t, callbacks.CycleCallback(shouldNotAbort))
			assert.Equal(t, int64(1), atomic.LoadInt64(&c4))
		})
	})

	t.Run("idle tick executes nothing", func(t *testing.T) {
		var c1, c2 int64
		callbacks := NewCallbackGroup("id", logger, 2)
		callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			atomic.AddInt64(&c1, 1)
			return true
		}, WithIntervals(NewFixedIntervals(time.Hour)))
		callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			atomic.AddInt64(&c2, 1)
			return true
		}, WithIntervals(NewFixedIntervals(time.Hour)))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c2))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
	})
}

func TestCycleCallback_Parallel_Unregister(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("1 executable callback 1 unregistered", func(t *testing.T) {
		var counter int64
		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&counter, 1)
			return true
		})
		require.Nil(t, ctrl.Unregister(ctx))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&counter))
	})

	t.Run("unregister is waiting till end of execution", func(t *testing.T) {
		var counter int64
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&counter, 1)
			return true
		})

		go func() {
			chStarted <- struct{}{}
			callbacks.CycleCallback(shouldNotAbort)
			chFinished <- struct{}{}
		}()
		<-chStarted
		start := time.Now()
		time.Sleep(25 * time.Millisecond)
		require.Nil(t, ctrl.Unregister(ctx))
		du := time.Since(start)
		<-chFinished

		assert.Equal(t, int64(1), atomic.LoadInt64(&counter))
		assert.GreaterOrEqual(t, du, 40*time.Millisecond)
	})

	t.Run("unregister fails due to context timeout", func(t *testing.T) {
		var counter int64
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&counter, 1)
			return true
		})

		go func() {
			chStarted <- struct{}{}
			callbacks.CycleCallback(shouldNotAbort)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(25 * time.Millisecond)
		ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		defer cancel()
		require.NotNil(t, ctrl.Unregister(ctxTimeout))
		<-chFinished

		assert.True(t, ctrl.IsActive())
		go func() {
			callbacks.CycleCallback(shouldNotAbort)
			chFinished <- struct{}{}
		}()
		<-chFinished

		assert.Equal(t, int64(2), atomic.LoadInt64(&counter))
	})

	t.Run("unregister while running cooperative abort", func(t *testing.T) {
		var counter int64
		max := int64(25)
		chStarted := make(chan struct{}, 1)
		chFinished := make(chan struct{}, 1)

		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c", func(shouldAbort ShouldAbortCallback) bool {
			for {
				if shouldAbort() {
					return false
				}
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt64(&counter, 1)
				if atomic.LoadInt64(&counter) > max {
					return true
				}
			}
		})

		go func() {
			chStarted <- struct{}{}
			callbacks.CycleCallback(shouldNotAbort)
			chFinished <- struct{}{}
		}()
		<-chStarted
		time.Sleep(50 * time.Millisecond)
		require.NoError(t, ctrl.Unregister(ctx))
		<-chFinished

		assert.LessOrEqual(t, atomic.LoadInt64(&counter), max)
	})

	t.Run("2 all unregistered 0 executed", func(t *testing.T) {
		var c1, c2 int64
		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		ctrl2 := callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		require.Nil(t, ctrl1.Unregister(ctx))
		require.Nil(t, ctrl2.Unregister(ctx))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c2))
	})

	t.Run("1 of 2 unregistered 1 executed", func(t *testing.T) {
		var c1, c2 int64
		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl1 := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		require.Nil(t, ctrl1.Unregister(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
	})

	t.Run("unregister 3rd and 4th while executing", func(t *testing.T) {
		var c1, c2, c3, c4 int64

		synctest.Test(t, func(t *testing.T) {
			// tickRunning is closed by c1 on its first execution, confirming
			// drainDue has already snapshotted the due set for that tick. c3
			// and c4 are registered only after that signal, so they are absent
			// from the first tick's due slice. Unregister removes them before
			// any future tick can dispatch them.
			tickRunning := make(chan struct{})
			var once sync.Once
			release := make(chan struct{})
			chFinished := make(chan struct{}, 1)

			callbacks := NewCallbackGroup("id", logger, 2)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				once.Do(func() { close(tickRunning) })
				<-release
				atomic.AddInt64(&c1, 1)
				return true
			})
			callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				<-release
				atomic.AddInt64(&c2, 1)
				return true
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-tickRunning

			// c1 is confirmed executing; drainDue already returned [c1, c2].
			// c3 and c4 registered now are not in this tick's dispatch set.
			ctrl3 := callbacks.Register("c3", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c3, 1)
				return true
			})
			ctrl4 := callbacks.Register("c4", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c4, 1)
				return true
			})
			require.Nil(t, ctrl3.Unregister(ctx))
			require.Nil(t, ctrl4.Unregister(ctx))

			// release lets c1 and c2 finish tick 1; closed channel unblocks
			// c1 and c2 again in tick 2 without stalling.
			close(release)
			<-chFinished

			assert.True(t, callbacks.CycleCallback(shouldNotAbort))
			assert.Equal(t, int64(2), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(2), atomic.LoadInt64(&c2))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c3))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c4))
		})
	})
}

func TestCycleCallback_Parallel_Deactivate(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("1 executable callback 1 deactivated", func(t *testing.T) {
		var counter int64
		callbacks := NewCallbackGroup("id", logger, 2)
		ctrl := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&counter, 1)
			return true
		})
		require.Nil(t, ctrl.Deactivate(ctx))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&counter))
	})

	t.Run("deactivate is waiting till end of execution", func(t *testing.T) {
		var counter int64
		chFinished := make(chan struct{}, 1)

		synctest.Test(t, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})

			callbacks := NewCallbackGroup("id", logger, 2)
			ctrl := callbacks.Register("c", func(shouldAbort ShouldAbortCallback) bool {
				started <- struct{}{}
				<-release
				atomic.AddInt64(&counter, 1)
				return true
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-started
			// Callback is mid-execution. Deactivate blocks on the done
			// channel until the callback finishes. Releasing it in a
			// goroutine lets both the callback and Deactivate complete.
			// When Deactivate returns, counter==1 proves it waited for the
			// callback to run to completion (its last act before returning).
			go func() { close(release) }()
			require.Nil(t, ctrl.Deactivate(ctx))
			<-chFinished

			assert.Equal(t, int64(1), atomic.LoadInt64(&counter))
		})
	})

	t.Run("deactivate fails due to context timeout", func(t *testing.T) {
		var counter int64
		chFinished := make(chan struct{}, 1)

		synctest.Test(t, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})

			callbacks := NewCallbackGroup("id", logger, 2)
			ctrl := callbacks.Register("c", func(shouldAbort ShouldAbortCallback) bool {
				started <- struct{}{}
				<-release
				atomic.AddInt64(&counter, 1)
				return true
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-started
			// Callback is mid-execution. The 5ms timeout expires before
			// release is closed, so Deactivate returns non-nil.
			ctxTimeout, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
			defer cancel()
			require.NotNil(t, ctrl.Deactivate(ctxTimeout))
			close(release)
			<-chFinished

			assert.True(t, ctrl.IsActive())
			chFinished2 := make(chan struct{}, 1)
			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished2 <- struct{}{}
			}()
			<-chFinished2

			assert.Equal(t, int64(2), atomic.LoadInt64(&counter))
		})
	})

	t.Run("deactivate while running cooperative abort", func(t *testing.T) {
		var counter int64
		max := int64(25)
		chFinished := make(chan struct{}, 1)

		synctest.Test(t, func(t *testing.T) {
			started := make(chan struct{}, 1)

			callbacks := NewCallbackGroup("id", logger, 2)
			ctrl := callbacks.Register("c", func(shouldAbort ShouldAbortCallback) bool {
				started <- struct{}{}
				for {
					if shouldAbort() {
						return false
					}
					time.Sleep(10 * time.Millisecond)
					atomic.AddInt64(&counter, 1)
					if atomic.LoadInt64(&counter) > max {
						return true
					}
				}
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-started
			// Callback is running and looping with 10ms fake sleeps.
			// Deactivate sets abort=true; callback sees it on next check.
			require.NoError(t, ctrl.Deactivate(ctx))
			<-chFinished

			assert.LessOrEqual(t, atomic.LoadInt64(&counter), max)
		})
	})

	// t.Run cannot be nested inside a synctest bubble. This sub-test
	// verifies the same callback (registered fresh here) does not abort
	// after being activated, exercising the activate/run sequence
	// independently of the deactivate scenario above.
	t.Run("deactivate while running cooperative abort/does not abort after being reactivated", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var counter int64
			chFinished := make(chan struct{}, 1)

			callbacks := NewCallbackGroup("id", logger, 2)
			ctrl := callbacks.Register("c", func(shouldAbort ShouldAbortCallback) bool {
				for {
					if shouldAbort() {
						return false
					}
					time.Sleep(10 * time.Millisecond)
					atomic.AddInt64(&counter, 1)
					if atomic.LoadInt64(&counter) > 10 {
						return true
					}
				}
			})
			// Deactivate immediately so we can test re-activation.
			require.NoError(t, ctrl.Deactivate(ctx))
			require.NoError(t, ctrl.Activate())

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-chFinished

			assert.Greater(t, atomic.LoadInt64(&counter), int64(10))
		})
	})

	t.Run("deactivate 3rd and 4th while 1st and 2nd are executing", func(t *testing.T) {
		var c1, c2, c3, c4 int64

		synctest.Test(t, func(t *testing.T) {
			// tickRunning is closed by c1 when it begins, confirming drainDue
			// has already snapshotted the due set for this tick. c3 and c4 are
			// registered only after that signal, so they are absent from the
			// current tick's due slice. Deactivate sets active=false immediately
			// (they are not running), preventing any future tick from running them.
			tickRunning := make(chan struct{})
			release := make(chan struct{})
			chFinished := make(chan struct{}, 1)

			callbacks := NewCallbackGroup("id", logger, 2)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				close(tickRunning)
				<-release
				atomic.AddInt64(&c1, 1)
				return true
			})
			callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				<-release
				atomic.AddInt64(&c2, 1)
				return true
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-tickRunning

			// c1 is confirmed executing; drainDue already returned [c1, c2].
			// c3 and c4 registered now are not in this tick's dispatch set.
			ctrl3 := callbacks.Register("c3", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c3, 1)
				return true
			})
			ctrl4 := callbacks.Register("c4", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c4, 1)
				return true
			})
			require.Nil(t, ctrl3.Deactivate(ctx))
			require.Nil(t, ctrl4.Deactivate(ctx))

			close(release)
			<-chFinished

			assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c3))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c4))
		})
	})
}

func TestCycleCallback_Sequential(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("no callbacks", func(t *testing.T) {
		callbacks := NewCallbackGroup("id", logger, 1)
		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
	})

	t.Run("2 executable callbacks", func(t *testing.T) {
		var c1, c2 int64

		synctest.Test(t, func(t *testing.T) {
			callbacks := NewCallbackGroup("id", logger, 1)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				time.Sleep(50 * time.Millisecond)
				atomic.AddInt64(&c1, 1)
				return true
			})
			callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				time.Sleep(25 * time.Millisecond)
				atomic.AddInt64(&c2, 1)
				return true
			})

			start := time.Now()
			executed := callbacks.CycleCallback(shouldNotAbort)
			d := time.Since(start)

			assert.True(t, executed)
			assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
			assert.GreaterOrEqual(t, d, 75*time.Millisecond)
		})
	})

	t.Run("2 executable callbacks not executed due to abort", func(t *testing.T) {
		var c1, c2 int64
		abortCtr := 0
		shouldAbort := func() bool {
			abortCtr++
			return abortCtr > 1
		}
		callbacks := NewCallbackGroup("id", logger, 1)
		callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})

		executed := callbacks.CycleCallback(shouldAbort)

		assert.True(t, executed)
		assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c2))
	})

	// drainDue snapshots the due set at the start of a tick, so a callback
	// registered while an earlier one is running is deferred to the next tick.
	// This matches the parallel path; both dispatch paths share one contract.
	t.Run("register new while executing runs it on next tick", func(t *testing.T) {
		var c1, c2 int64
		chFinished := make(chan struct{}, 1)

		synctest.Test(t, func(t *testing.T) {
			// tickRunning is closed by c1 when it starts executing; release is
			// closed by the test to let c1 proceed. This rendezvous is a true
			// happens-before guarantee that Register("c2") is called while the
			// tick is in-progress, after drainDue snapshotted the due set.
			tickRunning := make(chan struct{})
			release := make(chan struct{})

			callbacks := NewCallbackGroup("id", logger, 1)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				close(tickRunning)
				<-release
				atomic.AddInt64(&c1, 1)
				return true
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-tickRunning
			callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c2, 1)
				return true
			})
			close(release)
			<-chFinished

			assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c2))

			assert.True(t, callbacks.CycleCallback(shouldNotAbort))
			assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
		})
	})
}

func TestCycleCallback_Sequential_Unregister(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("2 all unregistered 0 executed", func(t *testing.T) {
		var c1, c2 int64
		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		ctrl2 := callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		require.Nil(t, ctrl1.Unregister(ctx))
		require.Nil(t, ctrl2.Unregister(ctx))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c2))
	})

	t.Run("1 of 2 unregistered 1 executed", func(t *testing.T) {
		var c1, c2 int64
		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		require.Nil(t, ctrl1.Unregister(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
	})

	t.Run("4 unregistered at different ticks", func(t *testing.T) {
		var c1, c2, c3, c4 int64
		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		ctrl2 := callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		ctrl3 := callbacks.Register("c3", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c3, 1)
			return true
		})
		ctrl4 := callbacks.Register("c4", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c4, 1)
			return true
		})
		require.Nil(t, ctrl3.Unregister(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c3))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c4))

		require.Nil(t, ctrl1.Unregister(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(2), atomic.LoadInt64(&c2))
		assert.Equal(t, int64(2), atomic.LoadInt64(&c4))

		require.Nil(t, ctrl4.Unregister(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(3), atomic.LoadInt64(&c2))

		require.Nil(t, ctrl2.Unregister(ctx))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
	})

	t.Run("unregister 2nd and 3rd while 1st is executing", func(t *testing.T) {
		var c1, c2, c3 int64
		chFinished := make(chan struct{}, 1)

		synctest.Test(t, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})

			callbacks := NewCallbackGroup("id", logger, 1)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				started <- struct{}{}
				<-release
				atomic.AddInt64(&c1, 1)
				return true
			})
			ctrl2 := callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c2, 1)
				return true
			})
			ctrl3 := callbacks.Register("c3", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c3, 1)
				return true
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-started
			// c1 is running (sequential mode); c2 and c3 are not yet
			// dispatched. Unregister sees running=false for both and
			// deletes them immediately under the lock.
			require.Nil(t, ctrl2.Unregister(ctx))
			require.Nil(t, ctrl3.Unregister(ctx))
			close(release)
			<-chFinished

			assert.True(t, callbacks.CycleCallback(shouldNotAbort))
			assert.Equal(t, int64(2), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c2))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c3))
		})
	})
}

func TestCycleCallback_Sequential_Deactivate(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("2 all deactivated 0 executed", func(t *testing.T) {
		var c1, c2 int64
		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		ctrl2 := callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		require.Nil(t, ctrl1.Deactivate(ctx))
		require.Nil(t, ctrl2.Deactivate(ctx))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c2))
	})

	t.Run("1 of 2 deactivated 1 executed", func(t *testing.T) {
		var c1, c2 int64
		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		require.Nil(t, ctrl1.Deactivate(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
	})

	t.Run("4 deactivated at different ticks", func(t *testing.T) {
		var c1, c2, c3, c4 int64
		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl1 := callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c1, 1)
			return true
		})
		ctrl2 := callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c2, 1)
			return true
		})
		ctrl3 := callbacks.Register("c3", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c3, 1)
			return true
		})
		ctrl4 := callbacks.Register("c4", func(shouldAbort ShouldAbortCallback) bool {
			time.Sleep(25 * time.Millisecond)
			atomic.AddInt64(&c4, 1)
			return true
		})
		require.Nil(t, ctrl3.Deactivate(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c2))
		assert.Equal(t, int64(0), atomic.LoadInt64(&c3))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c4))

		require.Nil(t, ctrl1.Deactivate(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
		assert.Equal(t, int64(2), atomic.LoadInt64(&c2))
		assert.Equal(t, int64(2), atomic.LoadInt64(&c4))

		require.Nil(t, ctrl4.Deactivate(ctx))

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(3), atomic.LoadInt64(&c2))

		require.Nil(t, ctrl2.Deactivate(ctx))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
	})

	t.Run("deactivate 2nd and 3rd while 1st is executing", func(t *testing.T) {
		var c1, c2, c3 int64
		chFinished := make(chan struct{}, 1)

		synctest.Test(t, func(t *testing.T) {
			started := make(chan struct{}, 1)
			release := make(chan struct{})

			callbacks := NewCallbackGroup("id", logger, 1)
			callbacks.Register("c1", func(shouldAbort ShouldAbortCallback) bool {
				started <- struct{}{}
				<-release
				atomic.AddInt64(&c1, 1)
				return true
			})
			ctrl2 := callbacks.Register("c2", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c2, 1)
				return true
			})
			ctrl3 := callbacks.Register("c3", func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&c3, 1)
				return true
			})

			go func() {
				callbacks.CycleCallback(shouldNotAbort)
				chFinished <- struct{}{}
			}()
			<-started
			// c1 is running (sequential mode); c2 and c3 are queued but
			// not dispatched. Deactivate sees running=false for both and
			// commits active=false immediately under the lock.
			require.Nil(t, ctrl2.Deactivate(ctx))
			require.Nil(t, ctrl3.Deactivate(ctx))
			close(release)
			<-chFinished

			assert.Equal(t, int64(1), atomic.LoadInt64(&c1))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c2))
			assert.Equal(t, int64(0), atomic.LoadInt64(&c3))
		})
	})
}

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

				assert.True(t, callbacks.CycleCallback(shouldNotAbort))
				assert.Equal(t, int64(1), atomic.LoadInt64(&alwaysCounter))
				assert.Equal(t, int64(1), atomic.LoadInt64(&longCounter))

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

				// second tick: entry was rescheduled, runs again
				assert.True(t, callbacks.CycleCallback(shouldNotAbort))
				assert.Equal(t, int64(2), atomic.LoadInt64(&counter))
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

				callbacks.CycleCallback(shouldNotAbort)
				assert.Equal(t, int64(siblings), atomic.LoadInt64(&okCounter))
				assert.Equal(t, int64(1), atomic.LoadInt64(&panicCounter))

				callbacks.CycleCallback(shouldNotAbort)
				assert.Equal(t, int64(2), atomic.LoadInt64(&panicCounter))
			})

			t.Run("panic does not advance intervals", func(t *testing.T) {
				// Use a two-step series so Advance() is observable via Get().
				intervals := NewSeriesIntervals([]time.Duration{
					10 * time.Millisecond, 20 * time.Millisecond,
				})
				intervalBefore := intervals.Get()

				callbacks := NewCallbackGroup("id", logger, mode.routines)
				callbacks.Register("panic", func(shouldAbort ShouldAbortCallback) bool {
					panic("boom")
				}, WithIntervals(intervals))

				callbacks.CycleCallback(shouldNotAbort)

				// Advance must NOT have been called: Get() returns the same value.
				assert.Equal(t, intervalBefore, intervals.Get())
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

				assert.False(t, callbacks.CycleCallback(func() bool { return true }))
				require.NoError(t, ctrls[0].Deactivate(context.Background()))

				assert.True(t, callbacks.CycleCallback(shouldNotAbort))
				assert.Equal(t, int64(0), atomic.LoadInt64(&counters[0]))
				for i := 1; i < total; i++ {
					assert.Equal(t, int64(1), atomic.LoadInt64(&counters[i]))
				}
			})
		})
	}
}

func TestCycleCallback_DueDispatch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	t.Run("only due callbacks execute among many not-due", func(t *testing.T) {
		const total = 200
		var executed int64

		callbacks := NewCallbackGroup("id", logger, 4)
		for i := 0; i < total; i++ {
			callbacks.Register(fmt.Sprintf("c%d", i),
				func(shouldAbort ShouldAbortCallback) bool {
					atomic.AddInt64(&executed, 1)
					return true
				}, WithIntervals(NewFixedIntervals(time.Hour)))
		}

		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(total), atomic.LoadInt64(&executed))

		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(total), atomic.LoadInt64(&executed))
	})

	t.Run("WithIntervals callback runs immediately on the first tick", func(t *testing.T) {
		var counter int64

		callbacks := NewCallbackGroup("id", logger, 4)
		callbacks.Register("c1",
			func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&counter, 1)
				return true
			}, WithIntervals(NewFixedIntervals(time.Hour)))

		// Despite the hour-long interval, WithIntervals back-dates the start time
		// so the first run is due immediately.
		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&counter))

		// The next tick is well within the interval, so it must not run again.
		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&counter))
	})

	t.Run("AsInactive callback is suppressed until activated", func(t *testing.T) {
		var counter int64

		callbacks := NewCallbackGroup("id", logger, 4)
		ctrl := callbacks.Register("c1",
			func(shouldAbort ShouldAbortCallback) bool {
				atomic.AddInt64(&counter, 1)
				return true
			}, AsInactive())

		// Registered inactive: a tick finds nothing due to run.
		assert.False(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(0), atomic.LoadInt64(&counter))

		// Once activated, the callback (nil intervals, always due) runs.
		ctrl.Activate()
		assert.True(t, callbacks.CycleCallback(shouldNotAbort))
		assert.Equal(t, int64(1), atomic.LoadInt64(&counter))
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

		callbacks.CycleCallback(shouldNotAbort)
	})
}

// ----------------------------------------------------------------------------
// White-box tests
// ----------------------------------------------------------------------------

// liveEntryCount counts dueHeap entries for callbackId whose schedGen
// matches the current meta's schedGen. Caller holds g's lock.
func liveEntryCount(g *cycleCallbackGroup, callbackId uint32) int {
	meta, ok := g.metas[callbackId]
	if !ok {
		return 0
	}
	count := 0
	for _, e := range g.heap {
		if e.callbackId == callbackId && e.schedGen == meta.schedGen {
			count++
		}
	}
	return count
}

func TestCycleCallback_SingleLiveEntry_AfterRegister(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)

	g.Register("c", func(shouldAbort ShouldAbortCallback) bool { return true })

	g.Lock()
	defer g.Unlock()

	require.Len(t, g.metas, 1)
	var callbackId uint32
	for id := range g.metas {
		callbackId = id
	}
	assert.Equal(t, 1, liveEntryCount(g, callbackId))
}

func TestCycleCallback_SingleLiveEntry_DeactivateThenActivate(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)

	ctrl := g.Register("c", func(shouldAbort ShouldAbortCallback) bool { return true })
	require.NoError(t, ctrl.Deactivate(context.Background()))
	require.NoError(t, ctrl.Activate())

	g.Lock()
	defer g.Unlock()

	var callbackId uint32
	for id := range g.metas {
		callbackId = id
	}
	assert.Equal(t, 1, liveEntryCount(g, callbackId))
}

func TestCycleCallback_SingleLiveEntry_ActivateDuringDrain(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 2).(*cycleCallbackGroup)

	started := make(chan struct{})
	release := make(chan struct{})

	ctrl := g.Register("blocking", func(shouldAbort ShouldAbortCallback) bool {
		close(started)
		<-release
		return true
	})

	done := make(chan bool, 1)
	go func() {
		done <- g.CycleCallback(func() bool { return false })
	}()
	<-started

	// Activate while the entry is in-flight (drained from heap, executing).
	require.NoError(t, ctrl.Activate())
	close(release)
	<-done

	// After the tick completes (teardown reschedules), exactly one live entry.
	g.Lock()
	defer g.Unlock()

	var callbackId uint32
	for id := range g.metas {
		callbackId = id
	}
	assert.Equal(t, 1, liveEntryCount(g, callbackId))
}

func TestCycleCallback_AbortRestoresEntries(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 4).(*cycleCallbackGroup)

	const total = 5
	for i := 0; i < total; i++ {
		g.Register(fmt.Sprintf("c%d", i), func(shouldAbort ShouldAbortCallback) bool {
			return true
		})
	}

	// Abort the entire tick; every drained entry must be restored.
	g.CycleCallback(func() bool { return true })

	g.Lock()
	defer g.Unlock()

	assert.Len(t, g.metas, total)
	liveTotal := 0
	for callbackId := range g.metas {
		liveTotal += liveEntryCount(g, callbackId)
	}
	assert.Equal(t, total, liveTotal)
}

func TestCycleCallback_SequentialAbortRestoresRemainder(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)

	const total = 4
	var counter int64
	for i := 0; i < total; i++ {
		g.Register(fmt.Sprintf("c%d", i), func(shouldAbort ShouldAbortCallback) bool {
			atomic.AddInt64(&counter, 1)
			return true
		})
	}

	// Abort once the first callback has run.
	g.CycleCallback(func() bool { return atomic.LoadInt64(&counter) >= 1 })
	assert.Equal(t, int64(1), atomic.LoadInt64(&counter))

	g.Lock()
	defer g.Unlock()

	// Every registered callback must have exactly one live entry.
	for callbackId := range g.metas {
		assert.Equal(t, 1, liveEntryCount(g, callbackId),
			"callbackId %d has wrong live-entry count", callbackId)
	}
}

func TestCycleCallback_StaleEntryBoundedGrowth(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)

	// Far-future interval so nothing ever drains: without compaction each cycle
	// would orphan a stale entry and the heap would grow linearly with N.
	const N = 1000
	ctrl := g.Register("c", func(shouldAbort ShouldAbortCallback) bool { return true },
		WithIntervals(NewFixedIntervals(time.Hour)))

	for i := 0; i < N; i++ {
		require.NoError(t, ctrl.Deactivate(context.Background()))
		require.NoError(t, ctrl.Activate())
	}

	g.Lock()
	defer g.Unlock()

	var callbackId uint32
	for id := range g.metas {
		callbackId = id
	}
	meta := g.metas[callbackId]

	liveEntries := 0
	for _, e := range g.heap {
		if e.callbackId == callbackId && e.schedGen == meta.schedGen {
			liveEntries++
		}
	}
	assert.Equal(t, 1, liveEntries, "exactly one live entry")
	// Compaction keeps the heap bounded by the registered-callback count, no matter
	// how many Deactivate/Activate cycles churn through it.
	assert.LessOrEqual(t, len(g.heap), 2*len(g.metas)+8,
		"heap stays bounded regardless of churn")
}

// TestCycleCallback_CompactHeapKeepsInvariant churns several callbacks with
// distinct due times to trigger compaction, then verifies the rebuilt heap is
// still a valid min-heap holding exactly the one current entry per callback.
func TestCycleCallback_CompactHeapKeepsInvariant(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)

	const callbacks = 6
	ctrls := make([]CycleCallbackCtrl, callbacks)
	for i := 0; i < callbacks; i++ {
		// Distinct intervals give distinct due times, so compaction's re-heapify
		// has a real multi-level heap to rebuild.
		ctrls[i] = g.Register(fmt.Sprintf("c%d", i),
			func(shouldAbort ShouldAbortCallback) bool { return true },
			WithIntervals(NewFixedIntervals(time.Duration(i+1)*time.Hour)))
	}

	// Churn every callback well past the compaction threshold.
	for round := 0; round < 200; round++ {
		ctrl := ctrls[round%callbacks]
		require.NoError(t, ctrl.Deactivate(context.Background()))
		require.NoError(t, ctrl.Activate())
	}

	g.Lock()
	defer g.Unlock()

	// Compaction bounds the total entry count (some bounded stale entries may
	// remain between compactions; only the count is capped, not stale presence).
	assert.LessOrEqual(t, len(g.heap), 2*len(g.metas)+8, "heap bounded after churn")

	// Every callback still has exactly one live (current-schedGen) entry, so it
	// will still fire; stragglers, if any, are stale duplicates.
	live := map[uint32]int{}
	for _, e := range g.heap {
		meta, ok := g.metas[e.callbackId]
		require.True(t, ok, "no entry may reference an unregistered callback")
		if e.schedGen == meta.schedGen {
			live[e.callbackId]++
		}
	}
	for id, n := range live {
		assert.Equal(t, 1, n, "callback %d has exactly one live entry", id)
	}
	assert.Len(t, live, callbacks, "every callback still scheduled")

	// Min-heap invariant: each parent is due no later than its children — this is
	// what compactHeap's Floyd re-heapify must restore.
	for i := range g.heap {
		if l := 2*i + 1; l < len(g.heap) {
			assert.LessOrEqual(t, g.heap[i].due, g.heap[l].due, "heap property at %d/left", i)
		}
		if r := 2*i + 2; r < len(g.heap) {
			assert.LessOrEqual(t, g.heap[i].due, g.heap[r].due, "heap property at %d/right", i)
		}
	}
}

// TestCycleCallback_RegisterDuringCompaction guards the interaction between
// Register and heap compaction: when schedule (called during Register) triggers
// compaction, it must recognize the callback being registered and keep its
// freshly-pushed entry rather than dropping it as stale.
func TestCycleCallback_RegisterDuringCompaction(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)

	// Bloat the heap so the next Register actually compacts. Churn can't: every
	// schedule self-compacts, and registering a callback raises the threshold faster
	// than it adds entries. Unregister is the lever — it leaves entries behind and
	// never compacts. Register a batch, then unregister most of it.
	const fillers = 20
	ctrls := make([]CycleCallbackCtrl, fillers)
	for i := 0; i < fillers; i++ {
		ctrls[i] = g.Register(fmt.Sprintf("filler%d", i),
			func(shouldAbort ShouldAbortCallback) bool { return true },
			WithIntervals(NewFixedIntervals(time.Hour)))
	}
	for i := 1; i < fillers; i++ { // keep filler0 so metas stays > 0
		require.NoError(t, ctrls[i].Unregister(context.Background()))
	}

	g.Lock()
	heapBefore := len(g.heap)
	metasBefore := len(g.metas)
	g.Unlock()
	// Registering "fresh" raises metas to metasBefore+1; the heap must exceed that
	// post-register threshold so schedule actually compacts mid-registration.
	require.Greater(t, heapBefore, 2*(metasBefore+1)+8,
		"setup must leave the heap over the post-register compaction threshold")

	g.Register("fresh", func(shouldAbort ShouldAbortCallback) bool { return true })

	g.Lock()
	defer g.Unlock()

	// Compaction actually ran during Register: the stale filler entries were reclaimed.
	require.Less(t, len(g.heap), heapBefore, "compaction should have run during Register")

	var freshID uint32
	found := false
	for id, m := range g.metas {
		if m.name == "fresh" {
			freshID, found = id, true
		}
	}
	require.True(t, found, "fresh callback is registered")

	live := 0
	for _, e := range g.heap {
		if e.callbackId == freshID && e.schedGen == g.metas[freshID].schedGen {
			live++
		}
	}
	assert.Equal(t, 1, live, "fresh callback must stay scheduled through compaction")
}

func TestCycleCallback_DeactivatePriorityClaimedButNotStarted(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)

	var bodyEntered int64
	g.Register("c", func(shouldAbort ShouldAbortCallback) bool {
		atomic.AddInt64(&bodyEntered, 1)
		return true
	})

	// Manually drain the meta from the heap (simulates what drainDue does).
	g.Lock()
	var drained *cycleCallbackMeta
	for len(g.heap) > 0 {
		e := g.heap.pop()
		meta, ok := g.metas[e.callbackId]
		if ok && e.schedGen == meta.schedGen && meta.active {
			drained = meta
			break
		}
	}
	g.Unlock()

	require.NotNil(t, drained)

	// Deactivate before runOne takes the lock: sets active=false.
	g.Lock()
	drained.active = false
	g.Unlock()

	// runOne should skip execution because active=false.
	executed := g.runOne(drained, func() bool { return false })

	assert.False(t, executed)
	assert.Equal(t, int64(0), atomic.LoadInt64(&bodyEntered))
}

func TestCycleCallback_UnregisterNilOnAbsentID(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)

	// unregister is called via a ctrl that holds the ID, but we can test via a
	// freshly created ctrl pointing to a non-existent callbackId.
	ctrl := &cycleCallbackCtrl{
		callbackId:       99,
		callbackCustomId: "ghost",
		unregister:       g.unregister,
	}
	assert.Nil(t, ctrl.Unregister(context.Background()))
}

// TestCycleCallback_ControlErrorBranches covers the early-return error paths of
// the control operations: acting on an absent callback id, and acting with an
// already-cancelled context.
func TestCycleCallback_ControlErrorBranches(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("absent id", func(t *testing.T) {
		g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)
		ghost := &cycleCallbackCtrl{
			callbackId:       99,
			callbackCustomId: "ghost",
			isActive:         g.isActive,
			activate:         g.activate,
			deactivate:       g.deactivate,
			unregister:       g.unregister,
		}

		assert.ErrorIs(t, ghost.Activate(), ErrorCallbackNotFound)
		assert.ErrorIs(t, ghost.Deactivate(context.Background()), ErrorCallbackNotFound)
	})

	t.Run("pre-cancelled context", func(t *testing.T) {
		g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)
		ctrl := g.Register("c", func(shouldAbort ShouldAbortCallback) bool { return true })

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assert.ErrorIs(t, ctrl.Deactivate(ctx), context.Canceled)
		assert.ErrorIs(t, ctrl.Unregister(ctx), context.Canceled)
	})
}

func TestCycleCallback_ConcurrentRegisterUnregisterLeakCheck(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("id", logger, 4).(*cycleCallbackGroup)
	callback := func(shouldAbort ShouldAbortCallback) bool { return true }
	shouldNotAbort := func() bool { return false }

	const stable = 8
	for i := 0; i < stable; i++ {
		g.Register(fmt.Sprintf("stable%d", i), callback)
	}

	stop := make(chan struct{})
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctrl := g.Register("churn", callback)
			assert.NoError(t, ctrl.Unregister(context.Background()))
		}
	}()

	for i := 0; i < 200; i++ {
		g.CycleCallback(shouldNotAbort)
	}
	close(stop)
	wg.Wait()

	// one final tick to flush any in-flight churn
	g.CycleCallback(shouldNotAbort)

	g.Lock()
	defer g.Unlock()
	assert.Len(t, g.metas, stable)
}

// ----------------------------------------------------------------------------
// Stress / race test
// ----------------------------------------------------------------------------

func TestCycleCallback_StressRace(t *testing.T) {
	logger, _ := test.NewNullLogger()
	callbacks := NewCallbackGroup("id", logger, 4)

	const stable = 4
	ctrls := make([]CycleCallbackCtrl, stable)
	for i := 0; i < stable; i++ {
		// Interval-bearing callbacks so the Activate path drives computeNextDue's
		// interval Get() concurrently with the running callback's Reset/Advance,
		// exercising the interval state under -race.
		ctrls[i] = callbacks.Register(fmt.Sprintf("stable%d", i),
			func(shouldAbort ShouldAbortCallback) bool { return true },
			WithIntervals(NewExpIntervals(time.Millisecond, 10*time.Millisecond, 2, 4)))
	}

	deadline := time.Now().Add(2 * time.Second)
	var wg sync.WaitGroup

	// tight CycleCallback loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			callbacks.CycleCallback(func() bool { return false })
		}
	}()

	// 8 goroutines hammering Activate/Deactivate/Register/Unregister
	for i := 0; i < 8; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for time.Now().Before(deadline) {
				switch i % 4 {
				case 0:
					ctrls[i%stable].Deactivate(ctx) //nolint:errcheck
				case 1:
					ctrls[i%stable].Activate() //nolint:errcheck
				case 2:
					ctrl := callbacks.Register(fmt.Sprintf("churn%d", i),
						func(shouldAbort ShouldAbortCallback) bool { return true })
					ctrl.Unregister(ctx) //nolint:errcheck
				case 3:
					ctrls[i%stable].Deactivate(ctx) //nolint:errcheck
					ctrls[i%stable].Activate()      //nolint:errcheck
				}
			}
		}()
	}

	wg.Wait()
}

// TestCycleCallback_StopWaitsForInFlightRun asserts the Deactivate/Unregister
// contract: the call must not return while a callback body is executing. Because
// teardown reschedules a still-active callback, a run that finishes mid-wait can
// start a fresh run before the stopper commits — the stopper must wait for that
// one too.
//
// The body sleeps briefly so an overlapping run is observable via inFlight, and
// the attempt is repeated because hitting the reschedule-then-rerun window
// depends on scheduling.
func TestCycleCallback_StopWaitsForInFlightRun(t *testing.T) {
	logger, _ := test.NewNullLogger()

	modes := []struct {
		name string
		stop func(ctrl CycleCallbackCtrl) error
	}{
		{"deactivate", func(ctrl CycleCallbackCtrl) error { return ctrl.Deactivate(context.Background()) }},
		{"unregister", func(ctrl CycleCallbackCtrl) error { return ctrl.Unregister(context.Background()) }},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			for attempt := 0; attempt < 50; attempt++ {
				g := NewCallbackGroup("id", logger, 4)

				var inFlight atomic.Int32
				firstRun := make(chan struct{})
				var once sync.Once
				ctrl := g.Register("c", func(shouldAbort ShouldAbortCallback) bool {
					inFlight.Add(1)
					defer inFlight.Add(-1)
					once.Do(func() { close(firstRun) })
					time.Sleep(10 * time.Microsecond)
					return true
				})

				stopTicking := make(chan struct{})
				ticking := runTicks(g, stopTicking)

				<-firstRun
				require.NoError(t, mode.stop(ctrl))
				require.Zero(t, inFlight.Load(),
					"%s returned while a callback body was in flight (attempt %d)", mode.name, attempt)

				close(stopTicking)
				<-ticking
			}
		})
	}
}

// runTicks drives CycleCallback in a loop until stop is closed, returning a channel
// that closes once the loop has exited.
func runTicks(g CycleCallbackGroup, stop <-chan struct{}) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				g.CycleCallback(func() bool { return false })
			}
		}
	}()
	return done
}

// TestCycleCallback_RunWithIntervals exercises computeNextDue and interval Reset/Advance
// over many real ticks driven by a live NewManager, for both parallel and
// sequential routinesLimit values.
func TestCycleCallback_RunWithIntervals(t *testing.T) {
	modes := []struct {
		name     string
		routines int
	}{
		{"parallel", 2},
		{"sequential", 1},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			var times1, times2, times3 []time.Duration

			synctest.Test(t, func(t *testing.T) {
				ticker := NewFixedTicker(10 * time.Millisecond)
				intervals2 := NewSeriesIntervals([]time.Duration{
					10 * time.Millisecond, 30 * time.Millisecond, 50 * time.Millisecond,
				})
				intervals3 := NewFixedIntervals(60 * time.Millisecond)
				now := time.Now()

				var mu sync.Mutex
				executionTimes1 := []time.Duration{}
				callback1 := func(shouldAbort ShouldAbortCallback) bool {
					mu.Lock()
					executionTimes1 = append(executionTimes1, time.Since(now))
					mu.Unlock()
					return true
				}
				executionCounter2 := 0
				executionTimes2 := []time.Duration{}
				callback2 := func(shouldAbort ShouldAbortCallback) bool {
					mu.Lock()
					executionCounter2++
					executionTimes2 = append(executionTimes2, time.Since(now))
					executed := executionCounter2%4 == 0
					mu.Unlock()
					return executed
				}
				executionTimes3 := []time.Duration{}
				callback3 := func(shouldAbort ShouldAbortCallback) bool {
					mu.Lock()
					executionTimes3 = append(executionTimes3, time.Since(now))
					mu.Unlock()
					return true
				}

				callbacks := NewCallbackGroup("id", logrus.New(), mode.routines)
				// called on every tick (nil interval = always due)
				callbacks.Register("c1", callback1)
				// called with 10, 30, 50, 50, 10, 30, 50, 50, ... intervals
				callbacks.Register("c2", callback2, WithIntervals(intervals2))
				// called with 60 ms fixed intervals
				callbacks.Register("c3", callback3, WithIntervals(intervals3))

				logger, _ := test.NewNullLogger()
				cm := NewManager("test", ticker, callbacks.CycleCallback, logger)
				cm.Start()
				// Under the fake clock, 400ms of simulated time is instant.
				// synctest.Wait() after the sleep ensures the manager has
				// processed all ticks before we call StopAndWait.
				time.Sleep(400 * time.Millisecond)
				synctest.Wait()
				cm.StopAndWait(context.Background()) //nolint:errcheck

				mu.Lock()
				times1 = make([]time.Duration, len(executionTimes1))
				copy(times1, executionTimes1)
				times2 = make([]time.Duration, len(executionTimes2))
				copy(times2, executionTimes2)
				times3 = make([]time.Duration, len(executionTimes3))
				copy(times3, executionTimes3)
				mu.Unlock()
			})

			// within 400 ms c1 should be called at least 30x
			require.GreaterOrEqual(t, len(times1), 30)
			sumDuration := time.Duration(10)
			for i := 0; i < 30; i++ {
				assert.GreaterOrEqual(t, times1[i], sumDuration)
				sumDuration += 10 * time.Millisecond
			}

			// within 400 ms c2 should be called at least 8x
			require.GreaterOrEqual(t, len(times2), 8)
			sumDuration = time.Duration(0)
			for i := 0; i < 8; i++ {
				assert.GreaterOrEqual(t, times2[i], sumDuration)
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
			require.GreaterOrEqual(t, len(times3), 6)
			sumDuration = time.Duration(0)
			for i := 0; i < 6; i++ {
				assert.GreaterOrEqual(t, times3[i], sumDuration)
				sumDuration += 60 * time.Millisecond
			}
		})
	}
}

// TestCycleCallback_IdleTickPrunesUnregistered asserts that after unregistering callbacks
// and running a tick, no live due-heap entries remain for those callbacks and
// g.metas contains only the still-registered ones.
func TestCycleCallback_IdleTickPrunesUnregistered(t *testing.T) {
	logger, _ := test.NewNullLogger()
	callback := func(shouldAbort ShouldAbortCallback) bool { return true }

	g := NewCallbackGroup("id", logger, 2).(*cycleCallbackGroup)
	ctrl1 := g.Register("c1", callback)
	ctrl2 := g.Register("c2", callback)
	g.Register("c3", callback)

	require.NoError(t, ctrl1.Unregister(context.Background()))
	require.NoError(t, ctrl2.Unregister(context.Background()))

	// A tick drains and discards entries for unregistered callbacks on pop.
	g.CycleCallback(func() bool { return false })

	g.Lock()
	defer g.Unlock()

	// Only c3 remains in metas.
	assert.Len(t, g.metas, 1)

	// No live due-heap entries for the unregistered callbacks (id 0 and 1).
	for _, e := range g.heap {
		_, registered := g.metas[e.callbackId]
		assert.True(t, registered, "heap entry for unregistered callbackId %d must not survive a tick", e.callbackId)
	}

	// c3 was not unregistered, its live entry must be present.
	liveCount := 0
	for id := range g.metas {
		liveCount += liveEntryCount(g, id)
	}
	assert.Equal(t, 1, liveCount)
}

// TestCycleCallback_CombinedCtrl_DeactivateTimeoutRollback verifies that when a combined
// controller's Deactivate times out on one callback, the rollback (re-activate
// only the ones that succeeded) does not leave the timed-out callback stuck
// in an inactive state.
func TestCycleCallback_CombinedCtrl_DeactivateTimeoutRollback(t *testing.T) {
	logger, _ := test.NewNullLogger()
	shouldNotAbort := func() bool { return false }

	// blocking holds the first callback's run hostage until we release it.
	blocking := make(chan struct{})
	chStarted := make(chan struct{}, 1)
	chFinished := make(chan struct{}, 1)

	callbacks := NewCallbackGroup("id", logger, 2)

	ctrl1 := callbacks.Register("slow", func(shouldAbort ShouldAbortCallback) bool {
		chStarted <- struct{}{}
		<-blocking
		return true
	})
	ctrl2 := callbacks.Register("fast", func(shouldAbort ShouldAbortCallback) bool {
		return true
	})

	// Start a tick so the slow callback runs.
	go func() {
		callbacks.CycleCallback(shouldNotAbort)
		chFinished <- struct{}{}
	}()
	<-chStarted

	// Deactivate via combined ctrl with a very short context so it times out
	// on ctrl1 (still running) but ctrl2 has already finished.
	combined := NewCombinedCallbackCtrl(2, logger, ctrl1, ctrl2)
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := combined.Deactivate(ctxTimeout)
	require.Error(t, err)

	// ctrl1 timed out: it must remain ACTIVE (rollback only re-activates
	// those that succeeded, so it must NOT have deactivated ctrl1).
	assert.True(t, ctrl1.IsActive())

	// Release the slow callback and wait for the tick to finish.
	close(blocking)
	<-chFinished

	// After the tick finishes, ctrl1 is still active and runs again.
	assert.True(t, ctrl1.IsActive())
	assert.True(t, callbacks.CycleCallback(shouldNotAbort))
}

// TestCycleCallback_DeactivateTimeoutState is a white-box test that verifies the
// documented post-timeout state (active=true, abort=true) and confirms that a
// subsequent Deactivate with a valid ctx succeeds and commits active=false.
func TestCycleCallback_DeactivateTimeoutState(t *testing.T) {
	logger, _ := test.NewNullLogger()

	release := make(chan struct{})
	started := make(chan struct{}, 1)

	g := NewCallbackGroup("id", logger, 1).(*cycleCallbackGroup)
	ctrl := g.Register("slow", func(shouldAbort ShouldAbortCallback) bool {
		started <- struct{}{}
		<-release
		return true
	})

	tickDone := make(chan struct{})
	go func() {
		g.CycleCallback(func() bool { return false })
		close(tickDone)
	}()
	<-started

	// Deactivate with a context that expires while the callback is still blocked.
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := ctrl.Deactivate(ctxTimeout)
	require.Error(t, err, "Deactivate must return an error on ctx timeout")

	// Under the lock: the callback is still running; state must be active=true, abort=true.
	g.Lock()
	var meta *cycleCallbackMeta
	for _, m := range g.metas {
		meta = m
	}
	require.NotNil(t, meta)
	assert.True(t, meta.active, "active must remain true after Deactivate timeout")
	assert.True(t, meta.abort, "abort must remain true after Deactivate timeout")
	g.Unlock()

	// Release the callback so the tick can finish.
	close(release)
	<-tickDone

	// A second Deactivate with a generous ctx must succeed.
	err = ctrl.Deactivate(context.Background())
	require.NoError(t, err, "second Deactivate must succeed after the run completes")

	// active is now false; abort remains true (only Activate clears it).
	g.Lock()
	assert.False(t, meta.active, "active must be false after successful Deactivate")
	assert.True(t, meta.abort, "abort remains true after Deactivate; only Activate clears it")
	g.Unlock()
}

// TestCycleCallback_ConcurrentWaiters verifies that two concurrent callers waiting on the
// same in-flight callback both wake when the run finishes — exercising the
// shared lazy-channel broadcast path.
func TestCycleCallback_ConcurrentWaiters(t *testing.T) {
	logger, _ := test.NewNullLogger()

	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		started := make(chan struct{}, 1)

		callbacks := NewCallbackGroup("id", logger, 1)
		ctrl := callbacks.Register("slow", func(shouldAbort ShouldAbortCallback) bool {
			started <- struct{}{}
			<-release
			return true
		})

		tickDone := make(chan struct{})
		go func() {
			callbacks.CycleCallback(func() bool { return false })
			close(tickDone)
		}()
		<-started

		// Two concurrent Unregister callers on the same ctrl land while the
		// callback runs. They share the lazily-created done channel and must
		// both wake after the run completes (the second delete is a no-op in Go).
		var wg sync.WaitGroup
		wg.Add(2)

		unregisterErr := make(chan error, 1)
		go func() {
			defer wg.Done()
			unregisterErr <- ctrl.Unregister(context.Background())
		}()

		unregisterErr2 := make(chan error, 1)
		go func() {
			defer wg.Done()
			// Second concurrent Unregister on the same ctrl exercises the shared-waiter path.
			unregisterErr2 <- ctrl.Unregister(context.Background())
		}()

		// synctest.Wait ensures both Unregister goroutines are durably parked
		// as waiters on the lazily-created done channel before release is
		// closed, deterministically exercising the shared-waiter broadcast path.
		synctest.Wait()
		close(release)
		wg.Wait()
		<-tickDone

		assert.NoError(t, <-unregisterErr)
		assert.NoError(t, <-unregisterErr2)
	})
}

// BenchmarkCycleCallback measures the hot-path allocation profile with a single
// always-due no-op callback on a sequential (routinesLimit=1) group, so no
// worker-goroutine allocations contaminate the measurement.
func BenchmarkCycleCallback(b *testing.B) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("bench", logger, 1)
	g.Register("noop", func(shouldAbort ShouldAbortCallback) bool { return true })
	shouldNotAbort := func() bool { return false }

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.CycleCallback(shouldNotAbort)
	}
}

// TestCycleCallback_AllocRegression asserts that the per-CycleCallback allocation count
// stays within a tight bound. Because the done channel is now allocated lazily
// (only when a Deactivate/Unregister waiter needs it), the no-waiter hot path
// has no make(chan struct{}) at all. If someone (a) reintroduces a
// per-invocation abort closure OR (b) reverts to eager done-channel allocation,
// the count rises by at least 1 and the test fails.
func TestCycleCallback_AllocRegression(t *testing.T) {
	logger, _ := test.NewNullLogger()
	g := NewCallbackGroup("alloc", logger, 1)
	g.Register("noop", func(shouldAbort ShouldAbortCallback) bool { return true })
	shouldNotAbort := func() bool { return false }

	allocs := testing.AllocsPerRun(100, func() {
		g.CycleCallback(shouldNotAbort)
	})

	// Alloc breakdown per CycleCallback call (1 total):
	//   1. drainDue: append([]*cycleCallbackMeta, meta) — fresh result slice per tick
	// The concrete dueHeap pushes/pops dueEntry by value (no container/heap
	// any-boxing), the abort closure is built once in Register, and the done
	// channel is lazy — none of those allocate on the no-waiter hot path. A
	// reintroduced per-invocation abort closure, eager done-channel allocation,
	// or a return to container/heap's any-boxing would raise the count.
	const maxAllocs = 1
	assert.LessOrEqual(t, allocs, float64(maxAllocs),
		"CycleCallback allocs per run: got %.1f, want <= %d",
		allocs, maxAllocs)
}

// BenchmarkCycleCallback_Parallel reports allocs and drives multi-tenant and
// single-tenant large-collection cases to measure per-tick scheduling overhead.
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
					// MT backed-off entry: long interval; the prime call below
					// records started=now so it stays not-due for the rest of the run
					callbacks.Register(fmt.Sprintf("idle%d", i), noop,
						WithIntervals(NewFixedIntervals(time.Hour)))
				}
			}
			callbacks.CycleCallback(shouldNotAbort)

			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				callbacks.CycleCallback(shouldNotAbort)
			}
		})
	}
}
