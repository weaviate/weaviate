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
	"time"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
)

var logger, _ = test.NewNullLogger()

type cycleCallbackProvider struct {
	sync.Mutex

	firstCycleStarted chan struct{}
	cycleCallback     CycleCallback
	results           chan string
}

func newProvider(cycleDuration time.Duration, resultsSize uint) *cycleCallbackProvider {
	return newProviderAbortable(cycleDuration, resultsSize, 1)
}

func newProviderAbortable(cycleDuration time.Duration, resultsSize uint, aborts int) *cycleCallbackProvider {
	fs := false
	p := &cycleCallbackProvider{}
	p.results = make(chan string, resultsSize)
	p.firstCycleStarted = make(chan struct{}, 1)
	p.cycleCallback = func(shouldAbort ShouldAbortCallback) bool {
		p.Lock()
		if !fs {
			p.firstCycleStarted <- struct{}{}
			fs = true
		}
		p.Unlock()

		if aborts > 1 {
			for i := 0; i < aborts; i++ {
				time.Sleep(cycleDuration / time.Duration(aborts))
				if shouldAbort() {
					return true
				}
			}
		} else {
			time.Sleep(cycleDuration)
		}
		p.results <- "something wonderful..."
		return true
	}
	return p
}

func TestCycleManager_beforeTimeout(t *testing.T) {
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 1 * time.Millisecond
	stopTimeout := 12 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	var cm CycleManager

	t.Run("create new", func(t *testing.T) {
		cm = NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

		assert.False(t, cm.Running())
	})

	t.Run("start", func(t *testing.T) {
		cm.Start()
		<-p.firstCycleStarted

		assert.True(t, cm.Running())
	})

	t.Run("stop", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()

		stopResult := cm.Stop(timeoutCtx)

		select {
		case <-timeoutCtx.Done():
			t.Fatal(timeoutCtx.Err().Error(), "failed to stop")
		case stopped := <-stopResult:
			assert.True(t, stopped)
			assert.False(t, cm.Running())
			assert.Equal(t, "something wonderful...", <-p.results)
		}
	})
}

func TestCycleManager_beforeTimeoutWithWait(t *testing.T) {
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 1 * time.Millisecond
	stopTimeout := 12 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	var cm CycleManager

	t.Run("create new", func(t *testing.T) {
		cm = NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

		assert.False(t, cm.Running())
	})

	t.Run("start", func(t *testing.T) {
		cm.Start()
		<-p.firstCycleStarted

		assert.True(t, cm.Running())
	})

	t.Run("stop", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()

		err := cm.StopAndWait(timeoutCtx)

		assert.Nil(t, err)
		assert.False(t, cm.Running())
		assert.Equal(t, "something wonderful...", <-p.results)
	})
}

func TestCycleManager_timeout(t *testing.T) {
	// Stop with a ctx shorter than the in-flight callback duration. The ctx
	// expiry no longer flips the "should stop" decision — Stop, once
	// requested, commits to stopping the cycle. The ctx only governs whether
	// the cycle callback honors an early abort. This callback (aborts=1) does
	// not check shouldAbort, so it runs to completion; the manager then
	// transitions to stopped.
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 20 * time.Millisecond
	stopTimeout := 12 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	t.Run("timeout is reached", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()

		cm.Start()
		<-p.firstCycleStarted

		stopResult := cm.Stop(timeoutCtx)

		select {
		case <-timeoutCtx.Done():
			assert.True(t, cm.Running())
		case <-stopResult:
			t.Fatal("stopped before timeout")
		}

		// callback runs to completion, then handleStopRequest(true) fires.
		assert.True(t, <-stopResult)
		assert.False(t, cm.Running())
		assert.Equal(t, "something wonderful...", <-p.results)
	})
}

func TestCycleManager_timeoutWithWait(t *testing.T) {
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 20 * time.Millisecond
	stopTimeout := 12 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	t.Run("timeout is reached", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()

		cm.Start()
		<-p.firstCycleStarted

		err := cm.StopAndWait(timeoutCtx)

		assert.NotNil(t, err)
		assert.Equal(t, "context deadline exceeded", err.Error())
		assert.True(t, cm.Running())
		assert.Equal(t, "something wonderful...", <-p.results)
	})

	t.Run("stop", func(t *testing.T) {
		stopResult := cm.Stop(context.Background())
		assert.True(t, <-stopResult)
		assert.False(t, cm.Running())
	})
}

func TestCycleManager_doesNotStartMultipleTimes(t *testing.T) {
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 1 * time.Millisecond

	startCount := 5

	p := newProvider(cycleDuration, uint(startCount))
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	t.Run("multiple starts", func(t *testing.T) {
		for i := 0; i < startCount; i++ {
			cm.Start()
		}
		<-p.firstCycleStarted

		stopResult := cm.Stop(context.Background())

		assert.True(t, <-stopResult)
		assert.False(t, cm.Running())
		// just one result produced
		assert.Equal(t, 1, len(p.results))
	})
}

func TestCycleManager_doesNotStartMultipleTimesWithWait(t *testing.T) {
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 1 * time.Millisecond

	startCount := 5

	p := newProvider(cycleDuration, uint(startCount))
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	t.Run("multiple starts", func(t *testing.T) {
		for i := 0; i < startCount; i++ {
			cm.Start()
		}
		<-p.firstCycleStarted

		err := cm.StopAndWait(context.Background())

		assert.Nil(t, err)
		assert.False(t, cm.Running())
		// just one result produced
		assert.Equal(t, 1, len(p.results))
	})
}

func TestCycleManager_handlesMultipleStops(t *testing.T) {
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 1 * time.Millisecond

	stopCount := 5

	p := newProvider(cycleDuration, 1)
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	t.Run("multiple stops", func(t *testing.T) {
		cm.Start()
		<-p.firstCycleStarted

		stopResult := make([]chan bool, stopCount)
		for i := 0; i < stopCount; i++ {
			stopResult[i] = cm.Stop(context.Background())
		}

		for i := 0; i < stopCount; i++ {
			assert.True(t, <-stopResult[i])
		}
		assert.False(t, cm.Running())
		assert.Equal(t, "something wonderful...", <-p.results)
	})
}

func TestCycleManager_stopsIfNotAllContextsAreCancelled(t *testing.T) {
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 1 * time.Millisecond
	stopTimeout := 5 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	t.Run("multiple stops, few cancelled", func(t *testing.T) {
		timeout1Ctx, cancel1 := context.WithTimeout(context.Background(), stopTimeout)
		timeout2Ctx, cancel2 := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel1()
		defer cancel2()

		cm.Start()
		<-p.firstCycleStarted

		stopResult1 := cm.Stop(timeout1Ctx)
		stopResult2 := cm.Stop(timeout2Ctx)
		stopResult3 := cm.Stop(context.Background())

		// all produce the same result: cycle was stopped
		assert.True(t, <-stopResult1)
		assert.True(t, <-stopResult2)
		assert.True(t, <-stopResult3)

		assert.False(t, cm.Running())
		assert.Equal(t, "something wonderful...", <-p.results)
	})
}

func TestCycleManager_stopsEvenIfAllContextsAreCancelled(t *testing.T) {
	// Once Stop is called, the cycle commits to stopping regardless of ctx
	// state. Expired ctx only signals "don't wait on me," not "never mind,
	// keep running" — that distinction is what the delete path relies on.
	cycleInterval := 50 * time.Millisecond
	cycleDuration := 10 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	t.Run("multiple stops, all already cancelled", func(t *testing.T) {
		expired1, cancel1 := context.WithCancel(context.Background())
		expired2, cancel2 := context.WithCancel(context.Background())
		expired3, cancel3 := context.WithCancel(context.Background())
		cancel1()
		cancel2()
		cancel3()

		cm.Start()
		<-p.firstCycleStarted

		stopResult1 := cm.Stop(expired1)
		stopResult2 := cm.Stop(expired2)
		stopResult3 := cm.Stop(expired3)

		assert.True(t, <-stopResult1)
		assert.True(t, <-stopResult2)
		assert.True(t, <-stopResult3)

		assert.False(t, cm.Running())
		assert.Equal(t, "something wonderful...", <-p.results)
	})
}

func TestCycleManager_cycleCallbackAbortsImmediatelyOnPreExpiredCtx(t *testing.T) {
	// The delete-path contract: a pre-expired ctx tells the cycle to abort
	// in-flight work immediately. A callback that honors shouldAbort at any
	// frequency bails on its very next check.
	cycleInterval := 50 * time.Millisecond
	cycleDuration := 300 * time.Millisecond

	// Callback checks shouldAbort every ~20ms (15 checks across 300ms).
	p := newProviderAbortable(cycleDuration, 1, 15)
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	expired, cancel := context.WithCancel(context.Background())
	cancel()

	cm.Start()
	<-p.firstCycleStarted

	start := time.Now()
	err := cm.StopAndWait(expired)
	elapsed := time.Since(start)

	// StopAndWait returns the ctx error — caller already gave up waiting.
	assert.ErrorIs(t, err, context.Canceled)
	// Callback should abort within ~20ms (its check frequency); we leave
	// generous headroom for slow CI.
	assert.Less(t, elapsed, 100*time.Millisecond)
	// Manager transitions to stopped after callback aborts.
	assert.Eventually(t, func() bool { return !cm.Running() }, time.Second, 5*time.Millisecond)
	// Callback aborted before sending its result.
	assert.Equal(t, 0, len(p.results))
}

func TestCycleManager_cycleCallbackAbortsOnExpiredCtx(t *testing.T) {
	// With the ctx-expiry-driven abort signal, even a callback with rare
	// shouldAbort checks aborts on its first check after ctx expiry. Compare
	// to the legacy semantics where "rare checks + tight ctx = callback runs
	// to completion"; the new semantics honour the caller's deadline.
	cycleInterval := 50 * time.Millisecond
	cycleDuration := 300 * time.Millisecond
	stopTimeout := 100 * time.Millisecond

	// Callback checks shouldAbort every 150ms during a 300ms cycle.
	p := newProviderAbortable(cycleDuration, 1, 2)
	cm := NewManager("test", NewFixedTicker(cycleInterval), p.cycleCallback, logger)

	timeoutCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()

	cm.Start()
	<-p.firstCycleStarted

	err := cm.StopAndWait(timeoutCtx)

	// StopAndWait returns ctx.Err() — the caller stopped waiting.
	assert.NotNil(t, err)
	assert.Equal(t, "context deadline exceeded", err.Error())
	// Callback aborted at its next abort-check (~150ms in), so it never
	// produced a result.
	assert.Eventually(t, func() bool { return !cm.Running() }, time.Second, 5*time.Millisecond)
	assert.Equal(t, 0, len(p.results))
}
