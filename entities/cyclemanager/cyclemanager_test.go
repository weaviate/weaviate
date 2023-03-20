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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type cycleFuncProvider struct {
	sync.Mutex

	firstCycleStarted chan struct{}
	cycleFunc         CycleFunc
	results           chan string
}

func newProvider(cycleDuration time.Duration, resultsSize uint) *cycleFuncProvider {
	return newProviderBreakable(cycleDuration, resultsSize, 1)
}

func newProviderBreakable(cycleDuration time.Duration, resultsSize uint, breaks int) *cycleFuncProvider {
	fs := false
	p := &cycleFuncProvider{}
	p.results = make(chan string, resultsSize)
	p.firstCycleStarted = make(chan struct{}, 1)
	p.cycleFunc = func(shouldBreak ShouldBreakFunc) bool {
		p.Lock()
		if !fs {
			p.firstCycleStarted <- struct{}{}
			fs = true
		}
		p.Unlock()

		if breaks > 1 {
			for i := 0; i < breaks; i++ {
				time.Sleep(cycleDuration / time.Duration(breaks))
				if shouldBreak() {
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
	var cm *CycleManager

	t.Run("create new", func(t *testing.T) {
		cm = New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

		assert.False(t, cm.Running())
		assert.NotNil(t, cm.cycleFunc)
		assert.NotNil(t, cm.stopSignal)
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
	var cm *CycleManager

	t.Run("create new", func(t *testing.T) {
		cm = New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

		assert.False(t, cm.Running())
		assert.NotNil(t, cm.cycleFunc)
		assert.NotNil(t, cm.stopSignal)
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
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 20 * time.Millisecond
	stopTimeout := 12 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

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

		// make sure it is still running
		assert.False(t, <-stopResult)
		assert.True(t, cm.Running())
		assert.Equal(t, "something wonderful...", <-p.results)
	})

	t.Run("stop", func(t *testing.T) {
		stopResult := cm.Stop(context.Background())
		assert.True(t, <-stopResult)
		assert.False(t, cm.Running())
	})
}

func TestCycleManager_timeoutWithWait(t *testing.T) {
	cycleInterval := 5 * time.Millisecond
	cycleDuration := 20 * time.Millisecond
	stopTimeout := 12 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

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
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

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
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

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
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

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
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

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

func TestCycleManager_doesNotStopIfAllContextsAreCancelled(t *testing.T) {
	cycleInterval := 50 * time.Millisecond
	cycleDuration := 10 * time.Millisecond
	stopTimeout := 50 * time.Millisecond

	p := newProvider(cycleDuration, 1)
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

	t.Run("multiple stops, few cancelled", func(t *testing.T) {
		timeout1Ctx, cancel1 := context.WithTimeout(context.Background(), stopTimeout)
		timeout2Ctx, cancel2 := context.WithTimeout(context.Background(), stopTimeout)
		timeout3Ctx, cancel3 := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel1()
		defer cancel2()
		defer cancel3()

		cm.Start()
		<-p.firstCycleStarted

		stopResult1 := cm.Stop(timeout1Ctx)
		stopResult2 := cm.Stop(timeout2Ctx)
		stopResult3 := cm.Stop(timeout3Ctx)

		// all produce the same result: cycle was stopped
		assert.False(t, <-stopResult1)
		assert.False(t, <-stopResult2)
		assert.False(t, <-stopResult3)

		assert.True(t, cm.Running())
		assert.Equal(t, "something wonderful...", <-p.results)
	})

	t.Run("stop", func(t *testing.T) {
		stopResult := cm.Stop(context.Background())
		assert.True(t, <-stopResult)
		assert.False(t, cm.Running())
	})
}

func TestCycleManager_cycleFuncStoppedDueToFrequentStopChecks(t *testing.T) {
	cycleInterval := 50 * time.Millisecond
	cycleDuration := 300 * time.Millisecond
	stopTimeout := 100 * time.Millisecond

	// despite cycleDuration is 30ms, cycle function checks every 20ms (300/15) if it needs to be stopped
	p := newProviderBreakable(cycleDuration, 1, 15)
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

	t.Run("cycle funcion stopped before timeout reached", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()

		cm.Start()
		<-p.firstCycleStarted

		err := cm.StopAndWait(timeoutCtx)

		assert.Nil(t, err)
		assert.False(t, cm.Running())
		assert.Equal(t, 0, len(p.results))
	})

	t.Run("stop", func(t *testing.T) {
		stopResult := cm.Stop(context.Background())
		assert.True(t, <-stopResult)
		assert.False(t, cm.Running())
	})
}

func TestCycleManager_cycleFuncNotStoppedDueToRareStopChecks(t *testing.T) {
	cycleInterval := 50 * time.Millisecond
	cycleDuration := 300 * time.Millisecond
	stopTimeout := 100 * time.Millisecond

	// despite cycleDuration is 30ms, cycle function checks every 150ms (300/2) if it needs to be stopped
	p := newProviderBreakable(cycleDuration, 1, 2)
	cm := New(NewFixedIntervalTicker(cycleInterval), p.cycleFunc)

	t.Run("timeout reached", func(t *testing.T) {
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
