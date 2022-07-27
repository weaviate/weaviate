//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package cyclemanager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCycleManager(t *testing.T) {
	sleeper := sleeper{
		dreams: make(chan string, 1),
	}

	t.Run("create new", func(t *testing.T) {
		description := "test cycle"
		sleeper.sleepCycle = New(sleeper.sleep, description)

		assert.False(t, sleeper.sleepCycle.running)
		assert.Equal(t, sleeper.sleepCycle.description, description)
		assert.NotNil(t, sleeper.sleepCycle.cycleFunc)
		assert.NotNil(t, sleeper.sleepCycle.Stopped)
	})

	t.Run("start", func(t *testing.T) {
		sleeper.sleepCycle.Start(100 * time.Millisecond)
		assert.True(t, sleeper.sleepCycle.running)
		assert.Equal(t, "something wonderful...", <-sleeper.dreams)
	})

	t.Run("stop", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		stopped := make(chan struct{})

		go func() {
			sleeper.sleepCycle.Stop(timeoutCtx)
			stopped <- struct{}{}
		}()

		select {
		case <-timeoutCtx.Done():
			t.Fatal(timeoutCtx.Err().Error(), "failed to stop sleeper")
		case <-stopped:
		}

		assert.False(t, sleeper.sleepCycle.running)
		assert.Empty(t, <-sleeper.dreams)
	})
}

func TestCycleManager_CancelContext(t *testing.T) {
	sleeper := sleeper{
		dreams: make(chan string, 1),
	}

	cycleInterval := 100 * time.Millisecond

	t.Run("create new", func(t *testing.T) {
		description := "test cycle"
		sleeper.sleepCycle = New(sleeper.sleepDelayedWakeup, description)

		assert.False(t, sleeper.sleepCycle.running)
		assert.Equal(t, sleeper.sleepCycle.description, description)
		assert.NotNil(t, sleeper.sleepCycle.cycleFunc)
		assert.NotNil(t, sleeper.sleepCycle.Stopped)
	})

	t.Run("start", func(t *testing.T) {
		sleeper.sleepCycle.Start(cycleInterval)
		assert.True(t, sleeper.sleepCycle.running)
		assert.Equal(t, "something wonderful...", <-sleeper.dreams)
	})

	t.Run("cancel early", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()

		awake := make(chan struct{})

		go func() {
			sleeper.sleepCycle.Stop(ctx)
			awake <- struct{}{}
		}()

		select {
		case <-ctx.Done():
			done := make(chan struct{})

			// if it takes longer than a second to restart
			// the cycle, that means that `Stop` still has
			// the lock obtained, and Start must wait.
			//
			// failure here will be obvious, because `Stop`
			// here is configured to block for well beyond
			// one second.
			go failIfTimeout(done, time.Second)

			sleeper.sleepCycle.Start(cycleInterval)
			done <- struct{}{}
		case <-awake:
			t.Fatal("context should have been cancelled")
		}
	})
}

type sleeper struct {
	sleepCycle *CycleManager
	dreams     chan string
}

func (s *sleeper) sleep(interval time.Duration) {
	go func() {
		t := time.Tick(interval)
		for {
			select {
			case <-s.sleepCycle.Stopped:
				close(s.dreams)
				return
			case <-t:
				s.dreams <- "something wonderful..."
			}
		}
	}()
}

func (s *sleeper) sleepDelayedWakeup(interval time.Duration) {
	go func() {
		t := time.Tick(interval)
		for {
			select {
			case <-s.sleepCycle.Stopped:
				// simulate a blocking channel receive
				fmt.Println("about to sleep for 24 hours")
				time.Sleep(24 * time.Hour)
				fmt.Println("done sleeping")
				return
			case <-t:
				s.dreams <- "something wonderful..."
			}
		}
	}()
}

func failIfTimeout(done chan struct{}, d time.Duration) {
	select {
	case <-done:
		return
	case <-time.After(d):
		panic("test timed out")
	}
}
