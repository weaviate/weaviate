package cyclemanager

import (
	"context"
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
