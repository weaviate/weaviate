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
	"fmt"
	"sync"
)

type (
	// indicates whether cyclemanager's stop was requested to allow safely
	// break execution of CycleFunc and stop cyclemanager earlier
	ShouldBreakFunc func() bool
	// return value indicates whether actual work was done in the cycle
	CycleFunc func(shouldBreak ShouldBreakFunc) bool
)

type CycleManager interface {
	Register(cycleFunc CycleFunc) UnregisterFunc
	Start()
	Stop(ctx context.Context) chan bool
	StopAndWait(ctx context.Context) error
	Running() bool
}

type cycleManager struct {
	sync.RWMutex

	callbacks   callbacks
	cycleTicker CycleTicker
	running     bool
	stopSignal  chan struct{}

	stopContexts []context.Context
	stopResults  []chan bool
}

func NewMulti(cycleTicker CycleTicker) CycleManager {
	return &cycleManager{
		callbacks:   newMultiCallbacks(),
		cycleTicker: cycleTicker,
		running:     false,
		stopSignal:  make(chan struct{}, 1),
	}
}

func (c *cycleManager) Register(cycleFunc CycleFunc) UnregisterFunc {
	return c.callbacks.register(cycleFunc)
}

// Starts instance, does not block
// Does nothing if instance is already started
func (c *cycleManager) Start() {
	c.Lock()
	defer c.Unlock()

	if c.running {
		return
	}

	go func() {
		c.cycleTicker.Start()
		defer c.cycleTicker.Stop()

		for {
			if c.isStopRequested() {
				c.Lock()
				if c.shouldStop() {
					c.handleStopRequest(true)
					c.Unlock()
					break
				}
				c.handleStopRequest(false)
				c.Unlock()
				continue
			}
			c.cycleTicker.CycleExecuted(c.callbacks.execute(c.shouldBreakCycleCallback))
		}
	}()

	c.running = true
}

// Stops running instance, does not block
// Returns channel with final stop result - true / false
//
// If given context is cancelled before it is handled by stop logic, instance is not stopped
// If called multiple times, all contexts have to be cancelled to cancel stop
// (any valid will result in stopping instance)
// stopResult is the same (consistent) for multiple calls
func (c *cycleManager) Stop(ctx context.Context) (stopResult chan bool) {
	c.Lock()
	defer c.Unlock()

	stopResult = make(chan bool, 1)
	if !c.running {
		stopResult <- true
		close(stopResult)
		return stopResult
	}

	if len(c.stopContexts) == 0 {
		defer func() {
			c.stopSignal <- struct{}{}
		}()
	}
	c.stopContexts = append(c.stopContexts, ctx)
	c.stopResults = append(c.stopResults, stopResult)

	return stopResult
}

// Stops running instance, waits for stop to occur or context to expire (which comes first)
// Returns error if instance was not stopped
func (c *cycleManager) StopAndWait(ctx context.Context) error {
	// if both channels are ready, chan is selected randomly, therefore regardless of
	// channel selected first, second one is also checked
	stop := c.Stop(ctx)
	done := ctx.Done()

	select {
	case <-done:
		select {
		case stopped := <-stop:
			if !stopped {
				return ctx.Err()
			}
		default:
			return ctx.Err()
		}
	case stopped := <-stop:
		if !stopped {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("failed to stop cycle")
		}
	}
	return nil
}

func (c *cycleManager) Running() bool {
	c.RLock()
	defer c.RUnlock()

	return c.running
}

func (c *cycleManager) shouldStop() bool {
	for _, ctx := range c.stopContexts {
		if ctx.Err() == nil {
			return true
		}
	}
	return false
}

func (c *cycleManager) shouldBreakCycleCallback() bool {
	c.RLock()
	defer c.RUnlock()

	return c.shouldStop()
}

func (c *cycleManager) isStopRequested() bool {
	select {
	case <-c.stopSignal:
	case <-c.cycleTicker.C():
		// as stop chan has higher priority,
		// it is checked again in case of ticker was selected over stop if both were ready
		select {
		case <-c.stopSignal:
		default:
			return false
		}
	}
	return true
}

func (c *cycleManager) handleStopRequest(stopped bool) {
	for _, stopResult := range c.stopResults {
		stopResult <- stopped
		close(stopResult)
	}
	c.running = !stopped
	c.stopContexts = nil
	c.stopResults = nil
}

func NewNoop() CycleManager {
	return &noopCycleManager{running: false}
}

type noopCycleManager struct {
	running bool
}

func (c *noopCycleManager) Register(cycleFunc CycleFunc) UnregisterFunc {
	return func(ctx context.Context) error {
		return nil
	}
}

func (c *noopCycleManager) Start() {
	c.running = true
}

func (c *noopCycleManager) Stop(ctx context.Context) chan bool {
	if !c.running {
		return c.closedChan(true)
	}
	if ctx.Err() != nil {
		return c.closedChan(false)
	}

	c.running = false
	return c.closedChan(true)
}

func (c *noopCycleManager) StopAndWait(ctx context.Context) error {
	if <-c.Stop(ctx) {
		return nil
	}
	return ctx.Err()
}

func (c *noopCycleManager) Running() bool {
	return c.running
}

func (c *noopCycleManager) closedChan(val bool) chan bool {
	ch := make(chan bool, 1)
	ch <- val
	close(ch)
	return ch
}
