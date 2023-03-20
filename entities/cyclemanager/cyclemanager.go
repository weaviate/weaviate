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

type CycleManager struct {
	sync.RWMutex

	cycleFunc   CycleFunc
	cycleTicker CycleTicker
	running     bool
	stopSignal  chan struct{}

	stopContexts []context.Context
	stopResults  []chan bool
}

func New(cycleTicker CycleTicker, cycleFunc CycleFunc) *CycleManager {
	return &CycleManager{
		cycleFunc:   cycleFunc,
		cycleTicker: cycleTicker,
		running:     false,
		stopSignal:  make(chan struct{}, 1),
	}
}

// Starts instance, does not block
// Does nothing if instance is already started
func (c *CycleManager) Start() {
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
			c.cycleTicker.CycleExecuted(c.cycleFunc(c.shouldBreakCycleCallback))
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
func (c *CycleManager) Stop(ctx context.Context) (stopResult chan bool) {
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
func (c *CycleManager) StopAndWait(ctx context.Context) error {
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
		select {
		case <-done:
			if !stopped {
				return ctx.Err()
			}
		default:
			if !stopped {
				return fmt.Errorf("failed to stop cycle")
			}
		}
	}
	return nil
}

func (c *CycleManager) Running() bool {
	c.RLock()
	defer c.RUnlock()

	return c.running
}

func (c *CycleManager) shouldStop() bool {
	for _, ctx := range c.stopContexts {
		if ctx.Err() == nil {
			return true
		}
	}
	return false
}

func (c *CycleManager) shouldBreakCycleCallback() bool {
	c.RLock()
	defer c.RUnlock()

	return c.shouldStop()
}

func (c *CycleManager) isStopRequested() bool {
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

func (c *CycleManager) handleStopRequest(stopped bool) {
	for _, stopResult := range c.stopResults {
		stopResult <- stopped
		close(stopResult)
	}
	c.running = !stopped
	c.stopContexts = nil
	c.stopResults = nil
}
