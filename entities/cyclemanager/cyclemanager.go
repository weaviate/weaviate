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
	"sync"
	"time"
)

type (
	StopFunc  func() bool
	CycleFunc func(StopFunc)
)

type CycleManager struct {
	sync.RWMutex

	cycleFunc     CycleFunc
	cycleInterval time.Duration
	running       bool
	stop          chan struct{}
	stopContexts  []context.Context
	stopResult    chan bool
}

func New(cycleInterval time.Duration, cycleFunc CycleFunc) *CycleManager {
	return &CycleManager{
		cycleFunc:     cycleFunc,
		cycleInterval: cycleInterval,
		running:       false,
		stop:          make(chan struct{}, 1),
	}
}

// Starts instance, does not block
// Does nothing if instance is already started
func (c *CycleManager) Start() {
	c.Lock()
	defer c.Unlock()

	if c.cycleInterval <= 0 || c.running {
		return
	}

	go func() {
		ticker := time.NewTicker(c.cycleInterval)
		defer ticker.Stop()

		for {
			if c.selectedStop(ticker) {
				c.Lock()
				if c.anyCtxValid() {
					c.handleStop(true)
					c.Unlock()
					break
				}
				c.handleStop(false)
				c.Unlock()
				continue
			}
			c.cycleFunc(c.stopFunc)
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
		sendAndClose(stopResult, true)
		return stopResult
	}

	if len(c.stopContexts) == 0 {
		// Stop called 1st time on running instance
		c.stopContexts = []context.Context{ctx}
		c.stopResult = stopResult
		c.stop <- struct{}{}

	} else {
		// Stop called another time on running instance
		// before 1st stop was handled
		// results of previous and current call are therefore combined to
		// return the same and consistent output
		combinedStopResult := make(chan bool, 1)
		prevStopResult := c.stopResult
		c.stopContexts = append(c.stopContexts, ctx)
		c.stopResult = combinedStopResult

		go func(combinedStopResult <-chan bool, prevStopResult chan<- bool, stopResult chan<- bool) {
			stopped := <-combinedStopResult
			sendAndClose(prevStopResult, stopped)
			sendAndClose(stopResult, stopped)
		}(combinedStopResult, prevStopResult, stopResult)
	}

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

func (c *CycleManager) anyCtxValid() bool {
	if len(c.stopContexts) > 0 {
		for _, ctx := range c.stopContexts {
			if ctx.Err() == nil {
				return true
			}
		}
	}
	return false
}

func (c *CycleManager) stopFunc() bool {
	c.RLock()
	defer c.RUnlock()

	return c.anyCtxValid()
}

func (c *CycleManager) selectedStop(ticker *time.Ticker) bool {
	select {
	case <-c.stop:
	case <-ticker.C:
		// as stop chan has higher priority,
		// it is checked again in case of ticker was selected over stop if both were ready
		select {
		case <-c.stop:
		default:
			return false
		}
	}
	return true
}

func (c *CycleManager) handleStop(stopped bool) {
	sendAndClose(c.stopResult, stopped)
	c.running = !stopped
	c.stopResult = nil
	c.stopContexts = nil
}

func sendAndClose(ch chan<- bool, value bool) {
	ch <- value
	close(ch)
}
