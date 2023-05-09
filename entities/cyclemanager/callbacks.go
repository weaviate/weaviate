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
)

type UnregisterFunc func(ctx context.Context) error

type callbacks interface {
	register(cycleFunc CycleFunc) UnregisterFunc
	execute(shouldBreak ShouldBreakFunc) bool
}

type callbackState struct {
	cycleFunc  CycleFunc
	runningCtx context.Context
}

type multiCallbacks struct {
	lock    *sync.Mutex
	counter uint32
	states  map[uint32]*callbackState
	keys    []uint32

	canceledCtx context.Context
}

func newMultiCallbacks() callbacks {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	return &multiCallbacks{
		lock:        new(sync.Mutex),
		counter:     0,
		states:      map[uint32]*callbackState{},
		keys:        []uint32{},
		canceledCtx: canceledCtx,
	}
}

func (c *multiCallbacks) register(cycleFunc CycleFunc) UnregisterFunc {
	c.lock.Lock()
	defer c.lock.Unlock()

	key := c.counter
	c.keys = append(c.keys, key)
	c.states[key] = &callbackState{
		cycleFunc:  cycleFunc,
		runningCtx: c.canceledCtx,
	}
	c.counter++

	return func(ctx context.Context) error {
		return c.unregister(ctx, key)
	}
}

func (c *multiCallbacks) unregister(ctx context.Context, key uint32) error {
	for {
		// remove from collection only if not running. wait if already running
		c.lock.Lock()
		state, ok := c.states[key]
		if !ok {
			c.lock.Unlock()
			return nil
		}
		runningCtx := state.runningCtx
		if runningCtx.Err() != nil {
			delete(c.states, key)
			c.lock.Unlock()
			return nil
		}
		c.lock.Unlock()

		// wait until exec finished
		select {
		case <-runningCtx.Done():
			// get back to the beginning of the loop
			continue
		case <-ctx.Done():
			// in case both context are ready, but incoming ctx was selected
			// check again running ctx as priority one
			select {
			case <-runningCtx.Done():
				// get back to the beginning of the loop
				continue
			default:
				return ctx.Err()
			}
		}
	}
}

func (c *multiCallbacks) execute(shouldBreak ShouldBreakFunc) bool {
	executed := false
	i := 0
	for {
		if shouldBreak() {
			break
		}

		c.lock.Lock()
		if i >= len(c.keys) {
			c.lock.Unlock()
			break
		}

		key := c.keys[i]
		state, ok := c.states[key]
		if !ok { // key deleted in the meantime, adjust keys
			c.keys = append(c.keys[:i], c.keys[i+1:]...)
			c.lock.Unlock()
			continue
		}

		cancelableCtx, cancel := context.WithCancel(context.Background())
		state.runningCtx = cancelableCtx
		i++
		c.lock.Unlock()

		executed = state.cycleFunc(shouldBreak) || executed
		cancel()
	}

	return executed
}
