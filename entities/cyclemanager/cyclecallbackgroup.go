//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entsentry "github.com/weaviate/weaviate/entities/sentry"

	"github.com/sirupsen/logrus"
)

// Container for multiple callbacks exposing CycleCallback method acting as single callback.
// Can be provided to CycleManager.
type CycleCallbackGroup interface {
	// Adds CycleCallback method to container
	Register(id string, cycleCallback CycleCallback, options ...RegisterOption) CycleCallbackCtrl
	// Method of CycleCallback acting as single callback for all callbacks added to the container
	CycleCallback(shouldAbort ShouldAbortCallback) bool
}

type cycleCallbackGroup struct {
	sync.Mutex

	logger        logrus.FieldLogger
	customId      string
	routinesLimit int
	nextId        uint32
	callbackIds   []uint32
	callbacks     map[uint32]*cycleCallbackMeta
}

func NewCallbackGroup(id string, logger logrus.FieldLogger, routinesLimit int) CycleCallbackGroup {
	return &cycleCallbackGroup{
		logger:        logger,
		customId:      id,
		routinesLimit: routinesLimit,
		nextId:        0,
		callbackIds:   []uint32{},
		callbacks:     map[uint32]*cycleCallbackMeta{},
	}
}

func (c *cycleCallbackGroup) Register(id string, cycleCallback CycleCallback, options ...RegisterOption) CycleCallbackCtrl {
	c.Lock()
	defer c.Unlock()

	meta := &cycleCallbackMeta{
		customId:      id,
		cycleCallback: cycleCallback,
		active:        true,
		runningCtx:    nil,
		started:       time.Now(),
		intervals:     nil,
	}
	for _, option := range options {
		if option != nil {
			option(meta)
		}
	}

	callbackId := c.nextId
	c.callbackIds = append(c.callbackIds, callbackId)
	c.callbacks[callbackId] = meta
	c.nextId++

	return &cycleCallbackCtrl{
		callbackId:       callbackId,
		callbackCustomId: id,

		isActive:   c.isActive,
		activate:   c.activate,
		deactivate: c.deactivate,
		unregister: c.unregister,
	}
}

func (c *cycleCallbackGroup) CycleCallback(shouldAbort ShouldAbortCallback) bool {
	if c.routinesLimit <= 1 {
		return c.cycleCallbackSequential(shouldAbort)
	}
	return c.cycleCallbackParallel(shouldAbort, c.routinesLimit)
}

func (c *cycleCallbackGroup) cycleCallbackSequential(shouldAbort ShouldAbortCallback) bool {
	anyExecuted := false
	i := 0
	for {
		if shouldAbort() {
			break
		}

		c.Lock()
		// no more callbacks left, exit the loop
		if i >= len(c.callbackIds) {
			c.Unlock()
			break
		}

		callbackId := c.callbackIds[i]
		meta, ok := c.callbacks[callbackId]
		// callback deleted in the meantime, remove its id
		// and proceed to the next one (no "i" increment required)
		if !ok {
			c.callbackIds = append(c.callbackIds[:i], c.callbackIds[i+1:]...)
			c.Unlock()
			continue
		}
		i++
		// callback deactivated, proceed to the next one
		if !meta.active {
			c.Unlock()
			continue
		}
		now := time.Now()
		// not enough time passed since previous execution
		if meta.intervals != nil && now.Sub(meta.started) < meta.intervals.Get() {
			c.Unlock()
			continue
		}
		// callback active, mark as running
		runningCtx, cancel := context.WithCancel(context.Background())
		meta.runningCtx = runningCtx
		meta.started = now
		c.Unlock()

		func() {
			// cancel called in recover, regardless of panic occurred or not
			defer c.recover(meta.customId, cancel)
			executed := meta.cycleCallback(func() bool {
				if shouldAbort() {
					return true
				}

				c.Lock()
				defer c.Unlock()

				return meta.shouldAbort
			})
			anyExecuted = executed || anyExecuted

			if meta.intervals != nil {
				if executed {
					meta.intervals.Reset()
				} else {
					meta.intervals.Advance()
				}
			}
		}()
	}

	return anyExecuted
}

func (c *cycleCallbackGroup) cycleCallbackParallel(shouldAbort ShouldAbortCallback, routinesLimit int) bool {
	anyExecuted := false
	ch := make(chan uint32)
	lock := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	wg.Add(routinesLimit)

	i := 0
	for r := 0; r < routinesLimit; r++ {
		f := func() {
			for callbackId := range ch {
				if shouldAbort() {
					// keep reading from channel until it is closed
					continue
				}

				c.Lock()
				meta, ok := c.callbacks[callbackId]
				// callback missing or deactivated, proceed to the next one
				if !ok || !meta.active {
					c.Unlock()
					continue
				}
				now := time.Now()
				// not enough time passed since previous execution
				if meta.intervals != nil && now.Sub(meta.started) < meta.intervals.Get() {
					c.Unlock()
					continue
				}
				// callback active, mark as running
				runningCtx, cancel := context.WithCancel(context.Background())
				meta.runningCtx = runningCtx
				meta.started = now
				c.Unlock()

				func() {
					// cancel called in recover, regardless of panic occurred or not
					defer c.recover(meta.customId, cancel)
					executed := meta.cycleCallback(func() bool {
						if shouldAbort() {
							return true
						}

						c.Lock()
						defer c.Unlock()

						return meta.shouldAbort
					})

					if executed {
						lock.Lock()
						anyExecuted = true
						lock.Unlock()
					}
					if meta.intervals != nil {
						if executed {
							meta.intervals.Reset()
						} else {
							meta.intervals.Advance()
						}
					}
				}()
			}
			wg.Done()
		}
		enterrors.GoWrapper(f, c.logger)
	}

	for {
		if shouldAbort() {
			close(ch)
			break
		}

		c.Lock()
		// no more callbacks left, exit the loop
		if i >= len(c.callbackIds) {
			c.Unlock()
			close(ch)
			break
		}

		callbackId := c.callbackIds[i]
		_, ok := c.callbacks[callbackId]
		// callback deleted in the meantime, remove its id
		// and proceed to the next one (no "i" increment required)
		if !ok {
			c.callbackIds = append(c.callbackIds[:i], c.callbackIds[i+1:]...)
			c.Unlock()
			continue
		}
		c.Unlock()
		ch <- callbackId
		i++
	}

	wg.Wait()
	return anyExecuted
}

func (c *cycleCallbackGroup) recover(callbackCustomId string, cancel context.CancelFunc) {
	if r := recover(); r != nil {
		entsentry.Recover(r)
		c.logger.WithFields(logrus.Fields{
			"action":       "cyclemanager",
			"callback_id":  callbackCustomId,
			"callbacks_id": c.customId,
			"trace":        trace(),
		}).Errorf("callback panic: %v", r)
	}
	cancel()
}

func (c *cycleCallbackGroup) mutateCallback(ctx context.Context, callbackId uint32,
	onMetaNotFound func(callbackId uint32) error,
	onMetaFound func(callbackId uint32, meta *cycleCallbackMeta, running bool) error,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	for {
		// mutate callback in collection only if not running (not yet started of finished)
		c.Lock()
		meta, ok := c.callbacks[callbackId]
		if !ok {
			err := onMetaNotFound(callbackId)
			c.Unlock()
			return err
		}
		runningCtx := meta.runningCtx
		running := runningCtx != nil && runningCtx.Err() == nil

		if err := onMetaFound(callbackId, meta, running); err != nil {
			c.Unlock()
			return err
		}
		if !running {
			c.Unlock()
			return nil
		}
		c.Unlock()

		// wait for callback to finish
		select {
		case <-runningCtx.Done():
			// get back to the beginning of the loop to make sure state.runningCtx
			// was not changed. If not, loop will finish on runningCtx.Err() != nil check
			continue
		case <-ctx.Done():
			// in case both contexts are ready, but input ctx was selected
			// check again running ctx as priority one
			if runningCtx.Err() != nil {
				// get back to the beginning of the loop to make sure state.runningCtx
				// was not changed. If not, loop will finish on runningCtx.Err() != nil check
				continue
			}
			// input ctx expired
			return ctx.Err()
		}
	}
}

func (c *cycleCallbackGroup) unregister(ctx context.Context, callbackId uint32, callbackCustomId string) error {
	err := c.mutateCallback(ctx, callbackId,
		func(callbackId uint32) error {
			return nil
		},
		func(callbackId uint32, meta *cycleCallbackMeta, running bool) error {
			meta.shouldAbort = true
			if !running {
				meta.active = false
				delete(c.callbacks, callbackId)
			}
			return nil
		},
	)
	return errorUnregisterCallback(callbackCustomId, c.customId, err)
}

func (c *cycleCallbackGroup) deactivate(ctx context.Context, callbackId uint32, callbackCustomId string) error {
	err := c.mutateCallback(ctx, callbackId,
		func(callbackId uint32) error {
			return ErrorCallbackNotFound
		},
		func(callbackId uint32, meta *cycleCallbackMeta, running bool) error {
			meta.shouldAbort = true
			if !running {
				meta.active = false
			}
			return nil
		},
	)
	return errorDeactivateCallback(callbackCustomId, c.customId, err)
}

func (c *cycleCallbackGroup) activate(callbackId uint32, callbackCustomId string) error {
	c.Lock()
	defer c.Unlock()

	meta, ok := c.callbacks[callbackId]
	if !ok {
		return errorActivateCallback(callbackCustomId, c.customId, ErrorCallbackNotFound)
	}

	meta.shouldAbort = false
	meta.active = true
	return nil
}

func (c *cycleCallbackGroup) isActive(callbackId uint32, callbackCustomId string) bool {
	c.Lock()
	defer c.Unlock()

	if meta, ok := c.callbacks[callbackId]; ok {
		return meta.active
	}
	return false
}

type cycleCallbackMeta struct {
	customId      string
	cycleCallback CycleCallback
	active        bool
	// indicates whether callback is already running - context active
	// or not running (already finished) - context expired
	// or not running (not yet started) - context nil
	runningCtx context.Context
	started    time.Time
	intervals  CycleIntervals
	// true if deactivate or unregister were requested to abort callback when running
	shouldAbort bool
}

type cycleCallbackGroupNoop struct{}

func NewCallbackGroupNoop() CycleCallbackGroup {
	return &cycleCallbackGroupNoop{}
}

func (c *cycleCallbackGroupNoop) Register(id string, cycleCallback CycleCallback, options ...RegisterOption) CycleCallbackCtrl {
	return NewCallbackCtrlNoop()
}

func (c *cycleCallbackGroupNoop) CycleCallback(shouldAbort ShouldAbortCallback) bool {
	return false
}

type RegisterOption func(meta *cycleCallbackMeta)

func AsInactive() RegisterOption {
	return func(meta *cycleCallbackMeta) {
		meta.active = false
	}
}

func WithIntervals(intervals CycleIntervals) RegisterOption {
	if intervals == nil {
		return nil
	}
	return func(meta *cycleCallbackMeta) {
		meta.intervals = intervals
		// adjusts start time to allow for immediate callback execution without
		// having to wait for interval duration to pass
		meta.started = time.Now().Add(-intervals.Get())
	}
}

func trace() string {
	var sb strings.Builder
	pcs := make([]uintptr, 10)
	n := runtime.Callers(3, pcs) // skip self, callers and recover
	pcs = pcs[:n]
	for i := range pcs {
		f := errors.Frame(pcs[i])
		sb.WriteString(fmt.Sprintf("%n@%s:%d", f, f, f))
		if i < n-1 {
			sb.WriteString(";")
		}
	}
	return sb.String()
}
