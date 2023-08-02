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

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Container for multiple callbacks exposing CycleCallback method acting as single callback.
// Can be provided to CycleManager.
type CycleCallbacks interface {
	// Adds CycleCallback method to container
	Register(id string, active bool, cycleCallback CycleCallback) CycleCallbackCtrl
	// Method of CycleCallback acting as single callback for all callbacks added to the container
	CycleCallback(shouldAbort ShouldAbortCallback) bool
}

type cycleCallbacks struct {
	sync.Mutex

	logger        logrus.FieldLogger
	customId      string
	routinesLimit int
	nextId        uint32
	callbackIds   []uint32
	callbacks     map[uint32]*cycleCallbackMeta
}

func NewCycleCallbacks(id string, logger logrus.FieldLogger, routinesLimit int) CycleCallbacks {
	return &cycleCallbacks{
		logger:        logger,
		customId:      id,
		routinesLimit: routinesLimit,
		nextId:        0,
		callbackIds:   []uint32{},
		callbacks:     map[uint32]*cycleCallbackMeta{},
	}
}

func (c *cycleCallbacks) Register(id string, active bool, cycleCallback CycleCallback) CycleCallbackCtrl {
	c.Lock()
	defer c.Unlock()

	callbackId := c.nextId
	c.callbackIds = append(c.callbackIds, callbackId)
	c.callbacks[callbackId] = &cycleCallbackMeta{
		customId:      id,
		cycleCallback: cycleCallback,
		active:        active,
		runningCtx:    nil,
	}
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

func (c *cycleCallbacks) CycleCallback(shouldAbort ShouldAbortCallback) bool {
	eg := &errgroup.Group{}
	eg.SetLimit(c.routinesLimit)
	lock := new(sync.Mutex)

	executed := false
	i := 0
	for {
		if shouldAbort() {
			break
		}

		c.Lock()
		if i >= len(c.callbackIds) {
			c.Unlock()
			break
		}

		callbackId := c.callbackIds[i]
		_, ok := c.callbacks[callbackId]
		// callback deleted in the meantime, adjust ids and proceed to the next one
		// no "i" increment required
		if !ok {
			c.callbackIds = append(c.callbackIds[:i], c.callbackIds[i+1:]...)
			c.Unlock()
			continue
		}
		i++
		c.Unlock()

		eg.Go(func() error {
			// check again if valid to execute, as conditions may have changed
			// between previous check and routine finally started
			if shouldAbort() {
				return nil
			}

			c.Lock()
			meta, ok := c.callbacks[callbackId]
			if !ok || !meta.active {
				c.Unlock()
				return nil
			}

			// callback active, mark as running and call
			runningCtx, cancel := context.WithCancel(context.Background())
			meta.runningCtx = runningCtx
			c.Unlock()

			// cancel called in recover, regardless of panic occurred or not
			defer c.recover(meta.customId, cancel)
			ex := meta.cycleCallback(shouldAbort)

			// skipped in case of panic, ok since ex will be false anyway
			lock.Lock()
			executed = ex || executed
			lock.Unlock()

			return nil
		})
	}

	eg.Wait()
	return executed
}

func (c *cycleCallbacks) recover(callbackCustomId string, cancel context.CancelFunc) {
	if r := recover(); r != nil {
		c.logger.WithFields(logrus.Fields{
			"action":       "cyclemanager",
			"callback_id":  callbackCustomId,
			"callbacks_id": c.customId,
		}).Errorf("callback panic: %v", r)
	}
	cancel()
}

func (c *cycleCallbacks) mutateCallback(ctx context.Context, callbackId uint32,
	onNotFound func(callbackId uint32) error,
	onFound func(callbackId uint32, meta *cycleCallbackMeta) error,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	for {
		// mutate callback in collection only if not running (not yet started of finished)
		c.Lock()
		meta, ok := c.callbacks[callbackId]
		if !ok {
			err := onNotFound(callbackId)
			c.Unlock()
			return err
		}
		runningCtx := meta.runningCtx
		if runningCtx == nil || runningCtx.Err() != nil {
			err := onFound(callbackId, meta)
			c.Unlock()
			return err
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

func (c *cycleCallbacks) unregister(ctx context.Context, callbackId uint32, callbackCustomId string) error {
	err := c.mutateCallback(ctx, callbackId,
		func(callbackId uint32) error {
			return nil
		},
		func(callbackId uint32, meta *cycleCallbackMeta) error {
			delete(c.callbacks, callbackId)
			return nil
		},
	)
	return errorUnregisterCallback(callbackCustomId, c.customId, err)
}

func (c *cycleCallbacks) deactivate(ctx context.Context, callbackId uint32, callbackCustomId string) error {
	err := c.mutateCallback(ctx, callbackId,
		func(callbackId uint32) error {
			return ErrorCallbackNotFound
		},
		func(callbackId uint32, meta *cycleCallbackMeta) error {
			meta.active = false
			return nil
		},
	)
	return errorDeactivateCallback(callbackCustomId, c.customId, err)
}

func (c *cycleCallbacks) activate(callbackId uint32, callbackCustomId string) error {
	c.Lock()
	defer c.Unlock()

	meta, ok := c.callbacks[callbackId]
	if !ok {
		return errorActivateCallback(callbackCustomId, c.customId, ErrorCallbackNotFound)
	}

	meta.active = true
	return nil
}

func (c *cycleCallbacks) isActive(callbackId uint32, callbackCustomId string) bool {
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
}

type cycleCallbacksNoop struct{}

func NewCycleCallbacksNoop() CycleCallbacks {
	return &cycleCallbacksNoop{}
}

func (c *cycleCallbacksNoop) Register(id string, active bool, cycleCallback CycleCallback) CycleCallbackCtrl {
	return NewCycleCallbackCtrlNoop()
}

func (c *cycleCallbacksNoop) CycleCallback(shouldAbort ShouldAbortCallback) bool {
	return false
}
