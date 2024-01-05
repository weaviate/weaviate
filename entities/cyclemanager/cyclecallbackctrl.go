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
	"sync"

	"github.com/weaviate/weaviate/entities/errorcompounder"
	"golang.org/x/sync/errgroup"
)

// Used to control of registered in CycleCallbacks container callback
// Allows deactivating and activating registered callback or unregistering it
type CycleCallbackCtrl interface {
	IsActive() bool
	Activate() error
	Deactivate(ctx context.Context) error
	Unregister(ctx context.Context) error
}

type cycleCallbackCtrl struct {
	callbackId       uint32
	callbackCustomId string

	isActive   func(callbackId uint32, callbackCustomId string) bool
	activate   func(callbackId uint32, callbackCustomId string) error
	deactivate func(ctx context.Context, callbackId uint32, callbackCustomId string) error
	unregister func(ctx context.Context, callbackId uint32, callbackCustomId string) error
}

func (c *cycleCallbackCtrl) IsActive() bool {
	return c.isActive(c.callbackId, c.callbackCustomId)
}

func (c *cycleCallbackCtrl) Activate() error {
	return c.activate(c.callbackId, c.callbackCustomId)
}

func (c *cycleCallbackCtrl) Deactivate(ctx context.Context) error {
	return c.deactivate(ctx, c.callbackId, c.callbackCustomId)
}

func (c *cycleCallbackCtrl) Unregister(ctx context.Context) error {
	return c.unregister(ctx, c.callbackId, c.callbackCustomId)
}

type cycleCombinedCallbackCtrl struct {
	routinesLimit int
	ctrls         []CycleCallbackCtrl
}

// Creates combined controller to manage all provided controllers at once as it was single instance.
// Methods (activate, deactivate, unregister) calls nested controllers' methods in parallel by number of
// goroutines given as argument. If < 1 value given, NumCPU is used.
func NewCombinedCallbackCtrl(routinesLimit int, ctrls ...CycleCallbackCtrl) CycleCallbackCtrl {
	if routinesLimit <= 0 {
		routinesLimit = _NUMCPU
	}

	return &cycleCombinedCallbackCtrl{routinesLimit: routinesLimit, ctrls: ctrls}
}

func (c *cycleCombinedCallbackCtrl) IsActive() bool {
	for _, ctrl := range c.ctrls {
		if !ctrl.IsActive() {
			return false
		}
	}
	return true
}

func (c *cycleCombinedCallbackCtrl) Activate() error {
	return c.combineErrors(c.activate()...)
}

func (c *cycleCombinedCallbackCtrl) activate() []error {
	eg := &errgroup.Group{}
	eg.SetLimit(c.routinesLimit)
	lock := new(sync.Mutex)

	errs := make([]error, 0, len(c.ctrls))
	for _, ctrl := range c.ctrls {
		ctrl := ctrl
		eg.Go(func() error {
			if err := ctrl.Activate(); err != nil {
				c.locked(lock, func() { errs = append(errs, err) })
				return err
			}
			return nil
		})
	}

	eg.Wait()
	return errs
}

func (c *cycleCombinedCallbackCtrl) Deactivate(ctx context.Context) error {
	errs, deactivated := c.deactivate(ctx)
	if len(errs) == 0 {
		return nil
	}

	// try activating back deactivated
	eg := &errgroup.Group{}
	eg.SetLimit(c.routinesLimit)
	for _, id := range deactivated {
		id := id
		eg.Go(func() error {
			return c.ctrls[id].Activate()
		})
	}

	eg.Wait()
	return c.combineErrors(errs...)
}

func (c *cycleCombinedCallbackCtrl) deactivate(ctx context.Context) ([]error, []int) {
	eg := &errgroup.Group{}
	eg.SetLimit(c.routinesLimit)
	lock := new(sync.Mutex)

	errs := make([]error, 0, len(c.ctrls))
	deactivated := make([]int, 0, len(c.ctrls))
	for id, ctrl := range c.ctrls {
		id, ctrl := id, ctrl
		eg.Go(func() error {
			if err := ctrl.Deactivate(ctx); err != nil {
				c.locked(lock, func() { errs = append(errs, err) })
				return err
			}
			c.locked(lock, func() { deactivated = append(deactivated, id) })
			return nil
		})
	}

	eg.Wait()
	return errs, deactivated
}

func (c *cycleCombinedCallbackCtrl) Unregister(ctx context.Context) error {
	return c.combineErrors(c.unregister(ctx)...)
}

func (c *cycleCombinedCallbackCtrl) unregister(ctx context.Context) []error {
	eg := &errgroup.Group{}
	eg.SetLimit(c.routinesLimit)
	lock := new(sync.Mutex)

	errs := make([]error, 0, len(c.ctrls))
	for _, ctrl := range c.ctrls {
		ctrl := ctrl
		eg.Go(func() error {
			if err := ctrl.Unregister(ctx); err != nil {
				c.locked(lock, func() { errs = append(errs, err) })
				return err
			}
			return nil
		})
	}

	eg.Wait()
	return errs
}

func (c *cycleCombinedCallbackCtrl) locked(lock *sync.Mutex, mutate func()) {
	lock.Lock()
	defer lock.Unlock()

	mutate()
}

func (c *cycleCombinedCallbackCtrl) combineErrors(errors ...error) error {
	ec := &errorcompounder.ErrorCompounder{}
	for _, err := range errors {
		ec.Add(err)
	}
	return ec.ToError()
}

type cycleCallbackCtrlNoop struct{}

func NewCallbackCtrlNoop() CycleCallbackCtrl {
	return &cycleCallbackCtrlNoop{}
}

func (c *cycleCallbackCtrlNoop) IsActive() bool {
	return false
}

func (c *cycleCallbackCtrlNoop) Activate() error {
	return nil
}

func (c *cycleCallbackCtrlNoop) Deactivate(ctx context.Context) error {
	return ctx.Err()
}

func (c *cycleCallbackCtrlNoop) Unregister(ctx context.Context) error {
	return ctx.Err()
}
