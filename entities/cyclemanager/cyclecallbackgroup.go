//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cyclemanager

import (
	"container/heap"
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
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
	dueHeap       dueHeap
	callbacks     map[uint32]*cycleCallbackMeta
}

func NewCallbackGroup(id string, logger logrus.FieldLogger, routinesLimit int) CycleCallbackGroup {
	return &cycleCallbackGroup{
		logger:        logger,
		customId:      id,
		routinesLimit: routinesLimit,
		nextId:        0,
		dueHeap:       dueHeap{},
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
		heapIndex:     -1,
	}
	for _, option := range options {
		if option != nil {
			option(meta)
		}
	}

	callbackId := c.nextId
	meta.callbackId = callbackId
	c.callbacks[callbackId] = meta
	c.nextId++
	c.pushDue(meta)

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
	due := c.drainDue(time.Now())
	anyExecuted := false
	for _, meta := range due {
		if shouldAbort() {
			// restore the un-run remainder so it is retried next tick
			c.repushDue(meta)
			continue
		}
		if c.execute(meta, shouldAbort) {
			anyExecuted = true
		}
		c.repushDue(meta)
	}
	return anyExecuted
}

func (c *cycleCallbackGroup) cycleCallbackParallel(shouldAbort ShouldAbortCallback, routinesLimit int) bool {
	due := c.drainDue(time.Now())
	// spawn no workers when nothing is due; callbacks becoming due right after
	// the drain are picked up on the next tick
	if len(due) == 0 {
		return false
	}

	var anyExecuted atomic.Bool
	wg := new(sync.WaitGroup)
	ch := make(chan *cycleCallbackMeta)

	worker := func() {
		defer wg.Done()
		for meta := range ch {
			if shouldAbort() {
				// restore the un-run entry so it is retried next tick
				c.repushDue(meta)
				continue
			}
			if c.execute(meta, shouldAbort) {
				anyExecuted.Store(true)
			}
			c.repushDue(meta)
		}
	}

	// no need for more workers than callbacks due
	if routinesLimit > len(due) {
		routinesLimit = len(due)
	}

	wg.Add(routinesLimit)
	for r := 0; r < routinesLimit; r++ {
		enterrors.GoWrapper(worker, c.logger)
	}

	// every drained meta must be handed to a worker so it is executed or
	// restored; an early break would orphan the remainder outside the heap
	for _, meta := range due {
		ch <- meta
	}
	close(ch)
	wg.Wait()
	return anyExecuted.Load()
}

// pushDue caches meta's next-due time and inserts it into the heap. Caller holds
// the lock.
func (c *cycleCallbackGroup) pushDue(meta *cycleCallbackMeta) {
	meta.nextDue = computeNextDue(meta)
	heap.Push(&c.dueHeap, meta)
}

// drainDue pops every callback whose next-due time has arrived and returns those
// still registered and active. Popped entries are removed from the heap, so each
// caller must execute or repush them (see execute / repushDue). Draining the whole
// due set up front runs each entry at most once per tick, which is what keeps a
// nil-interval "always due" entry from being re-popped within the same tick.
func (c *cycleCallbackGroup) drainDue(now time.Time) []*cycleCallbackMeta {
	c.Lock()
	defer c.Unlock()

	var due []*cycleCallbackMeta
	for c.dueHeap.Len() > 0 {
		if c.dueHeap[0].nextDue.After(now) {
			break
		}
		meta := heap.Pop(&c.dueHeap).(*cycleCallbackMeta)
		// drop callbacks unregistered or deactivated since they were queued
		if _, ok := c.callbacks[meta.callbackId]; !ok || !meta.active {
			continue
		}
		due = append(due, meta)
	}
	return due
}

// execute runs a single drained callback and returns whether it did work. It
// rechecks registration and active state under the lock, since the entry can be
// unregistered or deactivated between drainDue and here (it is no longer in the
// heap, so those paths cannot remove it).
func (c *cycleCallbackGroup) execute(meta *cycleCallbackMeta, shouldAbort ShouldAbortCallback) bool {
	c.Lock()
	if _, ok := c.callbacks[meta.callbackId]; !ok || !meta.active {
		c.Unlock()
		return false
	}
	// callback active, mark as running
	runningCtx, cancel := context.WithCancel(context.Background())
	meta.runningCtx = runningCtx
	meta.started = time.Now()
	c.Unlock()

	executed := false
	func() {
		// cancel called in recover, regardless of panic occurred or not
		defer c.recover(meta.customId, cancel)
		executed = meta.cycleCallback(func() bool {
			if shouldAbort() {
				return true
			}

			c.Lock()
			defer c.Unlock()

			return meta.shouldAbort
		})

		if meta.intervals != nil {
			if executed {
				meta.intervals.Reset()
			} else {
				meta.intervals.Advance()
			}
		}
	}()
	return executed
}

// repushDue returns a drained callback to the heap. It drops the callback if it
// was unregistered or deactivated in the meantime, and skips a re-push if a
// concurrent activate already re-queued it (heapIndex tracks a single membership).
func (c *cycleCallbackGroup) repushDue(meta *cycleCallbackMeta) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.callbacks[meta.callbackId]; !ok || !meta.active || meta.heapIndex != -1 {
		return
	}
	c.pushDue(meta)
}

func (c *cycleCallbackGroup) recover(callbackCustomId string, cancel context.CancelFunc) {
	if r := recover(); r != nil {
		entsentry.Recover(r)
		enterrors.PrintStack(c.logger)
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
				if meta.heapIndex >= 0 {
					heap.Remove(&c.dueHeap, meta.heapIndex)
				}
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
				if meta.heapIndex >= 0 {
					heap.Remove(&c.dueHeap, meta.heapIndex)
				}
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
	// push only if not already queued, keeping each callback in the heap at most once
	if meta.heapIndex == -1 {
		c.pushDue(meta)
	}
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
	callbackId    uint32
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
	// next-due time ordering the heap, cached from started (+ intervals.Get()) and
	// recomputed on every state change
	nextDue time.Time
	// position in the group's dueHeap, or -1 when not queued
	heapIndex int
}

// computeNextDue returns the time the callback is next eligible to run. A nil
// interval means "always due", so its next-due time is simply started.
func computeNextDue(meta *cycleCallbackMeta) time.Time {
	if meta.intervals == nil {
		return meta.started
	}
	return meta.started.Add(meta.intervals.Get())
}

// dueHeap is a min-heap of callbacks ordered by nextDue (earliest first). Each
// meta appears at most once; heapIndex tracks its slot for O(log n) heap.Remove.
type dueHeap []*cycleCallbackMeta

func (h dueHeap) Len() int { return len(h) }

func (h dueHeap) Less(i, j int) bool { return h[i].nextDue.Before(h[j].nextDue) }

func (h dueHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *dueHeap) Push(x any) {
	meta := x.(*cycleCallbackMeta)
	meta.heapIndex = len(*h)
	*h = append(*h, meta)
}

func (h *dueHeap) Pop() any {
	old := *h
	n := len(old)
	meta := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	meta.heapIndex = -1
	return meta
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
		fmt.Fprintf(&sb, "%n@%s:%d", f, f, f)
		if i < n-1 {
			sb.WriteString(";")
		}
	}
	return sb.String()
}
