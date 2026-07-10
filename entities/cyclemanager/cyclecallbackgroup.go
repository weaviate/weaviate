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
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
)

// Container for multiple callbacks exposing CycleCallback method acting as single callback.
// Can be provided to CycleManager.
type CycleCallbackGroup interface {
	// Adds CycleCallback method to container
	Register(id string, cycleCallback CycleCallback, options ...RegisterOption) CycleCallbackCtrl
	// Method of CycleCallback acting as single callback for all callbacks added to the container
	CycleCallback(shouldAbort ShouldAbortCallback) bool
}

type cycleCallbackMeta struct {
	// callbackId is the heap and metas-map key; name is the public id string.
	callbackId    uint32
	name          string
	cycleCallback CycleCallback
	// active means the callback is enabled and will be scheduled for future ticks.
	// abort is the cooperative-abort signal set by deactivate/unregister while running.
	active bool
	abort  bool
	// running is true while the callback body is executing. Set and read only under
	// the group lock. Deactivate/Unregister use this to decide whether to wait.
	running bool
	// done is the on-demand wait handle. It is nil unless a Deactivate or Unregister
	// caller needs to wait for an in-flight run; the first such caller creates it,
	// subsequent concurrent callers reuse the same channel. Teardown closes and nils
	// it after setting running=false. All accesses are under the group lock.
	done      chan struct{}
	started   time.Time
	intervals CycleIntervals
	schedGen  uint64
	// cycleShouldAbort holds the current cycle's abort signal, refreshed in runOne
	// before each invocation and read by abortCheck. Written and read by the single
	// goroutine handling this meta, so no lock is needed.
	cycleShouldAbort ShouldAbortCallback
	// abortCheck is the ShouldAbortCallback handed to cycleCallback. Built once in
	// Register and reused every cycle to avoid a per-invocation closure allocation.
	abortCheck ShouldAbortCallback
}

func (m *cycleCallbackMeta) setInactive() { m.active = false; m.abort = false }

func (m *cycleCallbackMeta) setIntervals(intervals CycleIntervals) {
	m.intervals = intervals
	m.started = time.Now().Add(-intervals.Get())
}

type cycleCallbackGroup struct {
	sync.Mutex

	logger        logrus.FieldLogger
	name          string
	routinesLimit int
	// nextCallbackId is monotone and IDs are never reused; reuse could cause a
	// stale dueEntry to match a new meta's schedGen.
	nextCallbackId uint32
	heap           dueHeap
	metas          map[uint32]*cycleCallbackMeta
}

func NewCallbackGroup(id string, logger logrus.FieldLogger, routinesLimit int) CycleCallbackGroup {
	return &cycleCallbackGroup{
		logger:        logger,
		name:          id,
		routinesLimit: routinesLimit,
		heap:          dueHeap{},
		metas:         map[uint32]*cycleCallbackMeta{},
	}
}

// schedule pushes a new heap entry for meta, bumping schedGen so any prior
// entry becomes stale. Caller holds the lock.
func (g *cycleCallbackGroup) schedule(m *cycleCallbackMeta) {
	m.schedGen++
	due := computeNextDue(m)
	g.heap.push(dueEntry{callbackId: m.callbackId, due: due, schedGen: m.schedGen})
}

// reschedule re-queues meta after a tick in which it was not run (abort path).
// Acquires the lock.
func (g *cycleCallbackGroup) reschedule(meta *cycleCallbackMeta) {
	g.Lock()
	defer g.Unlock()

	if g.metas[meta.callbackId] == nil || !meta.active {
		return
	}
	g.schedule(meta)
}

// drainDue pops every entry due by now, discards stale/inactive ones, and
// returns the live metas. Acquires and releases the lock.
func (g *cycleCallbackGroup) drainDue(now time.Time) []*cycleCallbackMeta {
	g.Lock()
	defer g.Unlock()

	nowNanos := now.UnixNano()
	var due []*cycleCallbackMeta
	for len(g.heap) > 0 {
		if g.heap[0].due > nowNanos {
			break
		}
		e := g.heap.pop()
		meta, ok := g.metas[e.callbackId]
		if !ok || e.schedGen != meta.schedGen || !meta.active {
			continue
		}
		due = append(due, meta)
	}
	return due
}

func (g *cycleCallbackGroup) Register(id string, cycleCallback CycleCallback, options ...RegisterOption) CycleCallbackCtrl {
	meta := &cycleCallbackMeta{
		name:          id,
		cycleCallback: cycleCallback,
		active:        true,
		started:       time.Now(),
	}
	for _, option := range options {
		if option != nil {
			option(meta)
		}
	}

	// Build the abort-check closure once here. g and meta are stable for the
	// lifetime of this registration; capturing them now avoids allocating a new
	// closure on every cycleCallback invocation.
	meta.abortCheck = func() bool {
		if meta.cycleShouldAbort() {
			return true
		}
		g.Lock()
		defer g.Unlock()
		return meta.abort
	}

	// Publish the meta and push the first heap entry under the lock. defer ensures
	// the lock is released even if schedule (which calls computeNextDue → Get())
	// panics, so a panic here cannot leave the group locked.
	var callbackId uint32
	func() {
		g.Lock()
		defer g.Unlock()
		callbackId = g.nextCallbackId
		meta.callbackId = callbackId
		g.metas[callbackId] = meta
		g.nextCallbackId++
		g.schedule(meta)
	}()

	return &cycleCallbackCtrl{
		callbackId:       callbackId,
		callbackCustomId: id,
		isActive:         g.isActive,
		activate:         g.activate,
		deactivate:       g.deactivate,
		unregister:       g.unregister,
	}
}

func (g *cycleCallbackGroup) CycleCallback(shouldAbort ShouldAbortCallback) bool {
	due := g.drainDue(time.Now())
	if len(due) == 0 {
		return false
	}
	if g.routinesLimit <= 1 || len(due) == 1 {
		return g.runSequential(due, shouldAbort)
	}
	return g.runParallel(due, shouldAbort)
}

func (g *cycleCallbackGroup) runSequential(due []*cycleCallbackMeta, shouldAbort ShouldAbortCallback) bool {
	anyExecuted := false
	for _, meta := range due {
		if shouldAbort() {
			g.reschedule(meta)
			continue
		}
		if g.runOne(meta, shouldAbort) {
			anyExecuted = true
		}
	}
	return anyExecuted
}

func (g *cycleCallbackGroup) runParallel(due []*cycleCallbackMeta, shouldAbort ShouldAbortCallback) bool {
	var anyExecuted atomic.Bool
	wg := new(sync.WaitGroup)
	ch := make(chan *cycleCallbackMeta)

	limit := min(len(due), g.routinesLimit)
	wg.Add(limit)
	for range limit {
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for meta := range ch {
				if shouldAbort() {
					g.reschedule(meta)
					continue
				}
				if g.runOne(meta, shouldAbort) {
					anyExecuted.Store(true)
				}
			}
		}, g.logger)
	}

	for _, meta := range due {
		ch <- meta
	}
	close(ch)
	wg.Wait()
	return anyExecuted.Load()
}

// beginRun performs the entry check and marks the callback as running, under
// the group lock. Returns false if the callback was deactivated or unregistered
// since drainDue returned it. Kept as a method rather than a closure so the
// entry check stays allocation-free on the dispatch path.
func (g *cycleCallbackGroup) beginRun(meta *cycleCallbackMeta) bool {
	g.Lock()
	defer g.Unlock()

	if g.metas[meta.callbackId] == nil || !meta.active {
		return false
	}
	meta.running = true
	meta.started = time.Now()
	return true
}

// endRun performs teardown under the group lock: clears running, wakes any
// Deactivate/Unregister waiters, and reschedules the callback if still active.
// Kept as a method rather than a closure so teardown stays allocation-free on
// the dispatch path.
func (g *cycleCallbackGroup) endRun(meta *cycleCallbackMeta) {
	g.Lock()
	defer g.Unlock()

	meta.running = false
	if meta.done != nil {
		close(meta.done)
		meta.done = nil
	}
	if g.metas[meta.callbackId] != nil && meta.active {
		g.schedule(meta)
	}
}

// runOne executes meta under the entry-check / run / teardown protocol.
func (g *cycleCallbackGroup) runOne(meta *cycleCallbackMeta, shouldAbort ShouldAbortCallback) bool {
	if !g.beginRun(meta) {
		return false
	}

	executed := false
	func() {
		defer g.recover(meta)
		meta.cycleShouldAbort = shouldAbort
		executed = meta.cycleCallback(meta.abortCheck)
		// Interval update is inside the closure so a panic unwinds past it,
		// leaving the interval unchanged (panic is not "no work done").
		if meta.intervals != nil {
			if executed {
				meta.intervals.Reset()
			} else {
				meta.intervals.Advance()
			}
		}
	}()

	g.endRun(meta)
	return executed
}

func (g *cycleCallbackGroup) recover(meta *cycleCallbackMeta) {
	if r := recover(); r != nil {
		entsentry.Recover(r)
		enterrors.PrintStack(g.logger)
		g.logger.WithFields(logrus.Fields{
			"action":       "cyclemanager",
			"callback_id":  meta.name,
			"callbacks_id": g.name,
			"trace":        trace(),
		}).Errorf("callback panic: %v", r)
	}
}

func (g *cycleCallbackGroup) isActive(callbackId uint32, _ string) bool {
	g.Lock()
	defer g.Unlock()

	if meta, ok := g.metas[callbackId]; ok {
		return meta.active
	}
	return false
}

// activate sets the callback active and pushes a new heap entry under the lock.
// defer ensures the lock is released even if schedule panics.
func (g *cycleCallbackGroup) activate(callbackId uint32, callbackCustomId string) error {
	g.Lock()
	defer g.Unlock()

	meta, ok := g.metas[callbackId]
	if !ok {
		return errorActivateCallback(callbackCustomId, g.name, ErrorCallbackNotFound)
	}
	meta.active = true
	meta.abort = false
	g.schedule(meta)
	return nil
}

// deactivatePrepare performs the decision block for deactivate under the lock.
// Returns the captured done channel and whether the caller must wait for it.
// On needWait==false the callback has already been committed as inactive, or
// was not found (immediateErr carries the result in that case).
func (g *cycleCallbackGroup) deactivatePrepare(callbackId uint32) (meta *cycleCallbackMeta, done chan struct{}, needWait bool, immediateErr error) {
	g.Lock()
	defer g.Unlock()

	m, ok := g.metas[callbackId]
	if !ok {
		return nil, nil, false, ErrorCallbackNotFound
	}
	m.abort = true
	if !m.running {
		m.active = false
		return m, nil, false, nil
	}
	if m.done == nil {
		m.done = make(chan struct{})
	}
	return m, m.done, true, nil
}

// commitDeactivated sets active=false on a meta that just finished running,
// under the group lock. Called after a successful wait on the done channel.
func (g *cycleCallbackGroup) commitDeactivated(meta *cycleCallbackMeta) {
	g.Lock()
	defer g.Unlock()
	meta.active = false
}

func (g *cycleCallbackGroup) deactivate(ctx context.Context, callbackId uint32, callbackCustomId string) error {
	if ctx.Err() != nil {
		return errorDeactivateCallback(callbackCustomId, g.name, ctx.Err())
	}
	meta, done, needWait, immediateErr := g.deactivatePrepare(callbackId)
	if !needWait {
		return errorDeactivateCallback(callbackCustomId, g.name, immediateErr)
	}

	select {
	case <-done:
		g.commitDeactivated(meta)
		return errorDeactivateCallback(callbackCustomId, g.name, nil)
	case <-ctx.Done():
		select {
		case <-done:
			g.commitDeactivated(meta)
			return errorDeactivateCallback(callbackCustomId, g.name, nil)
		default:
			// Timeout: leave active=true, abort=true. The running callback will
			// eventually finish; teardown reschedules it and abort is reset on next
			// Activate call.
			return errorDeactivateCallback(callbackCustomId, g.name, ctx.Err())
		}
	}
}

// unregisterPrepare performs the decision block for unregister under the lock.
// Returns the captured done channel and whether the caller must wait.
// On needWait==false, the meta has already been deleted (or was absent: nil return).
func (g *cycleCallbackGroup) unregisterPrepare(callbackId uint32) (done chan struct{}, needWait bool) {
	g.Lock()
	defer g.Unlock()

	meta, ok := g.metas[callbackId]
	if !ok {
		return nil, false
	}
	meta.abort = true
	if !meta.running {
		delete(g.metas, callbackId)
		return nil, false
	}
	if meta.done == nil {
		meta.done = make(chan struct{})
	}
	return meta.done, true
}

// commitUnregistered deletes the meta from the metas map under the group lock.
// Idempotent: deleting an absent key is safe in Go.
func (g *cycleCallbackGroup) commitUnregistered(callbackId uint32) {
	g.Lock()
	defer g.Unlock()
	delete(g.metas, callbackId)
}

func (g *cycleCallbackGroup) unregister(ctx context.Context, callbackId uint32, callbackCustomId string) error {
	if ctx.Err() != nil {
		return errorUnregisterCallback(callbackCustomId, g.name, ctx.Err())
	}
	done, needWait := g.unregisterPrepare(callbackId)
	if !needWait {
		return nil
	}

	select {
	case <-done:
		g.commitUnregistered(callbackId)
		return nil
	case <-ctx.Done():
		select {
		case <-done:
			g.commitUnregistered(callbackId)
			return nil
		default:
			// Timeout: leave meta in map with active=true, abort=true. The callback
			// finishes eventually; teardown reschedules it. Caller can retry later.
			return errorUnregisterCallback(callbackCustomId, g.name, ctx.Err())
		}
	}
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
