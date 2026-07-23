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

// CycleCallbackGroup holds multiple callbacks and exposes them as one combined
// CycleCallback that a CycleManager drives. Registered callbacks can be
// individually activated, deactivated, and unregistered while the group runs.
//
// CycleCallback must be driven by a single consumer — one CycleManager, one tick
// at a time. Scheduling reserves each due callback by popping it from an internal
// heap, so two concurrent CycleCallback calls could otherwise reserve and run the
// same callback twice. Every group has exactly one CycleManager, so this holds; it
// is a required precondition, not something the group synchronizes against.
type CycleCallbackGroup interface {
	// Register adds a CycleCallback to the container, returning a controller to
	// activate, deactivate, or unregister it.
	Register(id string, cycleCallback CycleCallback, options ...RegisterOption) CycleCallbackCtrl
	// CycleCallback runs every callback due this tick, acting as a single callback
	// for the whole container. It must be called by a single consumer (see the
	// type doc); concurrent calls are not supported.
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
	// subsequent concurrent callers reuse the same channel. endRun closes and nils
	// it after setting running=false. All accesses are under the group lock.
	done      chan struct{}
	started   time.Time
	intervals CycleIntervals
	schedGen  uint64
	// cycleShouldAbort holds the current run's manager-stop signal. Written in
	// beginRun, cleared in endRun, and read by abortCheck — all under the group
	// lock, so a retained abortCheck (e.g. an abort-watcher goroutine still in
	// flight after the callback returns) reads it safely across ticks.
	cycleShouldAbort ShouldAbortCallback
	// abortCheck is the ShouldAbortCallback handed to the callback. Built once in
	// Register and reused every run, so the dispatch path allocates no per-run
	// closure. It reads cycleShouldAbort and abort under the lock.
	abortCheck ShouldAbortCallback
}

func (m *cycleCallbackMeta) setInactive() { m.active = false }

func (m *cycleCallbackMeta) setIntervals(intervals CycleIntervals) {
	m.intervals = intervals
	m.started = time.Now().Add(-intervals.Get())
}

type cycleCallbackGroup struct {
	sync.Mutex

	logger        logrus.FieldLogger
	name          string
	routinesLimit int
	// epoch is a fixed reference captured at construction. Due-times are stored as
	// nanoseconds relative to it, so scheduling and draining compare via the
	// monotonic clock and stay correct across wall-clock jumps.
	epoch time.Time
	// nextCallbackId is monotone and IDs are never reused; reuse could make a stale
	// dueEntry match a new meta's schedGen. uint32 suffices: wrapping needs 2^32
	// registrations in one process, far beyond any real lifetime.
	nextCallbackId uint32
	heap           dueHeap
	metas          map[uint32]*cycleCallbackMeta
}

func NewCallbackGroup(id string, logger logrus.FieldLogger, routinesLimit int) CycleCallbackGroup {
	return &cycleCallbackGroup{
		logger:        logger,
		name:          id,
		routinesLimit: routinesLimit,
		epoch:         time.Now(),
		heap:          dueHeap{},
		metas:         map[uint32]*cycleCallbackMeta{},
	}
}

// computeNextDue returns when m is next eligible to run, as nanoseconds relative
// to the group epoch.
func (g *cycleCallbackGroup) computeNextDue(m *cycleCallbackMeta) int64 {
	if m.intervals == nil {
		return m.started.Sub(g.epoch).Nanoseconds()
	}
	return m.started.Add(m.intervals.Get()).Sub(g.epoch).Nanoseconds()
}

// schedule pushes a new heap entry for meta, bumping schedGen so any prior
// entry becomes stale. Caller holds the lock.
func (g *cycleCallbackGroup) schedule(m *cycleCallbackMeta) {
	m.schedGen++
	due := g.computeNextDue(m)
	g.heap.push(dueEntry{callbackId: m.callbackId, due: due, schedGen: m.schedGen})

	// Stale entries are otherwise reclaimed only lazily on pop, so churn on a
	// far-future callback can pile them up. Compact once the heap grows past the
	// live-entry ceiling to keep heap size, and pop cost, bounded by the
	// registered-callback count.
	if len(g.heap) > heapCompactThreshold(len(g.metas)) {
		g.compactHeap()
	}
}

// heapCompactThreshold is the heap size schedule tolerates before compacting,
// given the live-callback count. The factor bounds stale-entry growth relative to
// the live count; the slack keeps small groups from compacting on nearly every
// schedule.
func heapCompactThreshold(liveCount int) int {
	const (
		factor = 2
		slack  = 8
	)
	return factor*liveCount + slack
}

// compactHeap drops every stale entry — callback gone, or schedGen superseded —
// keeping the single current entry per registered callback. It removes only
// provably-dead entries, so every survivor is one drainDue would still act on.
// Caller holds the lock.
func (g *cycleCallbackGroup) compactHeap() {
	g.heap.compact(func(e dueEntry) bool {
		meta, ok := g.metas[e.callbackId]
		return ok && e.schedGen == meta.schedGen
	})
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

	nowNanos := now.Sub(g.epoch).Nanoseconds()
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

	// Built once and reused every run to avoid a per-run closure allocation. g and
	// meta are stable for this registration's lifetime. Reads cycleShouldAbort and
	// abort under the lock; safe to call even after the run returns.
	meta.abortCheck = func() bool {
		g.Lock()
		shouldAbort, abort := meta.cycleShouldAbort, meta.abort
		g.Unlock()
		return abort || (shouldAbort != nil && shouldAbort())
	}

	g.Lock()
	defer g.Unlock()

	callbackId := g.nextCallbackId
	meta.callbackId = callbackId
	g.metas[callbackId] = meta
	g.nextCallbackId++
	// Publish the meta before scheduling so compaction (which schedule may
	// trigger) keeps the entry it just pushed. An inactive callback gets no entry
	// — Activate schedules it if and when it is enabled.
	//
	// schedule reaches intervals.Get via computeNextDue. Get must not panic (see
	// CycleIntervals); a misbehaving implementation that did would unwind Register
	// with the meta already in g.metas but no controller returned — orphaned and
	// un-unregisterable. The in-tree intervals are index-bounded and cannot panic.
	if meta.active {
		g.schedule(meta)
	}

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
	// Buffered to len(due) so the dispatch loop never blocks on a send: if every
	// worker exits early (a panic outside runOne's recover), the sends still land
	// in the buffer and this goroutine finishes instead of hanging forever.
	ch := make(chan *cycleCallbackMeta, len(due))

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

	// A worker that exits abnormally (e.g. runtime.Goexit in a callback) stops
	// draining ch; if every worker exits, metas remain buffered. They were popped
	// from the heap by drainDue, so reschedule any left over to avoid losing them.
	for meta := range ch {
		g.reschedule(meta)
	}
	return anyExecuted.Load()
}

// beginRun performs the entry check and marks the callback as running, under
// the group lock. Returns false if the callback was deactivated or unregistered
// since drainDue returned it, or is already running. The running check guards the
// single-consumer contract (see CycleCallbackGroup): a concurrent CycleCallback
// plus a Deactivate+Activate could otherwise re-queue and reserve the same meta
// twice.
func (g *cycleCallbackGroup) beginRun(meta *cycleCallbackMeta, shouldAbort ShouldAbortCallback) bool {
	g.Lock()
	defer g.Unlock()

	if g.metas[meta.callbackId] == nil || !meta.active || meta.running {
		return false
	}
	meta.running = true
	meta.started = time.Now()
	meta.cycleShouldAbort = shouldAbort
	return true
}

// endRun runs under the group lock after a callback returns: it clears running,
// wakes any Deactivate/Unregister waiters, and reschedules the callback if it is
// still active.
func (g *cycleCallbackGroup) endRun(meta *cycleCallbackMeta) {
	g.Lock()
	defer g.Unlock()

	meta.running = false
	meta.cycleShouldAbort = nil
	if meta.done != nil {
		close(meta.done)
		meta.done = nil
	}
	if g.metas[meta.callbackId] != nil && meta.active {
		g.schedule(meta)
	}
}

// runOne executes meta under the entry-check / run / endRun protocol.
func (g *cycleCallbackGroup) runOne(meta *cycleCallbackMeta, shouldAbort ShouldAbortCallback) bool {
	if !g.beginRun(meta, shouldAbort) {
		return false
	}
	// endRun must run on every unwind path: if a panic escapes g.recover or the
	// callback calls runtime.Goexit, meta.running stays set (never rescheduled) and
	// meta.done stays unclosed (Deactivate/Unregister wait out their full deadline).
	// The interval update completes inside the inner closure before this defer fires.
	//
	// This keeps meta bookkeeping consistent, but only the parallel path contains a
	// Goexit: it kills a disposable worker, and runParallel reschedules any metas
	// left buffered. In sequential mode runOne runs on the CycleManager's driving
	// goroutine, so a Goexit there ends that manager's tick loop — as it did before
	// this rewrite. A panic, unlike Goexit, is caught by g.recover on both paths.
	defer g.endRun(meta)

	executed := false
	func() {
		defer g.recover(meta)
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

// activate clears any pending abort and, if the callback was inactive, marks it
// active and schedules it. An already-active callback is left as-is: it is either
// running (endRun reschedules it) or already holds a live entry, so re-scheduling
// would race computeNextDue's interval read against the running callback's unlocked
// Reset/Advance and leave a stale entry behind.
func (g *cycleCallbackGroup) activate(callbackId uint32, callbackCustomId string) error {
	g.Lock()
	defer g.Unlock()

	meta, ok := g.metas[callbackId]
	if !ok {
		return errorActivateCallback(callbackCustomId, g.name, ErrorCallbackNotFound)
	}
	meta.abort = false
	if !meta.active {
		g.schedule(meta)
		meta.active = true
	}
	return nil
}

// tryCommitIdle is one under-lock attempt of the commitIdle protocol. It sets
// abort, then reports one of three outcomes: the callback is absent
// (needWait=false, err=notFound); it is not running, so commit is applied and the
// operation is complete (needWait=false, err=nil); or it is running, so the
// lazily-created done channel is returned to wait on (needWait=true).
func (g *cycleCallbackGroup) tryCommitIdle(callbackId uint32, commit func(*cycleCallbackMeta), notFound error,
) (done chan struct{}, needWait bool, err error) {
	g.Lock()
	defer g.Unlock()

	meta, ok := g.metas[callbackId]
	if !ok {
		return nil, false, notFound
	}
	meta.abort = true
	if !meta.running {
		commit(meta)
		return nil, false, nil
	}
	if meta.done == nil {
		meta.done = make(chan struct{})
	}
	return meta.done, true, nil
}

// commitIdle drives the try/wait/re-check protocol shared by deactivate and
// unregister. One run's done channel is not enough: endRun reschedules a
// still-active callback, so a fresh tick can start another run while we wait. It
// retries tryCommitIdle after each finished run; commit is applied only once
// tryCommitIdle observes the callback not running. A ctx deadline hit while
// waiting returns ctx.Err(), but an already-finished run takes priority so a
// completed operation is never reported as a timeout. Every result passes through
// wrap (which maps nil to nil).
func (g *cycleCallbackGroup) commitIdle(ctx context.Context, callbackId uint32,
	commit func(*cycleCallbackMeta), notFound error, wrap func(error) error,
) error {
	if ctx.Err() != nil {
		return wrap(ctx.Err())
	}

	for {
		done, needWait, err := g.tryCommitIdle(callbackId, commit, notFound)
		if !needWait {
			return wrap(err)
		}

		select {
		case <-done:
			// re-check under the lock on the next iteration
		case <-ctx.Done():
			select {
			case <-done:
				// re-check under the lock on the next iteration
			default:
				// Timeout: leave the callback in place (abort stays set). The running
				// callback finishes eventually and endRun reschedules it; the caller
				// can retry, and Activate resets abort.
				return wrap(ctx.Err())
			}
		}
	}
}

func (g *cycleCallbackGroup) deactivate(ctx context.Context, callbackId uint32, callbackCustomId string) error {
	return g.commitIdle(ctx, callbackId,
		func(m *cycleCallbackMeta) { m.active = false },
		ErrorCallbackNotFound,
		func(err error) error { return errorDeactivateCallback(callbackCustomId, g.name, err) },
	)
}

func (g *cycleCallbackGroup) unregister(ctx context.Context, callbackId uint32, callbackCustomId string) error {
	return g.commitIdle(ctx, callbackId,
		func(m *cycleCallbackMeta) {
			delete(g.metas, callbackId)
			// With no live metas, schedule never runs again, so its size-based
			// compaction can't reclaim the entries left behind. Drop the heap now,
			// releasing every stale entry and the backing array.
			if len(g.metas) == 0 {
				g.heap = dueHeap{}
			}
		},
		nil, // an absent callback is a no-op success for unregister
		func(err error) error { return errorUnregisterCallback(callbackCustomId, g.name, err) },
	)
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
