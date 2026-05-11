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

package db

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/replication"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const (
	defaultAsyncReplicationSchedulerWorkers        = 10
	defaultAsyncReplicationHashtreeInitConcurrency = 100
	maxMaxWorkers                                  = 100

	// asyncRepRebuildBaseBackoff is the wait after the first consecutive rebuild
	// failure. Each subsequent failure doubles the wait up to asyncRepRebuildMaxBackoff.
	asyncRepRebuildBaseBackoff = 30 * time.Second
	asyncRepRebuildMaxBackoff  = 30 * time.Minute
)

// ErrSchedulerClosed is returned by Register / Deregister when called after Close.
var ErrSchedulerClosed = errors.New("AsyncReplicationScheduler is closed")

// asyncRepRebuildBackoffDuration returns the backoff to apply after
// consecutiveFailures consecutive rebuild failures.
// Schedule: 30 s, 60 s, 2 m, 4 m, 8 m, 16 m, 30 m (capped).
func asyncRepRebuildBackoffDuration(consecutiveFailures uint32) time.Duration {
	if consecutiveFailures == 0 {
		return 0
	}
	exp := consecutiveFailures - 1
	if exp > 10 { // 30s<<10 = ~8.5h >> maxBackoff; cap the shift to avoid overflow
		exp = 10
	}
	d := asyncRepRebuildBaseBackoff << exp
	if d > asyncRepRebuildMaxBackoff {
		d = asyncRepRebuildMaxBackoff
	}
	return d
}

// asyncSchedulerEntry is the per-shard state tracked inside the scheduler heap.
type asyncSchedulerEntry struct {
	shard     *Shard
	nextRunAt time.Time
	inFlight  bool // true while a worker is executing runHashbeatCycle
	heapIdx   int  // maintained by the heap for O(log n) Fix/Remove
	// seq is a monotonically-increasing counter assigned every time the entry
	// is pushed onto the heap. Entries with the same nextRunAt are served in
	// the order they were enqueued (FIFO within a tie).
	seq uint64
}

// asyncSchedulerHeap is a min-heap ordered by nextRunAt with FIFO tie-breaking
// on seq so entries that share the same deadline are served in arrival order.
// It implements heap.Interface.
type asyncSchedulerHeap []*asyncSchedulerEntry

func (h asyncSchedulerHeap) Len() int { return len(h) }
func (h asyncSchedulerHeap) Less(i, j int) bool {
	if h[i].nextRunAt.Equal(h[j].nextRunAt) {
		return h[i].seq < h[j].seq // FIFO tie-break: lower seq = enqueued earlier
	}
	return h[i].nextRunAt.Before(h[j].nextRunAt)
}

func (h asyncSchedulerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *asyncSchedulerHeap) Push(x any) {
	e := x.(*asyncSchedulerEntry)
	e.heapIdx = len(*h)
	*h = append(*h, e)
}

func (h *asyncSchedulerHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	e.heapIdx = -1
	return e
}

// asyncSchedulerResult carries the outcome of a single runHashbeatCycle call
// back to the dispatcher goroutine.
type asyncSchedulerResult struct {
	entry      *asyncSchedulerEntry
	propagated bool
	err        error
	// cfg is the effective config snapshot used for this cycle, pre-read in
	// the worker. onResultLocked uses it to avoid acquiring
	// asyncReplicationRWMux.RLock() while sched.mu is held.
	cfg AsyncReplicationConfig
	// ctx is the per-shard async replication context captured under
	// asyncReplicationRWMux.RLock in runEntry. Carrying it here lets
	// nextInterval check whether the cycle's error was caused by context
	// cancellation without re-reading the shard field (which would race
	// with a concurrent write in initAsyncReplication).
	ctx context.Context
}

// addRequest is sent on addCh to register a new shard. The dispatcher sets
// processed=true iff onAddLocked actually ran (i.e. closed was still false
// when the dispatcher dequeued the request). Register reads it after ackCh
// closes to decide whether to report success or ErrSchedulerClosed.
type addRequest struct {
	shard     *Shard
	ackCh     chan struct{}
	processed *atomic.Bool
}

// removeRequest is sent on removeCh to deregister a shard. The dispatcher
// sets processed=true iff onRemoveLocked actually ran. Deregister reads it
// after ackCh closes for the same reason as addRequest.
type removeRequest struct {
	shard     *Shard
	ackCh     chan struct{}
	processed *atomic.Bool
}

// asyncReplicationSchedulerMetrics holds the DB-level Prometheus metrics for
// the scheduler. All are optional: if promMetrics is nil, all methods are
// no-ops.
type asyncReplicationSchedulerMetrics struct {
	monitoring       bool
	workersActive    prometheus.Gauge
	shardsRegistered prometheus.Gauge
	// queueDepth is the number of heap entries not currently in-flight.
	queueDepth prometheus.Gauge
	// workerPoolSize is the current target worker pool size.
	workerPoolSize prometheus.Gauge
}

func newAsyncReplicationSchedulerMetrics(prom *monitoring.PrometheusMetrics) (asyncReplicationSchedulerMetrics, error) {
	if prom == nil {
		return asyncReplicationSchedulerMetrics{}, nil
	}
	m := asyncReplicationSchedulerMetrics{monitoring: true}

	var err error
	m.workersActive, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "async_replication_scheduler_workers_active",
			Help:      "Number of scheduler worker goroutines currently executing a hashbeat cycle",
		}),
	)
	if err != nil {
		return m, err
	}

	m.shardsRegistered, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "async_replication_scheduler_shards_registered",
			Help:      "Number of shards currently registered with the async replication scheduler",
		}),
	)
	if err != nil {
		return m, err
	}

	m.queueDepth, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "async_replication_scheduler_queue_depth",
			Help:      "Number of shards waiting in the scheduler heap (not in-flight).",
		}),
	)
	if err != nil {
		return m, err
	}

	m.workerPoolSize, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "weaviate",
			Name:      "async_replication_scheduler_worker_pool_size",
			Help:      "Current target size of the scheduler worker pool.",
		}),
	)
	if err != nil {
		return m, err
	}

	return m, err
}

func (m *asyncReplicationSchedulerMetrics) incWorkersActive() {
	if m.monitoring {
		m.workersActive.Inc()
	}
}

func (m *asyncReplicationSchedulerMetrics) decWorkersActive() {
	if m.monitoring {
		m.workersActive.Dec()
	}
}

func (m *asyncReplicationSchedulerMetrics) incShardsRegistered() {
	if m.monitoring {
		m.shardsRegistered.Inc()
	}
}

func (m *asyncReplicationSchedulerMetrics) decShardsRegistered() {
	if m.monitoring {
		m.shardsRegistered.Dec()
	}
}

func (m *asyncReplicationSchedulerMetrics) setQueueDepth(n int) {
	if m.monitoring {
		m.queueDepth.Set(float64(n))
	}
}

func (m *asyncReplicationSchedulerMetrics) setWorkerPoolSize(n int) {
	if m.monitoring {
		m.workerPoolSize.Set(float64(n))
	}
}

// AsyncReplicationScheduler is a DB-level scheduler that dispatches hashbeat
// cycles to a fixed-size worker pool. It replaces the former two-goroutines-
// per-shard model (hashbeater + trigger) with a single pool of N workers,
// one dispatcher goroutine, and one worker-count watcher goroutine — scaling
// O(1) with respect to tenant count instead of O(tenants).
//
// Lifecycle:
//
//	scheduler, err := NewAsyncReplicationScheduler(ctx, replicationCfg, prom, logger)
//	if err != nil { /* handle */ }
//	// ... register/deregister shards as they are created/destroyed ...
//	scheduler.Close()
type AsyncReplicationScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Lock ordering across all mutexes in this struct and related shard state:
	//   1. workersMu (if needed) — shortest critical section, protects targetWorkers.
	//   2. mu — protects heap, entries, nextSeq. Must NEVER be held while
	//      acquiring a shard's asyncReplicationRWMux, because the init-scan
	//      goroutine (spawned by initAsyncReplication) holds asyncReplicationRWMux
	//      .RLock while calling Deregister — which blocks on the dispatcher that
	//      needs mu. Holding both in the wrong order would deadlock.
	//      Close() enforces this by releasing mu before iterating stuck-shard
	//      contexts (see the comment in Close()).
	// Current code never holds workersMu and mu simultaneously, but the rule
	// prevents a future deadlock if that changes.

	// mu protects heap, entries, and nextSeq. Held only for brief heap operations.
	mu      sync.Mutex
	h       asyncSchedulerHeap
	entries map[*Shard]*asyncSchedulerEntry
	// nextSeq is incremented each time an entry is pushed onto the heap and
	// stamped into entry.seq for FIFO tie-breaking within equal nextRunAt values.
	nextSeq uint64

	// targetWorkers is the desired worker-pool size, adjusted dynamically by
	// adjustWorkers. Protected by workersMu.
	targetWorkers int
	workersMu     sync.Mutex

	// scaleDownCh receives one token per worker that should exit.
	// Sized to maxMaxWorkers: at most one token per possible worker.
	scaleDownCh chan struct{}

	// maxWorkersConfig is the runtime-dynamic worker-count ceiling.
	// workerWatcher polls it every 30 s and calls adjustWorkers.
	maxWorkersConfig *configRuntime.DynamicValue[int]

	// hashtreeInitSem gates how many hashtree initializations run their full
	// on-disk object scan concurrently. On restart with many tenants, each
	// shard without a persisted hashtree spawns one goroutine; without this
	// limit 10k concurrent scans would thrash disk and spike RSS.
	hashtreeInitSem *semaphore.Weighted

	// workCh feeds runHashbeatCycle calls to the worker pool.
	// Buffered to maxMaxWorkers so dispatchDueLocked completes the send in O(1)
	// without a goroutine rendezvous while sched.mu is held. The "all workers
	// busy" backpressure fires when the buffer is full (default branch in
	// dispatchDueLocked) rather than when no worker goroutine is immediately
	// waiting, which is semantically equivalent under steady-state scheduling.
	workCh chan *asyncSchedulerEntry

	// resultCh receives results from workers; buffered so workers never block.
	resultCh chan asyncSchedulerResult

	// addCh / removeCh are used to register / deregister shards synchronously
	// from the caller's goroutine through the dispatcher's select loop.
	addCh    chan addRequest
	removeCh chan removeRequest

	// asyncReplicationDisabled mirrors the runtime config flag. When true,
	// dispatchDueLocked skips all dispatching and timeUntilNextLocked sleeps
	// for 30 s so the dispatcher wakes up promptly once the flag is cleared.
	asyncReplicationDisabled *configRuntime.DynamicValue[bool]

	metrics asyncReplicationSchedulerMetrics
	logger  logrus.FieldLogger

	// closed is set to true once at the top of Close() (before cancel), never
	// reset. Read by Register/Deregister and the dispatcher's addCh branch to
	// reject post-Close calls deterministically.
	closed atomic.Bool
}

// NewAsyncReplicationScheduler creates and starts a scheduler bound to the
// given context and replication configuration. The dispatcher, worker-count
// watcher, and worker-pool goroutines are launched before this function
// returns; the scheduler is ready to accept Register/Deregister calls.
// prom may be nil, in which case all Prometheus metrics are no-ops.
// Returns an error only if Prometheus metric registration fails.
//
// Note: GlobalConfig.AsyncReplicationHashtreeInitConcurrency is read once at
// construction time to size the internal init semaphore; changing its DynamicValue
// after construction has no effect without creating a new scheduler.
func NewAsyncReplicationScheduler(
	ctx context.Context,
	replicationCfg replication.GlobalConfig,
	prom *monitoring.PrometheusMetrics,
	logger logrus.FieldLogger,
) (*AsyncReplicationScheduler, error) {
	// Guard against nil DynamicValue pointers that arise when GlobalConfig is
	// zero-valued (e.g. in unit/integration tests that don't configure
	// replication). A nil pointer's Get() returns the zero value for its type
	// which for int is 0 — treated as "use the default worker count".
	maxWorkersConfig := replicationCfg.AsyncReplicationSchedulerWorkers
	if maxWorkersConfig == nil {
		maxWorkersConfig = configRuntime.NewDynamicValue(defaultAsyncReplicationSchedulerWorkers)
	}
	workers := maxWorkersConfig.Get()
	if workers <= 0 {
		workers = defaultAsyncReplicationSchedulerWorkers
	}
	// Remember the requested value before capping so we can warn after the
	// logger is resolved below.
	requestedWorkers := workers
	if workers > maxMaxWorkers {
		// Cap at maxMaxWorkers to match the resultCh buffer size (maxMaxWorkers*2)
		// and keep invariants consistent with adjustWorkers.
		workers = maxMaxWorkers
	}

	hashtreeInitConcurrencyConfig := replicationCfg.AsyncReplicationHashtreeInitConcurrency
	if hashtreeInitConcurrencyConfig == nil {
		hashtreeInitConcurrencyConfig = configRuntime.NewDynamicValue(defaultAsyncReplicationHashtreeInitConcurrency)
	}
	hashtreeInitConcurrency := hashtreeInitConcurrencyConfig.Get()
	if hashtreeInitConcurrency <= 0 {
		hashtreeInitConcurrency = defaultAsyncReplicationHashtreeInitConcurrency
	}

	asyncReplicationDisabled := replicationCfg.AsyncReplicationDisabled
	if asyncReplicationDisabled == nil {
		asyncReplicationDisabled = configRuntime.NewDynamicValue(false)
	}

	if logger == nil {
		logger = logrus.StandardLogger()
	}

	if requestedWorkers > maxMaxWorkers {
		logger.
			WithField("action", "async_replication_scheduler_new").
			WithField("requested", requestedWorkers).
			WithField("effective", maxMaxWorkers).
			Warn("async replication scheduler workers capped at maxMaxWorkers")
	}

	sctx, cancel := context.WithCancel(ctx)

	m, err := newAsyncReplicationSchedulerMetrics(prom)
	if err != nil {
		cancel()
		return nil, err
	}

	s := &AsyncReplicationScheduler{
		ctx:              sctx,
		cancel:           cancel,
		h:                make(asyncSchedulerHeap, 0),
		entries:          make(map[*Shard]*asyncSchedulerEntry),
		targetWorkers:    workers,
		maxWorkersConfig: maxWorkersConfig,
		scaleDownCh:      make(chan struct{}, maxMaxWorkers),
		workCh:           make(chan *asyncSchedulerEntry, maxMaxWorkers),
		// Buffer is sized to maxMaxWorkers*2 (the hard ceiling on the pool)
		// so workers never block even if adjustWorkers scales the pool to its
		// maximum.
		resultCh:                 make(chan asyncSchedulerResult, maxMaxWorkers*2),
		addCh:                    make(chan addRequest),
		removeCh:                 make(chan removeRequest),
		asyncReplicationDisabled: asyncReplicationDisabled,
		hashtreeInitSem:          semaphore.NewWeighted(int64(hashtreeInitConcurrency)),
		metrics:                  m,
		logger:                   logger,
	}
	heap.Init(&s.h)

	s.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer s.wg.Done()
		s.dispatcher()
	}, s.logger)

	s.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer s.wg.Done()
		s.workerWatcher()
	}, s.logger)

	for range s.targetWorkers {
		s.wg.Add(1)
		enterrors.GoWrapper(func() {
			defer s.wg.Done()
			s.worker()
		}, s.logger)
	}
	s.metrics.setWorkerPoolSize(s.targetWorkers)

	return s, nil
}

// acquireHashtreeInitSlot blocks until a slot in the init-scan semaphore is
// available or ctx is cancelled. Returns an error if ctx is done first.
func (sched *AsyncReplicationScheduler) acquireHashtreeInitSlot(ctx context.Context) error {
	return sched.hashtreeInitSem.Acquire(ctx, 1)
}

// releaseHashtreeInitSlot returns a slot to the init-scan semaphore.
func (sched *AsyncReplicationScheduler) releaseHashtreeInitSlot() {
	sched.hashtreeInitSem.Release(1)
}

// Close shuts down the scheduler and waits for all goroutines to exit.
//
// Recommended shutdown sequence to bound latency:
//  1. Call mayStopAsyncReplication (or disableAsyncReplication followed by
//     asyncRepWg.Wait()) on every registered shard before calling Close.
//     This cancels each shard's per-shard context via asyncReplicationCancelFunc,
//     which causes any in-flight hashbeat RPC to abort promptly rather than
//     waiting for the full diffPerNodeTimeout.
//     NOTE: Deregister alone does NOT cancel the per-shard context; it only
//     removes the shard from the scheduler's dispatch queue.
//  2. Call Close(). Workers drain quickly because in-flight RPCs are already
//     cancelled.
//
// Skipping step 1 is safe: Close force-cancels the per-shard context of any
// shard still registered so that in-flight RPCs abort promptly rather than
// blocking until diffPerNodeTimeout. The force-cancellation is a safety net
// for ordering bugs; the recommended sequence above is still preferred.
//
// Close always waits for all goroutines to finish before returning — the 30 s
// mark is a diagnostic threshold only: a warning is logged if shutdown takes
// longer than expected, but Close continues waiting until the pool is fully
// drained.
func (sched *AsyncReplicationScheduler) Close() {
	// Mark closed before any other side effect. Establishes the happens-before
	// for Register/Deregister callers: any caller whose closed.Load() returned
	// false started before Close did anything observable. The dispatcher's
	// addCh branch also reads this flag to drop late-arriving registrations.
	sched.closed.Store(true)

	// Invariant: all shards must be deregistered before Close is called.
	// Log an error so ordering bugs surface early. As a safety net, collect
	// the stuck shards so we can force-cancel their per-shard contexts below,
	// bounding Close even when the caller violates the invariant.
	var stuckShards []*Shard
	sched.mu.Lock()
	if n := len(sched.entries); n > 0 {
		sched.logger.WithField("action", "async_replication_scheduler_close").
			WithField("registered_shards", n).
			Error("Close called with shards still registered; caller must deregister all shards before closing the scheduler")
		stuckShards = make([]*Shard, 0, n)
		for _, entry := range sched.entries {
			stuckShards = append(stuckShards, entry.shard)
		}
	}
	sched.mu.Unlock()

	// Force-cancel each stuck shard's asyncReplicationCancelFunc AFTER releasing
	// sched.mu. We must not hold sched.mu while acquiring asyncReplicationRWMux:
	// initAsyncReplication's goroutine can hold asyncReplicationRWMux.RLock while
	// calling Deregister (sending on removeCh); the dispatcher processes removeCh
	// while needing sched.mu — holding both in the opposite order would deadlock.
	for _, s := range stuckShards {
		s.asyncReplicationRWMux.RLock()
		cancel := s.asyncReplicationCancelFunc
		s.asyncReplicationRWMux.RUnlock()
		if cancel != nil {
			cancel()
		}
	}

	sched.cancel()
	done := make(chan struct{})
	var shutdownWg sync.WaitGroup
	shutdownWg.Add(1)
	enterrors.GoWrapper(func() {
		defer shutdownWg.Done()
		sched.wg.Wait()
		close(done)
	}, sched.logger)
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		sched.logger.WithField("action", "async_replication_scheduler_close").
			Warn("scheduler shutdown still waiting for goroutines after 30s; workers may be blocked on long RPCs")
	}
	// shutdownWg.Wait() is unbounded: Close blocks until every goroutine tracked
	// by sched.wg exits. Workers are context-cancellable so this is normally
	// fast, but callers should be aware there is no hard deadline.
	shutdownWg.Wait()
}

// Register adds a shard to the scheduler for periodic hashbeat dispatching.
// The first hashbeat cycle is triggered immediately.
// Register must not be called while holding asyncReplicationRWMux (see
// the same constraint that applied to startHashbeaterGoroutines).
//
// Strict nil-postcondition: returning nil implies the shard is in the
// scheduler's registry (added by this call or already present from a prior
// Register). The error branch is asymmetric: ErrSchedulerClosed is the only
// possible non-nil return, but it does NOT imply the shard was not added —
// a rare race between the dispatcher acking and ctx cancellation can produce
// a false negative (operation succeeded, error reported). Callers should
// treat any error as "registration is not confirmed" and not rely on it
// implying the absence of side effects.
func (sched *AsyncReplicationScheduler) Register(s *Shard) error {
	if sched.closed.Load() {
		return ErrSchedulerClosed
	}
	ackCh := make(chan struct{})
	var processed atomic.Bool
	req := addRequest{shard: s, ackCh: ackCh, processed: &processed}
	select {
	case sched.addCh <- req:
		// The send completed: the dispatcher has committed to processing the
		// request. Wait for the ack and read processed to decide the outcome.
		// The ctx.Done() branch is a panic backstop: if the dispatcher exits
		// without closing ackCh, this prevents the caller from deadlocking.
		select {
		case <-ackCh:
			if processed.Load() {
				return nil
			}
			// Dispatcher dropped the request because closed was set between
			// the up-front check and the dispatcher dequeueing the request.
			return ErrSchedulerClosed
		case <-sched.ctx.Done():
			return ErrSchedulerClosed
		}
	case <-sched.ctx.Done():
		return ErrSchedulerClosed
	}
}

// Deregister removes a shard from the scheduler. It returns only after the
// dispatcher has confirmed the removal, so subsequent in-flight cycles will
// not be re-enqueued. Callers that need to guarantee no in-flight cycle races
// with subsequent shard state mutations (e.g. rebuildHashtree) must separately
// call shard.asyncRepWg.Wait() after Deregister returns.
//
// Strict nil-postcondition: returning nil implies the shard is not in the
// scheduler's registry after this call. As with Register, the error branch
// is asymmetric — ErrSchedulerClosed does NOT imply the shard was not
// removed (the dispatcher may have removed it before the ctx cancellation
// backstop fired). Callers should treat any non-nil return as "removal not
// confirmed", not as proof of absence of side effects.
func (sched *AsyncReplicationScheduler) Deregister(s *Shard) error {
	if sched.closed.Load() {
		return ErrSchedulerClosed
	}
	ackCh := make(chan struct{})
	var processed atomic.Bool
	req := removeRequest{shard: s, ackCh: ackCh, processed: &processed}
	select {
	case sched.removeCh <- req:
		select {
		case <-ackCh:
			if processed.Load() {
				return nil
			}
			return ErrSchedulerClosed
		case <-sched.ctx.Done():
			return ErrSchedulerClosed
		}
	case <-sched.ctx.Done():
		return ErrSchedulerClosed
	}
}

// ----- dispatcher goroutine -------------------------------------------------

// timeUntilNext returns the time until the next entry is due, locking
// sched.mu around the read of the heap.
func (sched *AsyncReplicationScheduler) timeUntilNext() time.Duration {
	sched.mu.Lock()
	defer sched.mu.Unlock()
	return sched.timeUntilNextLocked()
}

// handleTimerTick processes a timer firing under sched.mu.
func (sched *AsyncReplicationScheduler) handleTimerTick() {
	sched.mu.Lock()
	defer sched.mu.Unlock()
	sched.dispatchDueLocked()
}

// handleResult processes a worker result under sched.mu.
func (sched *AsyncReplicationScheduler) handleResult(result asyncSchedulerResult) {
	sched.mu.Lock()
	defer sched.mu.Unlock()
	sched.onResultLocked(result)
}

// handleAdd processes an addRequest, dropping it if the scheduler is closing.
// processed is set only when onAddLocked actually ran; Register reads it after
// ackCh closes to decide between nil and ErrSchedulerClosed.
//
// The deferred close(req.ackCh) guarantees the caller's <-ackCh unblocks even
// if a future change introduces an early return path. defer ordering is LIFO:
// the deferred Unlock fires before the deferred close, preserving the current
// "release lock before notifying readers" sequencing.
func (sched *AsyncReplicationScheduler) handleAdd(req addRequest) {
	defer close(req.ackCh)
	if sched.closed.Load() {
		return
	}
	sched.mu.Lock()
	defer sched.mu.Unlock()
	sched.onAddLocked(req.shard)
	req.processed.Store(true)
}

// handleRemove mirrors handleAdd: skip when closing so that Deregister returns
// ErrSchedulerClosed (strict postcondition: nil iff the removal definitely
// took effect). Close iterates entries on its own to clean up and force-cancel
// per-shard contexts, so dropping late removeCh requests does not leak entries.
func (sched *AsyncReplicationScheduler) handleRemove(req removeRequest) {
	defer close(req.ackCh)
	if sched.closed.Load() {
		return
	}
	sched.mu.Lock()
	defer sched.mu.Unlock()
	sched.onRemoveLocked(req.shard)
	req.processed.Store(true)
}

func (sched *AsyncReplicationScheduler) dispatcher() {
	timer := time.NewTimer(math.MaxInt64)
	defer timer.Stop()

	resetTimer := func() {
		if !timer.Stop() {
			// Non-blocking drain: when resetTimer is called from the case <-timer.C
			// branch, the channel has already been consumed by the select case and is
			// empty — a blocking drain would deadlock the dispatcher. The default
			// branch handles that safely. When called from other branches (resultCh,
			// addCh, removeCh), the timer may or may not have fired concurrently;
			// the non-blocking select drains it if present and skips if not.
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(sched.timeUntilNext())
	}

	for {
		select {
		case <-sched.ctx.Done():
			return

		case <-timer.C:
			sched.handleTimerTick()
			resetTimer()

		case result := <-sched.resultCh:
			sched.handleResult(result)
			resetTimer()

		case req := <-sched.addCh:
			sched.handleAdd(req)
			resetTimer()

		case req := <-sched.removeCh:
			sched.handleRemove(req)
			resetTimer()
		}
	}
}

// minDispatchInterval is the shortest wait the dispatcher will use when a due
// entry exists and the worker channel has capacity. Prevents a tight loop in
// the brief window between a worker completing and its result arriving on
// resultCh.
const minDispatchInterval = time.Millisecond

// fullChannelBackoff is the wait used when due entries are queued but workCh
// is full (all workers busy). Without this distinction the dispatcher would
// wake every minDispatchInterval (1 ms), attempt a send, get the default
// branch, and spin — ~1000 syscalls/s of pure CPU churn under sustained load.
// 20 ms is short enough that the dispatcher catches a freed worker slot within
// one human-perceptible tick, yet long enough to eliminate the spin.
const fullChannelBackoff = 20 * time.Millisecond

// timeUntilNextLocked returns how long until the next heap entry is due.
// Returns 24h if the heap is empty. Returns 30 s if async replication is
// globally disabled, so the dispatcher wakes up promptly once re-enabled.
// mu must be held.
func (sched *AsyncReplicationScheduler) timeUntilNextLocked() time.Duration {
	if sched.asyncReplicationDisabled.Get() {
		return 30 * time.Second
	}
	if len(sched.h) == 0 {
		return 24 * time.Hour
	}
	d := time.Until(sched.h[0].nextRunAt)
	if d < minDispatchInterval {
		// Entry is due now. If the channel is full (all workers busy), use the
		// larger backoff so the dispatcher yields instead of spinning.
		if len(sched.workCh) == cap(sched.workCh) {
			return fullChannelBackoff
		}
		return minDispatchInterval
	}
	return d
}

// dispatchDueLocked pops entries whose nextRunAt has passed and sends them to
// the worker pool. It stops if the channel is full (all workers busy).
// mu must be held.
func (sched *AsyncReplicationScheduler) dispatchDueLocked() {
	if sched.asyncReplicationDisabled.Get() {
		return
	}
	now := time.Now()
	for len(sched.h) > 0 && !sched.h[0].nextRunAt.After(now) {
		entry := sched.h[0]
		if entry.inFlight {
			// Invariant violation: an inFlight entry must never be in the heap
			// (onResultLocked pops it before clearing inFlight). Reaching here
			// means the scheduler has a bug.
			//
			// Recovery goal is liveness only. Without the pop+reset below,
			// dispatchDueLocked would spin forever over this entry (it is due
			// now and inFlight=true keeps triggering `continue`), holding
			// sched.mu and stalling the entire dispatcher. The asyncRepWg is
			// already balanced: the worker that was dispatched for this entry
			// already called asyncRepWg.Add(1) and will call asyncRepWg.Done()
			// via its defer, so no additional Done() is needed here.
			//
			// This reset does NOT guarantee shard-internal safety: if two
			// concurrent runHashbeatCycle calls were actually running on the same
			// shard, the shard-level state maps accessed there (asyncRepLast*) are
			// not concurrency-safe. Such a bug must be fixed at the source.
			//
			// onResultLocked guards against a double heap insertion via heapIdx.
			heap.Pop(&sched.h)
			entry.inFlight = false
			entry.nextRunAt = time.Now()
			entry.seq = sched.nextSeq
			sched.nextSeq++
			heap.Push(&sched.h, entry)
			if sched.logger != nil {
				sched.logger.
					WithField("action", "async_replication_scheduler").
					WithField("class_name", entry.shard.class.Class).
					WithField("shard_name", entry.shard.name).
					WithField("bug", "inFlight_entry_in_heap").
					Error("dispatchDueLocked: inFlight entry found in heap — invariant violated; resetting and re-queuing (file a bug report)")
			}
			continue
		}

		// Add(1) before sending to workCh so that Deregister+asyncRepWg.Wait()
		// reliably waits for the cycle even if the worker hasn't started yet.
		entry.shard.asyncRepWg.Add(1)

		select {
		case sched.workCh <- entry:
			heap.Pop(&sched.h)
			entry.inFlight = true
		default:
			// workCh buffer full (all workers busy): undo Add(1) and leave entry in heap.
			entry.shard.asyncRepWg.Done()
			sched.metrics.setQueueDepth(len(sched.h))
			return
		}
	}
	sched.metrics.setQueueDepth(len(sched.h))
}

// onResultLocked re-enqueues a shard after a cycle completes (or discards
// the result if the shard has been deregistered).
// mu must be held.
func (sched *AsyncReplicationScheduler) onResultLocked(result asyncSchedulerResult) {
	entry := result.entry
	entry.inFlight = false

	// If the shard is no longer in the registry it was deregistered while
	// the cycle was running — do not re-enqueue.
	if _, ok := sched.entries[entry.shard]; !ok {
		return
	}

	// Guard against double heap insertion: if the dispatchDueLocked inFlight
	// guard already re-pushed this entry (heapIdx >= 0 means it is currently
	// in the heap), skip the push. The entry will be dispatched from its
	// current position; a duplicate push would corrupt the heap invariant.
	if entry.heapIdx >= 0 {
		return
	}

	// Use the config snapshot carried in the result (pre-read by the worker
	// under asyncReplicationRWMux.RLock before the cycle) rather than
	// re-reading it here. This avoids acquiring asyncReplicationRWMux.RLock
	// while sched.mu is held, which would stall the dispatcher if a pending
	// write-lock request (e.g. updateHashtreeOnFlush) is waiting.
	interval := sched.nextInterval(result.cfg, entry, result)
	now := time.Now()

	// Epoch-relative scheduling: compute the next run time from when the
	// cycle was DUE (entry.nextRunAt), not from when it actually completed.
	// Under worker saturation a shard that ran late keeps an earlier deadline
	// and climbs the heap against shards that ran on time, so short-interval
	// (propagating) shards cannot perpetually crowd out long-interval (idle)
	// ones.
	//
	// Safety floor: if the shard is so far behind that base.Add(interval) is
	// still in the past, cap look-back to one interval before now. This
	// prevents a shard delayed by many intervals from monopolising workers
	// with a flood of catch-up cycles.
	base := entry.nextRunAt
	if minBase := now.Add(-interval); base.Before(minBase) {
		base = minBase
	}
	entry.nextRunAt = base.Add(interval)

	// Assign a monotone sequence number for FIFO tie-breaking within equal
	// nextRunAt values.
	entry.seq = sched.nextSeq
	sched.nextSeq++
	heap.Push(&sched.h, entry)
	sched.metrics.setQueueDepth(len(sched.h))
}

// nextInterval computes how long to wait before the next hashbeat cycle.
func (sched *AsyncReplicationScheduler) nextInterval(cfg AsyncReplicationConfig, entry *asyncSchedulerEntry, result asyncSchedulerResult) time.Duration {
	if result.err != nil {
		if result.ctx != nil && errors.Is(result.err, result.ctx.Err()) {
			// Context cancelled (shard shutting down) — use a long interval;
			// the entry will be deregistered shortly.
			return 24 * time.Hour
		}
		return cfg.frequency
	}
	if result.propagated {
		return cfg.frequencyWhilePropagating
	}
	return cfg.frequency
}

// onAddLocked registers a shard with an immediate first dispatch.
// mu must be held.
func (sched *AsyncReplicationScheduler) onAddLocked(s *Shard) {
	if _, ok := sched.entries[s]; ok {
		return // already registered; idempotent
	}
	entry := &asyncSchedulerEntry{
		shard:     s,
		nextRunAt: time.Now(), // run immediately
		seq:       sched.nextSeq,
	}
	sched.nextSeq++
	sched.entries[s] = entry
	heap.Push(&sched.h, entry)
	sched.metrics.incShardsRegistered()
	sched.metrics.setQueueDepth(len(sched.h))
}

// onRemoveLocked removes a shard from the registry (and heap if not in-flight).
// mu must be held.
func (sched *AsyncReplicationScheduler) onRemoveLocked(s *Shard) {
	entry, ok := sched.entries[s]
	if !ok {
		return
	}
	delete(sched.entries, s)
	sched.metrics.decShardsRegistered()
	if !entry.inFlight {
		heap.Remove(&sched.h, entry.heapIdx)
	}
	// If inFlight: the worker will finish and send on resultCh; onResultLocked
	// will see the shard is no longer in entries and will not re-enqueue.
	sched.metrics.setQueueDepth(len(sched.h))
}

// ----- worker goroutines ----------------------------------------------------

func (sched *AsyncReplicationScheduler) worker() {
	for {
		// Prioritise work over scale-down so that a scale-down token and a work
		// item ready simultaneously always results in the work being processed.
		// Without this, Go's random select resolution could terminate a worker
		// while work sits in the channel, temporarily shrinking the effective pool
		// below the target size.
		select {
		case entry, ok := <-sched.workCh:
			if !ok {
				return
			}
			sched.runEntry(entry)
			continue
		default:
		}

		select {
		case <-sched.ctx.Done():
			// Drain any work items that were buffered before the context was cancelled.
			// Each one had asyncRepWg.Add(1) called in dispatchDueLocked; calling
			// Done() here keeps the WaitGroup balanced so that asyncRepWg.Wait()
			// callers (rebuildHashtree, mayStopAsyncReplication) are not stuck forever.
			// Check ok so that a future close(workCh) during shutdown does not panic,
			// and nil-guard entry for the same reason.
			// Clear inFlight so that these entries are not treated as live by any
			// subsequent inspection. Do NOT call decWorkersActive here: incWorkersActive
			// is called inside runEntry, not at dispatch time, so these buffered entries
			// never incremented the gauge — decrementing would drive it negative.
			// Each entry is owned by exactly one worker (workCh sends are 1:1), so
			// touching entry.inFlight here without sched.mu is safe.
			for {
				select {
				case entry, ok := <-sched.workCh:
					if !ok || entry == nil {
						return
					}
					entry.inFlight = false
					entry.shard.asyncRepWg.Done()
				default:
					return
				}
			}
		case <-sched.scaleDownCh:
			return
		case entry, ok := <-sched.workCh:
			if !ok {
				return
			}
			sched.runEntry(entry)
		}
	}
}

// workerWatcher polls maxWorkersConfig every 30 s and calls adjustWorkers so
// that changes to AsyncReplicationSchedulerWorkers take effect at runtime
// without a restart.
func (sched *AsyncReplicationScheduler) workerWatcher() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-ticker.C:
			sched.adjustWorkers(sched.maxWorkersConfig.Get())
		}
	}
}

// adjustWorkers scales the worker pool to n. It spawns new goroutines when
// n > targetWorkers and sends scale-down tokens when n < targetWorkers.
// n <= 0 is treated as defaultAsyncReplicationSchedulerWorkers, matching the
// construction-time behaviour in NewAsyncReplicationScheduler.
// Called by workerWatcher on each tick; may also be called manually for testing.
// Safe to call concurrently; workersMu serialises adjustments.
func (sched *AsyncReplicationScheduler) adjustWorkers(n int) {
	if n <= 0 {
		n = defaultAsyncReplicationSchedulerWorkers
	}
	if n > maxMaxWorkers {
		// Cap at maxMaxWorkers so resultCh (sized maxMaxWorkers*2) is always
		// large enough to hold all pending results without blocking workers.
		if sched.logger != nil {
			sched.logger.
				WithField("action", "async_replication_scheduler_adjust_workers").
				WithField("requested", n).
				WithField("effective", maxMaxWorkers).
				Warn("async replication scheduler workers capped at maxMaxWorkers")
		}
		n = maxMaxWorkers
	}
	sched.workersMu.Lock()
	defer sched.workersMu.Unlock()

	delta := n - sched.targetWorkers
	if delta == 0 {
		return
	}
	sched.targetWorkers = n
	sched.metrics.setWorkerPoolSize(n)

	if delta > 0 {
		// Drain any stale scale-down tokens that were sent by a prior shrink
		// before these new workers start. Without this, a rapid scale-down
		// followed by a scale-up would leave tokens in scaleDownCh that
		// immediately terminate the freshly spawned workers, leaving the pool
		// under-provisioned. workersMu prevents a concurrent shrink from adding
		// new tokens while we drain, but running workers can still consume tokens
		// concurrently, so we must use non-blocking receives to avoid deadlock.
		for drained := false; !drained; {
			select {
			case <-sched.scaleDownCh:
			default:
				drained = true
			}
		}
		for range delta {
			sched.wg.Add(1)
			enterrors.GoWrapper(func() {
				defer sched.wg.Done()
				sched.worker()
			}, sched.logger)
		}
		return
	}
	for range -delta {
		select {
		case sched.scaleDownCh <- struct{}{}:
		case <-sched.ctx.Done():
			return
		}
	}
}

func (sched *AsyncReplicationScheduler) runEntry(entry *asyncSchedulerEntry) {
	sched.metrics.incWorkersActive()
	s := entry.shard

	// Read base config and ctx under a single RLock. asyncRepCtx is set by
	// initAsyncReplication under the write lock; reading it without RLock is a
	// data race. A nil ctx means the shard was registered before
	// initAsyncReplication completed — skip this cycle and let the scheduler
	// re-dispatch at cfg.frequency.
	// Read base config, ctx, and current hashtree height under a single RLock so
	// all three values come from the same snapshot. Two separate RLocks would
	// allow a concurrent initAsyncReplication (WLock) to install a new config
	// and tree between the two acquisitions, producing a cross-snapshot mismatch
	// (e.g. base.height=16 from the old config, currentHTHeight=10 from the new
	// tree) that would trigger a spurious rebuild of an already-correct tree.
	// Height() is lock-free (immutable field set at construction), so calling it
	// under RLock is safe.
	s.asyncReplicationRWMux.RLock()
	base := s.asyncReplicationConfig
	ctx := s.asyncRepCtx
	currentHT := s.hashtree
	var currentHTHeight int
	if currentHT != nil {
		currentHTHeight = currentHT.Height()
	}
	s.asyncReplicationRWMux.RUnlock()

	cfg := base
	if s.index != nil && s.index.globalreplicationConfig != nil {
		cfg = base.Effective(*s.index.globalreplicationConfig)
	}

	// Detect a hashtree height change requested via runtime config. Mark the
	// shard for rebuild; the goroutine is spawned after asyncRepWg.Done() below
	// so that DisableAsyncReplication can safely call Deregister+Wait.
	if currentHT != nil && currentHTHeight != cfg.hashtreeHeight {
		s.asyncRepNeedsRebuild.Store(true)
	}

	needsRebuild := false
	propagated := false
	var err error

	defer func() {
		// Recover any panic from runHashbeatCycle so the worker goroutine stays
		// alive and the dispatcher receives a non-nil error (triggering a proper
		// retry delay) rather than a false success with err=nil.
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in runHashbeatCycle: %v", r)
			sched.logger.WithField("panic", r).Error("recovered from panic in async replication worker")
			enterrors.PrintStack(sched.logger)
		}

		// Done() before the result send: ensures the counter for this hashbeat
		// cycle drops before the dispatcher can re-enqueue and re-dispatch.
		// asyncRepWg may legitimately be >1 when an init goroutine (spawned by
		// initAsyncReplication) overlaps with a hashbeat cycle; the inFlight
		// flag in the scheduler heap enforces "at most one hashbeat cycle at a
		// time", not asyncRepWg alone.
		s.asyncRepWg.Done()

		if needsRebuild {
			// Respect exponential backoff from previous failures: if the last
			// rebuild failed, wait before retrying. Re-arm the flag so the next
			// cycle checks again without spinning.
			if until := s.asyncRepRebuildBackoffUntil.Load(); until > 0 && time.Now().UnixNano() < until {
				s.asyncRepNeedsRebuild.Store(true)
			} else if s.asyncRepRebuildInFlight.CompareAndSwap(false, true) {
				// Serialise rebuilds: CAS false→true before spawning so that at
				// most one rebuild goroutine runs at a time.
				sched.wg.Add(1)
				enterrors.GoWrapper(func() {
					defer sched.wg.Done()
					defer s.asyncRepRebuildInFlight.Store(false)
					sched.rebuildHashtree(s)
				}, sched.logger)
			} else {
				// A rebuild goroutine is already in flight. Re-arm the flag so
				// the next dispatch cycle retries. There is a narrow window where
				// the in-flight goroutine clears asyncRepRebuildInFlight after the
				// CAS above but before this Store, leaving the flag set after the
				// rebuild already completed. That results in one extra rebuild on
				// the next cycle — a benign over-trigger, not a missed rebuild.
				s.asyncRepNeedsRebuild.Store(true)
			}
		}

		sched.metrics.decWorkersActive()

		// Result sent after Done() so the dispatcher cannot dispatch a new
		// cycle (and increment the WaitGroup) before Done() has run.
		//
		// The send is unconditional (no ctx.Done() alternative) because
		// resultCh is buffered to maxMaxWorkers*2, which always exceeds the
		// number of in-flight workers. The send therefore never blocks even
		// when the dispatcher has already exited due to ctx cancellation.
		// Dropping the result via a ctx.Done() branch would leave
		// entry.inFlight=true permanently if the scheduler continued running.
		sched.resultCh <- asyncSchedulerResult{entry: entry, propagated: propagated, err: err, cfg: cfg, ctx: ctx}
	}()

	if ctx == nil {
		// Shard was registered before initAsyncReplication completed. Skip this
		// cycle; the deferred result with err != nil causes the scheduler to
		// re-dispatch at cfg.frequency rather than spinning immediately.
		err = errors.New("shard not yet initialized")
		return
	}
	if ctx.Err() != nil {
		// Per-shard context already cancelled: mayStopAsyncReplication fired
		// between dispatchDueLocked (which incremented asyncRepWg) and this
		// worker starting. Return immediately so asyncRepWg.Done() fires
		// without running any blocking RPCs, allowing asyncRepWg.Wait() in
		// mayStopAsyncReplication to unblock promptly.
		err = ctx.Err()
		return
	}

	propagated, err = s.runHashbeatCycle(ctx, cfg)
	needsRebuild = s.asyncRepNeedsRebuild.CompareAndSwap(true, false)
}

// rebuildHashtree stops async replication on s, waits for any in-flight cycle
// to complete, then restarts with the same base config. enableAsyncReplication
// re-evaluates Effective() internally, so it picks up the new hashtree height
// from the current runtime-config DynamicValues.
func (sched *AsyncReplicationScheduler) rebuildHashtree(s *Shard) {
	// Wait for any cycle that may have been dispatched between our Done() and
	// SetAsyncReplicationState acquiring the write lock.
	s.asyncRepWg.Wait()

	// Guard against re-enabling a shard whose store is already being torn down.
	// performShutdown sets s.shut=true (before calling mayStopAsyncReplication),
	// so this check is a reliable happens-before: if shut is true here, the shard
	// store is either already shut or in the process of shutting down. Calling
	// enableAsyncReplication in that state would race with s.store.Shutdown
	// (initAsyncReplication reads s.store.Bucket) and spawn an init-scan goroutine
	// with context.Background() — a context not cancelled by sched.ctx — letting
	// it outlive scheduler.Close().
	if s.shut.Load() {
		return
	}

	// Use sched.ctx so disable/enable respect scheduler shutdown; context.Background()
	// would allow enableAsyncReplication to spawn a goroutine that leaks after Close().
	if err := s.disableAsyncReplication(sched.ctx); err != nil {
		if sched.logger != nil {
			sched.logger.
				WithField("action", "async_replication_rebuild").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("hashtree rebuild: stop failed: %v", err)
		}
		failures := s.asyncRepRebuildFailures.Add(1)
		s.asyncRepRebuildBackoffUntil.Store(time.Now().Add(asyncRepRebuildBackoffDuration(failures)).UnixNano())
		s.asyncRepNeedsRebuild.Store(true)
		return
	}

	// Guard against re-enabling on a shutting-down scheduler: if Close() was
	// called between disableAsyncReplication and here, enableAsyncReplication
	// would spawn an init-scan goroutine with no context to cancel it.
	if sched.ctx.Err() != nil {
		return
	}

	s.asyncReplicationRWMux.RLock()
	baseCfg := s.asyncReplicationConfig
	s.asyncReplicationRWMux.RUnlock()

	// Double-check after reading baseCfg: Close() could have fired in the window
	// between the first ctx.Err() check and here.
	if sched.ctx.Err() != nil {
		return
	}

	if err := s.enableAsyncReplication(sched.ctx, baseCfg); err != nil {
		if sched.logger != nil {
			sched.logger.
				WithField("action", "async_replication_rebuild").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("hashtree rebuild: start failed: %v", err)
		}
		failures := s.asyncRepRebuildFailures.Add(1)
		s.asyncRepRebuildBackoffUntil.Store(time.Now().Add(asyncRepRebuildBackoffDuration(failures)).UnixNano())
		s.asyncRepNeedsRebuild.Store(true)
		return
	}

	// Rebuild succeeded: clear backoff state so subsequent height changes start
	// with a clean slate rather than inheriting a stale backoff window.
	s.asyncRepRebuildFailures.Store(0)
	s.asyncRepRebuildBackoffUntil.Store(0)

	// If Close() fired between the last ctx.Err() check and enableAsyncReplication
	// returning, the init-scan ran with a cancelled ctx and exited immediately.
	// The shard is registered but asyncRepNeedsRebuild was cleared, so disable
	// now to avoid leaving a stale registration with an un-initialized hashtree.
	// dumpHashTreeWithTimeout (called inside disableAsyncReplication) bounds the
	// hashtree fsync to hashtreeDumpTimeout, preventing an indefinite block.
	if sched.ctx.Err() != nil {
		s.disableAsyncReplication(sched.ctx)
	}
}
