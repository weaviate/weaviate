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
// one dispatcher, and one watcher — total goroutine count is fixed regardless
// of tenant count, instead of O(tenants).
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

	// Lock ordering: workersMu → mu → asyncReplicationRWMux. Never hold mu
	// while acquiring asyncReplicationRWMux: init-scan goroutines hold the
	// RLock while calling Deregister, which blocks on the dispatcher needing
	// mu — reverse ordering deadlocks.

	// mu protects heap, entries, and nextSeq.
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

	// workCh feeds entries to the worker pool. Buffered (maxMaxWorkers) so
	// dispatchDueLocked sends in O(1) under sched.mu; a full buffer triggers
	// "all workers busy" backpressure via the default branch.
	workCh chan *asyncSchedulerEntry

	// resultCh receives results from workers. Buffer absorbs normal-operation
	// bursts; Close()'s parallel drain keeps runEntry's unconditional send
	// non-blocking at shutdown.
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

	// runtimeClampWarner surfaces sub-minimum global runtime overrides as a
	// one-shot Warn so operator misconfiguration of the frequency env vars
	// (or their runtime-YAML equivalent) does not stay invisible behind the
	// per-cycle clamp in Effective().
	runtimeClampWarner *asyncReplicationClampWarner

	// closed is set true at the top of Close() (before cancel) and never reset.
	// Read by Register/Deregister and handleAdd/handleRemove to reject
	// post-Close calls deterministically.
	closed atomic.Bool
}

// NewAsyncReplicationScheduler creates and starts a scheduler. The dispatcher,
// watcher, and worker-pool goroutines are launched before returning, so the
// scheduler is ready to accept Register/Deregister calls. prom may be nil
// (metrics become no-ops). Returns an error only if metric registration fails.
//
// AsyncReplicationHashtreeInitConcurrency is read once here to size the init
// semaphore; later changes to its DynamicValue have no effect.
func NewAsyncReplicationScheduler(
	ctx context.Context,
	replicationCfg replication.GlobalConfig,
	prom *monitoring.PrometheusMetrics,
	logger logrus.FieldLogger,
) (*AsyncReplicationScheduler, error) {
	// Guard against nil DynamicValue pointers (zero-valued GlobalConfig in tests).
	maxWorkersConfig := replicationCfg.AsyncReplicationSchedulerWorkers
	if maxWorkersConfig == nil {
		maxWorkersConfig = configRuntime.NewDynamicValue(defaultAsyncReplicationSchedulerWorkers)
	}
	workers := maxWorkersConfig.Get()
	if workers <= 0 {
		workers = defaultAsyncReplicationSchedulerWorkers
	}
	// Cap at maxMaxWorkers so resultCh (sized maxMaxWorkers*2) fits the pool.
	requestedWorkers := workers
	if workers > maxMaxWorkers {
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
		ctx:                      sctx,
		cancel:                   cancel,
		h:                        make(asyncSchedulerHeap, 0),
		entries:                  make(map[*Shard]*asyncSchedulerEntry),
		targetWorkers:            workers,
		maxWorkersConfig:         maxWorkersConfig,
		scaleDownCh:              make(chan struct{}, maxMaxWorkers),
		workCh:                   make(chan *asyncSchedulerEntry, maxMaxWorkers),
		resultCh:                 make(chan asyncSchedulerResult, maxMaxWorkers*2),
		addCh:                    make(chan addRequest),
		removeCh:                 make(chan removeRequest),
		asyncReplicationDisabled: asyncReplicationDisabled,
		hashtreeInitSem:          semaphore.NewWeighted(int64(hashtreeInitConcurrency)),
		metrics:                  m,
		logger:                   logger,
		runtimeClampWarner:       &asyncReplicationClampWarner{logger: logger},
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

// Close shuts down the scheduler and blocks until all goroutines exit.
//
// Recommended: call mayStopAsyncReplication on every shard first so per-shard
// contexts are cancelled and in-flight RPCs abort promptly. Skipping this is
// safe — each shard's asyncRepCtx is derived from sched.ctx, so sched.cancel()
// propagates cancellation; the recommended order is for latency, not
// correctness. Close also force-cancels any still-registered shard's context
// as a safety net. The 30 s mark only triggers a diagnostic warning; Close
// keeps waiting until the pool drains.
func (sched *AsyncReplicationScheduler) Close() {
	// Set closed before anything else so Register/Deregister and handleAdd/
	// handleRemove can deterministically reject post-Close calls.
	sched.closed.Store(true)

	// Invariant: shards must be deregistered before Close. Collect any
	// stragglers so we can force-cancel their per-shard contexts.
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

	// Release sched.mu before acquiring asyncReplicationRWMux: holding both
	// in reverse order would deadlock against init-scan goroutines that hold
	// the RLock while calling Deregister.
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

	// Drain resultCh in parallel: the dispatcher stops draining once it picks
	// ctx.Done(), and runEntry's send is unconditional, so under heavy load a
	// full buffer would pin sched.wg.Wait(). Discarding is safe — runEntry
	// already called Done() before sending.
	shutdownWg.Add(1)
	enterrors.GoWrapper(func() {
		defer shutdownWg.Done()
		for {
			select {
			case <-done:
				return
			case <-sched.resultCh:
			}
		}
	}, sched.logger)

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		sched.logger.WithField("action", "async_replication_scheduler_close").
			Warn("scheduler shutdown still waiting for goroutines after 30s; workers may be blocked on long RPCs")
	}
	// Unbounded wait: Close has no hard deadline.
	shutdownWg.Wait()
}

// Register adds a shard for periodic hashbeat dispatching. The first cycle
// fires immediately. Must not be called while holding asyncReplicationRWMux.
//
// Strict pre/postcondition: nil iff the shard is in the registry afterwards;
// ErrSchedulerClosed iff it is not (closed at entry, ctx cancelled before
// send, or closed flipped before handleAdd checked). non-nil ⇒ no side effect.
func (sched *AsyncReplicationScheduler) Register(s *Shard) error {
	if sched.closed.Load() {
		return ErrSchedulerClosed
	}
	ackCh := make(chan struct{})
	var processed atomic.Bool
	req := addRequest{shard: s, ackCh: ackCh, processed: &processed}
	select {
	case sched.addCh <- req:
		// addCh is unbuffered: once the send succeeds, handleAdd has the
		// request and will close ackCh via its first-line defer. Waiting on
		// ackCh unconditionally keeps the strict postcondition — racing it
		// against ctx.Done() would let Close produce false-negative errors.
		<-ackCh
		if processed.Load() {
			return nil
		}
		return ErrSchedulerClosed
	case <-sched.ctx.Done():
		return ErrSchedulerClosed
	}
}

// Deregister removes a shard from the scheduler. Returns only after the
// dispatcher confirms the removal, so no further cycles will be re-enqueued.
// Callers needing to wait for an in-flight cycle must call asyncRepWg.Wait()
// separately afterwards.
//
// Strict pre/postcondition: nil iff the shard is no longer in the registry;
// ErrSchedulerClosed iff the removal did not run. non-nil ⇒ no side effect.
func (sched *AsyncReplicationScheduler) Deregister(s *Shard) error {
	if sched.closed.Load() {
		return ErrSchedulerClosed
	}
	ackCh := make(chan struct{})
	var processed atomic.Bool
	req := removeRequest{shard: s, ackCh: ackCh, processed: &processed}
	select {
	case sched.removeCh <- req:
		// See Register for why we wait on ackCh unconditionally.
		<-ackCh
		if processed.Load() {
			return nil
		}
		return ErrSchedulerClosed
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

// handleAdd processes an addRequest, dropping it if closing. processed is set
// only when onAddLocked ran. The deferred close(ackCh) is registered first so
// it fires last (LIFO) — readers see the ack after sched.mu is released.
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

// handleRemove mirrors handleAdd: skip when closing. Dropping late requests
// is safe — Close() force-cancels per-shard contexts and the entries map is
// discarded on teardown.
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
	// Sole drain mechanism for workCh. Workers exit promptly on ctx.Done()
	// without competing for items; the dispatcher is the only producer so
	// anything left here on exit gets its Add(1) balanced by Done().
	defer sched.drainWorkChOnExit()

	resetTimer := func() {
		if !timer.Stop() {
			// Non-blocking drain: when called from the timer.C branch the
			// channel is already empty (a blocking drain would deadlock);
			// from other branches the timer may or may not have fired.
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

// fullChannelBackoff is the wait when due entries exist but workCh is full
// (all workers busy). Prevents the dispatcher from spinning at
// minDispatchInterval while waiting for a worker slot.
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
			// Invariant violation: heap entries must have inFlight=false
			// (onResultLocked clears inFlight before heap.Push). Liveness
			// recovery only — without the pop+reset, dispatchDueLocked
			// would spin on this entry forever. asyncRepWg is already
			// balanced: the in-flight worker's defer will call Done().
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

		// Add(1) before send so Deregister+asyncRepWg.Wait() catches the cycle.
		entry.shard.asyncRepWg.Add(1)
		entry.inFlight = true

		select {
		case sched.workCh <- entry:
			heap.Pop(&sched.h)
		default:
			// All workers busy: roll back and leave entry in heap.
			entry.inFlight = false
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

	// Skip re-push if dispatchDueLocked's invariant-violation recovery
	// already re-queued this entry (heapIdx >= 0 ⇒ in heap).
	if entry.heapIdx >= 0 {
		return
	}

	// Use the cfg snapshot from the result (read under asyncReplicationRWMux
	// in the worker) so we don't acquire that RLock while holding sched.mu.
	interval := sched.nextInterval(result.cfg, entry, result)
	now := time.Now()

	// Epoch-relative scheduling: next run is interval after the original due
	// time, not after completion, so late shards retain priority over
	// on-time ones. Safety floor: cap look-back to one interval before now so
	// a far-behind shard can't flood the queue with catch-up cycles.
	base := entry.nextRunAt
	if minBase := now.Add(-interval); base.Before(minBase) {
		base = minBase
	}
	entry.nextRunAt = base.Add(interval)

	entry.seq = sched.nextSeq
	sched.nextSeq++
	heap.Push(&sched.h, entry)
	sched.metrics.setQueueDepth(len(sched.h))
}

// nextInterval computes how long to wait before the next hashbeat cycle.
func (sched *AsyncReplicationScheduler) nextInterval(cfg AsyncReplicationConfig, entry *asyncSchedulerEntry, result asyncSchedulerResult) time.Duration {
	if result.err != nil {
		if result.ctx != nil && errors.Is(result.err, result.ctx.Err()) {
			// Shard shutting down — long interval; will be deregistered soon.
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
// If in-flight, the worker's eventual result will be discarded by onResultLocked
// (entry no longer in sched.entries). mu must be held.
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
	sched.metrics.setQueueDepth(len(sched.h))
}

// drainWorkChOnExit empties workCh after the dispatcher has returned, balancing
// the Add(1) that dispatchDueLocked did before each send. Dispatcher is the
// sole producer so a single non-blocking pass suffices; channel receives are
// atomic so concurrent workers can't double-take any entry.
func (sched *AsyncReplicationScheduler) drainWorkChOnExit() {
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
}

// ----- worker goroutines ----------------------------------------------------

func (sched *AsyncReplicationScheduler) worker() {
	for {
		// Prioritise work over scale-down: prevents Go's random select from
		// terminating a worker while items sit in workCh.
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
			// drainWorkChOnExit handles any leftover items.
			return
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

// adjustWorkers scales the pool to n: spawns goroutines when growing, sends
// scale-down tokens when shrinking. n <= 0 falls back to the default. Safe
// to call concurrently; workersMu serialises adjustments.
func (sched *AsyncReplicationScheduler) adjustWorkers(n int) {
	if n <= 0 {
		n = defaultAsyncReplicationSchedulerWorkers
	}
	if n > maxMaxWorkers {
		// Cap so resultCh (sized maxMaxWorkers*2) stays large enough.
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
		// Drain stale scale-down tokens from a prior shrink, otherwise new
		// workers would consume them immediately and exit. Non-blocking
		// receives because running workers may consume concurrently.
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

	// Read config, ctx, and hashtree height under one RLock so all three come
	// from the same snapshot — a split read could be torn by initAsyncReplication
	// installing a new config+tree between acquisitions, triggering a spurious
	// rebuild. Height() is lock-free so calling it under RLock is fine.
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
		sched.runtimeClampWarner.checkGlobals(*s.index.globalreplicationConfig)
		cfg = base.Effective(*s.index.globalreplicationConfig)
	}

	// Runtime config changed the hashtree height — flag a rebuild. The rebuild
	// goroutine is spawned post-Done() so mayStopAsyncReplication's Wait isn't
	// blocked by this cycle.
	if currentHT != nil && currentHTHeight != cfg.hashtreeHeight {
		s.asyncRepNeedsRebuild.Store(true)
	}

	needsRebuild := false
	propagated := false
	var err error

	defer func() {
		// Recover panics so the worker stays alive and the dispatcher sees a
		// non-nil error (proper retry delay) rather than a false success.
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in runHashbeatCycle: %v", r)
			sched.logger.
				WithField("panic", r).
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Error("recovered from panic in async replication worker")
			enterrors.PrintStack(sched.logger)
		}

		// Done() before the resultCh send so the WG drops before the
		// dispatcher re-enqueues. inFlight (not asyncRepWg) enforces "at
		// most one cycle per shard"; asyncRepWg may legitimately be >1.
		s.asyncRepWg.Done()

		if needsRebuild {
			if until := s.asyncRepRebuildBackoffUntil.Load(); until > 0 && time.Now().UnixNano() < until {
				// Still in backoff from prior failure — re-arm for next cycle.
				s.asyncRepNeedsRebuild.Store(true)
			} else if s.asyncRepRebuildInFlight.CompareAndSwap(false, true) {
				// Serialise: at most one rebuild goroutine at a time.
				sched.wg.Add(1)
				enterrors.GoWrapper(func() {
					defer sched.wg.Done()
					defer s.asyncRepRebuildInFlight.Store(false)
					sched.rebuildHashtree(s)
				}, sched.logger)
			} else {
				// Rebuild already in flight; re-arm. A narrow window can
				// over-trigger one extra rebuild — benign.
				s.asyncRepNeedsRebuild.Store(true)
			}
		}

		sched.metrics.decWorkersActive()

		// Unconditional send: the buffered resultCh plus Close()'s parallel
		// drain keep this non-blocking. A ctx.Done() branch would leave
		// entry.inFlight=true permanently if the scheduler kept running.
		sched.resultCh <- asyncSchedulerResult{entry: entry, propagated: propagated, err: err, cfg: cfg, ctx: ctx}
	}()

	if ctx == nil {
		// Registered before initAsyncReplication completed — skip; the err
		// result re-dispatches at cfg.frequency rather than spinning.
		err = errors.New("shard not yet initialized")
		return
	}
	if ctx.Err() != nil {
		// mayStopAsyncReplication fired between dispatch and now — return
		// fast so asyncRepWg.Wait() unblocks promptly.
		err = ctx.Err()
		return
	}

	propagated, err = s.runHashbeatCycle(ctx, cfg)
	needsRebuild = s.asyncRepNeedsRebuild.CompareAndSwap(true, false)
}

// rebuildHashtree stops then restarts async replication on s, picking up the
// new hashtree height from runtime config via enableAsyncReplication's
// internal Effective() call.
func (sched *AsyncReplicationScheduler) rebuildHashtree(s *Shard) {
	// Wait for any cycle dispatched between our Done() and the disable below.
	s.asyncRepWg.Wait()

	// Skip if the shard store is being torn down. performShutdown sets
	// s.shut=true before calling mayStopAsyncReplication, so this is a
	// reliable happens-before. Re-enabling now would race with store shutdown
	// and leak an init-scan goroutine past scheduler.Close().
	if s.shut.Load() {
		return
	}

	// sched.ctx so disable/enable respect scheduler shutdown.
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

	// Bail if Close() fired: enableAsyncReplication would otherwise spawn an
	// init-scan goroutine with no cancellable context.
	if sched.ctx.Err() != nil {
		return
	}

	s.asyncReplicationRWMux.RLock()
	baseCfg := s.asyncReplicationConfig
	s.asyncReplicationRWMux.RUnlock()

	// Re-check after the RLock window.
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

	// Reset backoff so subsequent height changes start clean.
	s.asyncRepRebuildFailures.Store(0)
	s.asyncRepRebuildBackoffUntil.Store(0)

	// If Close() fired during enableAsyncReplication, or performShutdown set
	// s.shut between the entry-time check and now, the enable touched a store
	// being torn down or registered against a cancelled scheduler. Disable to
	// clean up; disableAsyncReplication's fsync is bounded by hashtreeDumpTimeout.
	if sched.ctx.Err() != nil || s.shut.Load() {
		s.disableAsyncReplication(sched.ctx)
	}
}
