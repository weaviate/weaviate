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
	"github.com/weaviate/weaviate/usecases/config"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

const (
	defaultAsyncReplicationSchedulerWorkers        = 10
	defaultAsyncReplicationHashtreeInitConcurrency = 100
	maxMaxWorkers                                  = 100

	// Root pre-filter batch defaults/cap; aliases of the usecases/config source of
	// truth. 1 disables the batched compare; <= 0 falls back to the default.
	fallbackRootPrefilterBatchSize = config.DefaultAsyncReplicationRootPrefilterBatchSize
	maxRootPrefilterBatchSize      = config.MaxAsyncReplicationRootPrefilterBatchSize

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
	// descendDirect: pre-filter classified this as diverging; dispatch ships it as a
	// standalone singleton so descents spread across the pool. Guarded by sched.mu.
	descendDirect bool
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
	// deferDescent: entry diverges; onResultLocked re-enqueues it immediately with
	// descendDirect instead of scheduling the next cycle.
	deferDescent bool
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
	// reconcileFailures counts indices that failed to reconcile their async
	// replication state with the global flag (see DB.ReconcileAsyncReplication).
	// A non-zero rate means a cluster may be stuck partially reconciled until
	// the next flag toggle or schema update.
	reconcileFailures prometheus.Counter
	// rootPrefilterSkips counts shard cycles short-circuited as in-sync by the pre-filter.
	rootPrefilterSkips prometheus.Counter
	// rootPrefilterBatchSize observes the number of shards per pre-filter batch.
	rootPrefilterBatchSize prometheus.Histogram
	// rootCompareRPC counts batched root-compare RPCs by outcome (ok|error|unsupported).
	rootCompareRPC *prometheus.CounterVec
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

	m.reconcileFailures, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "async_replication_reconcile_failures_total",
			Help:      "Number of indices that failed to reconcile async replication with the global AsyncReplicationDisabled flag.",
		}),
	)
	if err != nil {
		return m, err
	}

	m.rootPrefilterSkips, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "async_replication_root_prefilter_skips_total",
			Help:      "Shard cycles short-circuited as in-sync by the batched hashtree-root pre-filter.",
		}),
	)
	if err != nil {
		return m, err
	}

	m.rootPrefilterBatchSize, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "weaviate",
			Name:      "async_replication_root_prefilter_batch_size",
			Help:      "Number of shards compared in one batched hashtree-root pre-filter RPC group.",
			Buckets:   []float64{1, 8, 32, 64, 128, 256, 512, 1024, 2048, 4096},
		}),
	)
	if err != nil {
		return m, err
	}

	m.rootCompareRPC, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "async_replication_root_compare_rpc_total",
			Help:      "Batched hashtree-root compare RPCs by outcome.",
		}, []string{"result"}),
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

func (m *asyncReplicationSchedulerMetrics) addReconcileFailures(n int) {
	if m.monitoring {
		m.reconcileFailures.Add(float64(n))
	}
}

func (m *asyncReplicationSchedulerMetrics) setWorkerPoolSize(n int) {
	if m.monitoring {
		m.workerPoolSize.Set(float64(n))
	}
}

func (m *asyncReplicationSchedulerMetrics) incRootPrefilterSkips() {
	if m.monitoring {
		m.rootPrefilterSkips.Inc()
	}
}

func (m *asyncReplicationSchedulerMetrics) observeRootPrefilterBatch(n int) {
	if m.monitoring {
		m.rootPrefilterBatchSize.Observe(float64(n))
	}
}

func (m *asyncReplicationSchedulerMetrics) addRootCompareRPC(ok, errored, unsupported int) {
	if !m.monitoring {
		return
	}
	if ok > 0 {
		m.rootCompareRPC.WithLabelValues("ok").Add(float64(ok))
	}
	if errored > 0 {
		m.rootCompareRPC.WithLabelValues("error").Add(float64(errored))
	}
	if unsupported > 0 {
		m.rootCompareRPC.WithLabelValues("unsupported").Add(float64(unsupported))
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

	// workCh feeds pooled batches of same-index entries (usually 1) to the pool.
	// Buffered (maxMaxWorkers) so sends are O(1) under mu; a full buffer = backpressure.
	workCh chan *[]*asyncSchedulerEntry

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

	// rootPrefilterBatchSize: cluster-wide count of same-index shards batched into one
	// pre-filter RPC. 1 disables it; <= 0 warns and defaults; over-cap is clamped.
	rootPrefilterBatchSize *configRuntime.DynamicValue[int]
	warnedBatchSizeInvalid atomic.Bool
	warnedBatchSizeClamp   atomic.Bool

	// batchPool recycles the []*asyncSchedulerEntry slices sent on workCh.
	batchPool sync.Pool
	// dispatchBuckets groups due entries by index during one dispatch pass; dispatcher-owned.
	dispatchBuckets map[*Index][]*asyncSchedulerEntry

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

	rootPrefilterBatchSize := replicationCfg.AsyncReplicationRootPrefilterBatchSize
	if rootPrefilterBatchSize == nil {
		rootPrefilterBatchSize = configRuntime.NewDynamicValue(fallbackRootPrefilterBatchSize)
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
		workCh:                   make(chan *[]*asyncSchedulerEntry, maxMaxWorkers),
		resultCh:                 make(chan asyncSchedulerResult, maxMaxWorkers*2),
		addCh:                    make(chan addRequest),
		removeCh:                 make(chan removeRequest),
		asyncReplicationDisabled: asyncReplicationDisabled,
		rootPrefilterBatchSize:   rootPrefilterBatchSize,
		dispatchBuckets:          make(map[*Index][]*asyncSchedulerEntry),
		hashtreeInitSem:          semaphore.NewWeighted(int64(hashtreeInitConcurrency)),
		metrics:                  m,
		logger:                   logger,
		runtimeClampWarner:       &asyncReplicationClampWarner{logger: logger},
	}
	s.batchPool.New = func() any {
		b := make([]*asyncSchedulerEntry, 0, 64)
		return &b
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

// effectiveBatchSize returns the root pre-filter batch size: 1 disables it, <= 0
// warns and defaults, over-cap warns once and clamps. Called under mu.
func (sched *AsyncReplicationScheduler) effectiveBatchSize() int {
	def := fallbackRootPrefilterBatchSize

	n := def
	if sched.rootPrefilterBatchSize != nil {
		n = sched.rootPrefilterBatchSize.Get()
	}
	if n <= 0 {
		if sched.warnedBatchSizeInvalid.CompareAndSwap(false, true) {
			sched.logger.WithField("action", "async_replication_scheduler").
				Warnf("invalid async replication root prefilter batch size %d; using default %d", n, def)
		}
		n = def
	}
	if n > maxRootPrefilterBatchSize {
		if sched.warnedBatchSizeClamp.CompareAndSwap(false, true) {
			sched.logger.WithField("action", "async_replication_scheduler").
				Warnf("async replication root prefilter batch size %d exceeds max %d; clamping", n, maxRootPrefilterBatchSize)
		}
		n = maxRootPrefilterBatchSize
	}
	return n
}

// rollbackReservedLocked undoes a reserve (Add+inFlight+Pop) for an entry that
// could not be dispatched, re-queuing it with a fresh seq. mu must be held.
func (sched *AsyncReplicationScheduler) rollbackReservedLocked(entry *asyncSchedulerEntry) {
	entry.inFlight = false
	entry.shard.asyncRepWg.Done()
	entry.seq = sched.nextSeq
	sched.nextSeq++
	heap.Push(&sched.h, entry)
}

// trySendBatchLocked non-blockingly sends a pooled copy of entries to the pool;
// returns false (releasing the slice) if the channel is full. mu must be held.
func (sched *AsyncReplicationScheduler) trySendBatchLocked(entries []*asyncSchedulerEntry) bool {
	bp := sched.batchPool.Get().(*[]*asyncSchedulerEntry)
	*bp = append((*bp)[:0], entries...)
	select {
	case sched.workCh <- bp:
		return true
	default:
		*bp = (*bp)[:0]
		sched.batchPool.Put(bp)
		return false
	}
}

// dispatchDueLocked pops due entries, coalesces by index into capped batches, and
// sends each to the pool; rolls reserved entries back on a full channel. Held under mu.
func (sched *AsyncReplicationScheduler) dispatchDueLocked() {
	if sched.asyncReplicationDisabled.Get() {
		return
	}
	now := time.Now()

	full := false
	for len(sched.h) > 0 && !sched.h[0].nextRunAt.After(now) && !full {
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

		if entry.descendDirect {
			// Diverging shard: ship as a standalone singleton so its descent runs on
			// any free worker. Clear descendDirect only on a successful send.
			entry.shard.asyncRepWg.Add(1)
			entry.inFlight = true
			heap.Pop(&sched.h)
			if sched.trySendBatchLocked([]*asyncSchedulerEntry{entry}) {
				entry.descendDirect = false
			} else {
				sched.rollbackReservedLocked(entry)
				full = true
			}
			continue
		}

		idx := entry.shard.index

		// Add(1) before send so Deregister+asyncRepWg.Wait() catches the cycle; pop to advance.
		entry.shard.asyncRepWg.Add(1)
		entry.inFlight = true
		heap.Pop(&sched.h)

		sched.dispatchBuckets[idx] = append(sched.dispatchBuckets[idx], entry)
		if len(sched.dispatchBuckets[idx]) >= sched.effectiveBatchSize() {
			if !sched.trySendBatchLocked(sched.dispatchBuckets[idx]) {
				for _, e := range sched.dispatchBuckets[idx] {
					sched.rollbackReservedLocked(e)
				}
				full = true
			}
			sched.dispatchBuckets[idx] = sched.dispatchBuckets[idx][:0]
		}
	}

	// Flush partial buckets: a bucket below the cap ships as a smaller batch, or
	// rolls back on a full channel.
	for idx, entries := range sched.dispatchBuckets {
		if len(entries) == 0 {
			continue
		}
		if full || !sched.trySendBatchLocked(entries) {
			for _, e := range entries {
				sched.rollbackReservedLocked(e)
			}
			full = true
		}
		sched.dispatchBuckets[idx] = entries[:0]
	}
	sched.metrics.setQueueDepth(len(sched.h))
}

// onResultLocked re-enqueues a shard after a cycle completes (or discards
// the result if the shard has been deregistered).
// mu must be held.
func (sched *AsyncReplicationScheduler) onResultLocked(result asyncSchedulerResult) {
	entry := result.entry
	entry.inFlight = false

	// Match on entry identity, not just shard presence: a same-*Shard re-register
	// installs a new entry, and re-pushing this stale one would let two workers run.
	if cur, ok := sched.entries[entry.shard]; !ok || cur != entry {
		return
	}

	// Skip re-push if dispatchDueLocked's invariant-violation recovery
	// already re-queued this entry (heapIdx >= 0 ⇒ in heap).
	if entry.heapIdx >= 0 {
		return
	}

	// Diverging: re-enqueue immediately with descendDirect so it descends as a
	// singleton rather than waiting a full cycle. Bypasses the interval below.
	if result.deferDescent {
		entry.descendDirect = true
		entry.nextRunAt = time.Now()
		entry.seq = sched.nextSeq
		sched.nextSeq++
		heap.Push(&sched.h, entry)
		sched.metrics.setQueueDepth(len(sched.h))
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

// drainWorkChOnExit empties workCh after the dispatcher returned, balancing each
// reserved entry's Add(1). Single non-blocking pass suffices (sole producer).
func (sched *AsyncReplicationScheduler) drainWorkChOnExit() {
	for {
		select {
		case bp, ok := <-sched.workCh:
			if !ok || bp == nil {
				return
			}
			for _, entry := range *bp {
				entry.inFlight = false
				entry.shard.asyncRepWg.Done()
			}
		default:
			return
		}
	}
}

// ----- worker goroutines ----------------------------------------------------

// batchScratch holds a worker's reusable classify maps, reset per batch: no
// per-batch allocs and no cross-worker sharing.
type batchScratch struct {
	roots    map[string]hashtree.Digest
	eligible map[string]*asyncSchedulerEntry
	skip     map[*asyncSchedulerEntry]bool
}

func newBatchScratch() *batchScratch {
	return &batchScratch{
		roots:    make(map[string]hashtree.Digest),
		eligible: make(map[string]*asyncSchedulerEntry),
		skip:     make(map[*asyncSchedulerEntry]bool),
	}
}

func (bs *batchScratch) reset() {
	clear(bs.roots)
	clear(bs.eligible)
	clear(bs.skip)
}

func (sched *AsyncReplicationScheduler) worker() {
	scratch := newBatchScratch()
	for {
		// Prioritise work over scale-down: prevents Go's random select from
		// terminating a worker while items sit in workCh.
		select {
		case bp, ok := <-sched.workCh:
			if !ok {
				return
			}
			sched.runBatch(bp, scratch)
			continue
		default:
		}

		select {
		case <-sched.ctx.Done():
			// drainWorkChOnExit handles any leftover items.
			return
		case <-sched.scaleDownCh:
			return
		case bp, ok := <-sched.workCh:
			if !ok {
				return
			}
			sched.runBatch(bp, scratch)
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

// hasTargetNodeOverrides reports active target-node overrides (tenant movement /
// manual push); such shards skip the pre-filter and take the full per-shard path.
func (sched *AsyncReplicationScheduler) hasTargetNodeOverrides(s *Shard) bool {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	return len(s.targetNodeOverrides) > 0
}

// effectiveConfig snapshots a shard's async replication config with runtime global
// overrides applied, to derive the pre-filter RPC timeout for a batch.
func (sched *AsyncReplicationScheduler) effectiveConfig(s *Shard) AsyncReplicationConfig {
	s.asyncReplicationRWMux.RLock()
	base := s.asyncReplicationConfig
	s.asyncReplicationRWMux.RUnlock()
	cfg := base
	if s.index != nil && s.index.globalreplicationConfig != nil {
		sched.runtimeClampWarner.checkGlobals(*s.index.globalreplicationConfig)
		cfg = base.Effective(*s.index.globalreplicationConfig)
	}
	return cfg
}

// runBatch pre-filters a batch once, short-circuits in-sync shards inline, and
// defers diverging ones as singletons so descent spreads across the pool.
func (sched *AsyncReplicationScheduler) runBatch(bp *[]*asyncSchedulerEntry, scratch *batchScratch) {
	batch := *bp
	defer func() {
		*bp = (*bp)[:0]
		sched.batchPool.Put(bp)
	}()

	if len(batch) <= 1 {
		if len(batch) == 1 {
			sched.runEntry(batch[0], false)
		}
		return
	}

	// classifyBatch is panic-contained, so every entry still reaches a runEntry
	// call below — keeping each entry's Done()+result (asyncRepWg) balanced.
	scratch.reset()
	sched.classifyBatch(batch, scratch)
	for _, entry := range batch {
		if scratch.skip[entry] {
			// In sync this cycle — short-circuit inline (no network descent).
			sched.runEntry(entry, true)
			continue
		}
		// Diverging: hand back to the dispatcher for a singleton descent spread
		// across the pool (descending inline would serialize the batch).
		sched.deferDescent(entry)
	}
}

// deferDescent releases a diverging entry back to the dispatcher (re-dispatched as
// a singleton). Done() before the send so the re-enqueue sees a balanced asyncRepWg.
func (sched *AsyncReplicationScheduler) deferDescent(entry *asyncSchedulerEntry) {
	// Done() before the send, matching runEntry: inFlight (not asyncRepWg) enforces
	// one cycle per shard; buffered resultCh + Close()'s drain keep the send non-blocking.
	entry.shard.asyncRepWg.Done()
	sched.resultCh <- asyncSchedulerResult{entry: entry, deferDescent: true}
}

// classifyBatch runs the root pre-filter, recording confirmed-in-sync entries in
// scratch.skip; ineligible shards and any failure classify "descend" (conservative).
func (sched *AsyncReplicationScheduler) classifyBatch(batch []*asyncSchedulerEntry, scratch *batchScratch) {
	defer func() {
		if r := recover(); r != nil {
			sched.logger.WithField("action", "async_replication_scheduler").
				WithField("panic", r).
				Error("recovered from panic in root pre-filter; forcing full descent for the batch")
			enterrors.PrintStack(sched.logger)
			clear(scratch.skip)
		}
	}()

	for _, entry := range batch {
		s := entry.shard
		if sched.hasTargetNodeOverrides(s) {
			continue
		}
		root, ok := s.HashTreeRoot()
		if !ok {
			continue
		}
		scratch.roots[s.name] = root
		scratch.eligible[s.name] = entry
	}
	if len(scratch.roots) == 0 {
		return
	}

	s0 := batch[0].shard
	cfg := sched.effectiveConfig(s0)
	ctx, cancel := context.WithTimeout(sched.ctx, cfg.diffPerNodeTimeout)
	// defer so the timer is released even if PrefilterShardRoots panics (an inline
	// cancel() would be skipped, leaking it until sched.ctx is done).
	defer cancel()
	needFull, stats := s0.index.replicator.PrefilterShardRoots(ctx, scratch.roots)
	sched.metrics.observeRootPrefilterBatch(len(scratch.roots))
	sched.metrics.addRootCompareRPC(stats.OK, stats.Errored, stats.Unsupported)

	for name, entry := range scratch.eligible {
		if _, diverges := needFull[name]; !diverges {
			scratch.skip[entry] = true
		}
	}
}

func (sched *AsyncReplicationScheduler) runEntry(entry *asyncSchedulerEntry, skipHashbeat bool) {
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

	if skipHashbeat {
		// Pre-filter confirmed in sync this cycle (equal roots ⇒ no diff): skip the
		// network descent, but still honor a pending height-change rebuild.
		needsRebuild = s.asyncRepNeedsRebuild.CompareAndSwap(true, false)
		sched.metrics.incRootPrefilterSkips()
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

	// Serialize disable→enable against performShutdown (holds shutdownLock.Lock()
	// across store teardown): enable either finishes before shutdown or is skipped.
	s.shutdownLock.RLock()
	defer s.shutdownLock.RUnlock()

	if s.shut.Load() {
		return
	}

	// disableAsyncReplication is fsync-free (it never persists the live tree on
	// the runtime path), so it is safe to hold across the shutdownLock.RLock
	// region below.
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
	// clean up; disableAsyncReplication does no disk I/O so this is bounded
	// by the Deregister round-trip.
	if sched.ctx.Err() != nil || s.shut.Load() {
		if err := s.disableAsyncReplication(sched.ctx); err != nil {
			s.index.logger.WithField("action", "async_replication_rebuild").Error(err)
		}
	}
}
