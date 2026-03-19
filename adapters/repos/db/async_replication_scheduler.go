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
	"math"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/interval"
	"github.com/weaviate/weaviate/entities/replication"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
)

func isErrNoDiff(err error) bool {
	return errors.Is(err, replica.ErrNoDiffFound)
}

func isErrRootUnchanged(err error) bool {
	return errors.Is(err, replica.ErrHashtreeRootUnchanged)
}

// asyncSchedulerEntry is the per-shard state tracked inside the scheduler heap.
type asyncSchedulerEntry struct {
	shard     *Shard
	nextRunAt time.Time
	inFlight  bool // true while a worker is executing runHashbeatCycle
	backoff   *interval.BackoffTimer
	heapIdx   int // maintained by the heap for O(log n) Fix/Remove
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
	// immediateReschedule is set when a flush notification arrived while the
	// cycle was running (asyncRepImmediateReschedule flag was consumed in
	// runEntry). onResultLocked sets entry.nextRunAt = now so the shard is
	// dispatched on the very next dispatchDueLocked tick instead of waiting
	// for the next interval.
	immediateReschedule bool
	err                 error
	// cfg is the effective config snapshot used for this cycle, pre-read in
	// the worker. onResultLocked uses it to avoid acquiring
	// asyncReplicationRWMux.RLock() while sched.mu is held.
	cfg AsyncReplicationConfig
}

// addRequest is sent on addCh to register a new shard.
type addRequest struct {
	shard *Shard
	ackCh chan struct{}
}

// removeRequest is sent on removeCh to deregister a shard.
type removeRequest struct {
	shard *Shard
	ackCh chan struct{}
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
	// flushNotifications counts flush-triggered reprioritisations processed by
	// the dispatcher.
	flushNotifications prometheus.Counter
	// topologyChanges counts newly-alive-node reschedules detected by the
	// topology watcher.
	topologyChanges prometheus.Counter
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

	m.flushNotifications, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "async_replication_scheduler_flush_notifications_total",
			Help:      "Total flush notifications received by the scheduler dispatcher.",
		}),
	)
	if err != nil {
		return m, err
	}

	m.topologyChanges, _, err = monitoring.EnsureRegisteredMetric(
		prom.Registerer,
		prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "weaviate",
			Name:      "async_replication_scheduler_topology_changes_total",
			Help:      "Total topology change events (newly-alive nodes) that triggered an immediate reschedule.",
		}),
	)
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

func (m *asyncReplicationSchedulerMetrics) incFlushNotifications() {
	if m.monitoring {
		m.flushNotifications.Inc()
	}
}

func (m *asyncReplicationSchedulerMetrics) incTopologyChanges() {
	if m.monitoring {
		m.topologyChanges.Inc()
	}
}

// AsyncReplicationScheduler is a DB-level scheduler that dispatches hashbeat
// cycles to a fixed-size worker pool. It replaces the former two-goroutines-
// per-shard model (hashbeater + trigger) with a single pool of N workers,
// one dispatcher goroutine, and one topology-watcher goroutine — scaling
// O(1) with respect to tenant count instead of O(tenants).
//
// Lifecycle:
//
//	scheduler, err := NewAsyncReplicationScheduler(ctx, replicationCfg, prom, logger)
//	scheduler.Start()
//	// ... register/deregister shards as they are created/destroyed ...
//	scheduler.Close()
type AsyncReplicationScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

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
	// Capacity 1024 is far above any realistic worker count.
	scaleDownCh chan struct{}

	// maxWorkersConfig is the runtime-dynamic worker-count ceiling.
	// The topology watcher polls it each tick and calls adjustWorkers.
	maxWorkersConfig *configRuntime.DynamicValue[int]

	// workCh feeds runHashbeatCycle calls to the worker pool.
	workCh chan *asyncSchedulerEntry

	// resultCh receives results from workers; buffered so workers never block.
	resultCh chan asyncSchedulerResult

	// addCh / removeCh are used to register / deregister shards synchronously
	// from the caller's goroutine through the dispatcher's select loop.
	addCh    chan addRequest
	removeCh chan removeRequest

	// notifyCh receives shards that have pending flush notifications.
	// Capacity ~1024 with per-shard atomic dedup prevents flooding.
	notifyCh chan *Shard

	// aliveNodesCheckingFrequency controls how often the topology watcher
	// checks for newly-alive replica nodes. Polled on each tick so changes
	// take effect without a restart.
	aliveNodesCheckingFrequency *configRuntime.DynamicValue[time.Duration]

	// asyncReplicationDisabled mirrors the runtime config flag. When true,
	// dispatchDueLocked skips all dispatching and timeUntilNextLocked sleeps
	// for aliveNodesCheckingFrequency so the dispatcher wakes up promptly
	// once the flag is cleared.
	asyncReplicationDisabled *configRuntime.DynamicValue[bool]

	metrics asyncReplicationSchedulerMetrics
	logger  logrus.FieldLogger
}

// NewAsyncReplicationScheduler creates (but does not start) a scheduler.
// Call Start() to begin dispatching.
func NewAsyncReplicationScheduler(
	ctx context.Context,
	replicationCfg replication.GlobalConfig,
	prom *monitoring.PrometheusMetrics,
	logger logrus.FieldLogger,
) (*AsyncReplicationScheduler, error) {
	// Guard against nil DynamicValue pointers that arise when GlobalConfig is
	// zero-valued (e.g. in unit/integration tests that don't configure
	// replication). A nil pointer's Get() returns the zero value for its type,
	// which for time.Duration is 0 — causing the topology watcher to reset its
	// timer to 0 on every tick and spin in a tight busy loop that starves the
	// dispatcher goroutine and makes integration tests appear to hang.
	maxWorkersConfig := replicationCfg.AsyncReplicationClusterMaxWorkers
	if maxWorkersConfig == nil {
		maxWorkersConfig = configRuntime.NewDynamicValue(1)
	}
	workers := maxWorkersConfig.Get()
	if workers <= 0 {
		workers = 1
	}
	if workers > maxMaxWorkers {
		// Cap at maxMaxWorkers to match the resultCh buffer size (maxMaxWorkers*2)
		// and keep invariants consistent with adjustWorkers.
		workers = maxMaxWorkers
	}

	aliveNodesCheckingFreq := replicationCfg.AsyncReplicationAliveNodesCheckingFrequency
	if aliveNodesCheckingFreq == nil {
		aliveNodesCheckingFreq = configRuntime.NewDynamicValue(defaultAliveNodesCheckingFrequency)
	}
	asyncReplicationDisabled := replicationCfg.AsyncReplicationDisabled
	if asyncReplicationDisabled == nil {
		asyncReplicationDisabled = configRuntime.NewDynamicValue(false)
	}

	if logger == nil {
		logger = logrus.StandardLogger()
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
		scaleDownCh:      make(chan struct{}, 1024),
		workCh:           make(chan *asyncSchedulerEntry),
		// Buffer is sized to maxMaxWorkers*2 (the hard ceiling on the pool)
		// so workers never block even if adjustWorkers scales the pool to its
		// maximum. This must be updated if maxMaxWorkers changes.
		resultCh:                    make(chan asyncSchedulerResult, maxMaxWorkers*2),
		addCh:                       make(chan addRequest),
		removeCh:                    make(chan removeRequest),
		notifyCh:                    make(chan *Shard, 1024),
		aliveNodesCheckingFrequency: aliveNodesCheckingFreq,
		asyncReplicationDisabled:    asyncReplicationDisabled,
		metrics:                     m,
		logger:                      logger,
	}
	heap.Init(&s.h)
	return s, nil
}

// Start launches the dispatcher, topology-watcher, and worker-pool goroutines.
// It must be called exactly once.
func (sched *AsyncReplicationScheduler) Start() {
	sched.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer sched.wg.Done()
		sched.dispatcher()
	}, sched.logger)

	sched.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer sched.wg.Done()
		sched.topologyWatcher()
	}, sched.logger)

	sched.workersMu.Lock()
	n := sched.targetWorkers
	sched.workersMu.Unlock()

	for range n {
		sched.wg.Add(1)
		enterrors.GoWrapper(func() {
			defer sched.wg.Done()
			sched.worker()
		}, sched.logger)
	}
	sched.metrics.setWorkerPoolSize(n)
}

// Close shuts down the scheduler and waits for all goroutines to exit.
// Registered shards should be deregistered before calling Close.
func (sched *AsyncReplicationScheduler) Close() {
	sched.cancel()
	sched.wg.Wait()
}

// Register adds a shard to the scheduler for periodic hashbeat dispatching.
// The first hashbeat cycle is triggered immediately.
// Register must not be called while holding asyncReplicationRWMux (see
// the same constraint that applied to startHashbeaterGoroutines).
func (sched *AsyncReplicationScheduler) Register(s *Shard) {
	ackCh := make(chan struct{})
	select {
	case sched.addCh <- addRequest{shard: s, ackCh: ackCh}:
		// The send completed: the dispatcher has committed to processing the
		// request and closing ackCh. Wait for that ack, but also watch for
		// context cancellation so that a panic (or any other path that exits
		// the dispatcher without closing ackCh) cannot deadlock the caller.
		select {
		case <-ackCh:
		case <-sched.ctx.Done():
		}
	case <-sched.ctx.Done():
	}
}

// Deregister removes a shard from the scheduler. It returns only after the
// dispatcher has confirmed the removal, so subsequent in-flight cycles will
// not be re-enqueued. The caller must separately wait for any in-flight cycle
// to finish (via shard.asyncRepWg.Wait()).
func (sched *AsyncReplicationScheduler) Deregister(s *Shard) {
	ackCh := make(chan struct{})
	select {
	case sched.removeCh <- removeRequest{shard: s, ackCh: ackCh}:
		// Same pattern as Register: wait for the dispatcher's ack but escape
		// via ctx.Done() so the caller is never stranded if the dispatcher
		// exits without closing ackCh.
		select {
		case <-ackCh:
		case <-sched.ctx.Done():
		}
	case <-sched.ctx.Done():
	}
}

// NotifyShard reprioritises s to run immediately (used by flush callbacks).
// Non-blocking: if notifyCh is full the notification is dropped; the shard
// will still run on its next scheduled interval.
func (sched *AsyncReplicationScheduler) NotifyShard(s *Shard) {
	// Per-shard dedup: only enqueue if no notification is already pending.
	if s.asyncRepHasPendingFlush.CompareAndSwap(false, true) {
		select {
		case sched.notifyCh <- s:
		default:
			// Channel full: reset the flag so the next flush can re-notify.
			s.asyncRepHasPendingFlush.Store(false)
		}
	}
}

// ----- dispatcher goroutine -------------------------------------------------

func (sched *AsyncReplicationScheduler) dispatcher() {
	timer := time.NewTimer(math.MaxInt64)
	defer timer.Stop()

	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		sched.mu.Lock()
		d := sched.timeUntilNextLocked()
		sched.mu.Unlock()
		timer.Reset(d)
	}

	for {
		select {
		case <-sched.ctx.Done():
			return

		case <-timer.C:
			sched.mu.Lock()
			sched.dispatchDueLocked()
			sched.mu.Unlock()
			resetTimer()

		case result := <-sched.resultCh:
			sched.mu.Lock()
			sched.onResultLocked(result)
			sched.mu.Unlock()
			resetTimer()

		case s := <-sched.notifyCh:
			sched.metrics.incFlushNotifications()
			s.asyncRepHasPendingFlush.Store(false)
			sched.mu.Lock()
			sched.reprioritizeNowLocked(s)
			sched.mu.Unlock()
			resetTimer()

		case req := <-sched.addCh:
			sched.mu.Lock()
			sched.onAddLocked(req.shard)
			sched.mu.Unlock()
			close(req.ackCh)
			resetTimer()

		case req := <-sched.removeCh:
			sched.mu.Lock()
			sched.onRemoveLocked(req.shard)
			sched.mu.Unlock()
			close(req.ackCh)
			// No resetTimer needed: entry removed, heap shrunk or unchanged.
			resetTimer()
		}
	}
}

// minDispatchInterval is the shortest wait the dispatcher will use between
// dispatch attempts. This prevents a busy-loop when all workers are saturated
// and due shards are waiting: without a floor the timer would fire immediately
// every iteration until a worker result arrives on resultCh.
const minDispatchInterval = time.Millisecond

// timeUntilNextLocked returns how long until the next heap entry is due.
// Returns 24h if the heap is empty. Returns aliveNodesCheckingFrequency if
// async replication is globally disabled, so the dispatcher wakes up promptly
// once re-enabled.
// mu must be held.
func (sched *AsyncReplicationScheduler) timeUntilNextLocked() time.Duration {
	if sched.asyncReplicationDisabled.Get() {
		return sched.aliveNodesCheckingFreq()
	}
	if len(sched.h) == 0 {
		return 24 * time.Hour
	}
	d := time.Until(sched.h[0].nextRunAt)
	if d < minDispatchInterval {
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
			// Safety guard: entry in heap should never be inFlight.
			// This indicates a bug; skip and pop to avoid spinning.
			heap.Pop(&sched.h)
			continue
		}

		// Add(1) before sending to workCh so that Deregister+asyncRepWg.Wait()
		// reliably waits for the cycle even if the worker hasn't started yet.
		entry.shard.asyncRepWg.Add(1)

		select {
		case sched.workCh <- entry:
			heap.Pop(&sched.h)
			entry.inFlight = true
			sched.metrics.incWorkersActive()
		default:
			// All workers busy: undo Add(1) and leave entry in heap.
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
	sched.metrics.decWorkersActive()
	entry.inFlight = false

	// If the shard is no longer in the registry it was deregistered while
	// the cycle was running — do not re-enqueue.
	if _, ok := sched.entries[entry.shard]; !ok {
		return
	}

	// Use the config snapshot carried in the result (pre-read by the worker
	// under asyncReplicationRWMux.RLock before the cycle) rather than
	// re-reading it here. This avoids acquiring asyncReplicationRWMux.RLock
	// while sched.mu is held, which would stall the dispatcher if a pending
	// write-lock request (e.g. updateHashtreeOnFlush) is waiting.
	interval := sched.nextInterval(result.cfg, entry, result)
	now := time.Now()

	if result.immediateReschedule {
		// A flush notification arrived while the cycle was running; re-run
		// immediately so the freshly-flushed objects are propagated without
		// waiting for the next normal interval.
		entry.nextRunAt = now
	} else {
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
	}

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
		switch {
		case isErrNoDiff(result.err):
			entry.backoff.Reset()
			return cfg.frequency
		case isErrRootUnchanged(result.err):
			entry.backoff.Reset()
			return cfg.frequencyWhilePropagating
		case entry.shard.asyncRepCtx != nil && errors.Is(result.err, entry.shard.asyncRepCtx.Err()):
			// Context cancelled (shard shutting down) — use a long interval;
			// the entry will be deregistered shortly.
			return 24 * time.Hour
		default:
			d := entry.backoff.CurrentInterval()
			entry.backoff.IncreaseInterval()
			return d
		}
	}
	entry.backoff.Reset()
	if result.propagated {
		return cfg.frequencyWhilePropagating
	}
	return cfg.frequency
}

// reprioritizeNowLocked sets the shard's nextRunAt to now so it is dispatched
// on the next dispatchDue call. No-op if the shard is not registered or is
// already in-flight (it will be re-enqueued with nextRunAt=now after the
// current cycle completes via a flag).
// mu must be held.
func (sched *AsyncReplicationScheduler) reprioritizeNowLocked(s *Shard) {
	entry, ok := sched.entries[s]
	if !ok {
		return
	}
	if entry.inFlight {
		// Already running; mark it for an immediate re-run after the cycle
		// completes. onResultLocked reads immediateReschedule and sets
		// entry.nextRunAt = now so the shard is dispatched on the next tick.
		s.asyncRepImmediateReschedule.Store(true)
		return
	}
	// Only advance the deadline if the entry is not already overdue.
	// Setting nextRunAt = time.Now() on an already-overdue shard (whose
	// nextRunAt is in the past) would move it forward in the min-heap,
	// effectively demoting it behind shards that became due more recently.
	if now := time.Now(); now.Before(entry.nextRunAt) {
		entry.nextRunAt = now
		heap.Fix(&sched.h, entry.heapIdx)
	}
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
		backoff:   interval.NewBackoffTimer(1*time.Second, 3*time.Second, 5*time.Second),
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

// ----- topology watcher goroutine -------------------------------------------

// topologyWatcher checks once per aliveNodesCheckingFrequency whether any
// registered shard has a newly-alive replica node. When it does, the shard is
// reprioritised to run immediately. It also adjusts the worker pool size when
// maxWorkersConfig changes. Both settings are re-read on every tick, so
// changes take effect without a restart.
func (sched *AsyncReplicationScheduler) topologyWatcher() {
	timer := time.NewTimer(sched.aliveNodesCheckingFreq())
	defer timer.Stop()

	for {
		select {
		case <-sched.ctx.Done():
			return
		case <-timer.C:
			sched.checkTopology()
			sched.adjustWorkers(sched.maxWorkersConfig.Get())
			timer.Reset(sched.aliveNodesCheckingFreq())
		}
	}
}

// aliveNodesCheckingFreq returns the configured topology-check interval,
// clamping to defaultAliveNodesCheckingFrequency when the runtime-config value
// is zero or negative. This prevents a busy-spin if the value is set to 0 via
// a runtime override.
func (sched *AsyncReplicationScheduler) aliveNodesCheckingFreq() time.Duration {
	if d := sched.aliveNodesCheckingFrequency.Get(); d > 0 {
		return d
	}
	return defaultAliveNodesCheckingFrequency
}

func (sched *AsyncReplicationScheduler) checkTopology() {
	// Snapshot registered shards without holding mu across the per-shard checks.
	sched.mu.Lock()
	shards := make([]*Shard, 0, len(sched.entries))
	for s := range sched.entries {
		shards = append(shards, s)
	}
	sched.mu.Unlock()

	for _, s := range shards {
		comparedHosts := s.getLastComparedHosts()
		aliveHosts, ok := s.allAliveHostnames()
		if !ok {
			// Routing plan unavailable: skip both the trigger and the baseline
			// update so a transient error does not corrupt lastComparedHosts and
			// cause spurious "newly alive" detections once routing recovers.
			continue
		}

		hasNew := hasNewElement(aliveHosts, comparedHosts)
		if hasNew {
			sched.metrics.incTopologyChanges()
			sched.mu.Lock()
			sched.reprioritizeNowLocked(s)
			sched.mu.Unlock()
		}
		if hasNew || len(aliveHosts) != len(comparedHosts) {
			s.setLastComparedNodes(aliveHosts)
		}
	}
}

// ----- worker goroutines ----------------------------------------------------

func (sched *AsyncReplicationScheduler) worker() {
	for {
		select {
		case <-sched.ctx.Done():
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

// adjustWorkers scales the worker pool to n. It spawns new goroutines when
// n > targetWorkers and sends scale-down tokens when n < targetWorkers.
// Safe to call concurrently; workersMu serialises adjustments.
func (sched *AsyncReplicationScheduler) adjustWorkers(n int) {
	if n <= 0 {
		n = 1
	}
	if n > maxMaxWorkers {
		// Cap at maxMaxWorkers so resultCh (sized maxMaxWorkers*2) is always
		// large enough to hold all pending results without blocking workers.
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
	s := entry.shard

	// Read base config and compute the effective config for this cycle.
	s.asyncReplicationRWMux.RLock()
	base := s.asyncReplicationConfig
	s.asyncReplicationRWMux.RUnlock()

	cfg := base
	if s.index != nil && s.index.globalreplicationConfig != nil {
		cfg = base.Effective(*s.index.globalreplicationConfig)
	}

	// Detect a hashtree height change requested via runtime config. Mark the
	// shard for rebuild; the goroutine is spawned after asyncRepWg.Done() below
	// so that DisableAsyncReplication can safely call Deregister+Wait.
	s.asyncReplicationRWMux.RLock()
	currentHT := s.hashtree
	s.asyncReplicationRWMux.RUnlock()
	if currentHT != nil && currentHT.Height() != cfg.hashtreeHeight {
		s.asyncRepNeedsRebuild.Store(true)
	}

	needsRebuild := false
	propagated := false
	immediateReschedule := false
	var err error

	defer func() {
		// Done() before the result send: the counter drops to 0 before the
		// dispatcher can re-enqueue and re-dispatch this shard, preserving
		// the "counter 0 or 1" invariant documented on asyncRepWg.
		s.asyncRepWg.Done()

		if needsRebuild {
			// Serialise rebuilds: if another goroutine is already rebuilding
			// this shard, re-arm asyncRepNeedsRebuild so the next cycle retries
			// instead of running a second concurrent rebuild.
			if s.asyncRepRebuildInFlight.CompareAndSwap(false, true) {
				enterrors.GoWrapper(func() {
					defer s.asyncRepRebuildInFlight.Store(false)
					sched.rebuildHashtree(s)
				}, sched.logger)
			} else {
				s.asyncRepNeedsRebuild.Store(true)
			}
		}

		// Result sent after Done() so the dispatcher cannot dispatch a new
		// cycle (and increment the WaitGroup) before Done() has run.
		select {
		case sched.resultCh <- asyncSchedulerResult{entry: entry, propagated: propagated, immediateReschedule: immediateReschedule, err: err, cfg: cfg}:
		case <-sched.ctx.Done():
		}
	}()

	propagated, err = s.runHashbeatCycle(s.asyncRepCtx, cfg)

	// If a flush notification arrived while this cycle was running, schedule
	// the next cycle immediately (nextRunAt = now) rather than waiting for the
	// next normal interval. immediateReschedule is read by onResultLocked.
	if s.asyncRepImmediateReschedule.CompareAndSwap(true, false) {
		immediateReschedule = true
	}

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

	ctx := context.Background()

	if err := s.disableAsyncReplication(ctx); err != nil {
		if sched.logger != nil {
			sched.logger.
				WithField("action", "async_replication_rebuild").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("hashtree rebuild: stop failed: %v", err)
		}
		return
	}

	s.asyncReplicationRWMux.RLock()
	baseCfg := s.asyncReplicationConfig
	s.asyncReplicationRWMux.RUnlock()

	if err := s.enableAsyncReplication(ctx, baseCfg); err != nil {
		if sched.logger != nil {
			sched.logger.
				WithField("action", "async_replication_rebuild").
				WithField("class_name", s.class.Class).
				WithField("shard_name", s.name).
				Errorf("hashtree rebuild: start failed: %v", err)
		}
	}
}
