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

package lazyshardunloading

import (
	"context"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// shard is what the policy needs of a lazy shard wrapper; *db.LazyLoadShard in production.
type shard interface {
	comparable
	LastAccess() uint32
	LoadedAt() time.Time
	UnloadedAt() time.Time
	Namespace() string
	UnloadCandidate() bool
	TryMarkUnloadPending() bool
	ClearUnloadPending()
	TryUnload(ctx context.Context, req db.UnloadRequest) db.UnloadOutcome
}

// registry is what the policy needs of *db.LoadedShardRegistry.
type registry[S shard] interface {
	Loaded() int64
	Unloading() int64
	NamespaceLoaded(namespace string) int
	NamespaceCounts(dst map[string]int) map[string]int
	Sample(dst []S, k int) []S
}

// queueCapacity bounds each handoff channel; a full channel means overcommit.
const queueCapacity = 1024

// reindexSnapshotTimeout bounds the per-tick DTM list behind the reindex snapshot.
const reindexSnapshotTimeout = 3 * time.Second

type reindexSource = func(ctx context.Context) (db.ReindexActivity, error)

type item[S shard] struct {
	shard  S
	reason reason
	req    db.UnloadRequest
}

type policyDeps struct {
	logger     logrus.FieldLogger
	metrics    *metrics
	clock      func() uint32
	memRatio   func() float64
	nsActive   func(namespace string) bool
	namespaces bool
	queueCap   int
}

// policy is the unloading policy over any shard type, so tests can drive it with fakes.
//
// Concurrency: beforeLoad runs on loading goroutines and touches only atomics,
// the registry's leaf mutex, a sync.Pool buffer and non-blocking sends. The loop
// is the only goroutine that resizes workers and owns the loop* fields.
type policy[S shard] struct {
	policyDeps
	reg   registry[S]
	knobs *snapshotter

	snap  atomic.Pointer[snapshot]
	ratio atomic.Uint64

	// reindex is the last tick's snapshot; nil, or an unusable one, refuses every unload it guards.
	reindex       atomic.Pointer[db.ReindexActivity]
	reindexSrc    atomic.Pointer[reindexSource]
	reindexFailed bool

	makeRoomCh   chan item[S]
	backgroundCh chan item[S]
	wake         chan struct{}
	pending      atomic.Int64
	// enqueuing counts enqueues past their closed check; close drains only once it is 0.
	enqueuing atomic.Int64
	// warmupReserved counts warmup loads admitted but not yet released.
	warmupReserved atomic.Int64
	bufs           sync.Pool

	ctx       context.Context
	cancel    context.CancelFunc
	closed    atomic.Bool
	closeOnce sync.Once
	wg        sync.WaitGroup

	workersMu      sync.Mutex
	workers        []chan struct{}
	workersClosed  bool
	workersRunning atomic.Int64

	draining      bool
	lastIdleSweep time.Time
	idleBacklog   bool
	loopBuf       []S
	ranked        []ranked[S]
	nsCounts      map[string]int
	nsActiveCache map[string]bool
	nsGauges      map[string]struct{}

	unregisterNamespaceSeries func()
}

type ranked[S shard] struct {
	shard     S
	access    uint32
	preferred bool
}

func newPolicy[S shard](reg registry[S], knobs Knobs, deps policyDeps) *policy[S] {
	if deps.queueCap <= 0 {
		deps.queueCap = queueCapacity
	}
	if deps.memRatio == nil {
		deps.memRatio = func() float64 { return 0 }
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &policy[S]{
		policyDeps:    deps,
		reg:           reg,
		knobs:         &snapshotter{knobs: knobs.withDefaults(), logger: deps.logger},
		makeRoomCh:    make(chan item[S], deps.queueCap),
		backgroundCh:  make(chan item[S], deps.queueCap),
		wake:          make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
		nsCounts:      map[string]int{},
		nsActiveCache: map[string]bool{},
		nsGauges:      map[string]struct{}{},
	}
	p.bufs.New = func() any {
		buf := make([]S, 0, MaxMakeRoomCandidates)
		return &buf
	}
	p.unregisterNamespaceSeries = func() {}
	if deps.namespaces {
		p.unregisterNamespaceSeries = monitoring.RegisterNamespaceSeriesDeleter(p.deleteNamespaceSeries)
	}
	s := p.refresh()
	p.storeRatio(p.memRatio())
	p.adjustWorkers(s.workers)
	p.wg.Add(1)
	enterrors.GoWrapper(func() {
		defer p.wg.Done()
		p.runLoop()
	}, p.logger)
	return p
}

func (p *policy[S]) refresh() *snapshot {
	s := p.knobs.read()
	p.snap.Store(s)
	return s
}

func (p *policy[S]) storeRatio(r float64) {
	if math.IsNaN(r) {
		r = 0
	}
	p.ratio.Store(math.Float64bits(r))
}

func (p *policy[S]) cachedRatio() float64 { return math.Float64frombits(p.ratio.Load()) }

// refreshConfig re-reads the knobs now and wakes the loop to apply them.
func (p *policy[S]) refreshConfig() {
	if p.closed.Load() {
		return
	}
	p.refresh()
	p.wakeLoop()
}

func (p *policy[S]) wakeLoop() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// overLimit counts queued and in-flight unloads as already gone.
func (p *policy[S]) overLimit(s *snapshot) bool {
	if s.maxLoaded > 0 && p.reg.Loaded()-p.pending.Load() >= s.maxLoaded {
		return true
	}
	return s.high > 0 && p.cachedRatio() > s.high
}

// beforeLoad makes room for loading when a limit is reached. It never blocks
// and never fails: with no victim or a full queue the load overcommits.
func (p *policy[S]) beforeLoad(loading S) {
	if p.closed.Load() {
		return
	}
	s := p.snap.Load()
	if s.paused {
		return
	}
	if at := loading.UnloadedAt(); !at.IsZero() {
		p.metrics.reloadAfterUnload.Observe(time.Since(at).Seconds())
	}
	if !p.overLimit(s) {
		p.metrics.admitUnder.Inc()
		return
	}
	victim, pickedAt, ok := p.pickMakeRoomVictim(s, loading)
	req := db.UnloadRequest{Mode: db.UnloadModeRequest, PickedAt: pickedAt, Reindex: p.reindexSnapshot()}
	if !ok || !p.enqueue(p.makeRoomCh, item[S]{shard: victim, reason: reasonMakeRoom, req: req}) {
		p.metrics.admitOvercommit.Inc()
		return
	}
	p.metrics.admitMadeRoom.Inc()
}

// pickMakeRoomVictim claims the least recently used sampled candidate, ignoring
// residency, and returns the LastAccess it was picked at.
func (p *policy[S]) pickMakeRoomVictim(s *snapshot, loading S) (S, uint32, bool) {
	var zero S
	if s.candidates == 0 {
		return zero, 0, false
	}
	bp := p.bufs.Get().(*[]S)
	buf := p.reg.Sample((*bp)[:0], s.candidates)
	defer func() {
		clear(buf)
		*bp = buf[:0]
		p.bufs.Put(bp)
	}()
	for range buf {
		best, bestAt, bestPreferred := -1, uint32(0), false
		for i, c := range buf {
			if c == zero || c == loading || !c.UnloadCandidate() || !p.namespaceActive(c.Namespace()) {
				buf[i] = zero
				continue
			}
			preferred := p.overShare(s, c.Namespace(), -1)
			at := c.LastAccess()
			if best < 0 || (preferred && !bestPreferred) || (preferred == bestPreferred && at < bestAt) {
				best, bestAt, bestPreferred = i, at, preferred
			}
		}
		if best < 0 {
			return zero, 0, false
		}
		if victim := buf[best]; victim.TryMarkUnloadPending() {
			return victim, bestAt, true
		}
		buf[best] = zero
	}
	return zero, 0, false
}

// overShare reports whether namespace holds more than its share; count < 0 reads it from the registry.
func (p *policy[S]) overShare(s *snapshot, namespace string, count int) bool {
	if !p.namespaces || s.nsShare <= 0 || s.maxLoaded <= 0 {
		return false
	}
	if count < 0 {
		count = p.reg.NamespaceLoaded(namespace)
	}
	return float64(count) > s.nsShare*float64(s.maxLoaded)
}

func (p *policy[S]) namespaceActive(namespace string) bool {
	return namespace == "" || p.nsActive == nil || p.nsActive(namespace)
}

// enqueue hands a claimed shard to the workers without blocking; on failure,
// a closed policy included, it releases the claim.
func (p *policy[S]) enqueue(ch chan item[S], it item[S]) bool {
	p.enqueuing.Add(1)
	defer p.enqueuing.Add(-1)
	if p.closed.Load() {
		it.shard.ClearUnloadPending()
		return false
	}
	p.pending.Add(1)
	select {
	case ch <- it:
		return true
	default:
		p.pending.Add(-1)
		it.shard.ClearUnloadPending()
		return false
	}
}

// deleteNamespaceSeries drops a removed namespace's series; it runs on the RAFT
// apply and touches only the collectors, never loop-owned state.
func (p *policy[S]) deleteNamespaceSeries(namespace string) {
	p.metrics.loaded.DeleteLabelValues(namespace)
	p.metrics.unloads.DeletePartialMatch(prometheus.Labels{"namespace": namespace})
}

// setReindexSource installs the snapshot source and wakes the loop to take the first snapshot.
func (p *policy[S]) setReindexSource(src reindexSource) {
	p.reindexSrc.Store(&src)
	p.wakeLoop()
}

func (p *policy[S]) reindexSnapshot() db.ReindexActivity {
	if a := p.reindex.Load(); a != nil {
		return *a
	}
	return db.ReindexActivity{}
}

// refreshReindex takes this tick's snapshot outside every shard lock, logging
// a failure once when it starts and once when it ends.
func (p *policy[S]) refreshReindex() {
	src := p.reindexSrc.Load()
	if src == nil {
		return
	}
	ctx, cancel := context.WithTimeout(p.ctx, reindexSnapshotTimeout)
	a, err := (*src)(ctx)
	cancel()
	p.reindex.Store(&a)
	failed := err != nil || !a.Usable()
	switch {
	case failed && !p.reindexFailed:
		p.logger.WithField("action", "lazy_shard_unloading_reindex_snapshot").
			Warnf("cannot tell which shards a reindex works on; unloads are refused until it succeeds: %v", err)
	case !failed && p.reindexFailed:
		p.logger.WithField("action", "lazy_shard_unloading_reindex_snapshot").
			Info("reindex snapshot available again; unloads resume")
	default:
	}
	p.reindexFailed = failed
}

// reserveWarmupLoad counts in-flight warmup loads as loaded, so the warmups of
// every index together stop at the limit. Paused, warmup loads as plain lazy loading.
func (p *policy[S]) reserveWarmupLoad() bool {
	s := p.snap.Load()
	if !s.paused {
		if s.high > 0 && p.memRatio() >= s.high {
			return false
		}
		for s.maxLoaded > 0 {
			reserved := p.warmupReserved.Load()
			if p.reg.Loaded()+reserved >= s.maxLoaded {
				return false
			}
			if p.warmupReserved.CompareAndSwap(reserved, reserved+1) {
				return true
			}
		}
	}
	p.warmupReserved.Add(1)
	return true
}

func (p *policy[S]) releaseWarmupLoad() { p.warmupReserved.Add(-1) }

// close stops the loop and the workers. Queued handoffs are released before it
// waits, so a close that times out on an unload in flight strands no claim.
func (p *policy[S]) close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.unregisterNamespaceSeries()
		p.closed.Store(true)
		p.cancel()
		p.workersMu.Lock()
		p.workersClosed = true
		for _, stop := range p.workers {
			close(stop)
		}
		p.workers = nil
		p.workersMu.Unlock()
		// An enqueue that read closed as false is past its check; once none
		// is, every later one refuses, so this drain is the last.
		for p.enqueuing.Load() > 0 {
			runtime.Gosched()
		}
		p.drainQueues()
	})
	done := make(chan struct{})
	enterrors.GoWrapper(func() {
		p.wg.Wait()
		close(done)
	}, p.logger)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *policy[S]) drainQueues() {
	for {
		select {
		case it := <-p.makeRoomCh:
			p.release(it)
		case it := <-p.backgroundCh:
			p.release(it)
		default:
			return
		}
	}
}

func (p *policy[S]) release(it item[S]) {
	it.shard.ClearUnloadPending()
	p.pending.Add(-1)
}
