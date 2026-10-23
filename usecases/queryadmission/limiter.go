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

// Package queryadmission bounds the aggregate concurrency of the
// goroutine-fanning phases of object search on a single node, so a traffic
// spike degrades gracefully instead of exhausting the Go scheduler.
//
// It gates Shard.ObjectSearch when a filter or keyword ranking is present and
// the whole of Shard.ObjectVectorSearch. A grant is taken per shard searched
// on this node, since each shard search runs its own worker pool. Filtered
// aggregations and batch-delete-by-filter are not yet gated. The grant seeds
// entities/concurrency's per-query budget, which bounds one query's fan-out
// while this bounds the aggregate across queries.
//
// The limiter owns no goroutines; all coordination is a mutex plus per-waiter
// buffered channels.
package queryadmission

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/weaviate/weaviate/entities/concurrency"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// ErrOverloaded is returned by Admit when the node and wait queue are both
// full. Ingress maps it to HTTP 429 / gRPC ResourceExhausted.
var ErrOverloaded = errors.New("query admission: node overloaded, request shed (429)")

// Config configures a Limiter. The zero value is valid; <=0 fields resolve to
// the defaults in New.
type Config struct {
	// Capacity is the goroutine-equivalent budget shared by all admitted queries.
	Capacity int
	// MaxQueue is how many queries may wait for capacity before arrivals are shed.
	MaxQueue int
	// Disabled is a live kill switch; nil reads as false (enabled). Flipping it
	// on does not wake parked waiters (DynamicValue has no change hook); they
	// drain via normal releases or ctx cancellation, and since new arrivals
	// bypass admission the queue never re-fills.
	Disabled *configRuntime.DynamicValue[bool]
}

// waiter is a query parked in the FIFO queue. ready is buffered (cap 1) so the
// releaser can hand over a grant under the limiter mutex without blocking.
type waiter struct {
	want  int64
	ready chan int64
}

// Limiter is a node-level query admission controller, safe for concurrent use.
//
// Nesting: same-node re-entrant admits run on a fixed copy of the parent's
// grant (admissionKey), so they can't deadlock locally or resize it. A grant
// IS held while fanning out to a REMOTE node's admission; liveness there rests
// on the bounded wait queue, the coordinator's bounded retry backoff, and
// request deadlines, not on releasing the grant, so the cost is latency, not
// deadlock.
type Limiter struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	inflight int64
	waiters  *list.List // FIFO of *waiter
	maxQueue int
	cfg      Config

	usedGauge     prometheus.Gauge
	inflightGauge prometheus.Gauge
	waitingGauge  prometheus.Gauge
	shedTotal     prometheus.Counter
	grantSize     prometheus.Histogram
	waitDuration  prometheus.Histogram
}

// admissionKey marks a ctx that already holds a grant on this node, so a
// nested Admit takes no grant of its own.
type admissionKey struct{}

// New builds a Limiter and registers its metrics with reg (may be a noop).
func New(reg prometheus.Registerer, cfg Config) *Limiter {
	capacity := cfg.Capacity
	if capacity <= 0 {
		capacity = concurrency.TimesGOMAXPROCS(16)
	}
	maxQueue := cfg.MaxQueue
	if maxQueue <= 0 {
		maxQueue = concurrency.TimesGOMAXPROCS(10)
	}

	r := promauto.With(reg)
	return &Limiter{
		capacity: int64(capacity),
		waiters:  list.New(),
		maxQueue: maxQueue,
		cfg:      cfg,
		usedGauge: r.NewGauge(prometheus.GaugeOpts{
			Name: "query_admission_used_budget",
			Help: "Currently granted goroutine-equivalent budget across all admitted queries.",
		}),
		inflightGauge: r.NewGauge(prometheus.GaugeOpts{
			Name: "query_admission_inflight",
			Help: "Number of queries currently holding an admission grant.",
		}),
		waitingGauge: r.NewGauge(prometheus.GaugeOpts{
			Name: "query_admission_waiting",
			Help: "Number of queries currently waiting for admission.",
		}),
		shedTotal: r.NewCounter(prometheus.CounterOpts{
			Name: "query_admission_shed_total",
			Help: "Total queries shed because the node was overloaded and the wait queue was full.",
		}),
		grantSize: r.NewHistogram(prometheus.HistogramOpts{
			Name:    "query_admission_grant_size",
			Help:    "Distribution of granted budget per admitted query.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 11), // 1,2,4,...,1024
		}),
		waitDuration: r.NewHistogram(prometheus.HistogramOpts{
			Name:    "query_admission_wait_duration_seconds",
			Help:    "Time queued queries spent waiting before admission or cancellation.",
			Buckets: prometheus.DefBuckets,
		}),
	}
}

func (l *Limiter) disabled() bool {
	return l.cfg.Disabled.Get()
}

// Admit acquires a share of the node's query budget for the caller.
//
// On success it returns a ctx seeded with the grant (and a re-entrancy
// marker) and an idempotent release func that must be called exactly once.
// It returns ErrOverloaded if the node and wait queue are both full, or the
// ctx error if ctx is/becomes done before a grant.
//
// Admit is nil-receiver-safe. A nil or disabled Limiter takes no grant and
// returns a no-op release. Only tests pass a nil one. A re-entrant call on a
// nil, disabled or enabled Limiter also takes no grant and returns a no-op
// release. Its ctx holds a fixed copy of the parent's budget, so resizing it
// reaches neither the parent nor the pool.
func (l *Limiter) Admit(ctx context.Context, want int) (context.Context, func(), error) {
	if l == nil || l.disabled() {
		// a query admitted before the kill switch flipped still owns a pooled budget
		if ctx.Value(admissionKey{}) != nil {
			ctx = concurrency.CtxWithBudget(ctx, concurrency.BudgetFromCtx(ctx, want))
		}
		return ctx, func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
	}
	if ctx.Value(admissionKey{}) != nil {
		// Return a fixed copy, so parallel nested searches cannot resize the parent's grant.
		return concurrency.CtxWithBudget(ctx, concurrency.BudgetFromCtx(ctx, want)), func() {}, nil
	}

	w64 := int64(want)
	if w64 < 1 {
		w64 = 1
	}

	l.mu.Lock()
	if g := l.grantLocked(w64); g > 0 {
		l.used += g
		l.inflight++
		l.observeGrantLocked(g)
		l.updateGaugesLocked()
		l.mu.Unlock()
		ctx, release := l.admitted(ctx, g)
		return ctx, release, nil
	}

	if l.waiters.Len() >= l.maxQueue {
		l.shedTotal.Inc()
		l.mu.Unlock()
		return ctx, func() {}, ErrOverloaded
	}

	w := &waiter{want: w64, ready: make(chan int64, 1)}
	elem := l.waiters.PushBack(w)
	l.updateGaugesLocked()
	l.mu.Unlock()

	enqueuedAt := time.Now()
	select {
	case <-ctx.Done():
		l.cancelWaiter(w, elem)
		l.waitDuration.Observe(time.Since(enqueuedAt).Seconds())
		return ctx, func() {}, ctx.Err()
	case g := <-w.ready:
		l.waitDuration.Observe(time.Since(enqueuedAt).Seconds())
		ctx, release := l.admitted(ctx, g)
		return ctx, release, nil
	}
}

// cancelWaiter removes a cancelled waiter from the queue. If the waiter was
// concurrently woken (its grant already landed on w.ready under the same mutex),
// the grant is handed straight back so a wake/cancel race cannot leak capacity.
func (l *Limiter) cancelWaiter(w *waiter, elem *list.Element) {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case g := <-w.ready:
		// Raced with a wake: reclaim the grant we will never use.
		l.releaseLocked(g)
	default:
		l.waiters.Remove(elem)
	}
	l.updateGaugesLocked()
}

// grantLocked hands out min(want, max(remaining/4, 1)): a degrade-first curve
// so early queries get a generous share while later ones taper off, with a
// floor of 1 (while capacity remains) guaranteeing progress. 0 means saturated.
func (l *Limiter) grantLocked(want int64) int64 {
	remaining := l.capacity - l.used
	if remaining <= 0 {
		return 0
	}
	g := remaining / 4
	if g < 1 {
		g = 1
	}
	if g > want {
		g = want
	}
	return g
}

// releaseLocked returns a finished query's g units to the pool.
func (l *Limiter) releaseLocked(g int64) {
	l.used -= g
	l.inflight--
	l.wakeLocked()
}

// wakeLocked wakes waiters FIFO, granting each the current curve amount until
// capacity is exhausted or the queue empties.
func (l *Limiter) wakeLocked() {
	for l.waiters.Len() > 0 {
		front := l.waiters.Front()
		w := front.Value.(*waiter)
		gg := l.grantLocked(w.want)
		if gg == 0 {
			break
		}
		l.used += gg
		l.inflight++
		l.waiters.Remove(front)
		l.observeGrantLocked(gg)
		w.ready <- gg // buffered cap 1: never blocks under the mutex
	}
}

// Resize moves an admitted query's holding of held units toward n, floored at
// 1. A shrink wakes waiters. A grow takes only free units, of which wakeLocked
// leaves none while anyone is queued, so a running query never jumps the queue
// or waits.
func (l *Limiter) Resize(held, n int) int {
	n = max(n, 1)
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case n < held:
		l.used -= int64(held - n)
		l.wakeLocked()
	case n > held:
		n = held + int(min(int64(n-held), l.capacity-l.used))
		l.used += int64(n - held)
	default:
		n = held
	}
	l.updateGaugesLocked()
	return n
}

// admitted seeds ctx with a grant of g units this limiter can resize, and marks
// it with admissionKey. The idempotent release returns what the budget holds
// then, not g.
func (l *Limiter) admitted(ctx context.Context, g int64) (context.Context, func()) {
	budget := concurrency.NewBudget(l, int(g))
	ctx = concurrency.CtxWithResizableBudget(ctx, budget)
	ctx = context.WithValue(ctx, admissionKey{}, struct{}{})

	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			held := int64(budget.Detach())
			l.mu.Lock()
			l.releaseLocked(held)
			l.updateGaugesLocked()
			l.mu.Unlock()
		})
	}
}

func (l *Limiter) observeGrantLocked(g int64) {
	// Histograms are internally synchronized; observing under l.mu keeps the
	// per-grant accounting and its metric in lockstep without extra locking.
	l.grantSize.Observe(float64(g))
}

func (l *Limiter) updateGaugesLocked() {
	l.usedGauge.Set(float64(l.used))
	l.inflightGauge.Set(float64(l.inflight))
	l.waitingGauge.Set(float64(l.waiters.Len()))
}
