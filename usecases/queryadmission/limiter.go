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

// Package queryadmission provides a node-level admission control point for the
// expensive, goroutine-fanning phases of an object search (filter evaluation
// and keyword/BM25 ranking). It bounds the aggregate concurrency of the object
// searches that pass through those phases on a single node, so a traffic spike
// degrades gracefully instead of exhausting the Go scheduler's OS-thread budget.
//
// Scope: only Shard.ObjectSearch and the allow-list phase of
// Shard.ObjectVectorSearch are gated. Filtered aggregations and
// batch-delete-by-filter run the same inverted fan-out but are not yet admitted
// through this control (tracked as follow-ups). Each of those is still bounded
// per-query by the concurrency budget, just not in the aggregate.
//
// It composes with entities/concurrency's per-query budget: P1a bounds the
// fan-out of one query, this package (P1b) bounds the fan-out of the aggregate
// by handing each admitted query a share of a fixed node capacity.
//
// The limiter owns zero goroutines. All coordination happens under a single
// mutex plus per-waiter buffered channels; there is nothing to start and
// nothing to shut down.
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

// ErrOverloaded is returned by Admit when the node is at capacity and the wait
// queue is full. It is surfaced to clients as HTTP 429 / gRPC ResourceExhausted
// so the coordinator's retryer applies bounded exponential backoff.
var ErrOverloaded = errors.New("query admission: node overloaded, request shed (429)")

// Config configures a Limiter. The zero value is valid and yields the auto
// defaults documented per field.
type Config struct {
	// Capacity is the total goroutine-equivalent budget shared across all
	// concurrent admitted queries. <=0 resolves to concurrency.TimesGOMAXPROCS(16).
	Capacity int
	// MaxQueue is the maximum number of queries allowed to wait for capacity
	// before further arrivals are shed. <=0 resolves to
	// concurrency.TimesGOMAXPROCS(10).
	MaxQueue int
	// Disabled is a live kill switch. When it reports true, Admit is a
	// passthrough (no accounting, no shedding). A nil pointer reads as false,
	// i.e. admission control is enabled by default.
	Disabled *configRuntime.DynamicValue[bool]
}

// waiter is a query parked in the FIFO queue. ready is buffered (cap 1) so the
// releaser can hand over a grant while holding the limiter mutex without ever
// blocking; the woken query receives the granted amount on it.
type waiter struct {
	want  int64
	ready chan int64
}

// Limiter is a node-level query admission controller. It is safe for
// concurrent use. See the package doc for the model.
//
// Fairness and progress:
//   - Waiters are woken strictly in FIFO order, so no query is starved.
//   - The head waiter is guaranteed at least 1 unit whenever any capacity is
//     free (grantLocked floors a non-zero grant at 1), so the queue always
//     drains.
//   - A query woken but simultaneously cancelled hands its grant straight back
//     (see Admit's cancellation branch), so a wake/cancel race never leaks
//     capacity.
//
// Held grants and nesting:
//   - A grant is never held while blocking on another node's admission. The
//     one exception is the deadline-bounded nested cross-reference fan-out,
//     which re-enters Admit on the same node; re-entrant calls inherit the
//     parent grant instead of acquiring a second one (see the admissionKey
//     marker), so nesting cannot deadlock against the parent's own budget.
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

// admissionKey marks a ctx that already holds a grant on this node. Its
// presence makes Admit re-entrant: nested admits inherit the parent grant.
type admissionKey struct{}

// New builds a Limiter and registers its metrics with reg. reg may be a noop
// registerer. cfg's zero fields resolve to the documented auto defaults.
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

// disabled reports whether the live kill switch is engaged. A nil DynamicValue
// reads as false (enabled), so admission control is on by default.
func (l *Limiter) disabled() bool {
	return l.cfg.Disabled.Get()
}

// Admit acquires a share of the node's query budget for the caller.
//
// On success it returns a ctx seeded with the granted budget (and an internal
// re-entrancy marker), a release func that must be called exactly once when the
// admitted work completes, and a nil error. release is idempotent.
//
// It returns ErrOverloaded when the node is at capacity and the wait queue is
// full, or the ctx error if the ctx is (or becomes) done before a grant.
//
// Admit is nil-receiver-safe: a nil Limiter, a disabled limiter, and a
// re-entrant call all return an unchanged ctx and a no-op release. This lets
// test fixtures build an IndexConfig without wiring a limiter.
func (l *Limiter) Admit(ctx context.Context, want int) (context.Context, func(), error) {
	if l == nil || l.disabled() {
		return ctx, func() {}, nil
	}
	// Re-entrant call (nested cross-reference fan-out on this same node):
	// inherit the parent grant rather than acquire a second one, which would
	// risk the child waiting on capacity the parent already holds.
	if ctx.Value(admissionKey{}) != nil {
		return ctx, func() {}, nil
	}
	// Never admit an already-expired ctx: it would take a grant only to be
	// abandoned immediately downstream.
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
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
		return admittedCtx(ctx, g), l.releaseFn(g), nil
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
		return admittedCtx(ctx, g), l.releaseFn(g), nil
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

// grantLocked returns the budget to hand out for a request wanting want units,
// given current usage. It is a degrade-first curve: min(want, max(remaining/4,
// 1)). The remaining/4 share means early queries under contention get a
// generous slice while later ones get progressively less, and the floor of 1
// (whenever any capacity is free) is the per-query progress guarantee. Returns
// 0 only when the node is fully saturated. Caller must hold l.mu.
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

// releaseLocked returns g units to the pool and then wakes waiters FIFO,
// granting each the current curve amount until capacity is exhausted or the
// queue empties. Caller must hold l.mu.
func (l *Limiter) releaseLocked(g int64) {
	l.used -= g
	l.inflight--
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

// releaseFn returns an idempotent release closure for a grant of g units.
func (l *Limiter) releaseFn(g int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.releaseLocked(g)
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

// admittedCtx seeds the granted budget for the read path to consume and marks
// the ctx so nested admits on this node inherit the grant.
func admittedCtx(ctx context.Context, g int64) context.Context {
	ctx = concurrency.CtxWithBudget(ctx, int(g))
	return context.WithValue(ctx, admissionKey{}, struct{}{})
}
