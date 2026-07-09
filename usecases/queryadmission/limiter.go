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

// Package queryadmission bounds the aggregate concurrency of the expensive,
// goroutine-fanning phases of object search (filter evaluation, keyword/BM25
// ranking) on a single node, so a traffic spike degrades gracefully instead of
// exhausting the Go scheduler.
//
// It gates Shard.ObjectSearch and the allow-list phase of
// Shard.ObjectVectorSearch; filtered aggregations and batch-delete-by-filter
// are not yet gated. It composes with entities/concurrency's per-query budget:
// that bounds one query's fan-out, this bounds the aggregate across queries.
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
// full; it maps to HTTP 429 / gRPC ResourceExhausted so the coordinator backs off.
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
	//
	// Flipping to true takes effect for NEW arrivals immediately, but does not
	// actively wake waiters already parked in the queue: DynamicValue exposes no
	// change hook, so there is nothing to fire on the flip. Those waiters still
	// make progress — each drains via a normal capacity release from an
	// in-flight query (or leaves via its own ctx cancellation). Because new
	// arrivals now bypass admission entirely, in-flight grants only fall, so the
	// queue drains monotonically to empty and never re-fills.
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
// Fairness: waiters are woken strictly FIFO, and grantLocked floors a non-zero
// grant at 1, so the head waiter always drains once any capacity frees. A
// waiter cancelled the instant it wakes hands its grant back (cancelWaiter)
// rather than leaking it.
//
// Nesting: re-entrant admits on the SAME node inherit the parent's grant
// (admissionKey) instead of acquiring a second one, so a nested cross-reference
// search cannot deadlock against the parent's own budget. A grant IS, however,
// held while a ref-filter fans out to a REMOTE node and blocks on that node's
// admission. Liveness under mutual cross-node saturation does not rest on
// releasing the grant; it rests on the bounded wait queue (arrivals past
// MaxQueue are shed), the coordinator's bounded remote-retry backoff, and
// request deadlines. The cost is degraded latency, not deadlock. The nested
// cross-reference fan-out is itself bounded by a count (QueryNestedRefLimit),
// not a deadline.
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
// On success it returns a ctx seeded with the grant (and a re-entrancy
// marker) and an idempotent release func that must be called exactly once.
// It returns ErrOverloaded if the node and wait queue are both full, or the
// ctx error if ctx is/becomes done before a grant.
//
// Admit is nil-receiver-safe: a nil Limiter, disabled limiter, or re-entrant
// call all return the ctx unchanged and a no-op release. Both production
// constructors wire the limiter unconditionally — db.New always builds it
// (adapters/repos/db/repo.go) and it is threaded into every IndexConfig
// (adapters/repos/db/init.go, migrator.go) — so a nil receiver never occurs in
// production; it is purely a test-fixture affordance that lets a test build an
// IndexConfig without wiring a limiter.
func (l *Limiter) Admit(ctx context.Context, want int) (context.Context, func(), error) {
	if l == nil || l.disabled() {
		return ctx, func() {}, nil
	}
	// Never admit an already-expired ctx: it would take a grant only to be
	// abandoned immediately downstream. Checked before the re-entrancy
	// passthrough so a nested call under an already-cancelled parent short-
	// circuits instead of proceeding on a dead ctx.
	if err := ctx.Err(); err != nil {
		return ctx, func() {}, err
	}
	// Re-entrant call: inherit the parent grant rather than risk the child
	// waiting on capacity the parent already holds.
	if ctx.Value(admissionKey{}) != nil {
		return ctx, func() {}, nil
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

// grantLocked hands out min(want, max(remaining/4, 1)): a degrade-first curve
// so early queries get a generous share while later ones taper off, with a
// floor of 1 (while capacity remains) guaranteeing progress. 0 means saturated.
// Caller must hold l.mu.
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
