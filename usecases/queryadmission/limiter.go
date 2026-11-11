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
	group *shardGroup
	ready chan int64
}

// shardGroup marks the local shard searches of one query, so the limiter can
// grant them evenly instead of each sibling taking a quarter of what the
// previous one left. Its fields are guarded by Limiter.mu.
type shardGroup struct {
	n     int64
	share int64 // grant of a sibling admitted without queueing, 0 until the first one is
	// grants of the siblings woken in the running wakeLocked pass, 0 outside it
	passSum   int64
	passCount int64
}

type shardGroupKey struct{}

// WithShardGroup returns a ctx under which Admit grants a query's sibling
// shard searches even shares, sized for n running at once. Admit every
// sibling under that ctx with the same want. For n < 2 it returns ctx as is.
func WithShardGroup(ctx context.Context, n int) context.Context {
	if n < 2 {
		return ctx
	}
	return context.WithValue(ctx, shardGroupKey{}, &shardGroup{n: int64(n)})
}

func shardGroupFromCtx(ctx context.Context) *shardGroup {
	sg, _ := ctx.Value(shardGroupKey{}).(*shardGroup)
	return sg
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

	sg := shardGroupFromCtx(ctx)
	l.mu.Lock()
	if g := l.grantLocked(w64, sg); g > 0 {
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

	w := &waiter{want: w64, group: sg, ready: make(chan int64, 1)}
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

// grantLocked returns the curve grant, or for a sibling of sg the even share
// fixed by the first sibling granted without queueing, capped at what
// remains. 0 means saturated.
func (l *Limiter) grantLocked(want int64, sg *shardGroup) int64 {
	remaining := l.capacity - l.used
	if remaining <= 0 {
		return 0
	}
	if sg == nil {
		return curveGrant(remaining, want)
	}
	if sg.share == 0 {
		sg.share = evenShare(remaining, want, sg.n)
	}
	return min(sg.share, remaining)
}

// curveGrant is the degrade-first curve min(want, max(remaining/4, 1)). Early
// queries get a generous share and later ones taper off, while the floor of 1
// guarantees progress as long as capacity remains.
func curveGrant(remaining, want int64) int64 {
	return min(want, max(remaining/4, 1))
}

// evenShare is what the curve grants n queries admitted back to back from
// remaining, averaged and floored at 1.
func evenShare(remaining, want, n int64) int64 {
	var sum int64
	for i := int64(0); i < n && remaining > 0; i++ {
		g := curveGrant(remaining, want)
		sum += g
		remaining -= g
	}
	return max(sum/n, 1)
}

// releaseLocked returns a released or abandoned grant's g units to the pool, then wakes waiters.
func (l *Limiter) releaseLocked(g int64) {
	l.used -= g
	l.inflight--
	l.wakeLocked()
}

// wakeLocked wakes waiters FIFO, granting each the current curve amount until
// capacity is exhausted or the queue empties. Siblings woken together then
// split their grants evenly, so the pass hands out the same total.
func (l *Limiter) wakeLocked() {
	if l.waiters.Len() == 0 {
		return
	}
	// buf covers the usual pass without a heap allocation.
	var buf [16]wokenWaiter
	woken := buf[:0]
	for l.waiters.Len() > 0 {
		front := l.waiters.Front()
		w := front.Value.(*waiter)
		g := l.grantLocked(w.want, nil)
		if g == 0 {
			break
		}
		l.used += g
		l.inflight++
		l.waiters.Remove(front)
		if sg := w.group; sg != nil {
			sg.passSum += g
			sg.passCount++
		}
		woken = append(woken, wokenWaiter{w: w, grant: g})
	}

	for _, ww := range woken {
		if sg := ww.w.group; sg != nil {
			// the last sibling takes exactly the rest, leaving both counters at 0
			ww.grant = (sg.passSum + sg.passCount - 1) / sg.passCount
			sg.passSum -= ww.grant
			sg.passCount--
		}
		l.observeGrantLocked(ww.grant)
		ww.w.ready <- ww.grant // buffered cap 1: never blocks under the mutex
	}
}

// wokenWaiter is a waiter woken by one wakeLocked pass and its grant so far.
type wokenWaiter struct {
	w     *waiter
	grant int64
}

// budgetPool is the Limiter as an admitted grant's concurrency.Pool. Unexported,
// so only a grant's own Budget, which passes what it holds, can resize it.
type budgetPool struct{ l *Limiter }

func (p budgetPool) Resize(held, n int) int { return p.l.resize(held, n) }

// resize moves an admitted query's holding of held units toward n, floored at
// 1. A shrink happens only while a query waits, and wakes it: with nobody to
// take the units, giving them back would only slow the query down. A grow takes
// only free units, of which wakeLocked leaves none while anyone is queued, so a
// running query never jumps the queue or waits.
func (l *Limiter) resize(held, n int) int {
	n = max(n, 1)
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case n < held && l.waiters.Len() == 0:
		n = held
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
	budget := concurrency.NewBudget(budgetPool{l}, int(g))
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
