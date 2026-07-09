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

package queryadmission

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/concurrency"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

// --- test helpers -----------------------------------------------------------

func newLimiter(t *testing.T, cfg Config) *Limiter {
	t.Helper()
	// A fresh registry per limiter avoids duplicate-registration panics.
	return New(prometheus.NewRegistry(), cfg)
}

func (l *Limiter) usedForTest() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used
}

func (l *Limiter) inflightForTest() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inflight
}

func (l *Limiter) waitersLenForTest() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.waiters.Len()
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// saturate admits n want-1 requests, fully consuming a capacity-n limiter, and
// returns their release funcs.
func saturate(t *testing.T, l *Limiter, n int) []func() {
	t.Helper()
	releases := make([]func(), 0, n)
	for i := 0; i < n; i++ {
		_, rel, err := l.Admit(context.Background(), 1)
		require.NoError(t, err)
		releases = append(releases, rel)
	}
	return releases
}

// --- grant curve ------------------------------------------------------------

func TestGrantCurve(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 608})
	want := int64(76)
	// The degrade-first curve min(want, max(remaining/4, 1)) evaluated as a
	// single query keeps re-granting against shrinking remaining capacity.
	expected := []int64{76, 76, 76, 76, 76, 57, 42, 32, 24, 18, 13, 10, 8, 6, 4, 3, 2, 2, 1}

	l.mu.Lock()
	defer l.mu.Unlock()
	for i, exp := range expected {
		g := l.grantLocked(want)
		require.Equalf(t, exp, g, "grant %d", i)
		l.used += g
	}
}

func TestGrantFloorOfOne(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 10})
	l.mu.Lock()
	defer l.mu.Unlock()

	// remaining below 4 still yields a grant of at least 1 for a huge want.
	l.used = 9 // remaining 1
	require.Equal(t, int64(1), l.grantLocked(1000))
	l.used = 7 // remaining 3
	require.Equal(t, int64(1), l.grantLocked(1000))
	l.used = 10 // remaining 0 -> saturated
	require.Equal(t, int64(0), l.grantLocked(1000))
}

// --- uncontended path -------------------------------------------------------

func TestAdmitFullGrantUncontended(t *testing.T) {
	// capacity/4 >= want, so an uncontended query receives its full ask.
	l := newLimiter(t, Config{Capacity: 400})

	ctx, release, err := l.Admit(context.Background(), 76)
	require.NoError(t, err)
	require.Equal(t, 76, concurrency.BudgetFromCtx(ctx, -1))
	require.Equal(t, int64(76), l.usedForTest())
	require.Equal(t, int64(1), l.inflightForTest())

	release()
	require.Equal(t, int64(0), l.usedForTest())
	require.Equal(t, int64(0), l.inflightForTest())
}

func TestReleaseIsIdempotent(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 400})
	_, release, err := l.Admit(context.Background(), 8)
	require.NoError(t, err)
	require.Equal(t, int64(8), l.usedForTest())

	release()
	release() // second call must be a no-op, not a double-decrement
	require.Equal(t, int64(0), l.usedForTest())
}

// --- queueing & FIFO --------------------------------------------------------

func TestAdmitQueuesWhenSaturated(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 4, MaxQueue: 8})
	releases := saturate(t, l, 4)
	require.Equal(t, int64(4), l.usedForTest())

	admitted := make(chan struct{})
	go func() {
		_, rel, err := l.Admit(context.Background(), 1)
		if err == nil {
			rel()
		}
		close(admitted)
	}()

	waitFor(t, func() bool { return l.waitersLenForTest() == 1 })
	select {
	case <-admitted:
		t.Fatal("admit returned while node saturated")
	case <-time.After(50 * time.Millisecond):
	}

	releases[0]() // frees capacity, wakes the waiter
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not woken after release")
	}

	for _, r := range releases[1:] {
		r()
	}
	require.Equal(t, int64(0), l.usedForTest())
	require.Equal(t, 0, l.waitersLenForTest())
}

func TestFIFOWakeOnRelease(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 4, MaxQueue: 8})
	releases := saturate(t, l, 4)

	woke := make(chan int, 2)
	start := func(id int) {
		_, rel, err := l.Admit(context.Background(), 1)
		require.NoError(t, err)
		woke <- id
		rel()
	}

	// Enqueue W1, wait until it is parked, then W2 — deterministic FIFO order.
	go start(1)
	waitFor(t, func() bool { return l.waitersLenForTest() == 1 })
	go start(2)
	waitFor(t, func() bool { return l.waitersLenForTest() == 2 })

	// One release frees exactly 1 unit: only the head waiter (W1) can be woken.
	releases[0]()
	select {
	case id := <-woke:
		require.Equal(t, 1, id, "head-of-line waiter must wake first")
	case <-time.After(2 * time.Second):
		t.Fatal("no waiter woken")
	}

	// W1's release (from start) plus the remaining grants drain W2.
	for _, r := range releases[1:] {
		r()
	}
	select {
	case id := <-woke:
		require.Equal(t, 2, id)
	case <-time.After(2 * time.Second):
		t.Fatal("second waiter not woken")
	}
	require.Equal(t, int64(0), l.usedForTest())
}

// --- cancellation & shedding ------------------------------------------------

func TestCancelledWhileQueuedLeaksNothing(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 4, MaxQueue: 8})
	releases := saturate(t, l, 4)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, rel, err := l.Admit(ctx, 1)
		if err == nil {
			rel()
		}
		done <- err
	}()

	waitFor(t, func() bool { return l.waitersLenForTest() == 1 })
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter did not return")
	}

	// The cancelled waiter took no capacity and left the queue.
	require.Equal(t, int64(4), l.usedForTest())
	require.Equal(t, 0, l.waitersLenForTest())

	for _, r := range releases {
		r()
	}
	require.Equal(t, int64(0), l.usedForTest())
}

func TestExpiredCtxNeverAdmitted(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 400})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, release, err := l.Admit(ctx, 4)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int64(0), l.usedForTest())
	release() // no-op, must not panic
}

func TestShedAtMaxQueue(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 4, MaxQueue: 2})
	releases := saturate(t, l, 4)

	// Fill the queue with two blocked waiters.
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelA()
	defer cancelB()
	go l.Admit(ctxA, 1) //nolint:errcheck // blocks until cancelled
	go l.Admit(ctxB, 1) //nolint:errcheck // blocks until cancelled
	waitFor(t, func() bool { return l.waitersLenForTest() == 2 })

	// Queue is full: the next arrival is shed immediately.
	_, release, err := l.Admit(context.Background(), 1)
	require.ErrorIs(t, err, ErrOverloaded)
	release() // no-op

	cancelA()
	cancelB()
	waitFor(t, func() bool { return l.waitersLenForTest() == 0 })
	for _, r := range releases {
		r()
	}
	require.Equal(t, int64(0), l.usedForTest())
}

// TestWakeCancelRaceHandsGrantBack pins the race where a release wakes a
// waiter that is simultaneously cancelling; cancelWaiter must reclaim the
// grant instead of leaking it.
func TestWakeCancelRaceHandsGrantBack(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 4, MaxQueue: 8})
	releases := saturate(t, l, 4)

	// Park a waiter exactly as Admit would.
	w := &waiter{want: 1, ready: make(chan int64, 1)}
	l.mu.Lock()
	elem := l.waiters.PushBack(w)
	l.mu.Unlock()

	// Wake it: used 4->3 (release) then 3->4 (grant to w), grant on w.ready.
	releases[0]()
	require.Equal(t, int64(4), l.usedForTest())

	// The query races Done and abandons the grant; cancelWaiter reclaims it.
	l.cancelWaiter(w, elem)
	require.Equal(t, int64(3), l.usedForTest())
	require.Equal(t, 0, l.waitersLenForTest())

	for _, r := range releases[1:] {
		r()
	}
	require.Equal(t, int64(0), l.usedForTest())
}

// --- re-entrancy, nil, disabled ---------------------------------------------

func TestReEntrantCtxSkipsAcquire(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 400})
	ctx1, rel1, err := l.Admit(context.Background(), 4)
	require.NoError(t, err)
	usedAfterFirst := l.usedForTest()
	require.Positive(t, usedAfterFirst)

	// A nested admit on the granted ctx inherits the parent grant.
	ctx2, rel2, err := l.Admit(ctx1, 4)
	require.NoError(t, err)
	require.Equal(t, usedAfterFirst, l.usedForTest(), "re-entrant admit must not acquire again")
	// Budget carried by the child ctx is the parent's grant, unchanged.
	require.Equal(t, concurrency.BudgetFromCtx(ctx1, -1), concurrency.BudgetFromCtx(ctx2, -2))

	rel2() // no-op release for the inherited grant
	require.Equal(t, usedAfterFirst, l.usedForTest())
	rel1()
	require.Equal(t, int64(0), l.usedForTest())
}

func TestNilReceiverPassthrough(t *testing.T) {
	var l *Limiter
	ctx := context.Background()
	got, release, err := l.Admit(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, ctx, got)
	require.NotNil(t, release)
	release() // must not panic
}

func TestDisabledPassthrough(t *testing.T) {
	l := newLimiter(t, Config{Capacity: 4, Disabled: configRuntime.NewDynamicValue(true)})
	ctx := context.Background()
	got, release, err := l.Admit(ctx, 4)
	require.NoError(t, err)
	require.Equal(t, ctx, got, "disabled limiter must not seed budget")
	require.Equal(t, int64(0), l.usedForTest(), "disabled limiter must not account")
	release()
	require.Equal(t, int64(0), l.usedForTest())
}

func TestDynamicValueFlipMidFlight(t *testing.T) {
	d := configRuntime.NewDynamicValue(false)
	l := newLimiter(t, Config{Capacity: 8, Disabled: d})

	_, rel1, err := l.Admit(context.Background(), 2)
	require.NoError(t, err)
	require.Positive(t, l.usedForTest())
	accounted := l.usedForTest()

	// Flip to disabled: subsequent admits are passthrough.
	require.NoError(t, d.SetValue(true))
	_, rel2, err := l.Admit(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, accounted, l.usedForTest(), "admit while disabled must not account")

	// Flip back to enabled: accounting resumes.
	require.NoError(t, d.SetValue(false))
	_, rel3, err := l.Admit(context.Background(), 2)
	require.NoError(t, err)
	require.Greater(t, l.usedForTest(), accounted)

	rel2() // no-op
	rel1()
	rel3()
	require.Equal(t, int64(0), l.usedForTest())
}

// --- defaults ---------------------------------------------------------------

func TestAutoDefaults(t *testing.T) {
	l := newLimiter(t, Config{}) // zero config -> auto capacity & queue
	require.Equal(t, int64(concurrency.TimesGOMAXPROCS(16)), l.capacity)
	require.Equal(t, concurrency.TimesGOMAXPROCS(10), l.maxQueue)
}

// --- race stress ------------------------------------------------------------

func TestConcurrentStress(t *testing.T) {
	const (
		capacity   = 32
		goroutines = 64
		iters      = 500
	)
	l := newLimiter(t, Config{Capacity: capacity, MaxQueue: goroutines})

	var maxUsed atomic.Int64
	record := func(u int64) {
		for {
			m := maxUsed.Load()
			if u <= m || maxUsed.CompareAndSwap(m, u) {
				return
			}
		}
	}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			r := uint32(seed*2654435761 + 1)
			for i := 0; i < iters; i++ {
				r = r*1664525 + 1013904223
				want := int(r%8) + 1

				ctx := context.Background()
				var cancel context.CancelFunc
				if r%4 == 0 {
					// Occasionally use a tight deadline to exercise the
					// queue -> cancel reclaim path under contention.
					ctx, cancel = context.WithTimeout(ctx, time.Duration(r%3)*time.Millisecond)
				}

				_, rel, err := l.Admit(ctx, want)
				if cancel != nil {
					cancel()
				}
				if err != nil {
					continue // shed or cancelled: nothing held
				}
				record(l.usedForTest())
				rel()
			}
		}(g)
	}
	wg.Wait()

	require.LessOrEqual(t, maxUsed.Load(), int64(capacity), "used exceeded capacity")
	require.Equal(t, int64(0), l.usedForTest(), "capacity leaked")
	require.Equal(t, int64(0), l.inflightForTest(), "inflight leaked")
	require.Equal(t, 0, l.waitersLenForTest(), "waiters leaked")
}
