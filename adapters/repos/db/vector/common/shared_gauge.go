package common

import (
	"sync"
	"sync/atomic"
)

// SharedGauge is a thread-safe gauge that can be shared between multiple goroutines.
// It is used to track the number of running tasks, and allows to wait until all tasks are done.
type SharedGauge struct {
	count int64
	cond  *sync.Cond
}

func NewSharedGauge() *SharedGauge {
	return &SharedGauge{
		cond: sync.NewCond(&sync.Mutex{}),
	}
}

func (sc *SharedGauge) Incr() {
	atomic.AddInt64(&sc.count, 1)
}

func (sc *SharedGauge) Decr() {
	newCount := atomic.AddInt64(&sc.count, -1)
	if newCount == 0 {
		sc.cond.Broadcast()
	}
}

func (sc *SharedGauge) Count() int64 {
	return atomic.LoadInt64(&sc.count)
}

func (sc *SharedGauge) Wait() {
	sc.cond.L.Lock()
	defer sc.cond.L.Unlock()
	for atomic.LoadInt64(&sc.count) != 0 {
		sc.cond.Wait()
	}
}
