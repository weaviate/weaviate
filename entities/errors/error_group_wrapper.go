//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import (
	"context"
	"os"
	"runtime/debug"
	"sync"
	"time"
	"fmt"

	"github.com/sirupsen/logrus"

	"container/heap"

	entcfg "github.com/weaviate/weaviate/entities/config"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"golang.org/x/sync/errgroup"
)

var running = 0
var maxConcurrent = 500
var started map[string]time.Time
var pqlock sync.Mutex

// PriorityQueue_s is a priority queue that implements heap.Interface and holds ErrorGroupWrapper.
type PriorityQueue_s []*ErrorGroupWrapper

// Len returns the length of the queue.
func (pq PriorityQueue_s) Len() int { return len(pq) }

// Less reports whether the element with index i should sort before the element with index j.
func (pq PriorityQueue_s) Less(i, j int) bool {
    return pq[i].priority < pq[j].priority
}

// Swap swaps the elements with indexes i and j.
func (pq PriorityQueue_s) Swap(i, j int) {
    pq[i], pq[j] = pq[j], pq[i]
    pq[i].Index = i
    pq[j].Index = j
}

// Push pushes the element x onto the queue.
func (pq *PriorityQueue_s) Push(x interface{}) {
    n := len(*pq)
    item := x.(*ErrorGroupWrapper)
    item.Index = n
    *pq = append(*pq, item)
}

// Pop removes and returns the minimum element (according to Less) from the queue.
func (pq *PriorityQueue_s) Pop() interface{} {
    old := *pq
    n := len(old)
    item := old[n-1]
    item.Index = -1 // for safety
    *pq = old[0 : n-1]
    return item
}

// Peek returns the first element in the queue without removing it.
func (pq *PriorityQueue_s) Peek() *ErrorGroupWrapper {
    if pq.Len() == 0 {
        return nil
    }
    return (*pq)[0]
}

// Update modifies the priority and re-establishes the heap invariant.
func (pq *PriorityQueue_s) Update(item *ErrorGroupWrapper, priority int) {
    heap.Remove(pq, item.Index)
    item.priority = priority
    heap.Push(pq, item)
}

// Remove removes an item from the queue.
func (pq *PriorityQueue_s) Remove(item *ErrorGroupWrapper) {
    heap.Remove(pq, item.Index)
}

func (pq *PriorityQueue_s) QueueRunner() {
	for {
		if pq.Len() > 0 && running < maxConcurrent {
			running++

			item := heap.Pop(pq).(*ErrorGroupWrapper)
			item.logger.Debugf("Running managed threads: %d\n", running)
			item.logger.Debugf("Running with priority %d", item.priority)
			item.goChan <- struct{}{}
		}
	}
}

// NewPriorityQueue_s creates a new PriorityQueue_s.
func NewPriorityQueue_s() *PriorityQueue_s {
    pq := make(PriorityQueue_s, 0)
	started = make(map[string]time.Time)
	go pq.QueueRunner()
	pqlock = sync.Mutex{}
    return &pq
}

var priorities = NewPriorityQueue_s()


// ErrorGroupWrapper is a custom type that embeds errgroup.Group.
type ErrorGroupWrapper struct {
	*errgroup.Group
	returnError error
	variables   []interface{}
	logger      logrus.FieldLogger
	deferFunc   func(localVars ...interface{})
	cancelCtx   func()
	priority	int
	zone 	  string
	Index 	 int
	goChan 	chan struct{}
}

// NewErrorGroupWrapper creates a new ErrorGroupWrapper.
func NewErrorGroupWrapper(logger logrus.FieldLogger, vars ...interface{}) *ErrorGroupWrapper {
	egw := &ErrorGroupWrapper{
		Group:       new(errgroup.Group),
		returnError: nil,
		variables:   vars,
		logger:      logger,
		zone : "unknown",
		priority: 0,
		goChan: make(chan struct{}),

		// this dummy func makes it safe to call cancelCtx even if a wrapper without a
		// context is used. Avoids a nil check later on.
		cancelCtx: func() {},
	}
	egw.setDeferFunc()
	return egw
}

// NewErrorGroupWithContextWrapper creates a new ErrorGroupWrapper
func NewErrorGroupWithContextWrapper(logger logrus.FieldLogger, ctx context.Context, vars ...interface{}) (*ErrorGroupWrapper, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	eg, ctx := errgroup.WithContext(ctx)
	egw := &ErrorGroupWrapper{
		Group:       eg,
		returnError: nil,
		variables:   vars,
		logger:      logger,
		cancelCtx:   cancel,
	}
	egw.setDeferFunc()

	return egw, ctx
}

func (egw *ErrorGroupWrapper) setDeferFunc() {
	disable := entcfg.Enabled(os.Getenv("DISABLE_RECOVERY_ON_PANIC"))
	if !disable {
		egw.deferFunc = func(localVars ...interface{}) {
			if r := recover(); r != nil {
				entsentry.Recover(r)
				egw.logger.WithField("panic", r).Errorf("Recovered from panic: %v, local variables %v, additional localVars %v\n", r, localVars, egw.variables)
				debug.PrintStack()
				egw.returnError = fmt.Errorf("panic occurred: %v", r)
				egw.cancelCtx()
			}
		}
	} else {
		egw.deferFunc = func(localVars ...interface{}) {}
	}
}

// Go overrides the Go method to add panic recovery logic.
func (egw *ErrorGroupWrapper) Go(f func() error, localVars ...interface{}) {
		//priorities.Push(egw)
		//egw.GoWithPriority(f, localVars...)
		egw.Group.Go(func() error {
			pqlock.Lock()
			started[egw.zone] = time.Now()
			pqlock.Unlock()
			//<- egw.goChan
			defer egw.deferFunc(localVars)
			defer func(){running--}()
			defer func() {
				pqlock.Lock()
				startTime := started[egw.zone]
				pqlock.Unlock()
				endTime := time.Now()
				egw.logger.WithField("Zone", egw.zone, "Start", startTime, "End", endTime, "Duration", endTime.Sub(startTime)).Info("Finished")
			}()
			egw.logger.Debugf("Running with priority %d", egw.priority)
			egw.logger.Debugf("Running in zone %s", egw.zone)
			return f()
		})
}

func (egw *ErrorGroupWrapper) GoWithPriority(f func() error, localVars ...interface{}) {
	egw.Group.Go(func() error {
		pqlock.Lock()
		started[egw.zone] = time.Now()
		pqlock.Unlock()
		//<- egw.goChan
		defer egw.deferFunc(localVars)
		defer func(){running--}()
		defer func() {
			pqlock.Lock()
			startTime := started[egw.zone]
			pqlock.Unlock()
			endTime := time.Now()
			egw.logger.WithField("Zone", egw.zone, "Start", startTime, "End", endTime, "Duration", endTime.Sub(startTime)).Info("Finished")
		}()
		egw.logger.Debugf("Running with priority %d", egw.priority)
		egw.logger.Debugf("Running in zone %s", egw.zone)
		return f()
	})
}

// Wait waits for all goroutines to finish and returns the first non-nil error.
func (egw *ErrorGroupWrapper) Wait() error {
	if err := egw.Group.Wait(); err != nil {
		return err
	}
	return egw.returnError
}

func (egw *ErrorGroupWrapper) SetPriority(priority int) {
	egw.priority = priority
}

func (egw *ErrorGroupWrapper) SetZone(zone string) {
	egw.zone = zone
}