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

package common

import (
	"sync"
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
	sc.cond.L.Lock()
	defer sc.cond.L.Unlock()

	sc.count++
}

func (sc *SharedGauge) Decr() {
	sc.cond.L.Lock()
	defer sc.cond.L.Unlock()

	if sc.count == 0 {
		panic("illegal gauge state: count cannot be negative")
	}

	sc.count--

	if sc.count == 0 {
		sc.cond.Broadcast()
	}
}

func (sc *SharedGauge) Count() int64 {
	sc.cond.L.Lock()
	defer sc.cond.L.Unlock()

	return sc.count
}

func (sc *SharedGauge) Wait() {
	sc.cond.L.Lock()
	defer sc.cond.L.Unlock()

	for sc.count != 0 {
		sc.cond.Wait()
	}
}
