//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package helpers

import (
	"context"
	"runtime"

	"golang.org/x/sync/semaphore"
)

type OneBlockingMultipleConcurrently struct {
	sem1           *semaphore.Weighted
	sem2           *semaphore.Weighted
	maxConcurrency int64
	ctx            context.Context
}

func NewOneBlockingMultipleConcurrently(ctx context.Context) *OneBlockingMultipleConcurrently {
	maxConcurrency := int64(runtime.GOMAXPROCS(0))
	return &OneBlockingMultipleConcurrently{
		sem1:           semaphore.NewWeighted(int64(1)),
		sem2:           semaphore.NewWeighted(maxConcurrency),
		maxConcurrency: maxConcurrency,
		ctx:            ctx,
	}
}

func (s *OneBlockingMultipleConcurrently) CheckIn() {
	s.sem1.Acquire(s.ctx, 1)
	s.sem1.Release(1)
	s.sem2.Acquire(s.ctx, 1)
}

func (s *OneBlockingMultipleConcurrently) CheckOut() {
	s.sem2.Release(1)
}

func (s *OneBlockingMultipleConcurrently) Lock() {
	s.sem1.Acquire(s.ctx, 1)
	s.sem2.Acquire(s.ctx, s.maxConcurrency)
}

func (s *OneBlockingMultipleConcurrently) Release() {
	s.sem2.Release(s.maxConcurrency)
	s.sem1.Release(1)
}

type Action func()

func (s *OneBlockingMultipleConcurrently) InvokeConcurrentTask(do Action) {
	s.CheckIn()
	do()
	s.CheckOut()
}

func (s *OneBlockingMultipleConcurrently) InvokeBlockingTask(do Action) {
	s.Lock()
	do()
	s.Release()
}
