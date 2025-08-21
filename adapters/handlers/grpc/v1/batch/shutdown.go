//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

type Shutdown struct {
	HandlersCtx      context.Context
	HandlersCancel   context.CancelFunc
	HandlersWg       *sync.WaitGroup
	SchedulerCtx     context.Context
	SchedulerCancel  context.CancelFunc
	SchedulerWg      *sync.WaitGroup
	WorkersCtx       context.Context
	WorkersCancel    context.CancelFunc
	WorkersWg        *sync.WaitGroup
	ShutdownFinished chan struct{}
}

func NewShutdown(ctx context.Context) *Shutdown {
	var handlersWg sync.WaitGroup
	var schedulerWg sync.WaitGroup
	var workersWg sync.WaitGroup

	hCtx, hCancel := context.WithCancel(ctx)
	sCtx, sCancel := context.WithCancel(ctx)
	wCtx, wCancel := context.WithCancel(ctx)

	shutdownFinished := make(chan struct{})
	return &Shutdown{
		HandlersCtx:      hCtx,
		HandlersCancel:   hCancel,
		HandlersWg:       &handlersWg,
		SchedulerCtx:     sCtx,
		SchedulerCancel:  sCancel,
		SchedulerWg:      &schedulerWg,
		WorkersCtx:       wCtx,
		WorkersCancel:    wCancel,
		WorkersWg:        &workersWg,
		ShutdownFinished: shutdownFinished,
	}
}

func (s *Shutdown) Drain(logger logrus.FieldLogger) {
	// stop handlers first
	s.HandlersCancel()
	logger.Info("shutting down grpc batch handlers")
	// wait for all send requests to finish
	logger.Info("draining in-flight BatchSend methods")
	s.HandlersWg.Wait()
	// stop the scheduler
	s.SchedulerCancel()
	logger.Info("shutting down grpc batch scheduler")
	// wait for all objs in write queues to be added to internal queue
	s.SchedulerWg.Wait()
	// stop the workers now
	s.WorkersCancel()
	logger.Info("shutting down grpc batch workers")
	// wait for all the objects to be processed from the internal queue
	s.WorkersWg.Wait()
	logger.Info("finished draining the internal queues")
	close(s.ShutdownFinished)
}
