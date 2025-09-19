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
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type Scheduler struct {
	logger            logrus.FieldLogger
	writeQueues       *WriteQueues
	processingQueue   processingQueue
	reportingQueue    reportingQueue
	processingTimeEma float64
	batchSize         int
	batchSizeLock     sync.RWMutex
}

func NewScheduler(writeQueues *WriteQueues, processingQueue processingQueue, reportingQueue reportingQueue, logger logrus.FieldLogger) *Scheduler {
	return &Scheduler{
		logger:            logger,
		writeQueues:       writeQueues,
		processingQueue:   processingQueue,
		reportingQueue:    reportingQueue,
		processingTimeEma: 0,
		batchSize:         10000, // start with a high number to quickly find the optimum
	}
}

func (s *Scheduler) Loop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	enterrors.GoWrapper(func() {
		for workerStats := range s.reportingQueue {
			s.updateBatchSize(workerStats)
		}
	}, s.logger)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("shutting down scheduler loop")
			s.loop(s.drain)
			// Close the internal queue so that the workers can exit once they've drained the queue
			close(s.processingQueue)
			return
		case <-ticker.C:
			s.loop(s.schedule)
		}
	}
}

func (s *Scheduler) loop(op func(streamId string, wq *WriteQueue)) {
	for _, uuid := range s.writeQueues.Uuids() {
		wq, ok := s.writeQueues.Get(uuid)
		if !ok {
			continue
		}
		op(uuid, wq)
	}
}

func (s *Scheduler) drain(streamId string, wq *WriteQueue) {
	s.batchSizeLock.RLock()
	batchSize := s.batchSize
	s.batchSizeLock.RUnlock()

	objs := make([]*pb.BatchObject, 0, batchSize)
	refs := make([]*pb.BatchReference, 0, batchSize)
	for obj := range wq.queue {
		if obj.Object != nil {
			objs = append(objs, obj.Object)
		}
		if obj.Reference != nil {
			refs = append(refs, obj.Reference)
		}
		if len(objs) >= batchSize || len(refs) >= batchSize || obj.Stop {
			req := NewProcessRequest(objs, refs, streamId, obj.Stop, wq.consistencyLevel)
			s.processingQueue <- req
			// Reset the queues
			objs = make([]*pb.BatchObject, 0, batchSize)
			refs = make([]*pb.BatchReference, 0, batchSize)
		}
	}
	if len(objs) > 0 || len(refs) > 0 {
		req := NewProcessRequest(objs, refs, streamId, false, wq.consistencyLevel)
		s.processingQueue <- req
	}
}

func (s *Scheduler) schedule(streamId string, wq *WriteQueue) {
	s.batchSizeLock.RLock()
	batchSize := s.batchSize
	s.batchSizeLock.RUnlock()

	objs, refs, stop := s.pull(wq.queue, batchSize)
	req := NewProcessRequest(objs, refs, streamId, false, wq.consistencyLevel)
	if (req.Objects != nil && len(req.Objects.Values) > 0) || (req.References != nil && len(req.References.Values) > 0) {
		s.processingQueue <- req
	}
	if stop {
		s.processingQueue <- NewProcessRequest(nil, nil, streamId, true, wq.consistencyLevel)
	}
}

func (s *Scheduler) pull(queue writeQueue, max int) ([]*pb.BatchObject, []*pb.BatchReference, bool) {
	objs := make([]*pb.BatchObject, 0, max)
	refs := make([]*pb.BatchReference, 0, max)
	for i := 0; i < max && len(queue) > 0; i++ {
		select {
		case obj, ok := <-queue:
			if !ok {
				// channel is closed
				return objs, refs, false
			}
			if obj.Object != nil {
				objs = append(objs, obj.Object)
			}
			if obj.Reference != nil {
				refs = append(refs, obj.Reference)
			}
			if obj.Stop {
				return objs, refs, true
			}
		default:
			return objs, refs, false
		}
	}
	return objs, refs, false
}

func (s *Scheduler) updateBatchSize(stats *workerStats) {
	ideal := 1.0 // seconds
	// Optimum is that each worker takes at most 1s to process a batch so that shutdown does not take too long
	alpha := 1 - math.Exp(-math.Log(2)/float64(cap(s.processingQueue)))
	s.processingTimeEma = alpha*float64(stats.processingTime.Seconds()) + (1-alpha)*s.processingTimeEma
	s.batchSizeLock.Lock()
	defer s.batchSizeLock.Unlock()
	s.batchSize = int(float64(s.batchSize) * (ideal / s.processingTimeEma))
	if s.batchSize < 10 {
		s.batchSize = 10
	}
	if s.batchSize > 10000 {
		s.batchSize = 10000
	}
	s.logger.Infof("updated batch size to %d (processing time EMA: %.2fs)", s.batchSize, s.processingTimeEma)
}

type workerStats struct {
	processingTime time.Duration
}

func NewProcessRequest(objs []*pb.BatchObject, refs []*pb.BatchReference, streamId string, stop bool, consistencyLevel *pb.ConsistencyLevel) *processRequest {
	req := &processRequest{
		StreamId: streamId,
		Stop:     stop,
	}
	if len(objs) > 0 {
		req.Objects = &SendObjects{
			Values:           objs,
			ConsistencyLevel: consistencyLevel,
		}
	}
	if len(refs) > 0 {
		req.References = &SendReferences{
			Values:           refs,
			ConsistencyLevel: consistencyLevel,
		}
	}
	return req
}

func StartScheduler(ctx context.Context, wg *sync.WaitGroup, writeQueues *WriteQueues, processingQueue processingQueue, reportingQueue reportingQueue, logger logrus.FieldLogger) {
	scheduler := NewScheduler(writeQueues, processingQueue, reportingQueue, logger)
	wg.Add(1)
	enterrors.GoWrapper(func() {
		defer wg.Done()
		scheduler.Loop(ctx)
	}, logger)
}
