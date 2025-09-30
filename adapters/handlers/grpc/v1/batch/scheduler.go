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

type stats struct {
	lock              sync.RWMutex
	processingTimeEma float64
	batchSize         int
}

func newStats() *stats {
	return &stats{
		processingTimeEma: 0,
		batchSize:         100,
	}
}

func (s *stats) updateBatchSize(processingTime time.Duration, processingQueueLen int) {
	ideal := 1.0 // seconds
	// Optimum is that each worker takes at most 1s to process a batch so that shutdown does not take too long
	alpha := 1 - math.Exp(-math.Log(2)/float64(processingQueueLen))
	s.lock.Lock()
	defer s.lock.Unlock()
	s.processingTimeEma = alpha*processingTime.Seconds() + (1-alpha)*s.processingTimeEma
	s.batchSize = int(float64(s.batchSize) * (ideal / s.processingTimeEma))
	if s.batchSize < 10 {
		s.batchSize = 10
	}
	if s.batchSize > 10000 {
		s.batchSize = 10000
	}
}

func (s *stats) getBatchSize() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.batchSize
}

func (s *stats) getProcessingTimeEma() time.Duration {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return time.Duration(s.processingTimeEma * time.Second.Seconds())
}

type Scheduler struct {
	logger          logrus.FieldLogger
	writeQueues     *WriteQueues
	processingQueue processingQueue
	reportingQueue  reportingQueue
	statsPerStream  *sync.Map // map[string]*stats
	metrics         *BatchStreamingCallbacks
}

func (s *Scheduler) stats(streamId string) *stats {
	st, _ := s.statsPerStream.LoadOrStore(streamId, newStats())
	return st.(*stats)
}

func NewScheduler(writeQueues *WriteQueues, processingQueue processingQueue, reportingQueue reportingQueue, metrics *BatchStreamingCallbacks, logger logrus.FieldLogger) *Scheduler {
	return &Scheduler{
		logger:          logger,
		writeQueues:     writeQueues,
		processingQueue: processingQueue,
		reportingQueue:  reportingQueue,
		metrics:         metrics,
		statsPerStream:  &sync.Map{},
	}
}

func (s *Scheduler) Loop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	enterrors.GoWrapper(func() {
		for workerStats := range s.reportingQueue {
			stats := s.stats(workerStats.streamId)
			stats.updateBatchSize(workerStats.processingTime, len(s.processingQueue))
			if s.metrics != nil {
				s.metrics.OnSchedulerReport(workerStats.streamId, stats.getBatchSize(), stats.getProcessingTimeEma())
			}
		}
	}, s.logger)

	for {
		select {
		case <-ctx.Done():
			log := s.logger.WithField("action", "shutdown_scheduler_loop")
			log.Info("shutting down scheduler loop")
			s.loop(s.drain)
			log.Info("all streams drained, closing processing queue")
			// Close the processing queue so that the workers can exit once they've drained the queue
			close(s.processingQueue)
			log.Info("processing queue closed, exiting scheduler loop")
			return
		case <-ticker.C:
			s.loop(s.schedule)
		}
	}
}

func (s *Scheduler) loop(op func(streamId string, wq *WriteQueue) bool) {
	uuids := s.writeQueues.Uuids()
	if len(uuids) == 0 {
		return
	}
	s.logger.WithField("action", "scheduler_loop").WithField("uuids", uuids).Debug("scheduling streams")
	for _, uuid := range uuids {
		wq, ok := s.writeQueues.Get(uuid)
		if !ok {
			continue
		}
		if done := op(uuid, wq); done {
			s.writeQueues.Delete(uuid)
			s.statsPerStream.Delete(uuid)
			s.logger.WithField("streamId", uuid).Debug("stream completed and removed from scheduler")
		}
	}
}

func (s *Scheduler) drain(streamId string, wq *WriteQueue) bool {
	batchSize := s.stats(streamId).getBatchSize()
	objs := make([]*pb.BatchObject, 0, batchSize)
	refs := make([]*pb.BatchReference, 0, batchSize)
	if len(wq.queue) != 0 {
		for obj := range wq.queue {
			if obj.Object != nil {
				objs = append(objs, obj.Object)
			}
			if obj.Reference != nil {
				refs = append(refs, obj.Reference)
			}
			if len(objs) >= batchSize || len(refs) >= batchSize {
				s.logger.WithField("streamId", streamId).WithField("numObjects", len(objs)).WithField("numReferences", len(refs)).Debug("draining batch objects")
				req := NewProcessRequest(objs, refs, streamId, false, wq.consistencyLevel)
				s.processingQueue <- req
				// Reset the batches
				objs = make([]*pb.BatchObject, 0, batchSize)
				refs = make([]*pb.BatchReference, 0, batchSize)
			}
		}
	}
	s.logger.WithField("streamId", streamId).WithField("numObjects", len(objs)).WithField("numReferences", len(refs)).Debug("draining final batch objects")
	s.processingQueue <- NewProcessRequest(objs, refs, streamId, true, wq.consistencyLevel)
	return true
}

func (s *Scheduler) schedule(streamId string, wq *WriteQueue) bool {
	objs, refs, stop := s.pull(wq.queue, s.stats(streamId).getBatchSize())
	if len(objs) == 0 && len(refs) == 0 && !stop {
		// nothing to do
		return false
	}
	s.logger.
		WithField("streamId", streamId).
		WithField("numObjects", len(objs)).
		WithField("numReferences", len(refs)).
		WithField("stop", stop).
		Debug("scheduling batch objects")
	s.processingQueue <- NewProcessRequest(objs, refs, streamId, stop, wq.consistencyLevel)
	return stop
}

func (s *Scheduler) pull(queue writeQueue, max int) ([]*pb.BatchObject, []*pb.BatchReference, bool) {
	objs := make([]*pb.BatchObject, 0, max)
	refs := make([]*pb.BatchReference, 0, max)
	for i := 0; i < max && len(queue) >= 0; i++ {
		select {
		case obj, ok := <-queue:
			if !ok {
				// channel is closed, stop processing
				return objs, refs, true
			}
			if obj.Object != nil {
				objs = append(objs, obj.Object)
			}
			if obj.Reference != nil {
				refs = append(refs, obj.Reference)
			}
		default:
			return objs, refs, false
		}
	}
	return objs, refs, false
}

type workerStats struct {
	processingTime time.Duration
	streamId       string
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

func StartScheduler(ctx context.Context, wg *sync.WaitGroup, writeQueues *WriteQueues, processingQueue processingQueue, reportingQueue reportingQueue, metrics *BatchStreamingCallbacks, logger logrus.FieldLogger) {
	scheduler := NewScheduler(writeQueues, processingQueue, reportingQueue, metrics, logger)
	wg.Add(1)
	logger.WithField("action", "batch_stream_start").Info("entering scheduler loop")
	enterrors.GoWrapper(func() {
		defer wg.Done()
		scheduler.Loop(ctx)
	}, logger)
}
