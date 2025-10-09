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
	"math"
	"sync"
	"time"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func newBatchErrorMessage(err *pb.BatchStreamReply_Error) *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Error_{
			Error: err,
		},
	}
}

func newBatchShuttingDownMessage() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_ShuttingDown_{
			ShuttingDown: &pb.BatchStreamReply_ShuttingDown{},
		},
	}
}

type report struct {
	Errors []*pb.BatchStreamReply_Error
	Stats  *workerStats
}

type (
	processingQueue chan *processRequest
	reportingQueue  chan *report
)

// NewProcessingQueue creates a buffered channel to store objects for batch writing.
//
// The buffer size can be adjusted based on expected load and performance requirements
// to optimize throughput and resource usage. But is required so that there is a small buffer
// that can be quickly flushed in the event of a shutdown.
func NewProcessingQueue(bufferSize int) processingQueue {
	return make(processingQueue, bufferSize)
}

func NewReportingQueues() *reportingQueues {
	return &reportingQueues{
		queues: make(map[string]reportingQueue),
		closed: make(map[string]struct{}),
	}
}

type reportingQueues struct {
	lock   sync.RWMutex
	queues map[string]reportingQueue
	closed map[string]struct{}
}

// Get retrieves the read queue for the given stream ID.
func (r *reportingQueues) Get(streamId string) (reportingQueue, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	return queue, ok
}

func (r *reportingQueues) Close(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if queue, ok := r.queues[streamId]; ok {
		if _, alreadyClosed := r.closed[streamId]; alreadyClosed {
			return
		}
		close(queue)
		r.closed[streamId] = struct{}{}
	}
}

func (r *reportingQueues) CloseAll() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for streamId, queue := range r.queues {
		if _, ok := r.closed[streamId]; !ok {
			close(queue)
			r.closed[streamId] = struct{}{}
		}
	}
}

func (r *reportingQueues) Delete(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.queues, streamId)
	delete(r.closed, streamId)
}

func (r *reportingQueues) Send(streamId string, errs []*pb.BatchStreamReply_Error, stats *workerStats) bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	if !ok {
		return false
	}
	select {
	case queue <- &report{Errors: errs, Stats: stats}:
		return true
	case <-time.After(1 * time.Second):
		return false
	}
}

// Make initializes a read queue for the given stream ID if it does not already exist.
func (r *reportingQueues) Make(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.queues[streamId]; !ok {
		r.queues[streamId] = make(reportingQueue)
	}
}

type workerStats struct {
	processingTime time.Duration
}

func NewWorkersStats(processingTime time.Duration) *workerStats {
	return &workerStats{
		processingTime: processingTime,
	}
}

func NewProcessRequest(objs []*pb.BatchObject, refs []*pb.BatchReference, streamId string, cl *pb.ConsistencyLevel, wg *sync.WaitGroup) *processRequest {
	return &processRequest{
		StreamId:         streamId,
		ConsistencyLevel: cl,
		Objects:          objs,
		References:       refs,
		Wg:               wg,
	}
}

type stats struct {
	lock              sync.RWMutex
	processingTimeEma float64
	batchSize         int
	throughputEma     float64
}

func newStats() *stats {
	return &stats{
		processingTimeEma: 0,
		batchSize:         100,
		throughputEma:     0,
	}
}

func (s *stats) updateBatchSize(processingTime time.Duration, processingQueueLen int) {
	// Optimum is that each worker takes at most 1s to process a batch so that shutdown does not take too long
	ideal := 1.0 // seconds
	// Understand the smoothing factor of the EMA as the half-life of the queue length
	alpha := 1 - math.Exp(-math.Log(2)/float64(processingQueueLen))
	s.lock.Lock()
	defer s.lock.Unlock()
	s.throughputEma = alpha*float64(s.batchSize)/processingTime.Seconds() + (1-alpha)*s.throughputEma
	s.processingTimeEma = alpha*processingTime.Seconds() + (1-alpha)*s.processingTimeEma
	s.batchSize = int(float64(s.batchSize) * (ideal / s.processingTimeEma))
	if s.batchSize < 10 {
		s.batchSize = 10
	}
	if s.batchSize > 1000 {
		s.batchSize = 1000
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

func (s *stats) getThroughputEma() float64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.throughputEma
}

// Cubic backoff function based on processing queue utilisation:
// backoff(r) = b * max(0, (r - 0.6) / 0.4) ^ 3, with b = 1s
// E.g.
//   - usageRatio = 0.6 -> 0s
//   - usageRatio = 0.8 -> 0.13s
//   - usageRatio = 0.9 -> 0.42s
//   - usageRatio = 1.0 -> 1s
func (h *StreamHandler) thresholdCubicBackoff() float32 {
	usageRatio := float32(len(h.processingQueue)) / float32(cap(h.processingQueue))
	maximumBackoffSeconds := float32(1.0) // Aligns with ideal processing time of 1s
	return maximumBackoffSeconds * float32(math.Pow(float64(max(0, (usageRatio-0.6)/0.4)), 3))
}
