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

func newBatchStartedMessage() *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Started_{
			Started: &pb.BatchStreamReply_Started{},
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

func (r *reportingQueues) close(streamId string) {
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

func (r *reportingQueues) delete(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.queues, streamId)
	delete(r.closed, streamId)
}

func (r *reportingQueues) send(streamId string, errs []*pb.BatchStreamReply_Error, stats *workerStats) bool {
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

func newWorkersStats(processingTime time.Duration) *workerStats {
	return &workerStats{
		processingTime: processingTime,
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
		// Start assuming the batch size is correct. This will reduce if the batch size should be larger.
		processingTimeEma: 1,
		// Start with a mid-range batch size of 100 (min 10, max 1000)
		batchSize: 100,
		// Start at default batchSize / default processingTimeEma
		throughputEma: 100,
	}
}

// Optimum is that each worker takes at most 1s to process a batch so that shutdown does not take too long
const IDEAL_PROCESSING_TIME = 1.0 // seconds

func (s *stats) updateBatchSize(processingTime time.Duration, processingQueueLen int) {
	// Understand the smoothing factor of the EMA as the half-life of the queue length
	alpha := 1 - math.Exp(-math.Log(2)/float64(processingQueueLen))
	s.lock.Lock()
	defer s.lock.Unlock()
	// Update EMAs using standard formula
	// newEma = alpha*newValue + (1-alpha)*oldEma
	// Throughput is measured in objects per second
	s.throughputEma = alpha*float64(s.batchSize)/processingTime.Seconds() + (1-alpha)*s.throughputEma
	s.processingTimeEma = alpha*processingTime.Seconds() + (1-alpha)*s.processingTimeEma
	// Adjust batch size based on ratio of ideal time to processing time EMA
	// If processing time is higher than ideal, batch size will decrease, and vice versa
	s.batchSize = int(float64(s.batchSize) * (IDEAL_PROCESSING_TIME / s.processingTimeEma))
	// Set a minimum batch size of 10 to avoid chattiness with client sending many small batches
	if s.batchSize < 10 {
		s.batchSize = 10
	}
	// Cap batch size to 1000 to avoid excessive downstream load
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
// backoff(r) = b * max(0, (r - 0.4) / 0.6) ^ 3, with b = 1s
// E.g.
//   - usageRatio = 0.4 -> 0s
//   - usageRatio = 0.6 -> 0.037s
//   - usageRatio = 0.8 -> 0.296s
//   - usageRatio = 1.0 -> 1s
func (h *StreamHandler) thresholdCubicBackoff() float32 {
	usageRatio := float32(len(h.processingQueue)) / float32(cap(h.processingQueue))
	return float32(IDEAL_PROCESSING_TIME) * float32(math.Pow(float64(max(0, (usageRatio-0.4)/0.6)), 3))
}
