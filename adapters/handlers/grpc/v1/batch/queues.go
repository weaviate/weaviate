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

func newBatchResultsMessage(successes []*pb.BatchStreamReply_Results_Success, errors []*pb.BatchStreamReply_Results_Error) *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Results_{
			Results: &pb.BatchStreamReply_Results{
				Errors:    errors,
				Successes: successes,
			},
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
	Errors    []*pb.BatchStreamReply_Results_Error
	Successes []*pb.BatchStreamReply_Results_Success
	Stats     *workerStats
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
func NewProcessingQueue(numWorkers int) processingQueue {
	bufferSize := int(math.Ceil(float64(numWorkers) / 4))
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

func (r *reportingQueues) send(streamId string, successes []*pb.BatchStreamReply_Results_Success, errors []*pb.BatchStreamReply_Results_Error, stats *workerStats) bool {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	if !ok {
		return false
	}
	select {
	case queue <- &report{Successes: successes, Errors: errors, Stats: stats}:
		return true
	case <-time.After(1 * time.Second):
		return false
	}
}

// Make initializes a reporting queue for the given stream ID if it does not already exist.
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
	batchSizeEma      float64
	throughputEma     float64
}

func newStats() *stats {
	return &stats{
		// Start assuming the batch size is correct. This will reduce if the batch size should be larger.
		processingTimeEma: time.Second.Seconds(),
		// Start with a lower range batch size of 200 (min 100, max 1000)
		batchSizeEma: 200,
		// Start at default batchSizeEma / default processingTimeEma
		throughputEma: 200,
	}
}

// Optimum is that each worker takes at most 1s to process a batch so that shutdown does not take too long
var IDEAL_PROCESSING_TIME = time.Second.Seconds()

// Update EMAs using standard formula
// newEma = alpha*newValue + (1-alpha)*oldEma
func (s *stats) ema(new float64, ema float64) float64 {
	alpha := 0.25
	return alpha*new + (1-alpha)*ema
}

func (s *stats) updateBatchSize(processingTime time.Duration) {
	// Set alpha to favour historic data more heavily to smooth out spikes
	s.lock.Lock()
	defer s.lock.Unlock()

	s.processingTimeEma = s.ema(processingTime.Seconds(), s.processingTimeEma)
	if s.processingTimeEma-IDEAL_PROCESSING_TIME > 0.1 {
		s.batchSizeEma = s.ema(s.batchSizeEma-100, s.batchSizeEma)
	} else if s.processingTimeEma-IDEAL_PROCESSING_TIME < -0.1 {
		s.batchSizeEma = s.ema(s.batchSizeEma+100, s.batchSizeEma)
	}
	s.throughputEma = s.ema(s.batchSizeEma/s.processingTimeEma, s.throughputEma)

	if s.batchSizeEma < 100 {
		s.batchSizeEma = 100
	}
	if s.batchSizeEma > 1000 {
		s.batchSizeEma = 1000
	}
}

func (s *stats) getBatchSize() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return int(math.Ceil(s.batchSizeEma))
}

func (s *stats) getThroughputEma() float64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.throughputEma
}
