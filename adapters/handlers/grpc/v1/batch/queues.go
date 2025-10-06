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

type listen struct {
	streamId string
}

type (
	listeningQueue  chan *listen
	processingQueue chan *processRequest
	reportingQueue  chan *report
)

// NewBatchWriteQueue creates a buffered channel to store objects for batch writing.
//
// The buffer size can be adjusted based on expected load and performance requirements
// to optimize throughput and resource usage. But is required so that there is a small buffer
// that can be quickly flushed in the event of a shutdown.
func NewBatchProcessingQueue(bufferSize int) processingQueue {
	return make(processingQueue, bufferSize)
}

func NewBatchReportingQueue(bufferSize int) reportingQueue {
	return make(reportingQueue, bufferSize)
}

func NewListeningQueue() listeningQueue {
	return make(listeningQueue)
}

func NewBatchReportingQueues() *ReportingQueues {
	return &ReportingQueues{
		queues: make(map[string]reportingQueue),
		closed: make(map[string]struct{}),
	}
}

func NewErrorsObject(errs []*pb.BatchStreamReply_Error) *report {
	return &report{
		Errors: errs,
	}
}

type ReportingQueues struct {
	lock   sync.RWMutex
	queues map[string]reportingQueue
	closed map[string]struct{}
}

// Get retrieves the read queue for the given stream ID.
func (r *ReportingQueues) Get(streamId string) (reportingQueue, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	return queue, ok
}

func (r *ReportingQueues) Close(streamId string) {
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

func (r *ReportingQueues) CloseAll() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for streamId, queue := range r.queues {
		if _, ok := r.closed[streamId]; !ok {
			close(queue)
			r.closed[streamId] = struct{}{}
		}
	}
}

func (r *ReportingQueues) Delete(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.queues, streamId)
	delete(r.closed, streamId)
}

func (r *ReportingQueues) Send(streamId string, errs []*pb.BatchStreamReply_Error, stats *workerStats) bool {
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
func (r *ReportingQueues) Make(streamId string) {
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

func NewProcessRequest(objs []*pb.BatchObject, refs []*pb.BatchReference, streamId string, cl *pb.ConsistencyLevel) *processRequest {
	return &processRequest{
		StreamId:         streamId,
		ConsistencyLevel: cl,
		Objects:          objs,
		References:       refs,
	}
}

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
