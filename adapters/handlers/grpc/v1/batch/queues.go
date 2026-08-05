//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

var errReportingQueueClosed = errors.New("reporting queue closed")

const OOM_WAIT_TIME = 300

func newBatchAcksMessage(uuids, beacons []string) *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Acks_{
			Acks: &pb.BatchStreamReply_Acks{
				Uuids:   uuids,
				Beacons: beacons,
			},
		},
	}
}

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

func newBatchOutOfMemoryMessage(uuids, beacons []string) *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_OutOfMemory_{
			OutOfMemory: &pb.BatchStreamReply_OutOfMemory{
				Uuids:    uuids,
				Beacons:  beacons,
				WaitTime: OOM_WAIT_TIME,
			},
		},
	}
}

func newBatchBackoffMessage(batchSize int) *pb.BatchStreamReply {
	return &pb.BatchStreamReply{
		Message: &pb.BatchStreamReply_Backoff_{
			Backoff: &pb.BatchStreamReply_Backoff{
				BatchSize: int32(batchSize),
			},
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

// NewProcessingQueue creates a channel for batch writing.
func NewProcessingQueue() processingQueue {
	return make(processingQueue)
}

func NewReportingQueues() *reportingQueues {
	return &reportingQueues{}
}

// reportingQueues is a registry of per-stream reporting channels. Each stream owns its
// own lifecycle synchronization via streamQueue, so a slow consumer on one stream does
// not block operations on any other.
type reportingQueues struct {
	queues sync.Map // map[string]*streamQueue
}

// streamQueue is a per-stream reporting channel with done-channel + sends-WG semantics
// so that close can signal in-flight senders to abort, wait for them to release, and
// then close the underlying channel — without holding any cross-stream lock.
type streamQueue struct {
	ch        reportingQueue
	done      chan struct{}
	sendsWg   sync.WaitGroup
	mu        sync.Mutex
	closed    bool
	closeOnce sync.Once
}

func newStreamQueue() *streamQueue {
	return &streamQueue{
		ch:   make(reportingQueue),
		done: make(chan struct{}),
	}
}

func (sq *streamQueue) send(streamCtx context.Context, r *report) error {
	sq.mu.Lock()
	if sq.closed {
		sq.mu.Unlock()
		return errReportingQueueClosed
	}
	sq.sendsWg.Add(1)
	sq.mu.Unlock()
	defer sq.sendsWg.Done()

	select {
	case sq.ch <- r:
		return nil
	case <-sq.done:
		return errReportingQueueClosed
	case <-streamCtx.Done():
		return streamCtx.Err()
	}
}

func (sq *streamQueue) close() {
	sq.closeOnce.Do(func() {
		sq.mu.Lock()
		sq.closed = true
		sq.mu.Unlock()
		close(sq.done)
		sq.sendsWg.Wait()
		close(sq.ch)
	})
}

// Get returns the read end of the reporting channel for streamId.
func (r *reportingQueues) Get(streamId string) (reportingQueue, bool) {
	v, ok := r.queues.Load(streamId)
	if !ok {
		return nil, false
	}
	return v.(*streamQueue).ch, true
}

func (r *reportingQueues) close(streamId string) {
	if v, ok := r.queues.Load(streamId); ok {
		v.(*streamQueue).close()
	}
}

func (r *reportingQueues) delete(streamId string) {
	r.queues.Delete(streamId)
}

func (r *reportingQueues) send(streamCtx context.Context, streamId string, successes []*pb.BatchStreamReply_Results_Success, errs []*pb.BatchStreamReply_Results_Error, stats *workerStats) error {
	v, ok := r.queues.Load(streamId)
	if !ok {
		return fmt.Errorf("reporting queue not found for stream ID: %s", streamId)
	}
	return v.(*streamQueue).send(streamCtx, &report{Successes: successes, Errors: errs, Stats: stats})
}

// Make initializes a reporting queue for the given stream ID if it does not already exist.
func (r *reportingQueues) Make(streamId string) {
	r.queues.LoadOrStore(streamId, newStreamQueue())
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
