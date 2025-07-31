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
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type QueuesHandler struct {
	grpcShutdownCtx context.Context
	logger          logrus.FieldLogger
	writeQueue      writeQueue
	readQueues      *ReadQueues
}

func NewQueuesHandler(grpcShutdownCtx context.Context, writeQueue writeQueue, readQueues *ReadQueues, logger logrus.FieldLogger) *QueuesHandler {
	return &QueuesHandler{
		grpcShutdownCtx: grpcShutdownCtx,
		logger:          logger,
		writeQueue:      writeQueue,
		readQueues:      readQueues,
	}
}

func (h *QueuesHandler) Stream(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer) error {
	if err := stream.Send(newBatchStartMessage(streamId)); err != nil {
		return err
	}
	for {
		if readQueue, ok := h.readQueues.Get(streamId); ok {
			select {
			case <-ctx.Done():
				if innerErr := stream.Send(newBatchStopMessage(streamId)); innerErr != nil {
					return innerErr
				}
				return nil
			case <-h.grpcShutdownCtx.Done():
				if innerErr := stream.Send(newBatchShutdownMessage(streamId)); innerErr != nil {
					return innerErr
				}
				return nil
			case errs := <-readQueue:
				if errs.Stop {
					if innerErr := stream.Send(newBatchStopMessage(streamId)); innerErr != nil {
						return innerErr
					}
					return nil
				}
				for _, err := range errs.Errors {
					if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
						return innerErr
					}
				}
			}
		} else {
			// This should never happen, but if it does, we log it
			h.logger.WithField("streamId", streamId).Error("read queue not found")
			return fmt.Errorf("read queue for stream %s not found", streamId)
		}
	}
}

// Send adds a batch send request to the write queue and returns the number of objects in the request.
func (h *QueuesHandler) Send(ctx context.Context, request *pb.BatchSendRequest) int32 {
	h.writeQueue <- request
	return int32(len(request.GetObjects().GetValues()))
}

// Setup initializes a read queue for the given stream ID and adds it to the read queues map.
func (h *QueuesHandler) Setup(streamId string) {
	h.readQueues.Make(streamId)
}

// Teardown closes the read queue for the given stream ID and removes it from the read queues map.
func (h *QueuesHandler) Teardown(streamId string) {
	if _, ok := h.readQueues.Get(streamId); ok {
		h.readQueues.Delete(streamId)
	} else {
		h.logger.WithField("streamId", streamId).Warn("teardown called for non-existing stream")
	}
}

func newBatchStartMessage(streamId string) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_Start{
			Start: &pb.BatchStart{StreamId: streamId},
		},
	}
}

func newBatchErrorMessage(err *pb.BatchError) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_Error{
			Error: err,
		},
	}
}

func newBatchStopMessage(streamId string) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_Stop{
			Stop: &pb.BatchStop{StreamId: streamId},
		},
	}
}

func newBatchShutdownMessage(streamId string) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_Shutdown{
			Shutdown: &pb.BatchShutdown{StreamId: streamId},
		},
	}
}

type readObject struct {
	Errors []*pb.BatchError
	Stop   bool
}

type (
	writeQueue chan *pb.BatchSendRequest
	readQueue  chan readObject
	readQueues map[string]readQueue
)

// NewBatchWriteQueue creates a buffered channel to store objects for batch writing.
//
// The buffer size can be adjusted based on expected load and performance requirements
// to optimize throughput and resource usage. But is required so that there is a small buffer
// that can be quickly flushed in the event of a shutdown.
func NewBatchWriteQueue() writeQueue {
	return make(chan *pb.BatchSendRequest, 10)
}

func NewBatchReadQueues() *ReadQueues {
	return &ReadQueues{
		queues: make(readQueues),
	}
}

func NewBatchReadQueue() readQueue {
	return make(readQueue)
}

func NewStopObject() readObject {
	return readObject{
		Errors: nil,
		Stop:   true,
	}
}

func NewErrorsObject(errs []*pb.BatchError) readObject {
	return readObject{
		Errors: errs,
	}
}

type ReadQueues struct {
	lock   sync.RWMutex
	queues readQueues
}

// Get retrieves the read queue for the given stream ID.
func (r *ReadQueues) Get(streamId string) (readQueue, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	if !ok {
		return nil, false
	}
	return queue, true
}

// Delete removes the read queue for the given stream ID.
func (r *ReadQueues) Delete(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	close(r.queues[streamId])
	delete(r.queues, streamId)
}

func (r *ReadQueues) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, queue := range r.queues {
		close(queue)
	}
}

// Make initializes a read queue for the given stream ID if it does not already exist.
func (r *ReadQueues) Make(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.queues[streamId]; !ok {
		r.queues[streamId] = make(readQueue)
	}
}
