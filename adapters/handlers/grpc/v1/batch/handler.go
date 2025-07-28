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

type Batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
}

type Handler struct {
	logger     logrus.FieldLogger
	writeQueue writeQueue
	readQueues *ReadQueues
}

func NewHandler(writeQueue writeQueue, readQueues *ReadQueues, logger logrus.FieldLogger) *Handler {
	return &Handler{
		logger:     logger,
		writeQueue: writeQueue,
		readQueues: readQueues,
	}
}

func (h *Handler) Stream(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer) error {
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
				h.readQueues.Delete(streamId)
				return nil
			case errs := <-readQueue:
				if errs.Shutdown {
					if innerErr := stream.Send(newBatchStopMessage(streamId)); innerErr != nil {
						return innerErr
					}
					h.readQueues.Delete(streamId)
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

func (h *Handler) Send(ctx context.Context, request *pb.BatchSendRequest) int64 {
	h.writeQueue <- request
	return int64(len(request.GetSend().GetObjects()))
}

func (h *Handler) Setup(streamId string) {
	h.readQueues.Make(streamId)
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

type errorsObject struct {
	Errors   []*pb.BatchError
	Shutdown bool
}

type (
	writeQueue chan *pb.BatchSendRequest
	readQueue  chan errorsObject
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

func NewShutdownObject() errorsObject {
	return errorsObject{
		Errors:   nil,
		Shutdown: true,
	}
}

func NewErrorsObject(errs []*pb.BatchError) errorsObject {
	return errorsObject{
		Errors: errs,
	}
}

type ReadQueues struct {
	lock   sync.RWMutex
	queues readQueues
}

func (r *ReadQueues) Get(streamId string) (readQueue, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	if !ok {
		return nil, false
	}
	return queue, true
}

func (r *ReadQueues) Delete(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.queues, streamId)
}

func (r *ReadQueues) Make(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.queues[streamId]; !ok {
		r.queues[streamId] = make(readQueue)
	}
}
