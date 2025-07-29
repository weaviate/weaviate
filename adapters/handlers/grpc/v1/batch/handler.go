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

	"github.com/sirupsen/logrus"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type Batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
}

type Handler struct {
	grpcShutdownCtx context.Context
	logger          logrus.FieldLogger
	writeQueue      writeQueue
	readQueues      *ReadQueues
}

func NewHandler(grpcShutdownCtx context.Context, writeQueue writeQueue, readQueues *ReadQueues, logger logrus.FieldLogger) *Handler {
	return &Handler{
		grpcShutdownCtx: grpcShutdownCtx,
		logger:          logger,
		writeQueue:      writeQueue,
		readQueues:      readQueues,
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
			case <-h.grpcShutdownCtx.Done():
				if innerErr := stream.Send(newBatchShutdownMessage(streamId)); innerErr != nil {
					return innerErr
				}
				h.readQueues.Delete(streamId)
				return nil
			case errs := <-readQueue:
				if errs.Stop {
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

// Send adds a batch send request to the write queue and returns the number of objects in the request.
func (h *Handler) Send(ctx context.Context, request *pb.BatchSendRequest) int64 {
	h.writeQueue <- request
	return int64(len(request.GetSend().GetObjects()))
}

// Setup initializes a read queue for the given stream ID and adds it to the read queues map.
func (h *Handler) Setup(streamId string) {
	h.readQueues.Make(streamId)
}

// Teardown closes the read queue for the given stream ID and removes it from the read queues map.
func (h *Handler) Teardown(streamId string) {
	if readQueue, ok := h.readQueues.Get(streamId); ok {
		close(readQueue)
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
