//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
}

type StreamHandler struct {
	logger     logrus.FieldLogger
	writeQueue writeQueue
	readQueue  readQueue
}

type Worker struct {
	batcher    batcher
	ctx        context.Context
	logger     logrus.FieldLogger
	readQueue  readQueue
	writeQueue writeQueue
}

func NewStreamHandler(writeQueue writeQueue, readQueue readQueue, logger logrus.FieldLogger) *StreamHandler {
	return &StreamHandler{
		logger:     logger,
		writeQueue: writeQueue,
		readQueue:  readQueue,
	}
}

func (w *Worker) send(objects []queueObject, consistencyLevel pb.ConsistencyLevel) error {
	firstIndex := objects[0].Index
	objs := make([]*pb.BatchObject, len(objects))
	for i, obj := range objects {
		objs[i] = obj.Object
	}
	reply, err := w.batcher.BatchObjects(w.ctx, &pb.BatchObjectsRequest{Objects: objs, ConsistencyLevel: &consistencyLevel})
	if err != nil {
		return err
	}
	if len(reply.GetErrors()) > 0 {
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			err.Index += int32(firstIndex)
		}
		w.readQueue <- errorsObject{Errors: reply.GetErrors()}
	}
	return nil
}

func (w *Worker) Loop(consistencyLevel pb.ConsistencyLevel) error {
	objects := make([]queueObject, 0, 1000)
	now := time.Now()
	for {
		select {
		case <-w.ctx.Done():
			return nil
		case obj := <-w.writeQueue:
			if obj.IsSentinel() {
				if len(objects) > 0 {
					if err := w.send(objects, consistencyLevel); err != nil {
						return err
					}
				}
				// Signal to the reply handler that we are done
				w.readQueue <- errorsObject{Shutdown: true}
				return nil
			}
			if obj.IsObject() {
				objects = append(objects, obj)
			}
			if len(objects) == 1000 {
				if err := w.send(objects, consistencyLevel); err != nil {
					return err
				}
				objects = objects[:0]
			}
		default:
			if time.Since(now) > time.Second {
				if len(objects) > 0 {
					if err := w.send(objects, consistencyLevel); err != nil {
						return err
					}
					objects = objects[:0]
				}
				now = time.Now()
			}
		}
	}
}

func StartBatchWorkers(ctx context.Context, concurrency int, writeQueue writeQueue, readQueue readQueue, batcher batcher, logger logrus.FieldLogger) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	for range concurrency {
		eg.Go(func() error {
			w := &Worker{batcher: batcher, ctx: ctx, logger: logger, readQueue: readQueue, writeQueue: writeQueue}
			return w.Loop(pb.ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM)
		})
	}
}

func (h *StreamHandler) Read(stream pb.Weaviate_BatchReadServer) error {
	// haveShutdown := 0 // loops over all the workers to ensure all errors have been returned before shutting down
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case errs := <-h.readQueue:
			// if errs.Shutdown {
			// 	haveShutdown += 1
			// }
			for _, err := range errs.Errors {
				if innerErr := stream.Send(&pb.BatchError{
					Index: err.Index,
					Error: err.Error,
				}); innerErr != nil {
					return innerErr
				}
			}
		}
		// if haveShutdown == concurrency {
		// 	return nil
		// }
	}
}

func (h *StreamHandler) Write(stream pb.Weaviate_BatchWriteServer) error {
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("receive first batch object: %w", err)
	}
	init := first.GetInit()
	if init == nil {
		return fmt.Errorf("first object must be init object")
	}

	index := 0
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		request := req.GetRequest()
		if request == nil {
			return fmt.Errorf("object must be message object, got %T", req)
		}
		for _, object := range request.Objects {
			h.writeQueue <- queueObject{Index: index, Object: object}
			index++
		}
	}
}

type queueObject struct {
	Index    int
	Object   *pb.BatchObject
	Sentinel *pb.BatchStop
}

func (o queueObject) IsSentinel() bool {
	return o.Sentinel != nil
}

func (o queueObject) IsObject() bool {
	return o.Object != nil
}

type errorsObject struct {
	Errors   []*pb.BatchObjectsReply_BatchError
	Shutdown bool
}

type (
	writeQueue chan queueObject
	readQueue  chan errorsObject
)

func NewBatchWriteQueue() writeQueue {
	return make(chan queueObject)
}

func NewBatchReadQueue() readQueue {
	return make(chan errorsObject)
}
