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
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type Batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
	BatchReferences(ctx context.Context, req *pb.BatchReferencesRequest) (*pb.BatchReferencesReply, error)
}

type Worker struct {
	batcher    Batcher
	ctx        context.Context
	logger     logrus.FieldLogger
	readQueues *ReadQueues
	writeQueue writeQueue
}

func (w *Worker) sendObjects(req *pb.BatchSendObjects) error {
	if req == nil {
		return fmt.Errorf("received nil BatchSendObjects request")
	}
	reply, err := w.batcher.BatchObjects(w.ctx, &pb.BatchObjectsRequest{
		Objects:          req.Values,
		ConsistencyLevel: req.ConsistencyLevel,
	})
	if err != nil {
		return err
	}
	if len(reply.GetErrors()) > 0 {
		errs := make([]*pb.BatchError, 0, len(reply.GetErrors()))
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			errs = append(errs, &pb.BatchError{
				Error:  err.Error,
				Object: req.Values[err.Index],
			})
		}
		if ch, ok := w.readQueues.Get(req.StreamId); ok {
			ch <- readObject{Errors: errs}
		}
	}
	return nil
}

func (w *Worker) sendReferences(req *pb.BatchSendReferences) error {
	if req == nil {
		return fmt.Errorf("received nil BatchSendReferences request")
	}
	reply, err := w.batcher.BatchReferences(w.ctx, &pb.BatchReferencesRequest{
		References:       req.Values,
		ConsistencyLevel: req.ConsistencyLevel,
	})
	if err != nil {
		return err
	}
	if len(reply.GetErrors()) > 0 {
		errs := make([]*pb.BatchError, 0, len(reply.GetErrors()))
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			errs = append(errs, &pb.BatchError{
				Error:     err.Error,
				Reference: req.Values[err.Index],
			})
		}
		if ch, ok := w.readQueues.Get(req.StreamId); ok {
			ch <- readObject{Errors: errs}
		}
	}
	return nil
}

// Loop processes objects from the write queue, sending them to the batcher and handling shutdown signals.
func (w *Worker) Loop(consistencyLevel pb.ConsistencyLevel) error {
	for {
		select {
		case <-w.ctx.Done():
			// Drain the write queue and process any remaining requests
			for req := range w.writeQueue {
				if err := w.process(req); err != nil {
					return fmt.Errorf("failed to process batch request: %w", err)
				}
			}
			return nil
		case req := <-w.writeQueue:
			if err := w.process(req); err != nil {
				return fmt.Errorf("failed to process batch request: %w", err)
			}
		}
	}
}

func (w *Worker) process(req *pb.BatchSendRequest) error {
	if req.GetStop() != nil {
		// Signal to the reply handler that we are done
		if ch, ok := w.readQueues.Get(req.GetStop().StreamId); ok {
			ch <- readObject{Stop: true}
		}
	}
	if req.GetObjects() != nil {
		if err := w.sendObjects(req.GetObjects()); err != nil {
			return err
		}
	}
	if req.GetReferences() != nil {
		if err := w.sendReferences(req.GetReferences()); err != nil {
			return err
		}
	}
	return nil
}

func StartBatchWorkers(ctx context.Context, concurrency int, writeQueue writeQueue, readQueues *ReadQueues, batcher Batcher, logger logrus.FieldLogger) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	for range concurrency {
		eg.Go(func() error {
			w := &Worker{batcher: batcher, ctx: ctx, logger: logger, readQueues: readQueues, writeQueue: writeQueue}
			return w.Loop(pb.ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM)
		})
	}
}
