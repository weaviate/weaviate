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

type Worker struct {
	batcher    Batcher
	ctx        context.Context
	logger     logrus.FieldLogger
	readQueues *ReadQueues
	writeQueue writeQueue
}

func (w *Worker) send(req *pb.BatchSend) error {
	reply, err := w.batcher.BatchObjects(w.ctx, &pb.BatchObjectsRequest{
		Objects:          req.Objects,
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
				Object: req.Objects[err.Index],
			})
		}
		if ch, ok := w.readQueues.Get(req.StreamId); ok {
			ch <- errorsObject{Errors: errs}
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
			ch <- errorsObject{Stop: true}
		}
	}
	if req.GetSend() != nil {
		if err := w.send(req.GetSend()); err != nil {
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
