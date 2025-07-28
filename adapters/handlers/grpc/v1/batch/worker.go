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

func (w *Worker) Loop(consistencyLevel pb.ConsistencyLevel) error {
	for {
		select {
		case <-w.ctx.Done():
			return nil
		case obj := <-w.writeQueue:
			if obj.GetStop() != nil {
				// Signal to the reply handler that we are done
				if ch, ok := w.readQueues.Get(obj.GetStop().StreamId); ok {
					ch <- errorsObject{Shutdown: true}
				}
			}
			if obj.GetSend() != nil {
				if err := w.send(obj.GetSend()); err != nil {
					return err
				}
			}
		}
	}
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
