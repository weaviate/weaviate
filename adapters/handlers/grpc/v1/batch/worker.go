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
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type Batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
	BatchReferences(ctx context.Context, req *pb.BatchReferencesRequest) (*pb.BatchReferencesReply, error)
}

type Worker struct {
	batcher       Batcher
	logger        logrus.FieldLogger
	readQueues    *ReadQueues
	internalQueue internalQueue
	writeQueues   *WriteQueues
}

type SendObjects struct {
	Values           []*pb.BatchObject
	ConsistencyLevel *pb.ConsistencyLevel
	Index            int32
}

type SendReferences struct {
	Values           []*pb.BatchReference
	ConsistencyLevel *pb.ConsistencyLevel
	Index            int32
}

type ProcessRequest struct {
	StreamId   string
	Objects    *SendObjects
	References *SendReferences
	Stop       bool
}

func (w *Worker) sendObjects(ctx context.Context, streamId string, req *SendObjects) error {
	if req == nil {
		return fmt.Errorf("received nil sendObjects request")
	}
	reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
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
				Index:  req.Index + int32(err.Index),
			})
		}
		if ch, ok := w.readQueues.Get(streamId); ok {
			ch <- &readObject{Errors: errs}
		}
	}
	return nil
}

func (w *Worker) sendReferences(ctx context.Context, streamId string, req *SendReferences) error {
	if req == nil {
		return fmt.Errorf("received nil sendReferences request")
	}
	reply, err := w.batcher.BatchReferences(ctx, &pb.BatchReferencesRequest{
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
				Index:     req.Index + int32(err.Index),
			})
		}
		if ch, ok := w.readQueues.Get(streamId); ok {
			ch <- &readObject{Errors: errs}
		}
	}
	return nil
}

// Loop processes objects from the write queue, sending them to the batcher and handling shutdown signals.
func (w *Worker) Loop(ctx context.Context, consistencyLevel pb.ConsistencyLevel) error {
	for {
		select {
		case <-ctx.Done():
			// Drain the write queue and process any remaining requests
			for req := range w.internalQueue {
				if err := w.process(ctx, req); err != nil {
					return fmt.Errorf("failed to process batch request: %w", err)
				}
			}
			return nil
		case req, ok := <-w.internalQueue:
			if req != nil {
				if err := w.process(ctx, req); err != nil {
					return fmt.Errorf("failed to process batch request: %w", err)
				}
			}
			if !ok {
				return nil // channel closed, exit loop
			}
		}
	}
}

func (w *Worker) process(ctx context.Context, req *ProcessRequest) error {
	if req.Objects != nil {
		if err := w.sendObjects(ctx, req.StreamId, req.Objects); err != nil {
			return err
		}
	}
	if req.References != nil {
		if err := w.sendReferences(ctx, req.StreamId, req.References); err != nil {
			return err
		}
	}
	if req.Stop {
		// Signal to the reply handler that we are done
		if ch, ok := w.readQueues.Get(req.StreamId); ok {
			ch <- &readObject{Stop: true}
		}
	}
	return nil
}

func StartBatchWorkers(ctx context.Context, wg *sync.WaitGroup, concurrency int, internalQueue internalQueue, readQueues *ReadQueues, writeQueues *WriteQueues, batcher Batcher, logger logrus.FieldLogger) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	for range concurrency {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			w := &Worker{batcher: batcher, logger: logger, readQueues: readQueues, writeQueues: writeQueues, internalQueue: internalQueue}
			return w.Loop(ctx, pb.ConsistencyLevel_CONSISTENCY_LEVEL_QUORUM)
		})
	}
}
