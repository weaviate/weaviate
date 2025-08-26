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
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/replica"
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
	wgs           *sync.Map // map[string]*sync.WaitGroup; streamID -> wg
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

func (w *Worker) wgForStream(streamId string) *sync.WaitGroup {
	actual, _ := w.wgs.LoadOrStore(streamId, &sync.WaitGroup{})
	return actual.(*sync.WaitGroup)
}

func (w *Worker) isReplicationError(err string) bool {
	return strings.Contains(err, replica.ErrReplicas.Error()) || (strings.Contains(err, "connect: Post") && strings.Contains(err, ":commit"))
}

func (w *Worker) sendObjects(ctx context.Context, wg *sync.WaitGroup, streamId string, req *SendObjects) error {
	if req == nil {
		return fmt.Errorf("received nil sendObjects request")
	}
	wg.Add(1)
	defer wg.Done()
	reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
		Objects:          req.Values,
		ConsistencyLevel: req.ConsistencyLevel,
	})
	if err != nil {
		return err
	}
	// TODO: add logic to re-add any known transient errors to the queue here
	// e.g., broadcast: cannot reach enough replicas or internal clusterAPI failed comms
	// like dial tcp 10.244.0.15:7001: connect: connection refused
	if len(reply.GetErrors()) > 0 {
		if len(reply.GetErrors()) == len(req.Values) {
			indicesByError := make(map[string][]int32)
			for i, err := range reply.GetErrors() {
				if err == nil {
					continue
				}
				indicesByError[err.Error] = append(indicesByError[err.Error], req.Index+int32(i))
			}
			errs := make([]*pb.BatchFullError, 0, len(indicesByError))
			for errStr, indices := range indicesByError {
				errs = append(errs, &pb.BatchFullError{
					Error:     errStr,
					Indices:   indices,
					IsObject:  true,
					Retriable: w.isReplicationError(errStr),
				})
			}
			if ch, ok := w.readQueues.Get(streamId); ok {
				ch <- &readObject{FullErrors: errs}
			}
		} else {
			errs := make([]*pb.BatchPartialError, 0, len(reply.GetErrors()))
			for _, err := range reply.GetErrors() {
				if err == nil {
					continue
				}
				errs = append(errs, &pb.BatchPartialError{
					Error:    err.Error,
					IsObject: true,
					Index:    req.Index + int32(err.Index),
				})
			}
			if ch, ok := w.readQueues.Get(streamId); ok {
				ch <- &readObject{PartialErrors: errs}
			}
		}
	}
	return nil
}

func (w *Worker) sendReferences(ctx context.Context, wg *sync.WaitGroup, streamId string, req *SendReferences) error {
	if req == nil {
		return fmt.Errorf("received nil sendReferences request")
	}
	wg.Add(1)
	defer wg.Done()
	reply, err := w.batcher.BatchReferences(ctx, &pb.BatchReferencesRequest{
		References:       req.Values,
		ConsistencyLevel: req.ConsistencyLevel,
	})
	if err != nil {
		return err
	}
	if len(reply.GetErrors()) > 0 {
		if len(reply.GetErrors()) == len(req.Values) {
			indicesByError := make(map[string][]int32)
			for i, err := range reply.GetErrors() {
				if err == nil {
					continue
				}
				indicesByError[err.Error] = append(indicesByError[err.Error], req.Index+int32(i))
			}
			errs := make([]*pb.BatchFullError, 0, len(indicesByError))
			for errStr, indices := range indicesByError {
				errs = append(errs, &pb.BatchFullError{
					Error:       errStr,
					Indices:     indices,
					IsReference: true,
					Retriable:   w.isReplicationError(errStr),
				})
			}
			if ch, ok := w.readQueues.Get(streamId); ok {
				ch <- &readObject{FullErrors: errs}
			}
		} else {
			errs := make([]*pb.BatchPartialError, 0, len(reply.GetErrors()))
			for _, err := range reply.GetErrors() {
				if err == nil {
					continue
				}
				errs = append(errs, &pb.BatchPartialError{
					Error:       err.Error,
					IsReference: true,
					Index:       req.Index + int32(err.Index),
				})
			}
			if ch, ok := w.readQueues.Get(streamId); ok {
				ch <- &readObject{PartialErrors: errs}
			}
		}
	}
	return nil
}

// Loop processes objects from the write queue, sending them to the batcher and handling shutdown signals.
func (w *Worker) Loop(ctx context.Context) error {
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
	wg := w.wgForStream(req.StreamId)
	if req.Objects != nil {
		if err := w.sendObjects(ctx, wg, req.StreamId, req.Objects); err != nil {
			return err
		}
	}
	if req.References != nil {
		if err := w.sendReferences(ctx, wg, req.StreamId, req.References); err != nil {
			return err
		}
	}
	// This should only ever be received once so it’s okay to wait on the shared wait group here
	// If the scheduler does send more than one stop per stream then deadlocks may occur
	if req.Stop {
		wg.Wait() // Wait for all processing requests to complete
		// Signal to the reply handler that we are done
		w.readQueues.Close(req.StreamId)
		w.wgs.Delete(req.StreamId) // Clean up the wait group map
	}
	return nil
}

func StartBatchWorkers(ctx context.Context, wg *sync.WaitGroup, concurrency int, internalQueue internalQueue, readQueues *ReadQueues, batcher Batcher, logger logrus.FieldLogger) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	wgs := sync.Map{}
	for range concurrency {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			w := &Worker{batcher: batcher, logger: logger, readQueues: readQueues, internalQueue: internalQueue, wgs: &wgs}
			return w.Loop(ctx)
		})
	}
}
