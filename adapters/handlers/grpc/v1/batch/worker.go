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
	"time"

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
	batcher         Batcher
	logger          logrus.FieldLogger
	readQueues      *ReadQueues
	processingQueue processingQueue
	reportingQueue  reportingQueue
	wgs             *sync.Map // map[string]*sync.WaitGroup; streamID -> wg
}

type SendObjects struct {
	Values           []*pb.BatchObject
	ConsistencyLevel *pb.ConsistencyLevel
}

type SendReferences struct {
	Values           []*pb.BatchReference
	ConsistencyLevel *pb.ConsistencyLevel
}

type processRequest struct {
	StreamId   string
	Objects    *SendObjects
	References *SendReferences
	Stop       bool
}

func StartBatchWorkers(ctx context.Context, wg *sync.WaitGroup, concurrency int, processingQueue processingQueue, reportingQueue reportingQueue, readQueues *ReadQueues, batcher Batcher, logger logrus.FieldLogger) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	wgs := sync.Map{}
	for range concurrency {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			w := &Worker{batcher: batcher, logger: logger, readQueues: readQueues, processingQueue: processingQueue, reportingQueue: reportingQueue, wgs: &wgs}
			return w.Loop(ctx)
		})
	}
	eg.Go(func() error {
		wg.Wait() // wait for all workers to finish
		close(reportingQueue)
		return nil
	})
}

func (w *Worker) wgForStream(streamId string) *sync.WaitGroup {
	actual, _ := w.wgs.LoadOrStore(streamId, &sync.WaitGroup{})
	return actual.(*sync.WaitGroup)
}

func (w *Worker) isReplicationError(err string) bool {
	return strings.Contains(err, replica.ErrReplicas.Error()) || // broadcast error to shutdown node
		(strings.Contains(err, "connect: Post") && strings.Contains(err, ":commit")) || // failed to connect to shutdown node when committing
		(strings.Contains(err, "status code: 404, error: request not found")) // failed to find request on shutdown node
}

func (w *Worker) sendObjects(ctx context.Context, streamId string, req *SendObjects) error {
	if req == nil {
		return fmt.Errorf("received nil sendObjects request")
	}
	start := time.Now()
	reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
		Objects:          req.Values,
		ConsistencyLevel: req.ConsistencyLevel,
	})
	end := time.Now()
	if err != nil {
		return err
	}
	// Log processing time
	w.reportingQueue <- &workerStats{processingTime: end.Sub(start)}
	// Handle errors
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchObject, 0, len(reply.GetErrors()))
		errs := make([]*pb.BatchStreamReply_Error, 0, len(reply.GetErrors()))
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			if w.isReplicationError(err.Error) {
				retriable = append(retriable, req.Values[err.Index])
				continue
			}
			errs = append(errs, &pb.BatchStreamReply_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Error_Object{Object: req.Values[err.Index]},
			})
		}
		if ch, ok := w.readQueues.Get(streamId); ok {
			ch <- &readObject{Errors: errs}
		}
		if len(retriable) > 0 {
			w.sendObjects(ctx, streamId, &SendObjects{
				Values:           retriable,
				ConsistencyLevel: req.ConsistencyLevel,
			})
		}
	}
	return nil
}

func (w *Worker) sendReferences(ctx context.Context, streamId string, req *SendReferences) error {
	if req == nil {
		return fmt.Errorf("received nil sendReferences request")
	}
	start := time.Now()
	reply, err := w.batcher.BatchReferences(ctx, &pb.BatchReferencesRequest{
		References:       req.Values,
		ConsistencyLevel: req.ConsistencyLevel,
	})
	end := time.Now()
	if err != nil {
		return err
	}
	// Log processing time
	w.reportingQueue <- &workerStats{processingTime: end.Sub(start)}
	// Handle errors
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchReference, 0, len(reply.GetErrors()))
		errs := make([]*pb.BatchStreamReply_Error, 0, len(reply.GetErrors()))
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			if w.isReplicationError(err.Error) {
				retriable = append(retriable, req.Values[err.Index])
				continue
			}
			errs = append(errs, &pb.BatchStreamReply_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Error_Reference{Reference: req.Values[err.Index]},
			})
		}
		if ch, ok := w.readQueues.Get(streamId); ok {
			ch <- &readObject{Errors: errs}
		}
		if len(retriable) > 0 {
			return w.sendReferences(ctx, streamId, &SendReferences{
				Values:           retriable,
				ConsistencyLevel: req.ConsistencyLevel,
			})
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
			for req := range w.processingQueue {
				// This should only ever be received once so it’s okay to wait on the shared wait group here
				// If the scheduler does send more than one stop per stream then deadlocks may occur
				wg := w.wgForStream(req.StreamId)
				if w.isSentinel(req) {
					w.wait(req.StreamId, wg)
					return nil
				}
				if err := w.process(ctx, req, wg); err != nil {
					return fmt.Errorf("failed to process batch request: %w", err)
				}
			}
			return nil
		case req, ok := <-w.processingQueue:
			if req != nil {
				// This should only ever be received once so it’s okay to wait on the shared wait group here
				// If the scheduler does send more than one stop per stream then deadlocks may occur
				wg := w.wgForStream(req.StreamId)
				if w.isSentinel(req) {
					w.wait(req.StreamId, wg)
					return nil
				}
				if err := w.process(ctx, req, wg); err != nil {
					return fmt.Errorf("failed to process batch request: %w", err)
				}
			}
			if !ok {
				return nil // channel closed, exit loop
			}
		}
	}
}

func (w *Worker) isSentinel(req *processRequest) bool {
	return req != nil && (req.Objects == nil && req.References == nil && req.Stop)
}

func (w *Worker) wait(streamId string, wg *sync.WaitGroup) {
	wg.Wait() // Wait for all processing requests to complete
	// Signal to the reply handler that we are done
	w.readQueues.Close(streamId)
	w.wgs.Delete(streamId) // Clean up the wait group map
}

func (w *Worker) process(ctx context.Context, req *processRequest, wg *sync.WaitGroup) error {
	wg.Add(1)
	defer wg.Done()
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
	return nil
}
