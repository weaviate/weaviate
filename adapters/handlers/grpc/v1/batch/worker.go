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
}

func StartBatchWorkers(ctx context.Context, wg *sync.WaitGroup, concurrency int, processingQueue processingQueue, reportingQueue reportingQueue, readQueues *ReadQueues, batcher Batcher, logger logrus.FieldLogger) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	logger.WithField("action", "batch_stream_start").WithField("concurrency", concurrency).Info("entering worker loop(s)")
	for range concurrency {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			w := &Worker{batcher: batcher, logger: logger, readQueues: readQueues, processingQueue: processingQueue, reportingQueue: reportingQueue}
			return w.Loop(ctx)
		})
	}
	eg.Go(func() error {
		wg.Wait()             // wait for all workers to finish
		close(reportingQueue) // close reporting queue to stop the stats reporter in the scheduler
		readQueues.CloseAll() // close all read queues to stop the reply handlers in the stream handler
		return nil
	})
}

func (w *Worker) isReplicationError(err string) bool {
	return strings.Contains(err, replica.ErrReplicas.Error()) || // broadcast error to shutdown node
		(strings.Contains(err, "connect: Post") && strings.Contains(err, ":commit")) || // failed to connect to shutdown node when committing
		(strings.Contains(err, "status code: 404, error: request not found")) // failed to find request on shutdown node
}

func (w *Worker) sendObjects(ctx context.Context, streamId string, req *SendObjects, retries int) error {
	if req == nil {
		w.logger.WithField("streamId", streamId).Error("received nil sendObjects request")
		return fmt.Errorf("received nil sendObjects request")
	}
	start := time.Now()
	reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
		Objects:          req.Values,
		ConsistencyLevel: req.ConsistencyLevel,
	})
	end := time.Now()
	if err != nil {
		w.logger.WithField("streamId", streamId).WithError(err).Error("failed to batch objects")
		return err
	}

	// Handle errors
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchObject, 0, len(reply.GetErrors()))
		errs := make([]*pb.BatchStreamReply_Error, 0, len(reply.GetErrors()))
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			if w.isReplicationError(err.Error) && retries < 3 {
				retriable = append(retriable, req.Values[err.Index])
				continue
			}
			errs = append(errs, &pb.BatchStreamReply_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Error_Object{Object: req.Values[err.Index]},
			})
		}
		// Log errors
		if len(errs) > 0 {
			w.reportErrors(streamId, errs)
		}
		if len(retriable) > 0 {
			return w.sendObjects(ctx, streamId, &SendObjects{
				Values:           retriable,
				ConsistencyLevel: req.ConsistencyLevel,
			}, retries+1)
		}
	}
	// Log processing time
	w.reportStats(streamId, end.Sub(start))
	return nil
}

func (w *Worker) sendReferences(ctx context.Context, streamId string, req *SendReferences, retries int) error {
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
	// Handle errors
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchReference, 0, len(reply.GetErrors()))
		errs := make([]*pb.BatchStreamReply_Error, 0, len(reply.GetErrors()))
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			if w.isReplicationError(err.Error) && retries < 3 {
				retriable = append(retriable, req.Values[err.Index])
				continue
			}
			errs = append(errs, &pb.BatchStreamReply_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Error_Reference{Reference: req.Values[err.Index]},
			})
		}
		if len(errs) > 0 {
			w.reportErrors(streamId, errs)
		}
		if len(retriable) > 0 {
			return w.sendReferences(ctx, streamId, &SendReferences{
				Values:           retriable,
				ConsistencyLevel: req.ConsistencyLevel,
			}, retries+1)
		}
	}
	// Log processing time
	w.reportStats(streamId, end.Sub(start))
	return nil
}

func (w *Worker) reportErrors(streamId string, errs []*pb.BatchStreamReply_Error) {
	if ok := w.readQueues.Send(streamId, errs); !ok {
		w.logger.WithField("streamId", streamId).Warn("timed out sending errors to read queue, maybe the client disconnected?")
	}
}

func (w *Worker) reportStats(streamId string, processingTime time.Duration) {
	for {
		select {
		case w.reportingQueue <- NewWorkersStats(processingTime, streamId):
			return
		case <-time.After(1 * time.Second):
			w.logger.WithField("streamId", streamId).Warn("timed out sending stats to read queue, maybe the client disconnected?")
			return
		}
	}
}

// Loop processes objects from the write queue, sending them to the batcher and handling shutdown signals.
func (w *Worker) Loop(ctx context.Context) error {
	for {
		select {
		case req, ok := <-w.processingQueue:
			if req != nil {
				log := w.logger.WithField("streamId", req.StreamId)
				log.Debug("received processing request")
				if err := w.process(ctx, req); err != nil {
					log.Error(fmt.Errorf("failed to process batch request: %w", err))
				}
			}
			if !ok {
				w.logger.Debug("processing queue closed, shutting down worker")
				return nil // channel closed, exit loop
			}
		default:
			if ctx.Err() != nil {
				// Context canceled, exit loop
				w.logger.WithField("action", "shutdown_worker_loop").Error("shutting down worker loop due to cancellation")
				return ctx.Err()
			}
			time.Sleep(10 * time.Millisecond) // Prevent busy waiting
		}
	}
}

func (w *Worker) process(ctx context.Context, req *processRequest) error {
	if req.Objects != nil {
		if err := w.sendObjects(ctx, req.StreamId, req.Objects, 0); err != nil {
			return err
		}
	}
	if req.References != nil {
		if err := w.sendReferences(ctx, req.StreamId, req.References, 0); err != nil {
			return err
		}
	}
	return nil
}
