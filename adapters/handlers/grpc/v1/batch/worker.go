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
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/replica"
)

const PER_PROCESS_TIMEOUT = 60 * time.Second

type batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
	BatchReferences(ctx context.Context, req *pb.BatchReferencesRequest) (*pb.BatchReferencesReply, error)
}

type worker struct {
	batcher         batcher
	logger          logrus.FieldLogger
	reportingQueues *reportingQueues
	processingQueue processingQueue
}

type processRequest struct {
	streamId         string
	consistencyLevel *pb.ConsistencyLevel
	objects          []*pb.BatchObject
	references       []*pb.BatchReference
	// This waitgroup should be set by the method that creates the processRequest
	// and is used by the workers to signal when the processing of this request is complete.
	wg *sync.WaitGroup
	// This context contains metadata relevant to the stream as a whole, e.g. auth info,
	// that is required for downstream authZ checks by the workers, e.g. data-specific RBAC.
	streamCtx context.Context
}

// StartBatchWorkers launches a specified number of worker goroutines to process batch requests.
//
// Each worker listens on the provided processing queue for incoming batch requests, processes them
// using the provided batcher, and reports any errors or statistics to the reporting queues.
//
// The function takes a wait group to track the completion of all workers, the number of workers to start,
// the processing queue from which to read batch requests, the reporting queues for sending back results,
// the batcher interface for processing the requests, and a logger for logging purposes.
//
// This waitgroup is used by the drain shutdown logic to ensure that all workers have completed processing
// before any open server-side streams can be fully closed since ongoing workers may produce errors that need
// reporting to clients before the server shuts down completely.
func StartBatchWorkers(
	wg *sync.WaitGroup,
	concurrency int,
	processingQueue processingQueue,
	reportingQueues *reportingQueues,
	batcher batcher,
	logger logrus.FieldLogger,
) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	logger.WithField("action", "batch_workers_start").WithField("concurrency", concurrency).Debug("entering worker loop(s)")
	for range concurrency {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			w := &worker{
				batcher:         batcher,
				logger:          logger,
				reportingQueues: reportingQueues,
				processingQueue: processingQueue,
			}
			return w.Loop()
		})
	}
}

func (w *worker) isReplicationError(err string) bool {
	return strings.Contains(err, replica.ErrReplicas.Error()) || // any error due to replicating to shutdown node
		(strings.Contains(err, "connect: Post") && strings.Contains(err, ":commit")) || // failed to connect to shutdown node when committing
		(strings.Contains(err, "status code: 404, error: request not found")) // failed to find request on shutdown node
}

func (w *worker) sendObjects(ctx context.Context, streamId string, objs []*pb.BatchObject, cl *pb.ConsistencyLevel, retries int) []*pb.BatchStreamReply_Error {
	reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
		Objects:          objs,
		ConsistencyLevel: cl,
	})
	if err != nil {
		w.logger.WithField("streamId", streamId).Errorf("failed to batch objects: %s", err)
		errs := make([]*pb.BatchStreamReply_Error, 0, len(objs))
		for _, obj := range objs {
			errs = append(errs, &pb.BatchStreamReply_Error{
				Error:  err.Error(),
				Detail: &pb.BatchStreamReply_Error_Object{Object: obj},
			})
		}
		return errs
	}
	// Handle errors
	errs := make([]*pb.BatchStreamReply_Error, 0)
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchObject, 0)
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			if w.isReplicationError(err.Error) && retries < 3 {
				retriable = append(retriable, objs[err.Index])
				continue
			}
			errs = append(errs, &pb.BatchStreamReply_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Error_Object{Object: objs[err.Index]},
			})
		}
		if len(retriable) > 0 {
			errs = append(errs, w.sendObjects(ctx, streamId, retriable, cl, retries+1)...)
		}
	}
	return errs
}

func (w *worker) sendReferences(ctx context.Context, streamId string, refs []*pb.BatchReference, cl *pb.ConsistencyLevel, retries int) []*pb.BatchStreamReply_Error {
	reply, err := w.batcher.BatchReferences(ctx, &pb.BatchReferencesRequest{
		References:       refs,
		ConsistencyLevel: cl,
	})
	if err != nil {
		w.logger.WithField("streamId", streamId).Errorf("failed to batch references: %s", err)
		errs := make([]*pb.BatchStreamReply_Error, 0, len(refs))
		for _, ref := range refs {
			errs = append(errs, &pb.BatchStreamReply_Error{
				Error:  err.Error(),
				Detail: &pb.BatchStreamReply_Error_Reference{Reference: ref},
			})
		}
		return errs
	}
	// Handle errors
	errs := make([]*pb.BatchStreamReply_Error, 0)
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchReference, 0)
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			if w.isReplicationError(err.Error) && retries < 3 {
				retriable = append(retriable, refs[err.Index])
				continue
			}
			errs = append(errs, &pb.BatchStreamReply_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Error_Reference{Reference: refs[err.Index]},
			})
		}
		if len(retriable) > 0 {
			errs = append(errs, w.sendReferences(ctx, streamId, retriable, cl, retries+1)...)
		}
	}
	return errs
}

// Loop is the entrypoint for the worker logic.
//
// It continuously listens for incoming process requests on the processing queue,
// processes each request by sending the objects and references to the batcher,
// and reports any errors or statistics back to the reporting queues.
//
// The loop runs until the processing queue is closed, at which point the worker
// logs its shutdown and exits gracefully.
func (w *worker) Loop() error {
	for req := range w.processingQueue {
		if req == nil {
			w.logger.WithField("action", "batch_worker_loop").Error("received nil process request")
			continue
		}
		w.process(req)
	}
	w.logger.Debug("processing queue closed, shutting down worker")
	return nil // channel closed, exit loop
}

func (w *worker) process(req *processRequest) {
	start := time.Now()
	defer req.wg.Done()

	ctx, cancel := context.WithTimeout(req.streamCtx, PER_PROCESS_TIMEOUT)
	defer cancel()

	errs := make([]*pb.BatchStreamReply_Error, 0)
	if req.objects != nil {
		errs = append(errs, w.sendObjects(ctx, req.streamId, req.objects, req.consistencyLevel, 0)...)
	}
	if req.references != nil {
		errs = append(errs, w.sendReferences(ctx, req.streamId, req.references, req.consistencyLevel, 0)...)
	}

	stats := newWorkersStats(time.Since(start))
	if ok := w.reportingQueues.send(req.streamId, errs, stats); !ok {
		w.logger.WithField("streamId", req.streamId).Warn("timed out sending errors to read queue, maybe the client disconnected?")
	}
}
