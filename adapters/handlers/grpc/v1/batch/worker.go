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
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

const PER_PROCESS_TIMEOUT = 90 * time.Second

type batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
	BatchReferences(ctx context.Context, req *pb.BatchReferencesRequest) (*pb.BatchReferencesReply, error)
}

type worker struct {
	batcher                batcher
	logger                 logrus.FieldLogger
	reportingQueues        *reportingQueues
	processingQueue        processingQueue
	enqueuedObjectsCounter *atomic.Int32
	metrics                *BatchStreamingMetrics
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
// using the provided batcher, and results any errors or statistics to the resulting queues.
//
// The function takes a wait group to track the completion of all workers, the number of workers to start,
// the processing queue from which to read batch requests, the resulting queues for sending back results,
// the batcher interface for processing the requests, and a logger for logging purposes.
//
// This waitgroup is used by the drain shutdown logic to ensure that all workers have completed processing
// before any open server-side streams can be fully closed since ongoing workers may produce errors that need
// resulting to clients before the server shuts down completely.
func StartBatchWorkers(
	wg *sync.WaitGroup,
	concurrency int,
	processingQueue processingQueue,
	reportingQueues *reportingQueues,
	batcher batcher,
	enqueuedObjectsCounter *atomic.Int32,
	metrics *BatchStreamingMetrics,
	logger logrus.FieldLogger,
) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	logger.WithField("action", "batch_workers_start").WithField("concurrency", concurrency).Debug("entering worker loop(s)")
	for range concurrency {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			w := &worker{
				batcher:                batcher,
				logger:                 logger,
				reportingQueues:        reportingQueues,
				processingQueue:        processingQueue,
				enqueuedObjectsCounter: enqueuedObjectsCounter,
				metrics:                metrics,
			}
			return w.Loop()
		})
	}
}

func (w *worker) isReplicationError(err string) bool {
	return replicaerrors.IsReplicasError(errors.New(err)) || // coordinator: any error due to replicating to shutdown node
		strings.Contains(err, "connect: Post") || // rest: failed to connect to shutdown node
		strings.Contains(err, "status code: 404, error: request not found") || // rest: failed to find request on shutdown node
		(strings.Contains(err, "resolve node name") && strings.Contains(err, "to host")) || // memberlist: failed to resolve to other shutdown node in cluster
		(strings.Contains(err, "the client connection is closing")) // grpc: connection to other shutdown node is closed
}

func (w *worker) sendObjects(ctx context.Context, streamId string, objs []*pb.BatchObject, cl *pb.ConsistencyLevel, retries int) ([]*pb.BatchStreamReply_Results_Success, []*pb.BatchStreamReply_Results_Error) {
	reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
		Objects:          objs,
		ConsistencyLevel: cl,
	})
	if err != nil {
		errors := make([]*pb.BatchStreamReply_Results_Error, 0, len(objs))
		w.logger.WithField("streamId", streamId).Errorf("failed to batch objects: %s", err)
		for _, obj := range objs {
			errors = append(errors, &pb.BatchStreamReply_Results_Error{
				Error:  err.Error(),
				Detail: &pb.BatchStreamReply_Results_Error_Uuid{Uuid: obj.Uuid},
			})
		}
		return nil, errors
	}
	// Assumption is no errors, so don't preallocate error slice
	errors := make([]*pb.BatchStreamReply_Results_Error, 0)
	// Assumption is all successes, so preallocate success slice
	successes := make([]*pb.BatchStreamReply_Results_Success, 0, len(objs))
	// Handle errors
	errored := make(map[int32]struct{})
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchObject, 0)
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			errored[err.Index] = struct{}{}
			if w.isReplicationError(err.Error) && retries < 3 {
				retriable = append(retriable, objs[err.Index])
				continue
			}
			errors = append(errors, &pb.BatchStreamReply_Results_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Results_Error_Uuid{Uuid: objs[err.Index].Uuid},
			})
		}
		if len(retriable) > 0 {
			// exponential backoff with 2 ** n and 100ms base
			<-time.After(time.Duration(math.Pow(2, float64(retries))) * 100 * time.Millisecond)
			successesInner, errorsInner := w.sendObjects(ctx, streamId, retriable, cl, retries+1)
			successes = append(successes, successesInner...)
			errors = append(errors, errorsInner...)
		}
	}
	// Handle successes
	for i, obj := range objs {
		if _, ok := errored[int32(i)]; ok {
			continue
		}
		successes = append(successes, &pb.BatchStreamReply_Results_Success{
			Detail: &pb.BatchStreamReply_Results_Success_Uuid{Uuid: obj.Uuid},
		})
	}
	return successes, errors
}

func toBeacon(ref *pb.BatchReference) string {
	return fmt.Sprintf("%s%s/%s/%s", BEACON_START, ref.FromCollection, ref.FromUuid, ref.Name)
}

func (w *worker) sendReferences(ctx context.Context, streamId string, refs []*pb.BatchReference, cl *pb.ConsistencyLevel, retries int) ([]*pb.BatchStreamReply_Results_Success, []*pb.BatchStreamReply_Results_Error) {
	reply, err := w.batcher.BatchReferences(ctx, &pb.BatchReferencesRequest{
		References:       refs,
		ConsistencyLevel: cl,
	})
	if err != nil {
		w.logger.WithField("streamId", streamId).Errorf("failed to batch references: %s", err)
		errors := make([]*pb.BatchStreamReply_Results_Error, 0, len(refs))
		for _, ref := range refs {
			errors = append(errors, &pb.BatchStreamReply_Results_Error{
				Error:  err.Error(),
				Detail: &pb.BatchStreamReply_Results_Error_Beacon{Beacon: toBeacon(ref)},
			})
		}
		return nil, errors
	}
	// Assumption is no errors, so don't preallocate error slice
	errors := make([]*pb.BatchStreamReply_Results_Error, 0)
	// Assumption is all successes, so preallocate success slice
	successes := make([]*pb.BatchStreamReply_Results_Success, 0, len(refs))
	// Handle errors
	errored := make(map[int32]struct{})
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchReference, 0)
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			errored[err.Index] = struct{}{}
			if w.isReplicationError(err.Error) && retries < 3 {
				retriable = append(retriable, refs[err.Index])
				continue
			}
			errors = append(errors, &pb.BatchStreamReply_Results_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Results_Error_Beacon{Beacon: toBeacon(refs[err.Index])},
			})
		}
		if len(retriable) > 0 {
			// exponential backoff with 2 ** n and 100ms base
			<-time.After(time.Duration(math.Pow(2, float64(retries))) * 100 * time.Millisecond)
			successesInner, errorsInner := w.sendReferences(ctx, streamId, retriable, cl, retries+1)
			successes = append(successes, successesInner...)
			errors = append(errors, errorsInner...)
		}
	}
	// Handle successes
	for i, ref := range refs {
		if _, ok := errored[int32(i)]; ok {
			continue
		}
		successes = append(successes, &pb.BatchStreamReply_Results_Success{
			Detail: &pb.BatchStreamReply_Results_Success_Beacon{Beacon: toBeacon(ref)},
		})
	}
	return successes, errors
}

// Loop is the entrypoint for the worker logic.
//
// It continuously listens for incoming process requests on the processing queue,
// processes each request by sending the objects and references to the batcher,
// and results any errors or statistics back to the resulting queues.
//
// The loop runs until the processing queue is closed, at which point the worker
// logs its shutdown and exits gracefully.
func (w *worker) Loop() error {
	for req := range w.processingQueue {
		if req == nil {
			w.logger.WithField("action", "batch_worker_loop").Error("received nil process request")
			continue
		}
		howMany := len(req.objects) + len(req.references)
		if w.metrics != nil {
			w.metrics.OnProcessingQueuePull(howMany)
		}
		w.enqueuedObjectsCounter.Add(-int32(howMany))
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

	successes := make([]*pb.BatchStreamReply_Results_Success, 0, len(req.objects)+len(req.references))
	errors := make([]*pb.BatchStreamReply_Results_Error, 0)
	if len(req.objects) > 0 {
		successesInner, errorsInner := w.sendObjects(ctx, req.streamId, req.objects, req.consistencyLevel, 0)
		successes = append(successes, successesInner...)
		errors = append(errors, errorsInner...)
	}
	if len(req.references) > 0 {
		successesInner, errorsInner := w.sendReferences(ctx, req.streamId, req.references, req.consistencyLevel, 0)
		successes = append(successes, successesInner...)
		errors = append(errors, errorsInner...)
	}

	stats := newWorkersStats(time.Since(start))
	if ok := w.reportingQueues.send(req.streamId, successes, errors, stats); !ok {
		w.logger.WithField("streamId", req.streamId).Warn("timed out sending a worker report to the reporting queue, maybe the client disconnected?")
	}
}
