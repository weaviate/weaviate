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

type Batcher interface {
	BatchObjects(ctx context.Context, req *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error)
	BatchReferences(ctx context.Context, req *pb.BatchReferencesRequest) (*pb.BatchReferencesReply, error)
}

type Worker struct {
	batcher         Batcher
	logger          logrus.FieldLogger
	reportingQueues *reportingQueues
	processingQueue processingQueue
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
	StreamId         string
	ConsistencyLevel *pb.ConsistencyLevel
	Objects          []*pb.BatchObject
	References       []*pb.BatchReference
	Wg               *sync.WaitGroup
	Ctx              context.Context
}

func StartBatchWorkers(
	wg *sync.WaitGroup,
	concurrency int,
	processingQueue processingQueue,
	reportingQueues *reportingQueues,
	batcher Batcher,
	logger logrus.FieldLogger,
) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	logger.WithField("action", "batch_workers_start").WithField("concurrency", concurrency).Debug("entering worker loop(s)")
	for range concurrency {
		wg.Add(1)
		eg.Go(func() error {
			defer wg.Done()
			w := &Worker{
				batcher:         batcher,
				logger:          logger,
				reportingQueues: reportingQueues,
				processingQueue: processingQueue,
			}
			return w.Loop()
		})
	}
}

func (w *Worker) isReplicationError(err string) bool {
	return strings.Contains(err, replica.ErrReplicas.Error()) || // broadcast error to shutdown node
		(strings.Contains(err, "connect: Post") && strings.Contains(err, ":commit")) || // failed to connect to shutdown node when committing
		(strings.Contains(err, "status code: 404, error: request not found")) // failed to find request on shutdown node
}

func (w *Worker) sendObjects(ctx context.Context, streamId string, objs []*pb.BatchObject, cl *pb.ConsistencyLevel, retries int) []*pb.BatchStreamReply_Error {
	reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
		Objects:          objs,
		ConsistencyLevel: cl,
	})
	if err != nil {
		w.logger.WithField("streamId", streamId).WithError(err).Error("failed to batch objects")
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
	errs := make([]*pb.BatchStreamReply_Error, 0, len(reply.GetErrors()))
	retriable := make([]*pb.BatchObject, 0, len(reply.GetErrors()))
	if len(reply.GetErrors()) > 0 {
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
	}
	if len(retriable) > 0 {
		errs = append(errs, w.sendObjects(ctx, streamId, retriable, cl, retries+1)...)
	}
	return errs
}

func (w *Worker) sendReferences(ctx context.Context, streamId string, refs []*pb.BatchReference, cl *pb.ConsistencyLevel, retries int) []*pb.BatchStreamReply_Error {
	reply, err := w.batcher.BatchReferences(ctx, &pb.BatchReferencesRequest{
		References:       refs,
		ConsistencyLevel: cl,
	})
	if err != nil {
		w.logger.WithField("streamId", streamId).WithError(err).Error("failed to batch references")
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
	errs := make([]*pb.BatchStreamReply_Error, 0, len(reply.GetErrors()))
	retriable := make([]*pb.BatchReference, 0, len(reply.GetErrors()))
	if len(reply.GetErrors()) > 0 {
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
	}
	if len(retriable) > 0 {
		errs = append(errs, w.sendReferences(ctx, streamId, retriable, cl, retries+1)...)
	}
	return errs
}

func (w *Worker) report(streamId string, errs []*pb.BatchStreamReply_Error, stats *workerStats) {
	if ok := w.reportingQueues.Send(streamId, errs, stats); !ok {
		w.logger.WithField("streamId", streamId).Warn("timed out sending errors to read queue, maybe the client disconnected?")
	}
}

// Loop processes objects from the write queue, sending them to the batcher and handling shutdown signals.
func (w *Worker) Loop() error {
	for req := range w.processingQueue {
		if req != nil {
			start := time.Now()
			w.report(
				req.StreamId,
				w.process(req),
				newWorkersStats(time.Since(start)),
			)
		} else {
			w.logger.WithField("action", "batch_worker_loop").Error("received nil process request")
		}
	}
	w.logger.Debug("processing queue closed, shutting down worker")
	return nil // channel closed, exit loop
}

func (w *Worker) process(req *processRequest) []*pb.BatchStreamReply_Error {
	ctx, cancel := context.WithTimeout(req.Ctx, PER_PROCESS_TIMEOUT)
	defer cancel()
	defer req.Wg.Done()

	errs := make([]*pb.BatchStreamReply_Error, 0, len(req.Objects)+len(req.References))
	if req.Objects != nil {
		errs = append(errs, w.sendObjects(ctx, req.StreamId, req.Objects, req.ConsistencyLevel, 0)...)
	}
	if req.References != nil {
		errs = append(errs, w.sendReferences(ctx, req.StreamId, req.References, req.ConsistencyLevel, 0)...)
	}
	return errs
}
