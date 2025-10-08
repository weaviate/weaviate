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

const perProcessTimeout = 90 * time.Second

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
}

func StartBatchWorkers(
	ctx context.Context,
	wg *sync.WaitGroup,
	concurrency int,
	processingQueue processingQueue,
	reportingQueues *reportingQueues,
	batcher Batcher,
	logger logrus.FieldLogger,
) {
	eg := enterrors.NewErrorGroupWrapper(logger)
	logger.WithField("action", "batch_workers_start").WithField("concurrency", concurrency).Info("entering worker loop(s)")
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
			return w.Loop(ctx)
		})
	}
}

func (w *Worker) isReplicationError(err string) bool {
	return strings.Contains(err, replica.ErrReplicas.Error()) || // broadcast error to shutdown node
		(strings.Contains(err, "connect: Post") && strings.Contains(err, ":commit")) || // failed to connect to shutdown node when committing
		(strings.Contains(err, "status code: 404, error: request not found")) // failed to find request on shutdown node
}

type errCh chan *pb.BatchStreamReply_Error

func (w *Worker) sendObjects(ctx context.Context, errCh errCh, streamId string, objs []*pb.BatchObject, cl *pb.ConsistencyLevel, retries int) error {
	if len(objs) == 0 {
		w.logger.WithField("streamId", streamId).Error("received nil sendObjects request")
		return fmt.Errorf("received nil sendObjects request")
	}
	reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
		Objects:          objs,
		ConsistencyLevel: cl,
	})
	if err != nil {
		w.logger.WithField("streamId", streamId).WithError(err).Error("failed to batch objects")
		return err
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
	for _, err := range errs {
		errCh <- err
	}
	if len(retriable) > 0 {
		return w.sendObjects(ctx, errCh, streamId, retriable, cl, retries+1)
	}
	return nil
}

func (w *Worker) sendReferences(ctx context.Context, errCh errCh, streamId string, refs []*pb.BatchReference, cl *pb.ConsistencyLevel, retries int) error {
	if len(refs) == 0 {
		return fmt.Errorf("received nil sendReferences request")
	}
	reply, err := w.batcher.BatchReferences(ctx, &pb.BatchReferencesRequest{
		References:       refs,
		ConsistencyLevel: cl,
	})
	if err != nil {
		return err
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
	for _, err := range errs {
		errCh <- err
	}
	if len(retriable) > 0 {
		return w.sendReferences(ctx, errCh, streamId, retriable, cl, retries+1)
	}
	return nil
}

func (w *Worker) report(streamId string, errs []*pb.BatchStreamReply_Error, stats *workerStats) {
	if ok := w.reportingQueues.Send(streamId, errs, stats); !ok {
		w.logger.WithField("streamId", streamId).Warn("timed out sending errors to read queue, maybe the client disconnected?")
	}
}

// Loop processes objects from the write queue, sending them to the batcher and handling shutdown signals.
func (w *Worker) Loop(ctx context.Context) error {
	for req := range w.processingQueue {
		if req != nil {
			log := w.logger.WithField("streamId", req.StreamId)
			log.Debug("received processing request")
			if err := w.process(ctx, req); err != nil {
				log.Error(fmt.Errorf("failed to process batch request: %w", err))
			}
		}
	}
	w.logger.Info("processing queue closed, shutting down worker")
	return nil // channel closed, exit loop
}

func (w *Worker) process(ctx context.Context, req *processRequest) error {
	defer req.Wg.Done()

	ctx, cancel := context.WithTimeout(ctx, perProcessTimeout)
	defer cancel()

	start := time.Now()
	errCh := make(chan *pb.BatchStreamReply_Error, len(req.Objects)+len(req.References))
	if req.Objects != nil {
		if err := w.sendObjects(ctx, errCh, req.StreamId, req.Objects, req.ConsistencyLevel, 0); err != nil {
			return err
		}
	}
	if req.References != nil {
		if err := w.sendReferences(ctx, errCh, req.StreamId, req.References, req.ConsistencyLevel, 0); err != nil {
			return err
		}
	}
	close(errCh)
	errs := make([]*pb.BatchStreamReply_Error, 0, len(errCh))
	for err := range errCh {
		errs = append(errs, err)
	}
	w.report(req.StreamId, errs, NewWorkersStats(time.Since(start)))
	return nil
}
