//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

const (
	PER_PROCESS_TIMEOUT = 60 * time.Second
	MAX_RETRIES         = 5
	BACKOFF_RETRY_TIME  = 100 * time.Millisecond
)

// batcher implementations must not mutate the objects or references in the request:
// the retry round re-sends the same pointers, so any rewrite makes the retry a
// different request from the first attempt.
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
	// If the collections of the objects within this request use vectorisation or not;
	// this should be accounted for by fanning out the objects to better improve I/O concurrency
	// to the third-party vectoriser for the specific classes that use it.
	usesVectorisationByCollection map[string]bool
	// This context contains metadata relevant to the stream as a whole, e.g. auth info,
	// that is required for downstream authZ checks by the workers, e.g. data-specific RBAC.
	streamCtx context.Context
	// Callback to signal process completion and perform any necessary cleanup
	onComplete func()
	// Callback to signal process beginning
	onStart func()
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

func (w *worker) isTransientReplicationError(err string) bool {
	return strings.Contains(err, replicaerrors.ErrReplicas.Error()) || // coordinator: any error due to replicating to shutdown node
		strings.Contains(err, "connect: Post") || // rest: failed to connect to shutdown node
		strings.Contains(err, "status code: 404, error: request not found") || // rest: failed to find request on shutdown node
		(strings.Contains(err, "resolve node name") && strings.Contains(err, "to host")) || // memberlist: failed to resolve to other shutdown node in cluster
		strings.Contains(err, "the client connection is closing") || // grpc: connection to other shutdown node is closed
		strings.Contains(err, "Node not ready") // rest: node is not ready to accept requests (still starting up or shutting down)
}

// fanoutReply carries the sub-batch it answers, because replies arrive in
// whatever order the concurrent calls finish. subBatch says which error indices
// are valid; offset says where the sub-batch starts in the collection's object
// list. Together they trace every error index back to its object.
type fanoutReply struct {
	reply    *pb.BatchObjectsReply
	err      error
	subBatch []*pb.BatchObject
	offset   int
}

func (w *worker) fanoutObjects(
	ctx context.Context,
	objs []*pb.BatchObject,
	cl *pb.ConsistencyLevel,
	fanoutAmount int,
) chan fanoutReply {
	ch := make(chan fanoutReply, fanoutAmount)
	var wg sync.WaitGroup

	subBatchLen := int(math.Ceil(float64(len(objs)) / float64(fanoutAmount)))
	for i := 0; i < fanoutAmount; i++ {
		start := i * subBatchLen
		if start >= len(objs) {
			break
		}
		end := (i + 1) * subBatchLen
		if end > len(objs) {
			end = len(objs)
		}
		subBatch := objs[start:end]
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			reply, err := w.batcher.BatchObjects(ctx, &pb.BatchObjectsRequest{
				Objects:          subBatch,
				ConsistencyLevel: cl,
			})
			ch <- fanoutReply{reply: reply, err: err, subBatch: subBatch, offset: start}
		}, w.logger)
	}

	enterrors.GoWrapper(func() {
		wg.Wait()
		close(ch)
	}, w.logger)

	return ch
}

func (w *worker) sendObjects(
	ctx context.Context,
	streamId string,
	objs []*pb.BatchObject,
	cl *pb.ConsistencyLevel,
	usesVectorisationByCollection map[string]bool,
	retries int,
) ([]*pb.BatchStreamReply_Results_Success, []*pb.BatchStreamReply_Results_Error) {
	if ctx.Err() != nil {
		w.logger.WithField("streamId", streamId).Warnf("context error before sending objects: %s", ctx.Err())
		errors := make([]*pb.BatchStreamReply_Results_Error, 0, len(objs))
		for _, obj := range objs {
			errors = append(errors, &pb.BatchStreamReply_Results_Error{
				Error:  ctx.Err().Error(),
				Detail: &pb.BatchStreamReply_Results_Error_Uuid{Uuid: obj.Uuid},
			})
		}
		return nil, errors
	}

	// Assumption is no errors, so don't preallocate error slice
	errors := make([]*pb.BatchStreamReply_Results_Error, 0)
	// Assumption is all successes, so preallocate success slice
	successes := make([]*pb.BatchStreamReply_Results_Success, 0, len(objs))
	// keyed by index into objs, the same indexing the success loop below uses
	failed := make(map[int]struct{})
	// Keep track of retriable errors to send again
	retriable := make([]*pb.BatchObject, 0)

	idxsByCollection := make(map[string][]int)
	for i, obj := range objs {
		idxsByCollection[obj.Collection] = append(idxsByCollection[obj.Collection], i)
	}

	for collection, outerIdxs := range idxsByCollection {
		fanoutAmount := 1
		if usesVectorisationByCollection[collection] {
			fanoutAmount = 10
		}
		collectionObjs := make([]*pb.BatchObject, 0, len(outerIdxs))
		for _, i := range outerIdxs {
			collectionObjs = append(collectionObjs, objs[i])
		}
		replies := w.fanoutObjects(ctx, collectionObjs, cl, fanoutAmount)
		errorsInner, retriableInner := w.consumeFanoutReplies(streamId, replies, objs, outerIdxs, retries, failed)
		errors = append(errors, errorsInner...)
		retriable = append(retriable, retriableInner...)
	}
	if len(retriable) > 0 {
		// exponential backoff with 2 ** n
		if retries > 0 {
			// retry immediately on first retry
			<-time.After(time.Duration(math.Pow(2, float64(retries))) * BACKOFF_RETRY_TIME)
		}
		w.logger.WithField("streamId", streamId).Warnf("retrying %d transient replication errors for objects", len(retriable))
		successesInner, errorsInner := w.sendObjects(ctx, streamId, retriable, cl, usesVectorisationByCollection, retries+1)
		successes = append(successes, successesInner...)
		errors = append(errors, errorsInner...)
	}
	// Handle successes
	for i, obj := range objs {
		if _, ok := failed[i]; ok {
			continue
		}
		successes = append(successes, &pb.BatchStreamReply_Results_Success{
			Detail: &pb.BatchStreamReply_Results_Success_Uuid{Uuid: obj.Uuid},
		})
	}
	return successes, errors
}

// consumeFanoutReplies drains one collection's fanout replies and records every
// reply error on the object it belongs to. outerIdxs[j] gives the position of
// the collection's j-th object in the full batch; failed is keyed by that
// position and mutated in place. A reply's error indices only mean something
// inside its own sub-batch, so an out-of-range index is dropped rather than
// blamed on whichever object happens to sit at that position.
func (w *worker) consumeFanoutReplies(
	streamId string,
	replies <-chan fanoutReply,
	objs []*pb.BatchObject,
	outerIdxs []int,
	retries int,
	failed map[int]struct{},
) (errs []*pb.BatchStreamReply_Results_Error, retriable []*pb.BatchObject) {
	log := w.logger.WithField("streamId", streamId)
	for resp := range replies {
		if resp.err != nil {
			log.Errorf("failed to batch objects: %s", resp.err)
			for k := range resp.subBatch {
				outer := outerIdxs[resp.offset+k]
				failed[outer] = struct{}{}
				errs = append(errs, &pb.BatchStreamReply_Results_Error{
					Error:  resp.err.Error(),
					Detail: &pb.BatchStreamReply_Results_Error_Uuid{Uuid: objs[outer].Uuid},
				})
			}
			continue
		}
		for _, err := range resp.reply.GetErrors() {
			if err == nil {
				continue
			}
			if err.Index < 0 || int(err.Index) >= len(resp.subBatch) {
				log.Errorf("dropping batch reply error with index %d outside the sub-batch of %d objects: %s", err.Index, len(resp.subBatch), err.Error)
				continue
			}
			outer := outerIdxs[resp.offset+int(err.Index)]
			obj := objs[outer]
			failed[outer] = struct{}{}
			if w.isTransientReplicationError(err.Error) && retries < MAX_RETRIES {
				log.Infof("transient replication error for object %s: %s", obj.Uuid, err.Error)
				retriable = append(retriable, obj)
				continue
			}
			errs = append(errs, &pb.BatchStreamReply_Results_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Results_Error_Uuid{Uuid: obj.Uuid},
			})
		}
	}
	return errs, retriable
}

func toBeacon(ref *pb.BatchReference) string {
	return fmt.Sprintf("%s%s/%s/%s", BEACON_START, ref.FromCollection, ref.FromUuid, ref.Name)
}

func (w *worker) sendReferences(ctx context.Context, streamId string, refs []*pb.BatchReference, cl *pb.ConsistencyLevel, retries int) ([]*pb.BatchStreamReply_Results_Success, []*pb.BatchStreamReply_Results_Error) {
	if ctx.Err() != nil {
		w.logger.WithField("streamId", streamId).Warnf("context error before sending references: %s", ctx.Err())
		errors := make([]*pb.BatchStreamReply_Results_Error, 0, len(refs))
		for _, ref := range refs {
			errors = append(errors, &pb.BatchStreamReply_Results_Error{
				Error:  ctx.Err().Error(),
				Detail: &pb.BatchStreamReply_Results_Error_Beacon{Beacon: toBeacon(ref)},
			})
		}
		return nil, errors
	}
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
	failed := make(map[int32]struct{})
	if len(reply.GetErrors()) > 0 {
		retriable := make([]*pb.BatchReference, 0)
		for _, err := range reply.GetErrors() {
			if err == nil {
				continue
			}
			// an index outside the request names no reference, so it is dropped
			// rather than applied to whichever reference happens to sit there
			if err.Index < 0 || int(err.Index) >= len(refs) {
				w.logger.WithField("streamId", streamId).Errorf("dropping reference reply error with index %d outside the request of %d references: %s", err.Index, len(refs), err.Error)
				continue
			}
			failed[err.Index] = struct{}{}
			if w.isTransientReplicationError(err.Error) && retries < MAX_RETRIES {
				w.logger.WithField("streamId", streamId).Infof("transient replication error for reference %s: %s", toBeacon(refs[err.Index]), err.Error)
				retriable = append(retriable, refs[err.Index])
				continue
			}
			errors = append(errors, &pb.BatchStreamReply_Results_Error{
				Error:  err.Error,
				Detail: &pb.BatchStreamReply_Results_Error_Beacon{Beacon: toBeacon(refs[err.Index])},
			})
		}
		if len(retriable) > 0 {
			// exponential backoff with 2 ** n
			if retries > 0 {
				// retry immediately on first retry
				<-time.After(time.Duration(math.Pow(2, float64(retries))) * BACKOFF_RETRY_TIME)
			}
			w.logger.WithField("streamId", streamId).Warnf("retrying %d transient replication errors for references", len(retriable))
			successesInner, errorsInner := w.sendReferences(ctx, streamId, retriable, cl, retries+1)
			successes = append(successes, successesInner...)
			errors = append(errors, errorsInner...)
		}
	}
	// Handle successes
	for i, ref := range refs {
		if _, ok := failed[int32(i)]; ok {
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
		w.process(req)
	}
	w.logger.Debug("processing queue closed, shutting down worker")
	return nil // channel closed, exit loop
}

func (w *worker) process(req *processRequest) {
	req.onStart()
	defer req.onComplete()

	start := time.Now()
	ctx, cancel := context.WithTimeout(req.streamCtx, PER_PROCESS_TIMEOUT)
	defer cancel()

	successes := make([]*pb.BatchStreamReply_Results_Success, 0, len(req.objects)+len(req.references))
	errors := make([]*pb.BatchStreamReply_Results_Error, 0)
	if len(req.objects) > 0 {
		successesInner, errorsInner := w.sendObjects(ctx, req.streamId, req.objects, req.consistencyLevel, req.usesVectorisationByCollection, 0)
		successes = append(successes, successesInner...)
		errors = append(errors, errorsInner...)
	}
	if len(req.references) > 0 {
		successesInner, errorsInner := w.sendReferences(ctx, req.streamId, req.references, req.consistencyLevel, 0)
		successes = append(successes, successesInner...)
		errors = append(errors, errorsInner...)
	}

	stats := newWorkersStats(time.Since(start))
	if err := w.reportingQueues.send(req.streamCtx, req.streamId, successes, errors, stats); err != nil {
		w.logger.WithField("streamId", req.streamId).Warnf("failed to send report to reporting queue: %s", err)
	}
}
