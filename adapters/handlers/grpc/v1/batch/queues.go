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
	"time"

	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type QueuesHandler struct {
	grpcShutdownCtx context.Context
	logger          logrus.FieldLogger
	writeQueues     *WriteQueues
	readQueues      *ReadQueues
}

func NewQueuesHandler(grpcShutdownCtx context.Context, writeQueues *WriteQueues, readQueues *ReadQueues, logger logrus.FieldLogger) *QueuesHandler {
	return &QueuesHandler{
		grpcShutdownCtx: grpcShutdownCtx,
		logger:          logger,
		writeQueues:     writeQueues,
		readQueues:      readQueues,
	}
}

func (h *QueuesHandler) Stream(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer) error {
	if err := stream.Send(newBatchStartMessage(streamId)); err != nil {
		return err
	}
	for {
		if readQueue, ok := h.readQueues.Get(streamId); ok {
			select {
			case <-ctx.Done():
				if innerErr := stream.Send(newBatchStopMessage(streamId)); innerErr != nil {
					return innerErr
				}
				return nil
			case <-h.grpcShutdownCtx.Done():
				if innerErr := stream.Send(newBatchShutdownMessage(streamId)); innerErr != nil {
					return innerErr
				}
				return nil
			case errs := <-readQueue:
				if errs.Stop {
					if innerErr := stream.Send(newBatchStopMessage(streamId)); innerErr != nil {
						return innerErr
					}
					return nil
				}
				for _, err := range errs.Errors {
					if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
						return innerErr
					}
				}
			}
		} else {
			// This should never happen, but if it does, we log it
			h.logger.WithField("streamId", streamId).Error("read queue not found")
			return fmt.Errorf("read queue for stream %s not found", streamId)
		}
	}
}

// Send adds a batch send request to the write queue and returns the number of objects in the request.
func (h *QueuesHandler) Send(ctx context.Context, request *pb.BatchSendRequest) int32 {
	queue, ok := h.writeQueues.Get(request.GetStreamId())
	if !ok {
		h.logger.WithField("streamId", request.GetStreamId()).Error("write queue not found")
		return 0
	}
	for _, obj := range request.GetObjects().GetValues() {
		queue <- &writeObject{Object: obj}
	}
	for _, ref := range request.GetReferences().GetValues() {
		queue <- &writeObject{Reference: ref}
	}
	if request.GetStop() != nil {
		queue <- &writeObject{Stop: true}
	}
	return int32(len(request.GetObjects().GetValues()))
}

// Setup initializes a read queue for the given stream ID and adds it to the read queues map.
func (h *QueuesHandler) Setup(streamId string, consistencyLevel *pb.ConsistencyLevel) {
	h.readQueues.Make(streamId)
	h.writeQueues.Make(streamId, consistencyLevel)
}

// Teardown closes the read queue for the given stream ID and removes it from the read queues map.
func (h *QueuesHandler) Teardown(streamId string) {
	if _, ok := h.readQueues.Get(streamId); ok {
		h.readQueues.Delete(streamId)
	} else {
		h.logger.WithField("streamId", streamId).Warn("teardown called for non-existing stream")
	}
	if _, ok := h.writeQueues.Get(streamId); ok {
		h.writeQueues.Delete(streamId)
	} else {
		h.logger.WithField("streamId", streamId).Warn("teardown called for non-existing write queue")
	}
}

type Scheduler struct {
	grpcShutdownCtx context.Context
	logger          logrus.FieldLogger
	writeQueues     *WriteQueues
	internalQueue   internalQueue
}

func NewScheduler(grpcShutdownCtx context.Context, writeQueues *WriteQueues, internalQueue internalQueue, logger logrus.FieldLogger) *Scheduler {
	return &Scheduler{
		grpcShutdownCtx: grpcShutdownCtx,
		logger:          logger,
		writeQueues:     writeQueues,
		internalQueue:   internalQueue,
	}
}

func (s *Scheduler) Loop() {
	for {
		select {
		case <-s.grpcShutdownCtx.Done():
			s.logger.Info("shutting down scheduler loop")
			return
		default:
			s.writeQueues.queues.Range(func(key, value any) bool {
				streamId, ok := key.(string)
				if !ok {
					s.logger.WithField("key", key).Error("expected string key in write queues")
					return true // continue iteration
				}
				wq, ok := value.(*WriteQueue)
				if !ok {
					s.logger.WithField("value", value).Error("expected WriteQueue value in write queues")
					return true // continue iteration
				}
				if len(wq.queue) == 0 {
					return true // continue iteration if queue is empty
				}
				return s.add(streamId, wq)
			})
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (s *Scheduler) add(streamId string, wq *WriteQueue) bool {
	weight := len(wq.queue) // or smoothed average
	objs := make([]*pb.BatchObject, 0, weight)
	refs := make([]*pb.BatchReference, 0, weight)
	stop := false
	for i := 0; i < weight && len(wq.queue) > 0; i++ {
		select {
		case <-s.grpcShutdownCtx.Done():
			s.logger.Info("shutting down scheduler loop due to grpc shutdown")
			return false // stop iteration
		case obj := <-wq.queue:
			if obj.Object != nil {
				objs = append(objs, obj.Object)
			}
			if obj.Reference != nil {
				refs = append(refs, obj.Reference)
			}
			if obj.Stop {
				stop = true
			}
		default:
		}
	}
	if len(objs) > 0 {
		s.internalQueue <- &ProcessRequest{
			StreamId: streamId,
			Objects: &SendObjects{
				Values:           objs,
				ConsistencyLevel: wq.consistencyLevel,
				Index:            wq.objIndex,
			},
		}
		wq.objIndex += int32(len(objs))
		s.logger.WithFields(logrus.Fields{
			"streamId": streamId,
			"count":    len(objs),
		}).Debug("scheduled batch write request")
	}
	if len(refs) > 0 {
		s.internalQueue <- &ProcessRequest{
			StreamId: streamId,
			References: &SendReferences{
				Values:           refs,
				ConsistencyLevel: wq.consistencyLevel,
				Index:            wq.refIndex,
			},
		}
		wq.refIndex += int32(len(refs))
		s.logger.WithFields(logrus.Fields{
			"streamId": streamId,
			"count":    len(refs),
		}).Debug("scheduled batch reference request")
	}
	if stop {
		s.internalQueue <- &ProcessRequest{
			StreamId: streamId,
			Stop:     true,
		}
	}
	time.Sleep(time.Millisecond * 5)
	return true
}

func StartScheduler(ctx context.Context, wg *sync.WaitGroup, writeQueues *WriteQueues, internalQueue internalQueue, logger logrus.FieldLogger) {
	scheduler := NewScheduler(ctx, writeQueues, internalQueue, logger)
	wg.Add(1)
	enterrors.GoWrapper(func() {
		scheduler.Loop()
		wg.Done()
	}, logger)
}

func newBatchStartMessage(streamId string) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_Start{
			Start: &pb.BatchStart{StreamId: streamId},
		},
	}
}

func newBatchErrorMessage(err *pb.BatchError) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_Error{
			Error: err,
		},
	}
}

func newBatchStopMessage(streamId string) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_Stop{
			Stop: &pb.BatchStreamMessage_BatchStop{StreamId: streamId},
		},
	}
}

func newBatchShutdownMessage(streamId string) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_Shutdown{
			Shutdown: &pb.BatchShutdown{StreamId: streamId},
		},
	}
}

type readObject struct {
	Errors []*pb.BatchError
	Stop   bool
}

type writeObject struct {
	Object    *pb.BatchObject
	Reference *pb.BatchReference
	Stop      bool
}

type (
	internalQueue chan *ProcessRequest
	writeQueue    chan *writeObject
	readQueue     chan *readObject
	readQueues    map[string]readQueue
)

// NewBatchWriteQueue creates a buffered channel to store objects for batch writing.
//
// The buffer size can be adjusted based on expected load and performance requirements
// to optimize throughput and resource usage. But is required so that there is a small buffer
// that can be quickly flushed in the event of a shutdown.
func NewBatchInternalQueue() internalQueue {
	return make(internalQueue, 10)
}

func NewBatchWriteQueue() writeQueue {
	return make(writeQueue, 1000) // Adjust buffer size as needed
}

func NewBatchWriteQueues() *WriteQueues {
	return &WriteQueues{
		queues: sync.Map{},
	}
}

func NewBatchReadQueues() *ReadQueues {
	return &ReadQueues{
		queues: make(readQueues),
	}
}

func NewBatchReadQueue() readQueue {
	return make(readQueue)
}

func NewStopReadObject() *readObject {
	return &readObject{
		Errors: nil,
		Stop:   true,
	}
}

func NewStopWriteObject() *writeObject {
	return &writeObject{
		Object: nil,
		Stop:   true,
	}
}

func NewErrorsObject(errs []*pb.BatchError) *readObject {
	return &readObject{
		Errors: errs,
	}
}

func NewWriteObject(obj *pb.BatchObject) *writeObject {
	return &writeObject{
		Object: obj,
		Stop:   false,
	}
}

type ReadQueues struct {
	lock   sync.RWMutex
	queues readQueues
}

// Get retrieves the read queue for the given stream ID.
func (r *ReadQueues) Get(streamId string) (readQueue, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	if !ok {
		return nil, false
	}
	return queue, true
}

// Delete removes the read queue for the given stream ID.
func (r *ReadQueues) Delete(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	close(r.queues[streamId])
	delete(r.queues, streamId)
}

func (r *ReadQueues) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, queue := range r.queues {
		close(queue)
	}
}

// Make initializes a read queue for the given stream ID if it does not already exist.
func (r *ReadQueues) Make(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if _, ok := r.queues[streamId]; !ok {
		r.queues[streamId] = make(readQueue)
	}
}

type WriteQueue struct {
	queue            writeQueue
	consistencyLevel *pb.ConsistencyLevel
	objIndex         int32
	refIndex         int32
}

type WriteQueues struct {
	queues sync.Map
}

func (w *WriteQueues) Get(streamId string) (writeQueue, bool) {
	queue, ok := w.queues.Load(streamId)
	if !ok {
		return nil, false
	}
	return queue.(*WriteQueue).queue, true
}

func (w *WriteQueues) Delete(streamId string) {
	wq, ok := w.Get(streamId)
	if !ok {
		return
	}
	close(wq)
	w.queues.Delete(streamId)
}

func (w *WriteQueues) Close() {
	w.queues.Range(func(key, value any) bool {
		close(value.(*WriteQueue).queue)
		return true
	})
	w.queues = sync.Map{} // Clear the map after closing all queues
}

func (w *WriteQueues) Make(streamId string, consistencyLevel *pb.ConsistencyLevel) {
	if _, ok := w.queues.Load(streamId); !ok {
		w.queues.Store(streamId, &WriteQueue{
			queue:            NewBatchWriteQueue(),
			consistencyLevel: consistencyLevel,
		})
	}
}
