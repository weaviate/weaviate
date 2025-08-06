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
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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
func (h *QueuesHandler) Setup(streamId string, req *pb.BatchStreamRequest) {
	h.readQueues.Make(streamId)
	if req.Type == nil {
		h.writeQueues.MakeDynamic(streamId, req.ConsistencyLevel)
	}
	if req.GetDynamic() != nil {
		h.writeQueues.MakeDynamic(streamId, req.ConsistencyLevel)
	}
	if req.GetFixedSize() != nil {
		h.writeQueues.MakeFixedSize(streamId, req.ConsistencyLevel, int(req.GetFixedSize().GetSize()))
	}
	if req.GetRateLimited() != nil {
		h.writeQueues.MakeRateLimited(streamId, req.ConsistencyLevel, int(req.GetRateLimited().GetRate()))
	}
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
		queues: sync.Map{},
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
	queues sync.Map // map[string]readQueue
}

// Get retrieves the read queue for the given stream ID.
func (r *ReadQueues) Get(streamId string) (readQueue, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues.Load(streamId)
	if !ok {
		return nil, false
	}
	return queue.(readQueue), true
}

// Delete removes the read queue for the given stream ID.
func (r *ReadQueues) Delete(streamId string) {
	if queue, ok := r.queues.Load(streamId); ok {
		close(queue.(readQueue))
		r.queues.Delete(streamId)
	}
}

func (r *ReadQueues) Close() {
	r.queues.Range(func(key, value any) bool {
		close(value.(readQueue))
		return true
	})
}

// Make initializes a read queue for the given stream ID if it does not already exist.
func (r *ReadQueues) Make(streamId string) {
	if _, ok := r.queues.Load(streamId); !ok {
		r.queues.Store(streamId, make(readQueue))
	}
}

type dynamic struct{}

type fixedSize struct {
	size int
}

type rateLimited struct {
	desiredRate       int
	howManyToSendNext int
	whenToSendNext    time.Time
}

type WriteQueue struct {
	queue            writeQueue
	consistencyLevel *pb.ConsistencyLevel
	objIndex         int32
	refIndex         int32
	dynamic          *dynamic
	fixedSize        *fixedSize
	rateLimited      *rateLimited
}

func (w *WriteQueue) BatchSize() (int, error) {
	if w.dynamic != nil {
		return min(len(w.queue), 1000), nil
	}
	if w.fixedSize != nil {
		return int(w.fixedSize.size), nil
	}
	if w.rateLimited != nil {
		now := time.Now()
		// If we are past the next scheduled time, we can send the desired rate
		if now.After(w.rateLimited.whenToSendNext) {
			toSend := w.rateLimited.desiredRate

			w.rateLimited.howManyToSendNext = w.rateLimited.desiredRate
			w.rateLimited.whenToSendNext = now.Add(time.Second)

			return toSend, nil
		}
		// Otherwise, skip. TODO: can we optimise better here to send some partial list of objects?
		return 0, nil
	}
	return 0, errors.New("unknown batch size")
}

type WriteQueues struct {
	queues sync.Map // map[string]*WriteQueue
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

type makeWriteQueueOptions struct {
	consistencyLevel *pb.ConsistencyLevel
	dynamic          *dynamic
	fixedSize        *fixedSize
	rateLimited      *rateLimited
}

func NewMakeWriteQueueOptions(consistencyLevel *pb.ConsistencyLevel, dynamic *dynamic, fixedSize *fixedSize, rateLimited *rateLimited) *makeWriteQueueOptions {
	return &makeWriteQueueOptions{
		consistencyLevel: consistencyLevel,
		dynamic:          dynamic,
		fixedSize:        fixedSize,
		rateLimited:      rateLimited,
	}
}

func (w *WriteQueues) MakeDynamic(streamId string, consistencyLevel *pb.ConsistencyLevel) {
	if _, ok := w.queues.Load(streamId); !ok {
		w.queues.Store(streamId, &WriteQueue{
			queue:            NewBatchWriteQueue(),
			consistencyLevel: consistencyLevel,
			dynamic:          &dynamic{},
		})
	}
}

func (w *WriteQueues) MakeFixedSize(streamId string, consistencyLevel *pb.ConsistencyLevel, size int) {
	if _, ok := w.queues.Load(streamId); !ok {
		w.queues.Store(streamId, &WriteQueue{
			queue:            NewBatchWriteQueue(),
			consistencyLevel: consistencyLevel,
			fixedSize:        &fixedSize{size: size},
		})
	}
}

func (w *WriteQueues) MakeRateLimited(streamId string, consistencyLevel *pb.ConsistencyLevel, rate int) {
	if _, ok := w.queues.Load(streamId); !ok {
		w.queues.Store(streamId, &WriteQueue{
			queue:            NewBatchWriteQueue(),
			consistencyLevel: consistencyLevel,
			rateLimited:      &rateLimited{desiredRate: rate, howManyToSendNext: rate, whenToSendNext: time.Now().Add(time.Second)},
		})
	}
}
