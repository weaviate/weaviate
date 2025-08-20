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

	"github.com/sirupsen/logrus"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type QueuesHandler struct {
	grpcShutdownHandlersCtx context.Context
	grpcShutdownWorkersCtx  context.Context
	logger                  logrus.FieldLogger
	writeQueues             *WriteQueues
	readQueues              *ReadQueues
}

func NewQueuesHandler(grpcShutdownHandlersCtx, grpcShutdownWorkersCtx context.Context, writeQueues *WriteQueues, readQueues *ReadQueues, logger logrus.FieldLogger) *QueuesHandler {
	return &QueuesHandler{
		grpcShutdownHandlersCtx: grpcShutdownHandlersCtx,
		grpcShutdownWorkersCtx:  grpcShutdownWorkersCtx,
		logger:                  logger,
		writeQueues:             writeQueues,
		readQueues:              readQueues,
	}
}

func (h *QueuesHandler) Stream(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer) error {
	if err := stream.Send(newBatchStartMessage(streamId)); err != nil {
		return err
	}
	// workersDone acts as a soft cancel here so we can send the shutting down message to the client
	// once the workers are drained then the handler will be cancelled and we will exit for real
	workersDone := h.grpcShutdownWorkersCtx.Done()
	for {
		if readQueue, ok := h.readQueues.Get(streamId); ok {
			select {
			case <-ctx.Done():
				if innerErr := stream.Send(newBatchStopMessage(streamId)); innerErr != nil {
					return innerErr
				}
				return ctx.Err()
			case <-workersDone:
				if innerErr := stream.Send(newBatchShuttingDownMessage(streamId)); innerErr != nil {
					return innerErr
				}
				workersDone = nil
			case <-h.grpcShutdownHandlersCtx.Done():
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
func (h *QueuesHandler) Send(ctx context.Context, request *pb.BatchSendRequest) (int, error) {
	if h.grpcShutdownWorkersCtx.Err() != nil {
		return 0, fmt.Errorf("grpc shutdown in progress, no more requests are permitted on this node")
	}
	streamId := request.GetStreamId()
	queue, ok := h.writeQueues.GetQueue(streamId)
	if !ok {
		h.logger.WithField("streamId", streamId).Error("write queue not found")
		return 0, fmt.Errorf("write queue for stream %s not found", streamId)
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
	return h.writeQueues.NextBatchSize(streamId, len(request.GetObjects().GetValues())+len(request.GetReferences().GetValues())), nil
}

// Setup initializes a read queue for the given stream ID and adds it to the read queues map.
func (h *QueuesHandler) Setup(streamId string, req *pb.BatchStreamRequest) {
	h.readQueues.Make(streamId)
	h.writeQueues.Make(streamId, req.ConsistencyLevel)
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
	h.logger.WithField("streamId", streamId).Info("teardown completed")
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

func newBatchShuttingDownMessage(streamId string) *pb.BatchStreamMessage {
	return &pb.BatchStreamMessage{
		Message: &pb.BatchStreamMessage_ShuttingDown{
			ShuttingDown: &pb.BatchShuttingDown{StreamId: streamId},
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

func NewBatchWriteQueue(buffer int) writeQueue {
	return make(writeQueue, buffer) // Adjust buffer size as needed
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

type WriteQueue struct {
	queue            writeQueue
	consistencyLevel *pb.ConsistencyLevel

	// Indexes for the next object and reference to be sent
	objIndex int32
	refIndex int32

	emaQueueLen      float32 // Exponential moving average of the queue length
	buffer           int     // Buffer size for the write queue
	alpha            float32 // Smoothing factor for EMA, typically between 0 and 1
	initialBatchSize int     // The size of the first batch sent before scaling occurred
}

func (w *WriteQueue) NextBatchSize(batch int) int {
	if w.initialBatchSize == 0 {
		w.initialBatchSize = batch // Store the initial batch size
	}

	nowLen := len(w.queue)
	if w.emaQueueLen == 0 {
		w.emaQueueLen = float32(nowLen)
	} else {
		w.emaQueueLen = w.alpha*float32(nowLen) + (1-w.alpha)*w.emaQueueLen
	}
	usageRatio := w.emaQueueLen / float32(w.buffer)
	if usageRatio < 0.5 {
		return w.initialBatchSize // If usage is low, return the original batch size
	}

	// quadratic scaling based on usage ratio
	scaledSize := int(float32(batch) * usageRatio * usageRatio)
	if scaledSize < 1 {
		scaledSize = 1 // Ensure at least one object is requested
	}

	return scaledSize
}

type WriteQueues struct {
	queues sync.Map // map[string]*WriteQueue
}

func (w *WriteQueues) NextBatchSize(streamId string, batch int) int {
	wq, ok := w.Get(streamId)
	if !ok {
		return 0
	}
	return wq.NextBatchSize(batch)
}

func (w *WriteQueues) Get(streamId string) (*WriteQueue, bool) {
	value, ok := w.queues.Load(streamId)
	if !ok {
		return nil, false
	}
	wq, ok := value.(*WriteQueue)
	if !ok {
		return nil, false
	}
	return wq, true
}

func (w *WriteQueues) GetQueue(streamId string) (writeQueue, bool) {
	queue, ok := w.queues.Load(streamId)
	if !ok {
		return nil, false
	}
	return queue.(*WriteQueue).queue, true
}

func (w *WriteQueues) Delete(streamId string) {
	wq, ok := w.GetQueue(streamId)
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
	buffer := 10000 // Default buffer size
	if _, ok := w.queues.Load(streamId); !ok {
		w.queues.Store(streamId, &WriteQueue{
			queue:            NewBatchWriteQueue(buffer),
			consistencyLevel: consistencyLevel,
			buffer:           buffer,
			alpha:            0.2, // Smoothing factor for EMA of queue length
		})
	}
}
