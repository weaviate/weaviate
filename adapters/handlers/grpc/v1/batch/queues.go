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
	shuttingDownCtx  context.Context
	logger           logrus.FieldLogger
	writeQueues      *WriteQueues
	readQueues       *ReadQueues
	sendWg           *sync.WaitGroup
	streamWg         *sync.WaitGroup
	shutdownFinished chan struct{}
}

func NewQueuesHandler(shuttingDownCtx context.Context, sendWg, streamWg *sync.WaitGroup, shutdownFinished chan struct{}, writeQueues *WriteQueues, readQueues *ReadQueues, logger logrus.FieldLogger) *QueuesHandler {
	// Poll until the batch logic starts shutting down
	// Then wait for all BatchSend requests to finish and close all the write queues
	// Scheduler will then drain the write queues expecting the channels to be closed
	enterrors.GoWrapper(func() {
		for {
			select {
			case <-shuttingDownCtx.Done():
				logger.Info("shutting down batch queues handler, waiting for in-flight requests to finish")
				sendWg.Wait()
				logger.Info("all in-flight requests finished, closing write queues")
				writeQueues.Close()
				logger.Info("write queues closed, exiting handlers shutdown listener")
				return
			default:
				time.Sleep(100 * time.Millisecond) // Polling interval
			}
		}
	}, logger)
	return &QueuesHandler{
		shuttingDownCtx:  shuttingDownCtx,
		logger:           logger,
		writeQueues:      writeQueues,
		readQueues:       readQueues,
		sendWg:           sendWg,
		streamWg:         streamWg,
		shutdownFinished: shutdownFinished,
	}
}

func (h *QueuesHandler) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (h *QueuesHandler) Stream(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer) error {
	h.streamWg.Add(1)
	defer h.streamWg.Done()
	if err := stream.Send(newBatchStartMessage(streamId)); err != nil {
		return err
	}
	// shuttingDown acts as a soft cancel here so we can send the shutting down message to the client.
	// Once the workers are drained then h.shutdownFinished will be closed and we will shutdown completely
	shuttingDown := h.shuttingDownCtx.Done()
	for {
		if readQueue, ok := h.readQueues.Get(streamId); ok {
			select {
			case <-ctx.Done():
				if innerErr := stream.Send(newBatchStopMessage(streamId)); innerErr != nil {
					return innerErr
				}
				return ctx.Err()
			case <-shuttingDown:
				if innerErr := stream.Send(newBatchShuttingDownMessage(streamId)); innerErr != nil {
					return innerErr
				}
				shuttingDown = nil
			case <-h.shutdownFinished:
				if innerErr := stream.Send(newBatchShutdownMessage(streamId)); innerErr != nil {
					return innerErr
				}
				return h.wait(ctx)
			case readObj, ok := <-readQueue:
				if !ok {
					if innerErr := stream.Send(newBatchStopMessage(streamId)); innerErr != nil {
						return innerErr
					}
					return h.wait(ctx)
				}
				for _, err := range readObj.Errors {
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
	h.sendWg.Add(1)
	defer h.sendWg.Done()
	if h.shuttingDownCtx.Err() != nil {
		return 0, fmt.Errorf("grpc shutdown in progress, no more requests are permitted on this node")
	}
	streamId := request.GetStreamId()
	queue, ok := h.writeQueues.GetQueue(streamId)
	if !ok {
		h.logger.WithField("streamId", streamId).Error("write queue not found")
		return 0, fmt.Errorf("write queue for stream %s not found", streamId)
	}
	if request.GetObjects() != nil {
		for _, obj := range request.GetObjects().GetValues() {
			queue <- &writeObject{Object: obj}
		}
	} else if request.GetReferences() != nil {
		for _, ref := range request.GetReferences().GetValues() {
			queue <- &writeObject{Reference: ref}
		}
	} else if request.GetStop() != nil {
		queue <- &writeObject{Stop: true}
	} else {
		return 0, fmt.Errorf("invalid batch send request: neither objects, references nor stop signal provided")
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
		queues: make(map[string]*WriteQueue),
	}
}

func NewBatchReadQueues() *ReadQueues {
	return &ReadQueues{
		queues: make(map[string]readQueue),
	}
}

func NewBatchReadQueue() readQueue {
	return make(readQueue)
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
	queues map[string]readQueue
}

// Get retrieves the read queue for the given stream ID.
func (r *ReadQueues) Get(streamId string) (readQueue, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	queue, ok := r.queues[streamId]
	return queue, ok
}

// Delete removes the read queue for the given stream ID.
func (r *ReadQueues) Delete(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.queues, streamId)
}

func (r *ReadQueues) Close(streamId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if queue, ok := r.queues[streamId]; ok {
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

	// Indexes for the next object and reference to be sent
	objIndex int32
	refIndex int32

	lock             sync.RWMutex // Used when concurrently re-calculating NextBatchSize in Send method
	emaQueueLen      float32      // Exponential moving average of the queue length
	buffer           int          // Buffer size for the write queue
	alpha            float32      // Smoothing factor for EMA, typically between 0 and 1
	initialBatchSize int          // The size of the first batch sent before scaling occurred
}

func (w *WriteQueue) NextBatchSize(batch int) int {
	w.lock.Lock()
	defer w.lock.Unlock()

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
	lock   sync.RWMutex
	queues map[string]*WriteQueue
	uuids  []string
}

func (w *WriteQueues) Uuids() []string {
	w.lock.RLock()
	defer w.lock.RUnlock()
	return w.uuids
}

func (w *WriteQueues) NextBatchSize(streamId string, batch int) int {
	wq, ok := w.Get(streamId)
	if !ok {
		return 0
	}
	return wq.NextBatchSize(batch)
}

func (w *WriteQueues) Get(streamId string) (*WriteQueue, bool) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	queue, ok := w.queues[streamId]
	if !ok {
		return nil, false
	}
	return queue, true
}

func (w *WriteQueues) GetQueue(streamId string) (writeQueue, bool) {
	w.lock.RLock()
	defer w.lock.RUnlock()
	queue, ok := w.queues[streamId]
	if !ok {
		return nil, false
	}
	return queue.queue, true
}

func (w *WriteQueues) Delete(streamId string) {
	w.lock.Lock()
	defer w.lock.Unlock()
	delete(w.queues, streamId)
	// Remove from uuids slice
	for i, uuid := range w.uuids {
		if uuid == streamId {
			w.uuids = append(w.uuids[:i], w.uuids[i+1:]...)
			break
		}
	}
}

func (w *WriteQueues) Close() {
	w.lock.Lock()
	defer w.lock.Unlock()
	for _, queue := range w.queues {
		close(queue.queue)
	}
}

func (w *WriteQueues) Make(streamId string, consistencyLevel *pb.ConsistencyLevel) {
	w.lock.Lock()
	defer w.lock.Unlock()
	buffer := 10000 // Default buffer size
	if _, ok := w.queues[streamId]; !ok {
		w.queues[streamId] = &WriteQueue{
			queue:            NewBatchWriteQueue(buffer),
			consistencyLevel: consistencyLevel,
			buffer:           buffer,
			alpha:            0.2, // Smoothing factor for EMA of queue length
		}
		w.uuids = append(w.uuids, streamId)
	}
}
