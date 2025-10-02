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
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type StreamHandler struct {
	shuttingDownCtx context.Context
	logger          logrus.FieldLogger
	writeQueues     *WriteQueues
	readQueues      *ReadQueues
	recvWg          *sync.WaitGroup
	sendWg          *sync.WaitGroup
	stopping        *sync.Map // map[string]bool
	metrics         *BatchStreamingCallbacks
	shuttingDown    atomic.Bool
}

const POLLING_INTERVAL = 100 * time.Millisecond

func NewStreamHandler(shuttingDownCtx context.Context, recvWg, sendWg *sync.WaitGroup, writeQueues *WriteQueues, readQueues *ReadQueues, metrics *BatchStreamingCallbacks, logger logrus.FieldLogger) *StreamHandler {
	return &StreamHandler{
		shuttingDownCtx: shuttingDownCtx,
		logger:          logger,
		writeQueues:     writeQueues,
		readQueues:      readQueues,
		recvWg:          recvWg,
		sendWg:          sendWg,
		metrics:         metrics,
		stopping:        &sync.Map{},
	}
}

func (h *StreamHandler) Handle(stream pb.Weaviate_BatchStreamServer) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("stream ID generation failed: %w", err)
	}
	streamId := id.String()

	message, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("initial stream message receive: %w", err)
	}

	startReq := message.GetStart()
	if startReq == nil {
		return fmt.Errorf("first message must be a start message")
	}

	h.setup(streamId, startReq)
	defer h.teardown(streamId)

	// Ensure that internal goroutines are cancelled when the stream exits for any reason
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	g, ctx := enterrors.NewErrorGroupWithContextWrapper(h.logger, ctx)
	h.recvWg.Add(1)
	g.Go(func() error { return h.recv(ctx, streamId, stream) })
	h.sendWg.Add(1)
	g.Go(func() error { return h.send(ctx, streamId, stream) })

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (h *StreamHandler) send(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer) error {
	defer h.sendWg.Done()
	// shuttingDown acts as a soft cancel here so we can send the shutting down message to the client.
	// Once the workers are drained then h.shutdownFinished will be closed and we will shutdown completely
	shuttingDownDone := h.shuttingDownCtx.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if readQueue, ok := h.readQueues.Get(streamId); ok {
			select {
			case <-ctx.Done():
				// Context cancelled, send stop message to client
				if innerErr := stream.Send(newBatchStopMessage()); innerErr != nil {
					h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send stop message")
					return innerErr
				}
				return ctx.Err()
			case <-shuttingDownDone:
				// If shutting down context has been set by shutdown.Drain then send the shutdown triggered message to the client
				// so that it can backoff accordingly
				if innerErr := stream.Send(newBatchShutdownTriggeredMessage()); innerErr != nil {
					h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send shutdown triggered message")
					return innerErr
				}
				shuttingDownDone = nil // only send once
				h.shuttingDown.Store(true)
			case readObj, ok := <-readQueue:
				// readQueue is closed by the scheduler when all the workers are done processing for this stream
				if !ok {
					if stopping, ok := h.stopping.Load(streamId); ok && stopping.(bool) {
						// If this stream is being stopped by the client sending the stop sentinel then send the finishing stop message to the client
						if innerErr := stream.Send(newBatchStopMessage()); innerErr != nil {
							h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send stop message")
							return innerErr
						}
					} else {
						// Otherwise, the server must be shutting down on its own, so send the shutdown finished message
						if innerErr := stream.Send(newBatchShutdownFinishedMessage()); innerErr != nil {
							h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send shutdown finished message")
							return innerErr
						}
					}
					return nil
				}
				for _, err := range readObj.Errors {
					if h.metrics != nil {
						h.metrics.OnStreamError(streamId)
					}
					if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
						h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send error message")
						return innerErr
					}
				}
			case <-ticker.C:
				// Don't send backoff ticks if we're stopping or shutting down
				if stopping, ok := h.stopping.Load(streamId); ok && stopping.(bool) {
					continue
				}
				if h.shuttingDown.Load() {
					continue
				}
				if writeQueue, ok := h.writeQueues.Get(streamId); ok {
					if innerErr := stream.Send(&pb.BatchStreamReply{
						Message: &pb.BatchStreamReply_Backoff_{
							Backoff: &pb.BatchStreamReply_Backoff{
								NextBatchSize:  int32(writeQueue.NextBatchSize()),
								BackoffSeconds: float32(writeQueue.BackoffSeconds()),
							},
						},
					}); innerErr != nil {
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
func (h *StreamHandler) recv(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer) error {
	log := h.logger.WithField("streamId", streamId)
	closed := false
	done := func() {
		if !closed {
			h.writeQueues.Close(streamId)
			h.recvWg.Done()
		}
		closed = true
	}
	defer done()

	reqCh := make(chan *pb.BatchStreamRequest)
	errCh := make(chan error)
	// Receive from stream in child goroutine so we can also listen for shutdown signals in loop below
	// once the stream is empty
	enterrors.GoWrapper(func() {
		defer func() {
			close(errCh)
			close(reqCh)
		}()
		for {
			req, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			reqCh <- req
		}
	}, h.logger)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Wait for either a request, an error, or a shutdown signal from the looping stream.Recv goroutine
		// if there's a shutdown signal whilst there are no requests in the stream
		// then we close the write queue and block waiting until the client hangs-up
		// in the meantime, the send method will communicate to the client that the server is shutting down
		// and the client will deal with that appropriately. If they send more requests after that point
		// they will be lost since we will be blocking until we get io.EOF from the client
		var request *pb.BatchStreamRequest
		var err error
		select {
		case request = <-reqCh:
		case err = <-errCh:
		case <-time.After(1 * time.Minute):
			log.Debug("waiting for client request")
			if h.shuttingDown.Load() {
				log.Debug("shutting down, closing write queue for this stream")
				return nil
			}
			continue
		}

		if errors.Is(err, io.EOF) {
			// Client has closed the stream, we close the write queue and return
			log.Debug("client closed stream, closing write queue for this stream")
			return nil
		}
		if err != nil {
			log.WithError(err).Error("failed to receive batch stream request")
			// Tell the scheduler to stop processing this stream because of a client hangup error
			return err
		}
		objs := make([]*writeObject, 0, len(request.GetObjects().GetValues())+len(request.GetReferences().GetValues())+1)
		if request.GetObjects() != nil {
			for _, obj := range request.GetObjects().GetValues() {
				objs = append(objs, &writeObject{Object: obj})
			}
		} else if request.GetReferences() != nil {
			for _, ref := range request.GetReferences().GetValues() {
				objs = append(objs, &writeObject{Reference: ref})
			}
		} else if request.GetStop() != nil {
			h.stopping.Store(streamId, true)
		} else {
			return fmt.Errorf("invalid batch send request: neither objects, references nor stop signal provided")
		}
		if !h.writeQueues.Exists(streamId) {
			log.Error("write queue not found")
			return fmt.Errorf("write queue for stream %s not found", streamId)
		}
		if len(objs) > 0 {
			log.WithField("numObjects", len(objs)).Debug("writing batch objects")
			h.writeQueues.Write(streamId, objs)
			log.WithField("numObjects", len(objs)).Debug("batch objects written to queue")
		}
		if stopping, ok := h.stopping.Load(streamId); ok && stopping.(bool) {
			// Will loop back and block on stream.Recv() until io.EOF is returned
			done() // Close queue and mark wg as done, done is idempotent
			continue
		}
		h.writeQueues.UpdateBatchSize(streamId, len(request.GetObjects().GetValues())+len(request.GetReferences().GetValues()))
		wq, ok := h.writeQueues.Get(streamId)
		if ok && h.metrics != nil {
			h.metrics.OnStreamRequest(streamId, wq.emaQueueLen/float64(wq.buffer))
		}
	}
}

// Setup initializes a read and write queues for the given stream ID and adds it to the read queues map.
func (h *StreamHandler) setup(streamId string, req *pb.BatchStreamRequest_Start) {
	h.readQueues.Make(streamId)
	buffer := h.writeQueues.Make(streamId, req.ConsistencyLevel)
	h.logger.WithField("action", "stream_start").WithField("streamId", streamId).WithField("buffer", buffer).Debug("queues created")
}

// Teardown closes the read queue for the given stream ID and removes it from the read queues map.
func (h *StreamHandler) teardown(streamId string) {
	h.readQueues.Delete(streamId)
	h.writeQueues.Delete(streamId)
	h.logger.WithField("streamId", streamId).Debug("teardown completed")
}
