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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrShutdown = status.Error(codes.Aborted, "server is shutting down")

type StreamHandler struct {
	shuttingDownCtx      context.Context
	logger               logrus.FieldLogger
	reportingQueues      *ReportingQueues
	processingQueue      processingQueue
	recvWg               *sync.WaitGroup
	sendWg               *sync.WaitGroup
	metrics              *BatchStreamingCallbacks
	shuttingDown         atomic.Bool
	workerStatsPerStream *sync.Map // map[string]*stats
	processWgsPerStream  *sync.Map // map[string]*sync.WaitGroup
	stopped              *sync.Map // map[string]struct{}
}

const POLLING_INTERVAL = 100 * time.Millisecond

func NewStreamHandler(shuttingDownCtx context.Context, recvWg, sendWg *sync.WaitGroup, reportingQueues *ReportingQueues, processingQueue processingQueue, metrics *BatchStreamingCallbacks, logger logrus.FieldLogger) *StreamHandler {
	return &StreamHandler{
		shuttingDownCtx:      shuttingDownCtx,
		logger:               logger,
		reportingQueues:      reportingQueues,
		processingQueue:      processingQueue,
		recvWg:               recvWg,
		sendWg:               sendWg,
		metrics:              metrics,
		workerStatsPerStream: &sync.Map{},
		processWgsPerStream:  &sync.Map{},
		stopped:              &sync.Map{},
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

	h.setup(streamId)
	defer h.teardown(streamId)

	// Ensure that internal goroutines are cancelled when the stream exits for any reason
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// g, ctx := enterrors.NewErrorGroupWithContextWrapper(h.logger, ctx)
	errCh := make(chan error, 2)
	h.recvWg.Add(1)
	enterrors.GoWrapper(func() { errCh <- h.recv(ctx, streamId, startReq.ConsistencyLevel, stream) }, h.logger)
	h.sendWg.Add(1)
	enterrors.GoWrapper(func() { errCh <- h.send(ctx, streamId, stream) }, h.logger)

	return <-errCh
}

func (h *StreamHandler) close(streamId string) {
	if _, loaded := h.stopped.LoadOrStore(streamId, struct{}{}); loaded {
		// already closed
		return
	}
	// spawn a goroutine to wait until all workers are done before closing the reporting queue
	h.sendWg.Add(1)
	enterrors.GoWrapper(func() {
		defer h.sendWg.Done()
		// Wait until all workers are done before closing the reporting queue
		h.processWg(streamId).Wait()
		h.reportingQueues.Close(streamId)
		h.logger.WithField("streamId", streamId).Debug("all workers done, closed reporting queue")
		h.stopped.Delete(streamId)
	}, h.logger)
}

func (h *StreamHandler) send(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer) error {
	defer h.sendWg.Done()

	// shuttingDown acts as a soft cancel here so we can send the shutting down message to the client.
	// Once the workers are drained then h.shutdownFinished will be closed and we will shutdown completely
	shuttingDownDone := h.shuttingDownCtx.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		if reportingQueue, ok := h.reportingQueues.Get(streamId); ok {
			select {
			case <-ctx.Done():
				h.logger.WithField("streamId", streamId).Debug("context cancelled, closing stream")
				h.close(streamId)
				// drain reporting queue in effort to communicate any inflight errors back to client
				// despite the context being cancelled somewhere
				for report := range reportingQueue {
					h.processWg(streamId).Done()
					for _, err := range report.Errors {
						if h.metrics != nil {
							h.metrics.OnStreamError(streamId)
						}
						if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
							h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send error message")
						}
					}
				}
				// Context cancelled, send error to client
				return ctx.Err()
			case <-shuttingDownDone:
				h.logger.WithField("streamId", streamId).Debug("server is shutting down, will stop accepting new requests soon")
				// If shutting down context has been set by shutdown.Drain then send the shutdown triggered message to the client
				// so that it can backoff accordingly
				if innerErr := stream.Send(newBatchShuttingDownMessage()); innerErr != nil {
					h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send shutdown triggered message")
					return innerErr
				}
				shuttingDownDone = nil // only send once
				h.shuttingDown.Store(true)
				h.close(streamId)
			case report, ok := <-reportingQueue:
				h.logger.WithField("streamId", streamId).Debug("received report from worker")
				// If the reporting queue is closed, we must finish the stream
				if !ok {
					if h.shuttingDown.Load() {
						// the server must be shutting down on its own, so return an error saying so
						h.logger.WithField("streamId", streamId).Info("stream closed due to server shutdown")
						return ErrShutdown
					}
					// otherwise, the client must be closing its side of the stream, so close gracefully
					h.logger.WithField("streamId", streamId).Info("stream closed by client")
					return nil
				}
				// Received a report from a worker
				h.processWg(streamId).Done()
				for _, err := range report.Errors {
					if h.metrics != nil {
						h.metrics.OnStreamError(streamId)
					}
					if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
						h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send error message")
						return innerErr
					}
				}
				stats := h.workerStats(streamId)
				stats.updateBatchSize(report.Stats.processingTime, len(h.processingQueue))
				if h.metrics != nil {
					h.metrics.OnSchedulerReport(streamId, stats.getBatchSize(), stats.getProcessingTimeEma())
				}
				if innerErr := stream.Send(&pb.BatchStreamReply{
					Message: &pb.BatchStreamReply_Backoff_{
						Backoff: &pb.BatchStreamReply_Backoff{
							NextBatchSize: int32(stats.getBatchSize()),
							// BackoffSeconds: float32(writeQueue.BackoffSeconds()),
						},
					},
				}); innerErr != nil {
					return innerErr
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
func (h *StreamHandler) recv(ctx context.Context, streamId string, consistencyLevel *pb.ConsistencyLevel, stream pb.Weaviate_BatchStreamServer) error {
	defer h.recvWg.Done()
	defer h.close(streamId)
	log := h.logger.WithField("streamId", streamId)

	// reqCh := make(chan *pb.BatchStreamRequest)
	// errCh := make(chan error)
	// Receive from stream in child goroutine so we can also listen for shutdown signals in loop below
	// once the stream is empty
	// enterrors.GoWrapper(func() {
	// 	defer func() {
	// 		close(errCh)
	// 		close(reqCh)
	// 	}()
	// 	for {
	// 		req, err := stream.Recv()
	// 		if err != nil {
	// 			errCh <- err
	// 			return
	// 		}
	// 		reqCh <- req
	// 	}
	// }, h.logger)

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
		// var request *pb.BatchStreamRequest
		// var err error
		// select {
		// case request = <-reqCh:
		// case err = <-errCh:
		// case <-time.After(1 * time.Minute):
		// 	log.Debug("waiting for client request")
		// 	if h.shuttingDown.Load() {
		// 		log.Debug("shutting down due to shutdown signal")
		// 		return nil
		// 	}
		// 	continue
		// }
		request, err := stream.Recv()
		// Recevied from stream.Recv() so log that a worker is processing a request
		// We add to the wg here and mark as done when the worker has reported back to the stream handler
		// that it has finished processing this request
		// This ensures that if the client closes the stream we wait until all workers are done before closing the reporting queue
		// and thus allowing the send() method to finish gracefully
		// If the server is shutting down we stop accepting new requests but we must still wait until all workers are done
		// before closing the reporting queue
		h.processWg(streamId).Add(1)

		if errors.Is(err, io.EOF) {
			log.Debug("client closed stream")
			h.processWg(streamId).Done() // remove the one we added above since we're not processing a request
			return nil
		}
		if err != nil {
			log.WithError(err).Error("failed to receive batch stream request")
			// Tell the scheduler to stop processing this stream because of a client hangup error
			h.processWg(streamId).Done() // remove the one we added above since we're not processing a request
			return err
		}
		// wg for this process is marked as done in send() when the worker reports back
		if request.GetData() != nil {
			h.processingQueue <- &processRequest{
				StreamId:         streamId,
				ConsistencyLevel: consistencyLevel,
				Objects:          request.GetData().GetObjects().GetValues(),
				References:       request.GetData().GetReferences().GetValues(),
			}
		} else {
			h.processWg(streamId).Done() // remove the one we added above since we're not processing a request
			return fmt.Errorf("invalid batch send request: data field is nil")
		}
	}
}

func (h *StreamHandler) workerStats(streamId string) *stats {
	st, _ := h.workerStatsPerStream.LoadOrStore(streamId, newStats())
	return st.(*stats)
}

func (h *StreamHandler) processWg(streamId string) *sync.WaitGroup {
	wg, _ := h.processWgsPerStream.LoadOrStore(streamId, &sync.WaitGroup{})
	return wg.(*sync.WaitGroup)
}

// Setup initializes a reporting queue for the given stream ID and adds it to the reporting queues map.
func (h *StreamHandler) setup(streamId string) {
	h.reportingQueues.Make(streamId)
	h.logger.WithField("action", "stream_start").WithField("streamId", streamId).Debug("queues created")
}

// Teardown closes the reporting queue for the given stream ID and removes it from the reporting queues map.
func (h *StreamHandler) teardown(streamId string) {
	h.reportingQueues.Delete(streamId)
	h.logger.WithField("streamId", streamId).Debug("teardown completed")
}
