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
	reportingQueues      *reportingQueues
	processingQueue      processingQueue
	recvWg               *sync.WaitGroup
	sendWg               *sync.WaitGroup
	metrics              *BatchStreamingCallbacks
	shuttingDown         atomic.Bool
	workerStatsPerStream *sync.Map // map[string]*stats
}

const POLLING_INTERVAL = 100 * time.Millisecond

func NewStreamHandler(shuttingDownCtx context.Context, recvWg, sendWg *sync.WaitGroup, reportingQueues *reportingQueues, processingQueue processingQueue, metrics *BatchStreamingCallbacks, logger logrus.FieldLogger) *StreamHandler {
	h := &StreamHandler{
		shuttingDownCtx:      shuttingDownCtx,
		logger:               logger,
		reportingQueues:      reportingQueues,
		processingQueue:      processingQueue,
		recvWg:               recvWg,
		sendWg:               sendWg,
		metrics:              metrics,
		workerStatsPerStream: &sync.Map{},
	}
	return h
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

	// Channel to communicate receive errors from recv to the send loop
	errCh := make(chan error, 1)
	h.recvWg.Add(1)
	enterrors.GoWrapper(func() {
		defer h.recvWg.Done()
		errCh <- h.recv(ctx, streamId, startReq.ConsistencyLevel, stream)
		close(errCh)
	}, h.logger)
	h.sendWg.Add(1)
	defer h.sendWg.Done()
	return h.send(ctx, streamId, stream, errCh)
}

func (h *StreamHandler) send(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer, errCh chan error) error {
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
				// drain reporting queue in effort to communicate any inflight errors back to client
				// despite the context being cancelled somewhere
				for report := range reportingQueue {
					for _, err := range report.Errors {
						if h.metrics != nil {
							h.metrics.OnStreamError(streamId)
						}
						if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
							h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send error message")
						}
					}
				}
				// Context cancelled, return error ending the stream
				return ctx.Err()
			case recvErr := <-errCh:
				if recvErr != nil {
					h.logger.WithField("streamId", streamId).WithError(recvErr).Error("receive error, closing stream")
					// drain reporting queue in effort to communicate any inflight errors back to client
					// despite the recv side of the stream failing in some way
					for report := range reportingQueue {
						for _, err := range report.Errors {
							if h.metrics != nil {
								h.metrics.OnStreamError(streamId)
							}
							if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
								h.logger.WithField("streamId", streamId).WithError(innerErr).Error("failed to send error message")
							}
						}
					}
					if h.shuttingDown.Load() {
						// the server must be shutting down on its own, so return an error saying so
						h.logger.WithField("streamId", streamId).Infof("while server is shutting down, receiver errored: %v", recvErr)
						return ErrShutdown
					}
					// Receiver errored in some way, send error to client
					return recvErr
				}
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
					return <-errCh // will be nil if the client closed the stream gracefully or a recv error otherwise
				}
				// Received a report from a worker
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
							NextBatchSize:  int32(stats.getBatchSize()),
							BackoffSeconds: h.thresholdCubicBackoff(),
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

func (h *StreamHandler) close(streamId string, wg *sync.WaitGroup) {
	// Wait until all workers are done before closing the reporting queue
	wg.Wait()
	h.logger.WithField("streamId", streamId).Debug("all workers done, closed reporting queue")
	h.reportingQueues.Close(streamId)
	h.workerStatsPerStream.Delete(streamId)
}

// Send adds a batch send request to the write queue and returns the number of objects in the request.
func (h *StreamHandler) recv(ctx context.Context, streamId string, consistencyLevel *pb.ConsistencyLevel, stream pb.Weaviate_BatchStreamServer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start a goroutine to cancel the recv context after a grace period once shutdown is triggered
	// This ensures that we don't hang indefinitely if the client does not close the stream
	// after receiving the shutdown message for any reason
	enterrors.GoWrapper(func() {
		<-h.shuttingDownCtx.Done()
		<-time.After(2 * time.Minute)
		h.logger.WithField("streamId", streamId).Debug("grace period after shutdown elapsed, cancelling recv context")
		cancel()
	}, h.logger)

	reqCh := make(chan *pb.BatchStreamRequest)
	errCh := make(chan error)
	enterrors.GoWrapper(func() {
		defer func() {
			close(errCh)
			close(reqCh)
		}()
		for {
			// stream context is cancelled once the send() method returns
			// cleaning up this goroutine
			req, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			reqCh <- req
		}
	}, h.logger)

	wg := &sync.WaitGroup{}
	defer h.close(streamId, wg)
	log := h.logger.WithField("streamId", streamId)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var request *pb.BatchStreamRequest
		var err error
		select {
		case request = <-reqCh:
		case err = <-errCh:
		case <-ctx.Done():
			log.Warn("context cancelled waiting for request from stream, closing recv stream")
			return ctx.Err()
		}
		if errors.Is(err, io.EOF) {
			log.Debug("client closed stream")
			return nil
		}
		if err != nil {
			log.WithError(err).Error("failed to receive batch stream request")
			// Tell the scheduler to stop processing this stream because of a client hangup error
			return err
		}
		if request.GetData() != nil {
			wg.Add(1)
			h.processingQueue <- &processRequest{
				StreamId:         streamId,
				ConsistencyLevel: consistencyLevel,
				Objects:          request.GetData().GetObjects().GetValues(),
				References:       request.GetData().GetReferences().GetValues(),
				Wg:               wg,
			}
		} else {
			return fmt.Errorf("invalid batch send request: data field is nil")
		}
	}
}

func (h *StreamHandler) workerStats(streamId string) *stats {
	st, _ := h.workerStatsPerStream.LoadOrStore(streamId, newStats())
	return st.(*stats)
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
