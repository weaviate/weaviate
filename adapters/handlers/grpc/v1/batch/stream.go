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
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrShutdown = errors.New("server has shutdown")

func errShutdown(err error) error {
	return status.Error(codes.Aborted, err.Error())
}

type authenticator interface {
	PrincipalFromContext(ctx context.Context) (*models.Principal, error)
}

type StreamHandler struct {
	authenticator        authenticator
	authorizer           authorization.Authorizer
	shuttingDownCtx      context.Context
	logger               logrus.FieldLogger
	reportingQueues      *reportingQueues
	processingQueue      processingQueue
	recvWg               *sync.WaitGroup
	sendWg               *sync.WaitGroup
	metrics              *BatchStreamingMetrics
	shuttingDown         atomic.Bool
	workerStatsPerStream *sync.Map // map[string]*stats
}

func NewStreamHandler(authenticator authenticator, authorizer authorization.Authorizer, shuttingDownCtx context.Context, recvWg, sendWg *sync.WaitGroup, reportingQueues *reportingQueues, processingQueue processingQueue, metrics *BatchStreamingMetrics, logger logrus.FieldLogger) *StreamHandler {
	h := &StreamHandler{
		authenticator:        authenticator,
		authorizer:           authorizer,
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
	ctx := stream.Context()
	_, err := h.authenticator.PrincipalFromContext(ctx)
	if err != nil {
		return fmt.Errorf("authenticate: %w", err)
	}

	if h.shuttingDownCtx.Err() != nil {
		return errShutdown(fmt.Errorf("not accepting new streams: %w", h.shuttingDownCtx.Err()))
	}

	if h.metrics != nil {
		h.metrics.OnStreamStart()
		defer h.metrics.OnStreamStop()
	}

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
	ctx, cancel := context.WithCancel(ctx)
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
	log := h.logger.WithField("streamId", streamId)
	// shuttingDown acts as a soft cancel here so we can send the shutting down message to the client.
	// Once the workers are drained then h.shutdownFinished will be closed and we will shutdown completely
	shuttingDownDone := h.shuttingDownCtx.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	if err := stream.Send(newBatchStartedMessage()); err != nil {
		log.WithError(err).Error("failed to send started message")
		return err
	}
	for {
		if reportingQueue, ok := h.reportingQueues.Get(streamId); ok {
			select {
			case <-ctx.Done():
				log.Debug("context cancelled, closing stream")
				// drain reporting queue in effort to communicate any inflight errors back to client
				// despite the context being cancelled somewhere
				for report := range reportingQueue {
					for _, err := range report.Errors {
						if h.metrics != nil {
							h.metrics.OnStreamError(streamId)
						}
						if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
							log.WithError(innerErr).Error("failed to send error message")
						}
					}
				}
				// Context cancelled, return error ending the stream
				return ctx.Err()
			case recvErr := <-errCh:
				if recvErr != nil {
					log.WithError(recvErr).Error("receive error, closing stream")
					// drain reporting queue in effort to communicate any inflight errors back to client
					// despite the recv side of the stream failing in some way
					for report := range reportingQueue {
						for _, err := range report.Errors {
							if h.metrics != nil {
								h.metrics.OnStreamError(streamId)
							}
							if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
								log.WithError(innerErr).Error("failed to send error message")
							}
						}
					}
					if h.shuttingDown.Load() {
						// the server must be shutting down on its own, so return an error saying so
						log.Errorf("while server is shutting down, receiver errored: %v", recvErr)
						return errShutdown(recvErr)
					}
					// Receiver errored in some way, send error to client
					return recvErr
				}
			case <-shuttingDownDone:
				log.Debug("server is shutting down, will stop accepting new requests soon")
				// If shutting down context has been set by shutdown.Drain then send the shutdown triggered message to the client
				// so that it can backoff accordingly
				if innerErr := stream.Send(newBatchShuttingDownMessage()); innerErr != nil {
					log.WithError(innerErr).Error("failed to send shutdown triggered message")
					return innerErr
				}
				shuttingDownDone = nil // only send once
				h.shuttingDown.Store(true)
			case report, ok := <-reportingQueue:
				log.Debug("received report from worker")
				// If the reporting queue is closed, we must finish the stream
				if !ok {
					if h.shuttingDown.Load() {
						// the server must be shutting down on its own, so return an error saying so
						log.Info("stream closed due to server shutdown")
						return errShutdown(ErrShutdown)
					}
					// otherwise, the client must be closing its side of the stream, so close gracefully
					log.Info("stream closed by client")
					return <-errCh // will be nil if the client closed the stream gracefully or a recv error otherwise
				}
				// Received a report from a worker
				for _, err := range report.Errors {
					if h.metrics != nil {
						h.metrics.OnStreamError(streamId)
					}
					if innerErr := stream.Send(newBatchErrorMessage(err)); innerErr != nil {
						log.WithError(innerErr).Error("failed to send error message")
						return innerErr
					}
				}
				stats := h.workerStats(streamId)
				stats.updateBatchSize(report.Stats.processingTime, len(h.processingQueue))
				if h.metrics != nil {
					h.metrics.OnWorkerReport(streamId, stats.getThroughputEma(), stats.getProcessingTimeEma())
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
			log.Error("read queue not found")
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

// recv receives messages from the client through the stream and schedules them for processing by downstream workers.
func (h *StreamHandler) recv(ctx context.Context, streamId string, consistencyLevel *pb.ConsistencyLevel, stream pb.Weaviate_BatchStreamServer) error {
	log := h.logger.WithField("streamId", streamId)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var request *pb.BatchStreamRequest
		var err error
		select {
		case request = <-reqCh:
		case err = <-errCh:
		case <-time.After(2 * time.Minute):
			// This ensures that we don't hang indefinitely if the client does not close the stream
			// after receiving the shutdown message for any reason
			if h.shuttingDown.Load() {
				log.Warn("context cancelled waiting for request from stream, closing recv stream")
				cancel()
				return ctx.Err()
			}
			// If not shutting down, just loop around again and wait for the next request
			continue
		}
		if errors.Is(err, io.EOF) {
			log.Debug("client closed stream")
			return nil
		}
		if err != nil {
			log.WithError(err).Error("failed to receive batch stream request")
			// Tell the sender to stop processing this stream because of a client hangup error
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
				Ctx:              stream.Context(), // passes any authn information from the stream into the worker for authz
			}
			if h.metrics != nil {
				h.metrics.OnStreamRequest(float64(len(h.processingQueue)) / float64(cap(h.processingQueue)))
			}
		} else {
			h.logger.WithField("streamId", streamId).WithField("request", request).Error("received invalid batch send request: data field is nil")
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
