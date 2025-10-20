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

const SHUTDOWN_GRACE_PERIOD = 75 * time.Second

var ErrShutdown = errors.New("server has shutdown")

func errShutdown(err error) error {
	return status.Error(codes.Aborted, err.Error())
}

type authenticator interface {
	PrincipalFromContext(ctx context.Context) (*models.Principal, error)
}

type StreamHandler struct {
	authenticator          authenticator
	authorizer             authorization.Authorizer
	shuttingDownCtx        context.Context
	logger                 logrus.FieldLogger
	reportingQueues        *reportingQueues
	processingQueue        processingQueue
	recvWg                 *sync.WaitGroup
	sendWg                 *sync.WaitGroup
	enqueuedObjectsCounter *atomic.Int32
	metrics                *BatchStreamingMetrics
	shuttingDown           atomic.Bool
	workerStatsPerStream   *sync.Map // map[string]*stats
	stoppingPerStream      *sync.Map // map[string]struct{}
}

func NewStreamHandler(authenticator authenticator, authorizer authorization.Authorizer, shuttingDownCtx context.Context, recvWg, sendWg *sync.WaitGroup, reportingQueues *reportingQueues, processingQueue processingQueue, enqueuedObjectsCounter *atomic.Int32, metrics *BatchStreamingMetrics, logger logrus.FieldLogger) *StreamHandler {
	h := &StreamHandler{
		authenticator:          authenticator,
		authorizer:             authorizer,
		shuttingDownCtx:        shuttingDownCtx,
		logger:                 logger,
		reportingQueues:        reportingQueues,
		processingQueue:        processingQueue,
		recvWg:                 recvWg,
		sendWg:                 sendWg,
		enqueuedObjectsCounter: enqueuedObjectsCounter,
		metrics:                metrics,
		workerStatsPerStream:   &sync.Map{},
		stoppingPerStream:      &sync.Map{},
	}
	return h
}

// Handle is the main entrypoint for all Bidi StreamStream calls.
//
// It handles authentication, stream setup and teardown, and spawns the receiver goroutine
// before entering the sender loop itself.
//
// At a high-level, the stream handler works as follows:
//
//  1. Authenticate the client's API key
//  2. Check if the server is shutting down, if so, reject the stream
//  3. Setup the stream (create reporting queue, etc)
//  4. Spawn the receiver goroutine which receives messages from the stream and schedules them for processing by downstream workers in the processing queue
//  5. Enter the sender loop which sends messages back through the stream based on reports from downstream workers through the stream-specific reporting queue
//  6. Teardown the stream (delete reporting queue, etc)
//
// The receiver and sender loops communicate through channels to handle errors and stream closure gracefully.
func (h *StreamHandler) Handle(stream pb.Weaviate_BatchStreamServer) error {
	streamCtx := stream.Context()
	// Authenticate at the highest level
	_, err := h.authenticator.PrincipalFromContext(streamCtx)
	if err != nil {
		return fmt.Errorf("authenticate: %w", err)
	}

	// If the server is shutting down, we reject new streams
	// This prevents new streams from being added to the system while we are shutting down
	// Existing streams will be allowed to complete their work
	// See drain() in drain.go for the full shutdown sequence
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
	ctx, cancel := context.WithCancel(streamCtx)
	defer cancel()

	// Channel to communicate receive errors from recv to the send loop
	recvErrCh := make(chan error, 1)
	// Spawn recv process in its own goroutine
	h.recvWg.Add(1)
	enterrors.GoWrapper(func() {
		defer h.recvWg.Done()
		// If recv returns, then the stream has been closed by the client or an error has occurred
		// In either case, we need to inform the send loop so that it can exit cleanly
		// We do this by sending the error (or nil if the client closed the stream) to the recvErrCh channel
		// and then closing the channel to signal that no more errors will be sent
		if err := h.receiver(ctx, streamId, startReq.ConsistencyLevel, stream); err != nil {
			recvErrCh <- err
		}
		close(recvErrCh)
	}, h.logger)
	h.sendWg.Add(1)
	defer h.sendWg.Done()
	// Start the send loop in this goroutine, it will exit when the stream is closed or an error occurs (including shutdowns)
	return h.sender(ctx, streamId, stream, recvErrCh)
}

func (h *StreamHandler) drainReportingQueue(queue reportingQueue, batchResults *batchResults, stream pb.Weaviate_BatchStreamServer, logger *logrus.Entry) {
	for report := range queue {
		h.handleWorkerResults(report, batchResults, stream, logger)
	}
	if err := batchResults.send(stream); err != nil {
		logger.Errorf("failed to send final results message while draining reporting queue: %s", err)
	}
}

func (h *StreamHandler) handleRecvErr(recvErr error, logger *logrus.Entry) error {
	if h.shuttingDown.Load() {
		// the server must be shutting down on its own, so return an error saying so that wraps the receiver error
		logger.Errorf("while server is shutting down, receiver errored: %v", recvErr)
		return errShutdown(recvErr)
	} else {
		logger.Errorf("receive error, closing stream: %s", recvErr)
		return recvErr
	}
}

func (h *StreamHandler) handleRecvClosed(streamId string, logger *logrus.Entry) error {
	if h.shuttingDown.Load() && !h.isStopping(streamId) {
		// The server must be shutting down on its own, so return an error saying so provided that the client
		// hasn't indicated that it has stopped the stream itself. This avoids telling a client that has already stopped
		// of a shutting down server; it shouldn't care
		logger.Info("stream closed due to server shutdown")
		return errShutdown(ErrShutdown)
	}
	// otherwise, the client must be closing its side of the stream, so close gracefully
	// client has closed its side of the stream, so close gracefully
	logger.Info("stream closed by client gracefully")
	return nil
}

func (h *StreamHandler) handleServerShuttingDown(stream pb.Weaviate_BatchStreamServer, logger *logrus.Entry) error {
	logger.Debug("server is shutting down, will stop accepting new requests soon")
	// If shutting down context has been set by shutdown.Drain then send the shutdown triggered message to the client
	// so that it can backoff accordingly
	if innerErr := stream.Send(newBatchShuttingDownMessage()); innerErr != nil {
		logger.Errorf("failed to send shutdown triggered message: %s", innerErr)
		return innerErr
	}
	h.shuttingDown.Store(true)
	return nil
}

func (h *StreamHandler) handleWorkerReport(report *report, closed bool, batchResults *batchResults, streamId string, stream pb.Weaviate_BatchStreamServer, logger *logrus.Entry) error {
	logger.Debug("received report from worker")
	// If the reporting queue is closed, then h.recv must've closed it itself either through erroring or the client closing its side of the stream
	if closed {
		if h.shuttingDown.Load() && !h.isStopping(streamId) {
			// The server must be shutting down on its own, so return an error saying so provided that the client
			// hasn't indicated that it has stopped the stream itself. This avoids telling a client that has already stopped
			// of a shutting down server; it shouldn't care
			logger.Info("stream closed due to server shutdown")
			return errShutdown(ErrShutdown)
		}
		return nil
	}
	// Received a report from a worker
	h.handleWorkerResults(report, batchResults, stream, logger)
	// Recalculate stats
	stats := h.workerStats(streamId)
	stats.updateBatchSize(report.Stats.processingTime)
	if h.metrics != nil {
		h.metrics.OnWorkerReport(stats.getThroughputEma())
	}
	return nil
}

func (h *StreamHandler) handleWorkerResults(report *report, batchResults *batchResults, stream pb.Weaviate_BatchStreamServer, logger *logrus.Entry) {
	if h.metrics != nil {
		h.metrics.OnStreamError(len(report.Errors))
	}
	batchResults.add(report.Successes, report.Errors)
	if batchResults.shouldSend() {
		if innerErr := batchResults.send(stream); innerErr != nil {
			logger.Errorf("failed to send results message: %s", innerErr)
		}
	}
}

func (h *StreamHandler) sender(ctx context.Context, streamId string, stream pb.Weaviate_BatchStreamServer, recvErrCh chan error) error {
	defer h.stoppingPerStream.Delete(streamId)
	log := h.logger.WithField("streamId", streamId)
	// shuttingDown acts as a soft cancel here so we can send the shutting down message to the client.
	// Once the workers are drained then h.shutdownFinished will be closed and we will shutdown completely
	shuttingDownDone := h.shuttingDownCtx.Done()
	if err := stream.Send(newBatchStartedMessage()); err != nil {
		log.Errorf("failed to send started message: %s", err)
		return err
	}
	batchResults := newBatchResults()
	for {
		reportingQueue, exists := h.reportingQueues.Get(streamId)
		if !exists {
			// This should never happen, but if it does, we log it
			log.Error("reporting queue not found")
			return fmt.Errorf("reporting queue for stream %s not found", streamId)
		}
		select {
		case <-ctx.Done():
			// drain reporting queue in effort to communicate any inflight errors back to client
			// despite the context being cancelled somewhere
			log.Debug("context is done, draining reporting queue before closing stream")
			h.drainReportingQueue(reportingQueue, batchResults, stream, log)
			log.Error("context cancelled, closing stream")
			return ctx.Err()
		case recvErr, open := <-recvErrCh:
			// drain reporting queue in effort to communicate any inflight errors back to client
			// despite the receiver throwing an error of some kind, or the client closing its side of the stream
			log.Debug("receiver has closed, draining reporting queue before closing stream")
			h.drainReportingQueue(reportingQueue, batchResults, stream, log)
			if !open {
				log.Debug("recvErrCh closed, closing stream")
				// channel closed, client must have closed its side of the stream
				return h.handleRecvClosed(streamId, log)
			}
			// receiver errored, return the error to close the stream
			return h.handleRecvErr(recvErr, log)
		case <-shuttingDownDone:
			if err := h.handleServerShuttingDown(stream, log); err == nil {
				// only send server shutting down msg once, provided that it didn't error
				shuttingDownDone = nil
			}
		case report, open := <-reportingQueue:
			if err := h.handleWorkerReport(report, !open, batchResults, streamId, stream, log); err != nil {
				return err
			}
			if !open {
				if err := batchResults.send(stream); err != nil {
					log.Errorf("failed to send final results message after reporting queue closed: %s", err)
				}
				log.Debug("reportingQueue is closed, closing stream")
				return h.handleRecvClosed(streamId, log)
			}
		}
	}
}

func (h *StreamHandler) close(streamId string, wg *sync.WaitGroup) {
	// Wait until all workers are done before closing the reporting queue
	wg.Wait()
	h.logger.WithField("streamId", streamId).Debug("all workers done, closed reporting queue")
	h.reportingQueues.close(streamId)
	h.workerStatsPerStream.Delete(streamId)
}

func (h *StreamHandler) recv(stream pb.Weaviate_BatchStreamServer) (chan *pb.BatchStreamRequest, chan error) {
	reqCh := make(chan *pb.BatchStreamRequest)
	errCh := make(chan error)
	enterrors.GoWrapper(func() {
		defer func() {
			close(errCh)
			close(reqCh)
		}()
		for {
			// stream context is cancelled once the send() method returns
			// cleaning up this goroutine without needing any additional signalling
			// i.e. when the stream is closed by the client or an error occurs
			// including server shutdowns
			req, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			reqCh <- req
		}
	}, h.logger)
	return reqCh, errCh
}

func (h *StreamHandler) receiver(ctx context.Context, streamId string, consistencyLevel *pb.ConsistencyLevel, stream pb.Weaviate_BatchStreamServer) error {
	log := h.logger.WithField("streamId", streamId)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg := &sync.WaitGroup{}
	defer h.close(streamId, wg)

	shuttingDownDone := h.shuttingDownCtx.Done()
	var gracePeriod <-chan time.Time

	reqCh, errCh := h.recv(stream)
	for {
		// we must check for shutting down before we start blocking on h.recv in the event
		// that the client is misbehaving by sending more messages after the shutdown signal
		if h.shuttingDownCtx.Err() != nil {
			shuttingDownDone = nil // only do this once
			if gracePeriod == nil {
				// if we haven't already started the grace period timer then do so now
				gracePeriod = time.After(SHUTDOWN_GRACE_PERIOD)
				log.Info("server is shutting down, will force close recv stream after grace period")
			}
			select {
			case <-gracePeriod:
				// if we're still looping after the grace period has expired then force close
				log.Warn("grace period expired, closing recv stream")
				cancel()
				return ctx.Err()
			default:
				// otherwise continue as normal
			}
		}

		var request *pb.BatchStreamRequest
		var err error
		// non-blocking select to receive messages from the stream
		// this allows us to detect hanging clients during server shutdown
		// we either receive a request, an error, or a shutdown signal
		// if we receive a shutdown signal, we set up a grace period timer
		// after which we will force close the stream if it hasn't closed already
		// if we receive a request or an error, we process it as normal
		// if the context is cancelled, we exit the loop
		select {
		case request = <-reqCh:
		case err = <-errCh:
		case <-ctx.Done():
			return ctx.Err()
		case <-shuttingDownDone:
			// if the client is misbehaving by keeping the stream open without sending any messages
			// after the shutdown signal then we need to start the grace period timer
			// so that we can force close the stream after the grace period has expired
			shuttingDownDone = nil // only do this once
			gracePeriod = time.After(SHUTDOWN_GRACE_PERIOD)
			log.Info("server is shutting down, will force close recv stream after grace period")
			continue
		case <-gracePeriod:
			// if we block waiting for stream.Recv() until the grace period expires then force close
			log.Warn("grace period expired, closing recv stream")
			cancel()
			return ctx.Err()
		}

		if errors.Is(err, io.EOF) {
			log.Debug("client closed stream")
			return nil
		}
		if err != nil {
			log.Errorf("failed to receive batch stream request: %s", err)
			// Tell the sender to stop processing this stream because of a client hangup error
			return err
		}
		if request.GetData() != nil {
			push := func(objs []*pb.BatchObject, refs []*pb.BatchReference) {
				wg.Add(1)
				howMany := len(objs) + len(refs)
				if h.metrics != nil {
					h.metrics.OnProcessingQueuePush(howMany)
				}
				h.processingQueue <- &processRequest{
					streamId:         streamId,
					consistencyLevel: consistencyLevel,
					objects:          objs,
					references:       refs,
					wg:               wg,               // the worker will call wg.Done() when it is finished
					streamCtx:        stream.Context(), // passes any authN information from the stream into the worker for authZ
				}
				h.enqueuedObjectsCounter.Add(int32(howMany))
			}

			// Split the client-sent objects into batches according to the current batch size
			// This allows clients to send however many objects they want without overwhelming
			// the downstream workers
			batchSize := h.workerStats(streamId).getBatchSize()
			objs := request.GetData().GetObjects().GetValues()
			var batch []*pb.BatchObject
			if len(objs) > batchSize {
				batch = make([]*pb.BatchObject, 0, batchSize)
				for _, obj := range request.GetData().GetObjects().GetValues() {
					batch = append(batch, obj)
					if len(batch) == batchSize {
						push(batch, nil)
						batch = make([]*pb.BatchObject, 0, batchSize)
					}
				}
			} else {
				batch = objs
			}

			refs := request.GetData().GetReferences().GetValues()
			if len(batch) > 0 || len(refs) > 0 {
				// refs are fast so don't need to be efficiently batched
				// we just accept however many the client sends assuming it'll be fine
				push(batch, request.GetData().GetReferences().GetValues())
			}

		} else if request.GetStop() != nil {
			h.setStopping(streamId)
		} else {
			h.logger.WithField("streamId", streamId).WithField("request", request).Error("received invalid batch send request: data field is nil")
			return fmt.Errorf("invalid batch send request: data field is nil")
		}
	}
}

type batchResults struct {
	successes []*pb.BatchStreamReply_Results_Success
	errors    []*pb.BatchStreamReply_Results_Error
}

func newBatchResults() *batchResults {
	return &batchResults{
		successes: make([]*pb.BatchStreamReply_Results_Success, 0, 10000),
		errors:    make([]*pb.BatchStreamReply_Results_Error, 0),
	}
}

func (r *batchResults) add(successes []*pb.BatchStreamReply_Results_Success, errors []*pb.BatchStreamReply_Results_Error) {
	r.successes = append(r.successes, successes...)
	r.errors = append(r.errors, errors...)
}

func (r *batchResults) reset() {
	r.successes = r.successes[:0]
	r.errors = r.errors[:0]
}

func (r *batchResults) shouldSend() bool {
	return len(r.successes)+len(r.errors) > 10000
}

func (r *batchResults) send(stream pb.Weaviate_BatchStreamServer) error {
	defer r.reset()
	if err := stream.Send(newBatchResultsMessage(r.successes, r.errors)); err != nil {
		return err
	}
	return nil
}

func (h *StreamHandler) workerStats(streamId string) *stats {
	st, _ := h.workerStatsPerStream.LoadOrStore(streamId, newStats())
	return st.(*stats)
}

func (h *StreamHandler) isStopping(streamId string) bool {
	_, ok := h.stoppingPerStream.Load(streamId)
	return ok
}

func (h *StreamHandler) setStopping(streamId string) {
	h.stoppingPerStream.Store(streamId, struct{}{})
}

// Setup initializes a reporting queue for the given stream ID and adds it to the reporting queues map.
func (h *StreamHandler) setup(streamId string) {
	h.reportingQueues.Make(streamId)
	h.logger.WithField("action", "stream_start").WithField("streamId", streamId).Debug("queues created")
}

// Teardown closes the reporting queue for the given stream ID and removes it from the reporting queues map.
func (h *StreamHandler) teardown(streamId string) {
	h.reportingQueues.delete(streamId)
	h.logger.WithField("streamId", streamId).Debug("teardown completed")
}
