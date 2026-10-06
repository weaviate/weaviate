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

package rest

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	grpcHandler "github.com/weaviate/weaviate/adapters/handlers/grpc"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	batchmocks "github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// blockingHealthServer holds every Check until release is closed, or until
// its ctx is done when it respects ctx.
type blockingHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	respectCtx bool
	entered    chan struct{}
	release    chan struct{}
}

func (s *blockingHealthServer) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	s.entered <- struct{}{}
	done := ctx.Done()
	if !s.respectCtx {
		done = nil
	}
	select {
	case <-done:
	case <-s.release:
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// serveBufconn serves server in memory and returns a client connected to it.
func serveBufconn(t *testing.T, server *grpc.Server) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	go func() { _ = server.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestStartGrpcStop(t *testing.T) {
	const forcingStop = "grpc graceful stop timed out, forcing stop"

	cases := []struct {
		name            string
		callInFlight    bool
		respectCtx      bool
		cancelCalls     bool
		stopTimeout     time.Duration
		stopBeforeWait  bool
		maxWait         time.Duration
		wantForceStop   bool
		wantUnavailable int64
	}{
		{
			name:        "no calls in flight stops without waiting for the timeout",
			stopTimeout: time.Hour,
			maxWait:     5 * time.Second,
		},
		{
			name:            "call that respects ctx is cancelled and drained before the timeout",
			callInFlight:    true,
			respectCtx:      true,
			cancelCalls:     true,
			stopTimeout:     2 * time.Second,
			maxWait:         time.Second,
			wantUnavailable: 1,
		},
		{
			name:            "call that ignores ctx is disconnected at the timeout",
			callInFlight:    true,
			cancelCalls:     true,
			stopTimeout:     300 * time.Millisecond,
			maxWait:         5 * time.Second,
			wantForceStop:   true,
			wantUnavailable: 1,
		},
		{
			name:           "timeout counts from the start, not from the wait",
			callInFlight:   true,
			stopTimeout:    300 * time.Millisecond,
			stopBeforeWait: true,
			maxWait:        500 * time.Millisecond,
			wantForceStop:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			health := &blockingHealthServer{
				respectCtx: tc.respectCtx,
				entered:    make(chan struct{}, 1),
				release:    make(chan struct{}),
			}
			releaseHandler := sync.OnceFunc(func() { close(health.release) })
			t.Cleanup(releaseHandler)

			callsCtx, cancelCalls := context.WithCancel(context.Background())
			t.Cleanup(cancelCalls)
			inFlight := grpcHandler.NewInFlightCancel(callsCtx)
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(inFlight.UnavailableAfterCancel()))
			grpc_health_v1.RegisterHealthServer(server, health)
			conn := serveBufconn(t, server)

			callErr := make(chan error, 1)
			requireUnavailable := func() {
				select {
				case err := <-callErr:
					assert.Equal(t, codes.Unavailable, status.Code(err))
				case <-time.After(5 * time.Second):
					require.FailNow(t, "client call still blocked after the server stopped")
				}
			}
			if tc.callInFlight {
				go func() {
					_, err := grpc_health_v1.NewHealthClient(conn).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
					callErr <- err
				}()
				select {
				case <-health.entered:
				case <-time.After(5 * time.Second):
					require.FailNow(t, "call never reached the handler")
				}
			}

			logger, hook := test.NewNullLogger()
			wait := startGrpcStop(server, func() {}, tc.stopTimeout, logger)
			if tc.cancelCalls {
				cancelCalls()
			}
			if tc.stopBeforeWait {
				requireUnavailable()
			}
			waitTook := make(chan time.Duration, 1)
			go func() {
				start := time.Now()
				wait()
				waitTook <- time.Since(start)
			}()

			if tc.callInFlight && !tc.stopBeforeWait {
				requireUnavailable()
			}
			// Stop may or may not wait for a handler that ignores its ctx.
			releaseHandler()

			select {
			case took := <-waitTook:
				assert.Less(t, took, tc.maxWait)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "wait did not return")
			}

			var forcedStop bool
			for _, e := range hook.AllEntries() {
				forcedStop = forcedStop || (e.Level == logrus.WarnLevel && e.Message == forcingStop)
			}
			assert.Equal(t, tc.wantForceStop, forcedStop, "forced Stop")
			assert.Equal(t, tc.wantUnavailable, inFlight.UnavailableResponses())
		})
	}
}

// A drain that finishes is waited for in full, with no timeout warning.
func TestWaitBatchDrainReturnsWithDrain(t *testing.T) {
	const timeout = time.Hour
	logger, hook := test.NewNullLogger()
	waited := make(chan struct{})
	go func() {
		waitBatchDrain(func() {}, timeout, logger)
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "waitBatchDrain did not return once drain did")
	}
	assert.False(t, loggedDrainTimeout(hook))
}

func loggedDrainTimeout(hook *test.Hook) bool {
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && e.Data["action"] == "shutdown_drain" {
			return true
		}
	}
	return false
}

// A client that stops reading blocks the sender's last Send, so a real drain
// never returns on its own and only the timeout lets shutdown go on.
func TestWaitBatchDrainLeavesSenderBlockedOnClient(t *testing.T) {
	const (
		timeout   = 500 * time.Millisecond
		waitLimit = 5 * time.Second
	)
	await := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(waitLimit):
			require.FailNowf(t, "timed out", "waiting for %s", what)
		}
	}
	collection := "TestClass"

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clientCallsCtx, cancelClientCalls := context.WithCancel(context.Background())
	t.Cleanup(cancelClientCalls)
	release := make(chan struct{})
	releaseSend := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseSend)

	batchEntered, finishBatch := make(chan struct{}), make(chan struct{})
	batcher := batchmocks.NewMockBatcher(t)
	batcher.EXPECT().BatchObjects(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *pb.BatchObjectsRequest) (*pb.BatchObjectsReply, error) {
			close(batchEntered)
			<-finishBatch
			return &pb.BatchObjectsReply{}, nil
		}).Once()
	schemaManager := batchmocks.NewMockschemaManager(t)
	schemaManager.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
	schemaManager.EXPECT().GetCachedClassNoAuth(mock.Anything, collection).
		Return(map[string]versioned.Class{collection: {Class: &models.Class{Class: collection}}}, nil).Once()
	authenticator := batchmocks.NewMockauthenticator(t)
	authenticator.EXPECT().PrincipalFromContext(ctx).Return(&models.Principal{}, nil).Once()

	// Every Send after the shutdown message blocks, as for a client that stopped reading.
	acksSent, shutdownSent, sendBlocked := make(chan struct{}), make(chan struct{}), make(chan struct{})
	signalSendBlocked := sync.OnceFunc(func() { close(sendBlocked) })
	var shuttingDown atomic.Bool
	stream := batchmocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamRequest, pb.BatchStreamReply](t)
	stream.EXPECT().Context().Return(ctx).Maybe()
	stream.EXPECT().Send(mock.Anything).RunAndReturn(func(msg *pb.BatchStreamReply) error {
		switch {
		case msg.GetAcks() != nil:
			close(acksSent)
		case msg.GetShuttingDown() != nil:
			shuttingDown.Store(true)
			close(shutdownSent)
		case shuttingDown.Load():
			signalSendBlocked()
			<-release
		}
		return nil
	}).Maybe()
	recvCount := 0
	stream.EXPECT().Recv().RunAndReturn(func() (*pb.BatchStreamRequest, error) {
		recvCount++
		switch recvCount {
		case 1:
			return &pb.BatchStreamRequest{Message: &pb.BatchStreamRequest_Start_{Start: &pb.BatchStreamRequest_Start{}}}, nil
		case 2:
			return &pb.BatchStreamRequest{Message: &pb.BatchStreamRequest_Data_{Data: &pb.BatchStreamRequest_Data{
				Objects: &pb.BatchStreamRequest_Data_Objects{Values: []*pb.BatchObject{{Collection: collection}}},
			}}}, nil
		default:
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}).Maybe()

	logger, hook := test.NewNullLogger()
	handler, drain := batch.Start(authenticator, nil, batcher, schemaManager, nil, 1, logger, namespacing.Disabled,
		batch.WithClientCallsCtx(clientCallsCtx))
	go func() { _ = handler.Handle(stream) }()
	await(batchEntered, "the batch to reach the batcher")
	await(acksSent, "the batch to be acknowledged")

	drained := make(chan struct{})
	waited := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		waitBatchDrain(func() {
			drain()
			close(drained)
		}, timeout, logger)
		waited <- time.Since(start)
	}()
	await(shutdownSent, "the shutdown message")
	close(finishBatch)
	await(sendBlocked, "the results Send to block")
	cancelClientCalls()

	select {
	case took := <-waited:
		assert.GreaterOrEqual(t, took, timeout)
	case <-time.After(waitLimit):
		require.FailNow(t, "waitBatchDrain did not return at its timeout")
	}
	assert.True(t, loggedDrainTimeout(hook))
	select {
	case <-drained:
		require.FailNow(t, "drain returned while the sender was blocked in Send")
	default:
	}

	releaseSend()
	// Recv never ends the stream, so drain finishes only if client calls cut the receiver.
	await(drained, "drain to finish once Send returned")
}

// wait must not return while the graceful stop it joins is still draining a call.
func TestStartGrpcStopWaitJoinsDrain(t *testing.T) {
	health := &blockingHealthServer{entered: make(chan struct{}, 1), release: make(chan struct{})}
	releaseHandler := sync.OnceFunc(func() { close(health.release) })
	t.Cleanup(releaseHandler)
	server := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(server, health)
	conn := serveBufconn(t, server)

	callErr := make(chan error, 1)
	go func() {
		_, err := grpc_health_v1.NewHealthClient(conn).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		callErr <- err
	}()
	select {
	case <-health.entered:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "call never reached the handler")
	}

	logger, _ := test.NewNullLogger()
	wait := startGrpcStop(server, func() {}, time.Hour, logger)
	waited := make(chan struct{})
	go func() {
		wait()
		close(waited)
	}()
	// Correct code never returns while the call runs, so this window cannot flake.
	select {
	case <-waited:
		t.Fatal("wait returned while the graceful stop was still draining a call")
	case <-time.After(100 * time.Millisecond):
	}

	releaseHandler()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "wait did not return once the call finished")
	}
	require.NoError(t, <-callErr)
}
