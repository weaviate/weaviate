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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
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

func TestStartGrpcStopTimeoutCountsFromStart(t *testing.T) {
	const forcingStop = "grpc graceful stop timed out, forcing stop"

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

	logger, hook := test.NewNullLogger()
	wait := startGrpcStop(server, func() {}, 300*time.Millisecond, logger)
	select {
	case err := <-callErr:
		assert.Equal(t, codes.Unavailable, status.Code(err))
	case <-time.After(5 * time.Second):
		require.FailNow(t, "client call still blocked after the server stopped")
	}
	waitTook := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		wait()
		waitTook <- time.Since(start)
	}()
	// Stop may or may not wait for a handler that ignores its ctx.
	releaseHandler()

	select {
	case took := <-waitTook:
		assert.Less(t, took, 500*time.Millisecond)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "wait did not return")
	}

	var forcedStop bool
	for _, e := range hook.AllEntries() {
		forcedStop = forcedStop || (e.Level == logrus.WarnLevel && e.Message == forcingStop)
	}
	assert.True(t, forcedStop, "forced Stop")
}

// A drain that never finishes must not hold up the gRPC stop past the timeout.
func TestWaitBatchDrain(t *testing.T) {
	const (
		timeout     = 200 * time.Millisecond
		waitLimit   = 5 * time.Second
		drainNotice = "batch stream drain did not finish within 200ms, continuing shutdown"
	)

	cases := []struct {
		name       string
		drainHangs bool
	}{
		{name: "drain that finishes returns at once"},
		{name: "drain that hangs is left behind at the timeout", drainHangs: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			drain := func() {
				if tc.drainHangs {
					<-release
				}
			}

			logger, hook := test.NewNullLogger()
			waited := make(chan time.Duration, 1)
			go func() {
				start := time.Now()
				waitBatchDrain(drain, timeout, logger)
				waited <- time.Since(start)
			}()

			select {
			case took := <-waited:
				if tc.drainHangs {
					assert.GreaterOrEqual(t, took, timeout)
				} else {
					assert.Less(t, took, timeout)
				}
			case <-time.After(waitLimit):
				require.FailNow(t, "waitBatchDrain did not return")
			}

			var noticed bool
			for _, e := range hook.AllEntries() {
				noticed = noticed || (e.Level == logrus.WarnLevel && e.Message == drainNotice)
			}
			assert.Equal(t, tc.drainHangs, noticed, "drain timeout warning")
		})
	}
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
