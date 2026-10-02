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

	grpcHandler "github.com/weaviate/weaviate/adapters/handlers/grpc"
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

func TestStopGrpcServer(t *testing.T) {
	const forcingStop = "grpc graceful stop timed out, forcing stop"

	cases := []struct {
		name          string
		callInFlight  bool
		respectCtx    bool
		cancelDelay   time.Duration
		stopTimeout   time.Duration
		maxDuration   time.Duration
		wantForceStop bool
		wantCutShort  int64
	}{
		{
			name:        "no calls in flight stops without waiting for the cancel",
			cancelDelay: time.Hour,
			stopTimeout: time.Hour,
			maxDuration: 5 * time.Second,
		},
		{
			name:         "call that respects ctx is cancelled and drained before the backstop",
			callInFlight: true,
			respectCtx:   true,
			cancelDelay:  50 * time.Millisecond,
			stopTimeout:  2 * time.Second,
			maxDuration:  time.Second,
			wantCutShort: 1,
		},
		{
			name:          "call that ignores ctx is disconnected at the backstop",
			callInFlight:  true,
			cancelDelay:   50 * time.Millisecond,
			stopTimeout:   300 * time.Millisecond,
			maxDuration:   5 * time.Second,
			wantForceStop: true,
			wantCutShort:  1,
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

			inFlight := grpcHandler.NewInFlightCancel()
			server := grpc.NewServer(grpc.ChainUnaryInterceptor(inFlight.UnaryInterceptor()))
			grpc_health_v1.RegisterHealthServer(server, health)
			lis := bufconn.Listen(1024 * 1024)
			go func() { _ = server.Serve(lis) }()

			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })

			callErr := make(chan error, 1)
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
			stopTook := make(chan time.Duration, 1)
			go func() {
				start := time.Now()
				stopGrpcServer(server, inFlight, tc.cancelDelay, tc.stopTimeout, logger)
				stopTook <- time.Since(start)
			}()

			if tc.callInFlight {
				select {
				case err := <-callErr:
					assert.Equal(t, codes.Unavailable, status.Code(err))
				case <-time.After(5 * time.Second):
					require.FailNow(t, "client call still blocked after the server stopped")
				}
			}
			// Stop may or may not wait for a handler that ignores its ctx.
			releaseHandler()

			select {
			case took := <-stopTook:
				assert.Less(t, took, tc.maxDuration)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "stop did not return")
			}

			var forcedStop bool
			for _, e := range hook.AllEntries() {
				forcedStop = forcedStop || (e.Level == logrus.WarnLevel && e.Message == forcingStop)
			}
			assert.Equal(t, tc.wantForceStop, forcedStop, "forced Stop")
			assert.Equal(t, tc.wantCutShort, inFlight.CutShort())
		})
	}
}
