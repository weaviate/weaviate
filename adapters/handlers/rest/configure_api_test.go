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
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/handlers/grpc/grpcweb"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
)

// holdingHealth answers health checks at once, except for service "hold",
// which it holds until unblock is called or the RPC is cancelled, and service
// "wedge", which it holds until unblock is called. GracefulStop never cancels
// an RPC; only Stop does.
type holdingHealth struct {
	healthpb.UnimplementedHealthServer
	entered  chan struct{}
	release  chan struct{}
	unblock  func()
	returned atomic.Bool
}

func (h *holdingHealth) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	switch req.Service {
	case "hold":
		defer h.returned.Store(true)
		close(h.entered)
		select {
		case <-h.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	case "wedge":
		close(h.entered)
		<-h.release
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// startHoldingGrpc serves holdingHealth on a real gRPC server and returns a
// check func that calls it over a real client connection.
func startHoldingGrpc(t *testing.T) (*grpc.Server, func(service string) error, *holdingHealth) {
	t.Helper()
	server, conn, svc := serveHoldingGrpc(t)
	client := healthpb.NewHealthClient(conn)
	check := func(service string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
		return err
	}
	return server, check, svc
}

// serveHoldingGrpc serves holdingHealth on a real gRPC server and returns a
// real client connection to it. Cleanup unblocks held RPCs before it closes
// the connection and stops the server.
func serveHoldingGrpc(t *testing.T) (*grpc.Server, *grpc.ClientConn, *holdingHealth) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	release := make(chan struct{})
	svc := &holdingHealth{
		entered: make(chan struct{}),
		release: release,
		unblock: sync.OnceFunc(func() { close(release) }),
	}
	healthpb.RegisterHealthServer(server, svc)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	t.Cleanup(svc.unblock)
	return server, conn, svc
}

func Test_shutdownHooks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	idleDrain := (&grpcweb.Tracker{}).Drain

	t.Run("new RPC rejected while HTTP drain runs", func(t *testing.T) {
		server, check, _ := startHoldingGrpc(t)
		require.NoError(t, check(""), "gRPC must serve before shutdown")

		prev := configureServer
		configureServer = func(*http.Server, string, string) {}
		t.Cleanup(func() { configureServer = prev })

		pre, shutdown := shutdownHooks(shutdownHooksConfig{
			Server: server, Timeout: 5 * time.Second, Logger: logger, Wait: idleDrain,
			PreShutdown: func() {}, Shutdown: func() {},
		})
		s := NewServer(&operations.WeaviateAPI{PreServerShutdown: pre, ServerShutdown: shutdown})
		s.EnabledListeners = []string{schemeHTTP}
		s.Host = "127.0.0.1"
		s.GracefulTimeout = 10 * time.Second

		entered := make(chan struct{})
		release := make(chan struct{})
		s.SetHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(entered)
			<-release
		}))
		require.NoError(t, s.Listen())

		served := make(chan error, 1)
		go func() { served <- s.ServeAndShutdown() }()
		restDone := make(chan struct{})
		go func() {
			defer close(restDone)
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", s.Port))
			if err == nil {
				resp.Body.Close()
			}
		}()
		<-entered

		require.NoError(t, s.Shutdown())
		require.Eventually(t, func() bool {
			return status.Code(check("")) == codes.Unavailable
		}, 5*time.Second, 10*time.Millisecond)
		select {
		case <-restDone:
			t.Fatal("held REST request ended before gRPC rejected new RPCs")
		default:
		}

		close(release)
		require.NoError(t, <-served)
	})

	t.Run("in-flight RPC completes before body", func(t *testing.T) {
		server, check, svc := startHoldingGrpc(t)
		rpcErr := make(chan error, 1)
		go func() { rpcErr <- check("hold") }()
		<-svc.entered

		// The held RPC is released only when shutdown starts. A body that runs
		// before shutdown waits for gRPC would see the RPC still in flight.
		wait := func(stop <-chan struct{}) bool {
			<-stop
			svc.unblock()
			return true
		}
		var returnedAtBody bool
		pre, shutdown := shutdownHooks(shutdownHooksConfig{
			Server: server, Timeout: 5 * time.Second, Logger: logger, Wait: wait,
			PreShutdown: func() {}, Shutdown: func() { returnedAtBody = svc.returned.Load() },
		})

		pre()
		shutdown()
		require.True(t, returnedAtBody, "body ran while the RPC was still in flight")
		require.NoError(t, <-rpcErr)
	})

	t.Run("failed web drain still stops gRPC gracefully", func(t *testing.T) {
		server, check, svc := startHoldingGrpc(t)
		rpcErr := make(chan error, 1)
		go func() { rpcErr <- check("hold") }()
		<-svc.entered

		// The wait reports grpc-web stragglers (returns false). The in-flight
		// native RPC must still get its graceful drain, not a hard Stop: a
		// hard Stop cancels the RPC within microseconds, far before the
		// 100ms release, so it fails the NoError assertion below.
		wait := func(stop <-chan struct{}) bool {
			<-stop
			time.AfterFunc(100*time.Millisecond, svc.unblock)
			return false
		}
		var returnedAtBody bool
		pre, shutdown := shutdownHooks(shutdownHooksConfig{
			Server: server, Timeout: 5 * time.Second, Logger: logger, Wait: wait,
			PreShutdown: func() {}, Shutdown: func() { returnedAtBody = svc.returned.Load() },
		})

		pre()
		shutdown()
		require.True(t, returnedAtBody, "body ran while the RPC was still in flight")
		require.NoError(t, <-rpcErr, "a failed web drain must not hard-stop in-flight native RPCs")
	})

	t.Run("stuck RPC force-stopped after cap", func(t *testing.T) {
		server, check, svc := startHoldingGrpc(t)
		logger, hook := test.NewNullLogger()
		rpcErr := make(chan error, 1)
		go func() { rpcErr <- check("hold") }()
		<-svc.entered

		// Pre must call GracefulStop before shutdown closes the wait's stop
		// channel. Otherwise the wait may return false and log a spurious
		// in-flight warning.
		drained := make(chan struct{})
		wait := func(<-chan struct{}) bool {
			close(drained)
			return true
		}
		var bodyRan bool
		pre, shutdown := shutdownHooks(shutdownHooksConfig{
			Server: server, Timeout: 100 * time.Millisecond, Logger: logger, Wait: wait,
			PreShutdown: func() {}, Shutdown: func() { bodyRan = true },
		})

		pre()
		<-drained
		shutdown()
		require.True(t, bodyRan)
		require.Equal(t, codes.Unavailable, status.Code(<-rpcErr))
		require.Equal(t, "grpc graceful stop timed out, forcing stop without waiting", hook.LastEntry().Message)
	})

	t.Run("body runs at cap while GracefulStop is wedged", func(t *testing.T) {
		server, conn, svc := serveHoldingGrpc(t)
		go func() {
			_, _ = healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "wedge"})
		}()
		<-svc.entered
		// With its client gone, the server has no connections left.
		// GracefulStop then waits on the handler while it holds the server
		// mutex (grpc-go#9393).
		require.NoError(t, conn.Close())

		pre, shutdown := shutdownHooks(shutdownHooksConfig{
			Server: server, Timeout: 100 * time.Millisecond, Logger: logger, Wait: idleDrain,
			PreShutdown: func() {}, Shutdown: func() {},
		})
		pre()

		// Serve locks the server mutex first. On a stopped server or a closed
		// listener it then returns at once. So a Serve call that blocks means
		// GracefulStop holds the mutex. Cleanup's unblock releases it.
		probeLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		probeLis.Close()
		require.Eventually(t, func() bool {
			served := make(chan struct{})
			go func() {
				_ = server.Serve(probeLis)
				close(served)
			}()
			select {
			case <-served:
				return false
			case <-time.After(100 * time.Millisecond):
				return true
			}
		}, 5*time.Second, 10*time.Millisecond, "GracefulStop never held the server mutex")

		bodyDone := make(chan struct{})
		go func() {
			shutdown()
			close(bodyDone)
		}()
		select {
		case <-bodyDone:
		case <-time.After(5 * time.Second):
			t.Fatal("body did not run while GracefulStop was wedged")
		}
	})

	t.Run("stop waits for the pre-stop wait; join releases it", func(t *testing.T) {
		server, check, _ := startHoldingGrpc(t)
		waitReturned := make(chan struct{})
		wait := func(stop <-chan struct{}) bool {
			<-stop
			close(waitReturned)
			return false
		}
		var bodyRan bool
		pre, shutdown := shutdownHooks(shutdownHooksConfig{
			Server: server, Timeout: 5 * time.Second, Logger: logger, Wait: wait,
			PreShutdown: func() {},
			Shutdown: func() {
				bodyRan = true
				select {
				case <-waitReturned:
				default:
					t.Error("body ran before the join released the wait")
				}
			},
		})

		preDone := make(chan struct{})
		go func() {
			pre()
			close(preDone)
		}()
		select {
		case <-preDone:
		case <-time.After(5 * time.Second):
			t.Fatal("Pre blocked on the pre-stop wait")
		}
		require.NoError(t, check(""), "gRPC must keep serving until the wait returns")

		shutdown()
		require.True(t, bodyRan)
		require.Equal(t, codes.Unavailable, status.Code(check("")))
	})

	t.Run("hook without Pre times out and stops", func(t *testing.T) {
		server, check, _ := startHoldingGrpc(t)
		require.NoError(t, check(""), "gRPC must serve before shutdown")

		var bodyRan bool
		_, shutdown := shutdownHooks(shutdownHooksConfig{
			Server: server, Timeout: 100 * time.Millisecond, Logger: logger, Wait: idleDrain,
			PreShutdown: func() {}, Shutdown: func() { bodyRan = true },
		})

		shutdown()
		require.True(t, bodyRan)
		require.Eventually(t, func() bool {
			return status.Code(check("")) == codes.Unavailable
		}, 5*time.Second, 10*time.Millisecond)
	})
}
