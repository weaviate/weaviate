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

package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/usecases/cluster"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestMaintenanceModeUnaryInterceptor(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	t.Run("passes through when not in maintenance mode", func(t *testing.T) {
		interceptor := makeMaintenanceModeUnaryInterceptor(func() bool { return false })
		resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
	})

	t.Run("rejects with FailedPrecondition when in maintenance mode", func(t *testing.T) {
		interceptor := makeMaintenanceModeUnaryInterceptor(func() bool { return true })
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
		assert.Equal(t, "server is in maintenance mode", st.Message())
	})
}

func TestMaintenanceModeStreamInterceptor(t *testing.T) {
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	t.Run("passes through when not in maintenance mode", func(t *testing.T) {
		interceptor := makeMaintenanceModeStreamInterceptor(func() bool { return false })
		err := interceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.NoError(t, err)
	})

	t.Run("rejects with FailedPrecondition when in maintenance mode", func(t *testing.T) {
		interceptor := makeMaintenanceModeStreamInterceptor(func() bool { return true })
		err := interceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.FailedPrecondition, st.Code())
		assert.Equal(t, "server is in maintenance mode", st.Message())
	})
}

func TestNodeReadyUnaryInterceptor(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	t.Run("passes through when node is ready", func(t *testing.T) {
		interceptor := makeNodeReadyUnaryInterceptor(func() bool { return true }, []string{"/clusterapi.ReplicationService"})
		resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
	})

	t.Run("rejects when node is not ready", func(t *testing.T) {
		interceptor := makeNodeReadyUnaryInterceptor(func() bool { return false }, []string{"/clusterapi.ReplicationService"})
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Unavailable, st.Code())
		assert.Equal(t, "node not ready", st.Message())
	})

	t.Run("skips check for non-matching service", func(t *testing.T) {
		interceptor := makeNodeReadyUnaryInterceptor(func() bool { return false }, []string{"/clusterapi.ReplicationService"})
		resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.FileReplicationService/Copy"}, handler)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
	})
}

func TestQueueUnaryInterceptor(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	t.Run("passes through when queue is disabled", func(t *testing.T) {
		rq := shared.NewRequestQueue(newTestQueueConfig(false, 10, 1), logger(),
			func(item grpcQueueItem) {
				resp, err := item.handler(item.ctx, item.req)
				item.result <- grpcQueueResult{resp, err}
			},
			func(item grpcQueueItem) bool { return item.ctx.Err() != nil },
			func(item grpcQueueItem) {
				item.result <- grpcQueueResult{nil, status.Error(codes.DeadlineExceeded, "expired")}
			},
		)
		interceptor := makeQueueUnaryInterceptor(rq, []string{"/clusterapi.ReplicationService"})
		resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
	})

	t.Run("allows requests when queue is enabled and below capacity", func(t *testing.T) {
		rq := shared.NewRequestQueue(newTestQueueConfig(true, 10, 2), logger(),
			func(item grpcQueueItem) {
				resp, err := item.handler(item.ctx, item.req)
				item.result <- grpcQueueResult{resp, err}
			},
			func(item grpcQueueItem) bool { return item.ctx.Err() != nil },
			func(item grpcQueueItem) {
				item.result <- grpcQueueResult{nil, status.Error(codes.DeadlineExceeded, "expired")}
			},
		)
		interceptor := makeQueueUnaryInterceptor(rq, []string{"/clusterapi.ReplicationService"})
		resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
		require.NoError(t, rq.Close(context.Background()))
	})

	t.Run("returns ResourceExhausted when queue is full", func(t *testing.T) {
		// Queue size 1, 1 worker with a blocking handler
		block := make(chan struct{})
		rq := shared.NewRequestQueue(newTestQueueConfig(true, 1, 1), logger(),
			func(item grpcQueueItem) {
				<-block
				resp, err := item.handler(item.ctx, item.req)
				item.result <- grpcQueueResult{resp, err}
			},
			func(item grpcQueueItem) bool { return item.ctx.Err() != nil },
			func(item grpcQueueItem) {
				item.result <- grpcQueueResult{nil, status.Error(codes.DeadlineExceeded, "expired")}
			},
		)
		interceptor := makeQueueUnaryInterceptor(rq, []string{"/clusterapi.ReplicationService"})
		info := &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}

		// First request: picked up by the worker, which blocks
		done1 := make(chan struct{})
		go func() {
			defer close(done1)
			//nolint:errcheck
			interceptor(context.Background(), nil, info, handler)
		}()

		// Wait for worker to pick up the first item
		time.Sleep(50 * time.Millisecond)

		// Second request: fills the queue buffer (size 1)
		done2 := make(chan struct{})
		go func() {
			defer close(done2)
			//nolint:errcheck
			interceptor(context.Background(), nil, info, handler)
		}()

		// Wait for it to be enqueued
		time.Sleep(50 * time.Millisecond)

		// Third request: queue is full, should be rejected
		_, err := interceptor(context.Background(), nil, info, handler)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.ResourceExhausted, st.Code())
		assert.Equal(t, "replication request queue full", st.Message())

		close(block)
		<-done1
		<-done2
		require.NoError(t, rq.Close(context.Background()))
	})

	t.Run("returns Canceled when context is cancelled while waiting", func(t *testing.T) {
		block := make(chan struct{})
		rq := shared.NewRequestQueue(newTestQueueConfig(true, 10, 1), logger(),
			func(item grpcQueueItem) {
				<-block // block until test releases
				resp, err := item.handler(item.ctx, item.req)
				item.result <- grpcQueueResult{resp, err}
			},
			func(item grpcQueueItem) bool { return false },
			func(item grpcQueueItem) {
				item.result <- grpcQueueResult{nil, status.Error(codes.DeadlineExceeded, "expired")}
			},
		)
		interceptor := makeQueueUnaryInterceptor(rq, []string{"/clusterapi.ReplicationService"})
		info := &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan struct{})
		var interceptorErr error
		go func() {
			defer close(done)
			_, interceptorErr = interceptor(ctx, nil, info, handler)
		}()

		// Wait for the item to be picked up by the worker
		time.Sleep(50 * time.Millisecond)

		// Cancel the caller's context
		cancel()
		<-done

		require.Error(t, interceptorErr)
		st, ok := status.FromError(interceptorErr)
		require.True(t, ok)
		assert.Equal(t, codes.Canceled, st.Code())

		// Unblock the worker so it can finish and the queue can close cleanly
		close(block)
		require.NoError(t, rq.Close(context.Background()))
	})

	t.Run("skips check for non-matching service", func(t *testing.T) {
		rq := shared.NewRequestQueue(newTestQueueConfig(true, 10, 1), logger(),
			func(item grpcQueueItem) {
				resp, err := item.handler(item.ctx, item.req)
				item.result <- grpcQueueResult{resp, err}
			},
			func(item grpcQueueItem) bool { return item.ctx.Err() != nil },
			func(item grpcQueueItem) {
				item.result <- grpcQueueResult{nil, status.Error(codes.DeadlineExceeded, "expired")}
			},
		)
		interceptor := makeQueueUnaryInterceptor(rq, []string{"/clusterapi.ReplicationService"})
		resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.FileReplicationService/Copy"}, handler)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
		require.NoError(t, rq.Close(context.Background()))
	})
}

func newTestQueueConfig(enabled bool, queueSize, numWorkers int) cluster.RequestQueueConfig {
	dv := configRuntime.NewDynamicValue(enabled)
	return cluster.RequestQueueConfig{
		IsEnabled:                   dv,
		QueueSize:                   queueSize,
		NumWorkers:                  numWorkers,
		QueueShutdownTimeoutSeconds: 5,
	}
}

func logger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}
