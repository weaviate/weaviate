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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
