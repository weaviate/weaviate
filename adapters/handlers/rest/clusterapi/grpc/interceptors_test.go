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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
)

func TestMaintenanceModeUnaryInterceptor(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	t.Run("passes through when not in maintenance mode", func(t *testing.T) {
		interceptor := makeMaintenanceModeUnaryInterceptor(func() bool { return false }, nil)
		resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/clusterapi.ReplicationService/PutObject"}, handler)
		require.NoError(t, err)
		assert.Equal(t, "ok", resp)
	})

	t.Run("rejects with FailedPrecondition when in maintenance mode", func(t *testing.T) {
		interceptor := makeMaintenanceModeUnaryInterceptor(func() bool { return true }, nil)
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

func TestMaintenanceModeUnaryInterceptorAsyncReplicationAllowlist(t *testing.T) {
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}
	allowed := map[string]bool{
		pb.ReplicationService_CompareHashTreeRoots_FullMethodName:      true,
		pb.ReplicationService_CompareHashTreeRootsMulti_FullMethodName: true,
		pb.ReplicationService_HashTreeLevel_FullMethodName:             true,
		pb.ReplicationService_CompareDigests_FullMethodName:            true,
		pb.ReplicationService_DigestObjectsInRange_FullMethodName:      true,
		pb.ReplicationService_OverwriteObjects_FullMethodName:          true,
		pb.ReplicationService_CreateAsyncCheckpoint_FullMethodName:     true,
		pb.ReplicationService_DeleteAsyncCheckpoint_FullMethodName:     true,
		pb.ReplicationService_GetAsyncCheckpointStatus_FullMethodName:  true,
	}

	var methods []string
	for _, desc := range []grpc.ServiceDesc{pb.ReplicationService_ServiceDesc, pb.FileReplicationService_ServiceDesc} {
		for _, m := range desc.Methods {
			methods = append(methods, "/"+desc.ServiceName+"/"+m.MethodName)
		}
	}
	for m := range allowed {
		require.Contains(t, methods, m)
	}
	require.Contains(t, methods, pb.ReplicationService_PutObject_FullMethodName)
	require.Contains(t, methods, pb.FileReplicationService_CreateReplicaSnapshot_FullMethodName)
	methods = append(methods, "/clusterapi.Unknown/CompareHashTreeRoots", "/weaviate.v1.Weaviate/Search")

	flagStates := []struct {
		name string
		flag func() bool
	}{
		{name: "flag nil", flag: nil},
		{name: "flag off", flag: func() bool { return false }},
		{name: "flag on", flag: func() bool { return true }},
	}
	for _, maintenance := range []bool{false, true} {
		for _, fs := range flagStates {
			for _, method := range methods {
				t.Run(fmt.Sprintf("maintenance=%v/%s%s", maintenance, fs.name, method), func(t *testing.T) {
					interceptor := makeMaintenanceModeUnaryInterceptor(func() bool { return maintenance }, fs.flag)
					resp, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
					wantPass := !maintenance || (fs.flag != nil && fs.flag() && allowed[method])
					if wantPass {
						require.NoError(t, err)
						assert.Equal(t, "ok", resp)
						return
					}
					require.Equal(t, codes.FailedPrecondition, status.Code(err))
				})
			}
		}
	}

	t.Run("flag flips take effect on the next call", func(t *testing.T) {
		var on atomic.Bool
		interceptor := makeMaintenanceModeUnaryInterceptor(func() bool { return true }, on.Load)
		info := &grpc.UnaryServerInfo{FullMethod: pb.ReplicationService_HashTreeLevel_FullMethodName}
		_, err := interceptor(context.Background(), nil, info, handler)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		on.Store(true)
		_, err = interceptor(context.Background(), nil, info, handler)
		require.NoError(t, err)
		on.Store(false)
		_, err = interceptor(context.Background(), nil, info, handler)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})

	t.Run("allowlisted call still hits the node-ready gate", func(t *testing.T) {
		maintenanceGate := makeMaintenanceModeUnaryInterceptor(func() bool { return true }, func() bool { return true })
		readyGate := makeNodeReadyUnaryInterceptor(func() bool { return false }, []string{"/clusterapi.ReplicationService"})
		info := &grpc.UnaryServerInfo{FullMethod: pb.ReplicationService_OverwriteObjects_FullMethodName}
		_, err := maintenanceGate(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) {
			return readyGate(ctx, req, info, handler)
		})
		require.Equal(t, codes.Unavailable, status.Code(err))
	})
}

func TestMaintenanceModeStreamInterceptorBlocksEveryStream(t *testing.T) {
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}
	var streams []string
	for _, desc := range []grpc.ServiceDesc{pb.ReplicationService_ServiceDesc, pb.FileReplicationService_ServiceDesc} {
		for _, s := range desc.Streams {
			streams = append(streams, "/"+desc.ServiceName+"/"+s.StreamName)
		}
	}
	require.NotEmpty(t, streams)
	for _, method := range streams {
		t.Run(method, func(t *testing.T) {
			err := makeMaintenanceModeStreamInterceptor(func() bool { return true })(nil, nil, &grpc.StreamServerInfo{FullMethod: method}, handler)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}
