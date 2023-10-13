//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	context "context"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	ClusterService_RemovePeer_FullMethodName = "/weaviate.cloud.internal.cluster.ClusterService/RemovePeer"
	ClusterService_JoinPeer_FullMethodName   = "/weaviate.cloud.internal.cluster.ClusterService/JoinPeer"
)

// ClusterServiceClient is the client API for ClusterService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ClusterServiceClient interface {
	RemovePeer(ctx context.Context, in *RemovePeerRequest, opts ...grpc.CallOption) (*RemovePeerResponse, error)
	JoinPeer(ctx context.Context, in *JoinPeerRequest, opts ...grpc.CallOption) (*JoinPeerResponse, error)
}

type clusterServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewClusterServiceClient(cc grpc.ClientConnInterface) ClusterServiceClient {
	return &clusterServiceClient{cc}
}

func (c *clusterServiceClient) RemovePeer(ctx context.Context, in *RemovePeerRequest, opts ...grpc.CallOption) (*RemovePeerResponse, error) {
	out := new(RemovePeerResponse)
	err := c.cc.Invoke(ctx, ClusterService_RemovePeer_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *clusterServiceClient) JoinPeer(ctx context.Context, in *JoinPeerRequest, opts ...grpc.CallOption) (*JoinPeerResponse, error) {
	out := new(JoinPeerResponse)
	err := c.cc.Invoke(ctx, ClusterService_JoinPeer_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ClusterServiceServer is the server API for ClusterService service.
// All implementations should embed UnimplementedClusterServiceServer
// for forward compatibility
type ClusterServiceServer interface {
	RemovePeer(context.Context, *RemovePeerRequest) (*RemovePeerResponse, error)
	JoinPeer(context.Context, *JoinPeerRequest) (*JoinPeerResponse, error)
}

// UnimplementedClusterServiceServer should be embedded to have forward compatible implementations.
type UnimplementedClusterServiceServer struct {
}

func (UnimplementedClusterServiceServer) RemovePeer(context.Context, *RemovePeerRequest) (*RemovePeerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RemovePeer not implemented")
}
func (UnimplementedClusterServiceServer) JoinPeer(context.Context, *JoinPeerRequest) (*JoinPeerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method JoinPeer not implemented")
}

// UnsafeClusterServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ClusterServiceServer will
// result in compilation errors.
type UnsafeClusterServiceServer interface {
	mustEmbedUnimplementedClusterServiceServer()
}

func RegisterClusterServiceServer(s grpc.ServiceRegistrar, srv ClusterServiceServer) {
	s.RegisterService(&ClusterService_ServiceDesc, srv)
}

func _ClusterService_RemovePeer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RemovePeerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServiceServer).RemovePeer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ClusterService_RemovePeer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServiceServer).RemovePeer(ctx, req.(*RemovePeerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ClusterService_JoinPeer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(JoinPeerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ClusterServiceServer).JoinPeer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ClusterService_JoinPeer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ClusterServiceServer).JoinPeer(ctx, req.(*JoinPeerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ClusterService_ServiceDesc is the grpc.ServiceDesc for ClusterService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ClusterService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "weaviate.cloud.internal.cluster.ClusterService",
	HandlerType: (*ClusterServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "RemovePeer",
			Handler:    _ClusterService_RemovePeer_Handler,
		},
		{
			MethodName: "JoinPeer",
			Handler:    _ClusterService_JoinPeer_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "cluster/message.proto",
}
