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

package grpc

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

// GRPCInferenceServiceClient is the client API for GRPCInferenceService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type GRPCInferenceServiceClient interface {
	// The ServerLive API indicates if the inference server is able to receive
	// and respond to metadata and inference requests.
	ServerLive(ctx context.Context, in *ServerLiveRequest, opts ...grpc.CallOption) (*ServerLiveResponse, error)
	// The ServerReady API indicates if the server is ready for inferencing.
	ServerReady(ctx context.Context, in *ServerReadyRequest, opts ...grpc.CallOption) (*ServerReadyResponse, error)
	// The ModelReady API indicates if a specific model is ready for inferencing.
	ModelReady(ctx context.Context, in *ModelReadyRequest, opts ...grpc.CallOption) (*ModelReadyResponse, error)
	// The ServerMetadata API provides information about the server. Errors are
	// indicated by the google.rpc.Status returned for the request. The OK code
	// indicates success and other codes indicate failure.
	ServerMetadata(ctx context.Context, in *ServerMetadataRequest, opts ...grpc.CallOption) (*ServerMetadataResponse, error)
	// The per-model metadata API provides information about a model. Errors are
	// indicated by the google.rpc.Status returned for the request. The OK code
	// indicates success and other codes indicate failure.
	ModelMetadata(ctx context.Context, in *ModelMetadataRequest, opts ...grpc.CallOption) (*ModelMetadataResponse, error)
	// The ModelInfer API performs inference using the specified model. Errors are
	// indicated by the google.rpc.Status returned for the request. The OK code
	// indicates success and other codes indicate failure.
	ModelInfer(ctx context.Context, in *ModelInferRequest, opts ...grpc.CallOption) (*ModelInferResponse, error)
}

type gRPCInferenceServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewGRPCInferenceServiceClient(cc grpc.ClientConnInterface) GRPCInferenceServiceClient {
	return &gRPCInferenceServiceClient{cc}
}

func (c *gRPCInferenceServiceClient) ServerLive(ctx context.Context, in *ServerLiveRequest, opts ...grpc.CallOption) (*ServerLiveResponse, error) {
	out := new(ServerLiveResponse)
	err := c.cc.Invoke(ctx, "/inference.GRPCInferenceService/ServerLive", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gRPCInferenceServiceClient) ServerReady(ctx context.Context, in *ServerReadyRequest, opts ...grpc.CallOption) (*ServerReadyResponse, error) {
	out := new(ServerReadyResponse)
	err := c.cc.Invoke(ctx, "/inference.GRPCInferenceService/ServerReady", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gRPCInferenceServiceClient) ModelReady(ctx context.Context, in *ModelReadyRequest, opts ...grpc.CallOption) (*ModelReadyResponse, error) {
	out := new(ModelReadyResponse)
	err := c.cc.Invoke(ctx, "/inference.GRPCInferenceService/ModelReady", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gRPCInferenceServiceClient) ServerMetadata(ctx context.Context, in *ServerMetadataRequest, opts ...grpc.CallOption) (*ServerMetadataResponse, error) {
	out := new(ServerMetadataResponse)
	err := c.cc.Invoke(ctx, "/inference.GRPCInferenceService/ServerMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gRPCInferenceServiceClient) ModelMetadata(ctx context.Context, in *ModelMetadataRequest, opts ...grpc.CallOption) (*ModelMetadataResponse, error) {
	out := new(ModelMetadataResponse)
	err := c.cc.Invoke(ctx, "/inference.GRPCInferenceService/ModelMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *gRPCInferenceServiceClient) ModelInfer(ctx context.Context, in *ModelInferRequest, opts ...grpc.CallOption) (*ModelInferResponse, error) {
	out := new(ModelInferResponse)
	err := c.cc.Invoke(ctx, "/inference.GRPCInferenceService/ModelInfer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GRPCInferenceServiceServer is the server API for GRPCInferenceService service.
// All implementations must embed UnimplementedGRPCInferenceServiceServer
// for forward compatibility
type GRPCInferenceServiceServer interface {
	// The ServerLive API indicates if the inference server is able to receive
	// and respond to metadata and inference requests.
	ServerLive(context.Context, *ServerLiveRequest) (*ServerLiveResponse, error)
	// The ServerReady API indicates if the server is ready for inferencing.
	ServerReady(context.Context, *ServerReadyRequest) (*ServerReadyResponse, error)
	// The ModelReady API indicates if a specific model is ready for inferencing.
	ModelReady(context.Context, *ModelReadyRequest) (*ModelReadyResponse, error)
	// The ServerMetadata API provides information about the server. Errors are
	// indicated by the google.rpc.Status returned for the request. The OK code
	// indicates success and other codes indicate failure.
	ServerMetadata(context.Context, *ServerMetadataRequest) (*ServerMetadataResponse, error)
	// The per-model metadata API provides information about a model. Errors are
	// indicated by the google.rpc.Status returned for the request. The OK code
	// indicates success and other codes indicate failure.
	ModelMetadata(context.Context, *ModelMetadataRequest) (*ModelMetadataResponse, error)
	// The ModelInfer API performs inference using the specified model. Errors are
	// indicated by the google.rpc.Status returned for the request. The OK code
	// indicates success and other codes indicate failure.
	ModelInfer(context.Context, *ModelInferRequest) (*ModelInferResponse, error)
	mustEmbedUnimplementedGRPCInferenceServiceServer()
}

// UnimplementedGRPCInferenceServiceServer must be embedded to have forward compatible implementations.
type UnimplementedGRPCInferenceServiceServer struct {
}

func (UnimplementedGRPCInferenceServiceServer) ServerLive(context.Context, *ServerLiveRequest) (*ServerLiveResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ServerLive not implemented")
}
func (UnimplementedGRPCInferenceServiceServer) ServerReady(context.Context, *ServerReadyRequest) (*ServerReadyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ServerReady not implemented")
}
func (UnimplementedGRPCInferenceServiceServer) ModelReady(context.Context, *ModelReadyRequest) (*ModelReadyResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ModelReady not implemented")
}
func (UnimplementedGRPCInferenceServiceServer) ServerMetadata(context.Context, *ServerMetadataRequest) (*ServerMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ServerMetadata not implemented")
}
func (UnimplementedGRPCInferenceServiceServer) ModelMetadata(context.Context, *ModelMetadataRequest) (*ModelMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ModelMetadata not implemented")
}
func (UnimplementedGRPCInferenceServiceServer) ModelInfer(context.Context, *ModelInferRequest) (*ModelInferResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ModelInfer not implemented")
}
func (UnimplementedGRPCInferenceServiceServer) mustEmbedUnimplementedGRPCInferenceServiceServer() {}

// UnsafeGRPCInferenceServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to GRPCInferenceServiceServer will
// result in compilation errors.
type UnsafeGRPCInferenceServiceServer interface {
	mustEmbedUnimplementedGRPCInferenceServiceServer()
}

func RegisterGRPCInferenceServiceServer(s grpc.ServiceRegistrar, srv GRPCInferenceServiceServer) {
	s.RegisterService(&GRPCInferenceService_ServiceDesc, srv)
}

func _GRPCInferenceService_ServerLive_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ServerLiveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GRPCInferenceServiceServer).ServerLive(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/inference.GRPCInferenceService/ServerLive",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GRPCInferenceServiceServer).ServerLive(ctx, req.(*ServerLiveRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GRPCInferenceService_ServerReady_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ServerReadyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GRPCInferenceServiceServer).ServerReady(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/inference.GRPCInferenceService/ServerReady",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GRPCInferenceServiceServer).ServerReady(ctx, req.(*ServerReadyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GRPCInferenceService_ModelReady_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ModelReadyRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GRPCInferenceServiceServer).ModelReady(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/inference.GRPCInferenceService/ModelReady",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GRPCInferenceServiceServer).ModelReady(ctx, req.(*ModelReadyRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GRPCInferenceService_ServerMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ServerMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GRPCInferenceServiceServer).ServerMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/inference.GRPCInferenceService/ServerMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GRPCInferenceServiceServer).ServerMetadata(ctx, req.(*ServerMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GRPCInferenceService_ModelMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ModelMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GRPCInferenceServiceServer).ModelMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/inference.GRPCInferenceService/ModelMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GRPCInferenceServiceServer).ModelMetadata(ctx, req.(*ModelMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _GRPCInferenceService_ModelInfer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ModelInferRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GRPCInferenceServiceServer).ModelInfer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/inference.GRPCInferenceService/ModelInfer",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GRPCInferenceServiceServer).ModelInfer(ctx, req.(*ModelInferRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// GRPCInferenceService_ServiceDesc is the grpc.ServiceDesc for GRPCInferenceService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var GRPCInferenceService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "inference.GRPCInferenceService",
	HandlerType: (*GRPCInferenceServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ServerLive",
			Handler:    _GRPCInferenceService_ServerLive_Handler,
		},
		{
			MethodName: "ServerReady",
			Handler:    _GRPCInferenceService_ServerReady_Handler,
		},
		{
			MethodName: "ModelReady",
			Handler:    _GRPCInferenceService_ModelReady_Handler,
		},
		{
			MethodName: "ServerMetadata",
			Handler:    _GRPCInferenceService_ServerMetadata_Handler,
		},
		{
			MethodName: "ModelMetadata",
			Handler:    _GRPCInferenceService_ModelMetadata_Handler,
		},
		{
			MethodName: "ModelInfer",
			Handler:    _GRPCInferenceService_ModelInfer_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "modules/text2vec-kserve/grpc/service.proto",
}
