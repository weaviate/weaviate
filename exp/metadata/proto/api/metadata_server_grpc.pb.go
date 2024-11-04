// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             (unknown)
// source: api/metadata_server.proto

// NOTE run `buf generate` from `exp/metadata/proto` to regenerate code

package api

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
	MetadataService_QuerierStream_FullMethodName = "/weaviate.internal.metadata.MetadataService/QuerierStream"
)

// MetadataServiceClient is the client API for MetadataService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type MetadataServiceClient interface {
	// QuerierStream is experimental, may be changed/removed. A QuerierStream represents a
	// connection between this metadata cluster and a querier node. See the implementing
	// function's doc for more information.
	QuerierStream(ctx context.Context, opts ...grpc.CallOption) (MetadataService_QuerierStreamClient, error)
}

type metadataServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewMetadataServiceClient(cc grpc.ClientConnInterface) MetadataServiceClient {
	return &metadataServiceClient{cc}
}

func (c *metadataServiceClient) QuerierStream(ctx context.Context, opts ...grpc.CallOption) (MetadataService_QuerierStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, &MetadataService_ServiceDesc.Streams[0], MetadataService_QuerierStream_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &metadataServiceQuerierStreamClient{stream}
	return x, nil
}

type MetadataService_QuerierStreamClient interface {
	Send(*QuerierStreamRequest) error
	Recv() (*QuerierStreamResponse, error)
	grpc.ClientStream
}

type metadataServiceQuerierStreamClient struct {
	grpc.ClientStream
}

func (x *metadataServiceQuerierStreamClient) Send(m *QuerierStreamRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *metadataServiceQuerierStreamClient) Recv() (*QuerierStreamResponse, error) {
	m := new(QuerierStreamResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// MetadataServiceServer is the server API for MetadataService service.
// All implementations should embed UnimplementedMetadataServiceServer
// for forward compatibility
type MetadataServiceServer interface {
	// QuerierStream is experimental, may be changed/removed. A QuerierStream represents a
	// connection between this metadata cluster and a querier node. See the implementing
	// function's doc for more information.
	QuerierStream(MetadataService_QuerierStreamServer) error
}

// UnimplementedMetadataServiceServer should be embedded to have forward compatible implementations.
type UnimplementedMetadataServiceServer struct {
}

func (UnimplementedMetadataServiceServer) QuerierStream(MetadataService_QuerierStreamServer) error {
	return status.Errorf(codes.Unimplemented, "method QuerierStream not implemented")
}

// UnsafeMetadataServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to MetadataServiceServer will
// result in compilation errors.
type UnsafeMetadataServiceServer interface {
	mustEmbedUnimplementedMetadataServiceServer()
}

func RegisterMetadataServiceServer(s grpc.ServiceRegistrar, srv MetadataServiceServer) {
	s.RegisterService(&MetadataService_ServiceDesc, srv)
}

func _MetadataService_QuerierStream_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(MetadataServiceServer).QuerierStream(&metadataServiceQuerierStreamServer{stream})
}

type MetadataService_QuerierStreamServer interface {
	Send(*QuerierStreamResponse) error
	Recv() (*QuerierStreamRequest, error)
	grpc.ServerStream
}

type metadataServiceQuerierStreamServer struct {
	grpc.ServerStream
}

func (x *metadataServiceQuerierStreamServer) Send(m *QuerierStreamResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *metadataServiceQuerierStreamServer) Recv() (*QuerierStreamRequest, error) {
	m := new(QuerierStreamRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// MetadataService_ServiceDesc is the grpc.ServiceDesc for MetadataService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var MetadataService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "weaviate.internal.metadata.MetadataService",
	HandlerType: (*MetadataServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "QuerierStream",
			Handler:       _MetadataService_QuerierStream_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "api/metadata_server.proto",
}
