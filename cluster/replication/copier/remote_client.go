//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package copier

import (
	"context"
	"encoding/base64"

	"github.com/weaviate/weaviate/usecases/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

type FileReplicationServiceClientFactory = func(ctx context.Context, address string) (FileReplicationServiceClient, error)

type FileReplicationServiceClient interface {
	pbv1.FileReplicationServiceClient
	Close() error
}

type grpcFileReplicationServiceClient struct {
	clientConn   *grpc.ClientConn
	authMetadata metadata.MD
	client       pbv1.FileReplicationServiceClient
}

func NewFileReplicationServiceClient(clientConn *grpc.ClientConn, authConfig cluster.AuthConfig) FileReplicationServiceClient {
	var authMetadata metadata.MD

	if authConfig.BasicAuth.Enabled() {
		auth := base64.StdEncoding.EncodeToString([]byte(authConfig.BasicAuth.Username + ":" + authConfig.BasicAuth.Password))
		authMetadata = metadata.New(map[string]string{
			"authorization": "Basic " + auth,
		})
	}

	return &grpcFileReplicationServiceClient{
		clientConn:   clientConn,
		authMetadata: authMetadata,
		client:       pbv1.NewFileReplicationServiceClient(clientConn),
	}
}

func (g *grpcFileReplicationServiceClient) Close() error {
	return g.clientConn.Close()
}

func (g *grpcFileReplicationServiceClient) addAuthMetadataToContext(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, g.authMetadata)
}

func (g *grpcFileReplicationServiceClient) GetFile(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[pbv1.GetFileRequest, pbv1.FileChunk], error) {
	return g.client.GetFile(g.addAuthMetadataToContext(ctx), opts...)
}

func (g *grpcFileReplicationServiceClient) GetFileMetadata(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[pbv1.GetFileMetadataRequest, pbv1.FileMetadata], error) {
	return g.client.GetFileMetadata(g.addAuthMetadataToContext(ctx), opts...)
}

func (g *grpcFileReplicationServiceClient) ListFiles(ctx context.Context, in *pbv1.ListFilesRequest, opts ...grpc.CallOption) (*pbv1.ListFilesResponse, error) {
	return g.client.ListFiles(g.addAuthMetadataToContext(ctx), in, opts...)
}

func (g *grpcFileReplicationServiceClient) PauseFileActivity(ctx context.Context, in *pbv1.PauseFileActivityRequest, opts ...grpc.CallOption) (*pbv1.PauseFileActivityResponse, error) {
	return g.client.PauseFileActivity(g.addAuthMetadataToContext(ctx), in, opts...)
}

func (g *grpcFileReplicationServiceClient) ResumeFileActivity(ctx context.Context, in *pbv1.ResumeFileActivityRequest, opts ...grpc.CallOption) (*pbv1.ResumeFileActivityResponse, error) {
	return g.client.ResumeFileActivity(g.addAuthMetadataToContext(ctx), in, opts...)
}
