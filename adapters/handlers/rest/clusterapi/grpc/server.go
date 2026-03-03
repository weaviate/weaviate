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
	"encoding/base64"
	"fmt"
	"net"
	"strings"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // Install the gzip compressor
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Server struct {
	*grpc.Server
	state *state.State
}

const (
	PB_OVERHEAD       = 16 * 1024       // 16kB for any extra overhead
	DEFAULT_MSG_SIZE  = 4 * 1024 * 1024 // 4MB default from grpc
	READ_BUFFER_SIZE  = 4 << 20         // 4 MB
	WRITE_BUFFER_SIZE = 4 << 20         // 4 MB
)

// NewServer creates *grpc.Server with optional grpc.Serveroption passed.
// If replicationServer is non-nil, the ReplicationService will be registered.
func NewServer(state *state.State, replicationServer ReplicationServer, options ...grpc.ServerOption) *Server {
	fileCopyChunkSize := state.ServerConfig.Config.ReplicationEngineFileCopyChunkSize

	maxSize := GetMaxMessageSize(fileCopyChunkSize)
	initialConnWindowSize := GetInitialConnWindowSize(
		fileCopyChunkSize, state.ServerConfig.Config.ReplicationEngineFileCopyWorkers,
	)

	o := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxSize),
		grpc.MaxSendMsgSize(maxSize),
		grpc.InitialWindowSize(int32(maxSize)),
		grpc.InitialConnWindowSize(int32(initialConnWindowSize)),
		grpc.ReadBufferSize(READ_BUFFER_SIZE),
		grpc.WriteBufferSize(WRITE_BUFFER_SIZE),
	}

	// Both FileReplicationService and ReplicationService are internal cluster services
	// that require basic auth when enabled.
	servicePrefixes := []string{"/clusterapi.FileReplicationService", "/clusterapi.ReplicationService"}
	basicAuth := state.ServerConfig.Config.Cluster.AuthConfig.BasicAuth
	if basicAuth.Enabled() {
		o = append(o, grpc.UnaryInterceptor(
			multiServiceBasicAuthUnaryInterceptor(servicePrefixes, basicAuth.Username, basicAuth.Password),
		))

		o = append(o, grpc.StreamInterceptor(
			multiServiceBasicAuthStreamInterceptor(servicePrefixes, basicAuth.Username, basicAuth.Password),
		))
	}

	s := grpc.NewServer(o...)

	weaviateV1FileReplicationService := NewFileReplicationService(state.DB, state.ClusterService.SchemaReader(), fileCopyChunkSize)
	pb.RegisterFileReplicationServiceServer(s, weaviateV1FileReplicationService)

	if replicationServer != nil {
		replicationService := NewReplicationService(replicationServer)
		pb.RegisterReplicationServiceServer(s, replicationService)
	}

	return &Server{Server: s, state: state}
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d",
		s.state.ServerConfig.Config.Cluster.DataBindPort))
	if err != nil {
		return err
	}
	s.state.Logger.WithField("action", "internal_grpc_startup").
		Infof("internal grpc server listening at %v", lis.Addr())
	return s.Server.Serve(lis)
}

func (s *Server) Close(ctx context.Context) error {
	stopped := make(chan struct{})
	enterrors.GoWrapper(func() {
		s.GracefulStop()
		close(stopped)
	}, s.state.Logger)

	select {
	case <-ctx.Done():
		s.Stop()
		return ctx.Err()
	case <-stopped:
		return nil
	}
}

func multiServiceBasicAuthUnaryInterceptor(servicePrefixes []string, expectedUsername, expectedPassword string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		if !matchesAnyPrefix(info.FullMethod, servicePrefixes) {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if err := validateBasicAuth(md, ok, expectedUsername, expectedPassword); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func multiServiceBasicAuthStreamInterceptor(servicePrefixes []string, expectedUsername, expectedPassword string) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if !matchesAnyPrefix(info.FullMethod, servicePrefixes) {
			return handler(srv, ss)
		}

		md, ok := metadata.FromIncomingContext(ss.Context())
		if err := validateBasicAuth(md, ok, expectedUsername, expectedPassword); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

func matchesAnyPrefix(method string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(method, p) {
			return true
		}
	}
	return false
}

func validateBasicAuth(md metadata.MD, ok bool, expectedUsername, expectedPassword string) error {
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	authHeader := md["authorization"]
	if len(authHeader) == 0 || !strings.HasPrefix(authHeader[0], "Basic ") {
		return status.Error(codes.Unauthenticated, "missing or invalid auth header")
	}

	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(authHeader[0], "Basic "))
	if err != nil {
		return status.Error(codes.Unauthenticated, "invalid base64 encoding")
	}

	parts := strings.SplitN(string(payload), ":", 2)
	if len(parts) != 2 || parts[0] != expectedUsername || parts[1] != expectedPassword {
		return status.Error(codes.Unauthenticated, "invalid username or password")
	}

	return nil
}

func GetMaxMessageSize(fileCopyChunkSize int) int {
	return max(DEFAULT_MSG_SIZE, fileCopyChunkSize+PB_OVERHEAD)
}

func GetInitialConnWindowSize(fileCopyChunkSize, fileCopyWorkers int) int {
	return GetMaxMessageSize(fileCopyChunkSize) * fileCopyWorkers
}
