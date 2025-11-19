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

package clusterapi

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"

	pb "github.com/weaviate/weaviate/adapters/handlers/grpc/clusterapi/proto/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	authErrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
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

// NewServer creates *grpc.Server with optional grpc.Serveroption passed.
func NewServer(state *state.State, options ...grpc.ServerOption) *Server {
	o := []grpc.ServerOption{}
	var interceptors []grpc.UnaryServerInterceptor

	interceptors = append(interceptors, makeAuthInterceptor())

	basicAuth := state.ServerConfig.Config.Cluster.AuthConfig.BasicAuth
	if basicAuth.Enabled() {
		interceptors = append(interceptors,
			basicAuthUnaryInterceptor("/weaviate.v1.FileReplicationService", basicAuth.Username, basicAuth.Password))

		o = append(o, grpc.StreamInterceptor(
			basicAuthStreamInterceptor("/weaviate.v1.FileReplicationService", basicAuth.Username, basicAuth.Password),
		))
	}

	if len(interceptors) > 0 {
		o = append(o, grpc.ChainUnaryInterceptor(interceptors...))
	}

	s := grpc.NewServer(o...)
	weaviateV1FileReplicationService := NewFileReplicationService(state.DB, state.ClusterService.SchemaReader())
	pb.RegisterFileReplicationServiceServer(s, weaviateV1FileReplicationService)

	return &Server{Server: s, state: state}
}

func (s *Server) Serve() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d",
		s.state.ServerConfig.Config.Cluster.DataBindGrpcPort))
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

func makeAuthInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)

		if errors.As(err, &authErrs.Unauthenticated{}) {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}

		if errors.As(err, &authErrs.Forbidden{}) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}

		return resp, err
	}
}

func basicAuthUnaryInterceptor(servicePrefix, expectedUsername, expectedPassword string) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		if !strings.HasPrefix(info.FullMethod, servicePrefix) {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}

		authHeader := md["authorization"]
		if len(authHeader) == 0 || !strings.HasPrefix(authHeader[0], "Basic ") {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid auth header")
		}

		// Decode and validate Basic Auth credentials
		payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(authHeader[0], "Basic "))
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "invalid base64 encoding")
		}

		parts := strings.SplitN(string(payload), ":", 2)
		if len(parts) != 2 || parts[0] != expectedUsername || parts[1] != expectedPassword {
			return nil, status.Error(codes.Unauthenticated, "invalid username or password")
		}

		return handler(ctx, req)
	}
}

func basicAuthStreamInterceptor(servicePrefix, expectedUsername, expectedPassword string) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if !strings.HasPrefix(info.FullMethod, servicePrefix) {
			return handler(srv, ss) // no auth needed
		}

		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			return status.Error(codes.Unauthenticated, "missing metadata")
		}

		authHeader := md["authorization"]
		if len(authHeader) == 0 || !strings.HasPrefix(authHeader[0], "Basic ") {
			return status.Error(codes.Unauthenticated, "missing or invalid auth header")
		}

		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(authHeader[0], "Basic "))
		if err != nil {
			return status.Error(codes.Unauthenticated, "invalid base64 encoding")
		}

		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 || parts[0] != expectedUsername || parts[1] != expectedPassword {
			return status.Error(codes.Unauthenticated, "invalid username or password")
		}

		return handler(srv, ss)
	}
}
