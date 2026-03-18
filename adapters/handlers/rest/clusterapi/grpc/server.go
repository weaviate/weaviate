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
	"errors"
	"fmt"
	"math"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // Install the gzip compressor
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/grpc/generated/protocol"
	"github.com/weaviate/weaviate/adapters/handlers/rest/clusterapi/shared"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/usecases/cluster"
)

type Server struct {
	*grpc.Server
	state        *state.State
	requestQueue *shared.RequestQueue[grpcQueueItem]
}

type grpcQueueItem struct {
	ctx     context.Context
	req     any
	info    *grpc.UnaryServerInfo
	handler grpc.UnaryHandler
	result  chan grpcQueueResult
}

type grpcQueueResult struct {
	resp any
	err  error
}

const (
	READ_BUFFER_SIZE  = 1 * 1024 * 1024 // 1 MB
	WRITE_BUFFER_SIZE = 1 * 1024 * 1024 // 1 MB
)

// NewServer creates *grpc.Server with optional grpc.Serveroption passed.
func NewServer(state *state.State, options ...grpc.ServerOption) *Server {
	fileCopyChunkSize := state.ServerConfig.Config.ReplicationEngineFileCopyChunkSize

	maxSize := GetMaxMessageSize(state)
	windowSize := GetInitialConnWindowSize(state)

	o := append(options,
		grpc.MaxRecvMsgSize(maxSize),
		grpc.MaxSendMsgSize(maxSize),
		grpc.InitialWindowSize(int32(windowSize)),
		grpc.InitialConnWindowSize(int32(windowSize)),
		grpc.ReadBufferSize(READ_BUFFER_SIZE),
		grpc.WriteBufferSize(WRITE_BUFFER_SIZE),
	)

	// Both FileReplicationService and ReplicationService are internal cluster services
	// that require basic auth when enabled.
	servicePrefixes := []string{"/clusterapi.FileReplicationService", "/clusterapi.ReplicationService"}
	basicAuth := state.ServerConfig.Config.Cluster.AuthConfig.BasicAuth
	if basicAuth.Enabled() {
		o = append(o, grpc.ChainUnaryInterceptor(
			basicAuthUnaryInterceptor(servicePrefixes, basicAuth.Username, basicAuth.Password),
		))
		o = append(o, grpc.ChainStreamInterceptor(
			basicAuthStreamInterceptor(servicePrefixes, basicAuth.Username, basicAuth.Password),
		))
	}
	o = append(o, grpc.ChainUnaryInterceptor(makeMaintenanceModeUnaryInterceptor(state.Cluster.MaintenanceModeEnabledForLocalhost)))
	o = append(o, grpc.ChainStreamInterceptor(makeMaintenanceModeStreamInterceptor(state.Cluster.MaintenanceModeEnabledForLocalhost)))

	replicationPrefixes := []string{"/clusterapi.ReplicationService"}
	o = append(o, grpc.ChainUnaryInterceptor(
		makeNodeReadyUnaryInterceptor(state.ClusterService.Ready, replicationPrefixes),
	))

	rqc := state.ServerConfig.Config.Cluster.RequestQueueConfig
	if rqc.QueueSize == 0 {
		rqc.QueueSize = cluster.DefaultRequestQueueSize
	}
	rq := shared.NewRequestQueue(rqc, state.Logger,
		func(item grpcQueueItem) {
			defer func() {
				if r := recover(); r != nil {
					item.result <- grpcQueueResult{nil, status.Errorf(codes.Internal, "panic in handler: %v", r)}
				}
			}()
			resp, err := item.handler(item.ctx, item.req)
			item.result <- grpcQueueResult{resp, err}
		},
		func(item grpcQueueItem) bool { return item.ctx.Err() != nil },
		func(item grpcQueueItem) {
			item.result <- grpcQueueResult{nil, status.Error(codes.DeadlineExceeded, "request expired in queue")}
		},
	)

	o = append(o, grpc.ChainUnaryInterceptor(
		makeQueueUnaryInterceptor(rq, replicationPrefixes),
	))

	s := grpc.NewServer(o...)

	weaviateV1FileReplicationService := NewFileReplicationService(state.DB, state.ClusterService.SchemaReader(), fileCopyChunkSize)
	pb.RegisterFileReplicationServiceServer(s, weaviateV1FileReplicationService)

	replicationService := NewReplicationService(state.DB)
	pb.RegisterReplicationServiceServer(s, replicationService)

	return &Server{Server: s, state: state, requestQueue: rq}
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
	if s.requestQueue != nil {
		if err := s.requestQueue.Close(ctx); err != nil {
			s.state.Logger.WithField("action", "grpc_server_close").
				WithError(err).Warn("error closing request queue")
		}
	}

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

func basicAuthUnaryInterceptor(servicePrefixes []string, expectedUsername, expectedPassword string) grpc.UnaryServerInterceptor {
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

func basicAuthStreamInterceptor(servicePrefixes []string, expectedUsername, expectedPassword string) grpc.StreamServerInterceptor {
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

func GetMaxMessageSize(state *state.State) int {
	return state.ServerConfig.Config.GRPC.MaxMsgSize
}

func GetInitialConnWindowSize(state *state.State) int {
	// ratio of 8:1 between max message size and initial connection window size is a balance between
	// throughput backpressure and memory usage. It allows for efficient streaming of large messages without overwhelming the server's memory.
	return min(state.ServerConfig.Config.GRPC.MaxMsgSize/8, math.MaxInt32)
}

func makeMaintenanceModeUnaryInterceptor(maintenanceModeEnabledForLocalhost func() bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if maintenanceModeEnabledForLocalhost() {
			return nil, status.Error(codes.FailedPrecondition, "server is in maintenance mode")
		}
		return handler(ctx, req)
	}
}

func makeMaintenanceModeStreamInterceptor(maintenanceModeEnabledForLocalhost func() bool) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if maintenanceModeEnabledForLocalhost() {
			return status.Error(codes.FailedPrecondition, "server is in maintenance mode")
		}
		return handler(srv, ss)
	}
}

func makeNodeReadyUnaryInterceptor(nodeReady func() bool, servicePrefixes []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if matchesAnyPrefix(info.FullMethod, servicePrefixes) && !nodeReady() {
			return nil, status.Error(codes.Unavailable, "node not ready")
		}
		return handler(ctx, req)
	}
}

func makeQueueUnaryInterceptor(rq *shared.RequestQueue[grpcQueueItem], prefixes []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !matchesAnyPrefix(info.FullMethod, prefixes) || !rq.Enabled() {
			return handler(ctx, req)
		}
		rq.EnsureStarted()

		resultCh := make(chan grpcQueueResult, 1)
		if err := rq.Enqueue(grpcQueueItem{ctx: ctx, req: req, info: info, handler: handler, result: resultCh}); err != nil {
			if errors.Is(err, shared.ErrQueueFull) {
				return nil, status.Error(codes.ResourceExhausted, "replication request queue full")
			}
			return nil, status.Error(codes.Unavailable, err.Error())
		}

		select {
		case result := <-resultCh:
			return result.resp, result.err
		case <-ctx.Done():
			// Context cancelled while waiting for the worker. The worker will
			// still send its result to the buffered channel (cap 1), so it
			// won't leak.
			return nil, status.Error(codes.Canceled, ctx.Err().Error())
		}
	}
}
