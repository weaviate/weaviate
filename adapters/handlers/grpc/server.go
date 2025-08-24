//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_sentry "github.com/johnbellone/grpc-middleware-sentry"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	pbv0 "github.com/weaviate/weaviate/grpc/generated/protocol/v0"
	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	authErrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Install the gzip compressor
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v0 "github.com/weaviate/weaviate/adapters/handlers/grpc/v0"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
)

// CreateGRPCServer creates *grpc.Server with optional grpc.Serveroption passed.
func CreateGRPCServer(state *state.State, options ...grpc.ServerOption) *grpc.Server {
	o := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(state.ServerConfig.Config.GRPC.MaxMsgSize),
		grpc.MaxSendMsgSize(state.ServerConfig.Config.GRPC.MaxMsgSize),
	}

	o = append(o, options...)

	// Add TLS creds for the GRPC connection, if defined.
	if len(state.ServerConfig.Config.GRPC.CertFile) > 0 || len(state.ServerConfig.Config.GRPC.KeyFile) > 0 {
		c, err := credentials.NewServerTLSFromFile(state.ServerConfig.Config.GRPC.CertFile,
			state.ServerConfig.Config.GRPC.KeyFile)
		if err != nil {
			state.Logger.WithField("action", "grpc_startup").
				Fatalf("grpc server TLS credential error: %s", err)
		}
		o = append(o, grpc.Creds(c))
	}

	var interceptors []grpc.UnaryServerInterceptor

	interceptors = append(interceptors, makeAuthInterceptor())

	// If sentry is enabled add automatic spans on gRPC requests
	if state.ServerConfig.Config.Sentry.Enabled {
		interceptors = append(interceptors, grpc_middleware.ChainUnaryServer(
			grpc_sentry.UnaryServerInterceptor(),
		))
	}

	if state.Metrics != nil {
		interceptors = append(interceptors, makeMetricsInterceptor(state.Logger, state.Metrics))
	}

	interceptors = append(interceptors, makeIPInterceptor())

	if len(interceptors) > 0 {
		o = append(o, grpc.ChainUnaryInterceptor(interceptors...))
	}

	s := grpc.NewServer(o...)
	weaviateV0 := v0.NewService()
	weaviateV1 := v1.NewService(
		state.Traverser,
		composer.New(
			state.ServerConfig.Config.Authentication,
			state.APIKey, state.OIDC),
		state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled,
		state.SchemaManager,
		state.BatchManager,
		&state.ServerConfig.Config,
		state.Authorizer,
		state.Logger,
	)
	pbv0.RegisterWeaviateServer(s, weaviateV0)
	pbv1.RegisterWeaviateServer(s, weaviateV1)
	grpc_health_v1.RegisterHealthServer(s, weaviateV1)

	return s
}

func makeMetricsInterceptor(logger logrus.FieldLogger, metrics *monitoring.PrometheusMetrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if info.FullMethod != "/weaviate.v1.Weaviate/BatchObjects" {
			return handler(ctx, req)
		}

		// For now only Batch has specific metrics (in line with http API)
		startTime := time.Now()
		reqSizeBytes := float64(proto.Size(req.(proto.Message)))
		reqSizeMB := float64(reqSizeBytes) / (1024 * 1024)
		// Invoke the handler to process the request
		resp, err := handler(ctx, req)

		// Measure duration
		duration := time.Since(startTime)

		logger.WithFields(logrus.Fields{
			"action":             "grpc_batch_objects",
			"method":             info.FullMethod,
			"request_size_bytes": reqSizeBytes,
			"duration":           duration,
		}).Debugf("grpc BatchObjects request (%fMB) took %s", reqSizeMB, duration)

		// Metric uses non-standard base unit ms, use ms for backwards compatibility
		metrics.BatchTime.WithLabelValues("total_api_level_grpc", "n/a", "n/a").
			Observe(float64(duration.Milliseconds()))
		metrics.BatchSizeBytes.WithLabelValues("grpc").Observe(reqSizeBytes)

		return resp, err
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

func makeIPInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		clientIP := getRealClientIP(ctx)

		// Add IP to context
		ctx = context.WithValue(ctx, "sourceIp", clientIP)

		return handler(ctx, req)
	}
}

func getRealClientIP(ctx context.Context) string {
	// First, check for forwarded headers in metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if xRealIP := md.Get("x-real-ip"); len(xRealIP) > 0 {
			return xRealIP[0]
		}

		if xForwardedFor := md.Get("x-forwarded-for"); len(xForwardedFor) > 0 {
			// X-Forwarded-For can contain multiple IPs, take the first one
			ips := strings.Split(xForwardedFor[0], ",")
			if len(ips) > 0 {
				return strings.TrimSpace(ips[0])
			}
		}
	}

	// Fall back to peer address
	if p, ok := peer.FromContext(ctx); ok {
		host, _, err := net.SplitHostPort(p.Addr.String())
		if err != nil {
			return convertIP6ToIP4Loopback(p.Addr.String())
		}
		return convertIP6ToIP4Loopback(host)
	}

	return "unknown"
}

func convertIP6ToIP4Loopback(ip string) string {
	if ip == "::1" {
		return "127.0.0.1" // Convert IPv6 loopback to IPv4
	}
	return ip
}

func StartAndListen(s *grpc.Server, state *state.State) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d",
		state.ServerConfig.Config.GRPC.Port))
	if err != nil {
		return err
	}
	state.Logger.WithField("action", "grpc_startup").
		Infof("grpc server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}
