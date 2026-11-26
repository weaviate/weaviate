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
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/peer"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	pbv0 "github.com/weaviate/weaviate/grpc/generated/protocol/v0"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	authErrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/restrictions"
	"github.com/weaviate/weaviate/usecases/telemetry"
	"github.com/weaviate/weaviate/usecases/usagelimits"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Install the gzip compressor
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v0 "github.com/weaviate/weaviate/adapters/handlers/grpc/v0"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/auth"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

type Config struct {
	ClientTracker      *telemetry.ClientTracker
	IntegrationTracker *telemetry.IntegrationTracker
	Registerer         prometheus.Registerer
}

// CreateGRPCServer creates *grpc.Server with optional grpc.Serveroption passed.
// clientTracker is optional; when non-nil, a gRPC interceptor is added to track client SDK usage.
func CreateGRPCServer(state *state.State, conf Config, options ...grpc.ServerOption) (*grpc.Server, batch.Drain) {
	grpcConf := state.ServerConfig.Config.GRPC
	o := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(grpcConf.MaxMsgSize),
		grpc.MaxSendMsgSize(grpcConf.MaxMsgSize),
	}

	o = append(o, options...)

	// Add TLS creds for the GRPC connection, if defined.
	if len(grpcConf.CertFile) > 0 || len(grpcConf.KeyFile) > 0 {
		c, err := credentials.NewServerTLSFromFile(grpcConf.CertFile,
			grpcConf.KeyFile)
		if err != nil {
			state.Logger.WithField("action", "grpc_startup").
				Fatalf("grpc server TLS credential error: %s", err)
		}
		o = append(o, grpc.Creds(c))
	}

	var unaryInterceptors []grpc.UnaryServerInterceptor
	unaryInterceptors = append(unaryInterceptors, makeAuthInterceptor())

	if state.Metrics != nil {
		unaryInterceptors = append(unaryInterceptors, makeMetricsInterceptor(state.Logger, state.Metrics))
	}

	unaryInterceptors = append(unaryInterceptors, makeIPInterceptor())

	// TODO(dyma): hide this logic within the interceptor factory.
	// FIXME(dyma): the first branch may be using nil IntegrationTracker.
	if conf.ClientTracker != nil {
		// ClientTrackingUnaryInterceptor both tracks usage and sets "clientIdentifier" in context,
		// so no need for a separate makeClientIdentifierInterceptor.
		unaryInterceptors = append(unaryInterceptors, telemetry.ClientTrackingUnaryInterceptor(conf.ClientTracker, conf.IntegrationTracker))
	} else {
		unaryInterceptors = append(unaryInterceptors, makeClientIdentifierInterceptor())
	}

	unaryInterceptors = append(unaryInterceptors, serverStateUnaryServerInterceptor((*serverState)(state)))

	// Add OpenTelemetry tracing interceptors
	unaryInterceptors = append(unaryInterceptors, monitoring.GRPCTracingInterceptor())
	unaryInterceptors = append(unaryInterceptors, makeDocsLinkUnaryInterceptor())

	// TODO(dyma): redundant check, ChainUnaryInterceptor/ChainServerInterceptor already do that
	if len(unaryInterceptors) > 0 {
		o = append(o, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	}

	allowAnonymous := state.ServerConfig.Config.Authentication.AnonymousAccess.Enabled
	authComposer := composer.New(
		state.ServerConfig.Config.Authentication,
		state.ServerConfig.Config.Namespaces.Enabled,
		state.Logger,
		state.APIKey,
		state.OIDC,
	)

	o = append(o, grpc.ChainStreamInterceptor(makeAuthStreamInterceptor(auth.NewHandler(allowAnonymous, authComposer))))
	o = append(o, grpc.ChainStreamInterceptor(serverStateStreamServerInterceptor((*serverState)(state))))
	o = append(o, grpc.ChainStreamInterceptor(makeDocsLinkStreamInterceptor()))

	s := grpc.NewServer(o...)
	weaviateV0 := v0.NewService()
	weaviateV1, drainBatch := v1.NewService(state, v1.ServiceConfig{
		AllowAnonymous: allowAnonymous,
		AuthComposer:   authComposer,
		Registerer:     conf.Registerer,
	})
	pbv0.RegisterWeaviateServer(s, weaviateV0)
	pbv1.RegisterWeaviateServer(s, weaviateV1)

	grpc_health_v1.RegisterHealthServer(s, weaviateV1)

	return s, drainBatch
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
		ctx, batchNamespace := restCtx.WithBatchNamespaceSlot(ctx)
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
		metrics.BatchSizeBytes.WithLabelValues("grpc", batchNamespace.Namespace).Observe(reqSizeBytes)

		return resp, err
	}
}

// translateTypedError maps Weaviate's typed errors (auth, usage limits)
// to gRPC statuses. Returns nil when no mapping applies so callers can
// fall through. Shared by the unary and stream interceptors.
func translateTypedError(err error) error {
	if err == nil {
		return nil
	}
	if errors.As(err, &authErrs.Unauthenticated{}) {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if errors.As(err, &authErrs.Forbidden{}) {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	if le, ok := usagelimits.AsLimitExceeded(err); ok {
		return limitExceededToGrpcError(le)
	}
	if v, ok := restrictions.AsViolation(err); ok {
		return restrictionViolationToGrpcError(v)
	}
	return nil
}

// restrictionViolationToGrpcError maps a ViolationError to
// FailedPrecondition + ErrorInfo metadata mirroring the REST 422 body.
func restrictionViolationToGrpcError(v *restrictions.ViolationError) error {
	st := status.New(codes.FailedPrecondition, v.Error())
	withDetails, derr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: restrictions.ErrorCode,
		Domain: "weaviate.restrictions",
		Metadata: map[string]string{
			"restriction": string(v.Restriction),
			"value":       v.Value,
			"allowed":     strings.Join(v.Allowed, ","),
			"message":     v.RenderedMessage,
		},
	})
	if derr != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// FIXME(dyma): this shouldn't be called "auth interceptor", because it maps error types,
// and not just those related to auth.
func makeAuthInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)
		if translated := translateTypedError(err); translated != nil {
			return nil, translated
		}
		return resp, err
	}
}

// limitExceededToGrpcError converts a *usagelimits.LimitExceededError into
// a gRPC status with codes.ResourceExhausted and an attached
// errdetails.ErrorInfo proto carrying the structured fields that mirror the
// REST 429 body. SDKs may match on the gRPC code alone, or read the details
// payload for the same `errorCode`/`limit`/`value`/`message` fields they'd
// see over REST.
func limitExceededToGrpcError(le *usagelimits.LimitExceededError) error {
	st := status.New(codes.ResourceExhausted, le.Error())
	withDetails, derr := st.WithDetails(&errdetails.ErrorInfo{
		Reason: usagelimits.ErrorCode,
		Domain: "weaviate.usagelimits",
		Metadata: map[string]string{
			"limit":   string(le.Limit),
			"value":   strconv.FormatInt(le.Value, 10),
			"message": le.RenderedMessage,
		},
	})
	if derr != nil {
		// Fall back to the bare status if attaching details fails for any
		// reason (e.g. proto marshaling). The status code + message is
		// still a valid contract; we just lose the structured payload.
		return st.Err()
	}
	return withDetails.Err()
}

func makeAuthStreamInterceptor(auth *auth.Handler) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if _, err := auth.PrincipalFromContext(ss.Context()); err != nil {
			return status.Error(codes.Unauthenticated, err.Error())
		}
		// Mirror makeAuthInterceptor so streams get the same wire contract.
		err := handler(srv, ss)
		// FIXME(dyma): this doesn't need to return nil "so callers can fallthrough".
		// That's nonsence, it should just return the original error if it cannot be
		// mapped.
		if translated := translateTypedError(err); translated != nil {
			return translated
		}
		return err
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

func makeClientIdentifierInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			if vals := md.Get("x-weaviate-client"); len(vals) > 0 {
				ctx = context.WithValue(ctx, "clientIdentifier", telemetry.SanitizeClientHeader(vals[0]))
			}
		}
		return handler(ctx, req)
	}
}

// ----------------------------------------------------------------------------
// Operational + maintainance mode
// ----------------------------------------------------------------------------

type serverState state.State

// admit returns a non-nil error if the server cannot process the request.
func (s *serverState) admit(method string) error {
	if s.Cluster.MaintenanceModeEnabledForLocalhost() {
		return status.Error(codes.Unavailable, "server is in maintenance mode")
	}

	var err error
	switch method {

	// Healthcheck request.
	case grpc_health_v1.Health_Check_FullMethodName,
		grpc_health_v1.Health_Watch_FullMethodName,
		grpc_health_v1.Health_List_FullMethodName:

	// Read request.
	case protocol.Weaviate_Search_FullMethodName,
		protocol.Weaviate_Aggregate_FullMethodName,
		protocol.Weaviate_TenantsGet_FullMethodName:

		if s.ServerConfig.Config.OperationalMode.Get() == config.WRITE_ONLY {
			err = config.ErrWriteOnlyModeEnabled
		}

	// Write request.
	case protocol.Weaviate_BatchObjects_FullMethodName,
		protocol.Weaviate_BatchStream_FullMethodName,
		protocol.Weaviate_BatchReferences_FullMethodName,
		protocol.Weaviate_BatchDelete_FullMethodName:

		switch s.ServerConfig.Config.OperationalMode.Get() {
		case config.READ_ONLY:
			err = config.ErrReadOnlyModeEnabled
		case config.SCALE_OUT:
			err = config.ErrScaleOutModeEnabled
		}
	default:
		panic(method + " not handled by interceptor")
	}
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	return nil
}

// serverStateUnaryServerInterceptor rejects requests cannot be serviced in the current state.
func serverStateUnaryServerInterceptor(s *serverState) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if err := s.admit(info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// serverStateStreamServerInterceptor rejects requests cannot be serviced in the current state.
func serverStateStreamServerInterceptor(s *serverState) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := s.admit(info.FullMethod); err != nil {
			return err
		}
		return handler(srv, &serverStateStream{ServerStream: ss, info: *info, state: s})
	}
}

// serverStateStream checks [serverState.admit] for each incoming message.
type serverStateStream struct {
	grpc.ServerStream                       // Delegate stream handler.
	info              grpc.StreamServerInfo // Stream info.
	state             *serverState          // Server state.
}

func (s *serverStateStream) RecvMsg(m any) error {
	if err := s.state.admit(s.info.FullMethod); err != nil {
		return err
	}
	return s.ServerStream.RecvMsg(m)
}

// ----------------------------------------------------------------------------
// Documentation links for errors
// ----------------------------------------------------------------------------

// makeDocsLinkUnaryInterceptor appends the documenting page to the message of
// a documented error, keeping the status code the handler chose.
func makeDocsLinkUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		return resp, withDocsLink(err)
	}
}

func makeDocsLinkStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return withDocsLink(handler(srv, ss))
	}
}

func withDocsLink(err error) error {
	// NOTE(dyma): MessageWithDocsLink will check Documented too, maybe there isn't a need to do it twice.
	if _, ok := enterrors.Documented(err); !ok {
		return err
	}
	if st, isStatus := status.FromError(err); isStatus {
		// Clone and replace only the message, so the status keeps its code
		// and its protobuf details.
		p := proto.Clone(st.Proto()).(*spb.Status)
		p.Message = enterrors.MessageWithDocsLink(err)
		return status.ErrorProto(p)
	}
	// A plain error becomes codes.Unknown, which is what grpc does with it anyway.
	return status.Error(codes.Unknown, enterrors.MessageWithDocsLink(err))
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
