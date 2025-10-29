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

package monitoring

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
)

// HTTPTracingMiddleware creates a middleware that adds OpenTelemetry tracing to HTTP requests
func HTTPTracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !opentelemetry.IsEnabled() {
			next.ServeHTTP(w, r)
			return
		}

		// Extract trace context from headers
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagationHeaderCarrier(r.Header))

		// Create span
		spanName := r.Method + " " + r.URL.Path
		ctx, span := otel.Tracer("weaviate-http").Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.user_agent", r.UserAgent()),
				attribute.String("http.request_id", r.Header.Get("X-Request-ID")),
			),
		)
		defer span.End()

		// Add trace context to response headers
		otel.GetTextMapPropagator().Inject(ctx, propagationHeaderCarrier(w.Header()))

		// Create a response writer that captures status code
		wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Execute the request
		start := time.Now()
		next.ServeHTTP(wrappedWriter, r.WithContext(ctx))
		duration := time.Since(start)

		// Set span attributes based on response
		span.SetAttributes(
			attribute.Int("http.status_code", wrappedWriter.statusCode),
			attribute.Int64("http.response_size", wrappedWriter.size),
			attribute.Int64("http.duration_ms", duration.Milliseconds()),
		)

		// Set span status based on HTTP status code
		if wrappedWriter.statusCode >= 400 {
			span.SetStatus(codes.Error, "HTTP "+strconv.Itoa(wrappedWriter.statusCode))
		} else {
			span.SetStatus(codes.Ok, "HTTP OK")
		}

		// Add events for important milestones
		span.AddEvent("http.request.completed",
			trace.WithAttributes(
				attribute.Int64("duration_ms", duration.Milliseconds()),
				attribute.Int("status_code", wrappedWriter.statusCode),
			),
		)
	})
}

// GRPCTracingInterceptor creates a gRPC interceptor that adds OpenTelemetry tracing
func GRPCTracingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !opentelemetry.IsEnabled() {
			return handler(ctx, req)
		}

		// Extract trace context from metadata
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			ctx = otel.GetTextMapPropagator().Extract(ctx, propagationMetadataCarrier(md))
		}

		// Create span
		spanName := info.FullMethod
		ctx, span := otel.Tracer("weaviate-grpc").Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.method", info.FullMethod),
			),
		)
		defer span.End()

		// Add trace context to outgoing metadata
		md = metadata.New(nil)
		otel.GetTextMapPropagator().Inject(ctx, propagationMetadataCarrier(md))
		ctx = metadata.NewOutgoingContext(ctx, md)

		// Execute the request
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start)

		// Set span attributes based on response
		span.SetAttributes(
			attribute.Int64("rpc.duration_ms", duration.Milliseconds()),
		)

		// Set span status based on gRPC status
		if err != nil {
			st := status.Convert(err)
			span.SetStatus(codes.Error, st.Message())
			span.SetAttributes(
				attribute.String("rpc.grpc.status_code", st.Code().String()),
			)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		// Add events for important milestones
		span.AddEvent("rpc.request.completed",
			trace.WithAttributes(
				attribute.Int64("duration_ms", duration.Milliseconds()),
			),
		)

		// Add debug trace context to outgoing metadata for debugging (optional)
		outMD := metadata.New(map[string]string{"x-trace-id": span.SpanContext().TraceID().String()})
		if err := grpc.SetTrailer(ctx, outMD); err != nil {
			span.RecordError(err)
		}

		return resp, err
	}
}

// GRPCStreamTracingInterceptor creates a gRPC stream interceptor that adds OpenTelemetry tracing
func GRPCStreamTracingInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !opentelemetry.IsEnabled() {
			return handler(srv, ss)
		}

		// Extract trace context from metadata
		ctx := ss.Context()
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			ctx = otel.GetTextMapPropagator().Extract(ctx, propagationMetadataCarrier(md))
		}

		// Create span
		spanName := info.FullMethod
		ctx, span := otel.Tracer("weaviate-grpc-stream").Start(ctx, spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("rpc.system", "grpc"),
				attribute.String("rpc.method", info.FullMethod),
				attribute.Bool("rpc.stream", true),
			),
		)
		defer span.End()

		// Create wrapped stream
		wrappedStream := &tracingServerStream{
			ServerStream: ss,
			ctx:          ctx,
			span:         span,
		}

		// Execute the stream
		start := time.Now()
		err := handler(srv, wrappedStream)
		duration := time.Since(start)

		// Set span attributes based on response
		span.SetAttributes(
			attribute.Int64("rpc.duration_ms", duration.Milliseconds()),
		)

		// Set span status based on gRPC status
		if err != nil {
			st := status.Convert(err)
			span.SetStatus(codes.Error, st.Message())
			span.SetAttributes(
				attribute.String("rpc.grpc.status_code", st.Code().String()),
			)
		} else {
			span.SetStatus(codes.Ok, "")
		}

		// Add events for important milestones
		span.AddEvent("rpc.stream.completed",
			trace.WithAttributes(
				attribute.Int64("duration_ms", duration.Milliseconds()),
			),
		)

		return err
	}
}

// responseWriter wraps http.ResponseWriter to capture status code and size
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	size       int64
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	size, err := rw.ResponseWriter.Write(b)
	rw.size += int64(size)
	return size, err
}

// tracingServerStream wraps grpc.ServerStream to add tracing context
type tracingServerStream struct {
	grpc.ServerStream
	ctx  context.Context
	span trace.Span
}

func (tss *tracingServerStream) Context() context.Context {
	return tss.ctx
}

// propagationHeaderCarrier implements propagation.TextMapCarrier for HTTP headers
type propagationHeaderCarrier http.Header

func (c propagationHeaderCarrier) Get(key string) string {
	return http.Header(c).Get(key)
}

func (c propagationHeaderCarrier) Set(key, value string) {
	http.Header(c).Set(key, value)
}

func (c propagationHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// propagationMetadataCarrier implements propagation.TextMapCarrier for gRPC metadata
type propagationMetadataCarrier metadata.MD

func (c propagationMetadataCarrier) Get(key string) string {
	values := metadata.MD(c).Get(key)
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

func (c propagationMetadataCarrier) Set(key, value string) {
	metadata.MD(c).Set(key, value)
}

func (c propagationMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// AddTracingToHTTPMiddleware adds tracing to the existing HTTP middleware chain
func AddTracingToHTTPMiddleware(next http.Handler, logger logrus.FieldLogger) http.Handler {
	if !opentelemetry.IsEnabled() {
		logger.Debug("OpenTelemetry tracing disabled, skipping HTTP tracing middleware")
		return next
	}

	logger.Info("Adding OpenTelemetry HTTP tracing middleware")
	return HTTPTracingMiddleware(next)
}

// NewTracingTransport creates an HTTP transport that injects OpenTelemetry trace context
func NewTracingTransport(base http.RoundTripper) http.RoundTripper {
	return &tracingTransport{base: base}
}

type tracingTransport struct {
	base http.RoundTripper
}

func (t *tracingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !opentelemetry.IsEnabled() {
		return t.base.RoundTrip(req)
	}

	// TODO do we want to track client vs server on these? add if we want both send/receive side
	// Start client span
	// ctx, span := otel.Tracer("weaviate-http-client").Start(req.Context(), req.Method+" "+req.URL.Path,
	// 	trace.WithSpanKind(trace.SpanKindClient),
	// 	trace.WithAttributes(
	// 		attribute.String("http.method", req.Method),
	// 		attribute.String("http.url", req.URL.String()),
	// 		attribute.String("net.peer.name", req.URL.Hostname()),
	// 		attribute.String("http.target", req.URL.Path),
	// 	),
	// )
	// defer span.End()

	// Inject trace context into headers
	ctx := req.Context()
	propagator := otel.GetTextMapPropagator()
	carrier := propagationHeaderCarrier(req.Header)
	propagator.Inject(ctx, carrier)

	// Execute the request
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	if err != nil {
		// span.SetStatus(codes.Error, err.Error())
		return resp, err
	}

	// Set span attributes based on response
	// span.SetAttributes(attribute.Int("http.status_code", resp.StatusCode))
	// if resp.StatusCode >= 400 {
	// 	span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", resp.StatusCode))
	// } else {
	// 	span.SetStatus(codes.Ok, "")
	// }

	return resp, err
}

// AddTracingToGRPCOptions adds tracing interceptors to gRPC server options
func AddTracingToGRPCOptions(options []grpc.ServerOption, logger logrus.FieldLogger) []grpc.ServerOption {
	if !opentelemetry.IsEnabled() {
		logger.Debug("OpenTelemetry tracing disabled, skipping gRPC tracing interceptors")
		return options
	}

	logger.Info("Adding OpenTelemetry gRPC tracing interceptors")

	// Add unary interceptor
	options = append(options, grpc.UnaryInterceptor(GRPCTracingInterceptor()))

	// Add stream interceptor
	options = append(options, grpc.StreamInterceptor(GRPCStreamTracingInterceptor()))

	return options
}
