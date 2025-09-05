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
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
)

func TestHTTPTracingMiddleware(t *testing.T) {
	// Initialize OpenTelemetry for testing
	logger := logrus.New()
	err := opentelemetry.Init(logger)
	require.NoError(t, err)
	defer opentelemetry.Shutdown(context.Background())

	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test response"))
	})

	// Create middleware
	middleware := HTTPTracingMiddleware(testHandler)

	// Create test request
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("User-Agent", "test-agent")
	req.Header.Set("X-Request-ID", "test-request-id")

	// Create response recorder
	w := httptest.NewRecorder()

	// Execute request
	middleware.ServeHTTP(w, req)

	// Verify response
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "test response", w.Body.String())

	// Verify trace context was injected in response headers
	assert.NotEmpty(t, w.Header().Get("traceparent"))
}

func TestHTTPTracingMiddlewareWithTraceContext(t *testing.T) {
	// Initialize OpenTelemetry for testing
	logger := logrus.New()
	err := opentelemetry.Init(logger)
	require.NoError(t, err)
	defer opentelemetry.Shutdown(context.Background())

	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify trace context is available in handler
		span := trace.SpanFromContext(r.Context())
		assert.True(t, span.SpanContext().IsValid())
		w.WriteHeader(http.StatusOK)
	})

	// Create middleware
	middleware := HTTPTracingMiddleware(testHandler)

	// Create test request with trace context
	req := httptest.NewRequest("POST", "/test", nil)
	req.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")

	// Create response recorder
	w := httptest.NewRecorder()

	// Execute request
	middleware.ServeHTTP(w, req)

	// Verify response
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHTTPTracingMiddlewareDisabled(t *testing.T) {
	// Test when OpenTelemetry is disabled
	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test response"))
	})

	// Create middleware
	middleware := HTTPTracingMiddleware(testHandler)

	// Create test request
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	// Execute request
	middleware.ServeHTTP(w, req)

	// Verify response (should work normally when tracing is disabled)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "test response", w.Body.String())
}

func TestGRPCTracingInterceptor(t *testing.T) {
	// Initialize OpenTelemetry for testing
	logger := logrus.New()
	err := opentelemetry.Init(logger)
	require.NoError(t, err)
	defer opentelemetry.Shutdown(context.Background())

	// Create interceptor
	interceptor := GRPCTracingInterceptor()

	// Create test handler
	testHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		// Verify trace context is available in handler
		span := trace.SpanFromContext(ctx)
		assert.True(t, span.SpanContext().IsValid())
		return "test response", nil
	}

	// Create test context with metadata
	ctx := context.Background()
	md := metadata.New(map[string]string{
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	})
	ctx = metadata.NewIncomingContext(ctx, md)

	// Create gRPC info
	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/TestMethod",
	}

	// Execute interceptor
	resp, err := interceptor(ctx, "test request", info, testHandler)

	// Verify response
	assert.NoError(t, err)
	assert.Equal(t, "test response", resp)
}

func TestGRPCTracingInterceptorDisabled(t *testing.T) {
	// Test when OpenTelemetry is disabled
	// Create interceptor
	interceptor := GRPCTracingInterceptor()

	// Create test handler
	testHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return "test response", nil
	}

	// Create test context
	ctx := context.Background()
	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.Service/TestMethod",
	}

	// Execute interceptor
	resp, err := interceptor(ctx, "test request", info, testHandler)

	// Verify response (should work normally when tracing is disabled)
	assert.NoError(t, err)
	assert.Equal(t, "test response", resp)
}

func TestAddTracingToHTTPMiddleware(t *testing.T) {
	logger := logrus.New()

	// Test when OpenTelemetry is disabled
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := AddTracingToHTTPMiddleware(handler, logger)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	middleware.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAddTracingToGRPCOptions(t *testing.T) {
	logger := logrus.New()

	// Test when OpenTelemetry is disabled
	options := []grpc.ServerOption{}

	newOptions := AddTracingToGRPCOptions(options, logger)

	// Should return original options when tracing is disabled
	assert.Equal(t, options, newOptions)
}
