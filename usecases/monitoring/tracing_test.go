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
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrusTest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/weaviate/weaviate/usecases/telemetry/opentelemetry"
)

func TestHTTPTracingMiddleware(t *testing.T) {
	_, cleanup := setupOpenTelemetryForTest(t, true)
	defer cleanup()

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
	_, cleanup := setupOpenTelemetryForTest(t, true)
	defer cleanup()

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
	_, cleanup := setupOpenTelemetryForTest(t, false)
	defer cleanup()

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
	assert.Empty(t, w.Header().Get("traceparent"))
}

func TestGRPCTracingInterceptor(t *testing.T) {
	_, cleanup := setupOpenTelemetryForTest(t, true)
	defer cleanup()

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
	_, cleanup := setupOpenTelemetryForTest(t, false)
	defer cleanup()

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
	logger, cleanup := setupOpenTelemetryForTest(t, false)
	defer cleanup()

	// Test when OpenTelemetry is disabled
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := AddTracingToHTTPMiddleware(handler, logger)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	middleware.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("traceparent"))
}

func TestAddTracingToGRPCOptions(t *testing.T) {
	logger, cleanup := setupOpenTelemetryForTest(t, false)
	defer cleanup()

	// Test when OpenTelemetry is disabled
	options := []grpc.ServerOption{}

	newOptions := AddTracingToGRPCOptions(options, logger)

	// Should return original options when tracing is disabled
	assert.Equal(t, options, newOptions)
}

// setupOpenTelemetryForTest enables/inits OpenTelemetry and returns
// a logger and a cleanup function to be called after the test.
func setupOpenTelemetryForTest(t *testing.T, enableOpenTelemetry bool) (logrus.FieldLogger, func()) {
	logger, _ := logrusTest.NewNullLogger()
	if enableOpenTelemetry {
		os.Setenv("EXPERIMENTAL_OTEL_ENABLED", "true")
	}
	err := opentelemetry.Init(logger)
	require.NoError(t, err)
	return logger, func() {
		if enableOpenTelemetry {
			os.Unsetenv("EXPERIMENTAL_OTEL_ENABLED")
		}
		// Use a context with timeout to prevent hanging on shutdown
		// The provider's Shutdown has a 30s timeout, but we use a shorter timeout
		// to fail fast since there's not exporter in this test
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		opentelemetry.Shutdown(shutdownCtx)
	}
}
