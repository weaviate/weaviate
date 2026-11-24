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
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/test/bufconn"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/sentry"
	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestMakeClientIdentifierInterceptor(t *testing.T) {
	interceptor := makeClientIdentifierInterceptor()

	tests := []struct {
		name        string
		headerValue string
	}{
		{
			name:        "python client",
			headerValue: "weaviate-client-python/4.10.0",
		},
		{
			name:        "go client",
			headerValue: "weaviate-client-go/2.5.0",
		},
		{
			name:        "no header",
			headerValue: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.headerValue != "" {
				md := metadata.Pairs("x-weaviate-client", tt.headerValue)
				ctx = metadata.NewIncomingContext(ctx, md)
			}

			var capturedCtx context.Context
			handler := func(ctx context.Context, req any) (any, error) {
				capturedCtx = ctx
				return nil, nil
			}

			_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
			assert.NoError(t, err)

			if tt.headerValue != "" {
				assert.Equal(t, tt.headerValue, capturedCtx.Value("clientIdentifier"))
			} else {
				assert.Nil(t, capturedCtx.Value("clientIdentifier"))
			}
		})
	}
}

// newGRPCBatchMetrics builds the two vecs makeMetricsInterceptor writes,
// standalone so a subtest cannot see another's samples.
func newGRPCBatchMetrics(group bool) *monitoring.PrometheusMetrics {
	return &monitoring.PrometheusMetrics{
		Group: group,
		BatchTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "batch_durations_ms",
		}, []string{"operation", "class_name", "shard_name"}),
		BatchSizeBytes: prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Name: "batch_size_bytes",
		}, []string{"api", "collection_namespace"}),
	}
}

func grpcBatchCount(t *testing.T, metrics *monitoring.PrometheusMetrics, namespace string) uint64 {
	t.Helper()
	obs, err := metrics.BatchSizeBytes.GetMetricWithLabelValues("grpc", namespace)
	require.NoError(t, err)
	var m dto.Metric
	require.NoError(t, obs.(prometheus.Metric).Write(&m))
	return m.GetSummary().GetSampleCount()
}

func TestMakeMetricsInterceptor(t *testing.T) {
	logger, _ := test.NewNullLogger()
	batchInfo := &grpc.UnaryServerInfo{FullMethod: "/weaviate.v1.Weaviate/BatchObjects"}

	// The real handler resolves the principal and writes the slot; this stands
	// in for it.
	settingHandler := func(namespace string, err error) grpc.UnaryHandler {
		return func(ctx context.Context, req any) (any, error) {
			restCtx.SetBatchNamespace(ctx, namespace)
			return &pbv1.BatchObjectsReply{}, err
		}
	}

	t.Run("BatchObjects is observed with the handler's namespace", func(t *testing.T) {
		metrics := newGRPCBatchMetrics(false)

		_, err := makeMetricsInterceptor(logger, metrics)(context.Background(),
			&pbv1.BatchObjectsRequest{}, batchInfo, settingHandler("ns_a", nil))

		require.NoError(t, err)
		assert.Equal(t, uint64(1), grpcBatchCount(t, metrics, "ns_a"))
		assert.Zero(t, grpcBatchCount(t, metrics, ""),
			"the namespaced sample must not also land on the empty label")
	})

	t.Run("handler error is still observed", func(t *testing.T) {
		metrics := newGRPCBatchMetrics(false)
		handlerErr := errors.New("batch failed")

		_, err := makeMetricsInterceptor(logger, metrics)(context.Background(),
			&pbv1.BatchObjectsRequest{}, batchInfo, settingHandler("ns_a", handlerErr))

		require.ErrorIs(t, err, handlerErr)
		assert.Equal(t, uint64(1), grpcBatchCount(t, metrics, "ns_a"))
	})

	t.Run("handler that sets no namespace yields empty label", func(t *testing.T) {
		metrics := newGRPCBatchMetrics(false)

		_, err := makeMetricsInterceptor(logger, metrics)(context.Background(),
			&pbv1.BatchObjectsRequest{}, batchInfo, settingHandler("", nil))

		require.NoError(t, err)
		assert.Equal(t, uint64(1), grpcBatchCount(t, metrics, ""))
	})

	t.Run("grouped mode carries the handler's namespace", func(t *testing.T) {
		metrics := newGRPCBatchMetrics(true)

		_, err := makeMetricsInterceptor(logger, metrics)(context.Background(),
			&pbv1.BatchObjectsRequest{}, batchInfo, settingHandler("ns_a", nil))

		require.NoError(t, err)
		assert.Equal(t, uint64(1), grpcBatchCount(t, metrics, "ns_a"),
			"grouping collapses classes, never principals")
		assert.Zero(t, grpcBatchCount(t, metrics, ""))
	})

	t.Run("other methods are untouched", func(t *testing.T) {
		metrics := newGRPCBatchMetrics(false)

		_, err := makeMetricsInterceptor(logger, metrics)(context.Background(),
			&pbv1.BatchObjectsRequest{},
			&grpc.UnaryServerInfo{FullMethod: "/weaviate.v1.Weaviate/BatchStream"},
			settingHandler("ns_a", nil))

		require.NoError(t, err)
		assert.Zero(t, grpcBatchCount(t, metrics, "ns_a"))
		assert.Zero(t, grpcBatchCount(t, metrics, ""))
	})
}

// Test operational mode interceptor.
func TestInterceptors(t *testing.T) {
	const bufSize = 1024 * 1024

	// Start gRPC server.
	defaultScheme := resolver.GetDefaultScheme()
	t.Cleanup(func() { resolver.SetDefaultScheme(defaultScheme) })

	// grpc.Client defaults to "dns" scheme, but we want to effectively
	// disable this altogether and use our custom dialer instead.
	resolver.SetDefaultScheme("passthrough")

	// Run gRPC Server.
	logger, _ := test.NewNullLogger()
	s := state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				GRPC: config.GRPC{
					MaxMsgSize: bufSize,
				},
				Sentry: &sentry.ConfigOpts{
					Enabled: false,
				},
				Authentication: config.Authentication{
					AnonymousAccess: config.AnonymousAccess{
						Enabled: true,
					},
				},
				OperationalMode: runtime.NewDynamicValue[string](config.READ_WRITE),
			},
		},
		Cluster: &cluster.State{},
		Logger:  logger,
	}

	srv, stop := CreateGRPCServer(&s, nil, nil)
	t.Cleanup(stop)

	// bufconn Listener mocks network connections using an in-memory buffer.
	lis := bufconn.Listen(bufSize)
	go func() {
		defer lis.Close()
		require.NoError(t, srv.Serve(lis), "server exited with an error")
	}()

	// Create gRPC client.
	conn, err := grpc.NewClient("in-memory",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	require.NoError(t, err, "create gRPC client")
	t.Cleanup(func() { _ = conn.Close() })

	client := pbv1.NewWeaviateClient(conn)

	for _, tt := range []struct {
		mode string // Server's operational mode.
		err  error  // Expected RPC error.
	}{
		{
			mode: config.READ_WRITE,
			err:  nil,
		},
		{
			mode: config.WRITE_ONLY,
			err:  nil,
		},
		{
			mode: config.READ_ONLY,
			err:  config.ErrReadOnlyModeEnabled,
		},
		{
			mode: config.SCALE_OUT,
			err:  config.ErrScaleOutModeEnabled,
		},
	} {
		t.Run("operational mode="+tt.mode, func(t *testing.T) {
			mode := s.ServerConfig.Config.OperationalMode.Get()
			s.ServerConfig.Config.OperationalMode.SetValue(tt.mode)
			t.Cleanup(func() { s.ServerConfig.Config.OperationalMode.SetValue(mode) })

			// Run unary RPC.
			_, err = client.BatchObjects(t.Context(), new(pbv1.BatchObjectsRequest))

			if tt.err != nil {
				require.ErrorContains(t, err, tt.err.Error(), "interceptor error")
			} else {
				require.NoError(t, err, "interceptor error")
			}
		})
	}
}
