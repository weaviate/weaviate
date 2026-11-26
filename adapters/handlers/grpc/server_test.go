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
	"io"
	"net"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// func TestMakeClientIdentifierInterceptor(t *testing.T) {
// 	interceptor := makeClientIdentifierInterceptor()
//
// 	tests := []struct {
// 		name        string
// 		headerValue string
// 	}{
// 		{
// 			name:        "python client",
// 			headerValue: "weaviate-client-python/4.10.0",
// 		},
// 		{
// 			name:        "go client",
// 			headerValue: "weaviate-client-go/2.5.0",
// 		},
// 		{
// 			name:        "no header",
// 			headerValue: "",
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			ctx := context.Background()
// 			if tt.headerValue != "" {
// 				md := metadata.Pairs("x-weaviate-client", tt.headerValue)
// 				ctx = metadata.NewIncomingContext(ctx, md)
// 			}
//
// 			var capturedCtx context.Context
// 			handler := func(ctx context.Context, req any) (any, error) {
// 				capturedCtx = ctx
// 				return nil, nil
// 			}
//
// 			_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
// 			assert.NoError(t, err)
//
// 			if tt.headerValue != "" {
// 				assert.Equal(t, tt.headerValue, capturedCtx.Value("clientIdentifier"))
// 			} else {
// 				assert.Nil(t, capturedCtx.Value("clientIdentifier"))
// 			}
// 		})
// 	}
// }

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

type rpcDesc struct {
	method string                                      // Method name.
	fn     func(*testing.T, pbv1.WeaviateClient) error // Request function.
}

var (
	batchObjects = rpcDesc{
		method: "BatchObjects", fn: func(t *testing.T, c pbv1.WeaviateClient) error {
			_, err := c.BatchObjects(t.Context(), new(pbv1.BatchObjectsRequest))
			return err
		},
	}
	batchStream = rpcDesc{
		method: "BatchStream", fn: func(t *testing.T, c pbv1.WeaviateClient) error {
			// The stream handle is returned as long as all configuration is valid.
			// The server might've rejected any number of requests in the meantime,
			// but we need to read them from s.Recv().
			s, err := c.BatchStream(t.Context())
			require.NoError(t, err, "create batch stream")
			require.NotNil(t, s, "nil batch stream")

			require.NoError(t, s.Send(&pbv1.BatchStreamRequest{
				Message: &pbv1.BatchStreamRequest_Start_{},
			}), "send Start message")

			// Closing the batch stream means Recv() won't block
			// in cases where we expect the request to succeed,
			// because then it can return at least return io.EOF.
			require.NoError(t, s.CloseSend(), "close batch stream")

			if _, err = s.Recv(); errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		},
	}
)

func TestMaintenanceModeInterceptor(t *testing.T) {
	serverState := state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				Authentication: config.DefaultAuthentication, // Enabled anonymous access.
				Cluster: cluster.Config{
					Hostname: "node-0",
				},
			},
		},
	}
	conn := startTestServer(t, &serverState)
	client := pbv1.NewWeaviateClient(conn)

	for _, maintenance := range []bool{true, false} {
		t.Run(fmt.Sprintf("maintenance mode %t", maintenance), func(t *testing.T) {
			serverState.Cluster.SetMaintenanceModeForLocalhost(maintenance)

			for _, tt := range []rpcDesc{
				batchObjects,
				batchStream,
			} {
				t.Run(tt.method, func(t *testing.T) {
					err := tt.fn(t, client)

					if maintenance {
						require.ErrorContains(t, err, "maintenance mode", "interceptor error")
					} else {
						require.NoError(t, err, "interceptor error")
					}
				})
			}
		})
	}
}

func TestOperationalModeInterceptor(t *testing.T) {
	serverState := state.State{
		ServerConfig: &config.WeaviateConfig{
			Config: config.Config{
				OperationalMode: runtime.NewDynamicValue[string](config.READ_WRITE),
				Authentication:  config.DefaultAuthentication, // Enable anonymous access.
			},
		},
	}
	conn := startTestServer(t, &serverState)
	client := pbv1.NewWeaviateClient(conn)

	for _, tt := range []struct {
		mode string // Server's operational mode.
		err  error  // Expected RPC error.
	}{
		{mode: config.READ_WRITE, err: nil},
		{mode: config.WRITE_ONLY, err: nil},
		{mode: config.READ_ONLY, err: config.ErrReadOnlyModeEnabled},
		{mode: config.SCALE_OUT, err: config.ErrScaleOutModeEnabled},
	} {
		t.Run("mode="+tt.mode, func(t *testing.T) {
			// Put server in the expected mode.
			mode := serverState.ServerConfig.Config.OperationalMode.Get()
			serverState.ServerConfig.Config.OperationalMode.SetValue(tt.mode)
			t.Cleanup(func() { serverState.ServerConfig.Config.OperationalMode.SetValue(mode) })

			for _, rpc := range []rpcDesc{
				batchObjects,
				batchStream,
			} {
				t.Run(rpc.method, func(t *testing.T) {
					err := rpc.fn(t, client)

					if tt.err != nil {
						require.ErrorContains(t, err, tt.err.Error(), "interceptor error")
					} else {
						require.NoError(t, err, "interceptor error")
					}
				})
			}
		})
	}
}

// startTestServer creates a gRPC server backed by an in-memory buffer
// in place of a network. The function will add any necessary state
// configuration except for Authentication, which the caller must configure
// on the provided state s by itself.
// Returns a client connection for this server. Frees resources on test cleanup.
func startTestServer(t *testing.T, s *state.State) grpc.ClientConnInterface {
	t.Helper()

	// Size of the underlying request buffer.
	bufSize := max(s.ServerConfig.Config.GRPC.MaxMsgSize, 256)

	// grpc.Client defaults to "dns" scheme, but we want to effectively
	// disable this altogether and use our custom dialer instead.
	defaultScheme := resolver.GetDefaultScheme()
	resolver.SetDefaultScheme("passthrough")
	t.Cleanup(func() { resolver.SetDefaultScheme(defaultScheme) })

	// Run gRPC Server.
	logger, _ := test.NewNullLogger()
	if s.Logger == nil {
		s.Logger = logger
	}
	if s.ServerConfig == nil {
		s.ServerConfig = new(config.WeaviateConfig)
	}
	s.ServerConfig.Config.GRPC.MaxMsgSize = max(s.ServerConfig.Config.GRPC.MaxMsgSize, bufSize)
	if s.ServerConfig.Config.Sentry == nil {
		s.ServerConfig.Config.Sentry = new(sentry.ConfigOpts)
	}
	if s.ServerConfig.Config.OperationalMode == nil {
		s.ServerConfig.Config.OperationalMode = runtime.NewDynamicValue(config.READ_WRITE)
	}
	if s.Cluster == nil {
		s.Cluster = new(cluster.State)
	}

	srv, stop := CreateGRPCServer(s, Config{Registerer: monitoring.NoopRegisterer})
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

	return conn
}
