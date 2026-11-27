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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	restCtx "github.com/weaviate/weaviate/adapters/handlers/rest/context"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/sentry"
	pbv0 "github.com/weaviate/weaviate/grpc/generated/protocol/v0"
	pbv1 "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/telemetry"
)

// mockAddr returns itself as [net.Addr.String].
type mockAddr string

var _ net.Addr = (*mockAddr)(nil)

func (a mockAddr) Network() string { return "mock" }
func (a mockAddr) String() string  { return string(a) }

func TestContextWithMetadataValues(t *testing.T) {
	logger, _ := test.NewNullLogger()

	ct := telemetry.NewClientTracker(logger)
	t.Cleanup(ct.Stop)

	it := telemetry.NewIntegrationTracker(logger)
	t.Cleanup(it.Stop)

	const client = telemetry.ClientTypeGo
	const client_version = "v0.67.0"
	const x_weaviate_client = string("weaviate-client-" + client + "/" + client_version)
	const integration = "langchain"
	const integration_version = "v0.420.0"
	const x_weaviate_client_integration = integration + "/" + integration_version

	for _, tt := range []struct {
		name string
		md   metadata.MD
		peer *peer.Peer

		clientID    any // Expected "clientIdentifier" value.
		sourceIP    any // Expected "sourceIp" value.
		integration any // Expected X-Weaviate-Client-Integration header.
	}{
		{
			name: "IP in X-Real-IP",
			md: metadata.Pairs(
				"x-weaviate-client", x_weaviate_client,
				"x-real-ip", "65.41.169.99",
				"x-weaviate-client-integration", x_weaviate_client_integration,
			),
			clientID:    x_weaviate_client,
			sourceIP:    "65.41.169.99",
			integration: x_weaviate_client_integration,
		},
		{
			name: "IP in X-Forwarded-For",
			md: metadata.Pairs(
				"x-weaviate-client", x_weaviate_client,
				"x-forwarded-for", "65.41.169.99,110.44.57.14",
				"x-weaviate-client-integration", x_weaviate_client_integration,
			),
			clientID:    x_weaviate_client,
			sourceIP:    "65.41.169.99",
			integration: x_weaviate_client_integration,
		},
		{
			name:     "IPv4 from connection peer",
			peer:     &peer.Peer{Addr: mockAddr("65.41.169.99")},
			sourceIP: "65.41.169.99",
		},
		{
			name:     "IPv4:port from connection peer",
			peer:     &peer.Peer{Addr: mockAddr("65.41.169.99:80")},
			sourceIP: "65.41.169.99",
		},
		{
			name:     "IPv6 from connection peer",
			peer:     &peer.Peer{Addr: mockAddr("3e91:4f1c:5475:8e36:f71:b8bd:902a:5c37")},
			sourceIP: "3e91:4f1c:5475:8e36:f71:b8bd:902a:5c37",
		},
		{
			name:     "IPv6:80 from connection peer",
			peer:     &peer.Peer{Addr: mockAddr("[3e91:4f1c:5475:8e36:f71:b8bd:902a:5c37]:80")},
			sourceIP: "3e91:4f1c:5475:8e36:f71:b8bd:902a:5c37",
		},
		{
			name:     "IPv6 loopback from connection peer",
			peer:     &peer.Peer{Addr: mockAddr("::1")},
			sourceIP: "127.0.0.1",
		},
		{
			name:     "IPv6 loopback :80 from connection peer",
			peer:     &peer.Peer{Addr: mockAddr("[::1]:80")},
			sourceIP: "127.0.0.1",
		},
		{
			name:     "source IP unknown",
			sourceIP: "unknown",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inctx := context.Background()
			if tt.md != nil {
				inctx = metadata.NewIncomingContext(inctx, tt.md)
			}
			if tt.peer != nil {
				inctx = peer.NewContext(inctx, tt.peer)
			}

			ctx := contextWithMetadataValues(inctx, ct, it)

			require.Equal(t, tt.clientID, ctx.Value("clientIdentifier"), "client SDK")
			require.Equal(t, tt.sourceIP, ctx.Value("sourceIp"), "source IP")

			if tt.clientID != nil {
				v := ct.GetAndReset()[client]
				require.Len(t, v, 1, "# client SDKs")
				require.EqualValuesf(t, v[client_version], 1, "times %s recorded", x_weaviate_client)
			}

			if tt.integration != nil {
				v := it.GetAndReset()[integration]
				require.Len(t, v, 1, "# integration clients")
				require.EqualValuesf(t, v[integration_version], 1, "times %s recorded", x_weaviate_client_integration)
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

func TestServerState_admit(t *testing.T) {
	for _, tt := range []struct {
		method string     // Full method name.
		mode   string     // Operational mode.
		code   codes.Code // Expected status code, zero is codes.OK.
		err    error      // Expected error.
	}{
		// Healthcheck requests.
		{method: grpc_health_v1.Health_Check_FullMethodName, mode: config.READ_WRITE},
		{method: grpc_health_v1.Health_Check_FullMethodName, mode: config.READ_ONLY},
		{method: grpc_health_v1.Health_Check_FullMethodName, mode: config.WRITE_ONLY},
		{method: grpc_health_v1.Health_Check_FullMethodName, mode: config.SCALE_OUT},

		{method: grpc_health_v1.Health_Watch_FullMethodName, mode: config.READ_WRITE},
		{method: grpc_health_v1.Health_Watch_FullMethodName, mode: config.READ_ONLY},
		{method: grpc_health_v1.Health_Watch_FullMethodName, mode: config.WRITE_ONLY},
		{method: grpc_health_v1.Health_Watch_FullMethodName, mode: config.SCALE_OUT},

		{method: grpc_health_v1.Health_List_FullMethodName, mode: config.READ_WRITE},
		{method: grpc_health_v1.Health_List_FullMethodName, mode: config.READ_ONLY},
		{method: grpc_health_v1.Health_List_FullMethodName, mode: config.WRITE_ONLY},
		{method: grpc_health_v1.Health_List_FullMethodName, mode: config.SCALE_OUT},

		// Read requests.
		{method: pbv1.Weaviate_Aggregate_FullMethodName, mode: config.READ_WRITE},
		{method: pbv1.Weaviate_Aggregate_FullMethodName, mode: config.READ_ONLY},
		{method: pbv1.Weaviate_Aggregate_FullMethodName, mode: config.SCALE_OUT},
		{
			method: pbv1.Weaviate_Aggregate_FullMethodName, mode: config.WRITE_ONLY,
			code: codes.Unavailable, err: config.ErrWriteOnlyModeEnabled,
		},

		{method: pbv1.Weaviate_Search_FullMethodName, mode: config.READ_WRITE},
		{method: pbv1.Weaviate_Search_FullMethodName, mode: config.READ_ONLY},
		{method: pbv1.Weaviate_Search_FullMethodName, mode: config.SCALE_OUT},
		{
			method: pbv1.Weaviate_Search_FullMethodName, mode: config.WRITE_ONLY,
			code: codes.Unavailable, err: config.ErrWriteOnlyModeEnabled,
		},

		{method: pbv0.Weaviate_Search_FullMethodName, mode: config.READ_WRITE},
		{method: pbv0.Weaviate_Search_FullMethodName, mode: config.READ_ONLY},
		{method: pbv0.Weaviate_Search_FullMethodName, mode: config.SCALE_OUT},
		{
			method: pbv0.Weaviate_Search_FullMethodName, mode: config.WRITE_ONLY,
			code: codes.Unavailable, err: config.ErrWriteOnlyModeEnabled,
		},

		{method: pbv1.Weaviate_TenantsGet_FullMethodName, mode: config.READ_WRITE},
		{method: pbv1.Weaviate_TenantsGet_FullMethodName, mode: config.READ_ONLY},
		{method: pbv1.Weaviate_TenantsGet_FullMethodName, mode: config.SCALE_OUT},
		{
			method: pbv1.Weaviate_TenantsGet_FullMethodName, mode: config.WRITE_ONLY,
			code: codes.Unavailable, err: config.ErrWriteOnlyModeEnabled,
		},

		// Write requests.
		{method: pbv1.Weaviate_BatchObjects_FullMethodName, mode: config.READ_WRITE},
		{method: pbv1.Weaviate_BatchObjects_FullMethodName, mode: config.WRITE_ONLY},
		{
			method: pbv1.Weaviate_BatchObjects_FullMethodName, mode: config.READ_ONLY,
			code: codes.Unavailable, err: config.ErrReadOnlyModeEnabled,
		},
		{
			method: pbv1.Weaviate_BatchObjects_FullMethodName, mode: config.SCALE_OUT,
			code: codes.Unavailable, err: config.ErrScaleOutModeEnabled,
		},

		{method: pbv0.Weaviate_BatchObjects_FullMethodName, mode: config.READ_WRITE},
		{method: pbv0.Weaviate_BatchObjects_FullMethodName, mode: config.WRITE_ONLY},
		{
			method: pbv0.Weaviate_BatchObjects_FullMethodName, mode: config.READ_ONLY,
			code: codes.Unavailable, err: config.ErrReadOnlyModeEnabled,
		},
		{
			method: pbv0.Weaviate_BatchObjects_FullMethodName, mode: config.SCALE_OUT,
			code: codes.Unavailable, err: config.ErrScaleOutModeEnabled,
		},

		{method: pbv1.Weaviate_BatchStream_FullMethodName, mode: config.READ_WRITE},
		{method: pbv1.Weaviate_BatchStream_FullMethodName, mode: config.WRITE_ONLY},
		{
			method: pbv1.Weaviate_BatchStream_FullMethodName, mode: config.READ_ONLY,
			code: codes.Unavailable, err: config.ErrReadOnlyModeEnabled,
		},
		{
			method: pbv1.Weaviate_BatchStream_FullMethodName, mode: config.SCALE_OUT,
			code: codes.Unavailable, err: config.ErrScaleOutModeEnabled,
		},

		{method: pbv1.Weaviate_BatchReferences_FullMethodName, mode: config.READ_WRITE},
		{method: pbv1.Weaviate_BatchReferences_FullMethodName, mode: config.WRITE_ONLY},
		{
			method: pbv1.Weaviate_BatchReferences_FullMethodName, mode: config.READ_ONLY,
			code: codes.Unavailable, err: config.ErrReadOnlyModeEnabled,
		},
		{
			method: pbv1.Weaviate_BatchReferences_FullMethodName, mode: config.SCALE_OUT,
			code: codes.Unavailable, err: config.ErrScaleOutModeEnabled,
		},

		{method: pbv1.Weaviate_BatchDelete_FullMethodName, mode: config.READ_WRITE},
		{method: pbv1.Weaviate_BatchDelete_FullMethodName, mode: config.WRITE_ONLY},
		{
			method: pbv1.Weaviate_BatchDelete_FullMethodName, mode: config.READ_ONLY,
			code: codes.Unavailable, err: config.ErrReadOnlyModeEnabled,
		},
		{
			method: pbv1.Weaviate_BatchDelete_FullMethodName, mode: config.SCALE_OUT,
			code: codes.Unavailable, err: config.ErrScaleOutModeEnabled,
		},

		{
			method: "unknown method", mode: config.READ_WRITE,
			code: codes.Unimplemented, err: errors.New("unknown method"),
		},
	} {
		t.Run(fmt.Sprintf("method=%s mode=%s", tt.method, tt.mode), func(t *testing.T) {
			ss := serverState{
				Cluster: new(cluster.State),
				ServerConfig: &config.WeaviateConfig{
					Config: config.Config{
						OperationalMode: runtime.NewDynamicValue(tt.mode),
					},
				},
			}

			t.Run("maintenance mode on", func(t *testing.T) {
				ss.Cluster.SetMaintenanceModeForLocalhost(true)

				err := ss.admit(tt.method)
				st, ok := status.FromError(err)

				assert.True(t, ok, "interceptor must return gRPC status.Status")
				assert.Equal(t, st.Code(), codes.Unavailable, "error status code")
				require.ErrorContains(t, err, "maintenance mode")
			})

			t.Run("maintenance mode off", func(t *testing.T) {
				ss.Cluster.SetMaintenanceModeForLocalhost(false)

				err := ss.admit(tt.method)
				st, ok := status.FromError(err)

				require.True(t, ok, "interceptor must return gRPC status.Status")
				assert.Equal(t, st.Code(), tt.code, "response status code")
				if tt.err == nil {
					require.NoError(t, err, "should admit request")
				} else {
					require.ErrorContains(t, err, tt.err.Error(), "should reject request")
				}
			})
		})
	}
}

// Integration test to check that serverState interceptor
// is configured for both unary and stream RPCs.
func TestServerStateInterceptor(t *testing.T) {
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

	batchObjects := func(t *testing.T, c pbv1.WeaviateClient) error {
		_, err := c.BatchObjects(t.Context(), new(pbv1.BatchObjectsRequest))
		return err
	}
	batchStream := func(t *testing.T, c pbv1.WeaviateClient) error {
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
	}

	t.Run("server in maintenance mode", func(t *testing.T) {
		serverState.Cluster.SetMaintenanceModeForLocalhost(true)
		t.Cleanup(func() { serverState.Cluster.SetMaintenanceModeForLocalhost(false) })

		t.Run("unary RPC", func(t *testing.T) {
			require.ErrorContains(t, batchObjects(t, client), "maintenance mode", "must reject request")
		})
		t.Run("streaming RPC", func(t *testing.T) {
			require.ErrorContains(t, batchStream(t, client), "maintenance mode", "must reject request")
		})
	})

	t.Run("write in read-only mode", func(t *testing.T) {
		serverState.ServerConfig.Config.OperationalMode.SetValue(config.READ_ONLY)
		t.Cleanup(func() { serverState.ServerConfig.Config.OperationalMode.SetValue(config.READ_WRITE) })

		t.Run("unary RPC", func(t *testing.T) {
			require.ErrorContains(t, batchObjects(t, client), "read-only mode", "must reject request")
		})

		t.Run("stream RPC", func(t *testing.T) {
			require.ErrorContains(t, batchStream(t, client), "read-only mode", "must reject request")
		})
	})
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
