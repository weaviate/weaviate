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

package clusterapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	entsentry "github.com/weaviate/weaviate/entities/sentry"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema"
)

// Test_composeClusterHandler_PreservesRouting guards against regressing the
// observability wraps onto the bare mux (dropping the gRPC multiplexer and the
// /v1/cluster/* RAFT middleware). It asserts that, with Sentry enabled or
// disabled, gRPC requests still reach the gRPC server and /v1/cluster/* still
// reaches the RAFT router rather than falling through to the REST mux.
func Test_composeClusterHandler_PreservesRouting(t *testing.T) {
	for _, sentryEnabled := range []bool{false, true} {
		name := "sentry disabled"
		if sentryEnabled {
			name = "sentry enabled"
		}

		t.Run(name, func(t *testing.T) {
			appState := &state.State{
				SchemaManager: &schema.Manager{},
				ServerConfig: &config.WeaviateConfig{
					Config: config.Config{
						Sentry: &entsentry.ConfigOpts{Enabled: sentryEnabled},
						// Monitoring left disabled (zero value).
					},
				},
			}
			auth := NewBasicAuthHandler(cluster.AuthConfig{})

			var grpcCalled bool
			grpcProbe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				grpcCalled = true
				w.WriteHeader(http.StatusOK)
			})

			mux := http.NewServeMux()
			mux.Handle("/", index())

			handler := composeClusterHandler(mux, grpcProbe, appState, auth)

			t.Run("gRPC request reaches the gRPC server", func(t *testing.T) {
				grpcCalled = false
				req := httptest.NewRequest(http.MethodPost, "/grpc.health.v1.Health/Check", nil)
				req.ProtoMajor = 2
				req.Header.Set("content-type", "application/grpc")
				rec := httptest.NewRecorder()

				handler.ServeHTTP(rec, req)

				assert.True(t, grpcCalled, "gRPC request must be routed to the gRPC server, not the REST mux")
			})

			t.Run("/v1/cluster/join reaches the RAFT router", func(t *testing.T) {
				grpcCalled = false
				req := httptest.NewRequest(http.MethodPost, "/v1/cluster/join", strings.NewReader(""))
				rec := httptest.NewRecorder()

				handler.ServeHTTP(rec, req)

				// empty body -> 400 from the raft handler; a 404 would mean the
				// request fell through to the mux's index() (middleware dropped).
				assert.Equal(t, http.StatusBadRequest, rec.Code,
					"/v1/cluster/join must reach the RAFT router, not the REST mux")
				assert.False(t, grpcCalled)
			})

			t.Run("plain REST request reaches the mux", func(t *testing.T) {
				grpcCalled = false
				req := httptest.NewRequest(http.MethodGet, "/", nil)
				rec := httptest.NewRecorder()

				handler.ServeHTTP(rec, req)

				require.False(t, grpcCalled)
				assert.Equal(t, http.StatusOK, rec.Code)
			})
		})
	}
}
