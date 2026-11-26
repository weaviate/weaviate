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

package vector_index_params

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestVectorIndexParamsOperatorCaps starts a node with both caps set by the
// operator and pins that each surface enforces the configured value: the
// gRPC parser and the REST handler read their caps from the server config,
// and the two caps must not be swapped (hence two different values).
func TestVectorIndexParamsOperatorCaps(t *testing.T) {
	const (
		className = "EfCaps"
		maxEF     = 500
		maxProbe  = 50
		n         = 100
	)
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("HNSW_MAX_SEARCH_EF", "500").
		WithWeaviateEnv("HFRESH_MAX_SEARCH_PROBE", "50").
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	uri := compose.GetWeaviate().URI()
	helper.SetupClient(uri)
	c := newGRPC(t, compose.GetWeaviate().GrpcURI())
	ds := newDataset(53, n, 1)
	createSingleVectorCollection(t, className, "hnsw", rqConfig(), nil, ds, n, types.ConsistencyLevelOne)
	q := ds.queries[0]

	t.Run("gRPC", func(t *testing.T) {
		tests := []struct {
			name    string
			params  *pb.VectorIndexParams
			wantErr string // empty: accepted
		}{
			{"ef at the cap", &pb.VectorIndexParams{Ef: u32(maxEF)}, ""},
			{"ef above the cap", &pb.VectorIndexParams{Ef: u32(maxEF + 1)}, "ef 501 exceeds HNSW_MAX_SEARCH_EF (500)"},
			{"search_probe at the cap", &pb.VectorIndexParams{SearchProbe: u32(maxProbe)}, ""},
			{"search_probe above the cap", &pb.VectorIndexParams{SearchProbe: u32(maxProbe + 1)}, "search_probe 51 exceeds HFRESH_MAX_SEARCH_PROBE (50)"},
			// the built-in default (10000) must not leak through either cap
			{"ef at the built-in default", &pb.VectorIndexParams{Ef: u32(10000)}, "exceeds HNSW_MAX_SEARCH_EF (500)"},
			{"per-target ef above the cap is named", nil, "vector_index_params_for_targets"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				req := nearVectorRequest(className, q, k, nil)
				req.VectorIndexParams = tt.params
				if tt.params == nil {
					// a legacy single-vector class takes no per-target entries;
					// that error fires before the cap, which is what we want
					// to see named here
					req.VectorIndexParamsForTargets = []*pb.VectorIndexParamsForTarget{forTarget("x", &pb.VectorIndexParams{Ef: u32(maxEF + 1)})}
				}
				reply, err := c.Search(callCtx(t), req)
				if tt.wantErr == "" {
					require.NoError(t, err)
					assert.Len(t, reply.Results, k)
					return
				}
				requireInvalidArgument(t, err, tt.wantErr)
			})
		}
	})

	t.Run("REST", func(t *testing.T) {
		tests := []struct {
			name       string
			params     map[string]any
			wantStatus int
			wantErr    string
		}{
			{"ef at the cap", map[string]any{"ef": maxEF}, http.StatusOK, ""},
			{"ef above the cap", map[string]any{"ef": maxEF + 1}, http.StatusUnprocessableEntity, "ef 501 exceeds HNSW_MAX_SEARCH_EF (500)"},
			{"searchProbe at the cap", map[string]any{"searchProbe": maxProbe}, http.StatusOK, ""},
			{"searchProbe above the cap", map[string]any{"searchProbe": maxProbe + 1}, http.StatusUnprocessableEntity, "searchProbe 51 exceeds HFRESH_MAX_SEARCH_PROBE (50)"},
			{"ef at the built-in default", map[string]any{"ef": 10000}, http.StatusUnprocessableEntity, "exceeds HNSW_MAX_SEARCH_EF (500)"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				body := map[string]any{"vector": q, "limit": k, "vectorIndexParams": tt.params}
				status, out := restSearch(t, uri, className, "near-vector", body)
				require.Equal(t, tt.wantStatus, status, "%v", out)
				if tt.wantStatus == http.StatusOK {
					assert.Len(t, restResultIDs(t, out), k)
					return
				}
				assert.Contains(t, restErrorMessage(t, out), tt.wantErr)
			})
		}
	})
}

// restErrorMessage joins the messages of an ErrorResponse body.
func restErrorMessage(t *testing.T, out map[string]any) string {
	t.Helper()
	errs, ok := out["error"].([]any)
	require.True(t, ok, "not an error response: %v", out)
	msg := ""
	for _, e := range errs {
		msg += e.(map[string]any)["message"].(string) + "\n"
	}
	return msg
}
