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
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// TestVectorIndexParamsOperatorCaps pins that each surface enforces the caps
// the operator set on the shared node (see main_test.go): the gRPC parser and
// the REST handler read their caps from the server config, and the two caps
// must not be swapped (hence two different values).
func TestVectorIndexParamsOperatorCaps(t *testing.T) {
	c := setupSingleNode(t)
	uri := singleNode.GetWeaviate().URI()
	class, ds := hnswShared.get(t)
	q := ds.queries[0]

	t.Run("gRPC", func(t *testing.T) {
		tests := []struct {
			name    string
			params  *pb.VectorIndexParams
			wantErr string // empty: accepted
		}{
			{"ef at the cap", efP(maxSearchEF), ""},
			{"ef above the cap", efP(maxSearchEF + 1), fmt.Sprintf("ef %d exceeds HNSW_MAX_SEARCH_EF (%d)", maxSearchEF+1, maxSearchEF)},
			{"search_probe at the cap", probeP(maxSearchProbe), ""},
			{"search_probe above the cap", probeP(maxSearchProbe + 1), fmt.Sprintf("search_probe %d exceeds HFRESH_MAX_SEARCH_PROBE (%d)", maxSearchProbe+1, maxSearchProbe)},
			// the built-in default (10000) must not leak through either cap
			{"ef at the built-in default", efP(10000), fmt.Sprintf("exceeds HNSW_MAX_SEARCH_EF (%d)", maxSearchEF)},
			{"per-target ef above the cap is named", nil, "vector_index_params_for_targets"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				req := nearVectorRequest(class, q, k, nil)
				req.VectorIndexParams = tt.params
				if tt.params == nil {
					// a legacy single-vector class takes no per-target entries;
					// that error fires before the cap, which is what we want
					// to see named here
					req.VectorIndexParamsForTargets = []*pb.VectorIndexParamsForTarget{forTarget("x", efP(maxSearchEF+1))}
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
			{"ef at the cap", map[string]any{"ef": maxSearchEF}, http.StatusOK, ""},
			{"ef above the cap", map[string]any{"ef": maxSearchEF + 1}, http.StatusUnprocessableEntity, fmt.Sprintf("ef %d exceeds HNSW_MAX_SEARCH_EF (%d)", maxSearchEF+1, maxSearchEF)},
			{"searchProbe at the cap", map[string]any{"searchProbe": maxSearchProbe}, http.StatusOK, ""},
			{"searchProbe above the cap", map[string]any{"searchProbe": maxSearchProbe + 1}, http.StatusUnprocessableEntity, fmt.Sprintf("searchProbe %d exceeds HFRESH_MAX_SEARCH_PROBE (%d)", maxSearchProbe+1, maxSearchProbe)},
			{"ef at the built-in default", map[string]any{"ef": 10000}, http.StatusUnprocessableEntity, fmt.Sprintf("exceeds HNSW_MAX_SEARCH_EF (%d)", maxSearchEF)},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				body := map[string]any{"vector": q, "limit": k, "vectorIndexParams": tt.params}
				status, out := restSearch(t, uri, class, "near-vector", body)
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
