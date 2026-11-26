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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// TestVectorIndexParamsSearchProbe checks the request-wide search_probe on an
// HFresh collection with a single named vector: recall grows with the probe,
// the probe is floored at the limit (max(k, probe) centroids are routed), ef
// is ignored, and REST lands on the same search as gRPC.
func TestVectorIndexParamsSearchProbe(t *testing.T) {
	const class = "SearchProbe"
	c := setupSingleNode(t)
	ds := newDataset(52, perTargetObjects, statQueries)
	createNamedVectorCollection(t, class, map[string]namedIndex{ivfTarget: {"hfresh", hfreshConfig()}},
		nil, ds, perTargetObjects, types.ConsistencyLevelQuorum)

	// The collection has a single named vector, so no target is needed.
	req := func(params *pb.VectorIndexParams) func(q []float32) *pb.SearchRequest {
		return func(q []float32) *pb.SearchRequest {
			r := nearVectorRequest(class, q, k, nil)
			r.VectorIndexParams = params
			return r
		}
	}
	probe := func(p uint32) *pb.VectorIndexParams { return &pb.VectorIndexParams{SearchProbe: u32(p)} }
	waitForStableResults(t, c, ds, req(probe(1)))

	t.Run("recall non-decreasing", func(t *testing.T) {
		recalls := map[uint32]float64{}
		prev := -1.0
		for _, p := range []uint32{1, 8, 64, 256} {
			r := meanRecallOf(t, c, ds, req(probe(p)))
			t.Logf("search_probe=%d recall@%d=%.3f", p, k, r)
			// 0.01 tolerance: RQ rescoring can invert two recalls both near 1.0.
			assert.GreaterOrEqual(t, r, prev-0.01, "recall must not decrease when search_probe grows to %d", p)
			prev = r
			recalls[p] = r
		}
		assert.GreaterOrEqual(t, recalls[256], 0.95, "search_probe=256 recall")
		assert.LessOrEqual(t, recalls[1], recalls[256]-0.05, "search_probe=1 must have clearly lower recall than 256")
	})

	t.Run("probe below limit", func(t *testing.T) {
		// max(k, probe): probes 1 and 8 both route k=10 centroids.
		atLimit := idsPerQuery(t, c, ds, req(probe(k)))
		require.Equal(t, atLimit, idsPerQuery(t, c, ds, req(probe(1))))
		require.Equal(t, atLimit, idsPerQuery(t, c, ds, req(probe(8))))
	})

	t.Run("ef ignored", func(t *testing.T) {
		require.Equal(t, idsPerQuery(t, c, ds, req(nil)), idsPerQuery(t, c, ds, req(&pb.VectorIndexParams{Ef: u32(10)})))
		require.Equal(t, idsPerQuery(t, c, ds, req(probe(1))),
			idsPerQuery(t, c, ds, req(&pb.VectorIndexParams{Ef: u32(1000), SearchProbe: u32(1)})))
	})

	t.Run("REST matches gRPC", func(t *testing.T) {
		uri := singleNode.GetWeaviate().URI()
		for _, p := range []uint32{1, 256} {
			for qi, q := range ds.queries[:numQueries] {
				want := searchIDs(t, c, req(probe(p))(q))
				got := restOKIDs(t, uri, class, "near-vector", map[string]any{
					"vector": q, "limit": k, "vectorIndexParams": map[string]any{"searchProbe": p},
				})
				require.Equal(t, want, got, "search_probe %d query %d", p, qi)
			}
		}
	})
}
