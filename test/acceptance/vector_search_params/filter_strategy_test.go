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

package vector_search_params

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// TestVectorSearchParamsFilterStrategy reproduces the "with filter" table of
// TestVectorSearchParamsEF_HNSW with the filter strategy chosen per request on
// ONE collection (default filterStrategy, flatSearchCutoff 0 so the filtered
// graph walk is taken) instead of through the collection config.
func TestVectorSearchParamsFilterStrategy(t *testing.T) {
	const (
		class   = "FilterStrategy"
		buckets = 5 // the filter keeps 50% of the objects, see the EF_HNSW table
	)
	c := setupSingleNode(t)
	ds := newDataset(42, numObjects, statQueries)
	cfg := rqConfig()
	cfg["flatSearchCutoff"] = 0
	createSingleVectorCollection(t, class, "hnsw", cfg, nil, ds, numObjects, types.ConsistencyLevelQuorum)

	filter := &pb.Filters{
		Operator:  pb.Filters_OPERATOR_LESS_THAN,
		TestValue: &pb.Filters_ValueInt{ValueInt: buckets},
		Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "bucket"}},
	}
	keep := func(i int) bool { return i%numBuckets < buckets }

	req := func(ef uint32, strategy *pb.FilterStrategy, f *pb.Filters) func(q []float32) *pb.SearchRequest {
		return func(q []float32) *pb.SearchRequest {
			r := nearVectorRequest(class, q, k, nil)
			r.VectorSearchParams = &pb.VectorSearchParams{Ef: u32(ef), FilterStrategy: strategy}
			r.Filters = f
			return r
		}
	}
	filteredRecall := func(build func(q []float32) *pb.SearchRequest) float64 {
		var s float64
		for _, q := range ds.queries {
			s += recallOf(searchIDs(t, c, build(q)), ds.exactTopK(q, keep))
		}
		return s / float64(len(ds.queries))
	}

	strategies := []struct {
		name     string
		strategy pb.FilterStrategy
	}{
		{"sweeping", pb.FilterStrategy_FILTER_STRATEGY_SWEEPING},
		{"acorn", pb.FilterStrategy_FILTER_STRATEGY_ACORN},
		{"pathseer", pb.FilterStrategy_FILTER_STRATEGY_PATHSEER},
	}

	t.Run("filtered recall per strategy", func(t *testing.T) {
		lowIDs := map[string][][]string{"default": idsPerQuery(t, c, ds, req(10, nil, filter))}
		for _, s := range strategies {
			t.Run(s.name, func(t *testing.T) {
				low := filteredRecall(req(10, s.strategy.Enum(), filter))
				high := filteredRecall(req(1000, s.strategy.Enum(), filter))
				t.Logf("%s: filtered recall ef=10 %.3f, ef=1000 %.3f", s.name, low, high)
				assert.GreaterOrEqual(t, high, 0.95, "ef=1000 must be near-exact")
				assert.LessOrEqual(t, low, high-0.03, "ef=10 must have clearly lower recall on the graph path")
				lowIDs[s.name] = idsPerQuery(t, c, ds, req(10, s.strategy.Enum(), filter))
			})
		}
		for _, s := range strategies {
			t.Logf("ef=10 %s vs collection default: results differ on %d of %d queries",
				s.name, countDiffering(lowIDs[s.name], lowIDs["default"]), len(ds.queries))
		}
		// The collection default is acorn, so acorn must equal it, and
		// pathseer walks the graph differently: if the per-request strategy
		// were dropped, pathseer would equal the default too.
		assert.Equal(t, lowIDs["default"], lowIDs["acorn"], "per-request acorn must equal the collection default")
		assert.Positive(t, countDiffering(lowIDs["pathseer"], lowIDs["default"]),
			"per-request pathseer never changed the result, so the strategy did not reach the index")
	})

	t.Run("no filter ignores the strategy", func(t *testing.T) {
		base := idsPerQuery(t, c, ds, req(10, nil, nil))
		for _, s := range strategies {
			require.Equal(t, base, idsPerQuery(t, c, ds, req(10, s.strategy.Enum(), nil)), s.name)
		}
	})

	t.Run("REST matches gRPC", func(t *testing.T) {
		uri := singleNode.GetWeaviate().URI()
		where := map[string]any{"path": []string{"bucket"}, "operator": "LessThan", "valueInt": buckets}
		for qi, q := range ds.queries[:numQueries] {
			want := searchIDs(t, c, req(10, pb.FilterStrategy_FILTER_STRATEGY_PATHSEER.Enum(), filter)(q))
			got := restOKIDs(t, uri, class, "near-vector", map[string]any{
				"vector": q, "limit": k, "where": where,
				"vectorSearchParams": map[string]any{"ef": 10, "filterStrategy": "pathseer"},
			})
			require.Equal(t, want, got, "query %d", qi)
		}
	})
}
