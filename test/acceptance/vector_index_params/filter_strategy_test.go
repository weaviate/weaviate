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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
)

// TestVectorIndexParamsFilterStrategy checks the filter strategy chosen per
// request on ONE collection (default filterStrategy, flatSearchCutoff 0 so the
// filtered graph walk is taken) instead of through the collection config. The
// filter keeps 50% of the objects: on a 10% filter sweeping explores so much
// of the graph to find allowed candidates that ef=10 is already near-exact,
// while at 50% all three strategies lose about 0.10 recall at ef=10.
func TestVectorIndexParamsFilterStrategy(t *testing.T) {
	c := setupSingleNode(t)
	class, ds := filterShared.get(t)
	filter, half := bucketBelow(5)

	req := func(ef uint32, strategy *pb.FilterStrategy, f *pb.Filters) func(q []float32) *pb.SearchRequest {
		return func(q []float32) *pb.SearchRequest {
			r := nearVectorRequest(class, q, k, nil)
			r.VectorIndexParams = &pb.VectorIndexParams{Ef: u32(ef), FilterStrategy: strategy}
			r.Filters = f
			return r
		}
	}
	filteredRecall := func(build func(q []float32) *pb.SearchRequest) float64 {
		var s float64
		for qi, q := range ds.queries {
			s += recallOf(searchIDs(t, c, build(q)), ds.exactTopK(qi, half))
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
		where := map[string]any{"path": []string{"bucket"}, "operator": "LessThan", "valueInt": 5}
		for qi, q := range ds.queries[:numQueries] {
			want := searchIDs(t, c, req(10, pb.FilterStrategy_FILTER_STRATEGY_PATHSEER.Enum(), filter)(q))
			got := restOKIDs(t, uri, class, "near-vector", map[string]any{
				"vector": q, "limit": k, "where": where,
				"vectorIndexParams": map[string]any{"ef": 10, "filterStrategy": "pathseer"},
			})
			require.Equal(t, want, got, "query %d", qi)
		}
	})

	// Same graph, so the per-request strategy must return exactly what the
	// collection returns once it is configured with that strategy.
	t.Run("per-request strategy equals the collection configured with it", func(t *testing.T) {
		setFilterStrategy := func(strategy string) {
			cls := helper.GetClass(t, class)
			cls.VectorIndexConfig.(map[string]any)["filterStrategy"] = strategy
			helper.UpdateClass(t, cls)
		}
		t.Cleanup(func() { setFilterStrategy("acorn") })
		for _, s := range []string{"sweeping", "pathseer"} {
			t.Run(s, func(t *testing.T) {
				strategy := pb.FilterStrategy(pb.FilterStrategy_value["FILTER_STRATEGY_"+strings.ToUpper(s)])
				want := idsPerQuery(t, c, ds, req(10, strategy.Enum(), filter))
				setFilterStrategy(s)
				require.EventuallyWithT(t, func(ct *assert.CollectT) {
					assert.Equal(ct, want, idsPerQuery(t, c, ds, req(10, nil, filter)))
				}, 30*time.Second, 500*time.Millisecond)
			})
		}
	})
}
