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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

const (
	manual = pb.CombinationMethod_COMBINATION_METHOD_TYPE_MANUAL
	sum    = pb.CombinationMethod_COMBINATION_METHOD_TYPE_SUM
)

// TestVectorIndexParamsPerTarget checks that each per-target knob reaches
// only its target (HNSW graph, HFresh ivf) and overrides the request-wide
// value field by field. The other target gets the decoy query (see isolate).
func TestVectorIndexParamsPerTarget(t *testing.T) {
	c := setupSingleNode(t)
	class, ds := twoVectorShared.get(t)

	graphOnly := func(params *pb.VectorIndexParams, perTarget ...*pb.VectorIndexParamsForTarget) func(q []float32) *pb.SearchRequest {
		return func(q []float32) *pb.SearchRequest {
			return multiTargetRequest(class, k, manual, isolate(graphTarget, q), params, perTarget...)
		}
	}
	ivfOnly := func(params *pb.VectorIndexParams, perTarget ...*pb.VectorIndexParamsForTarget) func(q []float32) *pb.SearchRequest {
		return func(q []float32) *pb.SearchRequest {
			return multiTargetRequest(class, k, manual, isolate(ivfTarget, q), params, perTarget...)
		}
	}
	t.Run("ef reaches only the graph target", func(t *testing.T) {
		low := meanRecallOf(t, c, ds, graphOnly(nil, forTarget(graphTarget, efP(10))))
		high := meanRecallOf(t, c, ds, graphOnly(nil, forTarget(graphTarget, efP(1000))))
		t.Logf("graph target: mean recall@%d per-target ef=10 %.3f, ef=1000 %.3f", k, low, high)
		assert.GreaterOrEqual(t, high, 0.95)
		assert.LessOrEqual(t, low, high-0.05, "per-target ef=10 must lower the graph recall")

		// HFresh has no ef: a per-target ef on ivf must not change its result.
		// The probe is lowered to its minimum so the ivf result is far from
		// exact and any knob that did reach the routing would show.
		ivfEf10 := idsPerQuery(t, c, ds, ivfOnly(nil, forTarget(ivfTarget, &pb.VectorIndexParams{Ef: u32(10), SearchProbe: u32(1)})))
		ivfEf1000 := idsPerQuery(t, c, ds, ivfOnly(nil, forTarget(ivfTarget, &pb.VectorIndexParams{Ef: u32(1000), SearchProbe: u32(1)})))
		ivfNoEf := idsPerQuery(t, c, ds, ivfOnly(nil, forTarget(ivfTarget, probeP(1))))
		require.Equal(t, ivfNoEf, ivfEf10, "ef must be ignored by the hfresh target")
		require.Equal(t, ivfNoEf, ivfEf1000, "ef must be ignored by the hfresh target")
	})

	t.Run("search_probe reaches only the ivf target", func(t *testing.T) {
		low := meanRecallOf(t, c, ds, ivfOnly(nil, forTarget(ivfTarget, probeP(1))))
		high := meanRecallOf(t, c, ds, ivfOnly(nil, forTarget(ivfTarget, probeP(256))))
		t.Logf("ivf target: mean recall@%d per-target search_probe=1 %.3f, search_probe=256 %.3f", k, low, high)
		assert.GreaterOrEqual(t, high, 0.95)
		assert.LessOrEqual(t, low, high-0.05, "per-target search_probe=1 must lower the ivf recall")

		// HNSW has no search_probe. ef 10 keeps the graph result inexact so a
		// probe that leaked into the beam would show.
		base := idsPerQuery(t, c, ds, graphOnly(nil, forTarget(graphTarget, efP(10))))
		withProbe := idsPerQuery(t, c, ds, graphOnly(nil, forTarget(graphTarget, &pb.VectorIndexParams{Ef: u32(10), SearchProbe: u32(1)})))
		require.Equal(t, base, withProbe, "search_probe must be ignored by the hnsw target")
	})

	t.Run("precedence", func(t *testing.T) {
		requestEf10 := idsPerQuery(t, c, ds, graphOnly(efP(10)))
		targetEf1000 := idsPerQuery(t, c, ds, graphOnly(nil, forTarget(graphTarget, efP(1000))))
		diff := countDiffering(requestEf10, targetEf1000)
		t.Logf("graph target: request-wide ef=10 and per-target ef=1000 differ on %d of %d queries", diff, len(ds.queries))
		require.Positive(t, diff, "ef 10 and ef 1000 never differ, so the equalities below prove nothing")

		cases := []struct {
			name      string
			params    *pb.VectorIndexParams
			perTarget []*pb.VectorIndexParamsForTarget
			want      [][]string
		}{
			{
				name:      "per-target ef overrides request-wide ef",
				params:    efP(10),
				perTarget: []*pb.VectorIndexParamsForTarget{forTarget(graphTarget, efP(1000))},
				want:      targetEf1000,
			},
			{
				name:      "per-target ef overrides request-wide ef downwards",
				params:    efP(1000),
				perTarget: []*pb.VectorIndexParamsForTarget{forTarget(graphTarget, efP(10))},
				want:      requestEf10,
			},
			{
				name:      "entry for another target does not apply",
				params:    efP(10),
				perTarget: []*pb.VectorIndexParamsForTarget{forTarget(ivfTarget, efP(1000))},
				want:      requestEf10,
			},
			{
				name:      "entry overrides only the fields it sets",
				params:    efP(10),
				perTarget: []*pb.VectorIndexParamsForTarget{forTarget(graphTarget, probeP(5))},
				want:      requestEf10,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := idsPerQuery(t, c, ds, graphOnly(tc.params, tc.perTarget...))
				require.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("mixed request-wide bag on both targets", func(t *testing.T) {
		recall := meanRecallOf(t, c, ds, func(q []float32) *pb.SearchRequest {
			return multiTargetRequest(class, k, sum, both(q), &pb.VectorIndexParams{Ef: u32(1000), SearchProbe: u32(256)})
		})
		t.Logf("sum of both targets, ef=1000 search_probe=256: mean recall@%d %.3f", k, recall)
		assert.GreaterOrEqual(t, recall, 0.95)
	})
}

// TestVectorIndexParamsPerTarget_Validation checks the gRPC parser's
// rejections of malformed vector search params, on the shared two-vector
// collection and on the shared legacy single-vector one.
func TestVectorIndexParamsPerTarget_Validation(t *testing.T) {
	c := setupSingleNode(t)
	class, ds := twoVectorShared.get(t)
	legacy, _ := hnswShared.get(t)
	q := ds.queries[0]

	nearVec := func(params *pb.VectorIndexParams, perTarget ...*pb.VectorIndexParamsForTarget) *pb.SearchRequest {
		return multiTargetRequest(class, k, sum, both(q), params, perTarget...)
	}
	graphOnlyReq := func(perTarget ...*pb.VectorIndexParamsForTarget) *pb.SearchRequest {
		req := nearVectorRequest(class, q, k, nil)
		req.NearVector.Targets = &pb.Targets{TargetVectors: []string{graphTarget}}
		req.VectorIndexParamsForTargets = perTarget
		return req
	}
	bm25 := func(params *pb.VectorIndexParams, perTarget ...*pb.VectorIndexParamsForTarget) *pb.SearchRequest {
		return &pb.SearchRequest{
			Collection:                  class,
			Limit:                       k,
			Metadata:                    &pb.MetadataRequest{Uuid: true},
			Bm25Search:                  &pb.BM25{Query: "object"},
			VectorIndexParams:           params,
			VectorIndexParamsForTargets: perTarget,
			Uses_127Api:                 true,
		}
	}
	ef := &pb.VectorIndexParams{Ef: u32(10)}
	badEnum := pb.FilterStrategy(42)

	tests := []struct {
		name     string
		req      *pb.SearchRequest
		fragment string // empty: the request must succeed
	}{
		{"unknown target", nearVec(nil, forTarget("nope", ef)), `"nope" is not a target vector of this request`},
		{"configured target not searched", graphOnlyReq(forTarget(ivfTarget, ef)), `"ivf" is not a target vector of this request`},
		{"duplicate target", nearVec(nil, forTarget(graphTarget, ef), forTarget(graphTarget, ef)), `target "graph" given more than once`},
		{"empty target", nearVec(nil, forTarget("", ef)), "vector_index_params_for_targets[0]: target must be set"},
		{"nil params", nearVec(nil, forTarget(graphTarget, nil)), "vector_index_params_for_targets[graph]: params must be set"},
		{"unknown filter strategy", nearVec(&pb.VectorIndexParams{FilterStrategy: &badEnum}), "vector_index_params.filter_strategy must be one of sweeping, acorn, pathseer, got 42"},
		{"unknown per-target filter strategy", nearVec(nil, forTarget(graphTarget, &pb.VectorIndexParams{FilterStrategy: &badEnum})), "vector_index_params_for_targets[graph].filter_strategy must be one of sweeping, acorn, pathseer, got 42"},
		{"search_probe over cap", nearVec(probeP(maxSearchProbe + 1)), fmt.Sprintf("vector_index_params.search_probe %d exceeds HFRESH_MAX_SEARCH_PROBE (%d)", maxSearchProbe+1, maxSearchProbe)},
		{"per-target search_probe over cap", nearVec(nil, forTarget(ivfTarget, probeP(maxSearchProbe+1))), fmt.Sprintf("vector_index_params_for_targets[ivf].search_probe %d exceeds HFRESH_MAX_SEARCH_PROBE", maxSearchProbe+1)},
		{"per-target ef over cap", nearVec(nil, forTarget(graphTarget, efP(maxSearchEF+1))), fmt.Sprintf("vector_index_params_for_targets[graph].ef %d exceeds HNSW_MAX_SEARCH_EF", maxSearchEF+1)},
		{"per-target on bm25", bm25(nil, forTarget(graphTarget, ef)), "not applicable to bm25 search"},
		{"request-wide on bm25", bm25(ef), "not applicable to bm25 search"},
		{"per-target on legacy collection", func() *pb.SearchRequest {
			req := nearVectorRequest(legacy, q, k, nil)
			req.VectorIndexParamsForTargets = []*pb.VectorIndexParamsForTarget{forTarget("default", ef)}
			return req
		}(), "vector_index_params_for_targets requires named vectors; collection " + legacy + " has none"},
		{"params without a search", &pb.SearchRequest{
			Collection: class, Limit: k, Metadata: &pb.MetadataRequest{Uuid: true},
			VectorIndexParams: ef, Uses_127Api: true,
		}, "requires a near* or hybrid search"},
		{"ef and search_probe at cap", nearVec(&pb.VectorIndexParams{Ef: u32(maxSearchEF), SearchProbe: u32(maxSearchProbe)},
			forTarget(graphTarget, efP(maxSearchEF)), forTarget(ivfTarget, probeP(maxSearchProbe))), ""},
		{"every filter strategy", nearVec(nil,
			forTarget(graphTarget, &pb.VectorIndexParams{FilterStrategy: pb.FilterStrategy_FILTER_STRATEGY_PATHSEER.Enum()}),
			forTarget(ivfTarget, &pb.VectorIndexParams{FilterStrategy: pb.FilterStrategy_FILTER_STRATEGY_SWEEPING.Enum()})), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply, err := c.Search(callCtx(t), tt.req)
			if tt.fragment == "" {
				require.Len(t, idsOf(t, reply, err), k)
				return
			}
			requireInvalidArgument(t, err, tt.fragment)
		})
	}
}
