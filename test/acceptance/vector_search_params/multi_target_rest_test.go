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
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

// TestVectorSearchParamsMultiTarget_REST checks that the REST multi-target
// surface (targetVectors, vectorPerTarget, targetCombination,
// vectorSearchParams, targetVectorSearchParams) runs the same search as the
// equivalent gRPC request, id by id, and rejects malformed bodies.
func TestVectorSearchParamsMultiTarget_REST(t *testing.T) {
	const (
		class = "MultiTargetREST"
		n     = 3000
	)
	c := setupSingleNode(t)
	uri := singleNode.GetWeaviate().URI()
	ds := newDataset(53, n, numQueries)
	createNamedVectorCollection(t, class, twoVectorIndexes(), nil, ds, n, types.ConsistencyLevelQuorum)
	waitForStableResults(t, c, ds, func(q []float32) *pb.SearchRequest {
		return multiTargetRequest(class, k, manual, isolate(ivfTarget, q), &pb.VectorSearchParams{SearchProbe: u32(1)})
	})

	targets := []string{graphTarget, ivfTarget}
	graphQ := func(q []float32) []targetQuery { return isolate(graphTarget, q) }
	ivfQ := func(q []float32) []targetQuery { return isolate(ivfTarget, q) }
	// Each case pairs REST body fields with the equivalent gRPC request; the
	// query vectors (vectorPerTarget on REST) come from queries. The
	// parameters keep the results inexact (low ef, low probe) and therefore
	// sensitive to a parameter that one surface drops.
	nearVectorCases := []struct {
		name      string
		rest      map[string]any
		queries   func(q []float32) []targetQuery
		method    pb.CombinationMethod
		params    *pb.VectorSearchParams
		perTarget []*pb.VectorSearchParamsForTarget
	}{
		{
			name: "manualWeights with per-target ef",
			rest: map[string]any{
				"targetCombination":        map[string]any{"method": "manualWeights", "weights": map[string]float64{graphTarget: 1, ivfTarget: 0}},
				"vectorSearchParams":       map[string]any{"ef": 1000},
				"targetVectorSearchParams": map[string]any{graphTarget: map[string]any{"ef": 10}},
			},
			queries:   graphQ,
			method:    manual,
			params:    &pb.VectorSearchParams{Ef: u32(1000)},
			perTarget: []*pb.VectorSearchParamsForTarget{forTarget(graphTarget, &pb.VectorSearchParams{Ef: u32(10)})},
		},
		{
			name: "manualWeights on ivf with per-target searchProbe",
			rest: map[string]any{
				"targetCombination":        map[string]any{"method": "manualWeights", "weights": map[string]float64{graphTarget: 0, ivfTarget: 1}},
				"targetVectorSearchParams": map[string]any{ivfTarget: map[string]any{"searchProbe": 1}, graphTarget: map[string]any{"ef": 10}},
			},
			queries: ivfQ,
			method:  manual,
			perTarget: []*pb.VectorSearchParamsForTarget{
				forTarget(ivfTarget, &pb.VectorSearchParams{SearchProbe: u32(1)}),
				forTarget(graphTarget, &pb.VectorSearchParams{Ef: u32(10)}),
			},
		},
		{
			name: "relativeScore with request-wide params",
			rest: map[string]any{
				"targetCombination":  map[string]any{"method": "relativeScore", "weights": map[string]float64{graphTarget: 0.7, ivfTarget: 0.3}},
				"vectorSearchParams": map[string]any{"ef": 10, "searchProbe": 1},
			},
			queries: func(q []float32) []targetQuery {
				return []targetQuery{{graphTarget, q, 0.7}, {ivfTarget, negate(q), 0.3}}
			},
			method: pb.CombinationMethod_COMBINATION_METHOD_TYPE_RELATIVE_SCORE,
			params: &pb.VectorSearchParams{Ef: u32(10), SearchProbe: u32(1)},
		},
	}
	for _, tc := range nearVectorCases {
		t.Run("near-vector "+tc.name, func(t *testing.T) {
			for qi, q := range ds.queries {
				tq := tc.queries(q)
				vpt := map[string]any{}
				for _, x := range tq {
					vpt[x.name] = x.vec
				}
				body := map[string]any{"limit": k, "targetVectors": targets, "vectorPerTarget": vpt}
				for key, v := range tc.rest {
					body[key] = v
				}
				got := restOKIDs(t, uri, class, "near-vector", body)
				require.Len(t, got, k)
				want := searchIDs(t, c, multiTargetRequest(class, k, tc.method, tq, tc.params, tc.perTarget...))
				require.Equal(t, want, got, "query %d", qi)
			}
		})
	}

	t.Run("near-vector single vector fans out to every target", func(t *testing.T) {
		// No targetCombination: several targets default to minimum, which
		// needs no weights (sum and average are pinned separately below).
		params := map[string]any{"ef": 10, "searchProbe": 1}
		for qi, q := range ds.queries {
			fanOut := map[string]any{"vector": q, "limit": k, "targetVectors": targets, "vectorSearchParams": params}
			perTarget := map[string]any{
				"vectorPerTarget": map[string]any{graphTarget: q, ivfTarget: q},
				"limit":           k, "targetVectors": targets, "vectorSearchParams": params,
			}
			fanOutIDs := restOKIDs(t, uri, class, "near-vector", fanOut)
			require.Len(t, fanOutIDs, k)
			require.Equal(t, restOKIDs(t, uri, class, "near-vector", perTarget), fanOutIDs, "query %d", qi)

			// gRPC fan-out: one vector, several targets
			want := searchIDs(t, c, &pb.SearchRequest{
				Collection: class, Limit: k, Metadata: &pb.MetadataRequest{Uuid: true},
				NearVector: &pb.NearVector{
					Vectors: singleVectors(q),
					Targets: &pb.Targets{TargetVectors: targets},
				},
				VectorSearchParams: &pb.VectorSearchParams{Ef: u32(10), SearchProbe: u32(1)},
				Uses_127Api:        true,
			})
			require.Equal(t, want, fanOutIDs, "query %d", qi)
		}
	})

	// RED: REST sum and average combinations fail with HTTP 500 "number of
	// weights in join does not match number of results".
	// dto.NewTargetCombination returns TargetCombination{Type: Sum|Average}
	// without weights, while CombineMultiTargetResults requires one weight
	// per target for every type except minimum. gRPC fills them (1 for sum,
	// 1/n for average, extractTargets), so the two surfaces diverge.
	t.Run("sum and average combinations match gRPC", func(t *testing.T) {
		q := ds.queries[0]
		cases := []struct {
			method     string
			grpcMethod pb.CombinationMethod
		}{
			{"sum", sum},
			{"average", pb.CombinationMethod_COMBINATION_METHOD_TYPE_AVERAGE},
		}
		for _, tc := range cases {
			t.Run("near-vector "+tc.method, func(t *testing.T) {
				got := restOKIDs(t, uri, class, "near-vector", map[string]any{
					"vector": q, "limit": k, "targetVectors": targets,
					"targetCombination": map[string]any{"method": tc.method},
				})
				want := searchIDs(t, c, multiTargetRequest(class, k, tc.grpcMethod, both(q), nil))
				require.Equal(t, want, got)
			})
			t.Run("near-object "+tc.method, func(t *testing.T) {
				got := restOKIDs(t, uri, class, "near-object", map[string]any{
					"id": idFor(1), "limit": k, "targetVectors": targets,
					"targetCombination": map[string]any{"method": tc.method},
				})
				want := searchIDs(t, c, &pb.SearchRequest{
					Collection: class, Limit: k, Metadata: &pb.MetadataRequest{Uuid: true},
					NearObject:  &pb.NearObject{Id: idFor(1), Targets: &pb.Targets{TargetVectors: targets, Combination: tc.grpcMethod}},
					Uses_127Api: true,
				})
				require.Equal(t, want, got)
			})
		}
	})

	t.Run("near-object with targetVectors", func(t *testing.T) {
		for i := 0; i < numQueries; i++ {
			id := idFor(i * 97)
			got := restOKIDs(t, uri, class, "near-object", map[string]any{
				"id": id, "limit": k, "targetVectors": targets,
				"targetCombination":        map[string]any{"method": "manualWeights", "weights": map[string]float64{graphTarget: 0.5, ivfTarget: 0.5}},
				"targetVectorSearchParams": map[string]any{graphTarget: map[string]any{"ef": 10}, ivfTarget: map[string]any{"searchProbe": 1}},
			})
			want := searchIDs(t, c, &pb.SearchRequest{
				Collection: class, Limit: k, Metadata: &pb.MetadataRequest{Uuid: true},
				NearObject: &pb.NearObject{Id: id, Targets: &pb.Targets{
					TargetVectors: targets, Combination: manual,
					WeightsForTargets: []*pb.WeightsForTarget{{Target: graphTarget, Weight: 0.5}, {Target: ivfTarget, Weight: 0.5}},
				}},
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget(graphTarget, &pb.VectorSearchParams{Ef: u32(10)}),
					forTarget(ivfTarget, &pb.VectorSearchParams{SearchProbe: u32(1)}),
				},
				Uses_127Api: true,
			})
			require.Len(t, got, k)
			require.Equal(t, want, got, "object %s", id)
		}
	})

	t.Run("hybrid with targetVectors matches gRPC", func(t *testing.T) {
		alpha := float32(1)
		weights := map[string]float64{graphTarget: 0.5, ivfTarget: 0.5}
		for qi, q := range ds.queries {
			got := restOKIDs(t, uri, class, "hybrid", map[string]any{
				"query": "object", "alpha": 1, "vector": q, "limit": k, "targetVectors": targets,
				"targetCombination":        map[string]any{"method": "manualWeights", "weights": weights},
				"targetVectorSearchParams": map[string]any{graphTarget: map[string]any{"ef": 10}, ivfTarget: map[string]any{"searchProbe": 1}},
			})
			want := searchIDs(t, c, &pb.SearchRequest{
				Collection: class, Limit: k, Metadata: &pb.MetadataRequest{Uuid: true},
				HybridSearch: &pb.Hybrid{
					Query: "object", AlphaParam: &alpha, UseAlphaParam: true, Vectors: singleVectors(q),
					Targets: &pb.Targets{
						TargetVectors: targets, Combination: manual,
						WeightsForTargets: []*pb.WeightsForTarget{{Target: graphTarget, Weight: 0.5}, {Target: ivfTarget, Weight: 0.5}},
					},
				},
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget(graphTarget, &pb.VectorSearchParams{Ef: u32(10)}),
					forTarget(ivfTarget, &pb.VectorSearchParams{SearchProbe: u32(1)}),
				},
				Uses_127Api: true,
			})
			require.Len(t, got, k)
			require.Equal(t, want, got, "query %d", qi)
		}
	})

	t.Run("errors", func(t *testing.T) {
		q := ds.queries[0]
		vpt := map[string]any{graphTarget: q, ivfTarget: q}
		tests := []struct {
			name       string
			searchType string
			body       map[string]any
			wantStatus int
		}{
			{"targetVector and targetVectors", "near-vector", map[string]any{"vector": q, "targetVector": graphTarget, "targetVectors": targets}, http.StatusUnprocessableEntity},
			{"duplicate target", "near-vector", map[string]any{"vector": q, "targetVectors": []string{graphTarget, graphTarget}}, http.StatusBadRequest},
			{"unknown target", "near-vector", map[string]any{"vector": q, "targetVectors": []string{graphTarget, "nope"}}, http.StatusBadRequest},
			{"weights with sum", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "targetCombination": map[string]any{"method": "sum", "weights": map[string]float64{graphTarget: 1, ivfTarget: 1}}}, http.StatusUnprocessableEntity},
			{"manualWeights missing a weight", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "targetCombination": map[string]any{"method": "manualWeights", "weights": map[string]float64{graphTarget: 1}}}, http.StatusUnprocessableEntity},
			{"unknown combination method", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "targetCombination": map[string]any{"method": "max"}}, http.StatusUnprocessableEntity},
			{"neither vector nor vectorPerTarget", "near-vector", map[string]any{"targetVectors": targets}, http.StatusUnprocessableEntity},
			{"both vector and vectorPerTarget", "near-vector", map[string]any{"vector": q, "vectorPerTarget": vpt, "targetVectors": targets}, http.StatusUnprocessableEntity},
			{"vectorPerTarget extra key", "near-vector", map[string]any{"targetVectors": []string{graphTarget}, "vectorPerTarget": vpt}, http.StatusUnprocessableEntity},
			{"vectorPerTarget missing key", "near-vector", map[string]any{"targetVectors": targets, "vectorPerTarget": map[string]any{graphTarget: q}}, http.StatusUnprocessableEntity},
			{"targetVectorSearchParams unknown key", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "targetVectorSearchParams": map[string]any{"nope": map[string]any{"ef": 10}}}, http.StatusUnprocessableEntity},
			{"targetVectorSearchParams target not searched", "near-vector", map[string]any{"vector": q, "targetVectors": []string{graphTarget}, "targetVectorSearchParams": map[string]any{ivfTarget: map[string]any{"searchProbe": 10}}}, http.StatusUnprocessableEntity},
			{"searchProbe negative", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "vectorSearchParams": map[string]any{"searchProbe": -1}}, http.StatusUnprocessableEntity},
			{"per-target searchProbe negative", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "targetVectorSearchParams": map[string]any{ivfTarget: map[string]any{"searchProbe": -1}}}, http.StatusUnprocessableEntity},
			{"searchProbe above cap", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "vectorSearchParams": map[string]any{"searchProbe": 10001}}, http.StatusUnprocessableEntity},
			{"filterStrategy fast", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "vectorSearchParams": map[string]any{"filterStrategy": "fast"}}, http.StatusUnprocessableEntity},
			{"per-target filterStrategy fast", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "targetVectorSearchParams": map[string]any{graphTarget: map[string]any{"filterStrategy": "fast"}}}, http.StatusUnprocessableEntity},
			{"bm25 with targetVectorSearchParams", "bm25", map[string]any{"query": "object", "targetVectorSearchParams": map[string]any{graphTarget: map[string]any{"ef": 10}}}, http.StatusUnprocessableEntity},
			{"wrong-typed searchProbe", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "vectorSearchParams": map[string]any{"searchProbe": "ten"}}, http.StatusBadRequest},
			{"near-object targetVector and targetVectors", "near-object", map[string]any{"id": idFor(0), "targetVector": graphTarget, "targetVectors": targets}, http.StatusUnprocessableEntity},
			{"empty targetVectorSearchParams entry is a no-op", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "targetVectorSearchParams": map[string]any{graphTarget: map[string]any{}}}, http.StatusOK},
			{"null targetVectorSearchParams entry", "near-vector", map[string]any{"vector": q, "targetVectors": targets, "targetVectorSearchParams": map[string]any{graphTarget: nil}}, http.StatusUnprocessableEntity},
			{"hybrid unknown target in targetVectorSearchParams", "hybrid", map[string]any{"query": "object", "vector": q, "targetVectors": targets, "targetVectorSearchParams": map[string]any{"nope": map[string]any{"ef": 10}}}, http.StatusUnprocessableEntity},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tt.body["limit"] = k
				code, out := restSearch(t, uri, class, tt.searchType, tt.body)
				require.Equal(t, tt.wantStatus, code, "%v", out)
				t.Logf("%d: %v", code, out)
			})
		}
	})
}
