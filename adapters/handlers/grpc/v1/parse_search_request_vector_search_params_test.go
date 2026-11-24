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

package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/entities/searchparams"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/byteops"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// Negative ef values cannot be expressed: the proto field is a uint32, so the
// gRPC parser needs no negative branch (the REST surface validates that).
func TestGRPCSearchRequestVectorSearchParams(t *testing.T) {
	parser := NewParser(false, getClass, nil, namespacing.Disabled)
	cfg := &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}, HNSWMaxSearchEF: 10000, HFreshMaxSearchProbe: 10000}

	vec := byteops.Fp32SliceToBytes([]float32{1, 2, 3})
	nearVector := func() *pb.NearVector { return &pb.NearVector{VectorBytes: vec} }
	vsp := func(ef uint32) *pb.VectorSearchParams { return &pb.VectorSearchParams{Ef: ptr(ef)} }

	tests := []struct {
		name    string
		req     *pb.SearchRequest
		want    *searchparams.VectorSearchParams
		wantErr string
	}{
		{
			name: "unset",
			req:  &pb.SearchRequest{Collection: classname, NearVector: nearVector()},
		},
		{
			name: "ef zero",
			req:  &pb.SearchRequest{Collection: classname, NearVector: nearVector(), VectorSearchParams: vsp(0)},
		},
		{
			name: "ef 800 near vector",
			req:  &pb.SearchRequest{Collection: classname, NearVector: nearVector(), VectorSearchParams: vsp(800)},
			want: &searchparams.VectorSearchParams{EF: 800},
		},
		{
			name: "hybrid with vector search params",
			req: &pb.SearchRequest{
				Collection:         classname,
				HybridSearch:       &pb.Hybrid{Query: "q", NearVector: nearVector()},
				VectorSearchParams: vsp(800),
			},
			want: &searchparams.VectorSearchParams{EF: 800},
		},
		{
			name: "ef equals cap",
			req:  &pb.SearchRequest{Collection: classname, NearVector: nearVector(), VectorSearchParams: vsp(10000)},
			want: &searchparams.VectorSearchParams{EF: 10000},
		},
		{
			name:    "ef above cap",
			req:     &pb.SearchRequest{Collection: classname, NearVector: nearVector(), VectorSearchParams: vsp(10001)},
			wantErr: "vector_search_params.ef 10001 exceeds HNSW_MAX_SEARCH_EF (10000)",
		},
		{
			// Parity with REST, which answers 422 for bm25 plus vectorSearchParams.
			name: "bm25 only with ef",
			req: &pb.SearchRequest{
				Collection:         classname,
				Bm25Search:         &pb.BM25{Query: "q"},
				VectorSearchParams: vsp(800),
			},
			wantErr: "vector_search_params / vector_search_params_for_targets is not applicable to bm25 search",
		},
		{
			// A plain object listing runs no vector search either.
			name: "no search clause with ef",
			req: &pb.SearchRequest{
				Collection:         classname,
				VectorSearchParams: vsp(800),
			},
			wantErr: "vector_search_params / vector_search_params_for_targets requires a near* or hybrid search",
		},
		{
			name: "no search clause without params",
			req:  &pb.SearchRequest{Collection: classname},
		},
		{
			name: "bm25 with hybrid and ef",
			req: &pb.SearchRequest{
				Collection:         classname,
				Bm25Search:         &pb.BM25{Query: "q"},
				HybridSearch:       &pb.Hybrid{Query: "q", NearVector: nearVector()},
				VectorSearchParams: vsp(800),
			},
			want: &searchparams.VectorSearchParams{EF: 800},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := parser.Search(tt.req, cfg)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, out.VectorSearchParams)
		})
	}
}

func TestGRPCSearchRequestVectorSearchParamsFields(t *testing.T) {
	parser := NewParser(false, getClass, nil, namespacing.Disabled)
	cfg := &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}, HNSWMaxSearchEF: 10000, HFreshMaxSearchProbe: 10000}

	vec := byteops.Fp32SliceToBytes([]float32{1, 2, 3})
	nearVector := func(targets ...string) *pb.NearVector {
		nv := &pb.NearVector{VectorBytes: vec}
		if len(targets) > 0 {
			nv.Targets = &pb.Targets{TargetVectors: targets}
		}
		return nv
	}
	strategy := func(fs pb.FilterStrategy) *pb.VectorSearchParams {
		return &pb.VectorSearchParams{FilterStrategy: &fs}
	}
	forTarget := func(target string, p *pb.VectorSearchParams) *pb.VectorSearchParamsForTarget {
		return &pb.VectorSearchParamsForTarget{Target: target, Params: p}
	}

	tests := []struct {
		name    string
		req     *pb.SearchRequest
		want    *searchparams.VectorSearchParams
		wantErr string
	}{
		{
			name: "search probe only",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: &pb.VectorSearchParams{SearchProbe: ptr(uint32(64))},
			},
			want: &searchparams.VectorSearchParams{SearchProbe: 64},
		},
		{
			name: "search probe equals cap",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: &pb.VectorSearchParams{SearchProbe: ptr(uint32(10000))},
			},
			want: &searchparams.VectorSearchParams{SearchProbe: 10000},
		},
		{
			name: "search probe above cap",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: &pb.VectorSearchParams{SearchProbe: ptr(uint32(10001))},
			},
			wantErr: "vector_search_params.search_probe 10001 exceeds HFRESH_MAX_SEARCH_PROBE (10000)",
		},
		{
			name: "filter strategy sweeping",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: strategy(pb.FilterStrategy_FILTER_STRATEGY_SWEEPING),
			},
			want: &searchparams.VectorSearchParams{FilterStrategy: searchparams.FilterStrategySweeping},
		},
		{
			name: "filter strategy acorn",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: strategy(pb.FilterStrategy_FILTER_STRATEGY_ACORN),
			},
			want: &searchparams.VectorSearchParams{FilterStrategy: searchparams.FilterStrategyAcorn},
		},
		{
			name: "filter strategy pathseer",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: strategy(pb.FilterStrategy_FILTER_STRATEGY_PATHSEER),
			},
			want: &searchparams.VectorSearchParams{FilterStrategy: searchparams.FilterStrategyPathseer},
		},
		{
			name: "filter strategy unspecified is a no-op",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: strategy(pb.FilterStrategy_FILTER_STRATEGY_UNSPECIFIED),
			},
		},
		{
			name: "filter strategy unknown enum value",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: strategy(pb.FilterStrategy(42)),
			},
			wantErr: "vector_search_params.filter_strategy must be one of sweeping, acorn, pathseer, got 42",
		},
		{
			name: "all fields set",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: &pb.VectorSearchParams{
					Ef: ptr(uint32(200)), SearchProbe: ptr(uint32(32)),
					FilterStrategy: ptr(pb.FilterStrategy_FILTER_STRATEGY_ACORN),
				},
			},
			want: &searchparams.VectorSearchParams{EF: 200, SearchProbe: 32, FilterStrategy: searchparams.FilterStrategyAcorn},
		},
		{
			name: "request-wide all zero and no entries",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParams: &pb.VectorSearchParams{
					Ef: ptr(uint32(0)), SearchProbe: ptr(uint32(0)),
					FilterStrategy: ptr(pb.FilterStrategy_FILTER_STRATEGY_UNSPECIFIED),
				},
			},
		},
		{
			name: "per target on two named vectors",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParams: &pb.VectorSearchParams{Ef: ptr(uint32(100))},
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
					forTarget("second", strategy(pb.FilterStrategy_FILTER_STRATEGY_SWEEPING)),
				},
			},
			want: &searchparams.VectorSearchParams{
				EF: 100,
				PerTarget: map[string]searchparams.VectorSearchParams{
					"first":  {EF: 300},
					"second": {FilterStrategy: searchparams.FilterStrategySweeping},
				},
			},
		},
		{
			name: "per target only",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("second", &pb.VectorSearchParams{SearchProbe: ptr(uint32(48))}),
				},
			},
			want: &searchparams.VectorSearchParams{
				PerTarget: map[string]searchparams.VectorSearchParams{"second": {SearchProbe: 48}},
			},
		},
		{
			// An all-zero entry is a no-op but is kept so its name is validated.
			name: "per target all zero entry",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", &pb.VectorSearchParams{}),
				},
			},
			// validated for its name, but a no-op entry is not stored
			want: nil,
		},
		{
			name: "per target with hybrid",
			req: &pb.SearchRequest{
				Collection: multiVecClass,
				HybridSearch: &pb.Hybrid{
					Query: "q", NearVector: nearVector(),
					Targets: &pb.Targets{TargetVectors: []string{"first", "second"}},
				},
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			want: &searchparams.VectorSearchParams{
				PerTarget: map[string]searchparams.VectorSearchParams{"first": {EF: 300}},
			},
		},
		{
			name: "per target on implicit single named vector",
			req: &pb.SearchRequest{
				Collection: singleNamedVecClass, NearVector: nearVector(),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("default", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			want: &searchparams.VectorSearchParams{
				PerTarget: map[string]searchparams.VectorSearchParams{"default": {EF: 300}},
			},
		},
		{
			name: "per target unknown target",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("custom", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			wantErr: `vector_search_params_for_targets: "custom" is not a target vector of this request (targets: [first second])`,
		},
		{
			name: "per target duplicate target",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
					forTarget("first", &pb.VectorSearchParams{Ef: ptr(uint32(400))}),
				},
			},
			wantErr: `vector_search_params_for_targets: target "first" given more than once`,
		},
		{
			name: "per target empty target name",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			wantErr: "vector_search_params_for_targets[0]: target must be set",
		},
		{
			name: "per target nil params",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", nil),
				},
			},
			wantErr: "vector_search_params_for_targets[first]: params must be set",
		},
		{
			name: "per target ef above cap",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("second", &pb.VectorSearchParams{Ef: ptr(uint32(10001))}),
				},
			},
			wantErr: "vector_search_params_for_targets[second].ef 10001 exceeds HNSW_MAX_SEARCH_EF (10000)",
		},
		{
			name: "per target search probe above cap",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("second", &pb.VectorSearchParams{SearchProbe: ptr(uint32(10001))}),
				},
			},
			wantErr: "vector_search_params_for_targets[second].search_probe 10001 exceeds HFRESH_MAX_SEARCH_PROBE (10000)",
		},
		{
			name: "per target unknown filter strategy",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "second"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", strategy(pb.FilterStrategy(7))),
				},
			},
			wantErr: "vector_search_params_for_targets[first].filter_strategy must be one of sweeping, acorn, pathseer, got 7",
		},
		{
			name: "per target on legacy class",
			req: &pb.SearchRequest{
				Collection: classname, NearVector: nearVector(),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("default", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			wantErr: "vector_search_params_for_targets requires named vectors; collection TestClass has none",
		},
		{
			name: "per target on hybrid against legacy class",
			req: &pb.SearchRequest{
				Collection:   classname,
				HybridSearch: &pb.Hybrid{Query: "q", NearVector: nearVector()},
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("default", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			wantErr: "requires named vectors",
		},
		{
			// A target name may repeat in the target list; the single entry
			// for it is accepted and stored once.
			name: "per target with repeated target name",
			req: &pb.SearchRequest{
				Collection: multiVecClass, NearVector: nearVector("first", "first"),
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			want: &searchparams.VectorSearchParams{
				PerTarget: map[string]searchparams.VectorSearchParams{"first": {EF: 300}},
			},
		},
		{
			name: "per target on bm25 only",
			req: &pb.SearchRequest{
				Collection: multiVecClass, Bm25Search: &pb.BM25{Query: "q"},
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			wantErr: "vector_search_params / vector_search_params_for_targets is not applicable to bm25 search",
		},
		{
			name: "per target without search clause",
			req: &pb.SearchRequest{
				Collection: multiVecClass,
				VectorSearchParamsForTargets: []*pb.VectorSearchParamsForTarget{
					forTarget("first", &pb.VectorSearchParams{Ef: ptr(uint32(300))}),
				},
			},
			wantErr: "vector_search_params / vector_search_params_for_targets requires a near* or hybrid search",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := parser.Search(tt.req, cfg)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
				require.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, out.VectorSearchParams)
		})
	}
}

func TestValidateVectorSearchParamsUnsetCaps(t *testing.T) {
	// unresolved caps (0) fall back to the default QUERY_MAXIMUM_RESULTS
	// instead of rejecting every ef and search_probe
	got, err := validateVectorSearchParams(&pb.VectorSearchParams{Ef: ptr(uint32(500)), SearchProbe: ptr(uint32(64))}, 0, 0, "vector_search_params.")
	require.NoError(t, err)
	require.Equal(t, searchparams.VectorSearchParams{EF: 500, SearchProbe: 64}, got)

	_, err = validateVectorSearchParams(&pb.VectorSearchParams{Ef: ptr(uint32(10001))}, 0, 0, "vector_search_params.")
	require.ErrorContains(t, err, "vector_search_params.ef 10001 exceeds HNSW_MAX_SEARCH_EF (10000)")
}
