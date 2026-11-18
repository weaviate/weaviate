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
	cfg := &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}, HNSWMaxSearchEF: 10000}

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
			wantErr: "vector_search_params is not applicable to bm25 search",
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
