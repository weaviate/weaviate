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

	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/byteops"
	"github.com/weaviate/weaviate/usecases/config"
)

// TestParseHybridSelection: hybrid diversity is carried on the top-level
// Hybrid.selection field; selection on a near sub-query is ignored.
func TestParseHybridSelection(t *testing.T) {
	parser := NewParser(false, getClass, nil, false)
	cfg := &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}}

	mmr := func() *pb.Selection {
		return &pb.Selection{Selection: &pb.Selection_Mmr{
			Mmr: &pb.Selection_MMR{Limit: ptr(uint32(3)), Balance: ptr(float32(0))},
		}}
	}

	t.Run("top-level Hybrid selection reaches GetParams.Selection", func(t *testing.T) {
		out, err := parser.Search(&pb.SearchRequest{
			Collection: classname,
			HybridSearch: &pb.Hybrid{
				Query:      "q",
				Selection:  mmr(),
				NearVector: &pb.NearVector{VectorBytes: byteops.Fp32SliceToBytes([]float32{1, 2, 3})},
			},
		}, cfg)
		require.NoError(t, err)
		require.NotNil(t, out.Selection)
		require.NotNil(t, out.Selection.MMR)
		require.Equal(t, uint32(3), out.Selection.MMR.Limit)
		require.Equal(t, float32(0), out.Selection.MMR.Balance)
	})

	t.Run("selection on hybrid near_vector sub-query is ignored", func(t *testing.T) {
		out, err := parser.Search(&pb.SearchRequest{
			Collection: classname,
			HybridSearch: &pb.Hybrid{
				Query: "q",
				NearVector: &pb.NearVector{
					VectorBytes: byteops.Fp32SliceToBytes([]float32{1, 2, 3}),
					Selection:   mmr(),
				},
			},
		}, cfg)
		require.NoError(t, err)
		require.Nil(t, out.Selection, "nested near_vector selection must not drive hybrid diversity")
	})

	t.Run("selection on hybrid near_text sub-query is ignored", func(t *testing.T) {
		out, err := parser.Search(&pb.SearchRequest{
			Collection: classname,
			HybridSearch: &pb.Hybrid{
				Query: "q",
				NearText: &pb.NearTextSearch{
					Query:     []string{"cats"},
					Selection: mmr(),
				},
			},
		}, cfg)
		require.NoError(t, err)
		require.Nil(t, out.Selection, "nested near_text selection must not drive hybrid diversity")
	})

	t.Run("no selection leaves GetParams.Selection nil", func(t *testing.T) {
		out, err := parser.Search(&pb.SearchRequest{
			Collection:   classname,
			HybridSearch: &pb.Hybrid{Query: "q"},
		}, cfg)
		require.NoError(t, err)
		require.Nil(t, out.Selection)
	})
}

func TestParseSelectionMultiVectorRejected(t *testing.T) {
	parser := NewParser(false, getClass, nil, false)
	cfg := &config.Config{QueryDefaults: config.QueryDefaults{Limit: 10}}

	mmr := func() *pb.Selection {
		return &pb.Selection{Selection: &pb.Selection_Mmr{
			Mmr: &pb.Selection_MMR{Limit: ptr(uint32(3)), Balance: ptr(float32(0))},
		}}
	}

	t.Run("named multi-vector target errors", func(t *testing.T) {
		_, err := parser.Search(&pb.SearchRequest{
			Collection: multiVecClassWithColBERT,
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{
					{VectorBytes: byteops.Fp32SliceOfSlicesToBytes([][]float32{{1, 2, 3}}), Type: pb.Vectors_VECTOR_TYPE_MULTI_FP32},
				},
				TargetVectors: []string{"custom_colbert"},
				Selection:     mmr(),
			},
		}, cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "multi-vector")
	})

	t.Run("default (unnamed) multi-vector target errors", func(t *testing.T) {
		_, err := parser.Search(&pb.SearchRequest{
			Collection: legacyMultiVecClass,
			HybridSearch: &pb.Hybrid{
				Query:     "q",
				Selection: mmr(),
			},
		}, cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "multi-vector")
	})

	t.Run("default single-vector target is allowed", func(t *testing.T) {
		out, err := parser.Search(&pb.SearchRequest{
			Collection: classname,
			HybridSearch: &pb.Hybrid{
				Query:     "q",
				Selection: mmr(),
			},
		}, cfg)
		require.NoError(t, err)
		require.NotNil(t, out.Selection)
	})
}
