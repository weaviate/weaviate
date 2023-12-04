//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hybrid

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

type hybridTestSet struct {
	Documents      []*storobj.Object
	Weights        []float64
	InputScores    [][]float32
	ExpectedScores []float32
	ExpectedOrder  []uint64
}

func inputSet() []hybridTestSet {
	cases := []hybridTestSet{
		{
			Documents: []*storobj.Object{
				{Object: models.Object{}, Vector: []float32{1, 2, 3}, VectorLen: 3, DocID: 12345},
				{Object: models.Object{}, Vector: []float32{4, 5, 6}, VectorLen: 3, DocID: 12346},
				{Object: models.Object{}, Vector: []float32{7, 8, 9}, VectorLen: 3, DocID: 12347},
			},
			Weights:        []float64{0.5, 0.5},
			InputScores:    [][]float32{{1, 2, 3}, {0, 1, 2}},
			ExpectedScores: []float32{1, 0.5, 0},
			ExpectedOrder:  []uint64{2, 1, 0},
		},
	}

	/*
			{weights: []float64{0.5, 0.5}, inputScores: [][]float32{{0, 2, 0.1}, {0, 0.2, 2}}, expectedScores: []float32{0.55, 0.525, 0}, expectedOrder: []uint64{1, 2, 0}},
			{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{0.5, 0.5, 0}, {0, 0.01, 0.001}}, expectedScores: []float32{1, 0.75, 0.025}, expectedOrder: []uint64{1, 0, 2}},
			{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{}, {}}, expectedScores: []float32{}, expectedOrder: []uint64{}},
			{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{1}, {}}, expectedScores: []float32{0.75}, expectedOrder: []uint64{0}},
			{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{}, {1}}, expectedScores: []float32{0.25}, expectedOrder: []uint64{0}},
			{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{1, 2}, {}}, expectedScores: []float32{0.75, 0}, expectedOrder: []uint64{1, 0}},
			{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{}, {1, 2}}, expectedScores: []float32{0.25, 0}, expectedOrder: []uint64{1, 0}},
			{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{1, 1}, {1, 2}}, expectedScores: []float32{1, 0.75}, expectedOrder: []uint64{1, 0}},
			{weights: []float64{1}, inputScores: [][]float32{{1, 2, 3}}, expectedScores: []float32{1, 0.5, 0}, expectedOrder: []uint64{2, 1, 0}},
			{weights: []float64{0.75, 0.25}, inputScores: [][]float32{{1, 2, 3, 4}, {1, 2, 3}}, expectedScores: []float32{0.75, 0.75, 0.375, 0}, expectedOrder: []uint64{3, 2, 1, 0}},
		}
	*/
	return cases
}

func TestScoreFusionSearchWithoutModuleProvider(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	inputs := inputSet()
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			Alpha:           0.5,
			Query:           "some query",
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) {
		return inputs[0].Documents, inputs[0].InputScores[0], nil
	}
	dense := func([]float32) ([]*storobj.Object, []float32, error) {
		return inputs[0].Documents, inputs[0].InputScores[1], nil
	}

	res, err := Search(ctx, params, logger, sparse, dense, nil, nil)
	require.Nil(t, err)
	fmt.Printf("res: %v\n", res)
}

func TestScoreFusionSearchWithModuleProvider(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			Alpha:           0.5,
			Query:           "some query",
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) { return nil, nil, nil }
	dense := func([]float32) ([]*storobj.Object, []float32, error) { return nil, nil, nil }
	provider := &fakeModuleProvider{}
	_, err := Search(ctx, params, logger, sparse, dense, nil, provider)
	require.Nil(t, err)
}

func TestScoreFusionSearchWithSparseSearchOnly(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			Alpha:           0,
			Query:           "some query",
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "1889a225-3b28-477d-b8fc-5f6071bb4731",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{1, 2, 3},
				},
				Vector: []float32{1, 2, 3},
			},
		}, []float32{0.008}, nil
	}
	dense := func([]float32) ([]*storobj.Object, []float32, error) { return nil, nil, nil }
	res, err := Search(ctx, params, logger, sparse, dense, nil, nil)
	require.Nil(t, err)
	assert.Len(t, res, 1)
	assert.NotNil(t, res[0])
	assert.Contains(t, res[0].Result.ExplainScore, "(Result Set 'keyword') Document")
	assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
	assert.Equal(t, res[0].Result.Dist, float32(0.008))
}

func TestScoreFusionSearchWithDenseSearchOnly(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			Alpha:           1,
			Query:           "some query",
			Vector:          []float32{1, 2, 3},
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) { return nil, nil, nil }
	dense := func([]float32) ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "1889a225-3b28-477d-b8fc-5f6071bb4731",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{1, 2, 3},
				},
				Vector: []float32{1, 2, 3},
			},
		}, []float32{0.008}, nil
	}

	res, err := Search(ctx, params, logger, sparse, dense, nil, nil)
	require.Nil(t, err)
	assert.Len(t, res, 1)
	assert.NotNil(t, res[0])
	assert.Contains(t, res[0].Result.ExplainScore, "(Result Set 'vector') Document")
	assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
	assert.Equal(t, res[0].Result.Dist, float32(0.008))
}

func TestScoreFusionCombinedHybridSearch(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			Alpha:           0.5,
			Query:           "some query",
			Vector:          []float32{1, 2, 3},
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "1889a225-3b28-477d-b8fc-5f6071bb4731",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{1, 2, 3},
				},
				Vector: []float32{1, 2, 3},
			},
		}, []float32{0.008}, nil
	}
	dense := func([]float32) ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "79a636c2-3314-442e-a4d1-e94d7c0afc3a",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{4, 5, 6},
				},
				Vector: []float32{4, 5, 6},
			},
		}, []float32{0.008}, nil
	}
	res, err := Search(ctx, params, logger, sparse, dense, nil, nil)
	require.Nil(t, err)
	assert.Len(t, res, 2)
	assert.NotNil(t, res[0])
	assert.NotNil(t, res[1])
	assert.Contains(t, res[0].Result.ExplainScore, "(Result Set 'vector') Document")
	assert.Contains(t, res[0].Result.ExplainScore, "79a636c2-3314-442e-a4d1-e94d7c0afc3a")
	assert.Equal(t, res[0].Result.Vector, []float32{4, 5, 6})
	assert.Equal(t, res[0].Result.Dist, float32(0.008))
	assert.Contains(t, res[1].Result.ExplainScore, "(Result Set 'keyword') Document")
	assert.Contains(t, res[1].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Equal(t, res[1].Result.Vector, []float32{1, 2, 3})
	assert.Equal(t, res[1].Result.Dist, float32(0.008))
}

func TestScoreFusionWithSparseSubsearchFilter(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
			SubSearches: []searchparams.WeightedSearchResult{
				{
					Type: "sparseSearch",
					SearchParams: searchparams.KeywordRanking{
						Type:       "bm25",
						Properties: []string{"propA", "propB"},
						Query:      "some query",
					},
				},
			},
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "1889a225-3b28-477d-b8fc-5f6071bb4731",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{1, 2, 3},
					Additional: map[string]interface{}{"score": float32(0.008)},
				},
				Vector: []float32{1, 2, 3},
			},
		}, []float32{0.008}, nil
	}
	dense := func([]float32) ([]*storobj.Object, []float32, error) { return nil, nil, nil }
	res, err := Search(ctx, params, logger, sparse, dense, nil, nil)
	require.Nil(t, err)
	assert.Len(t, res, 1)
	assert.NotNil(t, res[0])
	assert.Contains(t, res[0].Result.ExplainScore, "(Result Set 'bm25f') Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
	assert.Equal(t, res[0].Result.Dist, float32(0.008))
}

func TestScoreFusionWithNearTextSubsearchFilter(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
			SubSearches: []searchparams.WeightedSearchResult{
				{
					Type: "nearText",
					SearchParams: searchparams.NearTextParams{
						Values:    []string{"some query"},
						Certainty: 0.8,
					},
				},
			},
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) { return nil, nil, nil }
	dense := func([]float32) ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "1889a225-3b28-477d-b8fc-5f6071bb4731",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{1, 2, 3},
					Additional: map[string]interface{}{"score": float32(0.008)},
				},
				Vector: []float32{1, 2, 3},
			},
		}, []float32{0.008}, nil
	}
	provider := &fakeModuleProvider{}
	res, err := Search(ctx, params, logger, sparse, dense, nil, provider)
	require.Nil(t, err)
	assert.Len(t, res, 1)
	assert.NotNil(t, res[0])
	assert.Contains(t, res[0].Result.ExplainScore, "(Result Set 'vector,nearText') Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
	assert.Equal(t, res[0].Result.Dist, float32(0.008))
}

func TestScoreFusionWithNearVectorSubsearchFilter(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
			SubSearches: []searchparams.WeightedSearchResult{
				{
					Type: "nearVector",
					SearchParams: searchparams.NearVector{
						Vector:    []float32{1, 2, 3},
						Certainty: 0.8,
					},
				},
			},
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) { return nil, nil, nil }
	dense := func([]float32) ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "1889a225-3b28-477d-b8fc-5f6071bb4731",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{1, 2, 3},
					Additional: map[string]interface{}{"score": float32(0.008)},
				},
				Vector: []float32{1, 2, 3},
			},
		}, []float32{0.008}, nil
	}
	provider := &fakeModuleProvider{}

	res, err := Search(ctx, params, logger, sparse, dense, nil, provider)
	require.Nil(t, err)
	assert.Len(t, res, 1)
	assert.NotNil(t, res[0])
	assert.Contains(t, res[0].Result.ExplainScore, "(Result Set 'vector,nearVector') Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
	assert.Equal(t, res[0].Result.Dist, float32(0.008))
}

func TestScoreFusionWithAllSubsearchFilters(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"
	params := &Params{
		HybridSearch: &searchparams.HybridSearch{
			Type:            "hybrid",
			FusionAlgorithm: common_filters.HybridRelativeScoreFusion,
			SubSearches: []searchparams.WeightedSearchResult{
				{
					Type: "nearVector",
					SearchParams: searchparams.NearVector{
						Vector:    []float32{1, 2, 3},
						Certainty: 0.8,
					},
					Weight: 100,
				},
				{
					Type: "nearText",
					SearchParams: searchparams.NearTextParams{
						Values:    []string{"some query"},
						Certainty: 0.8,
					},
					Weight: 2,
				},
				{
					Type: "sparseSearch",
					SearchParams: searchparams.KeywordRanking{
						Type:       "bm25",
						Properties: []string{"propA", "propB"},
						Query:      "some query",
					},
					Weight: 3,
				},
			},
		},
		Class: class,
	}
	sparse := func() ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "1889a225-3b28-477d-b8fc-5f6071bb4731",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{1, 2, 3},
					Additional: map[string]interface{}{"score": float32(0.008)},
				},
				Vector: []float32{1, 2, 3},
			},
		}, []float32{0.008}, nil
	}
	dense := func([]float32) ([]*storobj.Object, []float32, error) {
		return []*storobj.Object{
			{
				Object: models.Object{
					Class:      class,
					ID:         "79a636c2-3314-442e-a4d1-e94d7c0afc3a",
					Properties: map[string]any{"prop": "val"},
					Vector:     []float32{4, 5, 6},
					Additional: map[string]interface{}{"score": float32(0.8)},
				},
				Vector: []float32{4, 5, 6},
			},
		}, []float32{0.008}, nil
	}
	provider := &fakeModuleProvider{}
	res, err := Search(ctx, params, logger, sparse, dense, nil, provider)
	require.Nil(t, err)
	assert.Len(t, res, 2)
	assert.NotNil(t, res[0])
	assert.NotNil(t, res[1])
	assert.Contains(t, res[0].Result.ExplainScore, "(Result Set 'vector,nearText') Document 79a636c2-3314-442e-a4d1-e94d7c0afc3a")
	assert.Contains(t, res[0].Result.ExplainScore, "79a636c2-3314-442e-a4d1-e94d7c0afc3a")
	assert.Equal(t, res[0].Result.Vector, []float32{4, 5, 6})
	assert.Equal(t, res[0].Result.Dist, float32(0.008))
	assert.Contains(t, res[1].Result.ExplainScore, "(Result Set 'bm25f') Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Contains(t, res[1].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
	assert.Equal(t, res[1].Result.Vector, []float32{1, 2, 3})
	assert.Equal(t, res[1].Result.Dist, float32(0.008))
}
