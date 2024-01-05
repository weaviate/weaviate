//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hybrid

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestSearcher(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	class := "HybridClass"

	tests := []struct {
		name string
		f    func(t *testing.T)
	}{
		{
			name: "with module provider",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type:  "hybrid",
						Alpha: 0.5,
						Query: "some query",
					},
					Class: class,
				}
				sparse := func() ([]*storobj.Object, []float32, error) { return nil, nil, nil }
				dense := func([]float32) ([]*storobj.Object, []float32, error) { return nil, nil, nil }
				provider := &fakeModuleProvider{}
				provider.On("VectorFromInput", ctx, class, params.Query).Return([]float32{1, 2, 3}, nil)

				_, err := Search(ctx, params, logger, sparse, dense, nil, provider)
				require.Nil(t, err)
			},
		},
		{
			name: "without module provider",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type:  "hybrid",
						Alpha: 0.5,
						Query: "some query",
					},
					Class: class,
				}
				sparse := func() ([]*storobj.Object, []float32, error) { return nil, nil, nil }
				dense := func([]float32) ([]*storobj.Object, []float32, error) { return nil, nil, nil }

				_, err := Search(ctx, params, logger, sparse, dense, nil, nil)
				require.Nil(t, err)
			},
		},
		{
			name: "with sparse search only",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type:  "hybrid",
						Alpha: 0,
						Query: "some query",
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
				assert.Contains(t, res[0].Result.ExplainScore, "(bm25)")
				assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
				assert.Equal(t, res[0].Result.Dist, float32(0.008))
			},
		},
		{
			name: "with dense search only",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type:   "hybrid",
						Alpha:  1,
						Query:  "some query",
						Vector: []float32{1, 2, 3},
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
				assert.Contains(t, res[0].Result.ExplainScore, "(vector)")
				assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
				assert.Equal(t, res[0].Result.Dist, float32(0.008))
			},
		},
		{
			name: "combined hybrid search",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type:   "hybrid",
						Alpha:  0.5,
						Query:  "some query",
						Vector: []float32{1, 2, 3},
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
				assert.Contains(t, res[0].Result.ExplainScore, "(vector)")
				assert.Contains(t, res[0].Result.ExplainScore, "79a636c2-3314-442e-a4d1-e94d7c0afc3a")
				assert.Equal(t, res[0].Result.Vector, []float32{4, 5, 6})
				assert.Equal(t, res[0].Result.Dist, float32(0.008))
				assert.Contains(t, res[1].Result.ExplainScore, "(bm25)")
				assert.Contains(t, res[1].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[1].Result.Vector, []float32{1, 2, 3})
				assert.Equal(t, res[1].Result.Dist, float32(0.008))
			},
		},
		{
			name: "with sparse subsearch filter",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type: "hybrid",
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
				dense := func([]float32) ([]*storobj.Object, []float32, error) {
					return nil, nil, nil
				}
				res, err := Search(ctx, params, logger, sparse, dense, nil, nil)
				require.Nil(t, err)
				assert.Len(t, res, 1)
				assert.NotNil(t, res[0])
				assert.Contains(t, res[0].Result.ExplainScore, "(Result Set bm25f) Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
				assert.Equal(t, res[0].Result.Dist, float32(0.008))
			},
		},
		{
			name: "with nearText subsearch filter",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type: "hybrid",
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
				sparse := func() ([]*storobj.Object, []float32, error) {
					return nil, nil, nil
				}
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
				provider.On("VectorFromInput", ctx, class,
					params.HybridSearch.SubSearches.([]searchparams.WeightedSearchResult)[0].
						SearchParams.(searchparams.NearTextParams).Values[0]).Return([]float32{1, 2, 3}, nil)
				provider.On("VectorFromInput", ctx, class, "").Return([]float32{1, 2, 3}, nil)
				res, err := Search(ctx, params, logger, sparse, dense, nil, provider)
				require.Nil(t, err)
				assert.Len(t, res, 1)
				assert.NotNil(t, res[0])
				assert.Contains(t, res[0].Result.ExplainScore, "(Result Set vector,nearText) Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
				assert.Equal(t, res[0].Result.Dist, float32(0.008))
			},
		},
		{
			name: "with nearVector subsearch filter",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type: "hybrid",
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
				sparse := func() ([]*storobj.Object, []float32, error) {
					return nil, nil, nil
				}
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
				provider.On("VectorFromInput", ctx, class,
					params.HybridSearch.SubSearches.([]searchparams.WeightedSearchResult)[0].
						SearchParams.(searchparams.NearVector).Vector).Return([]float32{1, 2, 3}, nil)
				provider.On("VectorFromInput", ctx, class, "").Return([]float32{1, 2, 3}, nil)
				res, err := Search(ctx, params, logger, sparse, dense, nil, provider)
				require.Nil(t, err)
				assert.Len(t, res, 1)
				assert.NotNil(t, res[0])
				assert.Contains(t, res[0].Result.ExplainScore, "(Result Set vector,nearVector) Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Contains(t, res[0].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Result.Vector, []float32{1, 2, 3})
				assert.Equal(t, res[0].Result.Dist, float32(0.008))
			},
		},
		{
			name: "with all subsearch filters",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type: "hybrid",
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
				provider.On("VectorFromInput", ctx, class,
					params.HybridSearch.SubSearches.([]searchparams.WeightedSearchResult)[0].
						SearchParams.(searchparams.NearVector).Vector).Return([]float32{1, 2, 3}, nil)
				provider.On("VectorFromInput", ctx, class,
					params.HybridSearch.SubSearches.([]searchparams.WeightedSearchResult)[1].
						SearchParams.(searchparams.NearTextParams).Values[0]).Return([]float32{1, 2, 3}, nil)
				provider.On("VectorFromInput", ctx, class, "").Return([]float32{1, 2, 3}, nil)
				res, err := Search(ctx, params, logger, sparse, dense, nil, provider)
				require.Nil(t, err)
				assert.Len(t, res, 2)
				assert.NotNil(t, res[0])
				assert.NotNil(t, res[1])
				assert.Contains(t, res[0].Result.ExplainScore, "(Result Set vector,nearVector) Document 79a636c2-3314-442e-a4d1-e94d7c0afc3a")
				assert.Contains(t, res[0].Result.ExplainScore, "79a636c2-3314-442e-a4d1-e94d7c0afc3a")
				assert.Equal(t, res[0].Result.Vector, []float32{4, 5, 6})
				assert.Equal(t, res[0].Result.Dist, float32(0.008))
				assert.Contains(t, res[1].Result.ExplainScore, "(Result Set bm25f) Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Contains(t, res[1].Result.ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[1].Result.Vector, []float32{1, 2, 3})
				assert.Equal(t, res[1].Result.Dist, float32(0.008))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, test.f)
	}
}

type fakeModuleProvider struct {
	mock.Mock
}

func (f *fakeModuleProvider) VectorFromInput(ctx context.Context,
	className string, input string,
) ([]float32, error) {
	args := f.Called(ctx, className, input)
	return args.Get(0).([]float32), nil
}
