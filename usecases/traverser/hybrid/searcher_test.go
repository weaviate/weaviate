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
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
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
						Type:          "hybrid",
						Alpha:         0.5,
						Query:         "some query",
						TargetVectors: []string{"default"},
					},
					Class: class,
				}
				sparse := func() ([]*storobj.Object, []float32, error) { return nil, nil, nil }
				dense := func(models.Vector) ([]*storobj.Object, []float32, error) { return nil, nil, nil }
				provider := &fakeModuleProvider{}
				schemaGetter := newFakeSchemaManager()
				targetVectorParamHelper := newFakeTargetVectorParamHelper()
				_, err := Search(ctx, params, logger, sparse, dense, nil, provider, schemaGetter, targetVectorParamHelper)
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
				dense := func(models.Vector) ([]*storobj.Object, []float32, error) { return nil, nil, nil }
				res, err := Search(ctx, params, logger, sparse, dense, nil, nil, nil, nil)
				require.Nil(t, err)
				assert.Len(t, res, 1)
				assert.NotNil(t, res[0])
				assert.Contains(t, res[0].ExplainScore, "(Result Set keyword) Document")
				assert.Contains(t, res[0].ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Vector, []float32{1, 2, 3})
				assert.Equal(t, res[0].Dist, float32(0.000))
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
				dense := func(models.Vector) ([]*storobj.Object, []float32, error) {
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

				res, err := Search(ctx, params, logger, sparse, dense, nil, nil, nil, nil)
				require.Nil(t, err)
				assert.Len(t, res, 1)
				assert.NotNil(t, res[0])
				assert.Contains(t, res[0].ExplainScore, "(Result Set vector) Document")
				assert.Contains(t, res[0].ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Vector, []float32{1, 2, 3})
				assert.Equal(t, res[0].Dist, float32(0.008))
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
				dense := func(models.Vector) ([]*storobj.Object, []float32, error) {
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
				res, err := Search(ctx, params, logger, sparse, dense, nil, nil, nil, nil)
				require.Nil(t, err)
				assert.Len(t, res, 2)
				assert.NotNil(t, res[0])
				assert.NotNil(t, res[1])
				assert.Contains(t, res[0].ExplainScore, "(Result Set vector) Document")
				assert.Contains(t, res[0].ExplainScore, "79a636c2-3314-442e-a4d1-e94d7c0afc3a")
				assert.Equal(t, res[0].Vector, []float32{4, 5, 6})
				assert.Equal(t, res[0].Dist, float32(0.008))
				assert.Contains(t, res[1].ExplainScore, "(Result Set keyword) Document")
				assert.Contains(t, res[1].ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[1].Vector, []float32{1, 2, 3})
				assert.Equal(t, res[1].Dist, float32(0.000))
				assert.Equal(t, float32(0.008333334), res[0].Score)
			},
		},

		{
			name: "combined hybrid search",
			f: func(t *testing.T) {
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
				dense := func(models.Vector) ([]*storobj.Object, []float32, error) {
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
				res, err := Search(ctx, params, logger, sparse, dense, nil, nil, nil, nil)
				require.Nil(t, err)
				assert.Len(t, res, 2)
				assert.NotNil(t, res[0])
				assert.NotNil(t, res[1])
				assert.Contains(t, res[0].ExplainScore, "(Result Set vector) Document")
				assert.Contains(t, res[0].ExplainScore, "79a636c2-3314-442e-a4d1-e94d7c0afc3a")
				assert.Equal(t, res[0].Vector, []float32{4, 5, 6})
				assert.Equal(t, res[0].Dist, float32(0.008))
				assert.Contains(t, res[1].ExplainScore, "(Result Set keyword) Document")
				assert.Contains(t, res[1].ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[1].Vector, []float32{1, 2, 3})
				assert.Equal(t, res[1].Dist, float32(0.000))
				assert.Equal(t, float32(0.5), res[0].Score)
			},
		},
		{
			name: "with sparse subsearch filter",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						Type:       "hybrid",
						Properties: []string{"propA", "propB"},
						Query:      "some query",
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
				dense := func(models.Vector) ([]*storobj.Object, []float32, error) {
					return nil, nil, nil
				}
				res, err := Search(ctx, params, logger, sparse, dense, nil, nil, nil, nil)
				require.Nil(t, err)
				assert.Len(t, res, 1)
				assert.NotNil(t, res[0])
				assert.Contains(t, res[0].ExplainScore, "(Result Set keyword) Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Contains(t, res[0].ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Vector, []float32{1, 2, 3})
				assert.Equal(t, float32(0.008), res[0].SecondarySortValue)
			},
		},
		{
			name: "with nearText subsearch filter",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						TargetVectors: []string{"default"},
						Type:          "hybrid",
						Alpha:         1.0,
						NearTextParams: &searchparams.NearTextParams{
							Values:    []string{"some query"},
							Certainty: 0.8,
						},
					},
					Class: class,
				}
				sparse := func() ([]*storobj.Object, []float32, error) {
					return nil, nil, nil
				}
				dense := func(models.Vector) ([]*storobj.Object, []float32, error) {
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
				schemaGetter := newFakeSchemaManager()
				targetVectorParamHelper := newFakeTargetVectorParamHelper()
				res, err := Search(ctx, params, logger, sparse, dense, nil, provider, schemaGetter, targetVectorParamHelper)
				require.Nil(t, err)
				assert.Len(t, res, 1)
				assert.NotNil(t, res[0])
				assert.Contains(t, res[0].ExplainScore, "(Result Set vector) Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Contains(t, res[0].ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Vector, []float32{1, 2, 3})
				assert.Equal(t, float32(0.992), res[0].SecondarySortValue)
			},
		},
		{
			name: "with nearVector subsearch filter",
			f: func(t *testing.T) {
				params := &Params{
					HybridSearch: &searchparams.HybridSearch{
						TargetVectors: []string{"default"},
						Type:          "hybrid",
						Alpha:         1.0,
						NearVectorParams: &searchparams.NearVector{
							Vectors:   []models.Vector{[]float32{1, 2, 3}},
							Certainty: 0.8,
						},
					},
					Class: class,
				}
				sparse := func() ([]*storobj.Object, []float32, error) {
					return nil, nil, nil
				}
				dense := func(models.Vector) ([]*storobj.Object, []float32, error) {
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
				schemaGetter := newFakeSchemaManager()
				targetVectorParamHelper := newFakeTargetVectorParamHelper()
				res, err := Search(ctx, params, logger, sparse, dense, nil, provider, schemaGetter, targetVectorParamHelper)
				require.Nil(t, err)
				assert.Len(t, res, 1)
				assert.NotNil(t, res[0])
				assert.Contains(t, res[0].ExplainScore, "(Result Set vector) Document 1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Contains(t, res[0].ExplainScore, "1889a225-3b28-477d-b8fc-5f6071bb4731")
				assert.Equal(t, res[0].Vector, []float32{1, 2, 3})
				assert.Equal(t, res[0].Dist, float32(0.008))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, test.f)
	}
}

type fakeModuleProvider struct {
	vector []float32
}

func (f *fakeModuleProvider) VectorFromInput(ctx context.Context, className, input, targetVector string) ([]float32, error) {
	return f.vector, nil
}

func (f *fakeModuleProvider) MultiVectorFromInput(ctx context.Context, className, input, targetVector string) ([][]float32, error) {
	panic("not implemented")
}

func (f *fakeModuleProvider) IsTargetVectorMultiVector(className, targetVector string) (bool, error) {
	return false, nil
}
