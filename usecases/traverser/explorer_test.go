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

package traverser

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
)

var defaultConfig = config.Config{
	QueryDefaults: config.QueryDefaults{
		Limit: 100,
	},
	QueryMaximumResults: 100,
}

func Test_Explorer_GetClass(t *testing.T) {
	t.Run("when an explore param is set for nearVector", func(t *testing.T) {
		// TODO: this is a module specific test case, which relies on the
		// text2vec-contextionary module
		params := dto.GetParams{
			ClassName: "BestClass",
			NearVector: &searchparams.NearVector{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				Dims: 128,
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				Dims: 128,
			},
		}

		search := &fakeVectorSearcher{}
		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{0.8, 0.2, 0.7}
		search.
			On("VectorSearch", expectedParamsToSearch).
			Return(searchResults, nil)

		metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearVector", 128)
		res, err := explorer.GetClass(context.Background(), params)

		t.Run("vector search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
				}, res[1])
		})

		t.Run("usage must be tracked", func(t *testing.T) {
			metrics.AssertExpectations(t)
		})
	})

	t.Run("when an explore param is set for nearObject without id and beacon", func(t *testing.T) {
		t.Run("with distance", func(t *testing.T) {
			// TODO: this is a module specific test case, which relies on the
			// text2vec-contextionary module
			params := dto.GetParams{
				ClassName: "BestClass",
				NearObject: &searchparams.NearObject{
					Distance: 0.1,
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			search := &fakeVectorSearcher{}
			log, _ := test.NewNullLogger()
			metrics := &fakeMetrics{}
			explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
			explorer.SetSchemaGetter(&fakeSchemaGetter{
				schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
					{Class: "BestClass"},
				}}},
			})

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.NotNil(t, err)
				assert.Nil(t, res)
				assert.Contains(t, err.Error(), "explorer: get class: vectorize params: nearObject params: empty id and beacon")
			})
		})

		t.Run("with certainty", func(t *testing.T) {
			// TODO: this is a module specific test case, which relies on the
			// text2vec-contextionary module
			params := dto.GetParams{
				ClassName: "BestClass",
				NearObject: &searchparams.NearObject{
					Certainty: 0.9,
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			search := &fakeVectorSearcher{}
			log, _ := test.NewNullLogger()
			metrics := &fakeMetrics{}
			explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
			explorer.SetSchemaGetter(&fakeSchemaGetter{
				schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
					{Class: "BestClass"},
				}}},
			})

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.NotNil(t, err)
				assert.Nil(t, res)
				assert.Contains(t, err.Error(), "explorer: get class: vectorize params: nearObject params: empty id and beacon")
			})
		})
	})

	t.Run("when an explore param is set for nearObject with beacon", func(t *testing.T) {
		t.Run("with distance", func(t *testing.T) {
			t.Run("with certainty", func(t *testing.T) {
				// TODO: this is a module specific test case, which relies on the
				// text2vec-contextionary module
				params := dto.GetParams{
					ClassName: "BestClass",
					NearObject: &searchparams.NearObject{
						Beacon:   "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e041",
						Distance: 0.1,
					},
					Pagination: &filters.Pagination{Limit: 100},
					Filters:    nil,
				}

				searchRes := search.Result{
					ID: "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
				}

				searchResults := []search.Result{
					{
						ID: "id1",
						Schema: map[string]interface{}{
							"name": "Foo",
						},
						Dims: 128,
					},
					{
						ID: "id2",
						Schema: map[string]interface{}{
							"age": 200,
						},
						Dims: 128,
					},
				}

				search := &fakeVectorSearcher{}
				log, _ := test.NewNullLogger()
				metrics := &fakeMetrics{}
				explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
				explorer.SetSchemaGetter(&fakeSchemaGetter{
					schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
						{Class: "BestClass"},
					}}},
				})
				expectedParamsToSearch := params
				search.
					On("Object", "BestClass", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e041")).
					Return(&searchRes, nil)
				search.
					On("VectorSearch", expectedParamsToSearch).
					Return(searchResults, nil)
				metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearObject", 128)

				res, err := explorer.GetClass(context.Background(), params)

				t.Run("vector search must be called with right params", func(t *testing.T) {
					assert.Nil(t, err)
					search.AssertExpectations(t)
				})

				t.Run("response must contain object", func(t *testing.T) {
					require.Len(t, res, 2)
					assert.Equal(t,
						map[string]interface{}{
							"name": "Foo",
						}, res[0])
					assert.Equal(t,
						map[string]interface{}{
							"age": 200,
						}, res[1])
				})
			})
		})

		t.Run("with certainty", func(t *testing.T) {
			// TODO: this is a module specific test case, which relies on the
			// text2vec-contextionary module
			params := dto.GetParams{
				ClassName: "BestClass",
				NearObject: &searchparams.NearObject{
					Beacon:    "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e041",
					Certainty: 0.9,
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			searchRes := search.Result{
				ID: "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			}

			searchResults := []search.Result{
				{
					ID: "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					Dims: 128,
				},
				{
					ID: "id2",
					Schema: map[string]interface{}{
						"age": 200,
					},
					Dims: 128,
				},
			}

			search := &fakeVectorSearcher{}
			log, _ := test.NewNullLogger()
			metrics := &fakeMetrics{}
			explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
			explorer.SetSchemaGetter(&fakeSchemaGetter{
				schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
					{Class: "BestClass"},
				}}},
			})
			expectedParamsToSearch := params
			search.
				On("Object", "BestClass", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e041")).
				Return(&searchRes, nil)
			search.
				On("VectorSearch", expectedParamsToSearch).
				Return(searchResults, nil)
			metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearObject", 128)

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.Nil(t, err)
				search.AssertExpectations(t)
			})

			t.Run("response must contain object", func(t *testing.T) {
				require.Len(t, res, 2)
				assert.Equal(t,
					map[string]interface{}{
						"name": "Foo",
					}, res[0])
				assert.Equal(t,
					map[string]interface{}{
						"age": 200,
					}, res[1])
			})
		})
	})

	t.Run("when an explore param is set for nearObject with id", func(t *testing.T) {
		t.Run("with distance", func(t *testing.T) {
			// TODO: this is a module specific test case, which relies on the
			// text2vec-contextionary module
			params := dto.GetParams{
				ClassName: "BestClass",
				NearObject: &searchparams.NearObject{
					ID:       "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
					Distance: 0.1,
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			searchRes := search.Result{
				ID: "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			}

			searchResults := []search.Result{
				{
					ID: "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					Dims: 128,
				},
				{
					ID: "id2",
					Schema: map[string]interface{}{
						"age": 200,
					},
					Dims: 128,
				},
			}

			search := &fakeVectorSearcher{}
			log, _ := test.NewNullLogger()
			metrics := &fakeMetrics{}
			explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
			explorer.SetSchemaGetter(&fakeSchemaGetter{
				schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
					{Class: "BestClass"},
				}}},
			})
			expectedParamsToSearch := params
			search.
				On("Object", "BestClass", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e041")).
				Return(&searchRes, nil)
			search.
				On("VectorSearch", expectedParamsToSearch).
				Return(searchResults, nil)
			metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearObject", 128)

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.Nil(t, err)
				search.AssertExpectations(t)
			})

			t.Run("response must contain object", func(t *testing.T) {
				require.Len(t, res, 2)
				assert.Equal(t,
					map[string]interface{}{
						"name": "Foo",
					}, res[0])
				assert.Equal(t,
					map[string]interface{}{
						"age": 200,
					}, res[1])
			})
		})

		t.Run("with certainty", func(t *testing.T) {
			// TODO: this is a module specific test case, which relies on the
			// text2vec-contextionary module
			params := dto.GetParams{
				ClassName: "BestClass",
				NearObject: &searchparams.NearObject{
					ID:        "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
					Certainty: 0.9,
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			searchRes := search.Result{
				ID: "e9c12c22-766f-4bde-b140-d4cf8fd6e041",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			}

			searchResults := []search.Result{
				{
					ID: "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					Dims: 128,
				},
				{
					ID: "id2",
					Schema: map[string]interface{}{
						"age": 200,
					},
					Dims: 128,
				},
			}

			search := &fakeVectorSearcher{}
			metrics := &fakeMetrics{}
			log, _ := test.NewNullLogger()
			explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
			explorer.SetSchemaGetter(&fakeSchemaGetter{
				schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
					{Class: "BestClass"},
				}}},
			})
			expectedParamsToSearch := params
			search.
				On("Object", "BestClass", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e041")).
				Return(&searchRes, nil)
			search.
				On("VectorSearch", expectedParamsToSearch).
				Return(searchResults, nil)
			metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearObject", 128)

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.Nil(t, err)
				search.AssertExpectations(t)
			})

			t.Run("response must contain object", func(t *testing.T) {
				require.Len(t, res, 2)
				assert.Equal(t,
					map[string]interface{}{
						"name": "Foo",
					}, res[0])
				assert.Equal(t,
					map[string]interface{}{
						"age": 200,
					}, res[1])
			})
		})
	})

	t.Run("when an explore param is set for nearVector and the required distance not met",
		func(t *testing.T) {
			t.Run("with distance", func(t *testing.T) {
				params := dto.GetParams{
					ClassName: "BestClass",
					NearVector: &searchparams.NearVector{
						Vector:       []float32{0.8, 0.2, 0.7},
						Distance:     0.4,
						WithDistance: true,
					},
					Pagination: &filters.Pagination{Limit: 100},
					Filters:    nil,
				}

				searchResults := []search.Result{
					{
						ID:   "id1",
						Dist: 2 * 0.69,
						Dims: 128,
					},
					{
						ID:   "id2",
						Dist: 2 * 0.69,
						Dims: 128,
					},
				}

				search := &fakeVectorSearcher{}
				log, _ := test.NewNullLogger()
				metrics := &fakeMetrics{}
				explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
				explorer.SetSchemaGetter(&fakeSchemaGetter{
					schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
						{Class: "BestClass"},
					}}},
				})
				expectedParamsToSearch := params
				expectedParamsToSearch.SearchVector = []float32{0.8, 0.2, 0.7}
				search.
					On("VectorSearch", expectedParamsToSearch).
					Return(searchResults, nil)
				metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearVector", 128)

				res, err := explorer.GetClass(context.Background(), params)

				t.Run("vector search must be called with right params", func(t *testing.T) {
					assert.Nil(t, err)
					search.AssertExpectations(t)
				})

				t.Run("no concept met the required certainty", func(t *testing.T) {
					assert.Len(t, res, 0)
				})
			})

			t.Run("with certainty", func(t *testing.T) {
				params := dto.GetParams{
					ClassName: "BestClass",
					NearVector: &searchparams.NearVector{
						Vector:    []float32{0.8, 0.2, 0.7},
						Certainty: 0.8,
					},
					Pagination: &filters.Pagination{Limit: 100},
					Filters:    nil,
				}

				searchResults := []search.Result{
					{
						ID:   "id1",
						Dist: 2 * 0.69,
						Dims: 128,
					},
					{
						ID:   "id2",
						Dist: 2 * 0.69,
						Dims: 128,
					},
				}

				search := &fakeVectorSearcher{}
				log, _ := test.NewNullLogger()
				metrics := &fakeMetrics{}
				explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
				explorer.SetSchemaGetter(&fakeSchemaGetter{
					schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
						{Class: "BestClass"},
					}}},
				})
				expectedParamsToSearch := params
				expectedParamsToSearch.SearchVector = []float32{0.8, 0.2, 0.7}
				search.
					On("VectorSearch", expectedParamsToSearch).
					Return(searchResults, nil)
				metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearVector", 128)

				res, err := explorer.GetClass(context.Background(), params)

				t.Run("vector search must be called with right params", func(t *testing.T) {
					assert.Nil(t, err)
					search.AssertExpectations(t)
				})

				t.Run("no concept met the required certainty", func(t *testing.T) {
					assert.Len(t, res, 0)
				})
			})
		})

	t.Run("when two conflicting (nearVector, nearObject) near searchers are set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearVector: &searchparams.NearVector{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			NearObject: &searchparams.NearObject{
				Beacon: "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e041",
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when no explore param is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
				}, res[1])
		})
	})

	t.Run("near vector with group", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearVector: &searchparams.NearVector{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			Group: &dto.GroupParams{
				Strategy: "closest",
				Force:    1.0,
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				Dims: 128,
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{0.8, 0.2, 0.7}
		expectedParamsToSearch.AdditionalProperties.Vector = true
		search.
			On("VectorSearch", expectedParamsToSearch).
			Return(searchResults, nil)
		metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearVector", 128)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 1)
		})
	})

	t.Run("when the semanticPath prop is set but cannot be", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				ModuleParams: map[string]interface{}{
					"semanticPath": getDefaultParam("semanticPath"),
				},
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("error can't be nil", func(t *testing.T) {
			assert.NotNil(t, err)
			assert.Nil(t, res)
			assert.Contains(t, err.Error(), "unknown capability: semanticPath")
		})
	})

	t.Run("when the classification prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				Classification: true,
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": nil,
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": &additional.Classification{
						ID: "1234",
					},
				},
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
					"_additional": map[string]interface{}{
						"classification": &additional.Classification{
							ID: "1234",
						},
					},
				}, res[1])
		})
	})

	t.Run("when the interpretation prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				ModuleParams: map[string]interface{}{
					"interpretation": true,
				},
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				AdditionalProperties: models.AdditionalProperties{
					"interpretation": nil,
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				AdditionalProperties: models.AdditionalProperties{
					"interpretation": &Interpretation{
						Source: []*InterpretationSource{
							{
								Concept:    "foo",
								Weight:     0.123,
								Occurrence: 123,
							},
						},
					},
				},
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
					"_additional": map[string]interface{}{
						"interpretation": &Interpretation{
							Source: []*InterpretationSource{
								{
									Concept:    "foo",
									Weight:     0.123,
									Occurrence: 123,
								},
							},
						},
					},
				}, res[1])
		})
	})

	t.Run("when the vector _additional prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				Vector: true,
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				Vector: []float32{0.1, -0.3},
				Dims:   128,
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)
		metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "_additional.vector", 128)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain vector", func(t *testing.T) {
			require.Len(t, res, 1)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"_additional": map[string]interface{}{
						"vector": []float32{0.1, -0.3},
					},
				}, res[0])
		})
	})

	t.Run("when the creationTimeUnix _additional prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				CreationTimeUnix: true,
			},
		}

		now := time.Now().UnixNano() / int64(time.Millisecond)

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				Created: now,
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain creationTimeUnix", func(t *testing.T) {
			require.Len(t, res, 1)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"_additional": map[string]interface{}{
						"creationTimeUnix": now,
					},
				}, res[0])
		})
	})

	t.Run("when the lastUpdateTimeUnix _additional prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				LastUpdateTimeUnix: true,
			},
		}

		now := time.Now().UnixNano() / int64(time.Millisecond)

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				Updated: now,
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain lastUpdateTimeUnix", func(t *testing.T) {
			require.Len(t, res, 1)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"_additional": map[string]interface{}{
						"lastUpdateTimeUnix": now,
					},
				}, res[0])
		})
	})

	t.Run("when the nearestNeighbors prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				ModuleParams: map[string]interface{}{
					"nearestNeighbors": true,
				},
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"name": "Bar",
				},
			},
		}

		searcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{
			returnArgs: []search.Result{
				{
					ID: "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					AdditionalProperties: models.AdditionalProperties{
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "foo",
									Distance: 0.1,
								},
							},
						},
					},
				},
				{
					ID: "id2",
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					AdditionalProperties: models.AdditionalProperties{
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "bar",
									Distance: 0.1,
								},
							},
						},
					},
				},
			},
		}
		explorer := NewExplorer(searcher, log, getFakeModulesProviderWithCustomExtenders(extender, nil, nil), nil, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		searcher.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			searcher.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"_additional": map[string]interface{}{
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "foo",
									Distance: 0.1,
								},
							},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"name": "Bar",
					"_additional": map[string]interface{}{
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "bar",
									Distance: 0.1,
								},
							},
						},
					},
				}, res[1])
		})
	})

	t.Run("when the featureProjection prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				ModuleParams: map[string]interface{}{
					"featureProjection": getDefaultParam("featureProjection"),
				},
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"name": "Bar",
				},
			},
		}

		searcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{
			returnArgs: []search.Result{
				{
					ID: "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					AdditionalProperties: models.AdditionalProperties{
						"featureProjection": &FeatureProjection{
							Vector: []float32{0, 1},
						},
					},
				},
				{
					ID: "id2",
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					AdditionalProperties: models.AdditionalProperties{
						"featureProjection": &FeatureProjection{
							Vector: []float32{1, 0},
						},
					},
				},
			},
		}
		explorer := NewExplorer(searcher, log, getFakeModulesProviderWithCustomExtenders(nil, projector, nil), nil, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		searcher.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			searcher.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"_additional": map[string]interface{}{
						"featureProjection": &FeatureProjection{
							Vector: []float32{0, 1},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"name": "Bar",
					"_additional": map[string]interface{}{
						"featureProjection": &FeatureProjection{
							Vector: []float32{1, 0},
						},
					},
				}, res[1])
		})
	})

	t.Run("when the _additional on ref prop is set", func(t *testing.T) {
		now := time.Now().UnixMilli()
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			Properties: []search.SelectProperty{
				{
					Name: "ofBestRefClass",
					Refs: []search.SelectClass{
						{
							ClassName: "BestRefClass",
							AdditionalProperties: additional.Properties{
								ID:                 true,
								Vector:             true,
								CreationTimeUnix:   true,
								LastUpdateTimeUnix: true,
							},
						},
					},
				},
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
								"vector":             []float32{1, 0},
								"creationTimeUnix":   now,
								"lastUpdateTimeUnix": now,
							},
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": nil,
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
							},
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": &additional.Classification{
						ID: "1234",
					},
				},
			},
		}

		fakeSearch := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(fakeSearch, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		fakeSearch.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			fakeSearch.AssertExpectations(t)
		})

		t.Run("response must contain _additional id and vector params for ref prop", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"_additional": map[string]interface{}{
									"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
									"vector":             []float32{1, 0},
									"creationTimeUnix":   now,
									"lastUpdateTimeUnix": now,
								},
								"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
								"vector":             []float32{1, 0},
								"creationTimeUnix":   now,
								"lastUpdateTimeUnix": now,
							},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"_additional": map[string]interface{}{
									"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
									"vector":             nil,
									"creationTimeUnix":   nil,
									"lastUpdateTimeUnix": nil,
								},
								"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
							},
						},
					},
					"_additional": map[string]interface{}{
						"classification": &additional.Classification{
							ID: "1234",
						},
					},
				}, res[1])
		})
	})

	t.Run("when the _additional on all refs prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			Properties: []search.SelectProperty{
				{
					Name: "ofBestRefClass",
					Refs: []search.SelectClass{
						{
							ClassName: "BestRefClass",
							AdditionalProperties: additional.Properties{
								ID: true,
							},
							RefProperties: search.SelectProperties{
								search.SelectProperty{
									Name: "ofBestRefInnerClass",
									Refs: []search.SelectClass{
										{
											ClassName: "BestRefInnerClass",
											AdditionalProperties: additional.Properties{
												ID: true,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
								"ofBestRefInnerClass": []interface{}{
									search.LocalRef{
										Class: "BestRefInnerClass",
										Fields: map[string]interface{}{
											"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4caa",
										},
									},
								},
							},
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": nil,
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
								"ofBestRefInnerClass": []interface{}{
									search.LocalRef{
										Class: "BestRefInnerClass",
										Fields: map[string]interface{}{
											"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4cbb",
										},
									},
								},
							},
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": &additional.Classification{
						ID: "1234",
					},
				},
			},
		}

		fakeSearch := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(fakeSearch, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		fakeSearch.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			fakeSearch.AssertExpectations(t)
		})

		t.Run("response must contain _additional id param for ref prop", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"_additional": map[string]interface{}{
									"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
								},
								"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
								"ofBestRefInnerClass": []interface{}{
									search.LocalRef{
										Class: "BestRefInnerClass",
										Fields: map[string]interface{}{
											"_additional": map[string]interface{}{
												"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4caa",
											},
											"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4caa",
										},
									},
								},
							},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"_additional": map[string]interface{}{
									"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
								},
								"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
								"ofBestRefInnerClass": []interface{}{
									search.LocalRef{
										Class: "BestRefInnerClass",
										Fields: map[string]interface{}{
											"_additional": map[string]interface{}{
												"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4cbb",
											},
											"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4cbb",
										},
									},
								},
							},
						},
					},
					"_additional": map[string]interface{}{
						"classification": &additional.Classification{
							ID: "1234",
						},
					},
				}, res[1])
		})
	})

	t.Run("when the _additional on lots of refs prop is set", func(t *testing.T) {
		now := time.Now().UnixMilli()
		vec := []float32{1, 2, 3}
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			Properties: []search.SelectProperty{
				{
					Name: "ofBestRefClass",
					Refs: []search.SelectClass{
						{
							ClassName: "BestRefClass",
							AdditionalProperties: additional.Properties{
								ID:                 true,
								Vector:             true,
								CreationTimeUnix:   true,
								LastUpdateTimeUnix: true,
							},
							RefProperties: search.SelectProperties{
								search.SelectProperty{
									Name: "ofBestRefInnerClass",
									Refs: []search.SelectClass{
										{
											ClassName: "BestRefInnerClass",
											AdditionalProperties: additional.Properties{
												ID:                 true,
												Vector:             true,
												CreationTimeUnix:   true,
												LastUpdateTimeUnix: true,
											},
											RefProperties: search.SelectProperties{
												search.SelectProperty{
													Name: "ofBestRefInnerInnerClass",
													Refs: []search.SelectClass{
														{
															ClassName: "BestRefInnerInnerClass",
															AdditionalProperties: additional.Properties{
																ID:                 true,
																Vector:             true,
																CreationTimeUnix:   true,
																LastUpdateTimeUnix: true,
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
								"creationTimeUnix":   now,
								"lastUpdateTimeUnix": now,
								"vector":             vec,
								"ofBestRefInnerClass": []interface{}{
									search.LocalRef{
										Class: "BestRefInnerClass",
										Fields: map[string]interface{}{
											"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4caa",
											"creationTimeUnix":   now,
											"lastUpdateTimeUnix": now,
											"vector":             vec,
											"ofBestRefInnerInnerClass": []interface{}{
												search.LocalRef{
													Class: "BestRefInnerInnerClass",
													Fields: map[string]interface{}{
														"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4aaa",
														"creationTimeUnix":   now,
														"lastUpdateTimeUnix": now,
														"vector":             vec,
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": nil,
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
								"creationTimeUnix":   now,
								"lastUpdateTimeUnix": now,
								"vector":             vec,
								"ofBestRefInnerClass": []interface{}{
									search.LocalRef{
										Class: "BestRefInnerClass",
										Fields: map[string]interface{}{
											"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cbb",
											"creationTimeUnix":   now,
											"lastUpdateTimeUnix": now,
											"vector":             vec,
											"ofBestRefInnerInnerClass": []interface{}{
												search.LocalRef{
													Class: "BestRefInnerInnerClass",
													Fields: map[string]interface{}{
														"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4bbb",
														"creationTimeUnix":   now,
														"lastUpdateTimeUnix": now,
														"vector":             vec,
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": &additional.Classification{
						ID: "1234",
					},
				},
			},
		}

		fakeSearch := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(fakeSearch, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		fakeSearch.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			fakeSearch.AssertExpectations(t)
		})

		t.Run("response must contain _additional id param for ref prop", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"_additional": map[string]interface{}{
									"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
									"creationTimeUnix":   now,
									"lastUpdateTimeUnix": now,
									"vector":             vec,
								},
								"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cea",
								"creationTimeUnix":   now,
								"lastUpdateTimeUnix": now,
								"vector":             vec,
								"ofBestRefInnerClass": []interface{}{
									search.LocalRef{
										Class: "BestRefInnerClass",
										Fields: map[string]interface{}{
											"_additional": map[string]interface{}{
												"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4caa",
												"creationTimeUnix":   now,
												"lastUpdateTimeUnix": now,
												"vector":             vec,
											},
											"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4caa",
											"creationTimeUnix":   now,
											"lastUpdateTimeUnix": now,
											"vector":             vec,
											"ofBestRefInnerInnerClass": []interface{}{
												search.LocalRef{
													Class: "BestRefInnerInnerClass",
													Fields: map[string]interface{}{
														"_additional": map[string]interface{}{
															"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4aaa",
															"creationTimeUnix":   now,
															"lastUpdateTimeUnix": now,
															"vector":             vec,
														},
														"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4aaa",
														"creationTimeUnix":   now,
														"lastUpdateTimeUnix": now,
														"vector":             vec,
													},
												},
											},
										},
									},
								},
							},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
					"ofBestRefClass": []interface{}{
						search.LocalRef{
							Class: "BestRefClass",
							Fields: map[string]interface{}{
								"_additional": map[string]interface{}{
									"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
									"creationTimeUnix":   now,
									"lastUpdateTimeUnix": now,
									"vector":             vec,
								},
								"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4ceb",
								"creationTimeUnix":   now,
								"lastUpdateTimeUnix": now,
								"vector":             vec,
								"ofBestRefInnerClass": []interface{}{
									search.LocalRef{
										Class: "BestRefInnerClass",
										Fields: map[string]interface{}{
											"_additional": map[string]interface{}{
												"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cbb",
												"creationTimeUnix":   now,
												"lastUpdateTimeUnix": now,
												"vector":             vec,
											},
											"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4cbb",
											"creationTimeUnix":   now,
											"lastUpdateTimeUnix": now,
											"vector":             vec,
											"ofBestRefInnerInnerClass": []interface{}{
												search.LocalRef{
													Class: "BestRefInnerInnerClass",
													Fields: map[string]interface{}{
														"_additional": map[string]interface{}{
															"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4bbb",
															"creationTimeUnix":   now,
															"lastUpdateTimeUnix": now,
															"vector":             vec,
														},
														"id":                 "2d68456c-73b4-4cfc-a6dc-718efc5b4bbb",
														"creationTimeUnix":   now,
														"lastUpdateTimeUnix": now,
														"vector":             vec,
													},
												},
											},
										},
									},
								},
							},
						},
					},
					"_additional": map[string]interface{}{
						"classification": &additional.Classification{
							ID: "1234",
						},
					},
				}, res[1])
		})
	})

	t.Run("when the almost all _additional props set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				ID:             true,
				Classification: true,
				ModuleParams: map[string]interface{}{
					"interpretation":   true,
					"nearestNeighbors": true,
				},
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": &additional.Classification{
						ID: "1234",
					},
					"nearestNeighbors": &NearestNeighbors{
						Neighbors: []*NearestNeighbor{
							{
								Concept:  "foo",
								Distance: 0.1,
							},
						},
					},
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"name": "Bar",
				},
				AdditionalProperties: models.AdditionalProperties{
					"classification": &additional.Classification{
						ID: "5678",
					},
					"nearestNeighbors": &NearestNeighbors{
						Neighbors: []*NearestNeighbor{
							{
								Concept:  "bar",
								Distance: 0.1,
							},
						},
					},
				},
			},
		}

		searcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{
			returnArgs: []search.Result{
				{
					ID: "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					AdditionalProperties: models.AdditionalProperties{
						"classification": &additional.Classification{
							ID: "1234",
						},
						"interpretation": &Interpretation{
							Source: []*InterpretationSource{
								{
									Concept:    "foo",
									Weight:     0.123,
									Occurrence: 123,
								},
							},
						},
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "foo",
									Distance: 0.1,
								},
							},
						},
					},
				},
				{
					ID: "id2",
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					AdditionalProperties: models.AdditionalProperties{
						"classification": &additional.Classification{
							ID: "5678",
						},
						"interpretation": &Interpretation{
							Source: []*InterpretationSource{
								{
									Concept:    "bar",
									Weight:     0.456,
									Occurrence: 456,
								},
							},
						},
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "bar",
									Distance: 0.1,
								},
							},
						},
					},
				},
			},
		}
		explorer := NewExplorer(searcher, log, getFakeModulesProviderWithCustomExtenders(extender, nil, nil), nil, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		searcher.
			On("Search", expectedParamsToSearch).
			Return(searchResults, nil)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			searcher.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"_additional": map[string]interface{}{
						"id": strfmt.UUID("id1"),
						"classification": &additional.Classification{
							ID: "1234",
						},
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "foo",
									Distance: 0.1,
								},
							},
						},
						"interpretation": &Interpretation{
							Source: []*InterpretationSource{
								{
									Concept:    "foo",
									Weight:     0.123,
									Occurrence: 123,
								},
							},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"name": "Bar",
					"_additional": map[string]interface{}{
						"id": strfmt.UUID("id2"),
						"classification": &additional.Classification{
							ID: "5678",
						},
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "bar",
									Distance: 0.1,
								},
							},
						},
						"interpretation": &Interpretation{
							Source: []*InterpretationSource{
								{
									Concept:    "bar",
									Weight:     0.456,
									Occurrence: 456,
								},
							},
						},
					},
				}, res[1])
		})
	})
}

func Test_Explorer_GetClass_With_Modules(t *testing.T) {
	t.Run("when an explore param is set for nearCustomText", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "BestClass",
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
				}),
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				Dims: 128,
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				Dims: 128,
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
		search.
			On("VectorSearch", expectedParamsToSearch).
			Return(searchResults, nil)
		metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearCustomText", 128)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("vector search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"age": 200,
				}, res[1])
		})
	})

	t.Run("when an explore param is set for nearCustomText and the required distance not met",
		func(t *testing.T) {
			params := dto.GetParams{
				ClassName: "BestClass",
				ModuleParams: map[string]interface{}{
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
						"concepts": []interface{}{"foo"},
						"distance": float64(0.2),
					}),
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			searchResults := []search.Result{
				{
					ID:   "id1",
					Dist: 2 * 0.69,
					Dims: 128,
				},
				{
					ID:   "id2",
					Dist: 2 * 0.69,
					Dims: 128,
				},
			}

			search := &fakeVectorSearcher{}
			log, _ := test.NewNullLogger()
			metrics := &fakeMetrics{}
			explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
			explorer.SetSchemaGetter(&fakeSchemaGetter{
				schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
					{Class: "BestClass"},
				}}},
			})
			expectedParamsToSearch := params
			expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
			search.
				On("VectorSearch", expectedParamsToSearch).
				Return(searchResults, nil)
			metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearCustomText", 128)

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.Nil(t, err)
				search.AssertExpectations(t)
			})

			t.Run("no object met the required distance", func(t *testing.T) {
				assert.Len(t, res, 0)
			})
		})

	t.Run("when an explore param is set for nearCustomText and the required certainty not met",
		func(t *testing.T) {
			params := dto.GetParams{
				ClassName: "BestClass",
				ModuleParams: map[string]interface{}{
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
						"concepts":  []interface{}{"foo"},
						"certainty": float64(0.8),
					}),
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			searchResults := []search.Result{
				{
					ID:   "id1",
					Dist: 2 * 0.69,
					Dims: 128,
				},
				{
					ID:   "id2",
					Dist: 2 * 0.69,
					Dims: 128,
				},
			}

			search := &fakeVectorSearcher{}
			log, _ := test.NewNullLogger()
			metrics := &fakeMetrics{}
			explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
			explorer.SetSchemaGetter(&fakeSchemaGetter{
				schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
					{Class: "BestClass"},
				}}},
			})
			expectedParamsToSearch := params
			expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
			search.
				On("VectorSearch", expectedParamsToSearch).
				Return(searchResults, nil)
			metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearCustomText", 128)

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.Nil(t, err)
				search.AssertExpectations(t)
			})

			t.Run("no object met the required certainty", func(t *testing.T) {
				assert.Len(t, res, 0)
			})
		})

	t.Run("when two conflicting (nearVector, nearCustomText) near searchers are set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearVector: &searchparams.NearVector{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when two conflicting (nearCustomText, nearObject) near searchers are set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearObject: &searchparams.NearObject{
				Beacon: "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e041",
			},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when three conflicting (nearCustomText, nearVector, nearObject) near searchers are set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearVector: &searchparams.NearVector{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			NearObject: &searchparams.NearObject{
				Beacon: "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e041",
			},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when nearCustomText.moveTo has no concepts and objects defined", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
					"moveTo": map[string]interface{}{
						"force": float64(0.1),
					},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "needs to have defined either 'concepts' or 'objects' fields")
	})

	t.Run("when nearCustomText.moveAwayFrom has no concepts and objects defined", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
					"moveAwayFrom": map[string]interface{}{
						"force": float64(0.1),
					},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "needs to have defined either 'concepts' or 'objects' fields")
	})

	t.Run("when the distance prop is set", func(t *testing.T) {
		params := dto.GetParams{
			Filters:      nil,
			ClassName:    "BestClass",
			Pagination:   &filters.Pagination{Limit: 100},
			SearchVector: []float32{1.0, 2.0, 3.0},
			AdditionalProperties: additional.Properties{
				Distance: true,
			},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"foobar"},
					"limit":    100,
					"distance": float64(1.38),
				}),
			},
		}

		searchResults := []search.Result{
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				Vector: []float32{0.5, 1.5, 0.0},
				Dist:   2 * 0.69,
				Dims:   128,
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1.0, 2.0, 3.0}
		// expectedParamsToSearch.SearchVector = nil
		search.
			On("VectorSearch", expectedParamsToSearch).
			Return(searchResults, nil)
		metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearCustomText", 128)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 1)

			resMap := res[0].(map[string]interface{})
			assert.Equal(t, 2, len(resMap))
			assert.Contains(t, resMap, "age")
			assert.Equal(t, 200, resMap["age"])
			additionalMap := resMap["_additional"]
			assert.Contains(t, additionalMap, "distance")
			assert.InEpsilon(t, 1.38, additionalMap.(map[string]interface{})["distance"].(float32), 0.000001)
		})
	})

	t.Run("when the certainty prop is set", func(t *testing.T) {
		params := dto.GetParams{
			Filters:      nil,
			ClassName:    "BestClass",
			Pagination:   &filters.Pagination{Limit: 100},
			SearchVector: []float32{1.0, 2.0, 3.0},
			AdditionalProperties: additional.Properties{
				Certainty: true,
			},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts":  []interface{}{"foobar"},
					"limit":     100,
					"certainty": float64(0.1),
				}),
			},
		}

		searchResults := []search.Result{
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				Vector: []float32{0.5, 1.5, 0.0},
				Dist:   2 * 0.69,
				Dims:   128,
			},
		}

		search := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		metrics := &fakeMetrics{}
		explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		schemaGetter := newFakeSchemaGetter("BestClass")
		schemaGetter.SetVectorIndexConfig(hnsw.UserConfig{Distance: "cosine"})
		explorer.schemaGetter = schemaGetter
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1.0, 2.0, 3.0}
		// expectedParamsToSearch.SearchVector = nil
		search.
			On("VectorSearch", expectedParamsToSearch).
			Return(searchResults, nil)
		metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearCustomText", 128)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			search.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 1)

			resMap := res[0].(map[string]interface{})
			assert.Equal(t, 2, len(resMap))
			assert.Contains(t, resMap, "age")
			assert.Equal(t, 200, resMap["age"])
			additionalMap := resMap["_additional"]
			assert.Contains(t, additionalMap, "certainty")
			// Certainty is fixed to 0.69 in this mock
			assert.InEpsilon(t, 0.31, additionalMap.(map[string]interface{})["certainty"], 0.000001)
		})
	})

	t.Run("when the semanticPath prop is set", func(t *testing.T) {
		params := dto.GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: additional.Properties{
				ModuleParams: map[string]interface{}{
					"semanticPath": getDefaultParam("semanticPath"),
				},
			},
			ModuleParams: map[string]interface{}{
				"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
					"concepts": []interface{}{"foobar"},
				}),
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"name": "Bar",
				},
			},
		}

		searcher := &fakeVectorSearcher{}
		log, _ := test.NewNullLogger()
		pathBuilder := &fakePathBuilder{
			returnArgs: []search.Result{
				{
					ID:   "id1",
					Dims: 128,
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					AdditionalProperties: models.AdditionalProperties{
						"semanticPath": &SemanticPath{
							Path: []*SemanticPathElement{
								{
									Concept:            "pathelem1",
									DistanceToQuery:    0,
									DistanceToResult:   2.1,
									DistanceToPrevious: nil,
									DistanceToNext:     ptFloat32(0.5),
								},
								{
									Concept:            "pathelem2",
									DistanceToQuery:    2.1,
									DistanceToResult:   0,
									DistanceToPrevious: ptFloat32(0.5),
									DistanceToNext:     nil,
								},
							},
						},
					},
				},
				{
					ID:   "id2",
					Dims: 128,
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					AdditionalProperties: models.AdditionalProperties{
						"semanticPath": &SemanticPath{
							Path: []*SemanticPathElement{
								{
									Concept:            "pathelem1",
									DistanceToQuery:    0,
									DistanceToResult:   2.1,
									DistanceToPrevious: nil,
									DistanceToNext:     ptFloat32(0.5),
								},
								{
									Concept:            "pathelem2",
									DistanceToQuery:    2.1,
									DistanceToResult:   0,
									DistanceToPrevious: ptFloat32(0.5),
									DistanceToNext:     nil,
								},
							},
						},
					},
				},
			},
		}
		metrics := &fakeMetrics{}
		explorer := NewExplorer(searcher, log, getFakeModulesProviderWithCustomExtenders(nil, nil, pathBuilder), metrics, defaultConfig)
		explorer.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
		expectedParamsToSearch.AdditionalProperties.Vector = true // any custom additional params will trigger vector
		searcher.
			On("VectorSearch", expectedParamsToSearch).
			Return(searchResults, nil)
		metrics.On("AddUsageDimensions", "BestClass", "get_graphql", "nearCustomText", 128)

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("class search must be called with right params", func(t *testing.T) {
			assert.Nil(t, err)
			searcher.AssertExpectations(t)
		})

		t.Run("response must contain concepts", func(t *testing.T) {
			require.Len(t, res, 2)
			assert.Equal(t,
				map[string]interface{}{
					"name": "Foo",
					"_additional": map[string]interface{}{
						"vector": []float32(nil),
						"semanticPath": &SemanticPath{
							Path: []*SemanticPathElement{
								{
									Concept:            "pathelem1",
									DistanceToQuery:    0,
									DistanceToResult:   2.1,
									DistanceToPrevious: nil,
									DistanceToNext:     ptFloat32(0.5),
								},
								{
									Concept:            "pathelem2",
									DistanceToQuery:    2.1,
									DistanceToResult:   0,
									DistanceToPrevious: ptFloat32(0.5),
									DistanceToNext:     nil,
								},
							},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"name": "Bar",
					"_additional": map[string]interface{}{
						"vector": []float32(nil),
						"semanticPath": &SemanticPath{
							Path: []*SemanticPathElement{
								{
									Concept:            "pathelem1",
									DistanceToQuery:    0,
									DistanceToResult:   2.1,
									DistanceToPrevious: nil,
									DistanceToNext:     ptFloat32(0.5),
								},
								{
									Concept:            "pathelem2",
									DistanceToQuery:    2.1,
									DistanceToResult:   0,
									DistanceToPrevious: ptFloat32(0.5),
									DistanceToNext:     nil,
								},
							},
						},
					},
				}, res[1])
		})
	})
}

func ptFloat32(in float32) *float32 {
	return &in
}

type fakeModulesProvider struct {
	customC11yModule *fakeText2vecContextionaryModule
}

func (p *fakeModulesProvider) VectorFromInput(ctx context.Context, className string, input string) ([]float32, error) {
	panic("not implemented")
}

func (p *fakeModulesProvider) VectorFromSearchParam(ctx context.Context, className,
	param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn, tenant string,
) ([]float32, error) {
	txt2vec := p.getFakeT2Vec()
	vectorForParams := txt2vec.VectorSearches()["nearCustomText"]
	return vectorForParams(ctx, params, "", findVectorFn, nil)
}

func (p *fakeModulesProvider) CrossClassVectorFromSearchParam(ctx context.Context,
	param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn,
) ([]float32, error) {
	txt2vec := p.getFakeT2Vec()
	vectorForParams := txt2vec.VectorSearches()["nearCustomText"]
	return vectorForParams(ctx, params, "", findVectorFn, nil)
}

func (p *fakeModulesProvider) CrossClassValidateSearchParam(name string, value interface{}) error {
	return p.ValidateSearchParam(name, value, "")
}

func (p *fakeModulesProvider) ValidateSearchParam(name string, value interface{}, className string) error {
	txt2vec := p.getFakeT2Vec()
	arg := txt2vec.Arguments()["nearCustomText"]
	return arg.ValidateFunction(value)
}

func (p *fakeModulesProvider) GetExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{}, searchVector []float32,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	return p.additionalExtend(ctx, in, moduleParams, searchVector, "ExploreGet")
}

func (p *fakeModulesProvider) ListExploreAdditionalExtend(ctx context.Context, in []search.Result,
	moduleParams map[string]interface{},
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	return p.additionalExtend(ctx, in, moduleParams, nil, "ExploreList")
}

func (p *fakeModulesProvider) additionalExtend(ctx context.Context,
	in search.Results, moduleParams map[string]interface{},
	searchVector []float32, capability string,
) (search.Results, error) {
	txt2vec := p.getFakeT2Vec()
	if additionalProperties := txt2vec.AdditionalProperties(); len(additionalProperties) > 0 {
		for name, value := range moduleParams {
			additionalPropertyFn := p.getAdditionalPropertyFn(additionalProperties[name], capability)
			if additionalPropertyFn != nil && value != nil {
				searchValue := value
				if searchVectorValue, ok := value.(modulecapabilities.AdditionalPropertyWithSearchVector); ok {
					searchVectorValue.SetSearchVector(searchVector)
					searchValue = searchVectorValue
				}
				resArray, err := additionalPropertyFn(ctx, in, searchValue, nil, nil, nil)
				if err != nil {
					return nil, err
				}
				in = resArray
			} else {
				return nil, errors.Errorf("unknown capability: %s", name)
			}
		}
	}
	return in, nil
}

func (p *fakeModulesProvider) getAdditionalPropertyFn(additionalProperty modulecapabilities.AdditionalProperty,
	capability string,
) modulecapabilities.AdditionalPropertyFn {
	switch capability {
	case "ObjectGet":
		return additionalProperty.SearchFunctions.ObjectGet
	case "ObjectList":
		return additionalProperty.SearchFunctions.ObjectList
	case "ExploreGet":
		return additionalProperty.SearchFunctions.ExploreGet
	case "ExploreList":
		return additionalProperty.SearchFunctions.ExploreList
	default:
		return nil
	}
}

func (p *fakeModulesProvider) getFakeT2Vec() *fakeText2vecContextionaryModule {
	if p.customC11yModule != nil {
		return p.customC11yModule
	}
	return &fakeText2vecContextionaryModule{}
}

func extractNearCustomTextParam(param map[string]interface{}) interface{} {
	txt2vec := &fakeText2vecContextionaryModule{}
	argument := txt2vec.Arguments()["nearCustomText"]
	return argument.ExtractFunction(param)
}

func getDefaultParam(name string) interface{} {
	switch name {
	case "featureProjection":
		return &fakeProjectorParams{}
	case "semanticPath":
		return &pathBuilderParams{}
	case "nearestNeighbors":
		return true
	default:
		return nil
	}
}

func getFakeModulesProviderWithCustomExtenders(
	customExtender *fakeExtender,
	customProjector *fakeProjector,
	customPathBuilder *fakePathBuilder,
) ModulesProvider {
	return &fakeModulesProvider{
		newFakeText2vecContextionaryModuleWithCustomExtender(customExtender, customProjector, customPathBuilder),
	}
}

func getFakeModulesProvider() ModulesProvider {
	return &fakeModulesProvider{}
}
