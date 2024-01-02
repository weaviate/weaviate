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

package objects

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

func Test_GetAction(t *testing.T) {
	var (
		vectorRepo    *fakeVectorRepo
		manager       *Manager
		extender      *fakeExtender
		projectorFake *fakeProjector
		metrics       *fakeMetrics
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "ActionClass",
				},
			},
		},
	}

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		cfg.Config.QueryDefaults.Limit = 20
		cfg.Config.QueryMaximumResults = 200
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender = &fakeExtender{}
		projectorFake = &fakeProjector{}
		metrics = &fakeMetrics{}
		manager = NewManager(locks, schemaManager, cfg, logger,
			authorizer, vectorRepo,
			getFakeModulesProviderWithCustomExtenders(extender, projectorFake), metrics)
	}

	t.Run("get non-existing action by id", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return((*search.Result)(nil), nil).Once()

		_, err := manager.GetObject(context.Background(), &models.Principal{}, "",
			id, additional.Properties{}, nil, "")
		assert.Equal(t, NewErrNotFound("no object with id '99ee9968-22ec-416a-9032-cff80f2f7fdf'"), err)
	})

	t.Run("get existing action by id", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		result := &search.Result{
			ID:        id,
			ClassName: "ActionClass",
			Schema:    map[string]interface{}{"foo": "bar"},
		}
		vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()

		expected := &models.Object{
			ID:            id,
			Class:         "ActionClass",
			Properties:    map[string]interface{}{"foo": "bar"},
			VectorWeights: (map[string]string)(nil),
		}

		res, err := manager.GetObject(context.Background(), &models.Principal{}, "",
			id, additional.Properties{}, nil, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("get existing object by id with vector without classname (deprecated)", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		result := &search.Result{
			ID:        id,
			ClassName: "ActionClass",
			Schema:    map[string]interface{}{"foo": "bar"},
			Vector:    []float32{1, 2, 3},
			Dims:      3,
		}
		vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()

		expected := &models.Object{
			ID:            id,
			Class:         "ActionClass",
			Properties:    map[string]interface{}{"foo": "bar"},
			VectorWeights: (map[string]string)(nil),
			Vector:        []float32{1, 2, 3},
		}

		metrics.On("AddUsageDimensions", "ActionClass", "get_rest", "single_include_vector", 3)

		res, err := manager.GetObject(context.Background(), &models.Principal{}, "",
			id, additional.Properties{Vector: true}, nil, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("get existing object by id with vector with classname", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		result := &search.Result{
			ID:        id,
			ClassName: "ActionClass",
			Schema:    map[string]interface{}{"foo": "bar"},
			Vector:    []float32{1, 2, 3},
			Dims:      3,
		}
		vectorRepo.On("Object", "ActionClass", id, mock.Anything, mock.Anything, "").
			Return(result, nil).Once()

		expected := &models.Object{
			ID:            id,
			Class:         "ActionClass",
			Properties:    map[string]interface{}{"foo": "bar"},
			VectorWeights: (map[string]string)(nil),
			Vector:        []float32{1, 2, 3},
		}

		metrics.On("AddUsageDimensions", "ActionClass", "get_rest", "single_include_vector", 3)

		res, err := manager.GetObject(context.Background(), &models.Principal{},
			"ActionClass", id, additional.Properties{Vector: true}, nil, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("list all existing actions with all default pagination settings", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		results := []search.Result{
			{
				ID:        id,
				ClassName: "ActionClass",
				Schema:    map[string]interface{}{"foo": "bar"},
			},
		}
		vectorRepo.On("ObjectSearch", 0, 20, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything).Return(results, nil).Once()

		expected := []*models.Object{
			{
				ID:            id,
				Class:         "ActionClass",
				Properties:    map[string]interface{}{"foo": "bar"},
				VectorWeights: (map[string]string)(nil),
			},
		}

		res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, nil, nil, nil, nil, additional.Properties{}, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("list all existing objects with vectors", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		results := []search.Result{
			{
				ID:        id,
				ClassName: "ActionClass",
				Schema:    map[string]interface{}{"foo": "bar"},
				Vector:    []float32{1, 2, 3},
				Dims:      3,
			},
		}
		vectorRepo.On("ObjectSearch", 0, 20, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything).Return(results, nil).Once()

		metrics.On("AddUsageDimensions", "ActionClass", "get_rest", "list_include_vector", 3)

		expected := []*models.Object{
			{
				ID:            id,
				Class:         "ActionClass",
				Properties:    map[string]interface{}{"foo": "bar"},
				VectorWeights: (map[string]string)(nil),
				Vector:        []float32{1, 2, 3},
			},
		}

		res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, nil, nil, nil, nil, additional.Properties{Vector: true}, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("list all existing actions with all explicit offset and limit", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		results := []search.Result{
			{
				ID:        id,
				ClassName: "ActionClass",
				Schema:    map[string]interface{}{"foo": "bar"},
			},
		}
		vectorRepo.On("ObjectSearch", 7, 2, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything).Return(results, nil).Once()

		expected := []*models.Object{
			{
				ID:            id,
				Class:         "ActionClass",
				Properties:    map[string]interface{}{"foo": "bar"},
				VectorWeights: (map[string]string)(nil),
			},
		}

		res, err := manager.GetObjects(context.Background(), &models.Principal{}, ptInt64(7), ptInt64(2), nil, nil, nil, additional.Properties{}, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("with an offset greater than the maximum", func(t *testing.T) {
		reset()

		_, err := manager.GetObjects(context.Background(), &models.Principal{}, ptInt64(201), ptInt64(2), nil, nil, nil, additional.Properties{}, "")
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "query maximum results exceeded")
	})

	t.Run("with a limit greater than the minimum", func(t *testing.T) {
		reset()

		_, err := manager.GetObjects(context.Background(), &models.Principal{}, ptInt64(0), ptInt64(202), nil, nil, nil, additional.Properties{}, "")
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "query maximum results exceeded")
	})

	t.Run("with limit and offset individually smaller, but combined greater", func(t *testing.T) {
		reset()

		_, err := manager.GetObjects(context.Background(), &models.Principal{}, ptInt64(150), ptInt64(150), nil, nil, nil, additional.Properties{}, "")
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "query maximum results exceeded")
	})

	t.Run("additional props", func(t *testing.T) {
		t.Run("on get single requests", func(t *testing.T) {
			t.Run("feature projection", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := &search.Result{
					ID:        id,
					ClassName: "ActionClass",
					Schema:    map[string]interface{}{"foo": "bar"},
				}
				vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()
				_, err := manager.GetObject(context.Background(), &models.Principal{}, "",
					id, additional.Properties{
						ModuleParams: map[string]interface{}{
							"featureProjection": getDefaultParam("featureProjection"),
						},
					}, nil, "")
				assert.Equal(t, errors.New("get extend: unknown capability: featureProjection"), err)
			})

			t.Run("semantic path", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := &search.Result{
					ID:        id,
					ClassName: "ActionClass",
					Schema:    map[string]interface{}{"foo": "bar"},
				}
				vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()
				_, err := manager.GetObject(context.Background(), &models.Principal{}, "",
					id, additional.Properties{
						ModuleParams: map[string]interface{}{
							"semanticPath": getDefaultParam("semanticPath"),
						},
					}, nil, "")
				assert.Equal(t, errors.New("get extend: unknown capability: semanticPath"), err)
			})

			t.Run("nearest neighbors", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := &search.Result{
					ID:        id,
					ClassName: "ActionClass",
					Schema:    map[string]interface{}{"foo": "bar"},
				}
				vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()
				extender.multi = []search.Result{
					{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: models.AdditionalProperties{
							"nearestNeighbors": &NearestNeighbors{
								Neighbors: []*NearestNeighbor{
									{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				expected := &models.Object{
					ID:            id,
					Class:         "ActionClass",
					Properties:    map[string]interface{}{"foo": "bar"},
					VectorWeights: (map[string]string)(nil),
					Additional: models.AdditionalProperties{
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "foo",
									Distance: 0.3,
								},
							},
						},
					},
				}

				res, err := manager.GetObject(context.Background(), &models.Principal{}, "",
					id, additional.Properties{
						ModuleParams: map[string]interface{}{
							"nearestNeighbors": true,
						},
					}, nil, "")
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})
		})

		t.Run("on list requests", func(t *testing.T) {
			t.Run("nearest neighbors", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := []search.Result{
					{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
					},
				}
				vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
					mock.Anything).Return(result, nil).Once()
				extender.multi = []search.Result{
					{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: models.AdditionalProperties{
							"nearestNeighbors": &NearestNeighbors{
								Neighbors: []*NearestNeighbor{
									{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				expected := []*models.Object{
					{
						ID:            id,
						Class:         "ActionClass",
						Properties:    map[string]interface{}{"foo": "bar"},
						VectorWeights: (map[string]string)(nil),
						Additional: models.AdditionalProperties{
							"nearestNeighbors": &NearestNeighbors{
								Neighbors: []*NearestNeighbor{
									{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), nil, nil, nil, additional.Properties{
					ModuleParams: map[string]interface{}{
						"nearestNeighbors": true,
					},
				}, "")
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})

			t.Run("feature projection", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := []search.Result{
					{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
					},
				}
				vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
					mock.Anything).Return(result, nil).Once()
				projectorFake.multi = []search.Result{
					{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: models.AdditionalProperties{
							"featureProjection": &FeatureProjection{
								Vector: []float32{1, 2, 3},
							},
						},
					},
				}

				expected := []*models.Object{
					{
						ID:            id,
						Class:         "ActionClass",
						Properties:    map[string]interface{}{"foo": "bar"},
						VectorWeights: (map[string]string)(nil),
						Additional: models.AdditionalProperties{
							"featureProjection": &FeatureProjection{
								Vector: []float32{1, 2, 3},
							},
						},
					},
				}

				res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), nil, nil, nil, additional.Properties{
					ModuleParams: map[string]interface{}{
						"featureProjection": getDefaultParam("featureProjection"),
					},
				}, "")
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})
		})
	})

	t.Run("sort props", func(t *testing.T) {
		t.Run("sort=foo,number&order=asc,desc", func(t *testing.T) {
			reset()
			id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")
			sort := "foo,number"
			asc := "asc,desc"
			expectedSort := []filters.Sort{
				{Path: []string{"foo"}, Order: "asc"},
				{Path: []string{"number"}, Order: "desc"},
			}

			result := []search.Result{
				{
					ID:        id,
					ClassName: "ActionClass",
					Schema: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
				},
			}
			vectorRepo.On("ObjectSearch", mock.AnythingOfType("int"), mock.AnythingOfType("int"), expectedSort,
				mock.Anything, mock.Anything, mock.Anything).Return(result, nil).Once()
			projectorFake.multi = []search.Result{
				{
					ID:        id,
					ClassName: "ActionClass",
					Schema: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
				},
			}

			expected := []*models.Object{
				{
					ID:    id,
					Class: "ActionClass",
					Properties: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
					VectorWeights: (map[string]string)(nil),
				},
			}

			res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), &sort, &asc, nil, additional.Properties{}, "")
			require.Nil(t, err)
			assert.Equal(t, expected, res)
		})

		t.Run("sort=foo,number,prop1,prop2&order=desc", func(t *testing.T) {
			reset()
			id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")
			sort := "foo,number,prop1,prop2"
			asc := "desc"
			expectedSort := []filters.Sort{
				{Path: []string{"foo"}, Order: "desc"},
				{Path: []string{"number"}, Order: "asc"},
				{Path: []string{"prop1"}, Order: "asc"},
				{Path: []string{"prop2"}, Order: "asc"},
			}

			result := []search.Result{
				{
					ID:        id,
					ClassName: "ActionClass",
					Schema: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
				},
			}
			vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, expectedSort, mock.Anything, mock.Anything,
				mock.Anything).Return(result, nil).Once()
			projectorFake.multi = []search.Result{
				{
					ID:        id,
					ClassName: "ActionClass",
					Schema: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
				},
			}

			expected := []*models.Object{
				{
					ID:    id,
					Class: "ActionClass",
					Properties: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
					VectorWeights: (map[string]string)(nil),
				},
			}

			res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), &sort, &asc, nil, additional.Properties{}, "")
			require.Nil(t, err)
			assert.Equal(t, expected, res)
		})

		t.Run("sort=foo,number", func(t *testing.T) {
			reset()
			sort := "foo,number"
			expectedSort := []filters.Sort{
				{Path: []string{"foo"}, Order: "asc"},
				{Path: []string{"number"}, Order: "asc"},
			}
			result := []search.Result{
				{
					ID:        "uuid",
					ClassName: "ActionClass",
					Schema: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
				},
			}

			vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, expectedSort, mock.Anything, mock.Anything,
				mock.Anything).Return(result, nil).Once()

			_, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), &sort, nil, nil, additional.Properties{}, "")
			require.Nil(t, err)
		})

		t.Run("sort=foo,number,prop", func(t *testing.T) {
			reset()
			sort := "foo,number,prop"
			expectedSort := []filters.Sort{
				{Path: []string{"foo"}, Order: "asc"},
				{Path: []string{"number"}, Order: "asc"},
				{Path: []string{"prop"}, Order: "asc"},
			}
			result := []search.Result{
				{
					ID:        "uuid",
					ClassName: "ActionClass",
					Schema: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
				},
			}

			vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, expectedSort, mock.Anything, mock.Anything,
				mock.Anything).Return(result, nil).Once()

			_, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), &sort, nil, nil, additional.Properties{}, "")
			require.Nil(t, err)
		})

		t.Run("order=asc", func(t *testing.T) {
			reset()
			order := "asc"
			var expectedSort []filters.Sort
			result := []search.Result{
				{
					ID:        "uuid",
					ClassName: "ActionClass",
					Schema: map[string]interface{}{
						"foo":    "bar",
						"number": float64(1),
					},
				},
			}

			vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, expectedSort, mock.Anything, mock.Anything,
				mock.Anything).Return(result, nil).Once()

			_, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), nil, &order, nil, additional.Properties{}, "")
			require.Nil(t, err)
		})
	})
}

func Test_GetThing(t *testing.T) {
	var (
		vectorRepo    *fakeVectorRepo
		manager       *Manager
		extender      *fakeExtender
		projectorFake *fakeProjector
	)

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "ThingClass",
				},
			},
		},
	}

	reset := func() {
		vectorRepo = &fakeVectorRepo{}
		schemaManager := &fakeSchemaManager{
			GetSchemaResponse: schema,
		}
		locks := &fakeLocks{}
		cfg := &config.WeaviateConfig{}
		cfg.Config.QueryDefaults.Limit = 20
		cfg.Config.QueryMaximumResults = 200
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender = &fakeExtender{}
		projectorFake = &fakeProjector{}
		metrics := &fakeMetrics{}
		manager = NewManager(locks, schemaManager, cfg, logger,
			authorizer, vectorRepo,
			getFakeModulesProviderWithCustomExtenders(extender, projectorFake), metrics)
	}

	t.Run("get non-existing thing by id", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return((*search.Result)(nil), nil).Once()

		_, err := manager.GetObject(context.Background(), &models.Principal{}, "", id,
			additional.Properties{}, nil, "")
		assert.Equal(t, NewErrNotFound("no object with id '99ee9968-22ec-416a-9032-cff80f2f7fdf'"), err)
	})

	t.Run("get existing thing by id", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		result := &search.Result{
			ID:        id,
			ClassName: "ThingClass",
			Schema:    map[string]interface{}{"foo": "bar"},
		}
		vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()

		expected := &models.Object{
			ID:            id,
			Class:         "ThingClass",
			Properties:    map[string]interface{}{"foo": "bar"},
			VectorWeights: (map[string]string)(nil),
		}

		res, err := manager.GetObject(context.Background(), &models.Principal{}, "", id,
			additional.Properties{}, nil, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("list all existing things", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		results := []search.Result{
			{
				ID:        id,
				ClassName: "ThingClass",
				Schema:    map[string]interface{}{"foo": "bar"},
			},
		}
		vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
			mock.Anything).Return(results, nil).Once()

		expected := []*models.Object{
			{
				ID:            id,
				Class:         "ThingClass",
				Properties:    map[string]interface{}{"foo": "bar"},
				VectorWeights: (map[string]string)(nil),
			},
		}

		res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, nil, nil, nil, nil, additional.Properties{}, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("additional props", func(t *testing.T) {
		t.Run("on get single requests", func(t *testing.T) {
			t.Run("feature projection", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := &search.Result{
					ID:        id,
					ClassName: "ThingClass",
					Schema:    map[string]interface{}{"foo": "bar"},
				}
				vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()
				_, err := manager.GetObject(context.Background(), &models.Principal{}, "",
					id, additional.Properties{
						ModuleParams: map[string]interface{}{
							"featureProjection": getDefaultParam("featureProjection"),
						},
					}, nil, "")
				assert.Equal(t, errors.New("get extend: unknown capability: featureProjection"), err)
			})

			t.Run("nearest neighbors", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := &search.Result{
					ID:        id,
					ClassName: "ThingClass",
					Schema:    map[string]interface{}{"foo": "bar"},
				}
				vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()
				extender.multi = []search.Result{
					{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: models.AdditionalProperties{
							"nearestNeighbors": &NearestNeighbors{
								Neighbors: []*NearestNeighbor{
									{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				expected := &models.Object{
					ID:            id,
					Class:         "ThingClass",
					Properties:    map[string]interface{}{"foo": "bar"},
					VectorWeights: (map[string]string)(nil),
					Additional: models.AdditionalProperties{
						"nearestNeighbors": &NearestNeighbors{
							Neighbors: []*NearestNeighbor{
								{
									Concept:  "foo",
									Distance: 0.3,
								},
							},
						},
					},
				}

				res, err := manager.GetObject(context.Background(), &models.Principal{}, "",
					id, additional.Properties{
						ModuleParams: map[string]interface{}{
							"nearestNeighbors": true,
						},
					}, nil, "")
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})
		})

		t.Run("on list requests", func(t *testing.T) {
			t.Run("nearest neighbors", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := []search.Result{
					{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
					},
				}
				vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
					mock.Anything).Return(result, nil).Once()
				extender.multi = []search.Result{
					{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: models.AdditionalProperties{
							"nearestNeighbors": &NearestNeighbors{
								Neighbors: []*NearestNeighbor{
									{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				expected := []*models.Object{
					{
						ID:            id,
						Class:         "ThingClass",
						Properties:    map[string]interface{}{"foo": "bar"},
						VectorWeights: (map[string]string)(nil),
						Additional: models.AdditionalProperties{
							"nearestNeighbors": &NearestNeighbors{
								Neighbors: []*NearestNeighbor{
									{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), nil, nil, nil, additional.Properties{
					ModuleParams: map[string]interface{}{
						"nearestNeighbors": true,
					},
				}, "")
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})

			t.Run("feature projection", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := []search.Result{
					{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
					},
				}
				vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything,
					mock.Anything).Return(result, nil).Once()
				projectorFake.multi = []search.Result{
					{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: models.AdditionalProperties{
							"featureProjection": &FeatureProjection{
								Vector: []float32{1, 2, 3},
							},
						},
					},
				}

				expected := []*models.Object{
					{
						ID:            id,
						Class:         "ThingClass",
						Properties:    map[string]interface{}{"foo": "bar"},
						VectorWeights: (map[string]string)(nil),
						Additional: models.AdditionalProperties{
							"featureProjection": &FeatureProjection{
								Vector: []float32{1, 2, 3},
							},
						},
					},
				}

				res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, ptInt64(10), nil, nil, nil, additional.Properties{
					ModuleParams: map[string]interface{}{
						"featureProjection": getDefaultParam("featureProjection"),
					},
				}, "")
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})
		})
	})
}

func Test_GetObject(t *testing.T) {
	var (
		principal = models.Principal{}
		adds      = additional.Properties{}
		className = "MyClass"
		id        = strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")
		schema    = schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{
					{
						Class: className,
					},
				},
			},
		}
		result = &search.Result{
			ID:        id,
			ClassName: className,
			Schema:    map[string]interface{}{"foo": "bar"},
		}
	)

	t.Run("without projection", func(t *testing.T) {
		m := newFakeGetManager(schema)
		m.repo.On("Object", className, id, mock.Anything, mock.Anything, "").Return((*search.Result)(nil), nil).Once()
		_, err := m.GetObject(context.Background(), &principal, className, id, adds, nil, "")
		if err == nil {
			t.Errorf("GetObject() must return an error for non existing object")
		}

		m.repo.On("Object", className, id, mock.Anything, mock.Anything, "").Return(result, nil).Once()
		expected := &models.Object{
			ID:            id,
			Class:         className,
			Properties:    map[string]interface{}{"foo": "bar"},
			VectorWeights: (map[string]string)(nil),
		}

		got, err := m.GetObject(context.Background(), &principal, className, id, adds, nil, "")
		require.Nil(t, err)
		assert.Equal(t, expected, got)
	})

	t.Run("with projection", func(t *testing.T) {
		m := newFakeGetManager(schema)
		m.extender.multi = []search.Result{
			{
				ID:        id,
				ClassName: className,
				Schema:    map[string]interface{}{"foo": "bar"},
				AdditionalProperties: models.AdditionalProperties{
					"nearestNeighbors": &NearestNeighbors{
						Neighbors: []*NearestNeighbor{
							{
								Concept:  "foo",
								Distance: 0.3,
							},
						},
					},
				},
			},
		}
		m.repo.On("Object", className, id, mock.Anything, mock.Anything, "").Return(result, nil).Once()
		_, err := m.GetObject(context.Background(), &principal, className, id,
			additional.Properties{
				ModuleParams: map[string]interface{}{
					"Unknown": getDefaultParam("Unknown"),
				},
			}, nil, "")
		if err == nil {
			t.Errorf("GetObject() must return unknown feature projection error")
		}

		m.repo.On("Object", className, id, mock.Anything, mock.Anything, "").Return(result, nil).Once()
		expected := &models.Object{
			ID:            id,
			Class:         className,
			Properties:    map[string]interface{}{"foo": "bar"},
			VectorWeights: (map[string]string)(nil),
			Additional: models.AdditionalProperties{
				"nearestNeighbors": &NearestNeighbors{
					Neighbors: []*NearestNeighbor{
						{
							Concept:  "foo",
							Distance: 0.3,
						},
					},
				},
			},
		}

		res, err := m.GetObject(context.Background(), &principal, className, id,
			additional.Properties{
				ModuleParams: map[string]interface{}{
					"nearestNeighbors": true,
				},
			}, nil, "")
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})
}

func ptInt64(in int64) *int64 {
	return &in
}

type fakeGetManager struct {
	*Manager
	repo            *fakeVectorRepo
	extender        *fakeExtender
	projector       *fakeProjector
	authorizer      *fakeAuthorizer
	locks           *fakeLocks
	metrics         *fakeMetrics
	modulesProvider *fakeModulesProvider
}

func newFakeGetManager(schema schema.Schema, opts ...func(*fakeGetManager)) fakeGetManager {
	r := fakeGetManager{
		repo:            new(fakeVectorRepo),
		extender:        new(fakeExtender),
		projector:       new(fakeProjector),
		authorizer:      new(fakeAuthorizer),
		locks:           new(fakeLocks),
		metrics:         new(fakeMetrics),
		modulesProvider: new(fakeModulesProvider),
	}

	for _, opt := range opts {
		opt(&r)
	}

	schemaManager := &fakeSchemaManager{
		GetSchemaResponse: schema,
	}
	cfg := &config.WeaviateConfig{}
	cfg.Config.QueryDefaults.Limit = 20
	cfg.Config.QueryMaximumResults = 200
	cfg.Config.TrackVectorDimensions = true
	logger, _ := test.NewNullLogger()
	r.modulesProvider = getFakeModulesProviderWithCustomExtenders(r.extender, r.projector)
	r.Manager = NewManager(r.locks, schemaManager, cfg, logger,
		r.authorizer, r.repo, r.modulesProvider, r.metrics)

	return r
}
