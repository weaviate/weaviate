//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/projector"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_GetAction(t *testing.T) {
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
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender = &fakeExtender{}
		projectorFake = &fakeProjector{}
		vectorizer := &fakeVectorizer{}
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer,
			vectorizer, vectorRepo, extender, projectorFake)
	}

	t.Run("get non-existing action by id", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return((*search.Result)(nil), nil).Once()

		_, err := manager.GetObject(context.Background(), &models.Principal{}, id, traverser.AdditionalProperties{})
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

		res, err := manager.GetObject(context.Background(), &models.Principal{}, id, traverser.AdditionalProperties{})
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("list all existing actions", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		results := []search.Result{
			search.Result{
				ID:        id,
				ClassName: "ActionClass",
				Schema:    map[string]interface{}{"foo": "bar"},
			},
		}
		vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything).Return(results, nil).Once()

		expected := []*models.Object{
			&models.Object{
				ID:            id,
				Class:         "ActionClass",
				Properties:    map[string]interface{}{"foo": "bar"},
				VectorWeights: (map[string]string)(nil),
			},
		}

		res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, traverser.AdditionalProperties{})
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
					ClassName: "ActionClass",
					Schema:    map[string]interface{}{"foo": "bar"},
				}
				vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return(result, nil).Once()
				_, err := manager.GetObject(context.Background(), &models.Principal{}, id,
					traverser.AdditionalProperties{
						FeatureProjection: &projector.Params{},
					})
				assert.Equal(t, errors.New("feature projection is not possible on a non-list request"), err)
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
				extender.single = &search.Result{
					ID:        id,
					ClassName: "ActionClass",
					Schema:    map[string]interface{}{"foo": "bar"},
					AdditionalProperties: &models.AdditionalProperties{
						NearestNeighbors: &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "foo",
									Distance: 0.3,
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
					Additional: &models.AdditionalProperties{
						NearestNeighbors: &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "foo",
									Distance: 0.3,
								},
							},
						},
					},
				}

				res, err := manager.GetObject(context.Background(), &models.Principal{}, id,
					traverser.AdditionalProperties{
						NearestNeighbors: true,
					})
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})
		})

		t.Run("on list requests", func(t *testing.T) {
			t.Run("nearest neighbors", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := []search.Result{
					search.Result{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
					},
				}
				vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything).Return(result, nil).Once()
				extender.multi = []search.Result{
					search.Result{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: &models.AdditionalProperties{
							NearestNeighbors: &models.NearestNeighbors{
								Neighbors: []*models.NearestNeighbor{
									&models.NearestNeighbor{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				expected := []*models.Object{
					&models.Object{
						ID:            id,
						Class:         "ActionClass",
						Properties:    map[string]interface{}{"foo": "bar"},
						VectorWeights: (map[string]string)(nil),
						Additional: &models.AdditionalProperties{
							NearestNeighbors: &models.NearestNeighbors{
								Neighbors: []*models.NearestNeighbor{
									&models.NearestNeighbor{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				res, err := manager.GetObjects(context.Background(), &models.Principal{}, ptInt64(10),
					traverser.AdditionalProperties{
						NearestNeighbors: true,
					})
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})

			t.Run("feature projection", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := []search.Result{
					search.Result{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
					},
				}
				vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything).Return(result, nil).Once()
				projectorFake.multi = []search.Result{
					search.Result{
						ID:        id,
						ClassName: "ActionClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: &models.AdditionalProperties{
							FeatureProjection: &models.FeatureProjection{
								Vector: []float32{1, 2, 3},
							},
						},
					},
				}

				expected := []*models.Object{
					&models.Object{
						ID:            id,
						Class:         "ActionClass",
						Properties:    map[string]interface{}{"foo": "bar"},
						VectorWeights: (map[string]string)(nil),
						Additional: &models.AdditionalProperties{
							FeatureProjection: &models.FeatureProjection{
								Vector: []float32{1, 2, 3},
							},
						},
					},
				}

				res, err := manager.GetObjects(context.Background(), &models.Principal{}, ptInt64(10),
					traverser.AdditionalProperties{
						FeatureProjection: &projector.Params{},
					})
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})
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
		authorizer := &fakeAuthorizer{}
		logger, _ := test.NewNullLogger()
		extender = &fakeExtender{}
		projectorFake = &fakeProjector{}
		vectorizer := &fakeVectorizer{}
		manager = NewManager(locks, schemaManager, cfg, logger, authorizer,
			vectorizer, vectorRepo, extender, projectorFake)
	}

	t.Run("get non-existing thing by id", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		vectorRepo.On("ObjectByID", id, mock.Anything, mock.Anything).Return((*search.Result)(nil), nil).Once()

		_, err := manager.GetObject(context.Background(), &models.Principal{}, id, traverser.AdditionalProperties{})
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

		res, err := manager.GetObject(context.Background(), &models.Principal{}, id, traverser.AdditionalProperties{})
		require.Nil(t, err)
		assert.Equal(t, expected, res)
	})

	t.Run("list all existing things", func(t *testing.T) {
		reset()
		id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

		results := []search.Result{
			search.Result{
				ID:        id,
				ClassName: "ThingClass",
				Schema:    map[string]interface{}{"foo": "bar"},
			},
		}
		vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything).Return(results, nil).Once()

		expected := []*models.Object{
			&models.Object{
				ID:            id,
				Class:         "ThingClass",
				Properties:    map[string]interface{}{"foo": "bar"},
				VectorWeights: (map[string]string)(nil),
			},
		}

		res, err := manager.GetObjects(context.Background(), &models.Principal{}, nil, traverser.AdditionalProperties{})
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
				_, err := manager.GetObject(context.Background(), &models.Principal{}, id,
					traverser.AdditionalProperties{
						FeatureProjection: &projector.Params{},
					})
				assert.Equal(t, errors.New("feature projection is not possible on a non-list request"), err)
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
				extender.single = &search.Result{
					ID:        id,
					ClassName: "ThingClass",
					Schema:    map[string]interface{}{"foo": "bar"},
					AdditionalProperties: &models.AdditionalProperties{
						NearestNeighbors: &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "foo",
									Distance: 0.3,
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
					Additional: &models.AdditionalProperties{
						NearestNeighbors: &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "foo",
									Distance: 0.3,
								},
							},
						},
					},
				}

				res, err := manager.GetObject(context.Background(), &models.Principal{}, id,
					traverser.AdditionalProperties{
						NearestNeighbors: true,
					})
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})
		})

		t.Run("on list requests", func(t *testing.T) {
			t.Run("nearest neighbors", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := []search.Result{
					search.Result{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
					},
				}
				vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything).Return(result, nil).Once()
				extender.multi = []search.Result{
					search.Result{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: &models.AdditionalProperties{
							NearestNeighbors: &models.NearestNeighbors{
								Neighbors: []*models.NearestNeighbor{
									&models.NearestNeighbor{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				expected := []*models.Object{
					&models.Object{
						ID:            id,
						Class:         "ThingClass",
						Properties:    map[string]interface{}{"foo": "bar"},
						VectorWeights: (map[string]string)(nil),
						Additional: &models.AdditionalProperties{
							NearestNeighbors: &models.NearestNeighbors{
								Neighbors: []*models.NearestNeighbor{
									&models.NearestNeighbor{
										Concept:  "foo",
										Distance: 0.3,
									},
								},
							},
						},
					},
				}

				res, err := manager.GetObjects(context.Background(), &models.Principal{}, ptInt64(10),
					traverser.AdditionalProperties{
						NearestNeighbors: true,
					})
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})

			t.Run("feature projection", func(t *testing.T) {
				reset()
				id := strfmt.UUID("99ee9968-22ec-416a-9032-cff80f2f7fdf")

				result := []search.Result{
					search.Result{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
					},
				}
				vectorRepo.On("ObjectSearch", mock.Anything, mock.Anything, mock.Anything).Return(result, nil).Once()
				projectorFake.multi = []search.Result{
					search.Result{
						ID:        id,
						ClassName: "ThingClass",
						Schema:    map[string]interface{}{"foo": "bar"},
						AdditionalProperties: &models.AdditionalProperties{
							FeatureProjection: &models.FeatureProjection{
								Vector: []float32{1, 2, 3},
							},
						},
					},
				}

				expected := []*models.Object{
					&models.Object{
						ID:            id,
						Class:         "ThingClass",
						Properties:    map[string]interface{}{"foo": "bar"},
						VectorWeights: (map[string]string)(nil),
						Additional: &models.AdditionalProperties{
							FeatureProjection: &models.FeatureProjection{
								Vector: []float32{1, 2, 3},
							},
						},
					},
				}

				res, err := manager.GetObjects(context.Background(), &models.Principal{}, ptInt64(10),
					traverser.AdditionalProperties{
						FeatureProjection: &projector.Params{},
					})
				require.Nil(t, err)
				assert.Equal(t, expected, res)
			})
		})
	})
}

func ptInt64(in int64) *int64 {
	return &in
}
