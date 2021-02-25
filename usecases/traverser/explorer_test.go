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

package traverser

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/search"
	libprojector "github.com/semi-technologies/weaviate/usecases/projector"
	"github.com/semi-technologies/weaviate/usecases/sempath"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Explorer_GetClass(t *testing.T) {
	t.Run("when an explore param is set for nearVector", func(t *testing.T) {
		// TODO: this is a module specific test case, which relies on the
		// text2vec-contextionary module
		params := GetParams{
			ClassName: "BestClass",
			NearVector: &NearVectorParams{
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
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{0.8, 0.2, 0.7}
		search.
			On("VectorClassSearch", expectedParamsToSearch).
			Return(searchResults, nil)

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

	t.Run("when an explore param is set for nearObject without id and beacon", func(t *testing.T) {
		// TODO: this is a module specific test case, which relies on the
		// text2vec-contextionary module
		params := GetParams{
			ClassName: "BestClass",
			NearObject: &NearObjectParams{
				Certainty: 0.9,
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())

		res, err := explorer.GetClass(context.Background(), params)

		t.Run("vector search must be called with right params", func(t *testing.T) {
			assert.NotNil(t, err)
			assert.Nil(t, res)
			assert.Contains(t, err.Error(), "explorer: get class: vectorize params: nearObject params: empty id and beacon")
		})
	})

	t.Run("when an explore param is set for nearObject with beacon", func(t *testing.T) {
		// TODO: this is a module specific test case, which relies on the
		// text2vec-contextionary module
		params := GetParams{
			ClassName: "BestClass",
			NearObject: &NearObjectParams{
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
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		search.
			On("ObjectByID", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e041")).
			Return(&searchRes, nil)
		search.
			On("VectorClassSearch", expectedParamsToSearch).
			Return(searchResults, nil)

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

	t.Run("when an explore param is set for nearObject with id", func(t *testing.T) {
		// TODO: this is a module specific test case, which relies on the
		// text2vec-contextionary module
		params := GetParams{
			ClassName: "BestClass",
			NearObject: &NearObjectParams{
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
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		search.
			On("ObjectByID", strfmt.UUID("e9c12c22-766f-4bde-b140-d4cf8fd6e041")).
			Return(&searchRes, nil)
		search.
			On("VectorClassSearch", expectedParamsToSearch).
			Return(searchResults, nil)

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

	t.Run("when an explore param is set for nearVector and the required certainty not met",
		func(t *testing.T) {
			params := GetParams{
				ClassName: "BestClass",
				NearVector: &NearVectorParams{
					Vector:    []float32{0.8, 0.2, 0.7},
					Certainty: 0.8,
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			searchResults := []search.Result{
				{
					ID: "id1",
				},
				{
					ID: "id2",
				},
			}

			search := &fakeVectorSearcher{}
			extender := &fakeExtender{}
			log, _ := test.NewNullLogger()

			projector := &fakeProjector{}
			pathBuilder := &fakePathBuilder{}
			explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
			expectedParamsToSearch := params
			expectedParamsToSearch.SearchVector = []float32{0.8, 0.2, 0.7}
			search.
				On("VectorClassSearch", expectedParamsToSearch).
				Return(searchResults, nil)

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.Nil(t, err)
				search.AssertExpectations(t)
			})

			t.Run("no concept met the required certainty", func(t *testing.T) {
				assert.Len(t, res, 0)
			})
		})

	t.Run("when two conflicting (nearVector, nearObject) near searchers are set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearVector: &NearVectorParams{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			NearObject: &NearObjectParams{
				Beacon: "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e041",
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when no explore param is set", func(t *testing.T) {
		params := GetParams{
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
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("ClassSearch", expectedParamsToSearch).
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

	t.Run("when the classification prop is set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				Classification: true,
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				AdditionalProperties: &models.AdditionalProperties{
					Classification: nil,
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				AdditionalProperties: &models.AdditionalProperties{
					Classification: &models.AdditionalPropertiesClassification{
						ID: "1234",
					},
				},
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("ClassSearch", expectedParamsToSearch).
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
						"classification": &models.AdditionalPropertiesClassification{
							ID: "1234",
						},
					},
				}, res[1])
		})
	})

	t.Run("when the interpretation prop is set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				Interpretation: true,
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				AdditionalProperties: &models.AdditionalProperties{
					Interpretation: nil,
				},
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				AdditionalProperties: &models.AdditionalProperties{
					Interpretation: &models.Interpretation{
						Source: []*models.InterpretationSource{
							&models.InterpretationSource{
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
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		search.
			On("ClassSearch", expectedParamsToSearch).
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
						"interpretation": &models.Interpretation{
							Source: []*models.InterpretationSource{
								&models.InterpretationSource{
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

	t.Run("when the nearestNeighbors prop is set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				NearestNeighbors: true,
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
					AdditionalProperties: &models.AdditionalProperties{
						NearestNeighbors: &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
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
					AdditionalProperties: &models.AdditionalProperties{
						NearestNeighbors: &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "bar",
									Distance: 0.1,
								},
							},
						},
					},
				},
			},
		}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(searcher, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		searcher.
			On("ClassSearch", expectedParamsToSearch).
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
						"nearestNeighbors": &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
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
						"nearestNeighbors": &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
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
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				FeatureProjection: &libprojector.Params{},
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
		extender := &fakeExtender{}
		projector := &fakeProjector{
			returnArgs: []search.Result{
				{
					ID: "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					AdditionalProperties: &models.AdditionalProperties{
						FeatureProjection: &models.FeatureProjection{
							Vector: []float32{0, 1},
						},
					},
				},
				{
					ID: "id2",
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					AdditionalProperties: &models.AdditionalProperties{
						FeatureProjection: &models.FeatureProjection{
							Vector: []float32{1, 0},
						},
					},
				},
			},
		}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(searcher, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		searcher.
			On("ClassSearch", expectedParamsToSearch).
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
						"featureProjection": &models.FeatureProjection{
							Vector: []float32{0, 1},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"name": "Bar",
					"_additional": map[string]interface{}{
						"featureProjection": &models.FeatureProjection{
							Vector: []float32{1, 0},
						},
					},
				}, res[1])
		})
	})

	t.Run("when the _additional on ref prop is set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			Properties: []SelectProperty{
				{
					Name: "ofBestRefClass",
					Refs: []SelectClass{
						{
							ClassName: "BestRefClass",
							AdditionalProperties: AdditionalProperties{
								ID: true,
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
							},
						},
					},
				},
				AdditionalProperties: &models.AdditionalProperties{
					Classification: nil,
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
				AdditionalProperties: &models.AdditionalProperties{
					Classification: &models.AdditionalPropertiesClassification{
						ID: "1234",
					},
				},
			},
		}

		fakeSearch := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(fakeSearch, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		fakeSearch.
			On("ClassSearch", expectedParamsToSearch).
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
							},
						},
					},
					"_additional": map[string]interface{}{
						"classification": &models.AdditionalPropertiesClassification{
							ID: "1234",
						},
					},
				}, res[1])
		})
	})

	t.Run("when the _additional on all refs prop is set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			Properties: []SelectProperty{
				{
					Name: "ofBestRefClass",
					Refs: []SelectClass{
						{
							ClassName: "BestRefClass",
							AdditionalProperties: AdditionalProperties{
								ID: true,
							},
							RefProperties: SelectProperties{
								SelectProperty{
									Name: "ofBestRefInnerClass",
									Refs: []SelectClass{
										{
											ClassName: "BestRefInnerClass",
											AdditionalProperties: AdditionalProperties{
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
				AdditionalProperties: &models.AdditionalProperties{
					Classification: nil,
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
				AdditionalProperties: &models.AdditionalProperties{
					Classification: &models.AdditionalPropertiesClassification{
						ID: "1234",
					},
				},
			},
		}

		fakeSearch := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(fakeSearch, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		fakeSearch.
			On("ClassSearch", expectedParamsToSearch).
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
						"classification": &models.AdditionalPropertiesClassification{
							ID: "1234",
						},
					},
				}, res[1])
		})
	})

	t.Run("when the _additional on lots of refs prop is set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			Properties: []SelectProperty{
				{
					Name: "ofBestRefClass",
					Refs: []SelectClass{
						{
							ClassName: "BestRefClass",
							AdditionalProperties: AdditionalProperties{
								ID: true,
							},
							RefProperties: SelectProperties{
								SelectProperty{
									Name: "ofBestRefInnerClass",
									Refs: []SelectClass{
										{
											ClassName: "BestRefInnerClass",
											AdditionalProperties: AdditionalProperties{
												ID: true,
											},
											RefProperties: SelectProperties{
												SelectProperty{
													Name: "ofBestRefInnerInnerClass",
													Refs: []SelectClass{
														{
															ClassName: "BestRefInnerInnerClass",
															AdditionalProperties: AdditionalProperties{
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
											"ofBestRefInnerInnerClass": []interface{}{
												search.LocalRef{
													Class: "BestRefInnerInnerClass",
													Fields: map[string]interface{}{
														"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4aaa",
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
				AdditionalProperties: &models.AdditionalProperties{
					Classification: nil,
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
											"ofBestRefInnerInnerClass": []interface{}{
												search.LocalRef{
													Class: "BestRefInnerInnerClass",
													Fields: map[string]interface{}{
														"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4bbb",
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
				AdditionalProperties: &models.AdditionalProperties{
					Classification: &models.AdditionalPropertiesClassification{
						ID: "1234",
					},
				},
			},
		}

		fakeSearch := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(fakeSearch, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		fakeSearch.
			On("ClassSearch", expectedParamsToSearch).
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
											"ofBestRefInnerInnerClass": []interface{}{
												search.LocalRef{
													Class: "BestRefInnerInnerClass",
													Fields: map[string]interface{}{
														"_additional": map[string]interface{}{
															"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4aaa",
														},
														"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4aaa",
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
											"ofBestRefInnerInnerClass": []interface{}{
												search.LocalRef{
													Class: "BestRefInnerInnerClass",
													Fields: map[string]interface{}{
														"_additional": map[string]interface{}{
															"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4bbb",
														},
														"id": "2d68456c-73b4-4cfc-a6dc-718efc5b4bbb",
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
						"classification": &models.AdditionalPropertiesClassification{
							ID: "1234",
						},
					},
				}, res[1])
		})
	})

	t.Run("when the almost all _additional props set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				ID:               true,
				NearestNeighbors: true,
				Classification:   true,
				Interpretation:   true,
			},
		}

		searchResults := []search.Result{
			{
				ID: "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				AdditionalProperties: &models.AdditionalProperties{
					Classification: &models.AdditionalPropertiesClassification{
						ID: "1234",
					},
					NearestNeighbors: &models.NearestNeighbors{
						Neighbors: []*models.NearestNeighbor{
							&models.NearestNeighbor{
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
				AdditionalProperties: &models.AdditionalProperties{
					Classification: &models.AdditionalPropertiesClassification{
						ID: "5678",
					},
					NearestNeighbors: &models.NearestNeighbors{
						Neighbors: []*models.NearestNeighbor{
							&models.NearestNeighbor{
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
					AdditionalProperties: &models.AdditionalProperties{
						Classification: &models.AdditionalPropertiesClassification{
							ID: "1234",
						},
						NearestNeighbors: &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "foo",
									Distance: 0.1,
								},
							},
						},
						Interpretation: &models.Interpretation{
							Source: []*models.InterpretationSource{
								&models.InterpretationSource{
									Concept:    "foo",
									Weight:     0.123,
									Occurrence: 123,
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
					AdditionalProperties: &models.AdditionalProperties{
						Classification: &models.AdditionalPropertiesClassification{
							ID: "5678",
						},
						NearestNeighbors: &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "bar",
									Distance: 0.1,
								},
							},
						},
						Interpretation: &models.Interpretation{
							Source: []*models.InterpretationSource{
								&models.InterpretationSource{
									Concept:    "bar",
									Weight:     0.456,
									Occurrence: 456,
								},
							},
						},
					},
				},
			},
		}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(searcher, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = nil
		searcher.
			On("ClassSearch", expectedParamsToSearch).
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
						"classification": &models.AdditionalPropertiesClassification{
							ID: "1234",
						},
						"nearestNeighbors": &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "foo",
									Distance: 0.1,
								},
							},
						},
						"interpretation": &models.Interpretation{
							Source: []*models.InterpretationSource{
								&models.InterpretationSource{
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
						"classification": &models.AdditionalPropertiesClassification{
							ID: "5678",
						},
						"nearestNeighbors": &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								&models.NearestNeighbor{
									Concept:  "bar",
									Distance: 0.1,
								},
							},
						},
						"interpretation": &models.Interpretation{
							Source: []*models.InterpretationSource{
								&models.InterpretationSource{
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
	t.Run("when an explore param is set for nearText", func(t *testing.T) {
		params := GetParams{
			ClassName: "BestClass",
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
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
			},
			{
				ID: "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
		search.
			On("VectorClassSearch", expectedParamsToSearch).
			Return(searchResults, nil)

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

	t.Run("when an explore param is set for nearText and the required certainty not met",
		func(t *testing.T) {
			params := GetParams{
				ClassName: "BestClass",
				ModuleParams: map[string]interface{}{
					"nearText": extractNearTextParam(map[string]interface{}{
						"concepts":  []interface{}{"foo"},
						"certainty": float64(0.8),
					}),
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			searchResults := []search.Result{
				{
					ID: "id1",
				},
				{
					ID: "id2",
				},
			}

			search := &fakeVectorSearcher{}
			extender := &fakeExtender{}
			log, _ := test.NewNullLogger()

			projector := &fakeProjector{}
			pathBuilder := &fakePathBuilder{}
			explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
			expectedParamsToSearch := params
			expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
			search.
				On("VectorClassSearch", expectedParamsToSearch).
				Return(searchResults, nil)

			res, err := explorer.GetClass(context.Background(), params)

			t.Run("vector search must be called with right params", func(t *testing.T) {
				assert.Nil(t, err)
				search.AssertExpectations(t)
			})

			t.Run("no concept met the required certainty", func(t *testing.T) {
				assert.Len(t, res, 0)
			})
		})

	t.Run("when two conflicting (nearVector, nearText) near searchers are set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearVector: &NearVectorParams{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when two conflicting (nearText, nearObject) near searchers are set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearObject: &NearObjectParams{
				Beacon: "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e041",
			},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when three conflicting (nearText, nearVector, nearObject) near searchers are set", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearVector: &NearVectorParams{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			NearObject: &NearObjectParams{
				Beacon: "weaviate://localhost/e9c12c22-766f-4bde-b140-d4cf8fd6e041",
			},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when nearText.moveTo has no concepts and objects defined", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
					"moveTo": map[string]interface{}{
						"force": float64(0.1),
					},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "needs to have defined either 'concepts' or 'objects' fields")
	})

	t.Run("when nearText.moveAwayFrom has no concepts and objects defined", func(t *testing.T) {
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"foo"},
					"moveAwayFrom": map[string]interface{}{
						"force": float64(0.1),
					},
				}),
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "needs to have defined either 'concepts' or 'objects' fields")
	})

	t.Run("when the certainty prop is set", func(t *testing.T) {
		params := GetParams{
			Filters:      nil,
			ClassName:    "BestClass",
			Pagination:   &filters.Pagination{Limit: 100},
			SearchVector: []float32{1.0, 2.0, 3.0},
			AdditionalProperties: AdditionalProperties{
				Certainty: true,
			},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts":  []interface{}{"foobar"},
					"limit":     100,
					"certainty": float64(0.0),
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
			},
		}

		search := &fakeVectorSearcher{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, newFakeDistancer69(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1.0, 2.0, 3.0}
		// expectedParamsToSearch.SearchVector = nil
		search.
			On("VectorClassSearch", expectedParamsToSearch).
			Return(searchResults, nil)

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
		params := GetParams{
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				SemanticPath: &sempath.Params{},
			},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
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
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{
			returnArgs: []search.Result{
				{
					ID: "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					AdditionalProperties: &models.AdditionalProperties{
						SemanticPath: &models.SemanticPath{
							Path: []*models.SemanticPathElement{
								&models.SemanticPathElement{
									Concept:            "pathelem1",
									DistanceToQuery:    0,
									DistanceToResult:   2.1,
									DistanceToPrevious: nil,
									DistanceToNext:     ptFloat32(0.5),
								},
								&models.SemanticPathElement{
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
					ID: "id2",
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					AdditionalProperties: &models.AdditionalProperties{
						SemanticPath: &models.SemanticPath{
							Path: []*models.SemanticPathElement{
								&models.SemanticPathElement{
									Concept:            "pathelem1",
									DistanceToQuery:    0,
									DistanceToResult:   2.1,
									DistanceToPrevious: nil,
									DistanceToNext:     ptFloat32(0.5),
								},
								&models.SemanticPathElement{
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
		explorer := NewExplorer(searcher, newFakeDistancer(), log, extender, projector, pathBuilder, getFakeModulesProvider())
		expectedParamsToSearch := params
		expectedParamsToSearch.SearchVector = []float32{1, 2, 3}
		searcher.
			On("VectorClassSearch", expectedParamsToSearch).
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
						"semanticPath": &models.SemanticPath{
							Path: []*models.SemanticPathElement{
								&models.SemanticPathElement{
									Concept:            "pathelem1",
									DistanceToQuery:    0,
									DistanceToResult:   2.1,
									DistanceToPrevious: nil,
									DistanceToNext:     ptFloat32(0.5),
								},
								&models.SemanticPathElement{
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
						"semanticPath": &models.SemanticPath{
							Path: []*models.SemanticPathElement{
								&models.SemanticPathElement{
									Concept:            "pathelem1",
									DistanceToQuery:    0,
									DistanceToResult:   2.1,
									DistanceToPrevious: nil,
									DistanceToNext:     ptFloat32(0.5),
								},
								&models.SemanticPathElement{
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

func newFakeDistancer() func(a, b []float32) (float32, error) {
	return func(source, target []float32) (float32, error) {
		return 0.5, nil
	}
}

// newFakeDistancer69 return 0.69 to allow the assertion of certainty vs distance.
func newFakeDistancer69() func(a, b []float32) (float32, error) {
	return func(source, target []float32) (float32, error) {
		return 0.69, nil
	}
}

func ptFloat32(in float32) *float32 {
	return &in
}

type fakeModulesProvider struct{}

func (p *fakeModulesProvider) VectorFromSearchParam(ctx context.Context, param string, params interface{},
	findVectorFn modulecapabilities.FindVectorFn) ([]float32, error) {
	txt2vec := &fakeText2vecContextionaryModule{}
	vectorForParams := txt2vec.VectorSearches()["nearText"]
	return vectorForParams(ctx, params, findVectorFn)
}

func (p *fakeModulesProvider) ValidateSearchParam(name string, value interface{}) error {
	txt2vec := &fakeText2vecContextionaryModule{}
	validateFn := txt2vec.ValidateFunctions()["nearText"]
	return validateFn(value)
}

func extractNearTextParam(param map[string]interface{}) interface{} {
	txt2vec := &fakeText2vecContextionaryModule{}
	return txt2vec.ExtractFunctions()["nearText"](param)
}

func getFakeModulesProvider() ModulesProvider {
	return &fakeModulesProvider{}
}
