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
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	libprojector "github.com/semi-technologies/weaviate/usecases/projector"
	"github.com/semi-technologies/weaviate/usecases/sempath"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Explorer_GetClass(t *testing.T) {
	t.Run("when an explore param is set for nearText", func(t *testing.T) {
		// TODO: this is a module specific test case, which relies on the
		// text2vec-contextionary module
		params := GetParams{
			Kind:      kind.Object,
			ClassName: "BestClass",
			NearText: &NearTextParams{
				Values: []string{"foo"},
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Object,
				ID:   "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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

	t.Run("when an explore param is set for nearVector", func(t *testing.T) {
		// TODO: this is a module specific test case, which relies on the
		// text2vec-contextionary module
		params := GetParams{
			Kind:      kind.Object,
			ClassName: "BestClass",
			NearVector: &NearVectorParams{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Object,
				ID:   "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		// explicitly set the vectorizer to nil to make sure it is not used in this
		// case
		vectorizer := CorpiVectorizer(nil)
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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

	t.Run("when an explore param is set for nearText and the required certainty not met",
		func(t *testing.T) {
			params := GetParams{
				Kind:      kind.Object,
				ClassName: "BestClass",
				NearText: &NearTextParams{
					Values:    []string{"foo"},
					Certainty: 0.8,
				},
				Pagination: &filters.Pagination{Limit: 100},
				Filters:    nil,
			}

			searchResults := []search.Result{
				{
					Kind: kind.Object,
					ID:   "id1",
				},
				{
					Kind: kind.Object,
					ID:   "id2",
				},
			}

			search := &fakeVectorSearcher{}
			vectorizer := &fakeVectorizer{}
			extender := &fakeExtender{}
			log, _ := test.NewNullLogger()

			projector := &fakeProjector{}
			pathBuilder := &fakePathBuilder{}
			explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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

	t.Run("when an explore param is set for nearVector and the required certainty not met",
		func(t *testing.T) {
			params := GetParams{
				Kind:      kind.Object,
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
					Kind: kind.Object,
					ID:   "id1",
				},
				{
					Kind: kind.Object,
					ID:   "id2",
				},
			}

			search := &fakeVectorSearcher{}
			vectorizer := CorpiVectorizer(nil)
			extender := &fakeExtender{}
			log, _ := test.NewNullLogger()

			projector := &fakeProjector{}
			pathBuilder := &fakePathBuilder{}
			explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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

	t.Run("when two conflicting near searchers are set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Object,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			NearVector: &NearVectorParams{
				Vector: []float32{0.8, 0.2, 0.7},
			},
			NearText: &NearTextParams{
				Values: []string{"foo"},
			},
		}

		search := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
		_, err := explorer.GetClass(context.Background(), params)
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "parameters which are conflicting")
	})

	t.Run("when no explore param is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Object,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Object,
				ID:   "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
			},
		}

		search := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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
			Kind:       kind.Object,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				Classification: true,
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				AdditionalProperties: &models.AdditionalProperties{
					Classification: nil,
				},
			},
			{
				Kind: kind.Object,
				ID:   "id2",
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
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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

	t.Run("when the certainty prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:         kind.Object,
			Filters:      nil,
			ClassName:    "BestClass",
			Pagination:   &filters.Pagination{Limit: 100},
			SearchVector: []float32{1.0, 2.0, 3.0},
			NearText: &NearTextParams{
				Values:    []string{"foobar"},
				Limit:     100,
				Certainty: 0,
			},
			AdditionalProperties: AdditionalProperties{
				Certainty: true,
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				Vector: []float32{0.5, 1.5, 0.0},
			},
		}

		search := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, vectorizer, newFakeDistancer69(), log, extender, projector, pathBuilder)
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

	t.Run("when the interpretation prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Object,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				Interpretation: true,
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				AdditionalProperties: &models.AdditionalProperties{
					Interpretation: nil,
				},
			},
			{
				Kind: kind.Object,
				ID:   "id2",
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
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(search, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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
			Kind:       kind.Object,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				NearestNeighbors: true,
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Object,
				ID:   "id2",
				Schema: map[string]interface{}{
					"name": "Bar",
				},
			},
		}

		searcher := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{
			returnArgs: []search.Result{
				{
					Kind: kind.Object,
					ID:   "id1",
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
					Kind: kind.Object,
					ID:   "id2",
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
		explorer := NewExplorer(searcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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
			Kind:       kind.Object,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				FeatureProjection: &libprojector.Params{},
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Object,
				ID:   "id2",
				Schema: map[string]interface{}{
					"name": "Bar",
				},
			},
		}

		searcher := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{
			returnArgs: []search.Result{
				{
					Kind: kind.Object,
					ID:   "id1",
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
					Kind: kind.Object,
					ID:   "id2",
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
		explorer := NewExplorer(searcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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

	t.Run("when the semanticPath prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Object,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			AdditionalProperties: AdditionalProperties{
				SemanticPath: &sempath.Params{},
			},
			NearText: &NearTextParams{
				Values: []string{"foobar"},
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Object,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Object,
				ID:   "id2",
				Schema: map[string]interface{}{
					"name": "Bar",
				},
			},
		}

		searcher := &fakeVectorSearcher{}
		vectorizer := &fakeVectorizer{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{}
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{
			returnArgs: []search.Result{
				{
					Kind: kind.Object,
					ID:   "id1",
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
					Kind: kind.Object,
					ID:   "id2",
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
		explorer := NewExplorer(searcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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

	t.Run("when the _additional on ref prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Object,
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
				Kind: kind.Object,
				ID:   "id1",
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
				Kind: kind.Object,
				ID:   "id2",
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
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(fakeSearch, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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
			Kind:       kind.Object,
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
				Kind: kind.Object,
				ID:   "id1",
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
				Kind: kind.Object,
				ID:   "id2",
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
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(fakeSearch, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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
			Kind:       kind.Object,
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
				Kind: kind.Object,
				ID:   "id1",
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
				Kind: kind.Object,
				ID:   "id2",
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
		vectorizer := &fakeVectorizer{}
		extender := &fakeExtender{}
		log, _ := test.NewNullLogger()
		projector := &fakeProjector{}
		pathBuilder := &fakePathBuilder{}
		explorer := NewExplorer(fakeSearch, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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
			Kind:       kind.Object,
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
				Kind: kind.Object,
				ID:   "id1",
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
				Kind: kind.Object,
				ID:   "id2",
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
		vectorizer := &fakeVectorizer{}
		log, _ := test.NewNullLogger()
		extender := &fakeExtender{
			returnArgs: []search.Result{
				{
					Kind: kind.Object,
					ID:   "id1",
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
					Kind: kind.Object,
					ID:   "id2",
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
		explorer := NewExplorer(searcher, vectorizer, newFakeDistancer(), log, extender, projector, pathBuilder)
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
