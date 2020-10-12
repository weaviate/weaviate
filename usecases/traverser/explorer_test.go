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
	t.Run("when an explore param is set", func(t *testing.T) {
		params := GetParams{
			Kind:      kind.Thing,
			ClassName: "BestClass",
			Explore: &ExploreParams{
				Values: []string{"foo"},
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Action,
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

	t.Run("when an explore param is set and the required certainty not met", func(t *testing.T) {
		params := GetParams{
			Kind:      kind.Thing,
			ClassName: "BestClass",
			Explore: &ExploreParams{
				Values:    []string{"foo"},
				Certainty: 0.8,
			},
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
			},
			{
				Kind: kind.Action,
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

	t.Run("when no explore param is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Thing,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Action,
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

	t.Run("when the _classification prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Thing,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			UnderscoreProperties: UnderscoreProperties{
				Classification: true,
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				UnderscoreProperties: &models.UnderscoreProperties{
					Classification: nil,
				},
			},
			{
				Kind: kind.Action,
				ID:   "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				UnderscoreProperties: &models.UnderscoreProperties{
					Classification: &models.UnderscorePropertiesClassification{
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
					"_classification": &models.UnderscorePropertiesClassification{
						ID: "1234",
					},
				}, res[1])
		})
	})

	t.Run("when the _certainty prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:         kind.Thing,
			Filters:      nil,
			ClassName:    "BestClass",
			Pagination:   &filters.Pagination{Limit: 100},
			SearchVector: []float32{1.0, 2.0, 3.0},
			Explore: &ExploreParams{
				Values:    []string{"foobar"},
				Limit:     100,
				Certainty: 0,
			},
			UnderscoreProperties: UnderscoreProperties{
				Certainty: true,
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Action,
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
		//expectedParamsToSearch.SearchVector = nil
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
			assert.Contains(t, resMap, "_certainty")
			// Certainty is fixed to 0.69 in this mock
			assert.InEpsilon(t, 0.31, resMap["_certainty"], 0.000001)
		})
	})

	t.Run("when the _interpretation prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Thing,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			UnderscoreProperties: UnderscoreProperties{
				Interpretation: true,
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
				UnderscoreProperties: &models.UnderscoreProperties{
					Interpretation: nil,
				},
			},
			{
				Kind: kind.Action,
				ID:   "id2",
				Schema: map[string]interface{}{
					"age": 200,
				},
				UnderscoreProperties: &models.UnderscoreProperties{
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
					"_interpretation": &models.Interpretation{
						Source: []*models.InterpretationSource{
							&models.InterpretationSource{
								Concept:    "foo",
								Weight:     0.123,
								Occurrence: 123,
							},
						},
					},
				}, res[1])
		})
	})

	t.Run("when the _nearestNeighbors prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Thing,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			UnderscoreProperties: UnderscoreProperties{
				NearestNeighbors: true,
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Action,
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
					Kind: kind.Thing,
					ID:   "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					UnderscoreProperties: &models.UnderscoreProperties{
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
					Kind: kind.Action,
					ID:   "id2",
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					UnderscoreProperties: &models.UnderscoreProperties{
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
					"_nearestNeighbors": &models.NearestNeighbors{
						Neighbors: []*models.NearestNeighbor{
							&models.NearestNeighbor{
								Concept:  "foo",
								Distance: 0.1,
							},
						},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"name": "Bar",
					"_nearestNeighbors": &models.NearestNeighbors{
						Neighbors: []*models.NearestNeighbor{
							&models.NearestNeighbor{
								Concept:  "bar",
								Distance: 0.1,
							},
						},
					},
				}, res[1])
		})
	})

	t.Run("when the _featureProjection prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Thing,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			UnderscoreProperties: UnderscoreProperties{
				FeatureProjection: &libprojector.Params{},
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Action,
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
					Kind: kind.Thing,
					ID:   "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					UnderscoreProperties: &models.UnderscoreProperties{
						FeatureProjection: &models.FeatureProjection{
							Vector: []float32{0, 1},
						},
					},
				},
				{
					Kind: kind.Action,
					ID:   "id2",
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					UnderscoreProperties: &models.UnderscoreProperties{
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
					"_featureProjection": &models.FeatureProjection{
						Vector: []float32{0, 1},
					},
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"name": "Bar",
					"_featureProjection": &models.FeatureProjection{
						Vector: []float32{1, 0},
					},
				}, res[1])
		})
	})

	t.Run("when the _semanticPath prop is set", func(t *testing.T) {
		params := GetParams{
			Kind:       kind.Thing,
			ClassName:  "BestClass",
			Pagination: &filters.Pagination{Limit: 100},
			Filters:    nil,
			UnderscoreProperties: UnderscoreProperties{
				SemanticPath: &sempath.Params{},
			},
			Explore: &ExploreParams{
				Values: []string{"foobar"},
			},
		}

		searchResults := []search.Result{
			{
				Kind: kind.Thing,
				ID:   "id1",
				Schema: map[string]interface{}{
					"name": "Foo",
				},
			},
			{
				Kind: kind.Action,
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
					Kind: kind.Thing,
					ID:   "id1",
					Schema: map[string]interface{}{
						"name": "Foo",
					},
					UnderscoreProperties: &models.UnderscoreProperties{
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
					Kind: kind.Thing,
					ID:   "id2",
					Schema: map[string]interface{}{
						"name": "Bar",
					},
					UnderscoreProperties: &models.UnderscoreProperties{
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
					"_semanticPath": &models.SemanticPath{
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
				}, res[0])
			assert.Equal(t,
				map[string]interface{}{
					"name": "Bar",
					"_semanticPath": &models.SemanticPath{
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
