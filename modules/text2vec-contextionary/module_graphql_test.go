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

package modcontextionary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/models"
	helper "github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/traverser"
)

type testCase struct {
	name                      string
	query                     string
	expectedParamsToTraverser traverser.ExploreParams
	resolverReturn            []search.Result
	expectedResults           []result
}

type testCases []testCase

type result struct {
	pathToField   []string
	expectedValue interface{}
}

func TestExtractAdditionalFields(t *testing.T) {
	// We don't need to explicitly test every subselection as we did on
	// phoneNumber as these fields have fixed keys. So we can simply check for
	// the prop

	type test struct {
		name           string
		query          string
		expectedParams dto.GetParams
		resolverReturn interface{}
		expectedResult interface{}
	}

	tests := []test{
		{
			name:  "with _additional certainty",
			query: "{ Get { SomeAction { _additional { certainty distance } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					Certainty: true,
					Distance:  true,
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"certainty": 0.69,
						"distance":  helper.CertaintyToDist(t, 0.69),
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"certainty": 0.69,
					"distance":  helper.CertaintyToDist(t, 0.69),
				},
			},
		},
		{
			name:  "with _additional interpretation",
			query: "{ Get { SomeAction { _additional { interpretation { source { concept weight occurrence } }  } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"interpretation": true,
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"interpretation": &models.Interpretation{
							Source: []*models.InterpretationSource{
								{
									Concept:    "foo",
									Weight:     0.6,
									Occurrence: 1200,
								},
								{
									Concept:    "bar",
									Weight:     0.9,
									Occurrence: 800,
								},
							},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
						"source": []interface{}{
							map[string]interface{}{
								"concept":    "foo",
								"weight":     0.6,
								"occurrence": 1200,
							},
							map[string]interface{}{
								"concept":    "bar",
								"weight":     0.9,
								"occurrence": 800,
							},
						},
					},
				},
			},
		},
		{
			name:  "with _additional nearestNeighbors",
			query: "{ Get { SomeAction { _additional { nearestNeighbors { neighbors { concept distance } }  } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"nearestNeighbors": true,
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"nearestNeighbors": &models.NearestNeighbors{
							Neighbors: []*models.NearestNeighbor{
								{
									Concept:  "foo",
									Distance: 0.1,
								},
								{
									Concept:  "bar",
									Distance: 0.2,
								},
							},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"nearestNeighbors": map[string]interface{}{
						"neighbors": []interface{}{
							map[string]interface{}{
								"concept":  "foo",
								"distance": float32(0.1),
							},
							map[string]interface{}{
								"concept":  "bar",
								"distance": float32(0.2),
							},
						},
					},
				},
			},
		},
		{
			name:  "with _additional featureProjection without any optional parameters",
			query: "{ Get { SomeAction { _additional { featureProjection { vector }  } } } }",
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"featureProjection": extractAdditionalParam("featureProjection", nil),
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"featureProjection": &models.FeatureProjection{
							Vector: []float32{0.0, 1.1, 2.2},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"featureProjection": map[string]interface{}{
						"vector": []interface{}{float32(0.0), float32(1.1), float32(2.2)},
					},
				},
			},
		},
		{
			name:  "with _additional featureProjection with optional parameters",
			query: `{ Get { SomeAction { _additional { featureProjection(algorithm: "tsne", dimensions: 3, learningRate: 15, iterations: 100, perplexity: 10) { vector }  } } } }`,
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"featureProjection": extractAdditionalParam("featureProjection",
							[]*ast.Argument{
								createArg("algorithm", "tsne"),
								createArg("dimensions", "3"),
								createArg("iterations", "100"),
								createArg("learningRate", "15"),
								createArg("perplexity", "10"),
							},
						),
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"featureProjection": &models.FeatureProjection{
							Vector: []float32{0.0, 1.1, 2.2},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"featureProjection": map[string]interface{}{
						"vector": []interface{}{float32(0.0), float32(1.1), float32(2.2)},
					},
				},
			},
		},
		{
			name:  "with _additional semanticPath set",
			query: `{ Get { SomeAction { _additional { semanticPath { path { concept distanceToQuery distanceToResult distanceToPrevious distanceToNext } } } } } }`,
			expectedParams: dto.GetParams{
				ClassName: "SomeAction",
				AdditionalProperties: additional.Properties{
					ModuleParams: map[string]interface{}{
						"semanticPath": extractAdditionalParam("semanticPath", nil),
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"_additional": map[string]interface{}{
						"semanticPath": &models.SemanticPath{
							Path: []*models.SemanticPathElement{
								{
									Concept:            "foo",
									DistanceToNext:     ptFloat32(0.5),
									DistanceToPrevious: nil,
									DistanceToQuery:    0.1,
									DistanceToResult:   0.1,
								},
								{
									Concept:            "bar",
									DistanceToPrevious: ptFloat32(0.5),
									DistanceToNext:     nil,
									DistanceToQuery:    0.1,
									DistanceToResult:   0.1,
								},
							},
						},
					},
				},
			},
			expectedResult: map[string]interface{}{
				"_additional": map[string]interface{}{
					"semanticPath": map[string]interface{}{
						"path": []interface{}{
							map[string]interface{}{
								"concept":            "foo",
								"distanceToNext":     float32(0.5),
								"distanceToPrevious": nil,
								"distanceToQuery":    float32(0.1),
								"distanceToResult":   float32(0.1),
							},
							map[string]interface{}{
								"concept":            "bar",
								"distanceToPrevious": float32(0.5),
								"distanceToNext":     nil,
								"distanceToQuery":    float32(0.1),
								"distanceToResult":   float32(0.1),
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := newMockResolver()

			resolver.On("GetClass", test.expectedParams).
				Return(test.resolverReturn, nil).Once()
			result := resolver.AssertResolve(t, test.query)
			assert.Equal(t, test.expectedResult, result.Get("Get", "SomeAction").Result.([]interface{})[0])
		})
	}
}

func TestNearTextRanker(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	t.Run("for actions", func(t *testing.T) {
		query := `{ Get { SomeAction(nearText: {
                concepts: ["c1", "c2", "c3"],
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeAction",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}

		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for a class that does not have a text2vec module", func(t *testing.T) {
		query := `{ Get { CustomVectorClass(nearText: {
                concepts: ["c1", "c2", "c3"],
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
							}) { intField } } }`

		res := resolver.Resolve(query)
		require.Len(t, res.Errors, 1)
		assert.Contains(t, res.Errors[0].Message, "Unknown argument \"nearText\" on field \"CustomVectorClass\"")
	})

	t.Run("for things with optional distance set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearText: {
                concepts: ["c1", "c2", "c3"],
								distance: 0.6,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.6),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearText: {
                concepts: ["c1", "c2", "c3"],
								certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional distance and objects set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearText: {
								concepts: ["c1", "c2", "c3"],
								distance: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
									objects: [
										{ id: "moveTo-uuid1" }
										{ beacon: "weaviate://localhost/moveTo-uuid3" }
									]
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
									objects: [
										{ id: "moveAway-uuid1" }
										{ beacon: "weaviate://localhost/moveAway-uuid2" }
										{ beacon: "weaviate://localhost/moveAway-uuid3" }
									]
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts": []interface{}{"c1", "c2", "c3"},
					"distance": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveTo-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid3",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveAway-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAway-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAway-uuid3",
							},
						},
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})

	t.Run("for things with optional certainty and objects set", func(t *testing.T) {
		query := `{ Get { SomeThing(nearText: {
								concepts: ["c1", "c2", "c3"],
								certainty: 0.4,
								moveTo: {
									concepts:["positive"],
									force: 0.5
									objects: [
										{ id: "moveTo-uuid1" }
										{ beacon: "weaviate://localhost/moveTo-uuid3" }
									]
								},
								moveAwayFrom: {
									concepts:["epic"],
									force: 0.25
									objects: [
										{ id: "moveAway-uuid1" }
										{ beacon: "weaviate://localhost/moveAway-uuid2" }
										{ beacon: "weaviate://localhost/moveAway-uuid3" }
									]
								}
							}) { intField } } }`

		expectedParams := dto.GetParams{
			ClassName:  "SomeThing",
			Properties: []search.SelectProperty{{Name: "intField", IsPrimitive: true}},
			Pagination: &filters.Pagination{Limit: filters.LimitFlagSearchByDist},
			ModuleParams: map[string]interface{}{
				"nearText": extractNearTextParam(map[string]interface{}{
					"concepts":  []interface{}{"c1", "c2", "c3"},
					"certainty": float64(0.4),
					"moveTo": map[string]interface{}{
						"concepts": []interface{}{"positive"},
						"force":    float64(0.5),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveTo-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveTo-uuid3",
							},
						},
					},
					"moveAwayFrom": map[string]interface{}{
						"concepts": []interface{}{"epic"},
						"force":    float64(0.25),
						"objects": []interface{}{
							map[string]interface{}{
								"id": "moveAway-uuid1",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAway-uuid2",
							},
							map[string]interface{}{
								"beacon": "weaviate://localhost/moveAway-uuid3",
							},
						},
					},
				}),
			},
		}
		resolver.On("GetClass", expectedParams).
			Return([]interface{}{}, nil).Once()

		resolver.AssertResolve(t, query)
	})
}

func Test_ResolveExplore(t *testing.T) {
	t.Parallel()

	testsNearText := testCases{
		testCase{
			name: "Resolve Explore with nearText",
			query: `
			{
					Explore(nearText: {concepts: ["car", "best brand"]}) {
							beacon className certainty distance
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				ModuleParams: map[string]interface{}{
					"nearText": extractNearTextParam(map[string]interface{}{
						"concepts": []interface{}{"car", "best brand"},
					}),
				},
				WithCertaintyProp: true,
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
					Certainty: 0.7,
					Dist:      helper.CertaintyToDist(t, 0.7),
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
						"certainty": float32(0.7),
						"distance":  helper.CertaintyToDist(t, 0.7),
					},
				},
			}},
		},

		testCase{
			name: "with nearText with optional limit and distance set",
			query: `
			{
					Explore(
						nearText: {concepts: ["car", "best brand"], distance: 0.6}, limit: 17
						){
							beacon className
				}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				ModuleParams: map[string]interface{}{
					"nearText": extractNearTextParam(map[string]interface{}{
						"concepts": []interface{}{"car", "best brand"},
						"distance": float64(0.6),
					}),
				},
				Limit: 17,
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
					Dist:      0.6,
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
					},
				},
			}},
		},

		testCase{
			name: "with nearText with optional limit and certainty set",
			query: `
			{
					Explore(
						nearText: {concepts: ["car", "best brand"], certainty: 0.6}, limit: 17
						){
							beacon className
				}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				ModuleParams: map[string]interface{}{
					"nearText": extractNearTextParam(map[string]interface{}{
						"concepts":  []interface{}{"car", "best brand"},
						"certainty": float64(0.6),
					}),
				},
				Limit: 17,
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
					},
				},
			}},
		},

		testCase{
			name: "with moveTo set",
			query: `
			{
					Explore(
							limit: 17
							nearText: {
								concepts: ["car", "best brand"]
								moveTo: {
									concepts: ["mercedes"]
									force: 0.7
								}
							}
							) {
							beacon className
						}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Limit: 17,
				ModuleParams: map[string]interface{}{
					"nearText": extractNearTextParam(map[string]interface{}{
						"concepts": []interface{}{"car", "best brand"},
						"moveTo": map[string]interface{}{
							"concepts": []interface{}{"mercedes"},
							"force":    float64(0.7),
						},
					}),
				},
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
					},
				},
			}},
		},

		testCase{
			name: "with moveTo and moveAwayFrom set",
			query: `
			{
					Explore(
							limit: 17
							nearText: {
								concepts: ["car", "best brand"]
								moveTo: {
									concepts: ["mercedes"]
									force: 0.7
								}
								moveAwayFrom: {
									concepts: ["van"]
									force: 0.7
								}
							}
							) {
							beacon className
						}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Limit: 17,
				ModuleParams: map[string]interface{}{
					"nearText": extractNearTextParam(map[string]interface{}{
						"concepts": []interface{}{"car", "best brand"},
						"moveTo": map[string]interface{}{
							"concepts": []interface{}{"mercedes"},
							"force":    float64(0.7),
						},
						"moveAwayFrom": map[string]interface{}{
							"concepts": []interface{}{"van"},
							"force":    float64(0.7),
						},
					}),
				},
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
					},
				},
			}},
		},

		testCase{
			name: "with moveTo and objects set",
			query: `
			{
					Explore(
							limit: 17
							nearText: {
								concepts: ["car", "best brand"]
								moveTo: {
									concepts: ["mercedes"]
									force: 0.7
									objects: [
										{id: "moveto-uuid"},
										{beacon: "weaviate://localhost/other-moveto-uuid"},
									]
								}
							}
							) {
							beacon className
						}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Limit: 17,
				ModuleParams: map[string]interface{}{
					"nearText": extractNearTextParam(map[string]interface{}{
						"concepts": []interface{}{"car", "best brand"},
						"moveTo": map[string]interface{}{
							"concepts": []interface{}{"mercedes"},
							"force":    float64(0.7),
							"objects": []interface{}{
								map[string]interface{}{
									"id": "moveto-uuid",
								},
								map[string]interface{}{
									"beacon": "weaviate://localhost/other-moveto-uuid",
								},
							},
						},
					}),
				},
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
					},
				},
			}},
		},

		testCase{
			name: "with moveTo and moveAwayFrom and objects set",
			query: `
			{
					Explore(
							limit: 17
							nearText: {
								concepts: ["car", "best brand"]
								moveTo: {
									concepts: ["mercedes"]
									force: 0.7
									objects: [
										{id: "moveto-uuid1"},
										{beacon: "weaviate://localhost/moveto-uuid2"},
									]
								}
								moveAwayFrom: {
									concepts: ["van"]
									force: 0.7
									objects: [
										{id: "moveAway-uuid1"},
										{beacon: "weaviate://localhost/moveAway-uuid2"},
										{id: "moveAway-uuid3"},
										{id: "moveAway-uuid4"},
									]
								}
							}
							) {
							beacon className
						}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Limit: 17,
				ModuleParams: map[string]interface{}{
					"nearText": extractNearTextParam(map[string]interface{}{
						"concepts": []interface{}{"car", "best brand"},
						"moveTo": map[string]interface{}{
							"concepts": []interface{}{"mercedes"},
							"force":    float64(0.7),
							"objects": []interface{}{
								map[string]interface{}{
									"id": "moveto-uuid1",
								},
								map[string]interface{}{
									"beacon": "weaviate://localhost/moveto-uuid2",
								},
							},
						},
						"moveAwayFrom": map[string]interface{}{
							"concepts": []interface{}{"van"},
							"force":    float64(0.7),
							"objects": []interface{}{
								map[string]interface{}{
									"id": "moveAway-uuid1",
								},
								map[string]interface{}{
									"beacon": "weaviate://localhost/moveAway-uuid2",
								},
								map[string]interface{}{
									"id": "moveAway-uuid3",
								},
								map[string]interface{}{
									"id": "moveAway-uuid4",
								},
							},
						},
					}),
				},
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
					},
				},
			}},
		},
	}

	testsNearText.AssertExtraction(t, newExploreMockResolver())
}

func (tests testCases) AssertExtraction(t *testing.T, resolver *mockResolver) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver.On("Explore", testCase.expectedParamsToTraverser).
				Return(testCase.resolverReturn, nil).Once()

			result := resolver.AssertResolve(t, testCase.query)

			for _, expectedResult := range testCase.expectedResults {
				value := result.Get(expectedResult.pathToField...).Result

				assert.Equal(t, expectedResult.expectedValue, value)
			}
		})
	}
}

func ptFloat32(in float32) *float32 {
	return &in
}
