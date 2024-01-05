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

package explore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
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

func Test_ResolveExplore(t *testing.T) {
	t.Parallel()

	testsNearText := testCases{
		testCase{
			name: "Resolve Explore with nearCustomText",
			query: `
			{
					Explore(nearCustomText: {concepts: ["car", "best brand"]}) {
							beacon className certainty distance
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				ModuleParams: map[string]interface{}{
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
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
			name: "with nearCustomText with optional limit and distance set",
			query: `
			{
					Explore(
						nearCustomText: {concepts: ["car", "best brand"], distance: 0.4}, limit: 17
						){
							beacon className
				}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				ModuleParams: map[string]interface{}{
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
						"concepts": []interface{}{"car", "best brand"},
						"distance": float64(0.4),
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
			name: "with nearCustomText with optional limit and certainty set",
			query: `
			{
					Explore(
						nearCustomText: {concepts: ["car", "best brand"], certainty: 0.6}, limit: 17
						){
							beacon className
				}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				ModuleParams: map[string]interface{}{
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
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
							nearCustomText: {
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
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
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
							nearCustomText: {
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
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
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
							nearCustomText: {
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
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
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
			name: "with moveTo and objects set",
			query: `
			{
					Explore(
							limit: 17
							nearCustomText: {
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
					"nearCustomText": extractNearCustomTextParam(map[string]interface{}{
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

	tests := testCases{
		testCase{
			name: "Resolve Explore with nearVector",
			query: `
			{
					Explore(nearVector: {vector: [0, 1, 0.8]}) {
							beacon className certainty distance
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				NearVector: &searchparams.NearVector{
					Vector: []float32{0, 1, 0.8},
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
			name: "with nearVector with optional limit",
			query: `
			{
					Explore(limit: 17, nearVector: {vector: [0, 1, 0.8]}) {
							beacon className certainty distance
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				NearVector: &searchparams.NearVector{
					Vector: []float32{0, 1, 0.8},
				},
				Limit:             17,
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
			name: "Resolve Explore with nearObject, distance, and beacon set",
			query: `
			{
				Explore(
					nearObject: {
						beacon: "weaviate://localhost/27b5213d-e152-4fea-bd63-2063d529024d"
						distance: 0.3
					}) {
						beacon className certainty distance
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				NearObject: &searchparams.NearObject{
					Beacon:       "weaviate://localhost/27b5213d-e152-4fea-bd63-2063d529024d",
					Distance:     float64(0.3),
					WithDistance: true,
				},
				WithCertaintyProp: true,
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/27b5213d-e152-4fea-bd63-2063d529024d",
					ClassName: "bestClass",
					Certainty: 0.7,
					Dist:      helper.CertaintyToDist(t, 0.7),
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/27b5213d-e152-4fea-bd63-2063d529024d",
						"className": "bestClass",
						"certainty": float32(0.7),
						"distance":  helper.CertaintyToDist(t, 0.7),
					},
				},
			}},
		},

		testCase{
			name: "Resolve Explore with nearObject, certainty, and beacon set",
			query: `
			{
				Explore(
					nearObject: {
						beacon: "weaviate://localhost/27b5213d-e152-4fea-bd63-2063d529024d"
						certainty: 0.7
					}) {
						beacon className certainty distance
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				NearObject: &searchparams.NearObject{
					Beacon:    "weaviate://localhost/27b5213d-e152-4fea-bd63-2063d529024d",
					Certainty: 0.7,
				},
				WithCertaintyProp: true,
			},
			resolverReturn: []search.Result{
				{
					Beacon:    "weaviate://localhost/27b5213d-e152-4fea-bd63-2063d529024d",
					ClassName: "bestClass",
					Certainty: 0.7,
					Dist:      helper.CertaintyToDist(t, 0.7),
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/27b5213d-e152-4fea-bd63-2063d529024d",
						"className": "bestClass",
						"certainty": float32(0.7),
						"distance":  helper.CertaintyToDist(t, 0.7),
					},
				},
			}},
		},

		testCase{
			name: "Resolve Explore with nearObject, distance and id set",
			query: `
			{
					Explore(
							limit: 17
							nearObject: {
								id: "27b5213d-e152-4fea-bd63-2063d529024d"
								distance: 0.3
							}
							) {
							beacon className
						}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Limit: 17,
				NearObject: &searchparams.NearObject{
					ID:           "27b5213d-e152-4fea-bd63-2063d529024d",
					Distance:     0.3,
					WithDistance: true,
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
			name: "Resolve Explore with nearObject, certainty and id set",
			query: `
			{
					Explore(
							limit: 17
							nearObject: {
								id: "27b5213d-e152-4fea-bd63-2063d529024d"
								certainty: 0.7
							}
							) {
							beacon className
						}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Limit: 17,
				NearObject: &searchparams.NearObject{
					ID:        "27b5213d-e152-4fea-bd63-2063d529024d",
					Certainty: 0.7,
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

	tests.AssertExtraction(t, newMockResolver())
	testsNearText.AssertExtraction(t, newMockResolver())
	tests.AssertExtraction(t, newMockResolverNoModules())
}

func Test_ExploreWithNoText2VecClasses(t *testing.T) {
	t.Run("with distance", func(t *testing.T) {
		resolver := newMockResolverEmptySchema()
		query := `
		{
				Explore(
					nearCustomText: {concepts: ["car", "best brand"], distance: 0.6}, limit: 17
					){
						beacon className
			}
		}`
		res := resolver.Resolve(query)
		require.Len(t, res.Errors, 1)
		assert.Contains(t, res.Errors[0].Message, "Unknown argument \"nearCustomText\" on field \"Explore\"")
	})

	t.Run("with certainty", func(t *testing.T) {
		resolver := newMockResolverEmptySchema()
		query := `
		{
				Explore(
					nearCustomText: {concepts: ["car", "best brand"], certainty: 0.6}, limit: 17
					){
						beacon className
			}
		}`
		res := resolver.Resolve(query)
		require.Len(t, res.Errors, 1)
		assert.Contains(t, res.Errors[0].Message, "Unknown argument \"nearCustomText\" on field \"Explore\"")
	})
}

func Test_ExploreWithNoModules(t *testing.T) {
	t.Run("with distance", func(t *testing.T) {
		resolver := newMockResolverNoModules()
		query := `
	{
			Explore(
				nearCustomText: {concepts: ["car", "best brand"], distance: 0.6}, limit: 17
				){
					beacon className
		}
	}`
		res := resolver.Resolve(query)
		require.Len(t, res.Errors, 1)
		assert.Contains(t, res.Errors[0].Message, "Unknown argument \"nearCustomText\" on field \"Explore\"")
	})

	t.Run("with certainty", func(t *testing.T) {
		resolver := newMockResolverNoModules()
		query := `
	{
			Explore(
				nearCustomText: {concepts: ["car", "best brand"], certainty: 0.6}, limit: 17
				){
					beacon className
		}
	}`
		res := resolver.Resolve(query)
		require.Len(t, res.Errors, 1)
		assert.Contains(t, res.Errors[0].Message, "Unknown argument \"nearCustomText\" on field \"Explore\"")
	})
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
