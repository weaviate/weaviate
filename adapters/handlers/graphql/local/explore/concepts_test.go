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

package explore

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
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

	tests := testCases{
		testCase{
			name: "Resolve Explore with nearText",
			query: `
			{
					Explore(nearText: {concepts: ["car", "best brand"]}) {
							beacon className certainty
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				NearText: &traverser.NearTextParams{
					Values: []string{"car", "best brand"},
				},
			},
			resolverReturn: []search.Result{
				search.Result{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
					Certainty: 0.7,
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
						"certainty": float32(0.7),
					},
				},
			}},
		},
		testCase{
			name: "Resolve Explore with nearVector",
			query: `
			{
					Explore(nearVector: {vector: [0, 1, 0.8]}) {
							beacon className certainty
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				NearVector: &traverser.NearVectorParams{
					Vector: []float32{0, 1, 0.8},
				},
			},
			resolverReturn: []search.Result{
				search.Result{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
					Certainty: 0.7,
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
						"certainty": float32(0.7),
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
				NearText: &traverser.NearTextParams{
					Values:    []string{"car", "best brand"},
					Certainty: 0.6,
				},
				Limit: 17,
			},
			resolverReturn: []search.Result{
				search.Result{
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
			name: "with nearVector with optional limit",
			query: `
			{
					Explore(limit: 17, nearVector: {vector: [0, 1, 0.8]}) {
							beacon className certainty
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				NearVector: &traverser.NearVectorParams{
					Vector: []float32{0, 1, 0.8},
				},
				Limit: 17,
			},
			resolverReturn: []search.Result{
				search.Result{
					Beacon:    "weaviate://localhost/some-uuid",
					ClassName: "bestClass",
					Certainty: 0.7,
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/some-uuid",
						"className": "bestClass",
						"certainty": float32(0.7),
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
				NearText: &traverser.NearTextParams{
					Values: []string{"car", "best brand"},
					MoveTo: traverser.ExploreMove{
						Values: []string{"mercedes"},
						Force:  0.7,
					},
				},
			},
			resolverReturn: []search.Result{
				search.Result{
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
				NearText: &traverser.NearTextParams{
					Values: []string{"car", "best brand"},
					MoveTo: traverser.ExploreMove{
						Values: []string{"mercedes"},
						Force:  0.7,
					},
					MoveAwayFrom: traverser.ExploreMove{
						Values: []string{"van"},
						Force:  0.7,
					},
				},
			},
			resolverReturn: []search.Result{
				search.Result{
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

	tests.AssertExtraction(t)
}

func (tests testCases) AssertExtraction(t *testing.T) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := newMockResolver()

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
