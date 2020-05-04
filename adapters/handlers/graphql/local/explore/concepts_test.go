//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
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
			name: "Resolve Explore ",
			query: `
			{ 
					Explore(concepts: ["car", "best brand"]) {
							beacon className certainty
					}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Values: []string{"car", "best brand"},
			},
			resolverReturn: []search.Result{
				search.Result{
					Beacon:    "weaviate://localhost/things/some-uuid",
					ClassName: "bestClass",
					Certainty: 0.7,
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/things/some-uuid",
						"className": "bestClass",
						"certainty": float32(0.7),
					},
				},
			}},
		},

		testCase{
			name: "with optional limit and certainty set",
			query: `
			{
					Explore(
					concepts: ["car", "best brand"], limit: 17, certainty: 0.6, network: true) {
							beacon className
				}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Values:    []string{"car", "best brand"},
				Limit:     17,
				Certainty: 0.6,
				Network:   true,
			},
			resolverReturn: []search.Result{
				search.Result{
					Beacon:    "weaviate://localhost/things/some-uuid",
					ClassName: "bestClass",
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/things/some-uuid",
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
							concepts: ["car", "best brand"]
							limit: 17
							moveTo: {
								concepts: ["mercedes"]
								force: 0.7
							}
							) {
							beacon className
						}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Values: []string{"car", "best brand"},
				Limit:  17,
				MoveTo: traverser.ExploreMove{
					Values: []string{"mercedes"},
					Force:  0.7,
				},
			},
			resolverReturn: []search.Result{
				search.Result{
					Beacon:    "weaviate://localhost/things/some-uuid",
					ClassName: "bestClass",
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/things/some-uuid",
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
							concepts: ["car", "best brand"]
							limit: 17
							moveTo: {
								concepts: ["mercedes"]
								force: 0.7
							}
							moveAwayFrom: {
								concepts: ["van"]
								force: 0.7
							}
							) {
							beacon className
						}
			}`,
			expectedParamsToTraverser: traverser.ExploreParams{
				Values: []string{"car", "best brand"},
				Limit:  17,
				MoveTo: traverser.ExploreMove{
					Values: []string{"mercedes"},
					Force:  0.7,
				},
				MoveAwayFrom: traverser.ExploreMove{
					Values: []string{"van"},
					Force:  0.7,
				},
			},
			resolverReturn: []search.Result{
				search.Result{
					Beacon:    "weaviate://localhost/things/some-uuid",
					ClassName: "bestClass",
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Explore"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://localhost/things/some-uuid",
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

// func Test__Resolve_MissingOperator(t *testing.T) {
// 	query := `
// 			{
// 				Fetch {
// 					Things(where: {
// 						class: {
// 							name: "bestclass"
// 							certainty: 0.8
// 							concepts: [{value: "foo", weight: 0.9}]
// 						},
// 						properties: {
// 							name: "bestproperty"
// 							certainty: 0.8
// 							concepts: [{value: "bar", weight: 0.9}]
// 							valueString: "some-value"
// 						},
// 					}) {
// 						beacon certainty
// 					}
// 				}
// 			}`
// 	c11y := newEmptyContextionary()
// 	c11y.On("SchemaSearch", mock.Anything).Twice()
// 	resolver := newMockResolver(c11y)
// 	res := resolver.Resolve(query)
// 	require.Len(t, res.Errors, 1)
// 	assert.Equal(t,
// 		`Argument "where" has invalid value {class: {name: "bestclass", certainty: 0.8, concepts: `+
// 			`[{value: "foo", weight: 0.9}]}, properties: {name: "bestproperty", certainty: 0.8, concepts: `+
// 			`[{value: "bar", weight: 0.9}], valueString: "some-value"}}.`+"\n"+
// 			`In field "properties": In field "operator": Expected "FetchThingWhereOperatorEnum!", found null.`,
// 		res.Errors[0].Message)
// }
