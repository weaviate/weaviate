//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package fetch

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

// TODO: reenable

type filterTestCase struct {
	name          string
	queryFragment string
	expectedMatch traverser.FetchPropertyMatch
}

type filterTestCases []filterTestCase

func Test_Filter_ExtractOperatorsAndValues(t *testing.T) {
	t.Parallel()

	tests := filterTestCases{
		filterTestCase{
			name: "string equal 'some-value'",
			queryFragment: `
				operator: Equal
				valueString: "some-value"
			`,
			expectedMatch: traverser.FetchPropertyMatch{
				Value: &filters.Value{
					Value: "some-value",
					Type:  schema.DataTypeString,
				},
				Operator: filters.OperatorEqual,
			},
		},
		filterTestCase{
			name: "string notequal 'some-value'",
			queryFragment: `
				operator: NotEqual
				valueString: "some-value"
			`,
			expectedMatch: traverser.FetchPropertyMatch{
				Value: &filters.Value{
					Value: "some-value",
					Type:  schema.DataTypeString,
				},
				Operator: filters.OperatorNotEqual,
			},
		},
		filterTestCase{
			name: "int equal 123",
			queryFragment: `
				operator: Equal
				valueInt: 123
			`,
			expectedMatch: traverser.FetchPropertyMatch{
				Value: &filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: filters.OperatorEqual,
			},
		},
		filterTestCase{
			name: "int notequal 123",
			queryFragment: `
				operator: NotEqual
				valueInt: 123
			`,
			expectedMatch: traverser.FetchPropertyMatch{
				Value: &filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: filters.OperatorNotEqual,
			},
		},
		filterTestCase{
			name: "int LessThan 123",
			queryFragment: `
				operator: LessThan
				valueInt: 123
			`,
			expectedMatch: traverser.FetchPropertyMatch{
				Value: &filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: filters.OperatorLessThan,
			},
		},
		filterTestCase{
			name: "int LessThanEqual 123",
			queryFragment: `
				operator: LessThanEqual
				valueInt: 123
			`,
			expectedMatch: traverser.FetchPropertyMatch{
				Value: &filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: filters.OperatorLessThanEqual,
			},
		},
		filterTestCase{
			name: "int GreaterThan 123",
			queryFragment: `
				operator: GreaterThan
				valueInt: 123
			`,
			expectedMatch: traverser.FetchPropertyMatch{
				Value: &filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: filters.OperatorGreaterThan,
			},
		},
		filterTestCase{
			name: "int GreaterThanEqual 123",
			queryFragment: `
				operator: GreaterThanEqual
				valueInt: 123
			`,
			expectedMatch: traverser.FetchPropertyMatch{
				Value: &filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: filters.OperatorGreaterThanEqual,
			},
		},
	}
	_ = tests
	// TODO

	// tests.AssertExtraction(t)
}

// func (tests filterTestCases) AssertExtraction(t *testing.T) {
// 	for _, testCase := range tests {
// 		t.Run(testCase.name, func(t *testing.T) {

// 			resolver := newMockResolver()

// 			expectedParamsToConnector := &traverser.FetchSearch{
// 				Kind: kind.Thing,
// 				PossibleClassNames: contextionary.SearchResults{
// 					Type: contextionary.SearchTypeClass,
// 					Results: []contextionary.SearchResult{{
// 						Name:      "bestclass",
// 						Kind:      kind.Thing,
// 						Certainty: 0.95,
// 					}, {
// 						Name:      "bestclassalternative",
// 						Kind:      kind.Thing,
// 						Certainty: 0.85,
// 					}},
// 				},
// 				Properties: []Property{
// 					{
// 						PossibleNames: contextionary.SearchResults{
// 							Type: contextionary.SearchTypeProperty,
// 							Results: []contextionary.SearchResult{{
// 								Name:      "bestproperty",
// 								Kind:      kind.Thing,
// 								Certainty: 0.95,
// 							}, {
// 								Name:      "bestpropertyalternative",
// 								Kind:      kind.Thing,
// 								Certainty: 0.85,
// 							}},
// 						},
// 						Match: testCase.expectedMatch,
// 					},
// 				},
// 			}

// 			resolver.On("LocalFetchKindClass", expectedParamsToConnector).
// 				Return(nil, nil).Once()

// 			query := fmt.Sprintf(`
// 			{
// 				Fetch {
// 					Things(where: {
// 						class: {
// 							name: "bestclass"
// 							certainty: 0.8
// 						},
// 						properties: {
// 							name: "bestproperty"
// 							certainty: 0.8
// 							%s
// 						},
// 					}) {
// 						beacon certainty
// 					}
// 				}
// 			}`, testCase.queryFragment)
// 			resolver.AssertResolve(t, query)
// 		})
// 	}
// }
