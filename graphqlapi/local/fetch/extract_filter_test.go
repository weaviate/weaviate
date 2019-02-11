/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package fetch

import (
	"fmt"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_filters"
	"github.com/stretchr/testify/mock"
)

type filterTestCase struct {
	name          string
	queryFragment string
	expectedMatch PropertyMatch
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
			expectedMatch: PropertyMatch{
				Value: &common_filters.Value{
					Value: "some-value",
					Type:  schema.DataTypeString,
				},
				Operator: common_filters.OperatorEqual,
			},
		},
		filterTestCase{
			name: "string notequal 'some-value'",
			queryFragment: `
				operator: NotEqual
				valueString: "some-value"
			`,
			expectedMatch: PropertyMatch{
				Value: &common_filters.Value{
					Value: "some-value",
					Type:  schema.DataTypeString,
				},
				Operator: common_filters.OperatorNotEqual,
			},
		},
		filterTestCase{
			name: "int equal 123",
			queryFragment: `
				operator: Equal
				valueInt: 123
			`,
			expectedMatch: PropertyMatch{
				Value: &common_filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: common_filters.OperatorEqual,
			},
		},
		filterTestCase{
			name: "int notequal 123",
			queryFragment: `
				operator: NotEqual
				valueInt: 123
			`,
			expectedMatch: PropertyMatch{
				Value: &common_filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: common_filters.OperatorNotEqual,
			},
		},
		filterTestCase{
			name: "int LessThan 123",
			queryFragment: `
				operator: LessThan
				valueInt: 123
			`,
			expectedMatch: PropertyMatch{
				Value: &common_filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: common_filters.OperatorLessThan,
			},
		},
		filterTestCase{
			name: "int LessThanEqual 123",
			queryFragment: `
				operator: LessThanEqual
				valueInt: 123
			`,
			expectedMatch: PropertyMatch{
				Value: &common_filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: common_filters.OperatorLessThanEqual,
			},
		},
		filterTestCase{
			name: "int GreaterThan 123",
			queryFragment: `
				operator: GreaterThan
				valueInt: 123
			`,
			expectedMatch: PropertyMatch{
				Value: &common_filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: common_filters.OperatorGreaterThan,
			},
		},
		filterTestCase{
			name: "int GreaterThanEqual 123",
			queryFragment: `
				operator: GreaterThanEqual
				valueInt: 123
			`,
			expectedMatch: PropertyMatch{
				Value: &common_filters.Value{
					Value: 123,
					Type:  schema.DataTypeInt,
				},
				Operator: common_filters.OperatorGreaterThanEqual,
			},
		},
	}

	tests.AssertExtraction(t)
}

func (tests filterTestCases) AssertExtraction(t *testing.T) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			c11y := newMockContextionary()
			c11y.On("SchemaSearch", mock.Anything).Twice()

			resolver := newMockResolver(c11y)

			expectedParamsToConnector := &Params{
				Kind: kind.THING_KIND,
				PossibleClassNames: contextionary.SearchResults{
					Type: contextionary.SearchTypeClass,
					Results: []contextionary.SearchResult{{
						Name:      "bestclass",
						Kind:      kind.THING_KIND,
						Certainty: 0.95,
					}, {
						Name:      "bestclassalternative",
						Kind:      kind.THING_KIND,
						Certainty: 0.85,
					}},
				},
				Properties: []Property{
					{
						PossibleNames: contextionary.SearchResults{
							Type: contextionary.SearchTypeProperty,
							Results: []contextionary.SearchResult{{
								Name:      "bestproperty",
								Kind:      kind.THING_KIND,
								Certainty: 0.95,
							}, {
								Name:      "bestpropertyalternative",
								Kind:      kind.THING_KIND,
								Certainty: 0.85,
							}},
						},
						Match: testCase.expectedMatch,
					},
				},
			}

			resolver.On("LocalFetchKindClass", expectedParamsToConnector).
				Return(nil, nil).Once()

			query := fmt.Sprintf(`
			{
				Fetch {
					Things(where: {
						class: {
							name: "bestclass"
							certainty: 0.8
						},
						properties: {
							name: "bestproperty"
							certainty: 0.8
							%s
						},
					}) {
						beacon certainty
					}
				}
			}`, testCase.queryFragment)
			resolver.AssertResolve(t, query)
		})
	}
}
