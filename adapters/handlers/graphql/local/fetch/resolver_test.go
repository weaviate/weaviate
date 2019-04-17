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
	"testing"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	contextionary "github.com/creativesoftwarefdn/weaviate/database/schema_contextionary"
	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name                                string
	query                               string
	expectedParamsToConnector           *Params
	expectedSearchParamsToContextionary []contextionary.SearchParams
	resolverReturn                      interface{}
	expectedResults                     []result
}

type testCases []testCase

type result struct {
	pathToField   []string
	expectedValue interface{}
}

func Test_ResolveKinds(t *testing.T) {
	t.Parallel()

	tests := testCases{
		testCase{
			name: "Resolve Local Fetch Kinds (entire unit)",
			query: `
			{
				Fetch {
					Things(where: {
						class: {
							name: "bestclass"
							certainty: 0.8
							keywords: [{value: "foo", weight: 0.9}]
						},
						properties: {
							name: "bestproperty"
							certainty: 0.8
							keywords: [{value: "bar", weight: 0.9}]
							operator: Equal
							valueString: "some-value"
						},
					}) {
						beacon certainty className
					}
				}
			}`,
			expectedSearchParamsToContextionary: []contextionary.SearchParams{
				{
					SearchType: contextionary.SearchTypeClass,
					Name:       "bestclass",
					Kind:       kind.THING_KIND,
					Certainty:  0.8,
					Keywords: models.SemanticSchemaKeywords{{
						Keyword: "foo",
						Weight:  0.9,
					}},
				},
				{
					SearchType: contextionary.SearchTypeProperty,
					Name:       "bestproperty",
					Kind:       kind.THING_KIND,
					Certainty:  0.8,
					Keywords: models.SemanticSchemaKeywords{{
						Keyword: "bar",
						Weight:  0.9,
					}},
				},
			},
			expectedParamsToConnector: &Params{
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
								Certainty: 0.95,
								Kind:      kind.THING_KIND,
							}, {
								Name:      "bestpropertyalternative",
								Certainty: 0.85,
								Kind:      kind.THING_KIND,
							}},
						},
						Match: PropertyMatch{
							Value: &common_filters.Value{
								Value: "some-value",
								Type:  schema.DataTypeString,
							},
							Operator: common_filters.OperatorEqual,
						},
					},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"beacon":    "weaviate://peerName/things/uuid1",
					"className": "Superclass",
					"certainty": 0.7,
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Fetch", "Things"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"beacon":    "weaviate://peerName/things/uuid1",
						"className": "Superclass",
						"certainty": 0.7,
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
			c11y := newMockContextionary()

			if len(testCase.expectedSearchParamsToContextionary) != 2 {
				t.Fatalf("test setup incorrect: expectedSearchParamsToContextionary must have len 2")
			}

			c11y.On("SchemaSearch", testCase.expectedSearchParamsToContextionary[0]).Once()
			c11y.On("SchemaSearch", testCase.expectedSearchParamsToContextionary[1]).Once()

			resolver := newMockResolver(c11y)

			resolver.On("LocalFetchKindClass", testCase.expectedParamsToConnector).
				Return(testCase.resolverReturn, nil).Once()

			result := resolver.AssertResolve(t, testCase.query)
			c11y.AssertExpectations(t)

			for _, expectedResult := range testCase.expectedResults {
				value := result.Get(expectedResult.pathToField...).Result

				assert.Equal(t, expectedResult.expectedValue, value)
			}
		})
	}
}

func Test__ResolveKinds_NoResultsFromContextionary(t *testing.T) {
	query := `
			{
				Fetch {
					Things(where: {
						class: {
							name: "bestclass"
							certainty: 0.8
							keywords: [{value: "foo", weight: 0.9}]
						},
						properties: {
							name: "bestproperty"
							certainty: 0.8
							keywords: [{value: "bar", weight: 0.9}]
							operator: Equal
							valueString: "some-value"
						},
					}) {
						beacon certainty
					}
				}
			}`
	c11y := newEmptyContextionary()
	c11y.On("SchemaSearch", mock.Anything).Twice()
	resolver := newMockResolver(c11y)
	res := resolver.Resolve(query)
	require.Len(t, res.Errors, 1)
	assert.Equal(t, res.Errors[0].Message,
		"the contextionary contains no close matches to the provided class name. "+
			"Try using different search terms or lowering the desired certainty")
}

func Test__ResolveFuzzy_HappyPath(t *testing.T) {
	query := `
			{
				Fetch {
					Fuzzy(value:"steak", certainty: 0.7) {
						beacon certainty className
					}
				}
			}`
	resolverReturn := []interface{}{
		map[string]interface{}{
			"beacon":    "weaviate://localhost/things/foobar",
			"className": "Superclass",
			"certainty": 0.7,
		},
	}
	expectedResult := []interface{}{
		map[string]interface{}{
			"beacon":    "weaviate://localhost/things/foobar",
			"className": "Superclass",
			"certainty": 0.7,
		},
	}

	c11y := newMockContextionary()
	c11y.On("SafeGetSimilarWordsWithCertainty", "steak", float32(0.7))
	resolver := newMockResolver(c11y)
	resolver.On("LocalFetchFuzzy", []string{"steak", "steakalt1", "steakalt2"}).
		Return(resolverReturn, nil)

	res := resolver.AssertResolve(t, query)
	c11y.AssertExpectations(t)
	result := res.Get("Fetch", "Fuzzy").Result
	assert.Equal(t, expectedResult, result)
}

func Test__Resolve_MissingOperator(t *testing.T) {
	query := `
			{
				Fetch {
					Things(where: {
						class: {
							name: "bestclass"
							certainty: 0.8
							keywords: [{value: "foo", weight: 0.9}]
						},
						properties: {
							name: "bestproperty"
							certainty: 0.8
							keywords: [{value: "bar", weight: 0.9}]
							valueString: "some-value"
						},
					}) {
						beacon certainty
					}
				}
			}`
	c11y := newEmptyContextionary()
	c11y.On("SchemaSearch", mock.Anything).Twice()
	resolver := newMockResolver(c11y)
	res := resolver.Resolve(query)
	require.Len(t, res.Errors, 1)
	assert.Equal(t,
		`Argument "where" has invalid value {class: {name: "bestclass", certainty: 0.8, keywords: `+
			`[{value: "foo", weight: 0.9}]}, properties: {name: "bestproperty", certainty: 0.8, keywords: `+
			`[{value: "bar", weight: 0.9}], valueString: "some-value"}}.`+"\n"+
			`In field "properties": In field "operator": Expected "WeaviateLocalFetchThingWhereOperatorEnum!", found null.`,
		res.Errors[0].Message)
}
