/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package getmeta

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/kinds"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name            string
	query           string
	expectedProps   []kinds.MetaProperty
	resolverReturn  interface{}
	expectedResults []result
}

type testCases []testCase

type result struct {
	pathToField   []string
	expectedValue interface{}
}

func Test_Resolve(t *testing.T) {
	t.Parallel()

	tests := testCases{
		testCase{
			name:  "single prop: mean",
			query: "{ GetMeta { Things { Car { horsepower { mean } } } } }",
			expectedProps: []kinds.MetaProperty{
				{
					Name:                "horsepower",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Mean},
				},
			},
			resolverReturn: map[string]interface{}{
				"horsepower": map[string]interface{}{
					"mean": 275.7773,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "mean"},
				expectedValue: 275.7773,
			}},
		},

		testCase{
			name:  "single prop: type",
			query: "{ GetMeta { Things { Car { horsepower { type } } } } }",
			expectedProps: []kinds.MetaProperty{
				{
					Name:                "horsepower",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Type},
				},
			},
			resolverReturn: map[string]interface{}{
				"horsepower": map[string]interface{}{
					"type": "int",
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "type"},
				expectedValue: "int",
			}},
		},

		testCase{
			name:  "two props: maximum, minimum, remaining int props",
			query: "{ GetMeta { Things { Car { horsepower { maximum, minimum, count, sum } } } } }",
			expectedProps: []kinds.MetaProperty{
				{
					Name: "horsepower",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Maximum, kinds.Minimum,
						kinds.Count, kinds.Sum},
				},
			},
			resolverReturn: map[string]interface{}{
				"horsepower": map[string]interface{}{
					"maximum": 610.0,
					"minimum": 89.0,
					"count":   23,
					"sum":     6343.0,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "maximum"},
				expectedValue: 610.0,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "minimum"},
				expectedValue: 89.0,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "count"},
				expectedValue: 23,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "horsepower", "sum"},
				expectedValue: 6343.0,
			}},
		},

		testCase{
			name: "all props on a bool field",
			query: `{ GetMeta { Things { Car { stillInProduction {
					count, totalTrue, totalFalse, percentageTrue, percentageFalse
				} } } } }`,
			expectedProps: []kinds.MetaProperty{
				{
					Name: "stillInProduction",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count, kinds.TotalTrue,
						kinds.TotalFalse, kinds.PercentageTrue, kinds.PercentageFalse},
				},
			},
			resolverReturn: map[string]interface{}{
				"stillInProduction": map[string]interface{}{
					"count":           7,
					"totalTrue":       20,
					"totalFalse":      30,
					"percentageTrue":  0.4,
					"percentageFalse": 0.6,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "count"},
				expectedValue: 7,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "totalTrue"},
				expectedValue: 20,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "totalFalse"},
				expectedValue: 30,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "percentageTrue"},
				expectedValue: 0.4,
			}, {
				pathToField:   []string{"GetMeta", "Things", "Car", "stillInProduction", "percentageFalse"},
				expectedValue: 0.6,
			}},
		},

		testCase{
			name:  "single prop: string",
			query: "{ GetMeta { Things { Car { modelName { topOccurrences { value, occurs } } } } } }",
			expectedProps: []kinds.MetaProperty{
				{
					Name: "modelName",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.TopOccurrencesValue,
						kinds.TopOccurrencesOccurs},
				},
			},
			resolverReturn: map[string]interface{}{
				"modelName": map[string]interface{}{
					"topOccurrences": []map[string]interface{}{
						{"value": "CheapNSlow", "occurs": 3},
						{"value": "FastNPricy", "occurs": 2},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"GetMeta", "Things", "Car", "modelName", "topOccurrences"},
				expectedValue: []interface{}{
					map[string]interface{}{"value": "CheapNSlow", "occurs": 3},
					map[string]interface{}{"value": "FastNPricy", "occurs": 2},
				},
			}},
		},

		testCase{
			name:  "single prop: date",
			query: "{ GetMeta { Things { Car { startOfProduction { topOccurrences { value, occurs } } } } } }",
			expectedProps: []kinds.MetaProperty{
				{
					Name: "startOfProduction",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.TopOccurrencesValue,
						kinds.TopOccurrencesOccurs},
				},
			},
			resolverReturn: map[string]interface{}{
				"startOfProduction": map[string]interface{}{
					"topOccurrences": []map[string]interface{}{
						{"value": "some-timestamp", "occurs": 3},
						{"value": "another-timestamp", "occurs": 2},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"GetMeta", "Things", "Car", "startOfProduction", "topOccurrences"},
				expectedValue: []interface{}{
					map[string]interface{}{"value": "some-timestamp", "occurs": 3},
					map[string]interface{}{"value": "another-timestamp", "occurs": 2},
				},
			}},
		},

		testCase{
			name:  "single prop: refprop",
			query: "{ GetMeta { Things { Car { MadeBy { pointingTo } } } } }",
			expectedProps: []kinds.MetaProperty{
				{
					Name:                "MadeBy",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.PointingTo},
				},
			},
			resolverReturn: map[string]interface{}{
				"MadeBy": map[string]interface{}{
					"pointingTo": []string{"Manufacturer"},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"GetMeta", "Things", "Car", "MadeBy", "pointingTo"},
				expectedValue: []interface{}{
					"Manufacturer",
				},
			}},
		},

		testCase{
			name:  "single prop: meta",
			query: "{ GetMeta { Things { Car { meta { count } } } } }",
			expectedProps: []kinds.MetaProperty{
				{
					Name:                "meta",
					StatisticalAnalyses: []kinds.StatisticalAnalysis{kinds.Count},
				},
			},
			resolverReturn: map[string]interface{}{
				"meta": map[string]interface{}{
					"count": 4,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"GetMeta", "Things", "Car", "meta", "count"},
				expectedValue: 4,
			}},
		},

		testCase{
			name:           "single prop: __typename",
			query:          "{ GetMeta { Things { Car { __typename } } } }",
			expectedProps:  nil,
			resolverReturn: nil,
		},
		testCase{
			name:           "single prop: __typename",
			query:          "{ GetMeta { Things { Car { modelName { __typename } } } } }",
			expectedProps:  nil,
			resolverReturn: nil,
		},
	}

	tests.AssertExtraction(t, kind.Thing, "Car")
}

func (tests testCases) AssertExtraction(t *testing.T, k kind.Kind, className string) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := newMockResolver(config.Config{})

			expectedParams := &kinds.GetMetaParams{
				Kind:       k,
				ClassName:  schema.ClassName(className),
				Properties: testCase.expectedProps,
			}

			resolver.On("LocalGetMeta", expectedParams).
				Return(testCase.resolverReturn, nil).Once()

			result := resolver.AssertResolve(t, testCase.query)

			for _, expectedResult := range testCase.expectedResults {
				value := result.Get(expectedResult.pathToField...).Result

				assert.Equal(t, expectedResult.expectedValue, value)
			}
		})
	}
}
