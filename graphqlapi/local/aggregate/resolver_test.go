/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2018 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * AUTHOR: Bob van Luijt (bob@kub.design)
 * See www.creativesoftwarefdn.org for details
 * Contact: @CreativeSofwFdn / bob@kub.design
 */
package aggregate

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name            string
	query           string
	expectedProps   []Property
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
			query: `{ Aggregate { Things { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean } } } } }`,
			expectedProps: []Property{
				{
					Name:        "horsepower",
					Aggregators: []Aggregator{Mean},
				},
			},
			resolverReturn: map[string]interface{}{
				"horsepower": map[string]interface{}{
					"mean": 275.7773,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"Aggregate", "Things", "Car", "horsepower", "mean"},
				expectedValue: 275.7773,
			}},
		},

		testCase{
			name:  "all int props",
			query: `{ Aggregate { Things { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean, median, mode, maximum, minimum, count, sum } } } } }`,
			expectedProps: []Property{
				{
					Name:        "horsepower",
					Aggregators: []Aggregator{Mean, Median, Mode, Maximum, Minimum, Count, Sum},
				},
			},
			resolverReturn: map[string]interface{}{
				"horsepower": map[string]interface{}{
					"maximum": 610.0,
					"minimum": 89.0,
					"mean":    275.7,
					"median":  289.0,
					"mode":    115.0,
					"count":   23,
					"sum":     6343.0,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"Aggregate", "Things", "Car", "horsepower", "maximum"},
				expectedValue: 610.0,
			}, {
				pathToField:   []string{"Aggregate", "Things", "Car", "horsepower", "minimum"},
				expectedValue: 89.0,
			}, {
				pathToField:   []string{"Aggregate", "Things", "Car", "horsepower", "count"},
				expectedValue: 23,
			}, {
				pathToField:   []string{"Aggregate", "Things", "Car", "horsepower", "sum"},
				expectedValue: 6343.0,
			}, {
				pathToField:   []string{"Aggregate", "Things", "Car", "horsepower", "mean"},
				expectedValue: 275.7,
			}, {
				pathToField:   []string{"Aggregate", "Things", "Car", "horsepower", "median"},
				expectedValue: 289.0,
			}, {
				pathToField:   []string{"Aggregate", "Things", "Car", "horsepower", "mode"},
				expectedValue: 115.0,
			}},
		},

		testCase{
			name:  "single prop: string",
			query: `{ Aggregate { Things { Car(groupBy:["madeBy", "Manufacturer", "name"]) { modelName { count } } } } }`,
			expectedProps: []Property{
				{
					Name:        "modelName",
					Aggregators: []Aggregator{Count},
				},
			},
			resolverReturn: map[string]interface{}{
				"modelName": map[string]interface{}{
					"count": 7,
				},
			},
			expectedResults: []result{{
				pathToField:   []string{"Aggregate", "Things", "Car", "modelName", "count"},
				expectedValue: 7,
			}},
		},
	}

	tests.AssertExtraction(t, kind.THING_KIND, "Car")
}

func (tests testCases) AssertExtraction(t *testing.T, k kind.Kind, className string) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := newMockResolver()

			expectedParams := &Params{
				Kind:       k,
				ClassName:  schema.ClassName(className),
				Properties: testCase.expectedProps,
			}

			resolver.On("LocalAggregate", expectedParams).
				Return(testCase.resolverReturn, nil).Once()

			result := resolver.AssertResolve(t, testCase.query)

			for _, expectedResult := range testCase.expectedResults {
				value := result.Get(expectedResult.pathToField...).Result

				assert.Equal(t, expectedResult.expectedValue, value)
			}
		})
	}
}
