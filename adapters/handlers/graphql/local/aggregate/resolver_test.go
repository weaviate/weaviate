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
package aggregate

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name                string
	query               string
	expectedProps       []Property
	resolverReturn      interface{}
	expectedResults     []result
	expectedGroupBy     *common_filters.Path
	expectedWhereFilter *common_filters.LocalFilter
}

type testCases []testCase

type result struct {
	pathToField   []string
	expectedValue interface{}
}

func groupCarByMadeByManufacturerName() *common_filters.Path {
	return &common_filters.Path{
		Class:    schema.ClassName("Car"),
		Property: schema.PropertyName("madeBy"),
		Child: &common_filters.Path{
			Class:    schema.ClassName("Manufacturer"),
			Property: schema.PropertyName("name"),
		},
	}
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
			resolverReturn: []interface{}{
				map[string]interface{}{
					"horsepower": map[string]interface{}{
						"mean": 275.7773,
					},
				},
			},

			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Things", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"horsepower": map[string]interface{}{"mean": 275.7773},
					},
				},
			}},
		},

		testCase{
			name:  "single prop: mean with groupedBy path/value",
			query: `{ Aggregate { Things { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean } groupedBy { value path } } } } }`,
			expectedProps: []Property{
				{
					Name:        "horsepower",
					Aggregators: []Aggregator{Mean},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"horsepower": map[string]interface{}{
						"mean": 275.7773,
					},
					"groupedBy": map[string]interface{}{
						"path":  []interface{}{"madeBy", "Manufacturer", "name"},
						"value": "best-manufacturer",
					},
				},
			},

			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Things", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"horsepower": map[string]interface{}{"mean": 275.7773},
						"groupedBy": map[string]interface{}{
							"path":  []interface{}{"madeBy", "Manufacturer", "name"},
							"value": "best-manufacturer",
						},
					},
				},
			}},
		},

		testCase{
			name: "single prop: mean with a where filter",
			query: `{ 
				Aggregate { 
					Things { 
						Car(
							groupBy:["madeBy", "Manufacturer", "name"]
							where: {
								operator: LessThan,
								valueInt: 200,
								path: ["horsepower"],
							}
						) { 
							horsepower { 
								mean 
							} 
						} 
					} 
				} 
			}`,
			expectedProps: []Property{
				{
					Name:        "horsepower",
					Aggregators: []Aggregator{Mean},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"horsepower": map[string]interface{}{
						"mean": 275.7773,
					},
				},
			},
			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Things", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"horsepower": map[string]interface{}{
							"mean": 275.7773,
						},
					},
				},
			}},
			expectedWhereFilter: &common_filters.LocalFilter{
				Root: &common_filters.Clause{
					On: &common_filters.Path{
						Class:    schema.ClassName("Car"),
						Property: schema.PropertyName("horsepower"),
					},
					Value: &common_filters.Value{
						Value: 200,
						Type:  schema.DataTypeInt,
					},
					Operator: common_filters.OperatorLessThan,
				},
			},
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
			resolverReturn: []interface{}{
				map[string]interface{}{
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
			},
			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Things", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
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
				},
			},
			},
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
			resolverReturn: []interface{}{
				map[string]interface{}{
					"modelName": map[string]interface{}{
						"count": 7,
					},
				},
			},
			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Things", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"modelName": map[string]interface{}{
							"count": 7,
						},
					},
				},
			}},
		},
	}

	tests.AssertExtraction(t, kind.THING_KIND, "Car")
}

func (tests testCases) AssertExtraction(t *testing.T, k kind.Kind, className string) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := newMockResolver(config.Config{})

			expectedParams := &Params{
				Kind:       k,
				ClassName:  schema.ClassName(className),
				Properties: testCase.expectedProps,
				GroupBy:    testCase.expectedGroupBy,
				Filters:    testCase.expectedWhereFilter,
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
