//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name                string
	query               string
	expectedProps       []traverser.AggregateProperty
	resolverReturn      interface{}
	expectedResults     []result
	expectedGroupBy     *filters.Path
	expectedWhereFilter *filters.LocalFilter
}

type testCases []testCase

type result struct {
	pathToField   []string
	expectedValue interface{}
}

func groupCarByMadeByManufacturerName() *filters.Path {
	return &filters.Path{
		Class:    schema.ClassName("Car"),
		Property: schema.PropertyName("madeBy"),
		Child: &filters.Path{
			Class:    schema.ClassName("Manufacturer"),
			Property: schema.PropertyName("name"),
		},
	}
}

func Test_Resolve(t *testing.T) {
	t.Parallel()

	tests := testCases{
		testCase{
			name:  "without grouping prop",
			query: `{ Aggregate { Things { Car { horsepower { mean } } } } }`,
			expectedProps: []traverser.AggregateProperty{
				{
					Name:        "horsepower",
					Aggregators: []traverser.Aggregator{traverser.MeanAggregator},
				},
			},
			resolverReturn: []interface{}{
				map[string]interface{}{
					"horsepower": map[string]interface{}{
						"mean": 275.7773,
					},
				},
			},

			expectedGroupBy: nil,
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
			name:  "single prop: mean",
			query: `{ Aggregate { Things { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean } } } } }`,
			expectedProps: []traverser.AggregateProperty{
				{
					Name:        "horsepower",
					Aggregators: []traverser.Aggregator{traverser.MeanAggregator},
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
			expectedProps: []traverser.AggregateProperty{
				{
					Name:        "horsepower",
					Aggregators: []traverser.Aggregator{traverser.MeanAggregator},
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
			expectedProps: []traverser.AggregateProperty{
				{
					Name:        "horsepower",
					Aggregators: []traverser.Aggregator{traverser.MeanAggregator},
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
			expectedWhereFilter: &filters.LocalFilter{
				Root: &filters.Clause{
					On: &filters.Path{
						Class:    schema.ClassName("Car"),
						Property: schema.PropertyName("horsepower"),
					},
					Value: &filters.Value{
						Value: 200,
						Type:  schema.DataTypeInt,
					},
					Operator: filters.OperatorLessThan,
				},
			},
		},

		testCase{
			name:  "all int props",
			query: `{ Aggregate { Things { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean, median, mode, maximum, minimum, count, sum } } } } }`,
			expectedProps: []traverser.AggregateProperty{
				{
					Name:        "horsepower",
					Aggregators: []traverser.Aggregator{traverser.MeanAggregator, traverser.MedianAggregator, traverser.ModeAggregator, traverser.MaximumAggregator, traverser.MinimumAggregator, traverser.CountAggregator, traverser.SumAggregator},
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
			expectedProps: []traverser.AggregateProperty{
				{
					Name:        "modelName",
					Aggregators: []traverser.Aggregator{traverser.CountAggregator},
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

	tests.AssertExtraction(t, kind.Thing, "Car")
}

func (tests testCases) AssertExtraction(t *testing.T, k kind.Kind, className string) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := newMockResolver(config.Config{})

			expectedParams := &traverser.AggregateParams{
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
