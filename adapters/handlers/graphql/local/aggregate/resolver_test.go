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

package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/config"
)

type testCase struct {
	name                     string
	query                    string
	expectedProps            []aggregation.ParamProperty
	resolverReturn           interface{}
	expectedResults          []result
	expectedGroupBy          *filters.Path
	expectedWhereFilter      *filters.LocalFilter
	expectedNearObjectFilter *searchparams.NearObject
	expectedNearVectorFilter *searchparams.NearVector
	expectedIncludeMetaCount bool
	expectedLimit            *int
	expectedObjectLimit      *int
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
			name: "for gh-758 (multiple operands)",
			query: `
			{
					Aggregate {
						Car(where:{
							operator:Or
							operands:[{
								valueText:"Fast",
								operator:Equal,
								path:["modelName"]
							}, {
								valueText:"Slow",
								operator:Equal,
								path:["modelName"]
							}]
						}) {
							__typename
							modelName {
								__typename
								count
							}
						}
					}
			}`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "modelName",
					Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"modelName": {
							Type: aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Count: 20,
							},
						},
					},
				},
			},
			expectedWhereFilter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    schema.ClassName("Car"),
								Property: schema.PropertyName("modelName"),
							},
							Value: &filters.Value{
								Value: "Fast",
								Type:  schema.DataTypeText,
							},
						},
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    schema.ClassName("Car"),
								Property: schema.PropertyName("modelName"),
							},
							Value: &filters.Value{
								Value: "Slow",
								Type:  schema.DataTypeText,
							},
						},
					},
				},
			},

			expectedGroupBy: nil,
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"__typename": "AggregateCar",
						"modelName": map[string]interface{}{
							"count":      20,
							"__typename": "AggregateCarmodelNameObj",
						},
					},
				},
			}},
		},
		testCase{
			name:  "without grouping prop",
			query: `{ Aggregate { Car { horsepower { mean } } } }`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "horsepower",
					Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"horsepower": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"mean": 275.7773,
							},
						},
					},
				},
			},

			expectedGroupBy: nil,
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"horsepower": map[string]interface{}{"mean": 275.7773},
					},
				},
			}},
		},
		testCase{
			name:  "setting limits overall",
			query: `{ Aggregate { Car(limit:20) { horsepower { mean } } } }`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "horsepower",
					Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"horsepower": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"mean": 275.7773,
							},
						},
					},
				},
			},

			expectedGroupBy: nil,
			expectedLimit:   ptInt(20),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"horsepower": map[string]interface{}{"mean": 275.7773},
					},
				},
			}},
		},
		testCase{
			name: "with props formerly contained only in Meta",
			query: `{ Aggregate { Car {
				stillInProduction { type count totalTrue percentageTrue totalFalse percentageFalse }
				modelName { type count topOccurrences { value occurs } }
				madeBy { type pointingTo }
				meta { count }
				} } } `,
			expectedIncludeMetaCount: true,
			expectedProps: []aggregation.ParamProperty{
				{
					Name: "stillInProduction",
					Aggregators: []aggregation.Aggregator{
						aggregation.TypeAggregator,
						aggregation.CountAggregator,
						aggregation.TotalTrueAggregator,
						aggregation.PercentageTrueAggregator,
						aggregation.TotalFalseAggregator,
						aggregation.PercentageFalseAggregator,
					},
				},
				{
					Name: "modelName",
					Aggregators: []aggregation.Aggregator{
						aggregation.TypeAggregator,
						aggregation.CountAggregator,
						aggregation.NewTopOccurrencesAggregator(ptInt(5)),
					},
				},
				{
					Name: "madeBy",
					Aggregators: []aggregation.Aggregator{
						aggregation.TypeAggregator,
						aggregation.PointingToAggregator,
					},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					Count: 10,
					Properties: map[string]aggregation.Property{
						"stillInProduction": {
							SchemaType: "boolean",
							Type:       aggregation.PropertyTypeBoolean,
							BooleanAggregation: aggregation.Boolean{
								TotalTrue:       23,
								TotalFalse:      17,
								PercentageTrue:  60,
								PercentageFalse: 40,
								Count:           40,
							},
						},
						"modelName": {
							SchemaType: "string",
							Type:       aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Count: 40,
								Items: []aggregation.TextOccurrence{
									{
										Value:  "fastcar",
										Occurs: 39,
									},
									{
										Value:  "slowcar",
										Occurs: 1,
									},
								},
							},
						},
						"madeBy": {
							SchemaType: "cref",
							Type:       aggregation.PropertyTypeReference,
							ReferenceAggregation: aggregation.Reference{
								PointingTo: []string{"Manufacturer"},
							},
						},
					},
				},
			},

			expectedGroupBy: nil,
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"stillInProduction": map[string]interface{}{
							"type":            "boolean",
							"totalTrue":       23,
							"totalFalse":      17,
							"percentageTrue":  60.0,
							"percentageFalse": 40.0,
							"count":           40,
						},
						"modelName": map[string]interface{}{
							"count": 40,
							"type":  "string",
							"topOccurrences": []interface{}{
								map[string]interface{}{
									"value":  "fastcar",
									"occurs": 39,
								},
								map[string]interface{}{
									"value":  "slowcar",
									"occurs": 1,
								},
							},
						},
						"madeBy": map[string]interface{}{
							"type":       "cref",
							"pointingTo": []interface{}{"Manufacturer"},
						},
						"meta": map[string]interface{}{
							"count": 10,
						},
					},
				},
			}},
		},
		testCase{
			name: "with custom limit in topOccurrences",
			query: `{ Aggregate { Car {
				modelName { topOccurrences(limit: 7) { value occurs } }
				} } } `,
			expectedProps: []aggregation.ParamProperty{
				{
					Name: "modelName",
					Aggregators: []aggregation.Aggregator{
						aggregation.NewTopOccurrencesAggregator(ptInt(7)),
					},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					Count: 10,
					Properties: map[string]aggregation.Property{
						"modelName": {
							SchemaType: "string",
							Type:       aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Items: []aggregation.TextOccurrence{
									{
										Value:  "fastcar",
										Occurs: 39,
									},
									{
										Value:  "slowcar",
										Occurs: 1,
									},
								},
							},
						},
					},
				},
			},

			expectedGroupBy: nil,
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"modelName": map[string]interface{}{
							"topOccurrences": []interface{}{
								map[string]interface{}{
									"value":  "fastcar",
									"occurs": 39,
								},
								map[string]interface{}{
									"value":  "slowcar",
									"occurs": 1,
								},
							},
						},
					},
				},
			}},
		},
		testCase{
			name:  "single prop: mean (with type)",
			query: `{ Aggregate { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean type } } } }`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "horsepower",
					Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator, aggregation.TypeAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"horsepower": {
							Type:       aggregation.PropertyTypeNumerical,
							SchemaType: "int",
							NumericalAggregations: map[string]interface{}{
								"mean": 275.7773,
							},
						},
					},
				},
			},

			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"horsepower": map[string]interface{}{
							"mean": 275.7773,
							"type": "int",
						},
					},
				},
			}},
		},

		testCase{
			name:  "single prop: mean with groupedBy path/value",
			query: `{ Aggregate { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean } groupedBy { value path } } } }`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "horsepower",
					Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					GroupedBy: &aggregation.GroupedBy{
						Path:  []string{"madeBy", "Manufacturer", "name"},
						Value: "best-manufacturer",
					},
					Properties: map[string]aggregation.Property{
						"horsepower": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"mean": 275.7773,
							},
						},
					},
				},
			},
			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
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
			}`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "horsepower",
					Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					GroupedBy: &aggregation.GroupedBy{
						Path:  []string{"madeBy", "Manufacturer", "name"},
						Value: "best-manufacturer",
					},
					Properties: map[string]aggregation.Property{
						"horsepower": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
								"mean": 275.7773,
							},
						},
					},
				},
			},
			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
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
			query: `{ Aggregate { Car(groupBy:["madeBy", "Manufacturer", "name"]) { horsepower { mean, median, mode, maximum, minimum, count, sum } } } }`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "horsepower",
					Aggregators: []aggregation.Aggregator{aggregation.MeanAggregator, aggregation.MedianAggregator, aggregation.ModeAggregator, aggregation.MaximumAggregator, aggregation.MinimumAggregator, aggregation.CountAggregator, aggregation.SumAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					GroupedBy: &aggregation.GroupedBy{
						Path:  []string{"madeBy", "Manufacturer", "name"},
						Value: "best-manufacturer",
					},
					Properties: map[string]aggregation.Property{
						"horsepower": {
							Type: aggregation.PropertyTypeNumerical,
							NumericalAggregations: map[string]interface{}{
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
			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{
				{
					pathToField: []string{"Aggregate", "Car"},
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
			query: `{ Aggregate { Car(groupBy:["madeBy", "Manufacturer", "name"]) { modelName { count } } } }`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "modelName",
					Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					GroupedBy: &aggregation.GroupedBy{
						Path:  []string{"madeBy", "Manufacturer", "name"},
						Value: "best-manufacturer",
					},
					Properties: map[string]aggregation.Property{
						"modelName": {
							Type: aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Count: 7,
							},
						},
					},
				},
			},
			expectedGroupBy: groupCarByMadeByManufacturerName(),
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"modelName": map[string]interface{}{
							"count": 7,
						},
					},
				},
			}},
		},

		testCase{
			name: "with objectLimit + nearObject (distance)",
			query: `
				{
					Aggregate{
						Car(
							objectLimit: 1
							nearObject: {
								id: "123"
								distance: 0.3
							}
						) {
							modelName {
								count
							}
						}
					}
				}
			`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "modelName",
					Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
				},
			},
			expectedObjectLimit: ptInt(1),
			expectedNearObjectFilter: &searchparams.NearObject{
				ID:           "123",
				Beacon:       "",
				Distance:     0.3,
				WithDistance: true,
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"modelName": {
							Type: aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Count: 7,
							},
						},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"modelName": map[string]interface{}{
							"count": 7,
						},
					},
				},
			}},
		},

		testCase{
			name: "with objectLimit + nearObject (certainty)",
			query: `
				{
					Aggregate{
						Car(
							objectLimit: 1
							nearObject: {
								id: "123"
								certainty: 0.7
							}
						) {
							modelName {
								count
							}
						}
					}
				}
			`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "modelName",
					Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
				},
			},
			expectedObjectLimit: ptInt(1),
			expectedNearObjectFilter: &searchparams.NearObject{
				ID:        "123",
				Beacon:    "",
				Certainty: 0.7,
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"modelName": {
							Type: aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Count: 7,
							},
						},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"modelName": map[string]interface{}{
							"count": 7,
						},
					},
				},
			}},
		},

		testCase{
			name: "with objectLimit + nearVector (distance)",
			query: `
				{
					Aggregate{
						Car(
							objectLimit: 1
							nearVector: {
								vector: [1, 2, 3]
								distance: 0.3
							}
						) {
							modelName {
								count
							}
						}
					}
				}
			`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "modelName",
					Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
				},
			},
			expectedObjectLimit: ptInt(1),
			expectedNearVectorFilter: &searchparams.NearVector{
				Vector:       []float32{1, 2, 3},
				Distance:     0.3,
				WithDistance: true,
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"modelName": {
							Type: aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Count: 7,
							},
						},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"modelName": map[string]interface{}{
							"count": 7,
						},
					},
				},
			}},
		},

		testCase{
			name: "with objectLimit + nearVector (certainty)",
			query: `
				{
					Aggregate{
						Car(
							objectLimit: 1
							nearVector: {
								vector: [1, 2, 3]
								certainty: 0.7
							}
						) {
							modelName {
								count
							}
						}
					}
				}
			`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "modelName",
					Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
				},
			},
			expectedObjectLimit: ptInt(1),
			expectedNearVectorFilter: &searchparams.NearVector{
				Vector:    []float32{1, 2, 3},
				Certainty: 0.7,
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"modelName": {
							Type: aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Count: 7,
							},
						},
					},
				},
			},
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"modelName": map[string]interface{}{
							"count": 7,
						},
					},
				},
			}},
		},
		testCase{
			name: "[deprecated string] for gh-758 (multiple operands)",
			query: `
			{
					Aggregate {
						Car(where:{
							operator:Or
							operands:[{
								valueString:"Fast",
								operator:Equal,
								path:["modelName"]
							}, {
								valueString:"Slow",
								operator:Equal,
								path:["modelName"]
							}]
						}) {
							__typename
							modelName {
								__typename
								count
							}
						}
					}
			}`,
			expectedProps: []aggregation.ParamProperty{
				{
					Name:        "modelName",
					Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
				},
			},
			resolverReturn: []aggregation.Group{
				{
					Properties: map[string]aggregation.Property{
						"modelName": {
							Type: aggregation.PropertyTypeText,
							TextAggregation: aggregation.Text{
								Count: 20,
							},
						},
					},
				},
			},
			expectedWhereFilter: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorOr,
					Operands: []filters.Clause{
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    schema.ClassName("Car"),
								Property: schema.PropertyName("modelName"),
							},
							Value: &filters.Value{
								Value: "Fast",
								Type:  schema.DataTypeString,
							},
						},
						{
							Operator: filters.OperatorEqual,
							On: &filters.Path{
								Class:    schema.ClassName("Car"),
								Property: schema.PropertyName("modelName"),
							},
							Value: &filters.Value{
								Value: "Slow",
								Type:  schema.DataTypeString,
							},
						},
					},
				},
			},

			expectedGroupBy: nil,
			expectedResults: []result{{
				pathToField: []string{"Aggregate", "Car"},
				expectedValue: []interface{}{
					map[string]interface{}{
						"__typename": "AggregateCar",
						"modelName": map[string]interface{}{
							"count":      20,
							"__typename": "AggregateCarmodelNameObj",
						},
					},
				},
			}},
		},
	}

	tests.AssertExtraction(t, "Car")
}

func (tests testCases) AssertExtraction(t *testing.T, className string) {
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := newMockResolver(config.Config{})

			expectedParams := &aggregation.Params{
				ClassName:        schema.ClassName(className),
				Properties:       testCase.expectedProps,
				GroupBy:          testCase.expectedGroupBy,
				Filters:          testCase.expectedWhereFilter,
				NearObject:       testCase.expectedNearObjectFilter,
				NearVector:       testCase.expectedNearVectorFilter,
				IncludeMetaCount: testCase.expectedIncludeMetaCount,
				Limit:            testCase.expectedLimit,
				ObjectLimit:      testCase.expectedObjectLimit,
			}

			resolver.On("Aggregate", expectedParams).
				Return(testCase.resolverReturn, nil).Once()

			result := resolver.AssertResolve(t, testCase.query)

			for _, expectedResult := range testCase.expectedResults {
				value := result.Get(expectedResult.pathToField...).Result

				assert.Equal(t, expectedResult.expectedValue, value)
			}
		})
	}
}

func ptInt(in int) *int {
	return &in
}
