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

package filterext

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_ExtractFlatFilters(t *testing.T) {
	t.Parallel()

	type test struct {
		name           string
		input          *models.WhereFilter
		expectedFilter *filters.LocalFilter
		expectedErr    error
	}

	t.Run("all value types", func(t *testing.T) {
		tests := []test{
			{
				name: "no filter",
			},
			{
				name: "valid int filter",
				input: &models.WhereFilter{
					Operator: "Equal",
					ValueInt: ptInt(42),
					Path:     []string{"intField"},
				},
				expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.AssertValidClassName("Todo"),
						Property: schema.AssertValidPropertyName("intField"),
					},
					Value: &filters.Value{
						Value: 42,
						Type:  schema.DataTypeInt,
					},
				}},
			},
			{
				name: "valid date filter",
				input: &models.WhereFilter{
					Operator:  "Equal",
					ValueDate: ptString("foo bar"),
					Path:      []string{"dateField"},
				},
				expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.AssertValidClassName("Todo"),
						Property: schema.AssertValidPropertyName("dateField"),
					},
					Value: &filters.Value{
						Value: "foo bar",
						Type:  schema.DataTypeDate,
					},
				}},
			},
			{
				name: "valid text filter",
				input: &models.WhereFilter{
					Operator:  "Equal",
					ValueText: ptString("foo bar"),
					Path:      []string{"textField"},
				},
				expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.AssertValidClassName("Todo"),
						Property: schema.AssertValidPropertyName("textField"),
					},
					Value: &filters.Value{
						Value: "foo bar",
						Type:  schema.DataTypeText,
					},
				}},
			},
			{
				name: "valid number filter",
				input: &models.WhereFilter{
					Operator:    "Equal",
					ValueNumber: ptFloat(20.20),
					Path:        []string{"numberField"},
				},
				expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.AssertValidClassName("Todo"),
						Property: schema.AssertValidPropertyName("numberField"),
					},
					Value: &filters.Value{
						Value: 20.20,
						Type:  schema.DataTypeNumber,
					},
				}},
			},
			{
				name: "valid bool filter",
				input: &models.WhereFilter{
					Operator:     "Equal",
					ValueBoolean: ptBool(true),
					Path:         []string{"booleanField"},
				},
				expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.AssertValidClassName("Todo"),
						Property: schema.AssertValidPropertyName("booleanField"),
					},
					Value: &filters.Value{
						Value: true,
						Type:  schema.DataTypeBoolean,
					},
				}},
			},
			{
				name: "valid geo range filter",
				input: &models.WhereFilter{
					Operator:      "WithinGeoRange",
					ValueGeoRange: inputGeoRangeFilter(0.5, 0.6, 2.0),
					Path:          []string{"geoField"},
				},
				expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorWithinGeoRange,
					On: &filters.Path{
						Class:    schema.AssertValidClassName("Todo"),
						Property: schema.AssertValidPropertyName("geoField"),
					},
					Value: &filters.Value{
						Value: filters.GeoRange{
							GeoCoordinates: &models.GeoCoordinates{
								Latitude:  ptFloat32(0.5),
								Longitude: ptFloat32(0.6),
							},
							Distance: 2.0,
						},
						Type: schema.DataTypeGeoCoordinates,
					},
				}},
			},
			{
				name: "[deprecated string] valid string filter",
				input: &models.WhereFilter{
					Operator:    "Equal",
					ValueString: ptString("foo bar"),
					Path:        []string{"stringField"},
				},
				expectedFilter: &filters.LocalFilter{Root: &filters.Clause{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.AssertValidClassName("Todo"),
						Property: schema.AssertValidPropertyName("stringField"),
					},
					Value: &filters.Value{
						Value: "foo bar",
						Type:  schema.DataTypeString,
					},
				}},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				filter, err := Parse(test.input, "Todo")
				assert.Equal(t, test.expectedErr, err)
				assert.Equal(t, test.expectedFilter, filter)
			})
		}
	})

	t.Run("invalid cases", func(t *testing.T) {
		tests := []test{
			{
				name: "geo missing coordinates",
				input: &models.WhereFilter{
					Operator: "WithinGeoRange",
					ValueGeoRange: &models.WhereFilterGeoRange{
						Distance: &models.WhereFilterGeoRangeDistance{
							Max: 20.0,
						},
					},
					Path: []string{"geoField"},
				},
				expectedErr: fmt.Errorf("invalid where filter: valueGeoRange: " +
					"field 'geoCoordinates' must be set"),
			},
			{
				name: "geo missing distance object",
				input: &models.WhereFilter{
					Operator: "WithinGeoRange",
					ValueGeoRange: &models.WhereFilterGeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  ptFloat32(4.5),
							Longitude: ptFloat32(3.7),
						},
					},
					Path: []string{"geoField"},
				},
				expectedErr: fmt.Errorf("invalid where filter: valueGeoRange: " +
					"field 'distance' must be set"),
			},
			{
				name: "geo having negative distance",
				input: &models.WhereFilter{
					Operator: "WithinGeoRange",
					ValueGeoRange: &models.WhereFilterGeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  ptFloat32(4.5),
							Longitude: ptFloat32(3.7),
						},
						Distance: &models.WhereFilterGeoRangeDistance{
							Max: -20.0,
						},
					},
					Path: []string{"geoField"},
				},
				expectedErr: fmt.Errorf("invalid where filter: valueGeoRange: " +
					"field 'distance.max' must be a positive number"),
			},
			{
				name: "and operator and path set",
				input: &models.WhereFilter{
					Operator: "And",
					Path:     []string{"some field"},
				},
				expectedErr: fmt.Errorf("invalid where filter: " +
					"operator 'And' not compatible with field 'path', remove 'path' " +
					"or switch to compare operator (eg. Equal, NotEqual, etc.)"),
			},
			{
				name: "and operator and value set",
				input: &models.WhereFilter{
					Operator: "And",
					ValueInt: ptInt(43),
				},
				expectedErr: fmt.Errorf("invalid where filter: " +
					"operator 'And' not compatible with field 'value<Type>', " +
					"remove value field or switch to compare operator " +
					"(eg. Equal, NotEqual, etc.)"),
			},
			{
				name: "and operator and no operands set",
				input: &models.WhereFilter{
					Operator: "And",
				},
				expectedErr: fmt.Errorf("invalid where filter: " +
					"operator 'And', but no operands set - add at least one operand"),
			},
			{
				name: "equal operator and no values set",
				input: &models.WhereFilter{
					Operator: "Equal",
				},
				expectedErr: fmt.Errorf("invalid where filter: " +
					"got operator 'Equal', but no value<Type> field set"),
			},
			{
				name: "equal operator and no path set",
				input: &models.WhereFilter{
					Operator: "Equal",
					ValueInt: ptInt(43),
				},
				expectedErr: fmt.Errorf("invalid where filter: " +
					"field 'path': must have at least one element"),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				filter, err := Parse(test.input, "Todo")
				assert.Equal(t, test.expectedErr, err)
				assert.Equal(t, test.expectedFilter, filter)
			})
		}
	})

	t.Run("all operator types", func(t *testing.T) {
		// all tests use int as the value type, value types are tested separately
		tests := []test{
			{
				name:           "equal",
				input:          inputIntFilterWithOp("Equal"),
				expectedFilter: intFilterWithOp(filters.OperatorEqual),
			},
			{
				name:           "like", // doesn't make sense on an int, but that's irrelevant for parsing
				input:          inputIntFilterWithOp("Like"),
				expectedFilter: intFilterWithOp(filters.OperatorLike),
			},
			{
				name:           "not equal",
				input:          inputIntFilterWithOp("NotEqual"),
				expectedFilter: intFilterWithOp(filters.OperatorNotEqual),
			},
			{
				name:           "greater than",
				input:          inputIntFilterWithOp("GreaterThan"),
				expectedFilter: intFilterWithOp(filters.OperatorGreaterThan),
			},
			{
				name:           "greater than/equal",
				input:          inputIntFilterWithOp("GreaterThanEqual"),
				expectedFilter: intFilterWithOp(filters.OperatorGreaterThanEqual),
			},
			{
				name:           "less than",
				input:          inputIntFilterWithOp("LessThan"),
				expectedFilter: intFilterWithOp(filters.OperatorLessThan),
			},
			{
				name:           "less than/equal",
				input:          inputIntFilterWithOp("LessThanEqual"),
				expectedFilter: intFilterWithOp(filters.OperatorLessThanEqual),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				filter, err := Parse(test.input, "Todo")
				assert.Equal(t, test.expectedErr, err)
				assert.Equal(t, test.expectedFilter, filter)
			})
		}
	})

	t.Run("nested filters", func(t *testing.T) {
		// all tests use int as the value type, value types are tested separately
		tests := []test{
			{
				name: "chained together using and",
				input: &models.WhereFilter{
					Operator: "And",
					Operands: []*models.WhereFilter{
						inputIntFilterWithValue(42),
						inputIntFilterWithValueAndPath(43,
							[]string{"hasAction", "SomeAction", "intField"}),
					},
				},
				expectedFilter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorAnd,
						Operands: []filters.Clause{
							{
								Operator: filters.OperatorEqual,
								On: &filters.Path{
									Class:    schema.AssertValidClassName("Todo"),
									Property: schema.AssertValidPropertyName("intField"),
								},
								Value: &filters.Value{
									Value: 42,
									Type:  schema.DataTypeInt,
								},
							},
							{
								Operator: filters.OperatorEqual,
								On: &filters.Path{
									Class:    schema.AssertValidClassName("Todo"),
									Property: schema.AssertValidPropertyName("hasAction"),
									Child: &filters.Path{
										Class:    schema.AssertValidClassName("SomeAction"),
										Property: schema.AssertValidPropertyName("intField"),
									},
								},
								Value: &filters.Value{
									Value: 43,
									Type:  schema.DataTypeInt,
								},
							},
						},
					},
				},
			},
			{
				name: "chained together using or",
				input: &models.WhereFilter{
					Operator: "Or",
					Operands: []*models.WhereFilter{
						inputIntFilterWithValue(42),
						inputIntFilterWithValueAndPath(43,
							[]string{"hasAction", "SomeAction", "intField"}),
					},
				},
				expectedFilter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorOr,
						Operands: []filters.Clause{
							{
								Operator: filters.OperatorEqual,
								On: &filters.Path{
									Class:    schema.AssertValidClassName("Todo"),
									Property: schema.AssertValidPropertyName("intField"),
								},
								Value: &filters.Value{
									Value: 42,
									Type:  schema.DataTypeInt,
								},
							},
							{
								Operator: filters.OperatorEqual,
								On: &filters.Path{
									Class:    schema.AssertValidClassName("Todo"),
									Property: schema.AssertValidPropertyName("hasAction"),
									Child: &filters.Path{
										Class:    schema.AssertValidClassName("SomeAction"),
										Property: schema.AssertValidPropertyName("intField"),
									},
								},
								Value: &filters.Value{
									Value: 43,
									Type:  schema.DataTypeInt,
								},
							},
						},
					},
				},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				filter, err := Parse(test.input, "Todo")
				assert.Equal(t, test.expectedErr, err)
				assert.Equal(t, test.expectedFilter, filter)
			})
		}
	})
}

func ptInt(in int) *int64 {
	a := int64(in)
	return &a
}

func ptFloat(in float64) *float64 {
	return &in
}

func ptString(in string) *string {
	return &in
}

func ptBool(in bool) *bool {
	return &in
}

func intFilterWithOp(op filters.Operator) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: op,
			On: &filters.Path{
				Class:    schema.AssertValidClassName("Todo"),
				Property: schema.AssertValidPropertyName("intField"),
			},
			Value: &filters.Value{
				Value: 42,
				Type:  schema.DataTypeInt,
			},
		},
	}
}

func inputIntFilterWithOp(op string) *models.WhereFilter {
	return &models.WhereFilter{
		Operator: op,
		ValueInt: ptInt(42),
		Path:     []string{"intField"},
	}
}

func inputIntFilterWithValue(value int) *models.WhereFilter {
	return &models.WhereFilter{
		Operator: "Equal",
		ValueInt: ptInt(value),
		Path:     []string{"intField"},
	}
}

func inputIntFilterWithValueAndPath(value int,
	path []string,
) *models.WhereFilter {
	return &models.WhereFilter{
		Operator: "Equal",
		ValueInt: ptInt(value),
		Path:     path,
	}
}

func inputGeoRangeFilter(lat, lon, max float64) *models.WhereFilterGeoRange {
	return &models.WhereFilterGeoRange{
		Distance: &models.WhereFilterGeoRangeDistance{
			Max: max,
		},
		GeoCoordinates: &models.GeoCoordinates{
			Latitude:  ptFloat32(float32(lat)),
			Longitude: ptFloat32(float32(lon)),
		},
	}
}

func ptFloat32(in float32) *float32 {
	return &in
}
