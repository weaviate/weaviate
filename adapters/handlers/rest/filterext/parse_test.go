//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filterext

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
				filter, err := Parse(test.input, "Todo", false, nil)
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
				filter, err := Parse(test.input, "Todo", false, nil)
				assert.ErrorAs(t, err, &test.expectedErr)
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
				filter, err := Parse(test.input, "Todo", false, nil)
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
			{
				name: "chained together using not",
				input: &models.WhereFilter{
					Operator: "Not",
					Operands: []*models.WhereFilter{
						inputIntFilterWithValueAndPath(44,
							[]string{"hasAction", "SomeAction", "intField"}),
					},
				},
				expectedFilter: &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorNot,
						Operands: []filters.Clause{
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
									Value: 44,
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
				filter, err := Parse(test.input, "Todo", false, nil)
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

// Test_Parse_NamespacesEnabledQualifiesRefPath asserts that reference-path
// inner class segments are qualified via QualifyRefTarget against the previous
// level's namespace, including its policy (namespaced callers can't type a
// prefix; admins must match the source NS; foreign-NS rejected). Non-NS
// clusters pass through unchanged.
func Test_Parse_NamespacesEnabledQualifiesRefPath(t *testing.T) {
	t.Parallel()

	value := "v"
	directProp := &models.WhereFilter{
		Operator:  models.WhereFilterOperatorEqual,
		Path:      []string{"name"},
		ValueText: &value,
	}
	refPath := &models.WhereFilter{
		Operator:  models.WhereFilterOperatorEqual,
		Path:      []string{"inCountry", "Country", "name"},
		ValueText: &value,
	}
	nestedRefPath := &models.WhereFilter{
		Operator:  models.WhereFilterOperatorEqual,
		Path:      []string{"inCountry", "Country", "hasCapital", "City", "name"},
		ValueText: &value,
	}
	nestedAndWithRefPath := &models.WhereFilter{
		Operator: models.WhereFilterOperatorAnd,
		Operands: []*models.WhereFilter{directProp, refPath},
	}

	nsCaller := &models.Principal{Username: "u", Namespace: "customer1"}
	admin := &models.Principal{Username: "admin"}

	t.Run("namespaced caller short ref-path is qualified via parent NS", func(t *testing.T) {
		out, err := Parse(refPath, "customer1:City", true, nsCaller)
		require.NoError(t, err)
		require.NotNil(t, out)
		require.NotNil(t, out.Root.On)
		require.NotNil(t, out.Root.On.Child)
		assert.Equal(t, "customer1:City", out.Root.On.Class.String())
		assert.Equal(t, "customer1:Country", out.Root.On.Child.Class.String(),
			"inner class segment should inherit the source's namespace")
	})

	t.Run("multi-hop ref-path qualifies every level", func(t *testing.T) {
		out, err := Parse(nestedRefPath, "customer1:City", true, nsCaller)
		require.NoError(t, err)
		require.NotNil(t, out)
		require.NotNil(t, out.Root.On)
		require.NotNil(t, out.Root.On.Child)
		require.NotNil(t, out.Root.On.Child.Child)
		assert.Equal(t, "customer1:Country", out.Root.On.Child.Class.String())
		assert.Equal(t, "customer1:City", out.Root.On.Child.Child.Class.String(),
			"every linked class along the chain must inherit the source's namespace")
	})

	t.Run("admin own-namespace prefix on inner class does not double-qualify", func(t *testing.T) {
		ownNS := &models.WhereFilter{
			Operator:  models.WhereFilterOperatorEqual,
			Path:      []string{"inCountry", "customer1:Country", "name"},
			ValueText: &value,
		}
		out, err := Parse(ownNS, "customer1:City", true, admin)
		require.NoError(t, err)
		require.NotNil(t, out.Root.On.Child)
		assert.Equal(t, "customer1:Country", out.Root.On.Child.Class.String(),
			"admin's own-NS prefix must be normalized, not stacked")
	})

	t.Run("namespaced caller cannot type any prefix on inner class", func(t *testing.T) {
		withPrefix := &models.WhereFilter{
			Operator:  models.WhereFilterOperatorEqual,
			Path:      []string{"inCountry", "customer1:Country", "name"},
			ValueText: &value,
		}
		_, err := Parse(withPrefix, "customer1:City", true, nsCaller)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})

	t.Run("foreign-namespace prefix on inner class is rejected", func(t *testing.T) {
		foreign := &models.WhereFilter{
			Operator:  models.WhereFilterOperatorEqual,
			Path:      []string{"inCountry", "customer2:Country", "name"},
			ValueText: &value,
		}
		_, err := Parse(foreign, "customer1:City", true, admin)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a valid class name")
	})

	t.Run("compound AND threads qualification into nested operands", func(t *testing.T) {
		out, err := Parse(nestedAndWithRefPath, "customer1:City", true, nsCaller)
		require.NoError(t, err)
		require.NotNil(t, out)
		// The compound has two operands: a direct-prop and a ref-path.
		require.Len(t, out.Root.Operands, 2)
		var refOperand *filters.Clause
		for i := range out.Root.Operands {
			if out.Root.Operands[i].On != nil && out.Root.Operands[i].On.Child != nil {
				refOperand = &out.Root.Operands[i]
			}
		}
		require.NotNil(t, refOperand, "expected a ref-path operand in the AND")
		assert.Equal(t, "customer1:Country", refOperand.On.Child.Class.String())
	})

	t.Run("namespacesEnabled accepts direct property filter", func(t *testing.T) {
		out, err := Parse(directProp, "customer1:City", true, nsCaller)
		require.NoError(t, err)
		assert.NotNil(t, out)
	})

	t.Run("namespacesEnabled=false still accepts ref-path (pass-through)", func(t *testing.T) {
		out, err := Parse(refPath, "City", false, nil)
		require.NoError(t, err)
		assert.NotNil(t, out)
	})
}
