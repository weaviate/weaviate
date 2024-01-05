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

package traverser

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
)

func Test_Explorer_GetClass_WithFilters(t *testing.T) {
	valueNameFromDataType := func(dt schema.DataType) string {
		return "value" + strings.ToUpper(string(dt[0])) + string(dt[1:])
	}
	log, _ := test.NewNullLogger()
	type test struct {
		name          string
		filters       *filters.LocalFilter
		expectedError error
	}
	buildInvalidTests := func(op filters.Operator, path []interface{},
		correctDt schema.DataType, dts []schema.DataType, value interface{},
	) []test {
		out := make([]test, len(dts))
		for i, dt := range dts {
			useInstead := correctDt
			if baseType, ok := schema.IsArrayType(correctDt); ok {
				useInstead = baseType
			}

			out[i] = test{
				name:    fmt.Sprintf("invalid %s filter - using %s", correctDt, dt),
				filters: buildFilter(op, path, dt, value),
				expectedError: errors.Errorf("invalid 'where' filter: data type filter cannot use"+
					" \"%s\" on type \"%s\", use \"%s\" instead",
					valueNameFromDataType(dt),
					correctDt,
					valueNameFromDataType(useInstead),
				),
			}
		}

		return out
	}

	buildInvalidRefCountTests := func(op filters.Operator, path []interface{},
		correctDt schema.DataType, dts []schema.DataType, value interface{},
	) []test {
		out := make([]test, len(dts))
		for i, dt := range dts {
			out[i] = test{
				name:    fmt.Sprintf("invalid %s filter - using %s", correctDt, dt),
				filters: buildFilter(op, path, dt, value),
				expectedError: errors.Errorf("invalid 'where' filter: "+
					"Property %q is a ref prop to the class %q. Only "+
					"\"valueInt\" can be used on a ref prop directly to count the number of refs. "+
					"Or did you mean to filter on a primitive prop of the referenced class? "+
					"In this case make sure your path contains 3 elements in the form of "+
					"[<propName>, <ClassNameOfReferencedClass>, <primitvePropOnClass>]",
					path[0], "ClassTwo"),
			}
		}

		return out
	}

	buildInvalidNestedTests := func(op filters.Operator, path []interface{},
		correctDt schema.DataType, dts []schema.DataType, value interface{},
	) []test {
		out := make([]test, len(dts))
		for i, dt := range dts {
			useInstead := correctDt
			if baseType, ok := schema.IsArrayType(correctDt); ok {
				useInstead = baseType
			}

			out[i] = test{
				name: fmt.Sprintf("invalid %s filter - using %s", correctDt, dt),
				filters: buildNestedFilter(filters.OperatorAnd,
					// valid operand
					buildFilter(op, path, correctDt, value),
					// invalid operand
					buildFilter(op, path, dt, value),
				),
				expectedError: errors.Errorf("invalid 'where' filter: child operand at "+
					"position 1: data type filter cannot use"+
					" \"%s\" on type \"%s\", use \"%s\" instead",
					valueNameFromDataType(dt),
					correctDt,
					valueNameFromDataType(useInstead),
				),
			}
		}

		return out
	}

	tests := [][]test{
		{
			{
				name:          "without filter",
				expectedError: nil,
			},
		},

		// single level, primitive props + arrays
		{
			{
				name: "valid text search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"text_prop"},
					schema.DataTypeText, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"text_prop"},
			schema.DataTypeText, allValueTypesExcept(schema.DataTypeText, schema.DataTypeString), "foo"),
		{
			{
				name: "valid text array search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"text_array_prop"},
					schema.DataTypeText, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"text_array_prop"},
			schema.DataTypeTextArray, allValueTypesExcept(schema.DataTypeText, schema.DataTypeString), "foo"),
		{
			{
				name: "valid number search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"number_prop"},
					schema.DataTypeNumber, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"number_prop"},
			schema.DataTypeNumber, allValueTypesExcept(schema.DataTypeNumber), "foo"),
		{
			{
				name: "valid number array search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"number_array_prop"},
					schema.DataTypeNumber, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"number_array_prop"},
			schema.DataTypeNumberArray, allValueTypesExcept(schema.DataTypeNumber), "foo"),
		{
			{
				name: "valid int search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"int_prop"},
					schema.DataTypeInt, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"int_prop"},
			schema.DataTypeInt, allValueTypesExcept(schema.DataTypeInt), "foo"),
		{
			{
				name: "valid int array search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"int_array_prop"},
					schema.DataTypeInt, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"int_array_prop"},
			schema.DataTypeIntArray, allValueTypesExcept(schema.DataTypeInt), "foo"),
		{
			{
				name: "valid boolean search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"boolean_prop"},
					schema.DataTypeBoolean, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"boolean_prop"},
			schema.DataTypeBoolean, allValueTypesExcept(schema.DataTypeBoolean), "foo"),
		{
			{
				name: "valid boolean array search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"boolean_array_prop"},
					schema.DataTypeBoolean, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"boolean_array_prop"},
			schema.DataTypeBooleanArray, allValueTypesExcept(schema.DataTypeBoolean), "foo"),
		{
			{
				name: "valid date search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"date_prop"},
					schema.DataTypeDate, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"date_prop"},
			schema.DataTypeDate, allValueTypesExcept(schema.DataTypeDate), "foo"),
		{
			{
				name: "valid date array search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"date_array_prop"},
					schema.DataTypeDate, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"date_array_prop"},
			schema.DataTypeDateArray, allValueTypesExcept(schema.DataTypeDate), "foo"),
		{
			{
				name: "valid geoCoordinates search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"geo_prop"},
					schema.DataTypeGeoCoordinates, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"geo_prop"},
			schema.DataTypeGeoCoordinates, allValueTypesExcept(schema.DataTypeGeoCoordinates), "foo"),
		{
			{
				name: "valid phoneNumber search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"phone_prop"},
					schema.DataTypePhoneNumber, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"phone_prop"},
			schema.DataTypePhoneNumber, allValueTypesExcept(schema.DataTypePhoneNumber), "foo"),

		// nested filters
		{
			{
				name: "valid nested filter",
				filters: buildNestedFilter(filters.OperatorAnd,
					buildFilter(filters.OperatorEqual, []interface{}{"text_prop"},
						schema.DataTypeText, "foo"),
					buildFilter(filters.OperatorEqual, []interface{}{"int_prop"},
						schema.DataTypeInt, "foo"),
				),
				expectedError: nil,
			},
		},
		buildInvalidNestedTests(filters.OperatorEqual, []interface{}{"text_prop"},
			schema.DataTypeText, allValueTypesExcept(schema.DataTypeText, schema.DataTypeString), "foo"),

		// cross-ref filters
		{
			{
				name: "valid ref filter",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"ref_prop", "ClassTwo", "text_prop"},
					schema.DataTypeText, "foo"),
				expectedError: nil,
			},
		},
		buildInvalidTests(filters.OperatorEqual, []interface{}{"text_prop", "ClassTwo", "text_prop"},
			schema.DataTypeText, allValueTypesExcept(schema.DataTypeText, schema.DataTypeString), "foo"),
		{
			{
				name: "invalid ref filter, due to non-existing class",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"ref_prop", "ClassThree", "text_prop"},
					schema.DataTypeText, "foo"),
				expectedError: errors.Errorf("invalid 'where' filter: class " +
					"\"ClassThree\" does not exist in schema"),
			},
			{
				name: "invalid ref filter, due to non-existing prop on ref",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"ref_prop", "ClassTwo", "invalid_prop"},
					schema.DataTypeText, "foo"),
				expectedError: errors.Errorf("invalid 'where' filter: no such prop with name 'invalid_prop' " +
					"found in class 'ClassTwo' " +
					"in the schema. Check your schema files for which properties in this class are available"),
			},
		},
		{
			{
				name: "counting ref props",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"ref_prop"},
					schema.DataTypeInt, "foo"),
				expectedError: nil,
			},
		},

		// special case, trying to use filters on a ref prop directly
		buildInvalidRefCountTests(filters.OperatorEqual, []interface{}{"ref_prop"},
			schema.DataTypeInt, allValueTypesExcept(schema.DataTypeInt), "foo"),

		// id filters
		{
			{
				name: "filter by id",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"id"},
					schema.DataTypeText, "foo"),
				expectedError: nil,
			},
			{
				name: "filter by id with wrong type",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"id"},
					schema.DataTypeInt, "foo"),
				expectedError: errors.Errorf(
					"invalid 'where' filter: using [\"_id\"] to filter by uuid: " +
						"must use \"valueText\" to specify the id"),
			},
		},

		// string and stringArray are deprecated as of v1.19
		// however they are allowed in filters and considered aliases
		// for text and textArray
		{
			{
				name: "[deprecated string] valid text search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"text_prop"},
					schema.DataTypeString, "foo"),
				expectedError: nil,
			},
			{
				name: "[deprecated string] valid text array search",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"text_array_prop"},
					schema.DataTypeString, "foo"),
				expectedError: nil,
			},
			{
				name: "[deprecated string] valid nested filter",
				filters: buildNestedFilter(filters.OperatorAnd,
					buildFilter(filters.OperatorEqual, []interface{}{"text_prop"},
						schema.DataTypeString, "foo"),
					buildFilter(filters.OperatorEqual, []interface{}{"int_prop"},
						schema.DataTypeInt, "foo"),
				),
				expectedError: nil,
			},
			{
				name: "[deprecated string] valid ref filter",
				filters: buildFilter(filters.OperatorEqual, []interface{}{"ref_prop", "ClassTwo", "text_prop"},
					schema.DataTypeString, "foo"),
				expectedError: nil,
			},
		},
	}

	for _, outertest := range tests {
		for _, test := range outertest {
			t.Run(test.name, func(t *testing.T) {
				params := dto.GetParams{
					ClassName: "ClassOne",
					NearVector: &searchparams.NearVector{
						Vector: []float32{0.8, 0.2, 0.7},
					},
					Pagination: &filters.Pagination{Limit: 100},
					Filters:    test.filters,
				}

				searchResults := []search.Result{
					{
						ID: "id1",
						Schema: map[string]interface{}{
							"name": "Foo",
						},
					},
				}

				search := &fakeVectorSearcher{}
				sg := &fakeSchemaGetter{
					schema: schemaForFiltersValidation(),
				}
				metrics := &fakeMetrics{}
				metrics.On("AddUsageDimensions", mock.Anything, mock.Anything, mock.Anything,
					mock.Anything)
				explorer := NewExplorer(search, log, getFakeModulesProvider(), metrics, defaultConfig)
				explorer.SetSchemaGetter(sg)

				if test.expectedError == nil {
					search.
						On("VectorSearch", mock.Anything).
						Return(searchResults, nil)

					res, err := explorer.GetClass(context.Background(), params)

					t.Run("vector search must be called with right params", func(t *testing.T) {
						assert.Nil(t, err)
						search.AssertExpectations(t)
					})

					t.Run("response must contain concepts", func(t *testing.T) {
						require.Len(t, res, 1)
						assert.Equal(t,
							map[string]interface{}{
								"name": "Foo",
							}, res[0])
					})
				} else {
					_, err := explorer.GetClass(context.Background(), params)
					require.NotNil(t, err)
					assert.Equal(t, test.expectedError.Error(), err.Error())
				}
			})
		}
	}
}

// produces two classes including a cross-ref between them. Contains all
// possible prop types.
func schemaForFiltersValidation() schema.Schema {
	return schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class: "ClassOne",
					Properties: []*models.Property{
						{
							Name:     "text_prop",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "text_array_prop",
							DataType: schema.DataTypeTextArray.PropString(),
						},
						{
							Name:     "number_prop",
							DataType: []string{string(schema.DataTypeNumber)},
						},
						{
							Name:     "int_prop",
							DataType: []string{string(schema.DataTypeInt)},
						},
						{
							Name:     "number_array_prop",
							DataType: []string{string(schema.DataTypeNumberArray)},
						},
						{
							Name:     "int_array_prop",
							DataType: []string{string(schema.DataTypeIntArray)},
						},
						{
							Name:     "boolean_prop",
							DataType: []string{string(schema.DataTypeBoolean)},
						},
						{
							Name:     "boolean_array_prop",
							DataType: []string{string(schema.DataTypeBooleanArray)},
						},
						{
							Name:     "date_prop",
							DataType: []string{string(schema.DataTypeDate)},
						},
						{
							Name:     "date_array_prop",
							DataType: []string{string(schema.DataTypeDateArray)},
						},
						{
							Name:     "blob_prop",
							DataType: []string{string(schema.DataTypeBlob)},
						},
						{
							Name:     "geo_prop",
							DataType: []string{string(schema.DataTypeGeoCoordinates)},
						},
						{
							Name:     "phone_prop",
							DataType: []string{string(schema.DataTypePhoneNumber)},
						},
						{
							Name:     "ref_prop",
							DataType: []string{"ClassTwo"},
						},
					},
				},
				{
					Class: "ClassTwo",
					Properties: []*models.Property{
						{
							Name:     "text_prop",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
			},
		},
	}
}

func buildFilter(op filters.Operator, path []interface{}, dataType schema.DataType,
	value interface{},
) *filters.LocalFilter {
	pathParsed, err := filters.ParsePath(path, "ClassOne")
	if err != nil {
		panic(err)
	}
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: op,
			On:       pathParsed,
			Value: &filters.Value{
				Value: value,
				Type:  dataType,
			},
		},
	}
}

func buildNestedFilter(op filters.Operator,
	childFilters ...*filters.LocalFilter,
) *filters.LocalFilter {
	out := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: op,
			Operands: make([]filters.Clause, len(childFilters)),
		},
	}

	for i, child := range childFilters {
		out.Root.Operands[i] = *child.Root
	}

	return out
}

func allValueTypesExcept(except ...schema.DataType) []schema.DataType {
	all := []schema.DataType{
		schema.DataTypeString,
		schema.DataTypeText,
		schema.DataTypeInt,
		schema.DataTypeNumber,
		schema.DataTypeGeoCoordinates,
		schema.DataTypePhoneNumber,
		schema.DataTypeBoolean,
		schema.DataTypeDate,
	}

	out := make([]schema.DataType, 0, len(all))

	i := 0
outer:
	for _, dt := range all {
		for _, exc := range except {
			if dt == exc {
				continue outer
			}
		}
		out = append(out, dt)
		i++
	}

	return out[:i]
}
