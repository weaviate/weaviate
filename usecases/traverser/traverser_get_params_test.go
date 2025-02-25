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
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"

	logrus "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestGetParams(t *testing.T) {
	t.Run("without any select properties", func(t *testing.T) {
		sp := search.SelectProperties{}
		assert.Equal(t, false, sp.HasRefs(), "indicates no refs are present")
	})

	t.Run("with only primitive select properties", func(t *testing.T) {
		sp := search.SelectProperties{
			search.SelectProperty{
				IsPrimitive: true,
				Name:        "Foo",
			},
			search.SelectProperty{
				IsPrimitive: true,
				Name:        "Bar",
			},
		}

		assert.Equal(t, false, sp.HasRefs(), "indicates no refs are present")

		resolve, err := sp.ShouldResolve([]string{"inCountry", "Country"})
		require.Nil(t, err)
		assert.Equal(t, false, resolve)
	})

	t.Run("with a ref prop", func(t *testing.T) {
		sp := search.SelectProperties{
			search.SelectProperty{
				IsPrimitive: true,
				Name:        "name",
			},
			search.SelectProperty{
				IsPrimitive: false,
				Name:        "inCity",
				Refs: []search.SelectClass{
					{
						ClassName: "City",
						RefProperties: search.SelectProperties{
							search.SelectProperty{
								Name:        "name",
								IsPrimitive: true,
							},
							search.SelectProperty{
								Name:        "inCountry",
								IsPrimitive: false,
								Refs: []search.SelectClass{
									{
										ClassName: "Country",
										RefProperties: search.SelectProperties{
											search.SelectProperty{
												Name:        "name",
												IsPrimitive: true,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		t.Run("checking for refs", func(t *testing.T) {
			assert.Equal(t, true, sp.HasRefs(), "indicates refs are present")
		})

		t.Run("checking valid single level ref", func(t *testing.T) {
			resolve, err := sp.ShouldResolve([]string{"inCity", "City"})
			require.Nil(t, err)
			assert.Equal(t, true, resolve)
		})

		t.Run("checking invalid single level ref", func(t *testing.T) {
			resolve, err := sp.ShouldResolve([]string{"inCity", "Town"})
			require.Nil(t, err)
			assert.Equal(t, false, resolve)
		})

		t.Run("checking valid nested ref", func(t *testing.T) {
			resolve, err := sp.ShouldResolve([]string{"inCity", "City", "inCountry", "Country"})
			require.Nil(t, err)
			assert.Equal(t, true, resolve)
		})

		t.Run("checking invalid nested level refs", func(t *testing.T) {
			resolve, err := sp.ShouldResolve([]string{"inCity", "Town", "inCountry", "Country"})
			require.Nil(t, err)
			assert.Equal(t, false, resolve)

			resolve, err = sp.ShouldResolve([]string{"inCity", "City", "inCountry", "Land"})
			require.Nil(t, err)
			assert.Equal(t, false, resolve)
		})

		t.Run("selecting a specific prop", func(t *testing.T) {
			prop := sp.FindProperty("inCity")
			assert.Equal(t, prop, &sp[1])
		})
	})
}

func TestGet_NestedRefDepthLimit(t *testing.T) {
	type testcase struct {
		name        string
		props       search.SelectProperties
		maxDepth    int
		expectedErr string
	}

	makeNestedRefProps := func(depth int) search.SelectProperties {
		root := search.SelectProperties{}
		next := &root
		for i := 0; i < depth; i++ {
			*next = append(*next,
				search.SelectProperty{Name: "nextNode"},
				search.SelectProperty{Name: "otherRef"},
			)
			class0 := search.SelectClass{ClassName: "LinkedListNode"}
			refs0 := []search.SelectClass{class0}
			(*next)[0].Refs = refs0
			class1 := search.SelectClass{ClassName: "LinkedListNode"}
			refs1 := []search.SelectClass{class1}
			(*next)[1].Refs = refs1
			next = &refs0[0].RefProperties
		}
		return root
	}

	newTraverser := func(depth int) *Traverser {
		logger, _ := logrus.NewNullLogger()
		schemaGetter := &fakeSchemaGetter{aggregateTestSchema}
		cfg := config.WeaviateConfig{
			Config: config.Config{
				QueryCrossReferenceDepthLimit: depth,
			},
		}
		return NewTraverser(&cfg, logger, mocks.NewMockAuthorizer(),
			&fakeVectorRepo{}, &fakeExplorer{}, schemaGetter, nil, nil, -1)
	}

	tests := []testcase{
		{
			name:     "succeed with explicitly set low depth limit",
			maxDepth: 5,
			props:    makeNestedRefProps(5),
		},
		{
			name:        "fail with explicitly set low depth limit",
			maxDepth:    5,
			props:       makeNestedRefProps(6),
			expectedErr: "nested references depth exceeds QUERY_CROSS_REFERENCE_DEPTH_LIMIT (5)",
		},
		{
			name:     "succeed with explicitly set high depth limit",
			maxDepth: 500,
			props:    makeNestedRefProps(500),
		},
		{
			name:        "fail with explicitly set high depth limit",
			maxDepth:    500,
			props:       makeNestedRefProps(501),
			expectedErr: "nested references depth exceeds QUERY_CROSS_REFERENCE_DEPTH_LIMIT (500)",
		},
		{
			name:        "fail with explicitly set low depth limit, but high actual depth",
			maxDepth:    10,
			props:       makeNestedRefProps(5000),
			expectedErr: "nested references depth exceeds QUERY_CROSS_REFERENCE_DEPTH_LIMIT (10)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.maxDepth == 0 {
				t.Fatalf("must provide maxDepth param for test %q", test.name)
			}
			traverser := newTraverser(test.maxDepth)
			err := traverser.probeForRefDepthLimit(test.props)
			if test.expectedErr != "" {
				assert.EqualError(t, err, test.expectedErr)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func Test_GetClass_WithFilters(t *testing.T) {
	valueNameFromDataType := func(dt schema.DataType) string {
		return "value" + strings.ToUpper(string(dt[0])) + string(dt[1:])
	}
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

	newTraverser := func() *Traverser {
		logger, _ := logrus.NewNullLogger()
		schemaGetter := &fakeSchemaGetter{schemaForFiltersValidation()}
		cfg := config.WeaviateConfig{}
		return NewTraverser(&cfg, logger, mocks.NewMockAuthorizer(),
			&fakeVectorRepo{}, &fakeExplorer{}, schemaGetter, nil, nil, -1)
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
						Vectors: []models.Vector{[]float32{0.8, 0.2, 0.7}},
					},
					Pagination: &filters.Pagination{Limit: 100},
					Filters:    test.filters,
				}

				//searchResults := []search.Result{
				//	{
				//		ID: "id1",
				//		Schema: map[string]interface{}{
				//			"name": "Foo",
				//		},
				//	},
				//}

				// search := &fakeVectorSearcher{}
				metrics := &fakeMetrics{}
				metrics.On("AddUsageDimensions", mock.Anything, mock.Anything, mock.Anything,
					mock.Anything)
				traverser := newTraverser()

				if test.expectedError == nil {
					// search.
					//	On("VectorSearch", mock.Anything, mock.Anything).
					//	Return(searchResults, nil)

					_, err := traverser.GetClass(context.Background(), nil, params)
					assert.Nil(t, err)

				} else {
					_, err := traverser.GetClass(context.Background(), nil, params)
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
