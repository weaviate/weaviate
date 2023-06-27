//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func Test_Validation_ClassNames(t *testing.T) {
	type testCase struct {
		input    string
		valid    bool
		storedAs string
		name     string
	}

	// all inputs represent class names (!)
	tests := []testCase{
		// valid names
		{
			name:     "Single uppercase word",
			input:    "Car",
			valid:    true,
			storedAs: "Car",
		},
		{
			name:     "Single lowercase word, stored as uppercase",
			input:    "car",
			valid:    true,
			storedAs: "Car",
		},
		{
			name:  "empty class",
			input: "",
			valid: false,
		},
	}

	t.Run("adding a class", func(t *testing.T) {
		t.Run("different class names without keywords or properties", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "text2vec-contextionary",
						Class:      test.input,
					}

					m := newSchemaManager()
					err := m.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})
			}
		})

		t.Run("different class names with valid keywords", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "text2vec-contextionary",
						Class:      test.input,
					}

					m := newSchemaManager()
					err := m.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(m)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
				})
			}
		})
	})
}

func Test_Validation_PropertyNames(t *testing.T) {
	type testCase struct {
		input    string
		valid    bool
		storedAs string
		name     string
	}

	// for all test cases keep in mind that the word "carrot" is not present in
	// the fake c11y, but every other word is
	//
	// all inputs represent property names (!)
	tests := []testCase{
		// valid names
		{
			name:     "Single uppercase word, stored as lowercase",
			input:    "Brand",
			valid:    true,
			storedAs: "brand",
		},
		{
			name:     "Single lowercase word",
			input:    "brand",
			valid:    true,
			storedAs: "brand",
		},
		{
			name:     "Property with underscores",
			input:    "property_name",
			valid:    true,
			storedAs: "property_name",
		},
		{
			name:     "Property with underscores and numbers",
			input:    "property_name_2",
			valid:    true,
			storedAs: "property_name_2",
		},
		{
			name:     "Property starting with underscores",
			input:    "_property_name",
			valid:    true,
			storedAs: "_property_name",
		},
		{
			name:  "empty prop name",
			input: "",
			valid: false,
		},
		{
			name:  "reserved prop name: id",
			input: "id",
			valid: false,
		},
		{
			name:  "reserved prop name: _id",
			input: "_id",
			valid: false,
		},
		{
			name:  "reserved prop name: _additional",
			input: "_additional",
			valid: false,
		},
	}

	t.Run("when adding a new class", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "text2vec-contextionary",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					m := newSchemaManager()
					err := m.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					schema, _ := m.GetSchema(nil)
					propName := schema.Objects.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "text2vec-contextionary",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					m := newSchemaManager()
					err := m.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					schema, _ := m.GetSchema(nil)
					propName := schema.Objects.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})
	})

	t.Run("when updating an existing class with a new property", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "text2vec-contextionary",
						Class:      "ValidName",
						Properties: []*models.Property{
							{
								Name:     "dummyPropSoWeDontRunIntoAllNoindexedError",
								DataType: schema.DataTypeText.PropString(),
							},
						},
					}

					m := newSchemaManager()
					err := m.AddClass(context.Background(), nil, class)
					require.Nil(t, err)

					property := &models.Property{
						DataType: schema.DataTypeText.PropString(),
						Name:     test.input,
						ModuleConfig: map[string]interface{}{
							"text2vec-contextionary": map[string]interface{}{},
						},
					}
					err = m.AddClassProperty(context.Background(), nil, "ValidName", property)
					t.Log(err)
					require.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					schema, _ := m.GetSchema(nil)
					propName := schema.Objects.Classes[0].Properties[1].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "text2vec-contextionary",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					m := newSchemaManager()
					err := m.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					schema, _ := m.GetSchema(nil)
					propName := schema.Objects.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})
	})
}

func Test_Validation_PropertyTokenization(t *testing.T) {
	type testCase struct {
		name             string
		tokenization     string
		propertyDataType schema.PropertyDataType
		expectedErrMsg   string
	}

	runTestCases := func(t *testing.T, testCases []testCase) {
		m := newSchemaManager()
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := m.validatePropertyTokenization(tc.tokenization, tc.propertyDataType)
				if tc.expectedErrMsg == "" {
					assert.Nil(t, err)
				} else {
					assert.NotNil(t, err)
					assert.EqualError(t, err, tc.expectedErrMsg)
				}
			})
		}
	}

	t.Run("validates text/textArray and all tokenizations", func(t *testing.T) {
		testCases := []testCase{}
		for _, dataType := range []schema.DataType{
			schema.DataTypeText, schema.DataTypeTextArray,
		} {
			for _, tokenization := range helpers.Tokenizations {
				testCases = append(testCases, testCase{
					name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
					propertyDataType: newFakePropertyDataType(dataType),
					tokenization:     tokenization,
					expectedErrMsg:   "",
				})
			}

			for _, tokenization := range []string{"non_existing", ""} {
				testCases = append(testCases, testCase{
					name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
					propertyDataType: newFakePropertyDataType(dataType),
					tokenization:     tokenization,
					expectedErrMsg:   fmt.Sprintf("Tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
				})
			}
		}

		runTestCases(t, testCases)
	})

	t.Run("validates non text/textArray and all tokenizations", func(t *testing.T) {
		testCases := []testCase{}
		for _, dataType := range schema.PrimitiveDataTypes {
			switch dataType {
			case schema.DataTypeText, schema.DataTypeTextArray:
				continue
			default:
				testCases = append(testCases, testCase{
					name:             fmt.Sprintf("%s + ''", dataType),
					propertyDataType: newFakePropertyDataType(dataType),
					tokenization:     "",
					expectedErrMsg:   "",
				})

				for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
					testCases = append(testCases, testCase{
						name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
						propertyDataType: newFakePropertyDataType(dataType),
						tokenization:     tokenization,
						expectedErrMsg:   fmt.Sprintf("Tokenization is not allowed for data type '%s'", dataType),
					})
				}
			}
		}

		runTestCases(t, testCases)
	})

	t.Run("validates ref datatype (empty) and all tokenizations", func(t *testing.T) {
		testCases := []testCase{}

		testCases = append(testCases, testCase{
			name:             "ref + ''",
			propertyDataType: newFakePropertyDataType(""),
			tokenization:     "",
			expectedErrMsg:   "",
		})

		for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("ref + '%s'", tokenization),
				propertyDataType: newFakePropertyDataType(""),
				tokenization:     tokenization,
				expectedErrMsg:   "Tokenization is not allowed for reference data type",
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("[deprecated string] validates string/stringArray and all tokenizations", func(t *testing.T) {
		testCases := []testCase{}
		for _, dataType := range []schema.DataType{
			schema.DataTypeString, schema.DataTypeStringArray,
		} {
			for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
				switch tokenization {
				case models.PropertyTokenizationWord, models.PropertyTokenizationField:
					testCases = append(testCases, testCase{
						name:             fmt.Sprintf("%s + %s", dataType, tokenization),
						propertyDataType: newFakePropertyDataType(dataType),
						tokenization:     tokenization,
						expectedErrMsg:   "",
					})
				default:
					testCases = append(testCases, testCase{
						name:             fmt.Sprintf("%s + %s", dataType, tokenization),
						propertyDataType: newFakePropertyDataType(dataType),
						tokenization:     tokenization,
						expectedErrMsg:   fmt.Sprintf("Tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
					})
				}
			}
		}

		runTestCases(t, testCases)
	})
}

func Test_Validation_PropertyIndexing(t *testing.T) {
	t.Run("validates indexInverted / indexFilterable / indexSearchable combinations", func(t *testing.T) {
		vFalse := false
		vTrue := true
		allBoolPtrs := []*bool{nil, &vFalse, &vTrue}

		type testCase struct {
			propName        string
			dataType        schema.DataType
			indexInverted   *bool
			indexFilterable *bool
			indexSearchable *bool

			expectedErrMsg string
		}

		boolPtrToStr := func(ptr *bool) string {
			if ptr == nil {
				return "nil"
			}
			return fmt.Sprintf("%v", *ptr)
		}
		propName := func(dt schema.DataType, inverted, filterable, searchable *bool) string {
			return fmt.Sprintf("%s_inverted_%s_filterable_%s_searchable_%s",
				dt.String(), boolPtrToStr(inverted), boolPtrToStr(filterable), boolPtrToStr(searchable))
		}

		testCases := []testCase{}

		for _, dataType := range []schema.DataType{schema.DataTypeText, schema.DataTypeInt} {
			for _, inverted := range allBoolPtrs {
				for _, filterable := range allBoolPtrs {
					for _, searchable := range allBoolPtrs {
						if inverted != nil {
							if filterable != nil || searchable != nil {
								testCases = append(testCases, testCase{
									propName:        propName(dataType, inverted, filterable, searchable),
									dataType:        dataType,
									indexInverted:   inverted,
									indexFilterable: filterable,
									indexSearchable: searchable,
									expectedErrMsg:  "`indexInverted` is deprecated and can not be set together with `indexFilterable` or `indexSearchable`.",
								})
								continue
							}
						}

						if searchable != nil && *searchable {
							if dataType != schema.DataTypeText {
								testCases = append(testCases, testCase{
									propName:        propName(dataType, inverted, filterable, searchable),
									dataType:        dataType,
									indexInverted:   inverted,
									indexFilterable: filterable,
									indexSearchable: searchable,
									expectedErrMsg: "`indexSearchable` is allowed only for text/text[] data types. " +
										"For other data types set false or leave empty",
								})
								continue
							}
						}

						testCases = append(testCases, testCase{
							propName:        propName(dataType, inverted, filterable, searchable),
							dataType:        dataType,
							indexInverted:   inverted,
							indexFilterable: filterable,
							indexSearchable: searchable,
							expectedErrMsg:  "",
						})
					}
				}
			}
		}

		mgr := newSchemaManager()
		for _, tc := range testCases {
			t.Run(tc.propName, func(t *testing.T) {
				err := mgr.validatePropertyIndexing(&models.Property{
					Name:            tc.propName,
					DataType:        tc.dataType.PropString(),
					IndexInverted:   tc.indexInverted,
					IndexFilterable: tc.indexFilterable,
					IndexSearchable: tc.indexSearchable,
				})

				if tc.expectedErrMsg != "" {
					require.NotNil(t, err)
					assert.EqualError(t, err, tc.expectedErrMsg)
				} else {
					require.Nil(t, err)
				}
			})
		}
	})
}

type fakePropertyDataType struct {
	primitiveDataType schema.DataType
}

func newFakePropertyDataType(primitiveDataType schema.DataType) schema.PropertyDataType {
	return &fakePropertyDataType{primitiveDataType}
}

func (pdt *fakePropertyDataType) Kind() schema.PropertyKind {
	if pdt.IsPrimitive() {
		return schema.PropertyKindPrimitive
	}
	return schema.PropertyKindRef
}

func (pdt *fakePropertyDataType) IsPrimitive() bool {
	return pdt.primitiveDataType != ""
}

func (pdt *fakePropertyDataType) AsPrimitive() schema.DataType {
	return pdt.primitiveDataType
}

func (pdt *fakePropertyDataType) IsReference() bool {
	return !pdt.IsPrimitive()
}

func (pdt *fakePropertyDataType) Classes() []schema.ClassName {
	if pdt.IsPrimitive() {
		return nil
	}
	return []schema.ClassName{}
}

func (pdt *fakePropertyDataType) ContainsClass(name schema.ClassName) bool {
	return false
}
