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

package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestHandler_AddProperty(t *testing.T) {
	ctx := context.Background()

	t.Run("adds property of each data type", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		class := models.Class{
			Class:             "NewClass",
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}
		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		_, _, err := handler.AddClass(ctx, nil, &class)
		require.NoError(t, err)
		dataTypes := []schema.DataType{
			schema.DataTypeInt,
			schema.DataTypeIntArray,
			schema.DataTypeNumber,
			schema.DataTypeNumberArray,
			schema.DataTypeText,
			schema.DataTypeTextArray,
			schema.DataTypeBoolean,
			schema.DataTypeBooleanArray,
			schema.DataTypeDate,
			schema.DataTypeDateArray,
			schema.DataTypeUUID,
			schema.DataTypeUUIDArray,
			schema.DataTypeBlob,
			schema.DataTypeGeoCoordinates,
			schema.DataTypePhoneNumber,
			schema.DataTypeString,
			schema.DataTypeStringArray,
		}

		t.Run("adds properties", func(t *testing.T) {
			for _, dt := range dataTypes {
				t.Run(dt.AsName(), func(t *testing.T) {
					prop := &models.Property{
						Name:     dt.AsName(),
						DataType: dt.PropString(),
					}
					fakeSchemaManager.On("AddProperty", class.Class, []*models.Property{prop}).Return(nil)
					_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false, prop)
					require.NoError(t, err)
				})
			}
			fakeSchemaManager.AssertExpectations(t)
		})
	})

	t.Run("fails adding property of existing name", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		class := models.Class{
			Class: "NewClass",
			Properties: []*models.Property{
				{
					Name:     "my_prop",
					DataType: schema.DataTypeText.PropString(),
				},
				{
					Name:     "otherProp",
					DataType: schema.DataTypeText.PropString(),
				},
			},
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}
		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		_, _, err := handler.AddClass(ctx, nil, &class)
		require.NoError(t, err)

		existingNames := []string{
			"my_prop",   // lowercase, same casing
			"my_Prop",   // lowercase, different casing
			"otherProp", // mixed case, same casing
			"otherprop", // mixed case, all lower
			"OtHerProP", // mixed case, other casing
		}

		t.Run("adding properties", func(t *testing.T) {
			for _, propName := range existingNames {
				t.Run(propName, func(t *testing.T) {
					prop := &models.Property{
						Name:     propName,
						DataType: schema.DataTypeText.PropString(),
					}
					_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false, prop)
					require.ErrorContains(t, err, "conflict for property")
					require.ErrorContains(t, err, "already in use or provided multiple times")
				})
			}
			fakeSchemaManager.AssertNotCalled(t, "AddProperty", mock.Anything, mock.Anything)
		})
	})
}

// TestHandler_AddProperty_Object verifies that we can add properties on class with the Object and ObjectArray type.
// This test is different than TestHandler_AddProperty because Object and ObjectArray require nested properties to be validated.
func TestHandler_AddProperty_Object(t *testing.T) {
	ctx := context.Background()

	t.Run("adds property of each object data type", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		class := models.Class{
			Class:             "NewClass",
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}
		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		_, _, err := handler.AddClass(ctx, nil, &class)
		require.NoError(t, err)
		dataTypes := []schema.DataType{
			schema.DataTypeObject,
			schema.DataTypeObjectArray,
		}

		t.Run("adds properties", func(t *testing.T) {
			for _, dt := range dataTypes {
				t.Run(dt.AsName(), func(t *testing.T) {
					prop := &models.Property{
						Name:             dt.AsName(),
						DataType:         dt.PropString(),
						NestedProperties: []*models.NestedProperty{{Name: "test", DataType: schema.DataTypeInt.PropString()}},
					}
					fakeSchemaManager.On("AddProperty", class.Class, []*models.Property{prop}).Return(nil)
					_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false, prop)
					require.NoError(t, err)
				})
			}
			fakeSchemaManager.AssertExpectations(t)
		})
	})
}

func TestHandler_AddProperty_Tokenization(t *testing.T) {
	ctx := context.Background()

	class := models.Class{
		Class:             "NewClass",
		Vectorizer:        "none",
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}

	type testCase struct {
		dataType             schema.DataType
		tokenization         string
		expectedTokenization string
		expectedErrContains  []string
	}

	runTestCases := func(t *testing.T, testCases []testCase) {
		for _, tc := range testCases {
			// Set up schema independently for each test
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
			fakeSchemaManager.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(&class)

			strTokenization := "empty"
			if tc.tokenization != "" {
				strTokenization = tc.tokenization
			}
			propName := fmt.Sprintf("%s_%s", tc.dataType.AsName(), strTokenization)

			t.Run(propName, func(t *testing.T) {
				prop := &models.Property{
					Name:         propName,
					DataType:     tc.dataType.PropString(),
					Tokenization: tc.tokenization,
				}

				// If the dataType is a nested data type ensure we pass validation by adding a dummy nested property
				if tc.dataType == schema.DataTypeObject || tc.dataType == schema.DataTypeObjectArray {
					prop.NestedProperties = []*models.NestedProperty{{Name: "test", DataType: schema.DataTypeInt.PropString()}}
				}

				// If we expect no error, assert that the call is made with the property, else assert that no call was made to add the
				// property
				fakeSchemaManager.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(&class)
				if len(tc.expectedErrContains) == 0 {
					fakeSchemaManager.On("AddProperty", class.Class, []*models.Property{prop}).Return(nil)
				} else {
					fakeSchemaManager.AssertNotCalled(t, "AddProperty", mock.Anything, mock.Anything)
				}

				_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false, prop)
				if len(tc.expectedErrContains) == 0 {
					require.NoError(t, err)
				} else {
					for i := range tc.expectedErrContains {
						assert.ErrorContains(t, err, tc.expectedErrContains[i])
					}
				}
			})
		}
	}

	t.Run("text/text[]", func(t *testing.T) {
		dataTypes := []schema.DataType{schema.DataTypeText, schema.DataTypeTextArray}

		testCases := []testCase{}
		for _, dataType := range dataTypes {
			// all tokenizations
			for _, tokenization := range helpers.Tokenizations {
				testCases = append(testCases, testCase{
					dataType:             dataType,
					tokenization:         tokenization,
					expectedTokenization: tokenization,
				})
			}

			// empty tokenization
			testCases = append(testCases, testCase{
				dataType:             dataType,
				tokenization:         "",
				expectedTokenization: models.PropertyTokenizationWord,
			})

			// non-existent tokenization
			testCases = append(testCases, testCase{
				dataType:     dataType,
				tokenization: "nonExistent",
				expectedErrContains: []string{
					"tokenization 'nonExistent' is not allowed for data type",
					dataType.String(),
				},
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("[deprecated string] string/string[]", func(t *testing.T) {
		dataTypes := []schema.DataType{schema.DataTypeString, schema.DataTypeStringArray}

		testCases := []testCase{}
		for _, dataType := range dataTypes {
			// all tokenizations
			for _, tokenization := range helpers.Tokenizations {
				switch tokenization {
				case models.PropertyTokenizationWord:
					testCases = append(testCases, testCase{
						dataType:             dataType,
						tokenization:         tokenization,
						expectedTokenization: models.PropertyTokenizationWhitespace,
					})
				case models.PropertyTokenizationField:
					testCases = append(testCases, testCase{
						dataType:             dataType,
						tokenization:         tokenization,
						expectedTokenization: models.PropertyTokenizationField,
					})
				default:
					testCases = append(testCases, testCase{
						dataType:     dataType,
						tokenization: tokenization,
						expectedErrContains: []string{
							"is not allowed for data type",
							tokenization,
							dataType.String(),
						},
					})
				}
			}

			// empty tokenization
			testCases = append(testCases, testCase{
				dataType:             dataType,
				tokenization:         "",
				expectedTokenization: models.PropertyTokenizationWhitespace,
			})

			// non-existent tokenization
			testCases = append(testCases, testCase{
				dataType:     dataType,
				tokenization: "nonExistent",
				expectedErrContains: []string{
					"tokenization 'nonExistent' is not allowed for data type",
					dataType.String(),
				},
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("non text/text[]", func(t *testing.T) {
		dataTypes := []schema.DataType{}
		for _, dt := range schema.PrimitiveDataTypes {
			switch dt {
			case schema.DataTypeText, schema.DataTypeTextArray:
				// skip
			default:
				dataTypes = append(dataTypes, dt)
			}
		}

		testCases := []testCase{}
		for _, dataType := range dataTypes {
			// all tokenizations
			for _, tokenization := range helpers.Tokenizations {
				testCases = append(testCases, testCase{
					dataType:     dataType,
					tokenization: tokenization,
					expectedErrContains: []string{
						"tokenization is not allowed for data type",
						dataType.String(),
					},
				})
			}

			// empty tokenization
			testCases = append(testCases, testCase{
				dataType:             dataType,
				tokenization:         "",
				expectedTokenization: "",
			})

			// non-existent tokenization
			testCases = append(testCases, testCase{
				dataType:     dataType,
				tokenization: "nonExistent",
				expectedErrContains: []string{
					"tokenization is not allowed for data type",
					dataType.String(),
				},
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("object/object[]", func(t *testing.T) {
		dataTypes := schema.NestedDataTypes

		testCases := []testCase{}
		for _, dataType := range dataTypes {
			// all tokenizations
			for _, tokenization := range helpers.Tokenizations {
				testCases = append(testCases, testCase{
					dataType:     dataType,
					tokenization: tokenization,
					expectedErrContains: []string{
						"tokenization is not allowed for object/object[] data types",
					},
				})
			}

			// empty tokenization
			testCases = append(testCases, testCase{
				dataType:             dataType,
				tokenization:         "",
				expectedTokenization: "",
			})

			// non-existent tokenization
			testCases = append(testCases, testCase{
				dataType:     dataType,
				tokenization: "nonExistent",
				expectedErrContains: []string{
					"tokenization is not allowed for object/object[] data types",
				},
			})
		}

		runTestCases(t, testCases)
	})
}

func TestHandler_AddProperty_Reference_Tokenization(t *testing.T) {
	ctx := context.Background()

	handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

	class := models.Class{
		Class:             "NewClass",
		Vectorizer:        "none",
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	refClass := models.Class{
		Class:             "RefClass",
		Vectorizer:        "none",
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	fakeSchemaManager.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(&refClass)
	fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil).Twice()
	fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil).Twice()
	fakeSchemaManager.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(&class)
	_, _, err := handler.AddClass(ctx, nil, &class)
	require.NoError(t, err)
	_, _, err = handler.AddClass(ctx, nil, &refClass)
	require.NoError(t, err)

	dataType := []string{refClass.Class}

	// all tokenizations
	for _, tokenization := range helpers.Tokenizations {
		propName := fmt.Sprintf("ref_%s", tokenization)
		t.Run(propName, func(t *testing.T) {
			_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false,
				&models.Property{
					Name:         propName,
					DataType:     dataType,
					Tokenization: tokenization,
				})

			assert.ErrorContains(t, err, "tokenization is not allowed for reference data type")
		})
	}

	fakeSchemaManager.AssertNotCalled(t, "AddProperty", mock.Anything, mock.Anything)

	// non-existent tokenization
	propName := "ref_nonExistent"
	t.Run(propName, func(t *testing.T) {
		_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false,
			&models.Property{
				Name:         propName,
				DataType:     dataType,
				Tokenization: "nonExistent",
			})

		assert.ErrorContains(t, err, "tokenization is not allowed for reference data type")
	})

	fakeSchemaManager.AssertNotCalled(t, "AddProperty", mock.Anything, mock.Anything)

	// empty tokenization
	propName = "ref_empty"
	t.Run(propName, func(t *testing.T) {
		fakeSchemaManager.On("AddProperty", mock.Anything, mock.Anything).Return(nil)
		_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false,
			&models.Property{
				Name:         propName,
				DataType:     dataType,
				Tokenization: "",
			})

		require.NoError(t, err)
		fakeSchemaManager.AssertExpectations(t)
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
		handler, _ := newTestHandler(t, &fakeDB{})
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := handler.validatePropertyTokenization(tc.tokenization, tc.propertyDataType)
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
					propertyDataType: newFakePrimitivePDT(dataType),
					tokenization:     tokenization,
					expectedErrMsg:   "",
				})
			}

			for _, tokenization := range []string{"non_existing", ""} {
				testCases = append(testCases, testCase{
					name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
					propertyDataType: newFakePrimitivePDT(dataType),
					tokenization:     tokenization,
					expectedErrMsg:   fmt.Sprintf("tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
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
					propertyDataType: newFakePrimitivePDT(dataType),
					tokenization:     "",
					expectedErrMsg:   "",
				})

				for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
					testCases = append(testCases, testCase{
						name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
						propertyDataType: newFakePrimitivePDT(dataType),
						tokenization:     tokenization,
						expectedErrMsg:   fmt.Sprintf("tokenization is not allowed for data type '%s'", dataType),
					})
				}
			}
		}

		runTestCases(t, testCases)
	})

	t.Run("validates nested datatype and all tokenizations", func(t *testing.T) {
		testCases := []testCase{}
		for _, dataType := range schema.NestedDataTypes {
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%s + ''", dataType),
				propertyDataType: newFakeNestedPDT(dataType),
				tokenization:     "",
				expectedErrMsg:   "",
			})

			for _, tokenization := range append(helpers.Tokenizations, "non_existent") {
				testCases = append(testCases, testCase{
					name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
					propertyDataType: newFakeNestedPDT(dataType),
					tokenization:     tokenization,
					expectedErrMsg:   "tokenization is not allowed for object/object[] data types",
				})
			}
		}

		runTestCases(t, testCases)
	})

	t.Run("validates ref datatype (empty) and all tokenizations", func(t *testing.T) {
		testCases := []testCase{}

		testCases = append(testCases, testCase{
			name:             "ref + ''",
			propertyDataType: newFakePrimitivePDT(""),
			tokenization:     "",
			expectedErrMsg:   "",
		})

		for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("ref + '%s'", tokenization),
				propertyDataType: newFakePrimitivePDT(""),
				tokenization:     tokenization,
				expectedErrMsg:   "tokenization is not allowed for reference data type",
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
						propertyDataType: newFakePrimitivePDT(dataType),
						tokenization:     tokenization,
						expectedErrMsg:   "",
					})
				default:
					testCases = append(testCases, testCase{
						name:             fmt.Sprintf("%s + %s", dataType, tokenization),
						propertyDataType: newFakePrimitivePDT(dataType),
						tokenization:     tokenization,
						expectedErrMsg:   fmt.Sprintf("tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
					})
				}
			}
		}

		runTestCases(t, testCases)
	})
}

func Test_Validation_PropertyIndexing(t *testing.T) {
	vFalse := false
	vTrue := true

	handler, _ := newTestHandler(t, &fakeDB{})

	t.Run("validates indexInverted + indexFilterable + indexSearchable combinations", func(t *testing.T) {
		type testCase struct {
			propName            string
			dataType            schema.DataType
			indexInverted       *bool
			indexFilterable     *bool
			indexSearchable     *bool
			expectedErrContains []string
		}

		boolPtrToStr := func(ptr *bool) string {
			if ptr == nil {
				return "nil"
			}
			return fmt.Sprintf("%v", *ptr)
		}

		allBoolPtrs := []*bool{nil, &vFalse, &vTrue}
		dataTypes := append([]schema.DataType{}, schema.PrimitiveDataTypes...)
		dataTypes = append(dataTypes, schema.NestedDataTypes...)

		testCases := []testCase{}
		for _, dataType := range dataTypes {
			for _, inverted := range allBoolPtrs {
				for _, filterable := range allBoolPtrs {
					for _, searchable := range allBoolPtrs {
						propName := fmt.Sprintf("%s_inverted_%s_filterable_%s_searchable_%s",
							dataType.AsName(), boolPtrToStr(inverted), boolPtrToStr(filterable), boolPtrToStr(searchable))

						// inverted can not be set when filterable or/and searchable is already set
						if inverted != nil && (filterable != nil || searchable != nil) {
							testCases = append(testCases, testCase{
								propName:        propName,
								dataType:        dataType,
								indexInverted:   inverted,
								indexFilterable: filterable,
								indexSearchable: searchable,
								expectedErrContains: []string{
									"`indexInverted` is deprecated and can not be set together with `indexFilterable`, `indexSearchable` or `indexRangeFilters`",
								},
							})
							continue
						}
						// searchable=true can be set only for text/text[]
						if searchable != nil && *searchable {
							switch dataType {
							case schema.DataTypeText, schema.DataTypeTextArray:
								// ignore
							default:
								testCases = append(testCases, testCase{
									propName:        propName,
									dataType:        dataType,
									indexInverted:   inverted,
									indexFilterable: filterable,
									indexSearchable: searchable,
									expectedErrContains: []string{
										// TODO should be changed as on master
										"`indexSearchable` is allowed only for text/text[] data types. For other data types set false or leave empty",
										// "`indexSearchable` is not allowed for other than text/text[] data types" ,
									},
								})
								continue
							}
						}

						testCases = append(testCases, testCase{
							propName:        propName,
							dataType:        dataType,
							indexInverted:   inverted,
							indexFilterable: filterable,
							indexSearchable: searchable,
						})
					}
				}
			}
		}

		for _, tc := range testCases {
			t.Run(tc.propName, func(t *testing.T) {
				err := handler.validatePropertyIndexing(&models.Property{
					Name:            tc.propName,
					DataType:        tc.dataType.PropString(),
					IndexInverted:   tc.indexInverted,
					IndexFilterable: tc.indexFilterable,
					IndexSearchable: tc.indexSearchable,
				})

				if len(tc.expectedErrContains) == 0 {
					require.NoError(t, err)
				} else {
					for i := range tc.expectedErrContains {
						assert.ErrorContains(t, err, tc.expectedErrContains[i])
					}
				}
			})
		}
	})
}

type fakePropertyDataType struct {
	primitiveDataType schema.DataType
	nestedDataType    schema.DataType
}

func newFakePrimitivePDT(primitiveDataType schema.DataType) schema.PropertyDataType {
	return &fakePropertyDataType{primitiveDataType: primitiveDataType}
}

func newFakeNestedPDT(nestedDataType schema.DataType) schema.PropertyDataType {
	return &fakePropertyDataType{nestedDataType: nestedDataType}
}

func (pdt *fakePropertyDataType) Kind() schema.PropertyKind {
	if pdt.IsPrimitive() {
		return schema.PropertyKindPrimitive
	}
	if pdt.IsNested() {
		return schema.PropertyKindNested
	}
	return schema.PropertyKindRef
}

func (pdt *fakePropertyDataType) IsPrimitive() bool {
	return pdt.primitiveDataType != ""
}

func (pdt *fakePropertyDataType) AsPrimitive() schema.DataType {
	return pdt.primitiveDataType
}

func (pdt *fakePropertyDataType) IsNested() bool {
	return pdt.nestedDataType != ""
}

func (pdt *fakePropertyDataType) AsNested() schema.DataType {
	return pdt.nestedDataType
}

func (pdt *fakePropertyDataType) IsReference() bool {
	return !(pdt.IsPrimitive() || pdt.IsNested())
}

func (pdt *fakePropertyDataType) Classes() []schema.ClassName {
	if pdt.IsReference() {
		return []schema.ClassName{}
	}
	return nil
}

func (pdt *fakePropertyDataType) ContainsClass(name schema.ClassName) bool {
	return false
}
