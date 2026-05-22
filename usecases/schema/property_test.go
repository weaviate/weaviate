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

package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/tokenizer"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
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
		fakeSchemaManager.On("QueryCollectionsCount", "").Return(0, nil)
		fakeSchemaManager.On("ReadOnlyClass", class.Class).Return(&class)
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
					_, _, err := handler.AddClassProperty(ctx, nil, class.Class, false, prop)
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
		fakeSchemaManager.On("QueryCollectionsCount", "").Return(0, nil)
		fakeSchemaManager.On("ReadOnlyClass", class.Class).Return(&class)
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
					_, _, err := handler.AddClassProperty(ctx, nil, class.Class, false, prop)
					require.ErrorContains(t, err, "conflict for property")
					require.ErrorContains(t, err, "already in use or provided multiple times")
				})
			}
			fakeSchemaManager.AssertNotCalled(t, "AddProperty", mock.Anything, mock.Anything)
		})
	})
}

func TestHandler_AddProperty_ReservedSuffix(t *testing.T) {
	ctx := context.Background()

	suffixes := []string{
		"foo_searchable",
		"foo_rangeable",
		"foo_temp",
		"foo__meta_count",
		"foo_propertyLength",
		"foo_nullState",
	}

	t.Run("rejects new property with reserved suffix", func(t *testing.T) {
		for _, propName := range suffixes {
			t.Run(propName, func(t *testing.T) {
				handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
				class := &models.Class{
					Class:             "NewClass",
					Vectorizer:        "none",
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				}
				fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
				fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
				_, _, err := handler.AddClass(ctx, nil, class)
				require.NoError(t, err)

				prop := &models.Property{
					Name:     propName,
					DataType: schema.DataTypeText.PropString(),
				}
				_, _, err = handler.AddClassProperty(ctx, nil, class.Class, false, prop)
				require.ErrorContains(t, err, "reserved for internal indices")
				fakeSchemaManager.AssertNotCalled(t, "AddProperty", mock.Anything, mock.Anything)
			})
		}
	})

	t.Run("allows merge upsert of already-existing legacy property", func(t *testing.T) {
		// Simulate a legacy schema that predates the suffix restriction by
		// placing the property directly into class.Properties, bypassing AddClass.
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		legacyProp := &models.Property{
			Name:     "foo_searchable",
			DataType: schema.DataTypeText.PropString(),
		}
		class := &models.Class{
			Class:             "LegacyClass",
			Properties:        []*models.Property{legacyProp},
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}

		// merge=true with a name matching an existing property must skip the
		// suffix check (case-insensitively) so legacy schemas stay upsert-able.
		fakeSchemaManager.On("AddProperty", class.Class, mock.Anything).Return(nil).Maybe()
		upsert := &models.Property{
			Name:     "Foo_searchable",
			DataType: schema.DataTypeText.PropString(),
		}
		_, _, err := handler.AddClassProperty(ctx, nil, class.Class, true, upsert)
		require.NoError(t, err)
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
		fakeSchemaManager.On("QueryCollectionsCount", "").Return(0, nil)
		fakeSchemaManager.On("ReadOnlyClass", class.Class).Return(&class)
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
					_, _, err := handler.AddClassProperty(ctx, nil, class.Class, false, prop)
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
			fakeSchemaManager.On("ReadOnlyClass", class.Class).Return(&class)

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
				fakeSchemaManager.On("ReadOnlyClass", class.Class).Return(&class)
				if len(tc.expectedErrContains) == 0 {
					fakeSchemaManager.On("AddProperty", class.Class, []*models.Property{prop}).Return(nil)
				} else {
					fakeSchemaManager.AssertNotCalled(t, "AddProperty", mock.Anything, mock.Anything)
				}

				_, _, err := handler.AddClassProperty(ctx, nil, class.Class, false, prop)
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
			for _, tokenization := range tokenizer.Tokenizations {
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
			for _, tokenization := range tokenizer.Tokenizations {
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
			for _, tokenization := range tokenizer.Tokenizations {
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
			for _, tokenization := range tokenizer.Tokenizations {
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
	fakeSchemaManager.On("ReadOnlyClass", refClass.Class).Return(&refClass)
	fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil).Twice()
	fakeSchemaManager.On("QueryCollectionsCount", "").Return(0, nil).Twice()
	fakeSchemaManager.On("ReadOnlyClass", class.Class).Return(&class)
	_, _, err := handler.AddClass(ctx, nil, &class)
	require.NoError(t, err)
	_, _, err = handler.AddClass(ctx, nil, &refClass)
	require.NoError(t, err)

	dataType := []string{refClass.Class}

	// all tokenizations
	for _, tokenization := range tokenizer.Tokenizations {
		propName := fmt.Sprintf("ref_%s", tokenization)
		t.Run(propName, func(t *testing.T) {
			_, _, err := handler.AddClassProperty(ctx, nil, class.Class, false,
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
		_, _, err := handler.AddClassProperty(ctx, nil, class.Class, false,
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
		_, _, err := handler.AddClassProperty(ctx, nil, class.Class, false,
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
			for _, tokenization := range tokenizer.Tokenizations {
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

				for _, tokenization := range append(tokenizer.Tokenizations, "non_existing") {
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

			for _, tokenization := range append(tokenizer.Tokenizations, "non_existent") {
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

		for _, tokenization := range append(tokenizer.Tokenizations, "non_existing") {
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
			for _, tokenization := range append(tokenizer.Tokenizations, "non_existing") {
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

func TestHandler_DeleteClassVectorIndex(t *testing.T) {
	t.Setenv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true")
	ctx := context.Background()

	t.Run("class not found returns error", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		fakeSchemaManager.On("QueryReadOnlyClasses", []string{"TestClass"}).
			Return(map[string]versioned.Class{}, nil)

		err := handler.DeleteClassVectorIndex(ctx, nil, "TestClass", "vec1")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("empty vector index name returns error", func(t *testing.T) {
		handler, _ := newTestHandler(t, &fakeDB{})
		err := handler.DeleteClassVectorIndex(ctx, nil, "TestClass", "")
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("class with no vector config returns error", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		fakeSchemaManager.On("QueryReadOnlyClasses", []string{"TestClass"}).
			Return(map[string]versioned.Class{
				"TestClass": {Class: &models.Class{Class: "TestClass"}},
			}, nil)

		err := handler.DeleteClassVectorIndex(ctx, nil, "TestClass", "vec1")
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("non-existent vector index returns error", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		fakeSchemaManager.On("QueryReadOnlyClasses", []string{"TestClass"}).
			Return(map[string]versioned.Class{
				"TestClass": {Class: &models.Class{
					Class: "TestClass",
					VectorConfig: map[string]models.VectorConfig{
						"other": {VectorIndexType: "hnsw"},
					},
				}},
			}, nil)

		err := handler.DeleteClassVectorIndex(ctx, nil, "TestClass", "vec1")
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("already dropped vector index is a no-op", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		fakeSchemaManager.On("QueryReadOnlyClasses", []string{"TestClass"}).
			Return(map[string]versioned.Class{
				"TestClass": {Class: &models.Class{
					Class: "TestClass",
					VectorConfig: map[string]models.VectorConfig{
						"vec1": {VectorIndexType: "none"},
					},
				}},
			}, nil)

		err := handler.DeleteClassVectorIndex(ctx, nil, "TestClass", "vec1")
		require.NoError(t, err)
	})

	t.Run("successful drop sets VectorIndexType to none", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		fakeSchemaManager.On("QueryReadOnlyClasses", []string{"TestClass"}).
			Return(map[string]versioned.Class{
				"TestClass": {Class: &models.Class{
					Class: "TestClass",
					VectorConfig: map[string]models.VectorConfig{
						"vec1": {
							VectorIndexType: "hnsw",
							Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
						},
						"vec2": {
							VectorIndexType: "flat",
							Vectorizer:      map[string]interface{}{"text2vec-contextionary": map[string]interface{}{}},
						},
					},
				}},
			}, nil)
		fakeSchemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)

		err := handler.DeleteClassVectorIndex(ctx, nil, "TestClass", "vec1")
		require.NoError(t, err)

		// Verify UpdateClass was called with vec1 set to "none".
		call := fakeSchemaManager.Calls[len(fakeSchemaManager.Calls)-1]
		updatedClass := call.Arguments[0].(*models.Class)
		require.Equal(t, "none", updatedClass.VectorConfig["vec1"].VectorIndexType)
		require.Equal(t, "flat", updatedClass.VectorConfig["vec2"].VectorIndexType)
		require.NotNil(t, updatedClass.VectorConfig["vec1"].Vectorizer)

		fakeSchemaManager.AssertExpectations(t)
	})
}

func TestAddClassProperty_Namespacing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		enabled      bool
		principal    *models.Principal
		inputName    string
		stored       string // class name present in storage, "" if absent
		wantAuthName string // qualified name authorized + persisted
		wantErrIs    error
	}{
		{
			name:         "namespaced: short input qualifies and authorizes against qualified",
			enabled:      true,
			principal:    namespacedPrincipal("customer1"),
			inputName:    "Movies",
			stored:       "customer1:Movies",
			wantAuthName: "customer1:Movies",
		},
		{
			name:         "global on namespaces enabled: qualified input passes through",
			enabled:      true,
			principal:    globalPrincipal(),
			inputName:    "customer1:Movies",
			stored:       "customer1:Movies",
			wantAuthName: "customer1:Movies",
		},
		{
			name:         "namespaces disabled: input passes through",
			enabled:      false,
			principal:    nil,
			inputName:    "Movies",
			stored:       "Movies",
			wantAuthName: "Movies",
		},
		{
			name:      "namespaced: alias name is not a backdoor (no class at qualified alias name)",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			inputName: "Films",
			stored:    "customer1:Movies",
			wantErrIs: ErrNotFound,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)

			lookup, err := namespacing.QualifyClass(tt.principal, tt.enabled, tt.inputName)
			require.NoError(t, err)
			if lookup == tt.stored {
				sm.On("ReadOnlyClass", lookup).Return(&models.Class{
					Class:             tt.stored,
					Vectorizer:        "none",
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				})
			} else {
				sm.On("ReadOnlyClass", lookup).Return((*models.Class)(nil))
			}
			if tt.wantErrIs == nil {
				sm.On("AddProperty", tt.wantAuthName, mock.Anything).Return(nil)
			}

			prop := &models.Property{Name: "genre", DataType: schema.DataTypeText.PropString()}
			_, _, err = handler.AddClassProperty(context.Background(), tt.principal,
				tt.inputName, false, prop)
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				return
			}
			require.NoError(t, err)
			sm.AssertExpectations(t)
		})
	}
}

func TestDeleteClassPropertyIndex_Namespacing(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		enabled      bool
		principal    *models.Principal
		inputName    string
		stored       string
		wantAuthName string
		wantErrIs    error
	}{
		{
			name:         "namespaced: short input qualifies and authorizes against qualified",
			enabled:      true,
			principal:    namespacedPrincipal("customer1"),
			inputName:    "Movies",
			stored:       "customer1:Movies",
			wantAuthName: "customer1:Movies",
		},
		{
			name:         "global on namespaces enabled: qualified input passes through",
			enabled:      true,
			principal:    globalPrincipal(),
			inputName:    "customer1:Movies",
			stored:       "customer1:Movies",
			wantAuthName: "customer1:Movies",
		},
		{
			name:         "namespaces disabled: input passes through",
			enabled:      false,
			principal:    nil,
			inputName:    "Movies",
			stored:       "Movies",
			wantAuthName: "Movies",
		},
		{
			name:      "namespaced: alias name is not a backdoor (no class at qualified alias name)",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			inputName: "Films",
			stored:    "customer1:Movies",
			wantErrIs: ErrNotFound,
		},
	}

	indexed := true
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)

			lookup, err := namespacing.QualifyClass(tt.principal, tt.enabled, tt.inputName)
			require.NoError(t, err)
			prop := &models.Property{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: &indexed,
			}
			if lookup == tt.stored {
				sm.On("ReadOnlyClass", lookup).Return(&models.Class{
					Class:      tt.stored,
					Vectorizer: "none",
					Properties: []*models.Property{prop},
				})
			} else {
				sm.On("ReadOnlyClass", lookup).Return((*models.Class)(nil))
			}
			if tt.wantErrIs == nil {
				sm.On("UpdateProperty", tt.wantAuthName, mock.Anything, mock.Anything).Return(nil)
			}

			err = handler.DeleteClassPropertyIndex(context.Background(), tt.principal,
				tt.inputName, "title", "filterable")
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				return
			}
			require.NoError(t, err)
			sm.AssertExpectations(t)
		})
	}
}

// TestDeleteClassPropertyIndex_NoLocalMutationOnUpdatePropertyError pins
// the regression fixed in PR https://github.com/weaviate/weaviate/pull/11320 after Copilot's review on
// `cluster/schema/manager.go:520`:
//
// SchemaReader.ReadOnlyClass returns a SHALLOW clone of the live FSM
// class — class.Properties is a slice of pointers to the FSM's actual
// *models.Property structs. If DeleteClassPropertyIndex mutated those
// pointers' fields BEFORE calling UpdateProperty, an apply-time
// rejection (the in-flight-reindex MutationGuard from #218, but also
// any pre-existing rejection like a RAFT timeout or downstream
// validation) would leave the local node's in-memory schema diverged
// from the cluster-wide RAFT state.
//
// The fix: deep-copy the located property struct before mutating its
// IndexFilterable / IndexSearchable / IndexRangeFilters pointers.
//
// This test exercises every index name (filterable / searchable /
// rangeFilters) for both directions of the mutation, and asserts the
// FSM's Property pointer's fields are STILL the original values after
// an UpdateProperty failure.
func TestDeleteClassPropertyIndex_NoLocalMutationOnUpdatePropertyError(t *testing.T) {
	t.Parallel()

	indexNames := []struct {
		indexName   string
		fieldOnProp func(p *models.Property) *bool
	}{
		{"filterable", func(p *models.Property) *bool { return p.IndexFilterable }},
		{"searchable", func(p *models.Property) *bool { return p.IndexSearchable }},
		{"rangeFilters", func(p *models.Property) *bool { return p.IndexRangeFilters }},
	}

	for _, idx := range indexNames {
		t.Run(idx.indexName, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, false)

			trueVal := true
			// Construct the FSM-stored property. All three flags are
			// true so DeleteClassPropertyIndex reaches the mutation
			// branch regardless of which indexName we exercise.
			fsmProp := &models.Property{
				Name:              "title",
				DataType:          schema.DataTypeText.PropString(),
				IndexFilterable:   &trueVal,
				IndexSearchable:   &trueVal,
				IndexRangeFilters: &trueVal,
				Tokenization:      "word",
			}
			fsmClass := &models.Class{
				Class:      "Movies",
				Vectorizer: "none",
				Properties: []*models.Property{fsmProp},
			}
			sm.On("ReadOnlyClass", "Movies").Return(fsmClass)
			// Simulate an apply-time rejection — the
			// MutationGuard's in-flight-reindex error shape.
			sm.On("UpdateProperty", "Movies", mock.Anything, mock.Anything).Return(
				fmt.Errorf("reindex task is in flight on this property"))

			err := handler.DeleteClassPropertyIndex(context.Background(), nil,
				"Movies", "title", idx.indexName)
			require.Error(t, err, "UpdateProperty was mocked to fail")

			// The CRITICAL assertion: after the failed apply, the FSM's
			// Property struct's pointer field for this index name must
			// STILL point at the original true value. Without the
			// defensive copy in DeleteClassPropertyIndex, this would
			// be a pointer to `false` because the mutation would have
			// leaked.
			fsmField := idx.fieldOnProp(fsmProp)
			require.NotNil(t, fsmField, "FSM Property's index pointer must not be nil after rejection")
			require.True(t, *fsmField,
				"FSM Property.%s was mutated to false despite UpdateProperty failing — local FSM diverged from RAFT", idx.indexName)
		})
	}
}

// TestDeleteClassPropertyIndex_FieldMaskScopedToTouchedFlag pins the
// regression caught by
// test/acceptance/alter_schema/delete_property_index_empty_test.go on
// a 3-node cluster after the Copilot defensive-copy fix:
//
// Without a field mask, the RAFT FSM falls back to "replace every
// field" semantics on UpdateProperty. A REST request handled by a
// follower whose local FSM lags one RAFT entry behind the leader will
// read stale `IndexFilterable=true` into its read-modify-write
// payload, then commit a property-replace command that clobbers the
// leader's IndexFilterable=false back to true.
//
// The fix is the field mask — only the flag the REST request touched
// gets merged; the leader's current value of unmasked flags is
// preserved. This test asserts the correct PropertyField* constant is
// forwarded for each of the three index names.
func TestDeleteClassPropertyIndex_FieldMaskScopedToTouchedFlag(t *testing.T) {
	t.Parallel()

	cases := []struct {
		indexName string
		wantField string
		// fsmProp is built per-case because rangeFilters validation
		// requires a numeric data type and forbids the searchable flag,
		// while the text-typed property forbids rangeFilters.
		fsmProp *models.Property
	}{
		{
			indexName: "filterable",
			wantField: command.PropertyFieldIndexFilterable,
			fsmProp: &models.Property{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: boolPtr(true),
				IndexSearchable: boolPtr(true),
				Tokenization:    "word",
			},
		},
		{
			indexName: "searchable",
			wantField: command.PropertyFieldIndexSearchable,
			fsmProp: &models.Property{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				IndexFilterable: boolPtr(true),
				IndexSearchable: boolPtr(true),
				Tokenization:    "word",
			},
		},
		{
			indexName: "rangeFilters",
			wantField: command.PropertyFieldIndexRangeFilters,
			fsmProp: &models.Property{
				Name:              "size",
				DataType:          schema.DataTypeNumber.PropString(),
				IndexFilterable:   boolPtr(true),
				IndexRangeFilters: boolPtr(true),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.indexName, func(t *testing.T) {
			t.Parallel()
			handler, sm := newTestHandlerWithNamespaces(t, false)

			fsmClass := &models.Class{
				Class:      "Movies",
				Vectorizer: "none",
				Properties: []*models.Property{tc.fsmProp},
			}
			sm.On("ReadOnlyClass", "Movies").Return(fsmClass)
			sm.On("UpdateProperty", "Movies", mock.Anything,
				mock.MatchedBy(func(fields []string) bool {
					// Exactly one field tag, exactly the one the
					// REST request touched. Anything else (empty
					// mask → replace-all; multiple fields → could
					// clobber unrelated state) is the regression.
					return len(fields) == 1 && fields[0] == tc.wantField
				}),
			).Return(nil)

			err := handler.DeleteClassPropertyIndex(context.Background(), nil,
				"Movies", tc.fsmProp.Name, tc.indexName)
			require.NoError(t, err)
			sm.AssertExpectations(t)
		})
	}
}

func boolPtr(b bool) *bool { return &b }

func TestDeleteClassVectorIndex_Namespacing(t *testing.T) {
	t.Setenv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true")
	cases := []struct {
		name         string
		enabled      bool
		principal    *models.Principal
		inputName    string
		stored       string
		wantAuthName string
		wantErrIs    error
	}{
		{
			name:         "namespaced: short input qualifies and authorizes against qualified",
			enabled:      true,
			principal:    namespacedPrincipal("customer1"),
			inputName:    "Movies",
			stored:       "customer1:Movies",
			wantAuthName: "customer1:Movies",
		},
		{
			name:         "global on namespaces enabled: qualified input passes through",
			enabled:      true,
			principal:    globalPrincipal(),
			inputName:    "customer1:Movies",
			stored:       "customer1:Movies",
			wantAuthName: "customer1:Movies",
		},
		{
			name:         "namespaces disabled: input passes through",
			enabled:      false,
			principal:    nil,
			inputName:    "Movies",
			stored:       "Movies",
			wantAuthName: "Movies",
		},
		{
			name:      "namespaced: alias name is not a backdoor (no class at qualified alias name)",
			enabled:   true,
			principal: namespacedPrincipal("customer1"),
			inputName: "Films",
			stored:    "customer1:Movies",
			wantErrIs: ErrNotFound,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			handler, sm := newTestHandlerWithNamespaces(t, tt.enabled)

			lookup, err := namespacing.QualifyClass(tt.principal, tt.enabled, tt.inputName)
			require.NoError(t, err)
			storedClass := &models.Class{
				Class: tt.stored,
				VectorConfig: map[string]models.VectorConfig{
					"vec1": {VectorIndexType: "hnsw", Vectorizer: "none"},
				},
			}
			if lookup == tt.stored {
				sm.On("QueryReadOnlyClasses", []string{lookup}).
					Return(map[string]versioned.Class{lookup: {Class: storedClass}}, nil)
			} else {
				sm.On("QueryReadOnlyClasses", []string{lookup}).
					Return(map[string]versioned.Class{}, nil)
			}
			if tt.wantErrIs == nil {
				sm.On("UpdateClass", mock.MatchedBy(func(c *models.Class) bool {
					return c.Class == tt.wantAuthName
				}), mock.Anything).Return(nil)
			}

			err = handler.DeleteClassVectorIndex(context.Background(), tt.principal,
				tt.inputName, "vec1")
			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				return
			}
			require.NoError(t, err)
			sm.AssertExpectations(t)
		})
	}
}
