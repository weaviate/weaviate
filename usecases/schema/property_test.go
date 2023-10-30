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

/// TODO-RAFT START
// Fix Unit Tests
// With property-related transactions refactored into property.go, we need to re-implement the following test cases:
// - Adding new property (previously in add_property_test.go)
// - Updating existing property
// - Validation (previously in remove_duplicate_props_test.go)
// Please consult the previous implementation's test files for reference.
/// TODO-RAFT END

func TestHandler_AddProperty(t *testing.T) {
	ctx := context.Background()

	t.Run("adds property of each data type", func(t *testing.T) {
		handler, shutdown := newTestHandler(t, &fakeDB{})
		defer func() {
			future := shutdown()
			require.NoError(t, future.Error())
		}()

		class := models.Class{
			Class:      "NewClass",
			Vectorizer: "none",
		}
		require.NoError(t, handler.AddClass(ctx, nil, &class))

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
			schema.DataTypeObject,
			schema.DataTypeObjectArray,
			schema.DataTypeBlob,
			schema.DataTypeGeoCoordinates,
			schema.DataTypePhoneNumber,

			schema.DataTypeString,
			schema.DataTypeStringArray,
		}

		t.Run("adds properties", func(t *testing.T) {
			for _, dt := range dataTypes {
				t.Run(dt.AsName(), func(t *testing.T) {
					err := handler.AddClassProperty(ctx, nil, class.Class,
						&models.Property{
							Name:     dt.AsName(),
							DataType: dt.PropString(),
						})
					require.NoError(t, err)
				})
			}
		})

		t.Run("verify properties exist in schema", func(t *testing.T) {
			properties := handler.GetSchemaSkipAuth().Objects.Classes[0].Properties
			for i, dt := range dataTypes {
				t.Run(dt.AsName(), func(t *testing.T) {
					var expectedDt schema.DataType

					switch dt {
					case schema.DataTypeString:
						expectedDt = schema.DataTypeText
					case schema.DataTypeStringArray:
						expectedDt = schema.DataTypeTextArray
					default:
						expectedDt = dt
					}

					require.Equal(t, dt.AsName(), properties[i].Name)
					require.Equal(t, expectedDt.PropString(), properties[i].DataType)
				})
			}
		})
	})

	t.Run("fails adding property of existing name", func(t *testing.T) {
		handler, shutdown := newTestHandler(t, &fakeDB{})
		defer func() {
			future := shutdown()
			require.NoError(t, future.Error())
		}()

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
			Vectorizer: "none",
		}
		require.NoError(t, handler.AddClass(ctx, nil, &class))

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
					err := handler.AddClassProperty(ctx, nil, class.Class,
						&models.Property{
							Name:     propName,
							DataType: schema.DataTypeInt.PropString(),
						})
					require.ErrorContains(t, err, "conflict for property")
					require.ErrorContains(t, err, "already in use or provided multiple times")
				})
			}
		})

		t.Run("verify properties do not exist in schema", func(t *testing.T) {
			properties := handler.GetSchemaSkipAuth().Objects.Classes[0].Properties
			require.Len(t, properties, 2)

			myProp := properties[0]
			require.Equal(t, "my_prop", myProp.Name)
			require.Equal(t, schema.DataTypeText.PropString(), myProp.DataType)

			otherProp := properties[1]
			require.Equal(t, "otherProp", otherProp.Name)
			require.Equal(t, schema.DataTypeText.PropString(), otherProp.DataType)
		})
	})
}

func TestHandler_AddProperty_Tokenization(t *testing.T) {
	ctx := context.Background()

	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		future := shutdown()
		require.NoError(t, future.Error())
	}()

	class := models.Class{
		Class:      "NewClass",
		Vectorizer: "none",
	}
	require.NoError(t, handler.AddClass(ctx, nil, &class))

	refClass := models.Class{
		Class:      "RefClass",
		Vectorizer: "none",
	}
	require.NoError(t, handler.AddClass(ctx, nil, &refClass))

	type testCase struct {
		dataType             schema.DataType
		tokenization         string
		expectedTokenization string
		expectedErrContains  []string
	}

	runTestCases := func(t *testing.T, testCases []testCase) {
		for _, tc := range testCases {
			strTokenization := "empty"
			if tc.tokenization != "" {
				strTokenization = tc.tokenization
			}
			propName := fmt.Sprintf("%s_%s", tc.dataType.AsName(), strTokenization)

			t.Run(propName, func(t *testing.T) {
				err := handler.AddClassProperty(ctx, nil, class.Class,
					&models.Property{
						Name:         propName,
						DataType:     tc.dataType.PropString(),
						Tokenization: tc.tokenization,
					})

				if len(tc.expectedErrContains) == 0 {
					require.NoError(t, err)

					classes := handler.GetSchemaSkipAuth().Objects.Classes
					found := false
					for _, c := range classes {
						if c.Class == class.Class {
							found = true
							property := c.Properties[len(c.Properties)-1]
							assert.Equal(t, propName, property.Name)
							assert.Equal(t, tc.expectedTokenization, property.Tokenization)
							break
						}
					}
					require.Truef(t, found, "class %s not found in schema", class.Class)
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
					"Tokenization 'nonExistent' is not allowed for data type",
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
					"Tokenization 'nonExistent' is not allowed for data type",
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
						"Tokenization is not allowed for data type",
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
					"Tokenization is not allowed for data type",
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
						// TODO should be changed as on master
						"Tokenization is not allowed for reference data type",
						// "Tokenization is not allowed for object/object[] data types",
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
					// TODO should be changed as on master
					"Tokenization is not allowed for reference data type",
					// "Tokenization is not allowed for object/object[] data types",
				},
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("reference", func(t *testing.T) {
		dataType := []string{refClass.Class}

		// all tokenizations
		for _, tokenization := range helpers.Tokenizations {
			propName := fmt.Sprintf("ref_%s", tokenization)
			t.Run(propName, func(t *testing.T) {
				err := handler.AddClassProperty(ctx, nil, class.Class,
					&models.Property{
						Name:         propName,
						DataType:     dataType,
						Tokenization: tokenization,
					})

				assert.ErrorContains(t, err, "Tokenization is not allowed for reference data type")
			})
		}

		// empty tokenization
		propName := "ref_empty"
		t.Run(propName, func(t *testing.T) {
			err := handler.AddClassProperty(ctx, nil, class.Class,
				&models.Property{
					Name:         propName,
					DataType:     dataType,
					Tokenization: "",
				})

			require.NoError(t, err)

			classes := handler.GetSchemaSkipAuth().Objects.Classes
			found := false
			for _, c := range classes {
				if c.Class == class.Class {
					found = true
					property := c.Properties[len(c.Properties)-1]
					assert.Equal(t, propName, property.Name)
					assert.Equal(t, "", property.Tokenization)
					break
				}
			}
			require.Truef(t, found, "class %s not found in schema", class.Class)
		})

		// non-existent tokenization
		propName = "ref_nonExistent"
		t.Run(propName, func(t *testing.T) {
			err := handler.AddClassProperty(ctx, nil, class.Class,
				&models.Property{
					Name:         propName,
					DataType:     dataType,
					Tokenization: "nonExistent",
				})

			assert.ErrorContains(t, err, "Tokenization is not allowed for reference data type")
		})
	})
}

func Test_Validation_PropertyTokenization(t *testing.T) {
	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		future := shutdown()
		require.NoError(t, future.Error())
	}()

	type testCase struct {
		name                string
		tokenization        string
		propertyDataType    schema.PropertyDataType
		expectedErrContains []string
	}

	runTestCases := func(t *testing.T, testCases []testCase) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := handler.validatePropertyTokenization(tc.tokenization, tc.propertyDataType)
				if len(tc.expectedErrContains) == 0 {
					assert.NoError(t, err)
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
					name:             fmt.Sprintf("%s_%s", dataType.AsName(), tokenization),
					propertyDataType: newFakePrimitivePDT(dataType),
					tokenization:     tokenization,
				})
			}

			// empty tokenization
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%s_empty", dataType.AsName()),
				propertyDataType: newFakePrimitivePDT(dataType),
				tokenization:     "",
				expectedErrContains: []string{
					"Tokenization '' is not allowed for data type",
					dataType.String(),
				},
			})

			// non-existent tokenization
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%s_nonExistent", dataType.AsName()),
				propertyDataType: newFakePrimitivePDT(dataType),
				tokenization:     "nonExistent",
				expectedErrContains: []string{
					"Tokenization 'nonExistent' is not allowed for data type",
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
				case models.PropertyTokenizationWord, models.PropertyTokenizationField:
					testCases = append(testCases, testCase{
						name:             fmt.Sprintf("%s_%s", dataType.AsName(), tokenization),
						propertyDataType: newFakePrimitivePDT(dataType),
						tokenization:     tokenization,
					})
				default:
					testCases = append(testCases, testCase{
						name:             fmt.Sprintf("%s_%s", dataType.AsName(), tokenization),
						propertyDataType: newFakePrimitivePDT(dataType),
						tokenization:     tokenization,
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
				name:             fmt.Sprintf("%s_empty", dataType.AsName()),
				propertyDataType: newFakePrimitivePDT(dataType),
				tokenization:     "",
				expectedErrContains: []string{
					"Tokenization '' is not allowed for data type",
					dataType.String(),
				},
			})

			// non-existent tokenization
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%s_nonExistent", dataType.AsName()),
				propertyDataType: newFakePrimitivePDT(dataType),
				tokenization:     "nonExistent",
				expectedErrContains: []string{
					"Tokenization 'nonExistent' is not allowed for data type",
					dataType.String(),
				},
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("non text/text[]", func(t *testing.T) {
		dataTypes := []schema.DataType{}
		for _, dataType := range schema.PrimitiveDataTypes {
			switch dataType {
			case schema.DataTypeText, schema.DataTypeTextArray:
				// skip
			default:
				dataTypes = append(dataTypes, dataType)
			}
		}

		testCases := []testCase{}
		for _, dataType := range dataTypes {
			// all tokenizations
			for _, tokenization := range helpers.Tokenizations {
				testCases = append(testCases, testCase{
					name:             fmt.Sprintf("%s_%s", dataType.AsName(), tokenization),
					propertyDataType: newFakePrimitivePDT(dataType),
					tokenization:     tokenization,
					expectedErrContains: []string{
						"Tokenization is not allowed for data type",
						dataType.String(),
					},
				})
			}

			// empty tokenization
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%s_empty", dataType.AsName()),
				propertyDataType: newFakePrimitivePDT(dataType),
				tokenization:     "",
			})

			// non-existent tokenization
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%s_nonExistent", dataType.AsName()),
				propertyDataType: newFakePrimitivePDT(dataType),
				tokenization:     "nonExistent",
				expectedErrContains: []string{
					"Tokenization is not allowed for data type",
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
					name:             fmt.Sprintf("%s_%s", dataType.AsName(), tokenization),
					propertyDataType: newFakeNestedPDT(dataType),
					tokenization:     tokenization,
					expectedErrContains: []string{
						// TODO should be changed as on master
						"Tokenization is not allowed for reference data type",
						// "Tokenization is not allowed for object/object[] data types",
					},
				})
			}

			// empty tokenization
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%s_empty", dataType.AsName()),
				propertyDataType: newFakeNestedPDT(dataType),
				tokenization:     "",
			})

			// non-existent tokenization
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("%s_nonExistent", dataType.AsName()),
				propertyDataType: newFakeNestedPDT(dataType),
				tokenization:     "nonExistent",
				expectedErrContains: []string{
					// TODO should be changed as on master
					"Tokenization is not allowed for reference data type",
					// "Tokenization is not allowed for object/object[] data types",
				},
			})
		}

		runTestCases(t, testCases)
	})

	t.Run("reference", func(t *testing.T) {
		testCases := []testCase{}

		// all tokenizations
		for _, tokenization := range helpers.Tokenizations {
			testCases = append(testCases, testCase{
				name:             fmt.Sprintf("ref_%s", tokenization),
				propertyDataType: newFakePrimitivePDT(""),
				tokenization:     tokenization,
				expectedErrContains: []string{
					"Tokenization is not allowed for reference data type",
				},
			})
		}

		// empty tokenization
		testCases = append(testCases, testCase{
			name:             "ref_empty",
			propertyDataType: newFakePrimitivePDT(""),
			tokenization:     "",
		})

		// non-existent tokenization
		testCases = append(testCases, testCase{
			name:             "ref_nonExistent",
			propertyDataType: newFakePrimitivePDT(""),
			tokenization:     "nonExistent",
			expectedErrContains: []string{
				"Tokenization is not allowed for reference data type",
			},
		})

		runTestCases(t, testCases)
	})
}

func Test_Validation_PropertyIndexing(t *testing.T) {
	vFalse := false
	vTrue := true

	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		future := shutdown()
		require.NoError(t, future.Error())
	}()

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
									"`indexInverted` is deprecated and can not be set together with `indexFilterable` or `indexSearchable`",
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

// TODO: Fix me
//func Test_Validation_NestedProperties(t *testing.T) {
//	vFalse := false
//	vTrue := true
//
//	t.Run("does not validate wrong names", func(t *testing.T) {
//		for _, name := range []string{"prop@1", "prop-2", "prop$3", "4prop"} {
//			t.Run(name, func(t *testing.T) {
//				nestedProperties := []*models.NestedProperty{
//					{
//						Name:            name,
//						DataType:        schema.DataTypeInt.PropString(),
//						IndexFilterable: &vFalse,
//						IndexSearchable: &vFalse,
//						Tokenization:    "",
//					},
//				}
//
//				for _, ndt := range schema.NestedDataTypes {
//					t.Run(ndt.String(), func(t *testing.T) {
//						propPrimitives := &models.Property{
//							Name:             "objectProp",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						}
//						propLvl2Primitives := &models.Property{
//							Name:            "objectPropLvl2",
//							DataType:        ndt.PropString(),
//							IndexFilterable: &vFalse,
//							IndexSearchable: &vFalse,
//							Tokenization:    "",
//							NestedProperties: []*models.NestedProperty{
//								{
//									Name:             "nested_object",
//									DataType:         ndt.PropString(),
//									IndexFilterable:  &vFalse,
//									IndexSearchable:  &vFalse,
//									Tokenization:     "",
//									NestedProperties: nestedProperties,
//								},
//							},
//						}
//
//						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//							t.Run(prop.Name, func(t *testing.T) {
//								err := validateNestedProperties(prop.NestedProperties, prop.Name)
//								assert.ErrorContains(t, err, prop.Name)
//								assert.ErrorContains(t, err, "is not a valid nested property name")
//							})
//						}
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("validates primitive data types", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{}
//		for _, pdt := range schema.PrimitiveDataTypes {
//			tokenization := ""
//			switch pdt {
//			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
//				// skip - not supported as nested
//				continue
//			case schema.DataTypeText, schema.DataTypeTextArray:
//				tokenization = models.PropertyTokenizationWord
//			default:
//				// do nothing
//			}
//
//			nestedProperties = append(nestedProperties, &models.NestedProperty{
//				Name:            "nested_" + pdt.AsName(),
//				DataType:        pdt.PropString(),
//				IndexFilterable: &vFalse,
//				IndexSearchable: &vFalse,
//				Tokenization:    tokenization,
//			})
//		}
//
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propPrimitives := &models.Property{
//					Name:             "objectProp",
//					DataType:         ndt.PropString(),
//					IndexFilterable:  &vFalse,
//					IndexSearchable:  &vFalse,
//					Tokenization:     "",
//					NestedProperties: nestedProperties,
//				}
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:             "nested_object",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.NoError(t, err)
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate deprecated primitive types", func(t *testing.T) {
//		for _, pdt := range schema.DeprecatedPrimitiveDataTypes {
//			t.Run(pdt.String(), func(t *testing.T) {
//				nestedProperties := []*models.NestedProperty{
//					{
//						Name:            "nested_" + pdt.AsName(),
//						DataType:        pdt.PropString(),
//						IndexFilterable: &vFalse,
//						IndexSearchable: &vFalse,
//						Tokenization:    "",
//					},
//				}
//
//				for _, ndt := range schema.NestedDataTypes {
//					t.Run(ndt.String(), func(t *testing.T) {
//						propPrimitives := &models.Property{
//							Name:             "objectProp",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						}
//						propLvl2Primitives := &models.Property{
//							Name:            "objectPropLvl2",
//							DataType:        ndt.PropString(),
//							IndexFilterable: &vFalse,
//							IndexSearchable: &vFalse,
//							Tokenization:    "",
//							NestedProperties: []*models.NestedProperty{
//								{
//									Name:             "nested_object",
//									DataType:         ndt.PropString(),
//									IndexFilterable:  &vFalse,
//									IndexSearchable:  &vFalse,
//									Tokenization:     "",
//									NestedProperties: nestedProperties,
//								},
//							},
//						}
//
//						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//							t.Run(prop.Name, func(t *testing.T) {
//								err := validateNestedProperties(prop.NestedProperties, prop.Name)
//								assert.ErrorContains(t, err, prop.Name)
//								assert.ErrorContains(t, err, fmt.Sprintf("data type '%s' is deprecated and not allowed as nested property", pdt.String()))
//							})
//						}
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate unsupported primitive types", func(t *testing.T) {
//		for _, pdt := range []schema.DataType{schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber} {
//			t.Run(pdt.String(), func(t *testing.T) {
//				nestedProperties := []*models.NestedProperty{
//					{
//						Name:            "nested_" + pdt.AsName(),
//						DataType:        pdt.PropString(),
//						IndexFilterable: &vFalse,
//						IndexSearchable: &vFalse,
//						Tokenization:    "",
//					},
//				}
//
//				for _, ndt := range schema.NestedDataTypes {
//					t.Run(ndt.String(), func(t *testing.T) {
//						propPrimitives := &models.Property{
//							Name:             "objectProp",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						}
//						propLvl2Primitives := &models.Property{
//							Name:            "objectPropLvl2",
//							DataType:        ndt.PropString(),
//							IndexFilterable: &vFalse,
//							IndexSearchable: &vFalse,
//							Tokenization:    "",
//							NestedProperties: []*models.NestedProperty{
//								{
//									Name:             "nested_object",
//									DataType:         ndt.PropString(),
//									IndexFilterable:  &vFalse,
//									IndexSearchable:  &vFalse,
//									Tokenization:     "",
//									NestedProperties: nestedProperties,
//								},
//							},
//						}
//
//						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//							t.Run(prop.Name, func(t *testing.T) {
//								err := validateNestedProperties(prop.NestedProperties, prop.Name)
//								assert.ErrorContains(t, err, prop.Name)
//								assert.ErrorContains(t, err, fmt.Sprintf("data type '%s' not allowed as nested property", pdt.String()))
//							})
//						}
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate ref types", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{
//			{
//				Name:            "nested_ref",
//				DataType:        []string{"SomeClass"},
//				IndexFilterable: &vFalse,
//				IndexSearchable: &vFalse,
//				Tokenization:    "",
//			},
//		}
//
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propPrimitives := &models.Property{
//					Name:             "objectProp",
//					DataType:         ndt.PropString(),
//					IndexFilterable:  &vFalse,
//					IndexSearchable:  &vFalse,
//					Tokenization:     "",
//					NestedProperties: nestedProperties,
//				}
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:             "nested_object",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.ErrorContains(t, err, prop.Name)
//						assert.ErrorContains(t, err, "reference data type not allowed")
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate empty nested properties", func(t *testing.T) {
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propPrimitives := &models.Property{
//					Name:            "objectProp",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//				}
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:            "nested_object",
//							DataType:        ndt.PropString(),
//							IndexFilterable: &vFalse,
//							IndexSearchable: &vFalse,
//							Tokenization:    "",
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.ErrorContains(t, err, prop.Name)
//						assert.ErrorContains(t, err, "At least one nested property is required for data type object/object[]")
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate tokenization on non text/text[] primitive data types", func(t *testing.T) {
//		for _, pdt := range schema.PrimitiveDataTypes {
//			switch pdt {
//			case schema.DataTypeText, schema.DataTypeTextArray:
//				continue
//			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
//				// skip - not supported as nested
//				continue
//			default:
//				// do nothing
//			}
//
//			t.Run(pdt.String(), func(t *testing.T) {
//				nestedProperties := []*models.NestedProperty{
//					{
//						Name:            "nested_" + pdt.AsName(),
//						DataType:        pdt.PropString(),
//						IndexFilterable: &vFalse,
//						IndexSearchable: &vFalse,
//						Tokenization:    models.PropertyTokenizationWord,
//					},
//				}
//
//				for _, ndt := range schema.NestedDataTypes {
//					t.Run(ndt.String(), func(t *testing.T) {
//						propPrimitives := &models.Property{
//							Name:             "objectProp",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						}
//						propLvl2Primitives := &models.Property{
//							Name:            "objectPropLvl2",
//							DataType:        ndt.PropString(),
//							IndexFilterable: &vFalse,
//							IndexSearchable: &vFalse,
//							Tokenization:    "",
//							NestedProperties: []*models.NestedProperty{
//								{
//									Name:             "nested_object",
//									DataType:         ndt.PropString(),
//									IndexFilterable:  &vFalse,
//									IndexSearchable:  &vFalse,
//									Tokenization:     "",
//									NestedProperties: nestedProperties,
//								},
//							},
//						}
//
//						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//							t.Run(prop.Name, func(t *testing.T) {
//								err := validateNestedProperties(prop.NestedProperties, prop.Name)
//								assert.ErrorContains(t, err, prop.Name)
//								assert.ErrorContains(t, err, fmt.Sprintf("Tokenization is not allowed for data type '%s'", pdt.String()))
//							})
//						}
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate tokenization on nested data types", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{
//			{
//				Name:            "nested_int",
//				DataType:        schema.DataTypeInt.PropString(),
//				IndexFilterable: &vFalse,
//				IndexSearchable: &vFalse,
//				Tokenization:    "",
//			},
//		}
//
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:             "nested_object",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     models.PropertyTokenizationWord,
//							NestedProperties: nestedProperties,
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.ErrorContains(t, err, prop.Name)
//						assert.ErrorContains(t, err, "Tokenization is not allowed for object/object[] data types")
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("validates indexFilterable on primitive data types", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{}
//		for _, pdt := range schema.PrimitiveDataTypes {
//			tokenization := ""
//			switch pdt {
//			case schema.DataTypeBlob:
//				// skip - not indexable
//				continue
//			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
//				// skip - not supported as nested
//				continue
//			case schema.DataTypeText, schema.DataTypeTextArray:
//				tokenization = models.PropertyTokenizationWord
//			default:
//				// do nothing
//			}
//
//			nestedProperties = append(nestedProperties, &models.NestedProperty{
//				Name:            "nested_" + pdt.AsName(),
//				DataType:        pdt.PropString(),
//				IndexFilterable: &vTrue,
//				IndexSearchable: &vFalse,
//				Tokenization:    tokenization,
//			})
//		}
//
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propPrimitives := &models.Property{
//					Name:             "objectProp",
//					DataType:         ndt.PropString(),
//					IndexFilterable:  &vFalse,
//					IndexSearchable:  &vFalse,
//					Tokenization:     "",
//					NestedProperties: nestedProperties,
//				}
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:             "nested_object",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.NoError(t, err)
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate indexFilterable on blob data type", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{
//			{
//				Name:            "nested_blob",
//				DataType:        schema.DataTypeBlob.PropString(),
//				IndexFilterable: &vTrue,
//				IndexSearchable: &vFalse,
//				Tokenization:    "",
//			},
//		}
//
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propPrimitives := &models.Property{
//					Name:             "objectProp",
//					DataType:         ndt.PropString(),
//					IndexFilterable:  &vFalse,
//					IndexSearchable:  &vFalse,
//					Tokenization:     "",
//					NestedProperties: nestedProperties,
//				}
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:             "nested_object",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.ErrorContains(t, err, prop.Name)
//						assert.ErrorContains(t, err, "indexFilterable is not allowed for blob data type")
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("validates indexFilterable on nested data types", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{
//			{
//				Name:            "nested_int",
//				DataType:        schema.DataTypeInt.PropString(),
//				IndexFilterable: &vFalse,
//				IndexSearchable: &vFalse,
//				Tokenization:    "",
//			},
//		}
//
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vTrue,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:             "nested_object",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.NoError(t, err)
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("validates indexSearchable on text/text[] data types", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{}
//		for _, pdt := range []schema.DataType{schema.DataTypeText, schema.DataTypeTextArray} {
//			nestedProperties = append(nestedProperties, &models.NestedProperty{
//				Name:            "nested_" + pdt.AsName(),
//				DataType:        pdt.PropString(),
//				IndexFilterable: &vFalse,
//				IndexSearchable: &vTrue,
//				Tokenization:    models.PropertyTokenizationWord,
//			})
//		}
//
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propPrimitives := &models.Property{
//					Name:             "objectProp",
//					DataType:         ndt.PropString(),
//					IndexFilterable:  &vFalse,
//					IndexSearchable:  &vFalse,
//					Tokenization:     "",
//					NestedProperties: nestedProperties,
//				}
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:             "nested_object",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.NoError(t, err)
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate indexSearchable on primitive data types", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{}
//		for _, pdt := range schema.PrimitiveDataTypes {
//			switch pdt {
//			case schema.DataTypeText, schema.DataTypeTextArray:
//				continue
//			case schema.DataTypeGeoCoordinates, schema.DataTypePhoneNumber:
//				// skip - not supported as nested
//				continue
//			default:
//				// do nothing
//			}
//
//			t.Run(pdt.String(), func(t *testing.T) {
//				nestedProperties = append(nestedProperties, &models.NestedProperty{
//					Name:            "nested_" + pdt.AsName(),
//					DataType:        pdt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vTrue,
//					Tokenization:    "",
//				})
//
//				for _, ndt := range schema.NestedDataTypes {
//					t.Run(ndt.String(), func(t *testing.T) {
//						propPrimitives := &models.Property{
//							Name:             "objectProp",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vFalse,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						}
//						propLvl2Primitives := &models.Property{
//							Name:            "objectPropLvl2",
//							DataType:        ndt.PropString(),
//							IndexFilterable: &vFalse,
//							IndexSearchable: &vFalse,
//							Tokenization:    "",
//							NestedProperties: []*models.NestedProperty{
//								{
//									Name:             "nested_object",
//									DataType:         ndt.PropString(),
//									IndexFilterable:  &vFalse,
//									IndexSearchable:  &vFalse,
//									Tokenization:     "",
//									NestedProperties: nestedProperties,
//								},
//							},
//						}
//
//						for _, prop := range []*models.Property{propPrimitives, propLvl2Primitives} {
//							t.Run(prop.Name, func(t *testing.T) {
//								err := validateNestedProperties(prop.NestedProperties, prop.Name)
//								assert.ErrorContains(t, err, prop.Name)
//								assert.ErrorContains(t, err, "`indexSearchable` is not allowed for other than text/text[] data types")
//							})
//						}
//					})
//				}
//			})
//		}
//	})
//
//	t.Run("does not validate indexSearchable on nested data types", func(t *testing.T) {
//		nestedProperties := []*models.NestedProperty{
//			{
//				Name:            "nested_int",
//				DataType:        schema.DataTypeInt.PropString(),
//				IndexFilterable: &vFalse,
//				IndexSearchable: &vFalse,
//				Tokenization:    "",
//			},
//		}
//
//		for _, ndt := range schema.NestedDataTypes {
//			t.Run(ndt.String(), func(t *testing.T) {
//				propLvl2Primitives := &models.Property{
//					Name:            "objectPropLvl2",
//					DataType:        ndt.PropString(),
//					IndexFilterable: &vFalse,
//					IndexSearchable: &vFalse,
//					Tokenization:    "",
//					NestedProperties: []*models.NestedProperty{
//						{
//							Name:             "nested_object",
//							DataType:         ndt.PropString(),
//							IndexFilterable:  &vFalse,
//							IndexSearchable:  &vTrue,
//							Tokenization:     "",
//							NestedProperties: nestedProperties,
//						},
//					},
//				}
//
//				for _, prop := range []*models.Property{propLvl2Primitives} {
//					t.Run(prop.Name, func(t *testing.T) {
//						err := validateNestedProperties(prop.NestedProperties, prop.Name)
//						assert.ErrorContains(t, err, prop.Name)
//						assert.ErrorContains(t, err, "`indexSearchable` is not allowed for other than text/text[] data types")
//					})
//				}
//			})
//		}
//	})
//}

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
