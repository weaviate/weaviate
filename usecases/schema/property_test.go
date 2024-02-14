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

/// TODO-RAFT START
// Fix Unit Tests
// With property-related transactions refactored into property.go, we need to re-implement the following test cases:
// - Adding new property (previously in add_property_test.go)
// - Updating existing property
// - Validation (previously in remove_duplicate_props_test.go)
// Please consult the previous implementation's test files for reference.
/// TODO-RAFT END

// TODO: Fix me
//func Test_Validation_PropertyTokenization(t *testing.T) {
//	type testCase struct {
//		name             string
//		tokenization     string
//		propertyDataType schema.PropertyDataType
//		expectedErrMsg   string
//	}
//
//	runTestCases := func(t *testing.T, testCases []testCase) {
//		m := newSchemaManager()
//		for _, tc := range testCases {
//			t.Run(tc.name, func(t *testing.T) {
//				err := m.validatePropertyTokenization(tc.tokenization, tc.propertyDataType)
//				if tc.expectedErrMsg == "" {
//					assert.Nil(t, err)
//				} else {
//					assert.NotNil(t, err)
//					assert.EqualError(t, err, tc.expectedErrMsg)
//				}
//			})
//		}
//	}
//
//	t.Run("validates text/textArray and all tokenizations", func(t *testing.T) {
//		testCases := []testCase{}
//		for _, dataType := range []schema.DataType{
//			schema.DataTypeText, schema.DataTypeTextArray,
//		} {
//			for _, tokenization := range helpers.Tokenizations {
//				testCases = append(testCases, testCase{
//					name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
//					propertyDataType: newFakePrimitivePDT(dataType),
//					tokenization:     tokenization,
//					expectedErrMsg:   "",
//				})
//			}
//
//			for _, tokenization := range []string{"non_existing", ""} {
//				testCases = append(testCases, testCase{
//					name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
//					propertyDataType: newFakePrimitivePDT(dataType),
//					tokenization:     tokenization,
//					expectedErrMsg:   fmt.Sprintf("Tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
//				})
//			}
//		}
//
//		runTestCases(t, testCases)
//	})
//
//	t.Run("validates non text/textArray and all tokenizations", func(t *testing.T) {
//		testCases := []testCase{}
//		for _, dataType := range schema.PrimitiveDataTypes {
//			switch dataType {
//			case schema.DataTypeText, schema.DataTypeTextArray:
//				continue
//			default:
//				testCases = append(testCases, testCase{
//					name:             fmt.Sprintf("%s + ''", dataType),
//					propertyDataType: newFakePrimitivePDT(dataType),
//					tokenization:     "",
//					expectedErrMsg:   "",
//				})
//
//				for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
//					testCases = append(testCases, testCase{
//						name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
//						propertyDataType: newFakePrimitivePDT(dataType),
//						tokenization:     tokenization,
//						expectedErrMsg:   fmt.Sprintf("Tokenization is not allowed for data type '%s'", dataType),
//					})
//				}
//			}
//		}
//
//		runTestCases(t, testCases)
//	})
//
//	t.Run("validates nested datatype and all tokenizations", func(t *testing.T) {
//		testCases := []testCase{}
//		for _, dataType := range schema.NestedDataTypes {
//			testCases = append(testCases, testCase{
//				name:             fmt.Sprintf("%s + ''", dataType),
//				propertyDataType: newFakeNestedPDT(dataType),
//				tokenization:     "",
//				expectedErrMsg:   "",
//			})
//
//			for _, tokenization := range append(helpers.Tokenizations, "non_existent") {
//				testCases = append(testCases, testCase{
//					name:             fmt.Sprintf("%s + '%s'", dataType, tokenization),
//					propertyDataType: newFakeNestedPDT(dataType),
//					tokenization:     tokenization,
//					expectedErrMsg:   "Tokenization is not allowed for object/object[] data types",
//				})
//			}
//		}
//
//		runTestCases(t, testCases)
//	})
//
//	t.Run("validates ref datatype (empty) and all tokenizations", func(t *testing.T) {
//		testCases := []testCase{}
//
//		testCases = append(testCases, testCase{
//			name:             "ref + ''",
//			propertyDataType: newFakePrimitivePDT(""),
//			tokenization:     "",
//			expectedErrMsg:   "",
//		})
//
//		for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
//			testCases = append(testCases, testCase{
//				name:             fmt.Sprintf("ref + '%s'", tokenization),
//				propertyDataType: newFakePrimitivePDT(""),
//				tokenization:     tokenization,
//				expectedErrMsg:   "Tokenization is not allowed for reference data type",
//			})
//		}
//
//		runTestCases(t, testCases)
//	})
//
//	t.Run("[deprecated string] validates string/stringArray and all tokenizations", func(t *testing.T) {
//		testCases := []testCase{}
//		for _, dataType := range []schema.DataType{
//			schema.DataTypeString, schema.DataTypeStringArray,
//		} {
//			for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
//				switch tokenization {
//				case models.PropertyTokenizationWord, models.PropertyTokenizationField:
//					testCases = append(testCases, testCase{
//						name:             fmt.Sprintf("%s + %s", dataType, tokenization),
//						propertyDataType: newFakePrimitivePDT(dataType),
//						tokenization:     tokenization,
//						expectedErrMsg:   "",
//					})
//				default:
//					testCases = append(testCases, testCase{
//						name:             fmt.Sprintf("%s + %s", dataType, tokenization),
//						propertyDataType: newFakePrimitivePDT(dataType),
//						tokenization:     tokenization,
//						expectedErrMsg:   fmt.Sprintf("Tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
//					})
//				}
//			}
//		}
//
//		runTestCases(t, testCases)
//	})
//}

// TODO: Fix me
//func Test_Validation_PropertyIndexing(t *testing.T) {
//	t.Run("validates indexInverted / indexFilterable / indexSearchable combinations", func(t *testing.T) {
//		vFalse := false
//		vTrue := true
//		allBoolPtrs := []*bool{nil, &vFalse, &vTrue}
//
//		type testCase struct {
//			propName        string
//			dataType        schema.DataType
//			indexInverted   *bool
//			indexFilterable *bool
//			indexSearchable *bool
//
//			expectedErrMsg string
//		}
//
//		boolPtrToStr := func(ptr *bool) string {
//			if ptr == nil {
//				return "nil"
//			}
//			return fmt.Sprintf("%v", *ptr)
//		}
//		propName := func(dt schema.DataType, inverted, filterable, searchable *bool) string {
//			return fmt.Sprintf("%s_inverted_%s_filterable_%s_searchable_%s",
//				dt.String(), boolPtrToStr(inverted), boolPtrToStr(filterable), boolPtrToStr(searchable))
//		}
//
//		testCases := []testCase{}
//
//		for _, dataType := range []schema.DataType{schema.DataTypeText, schema.DataTypeInt, schema.DataTypeObject} {
//			for _, inverted := range allBoolPtrs {
//				for _, filterable := range allBoolPtrs {
//					for _, searchable := range allBoolPtrs {
//						if inverted != nil {
//							if filterable != nil || searchable != nil {
//								testCases = append(testCases, testCase{
//									propName:        propName(dataType, inverted, filterable, searchable),
//									dataType:        dataType,
//									indexInverted:   inverted,
//									indexFilterable: filterable,
//									indexSearchable: searchable,
//									expectedErrMsg:  "`indexInverted` is deprecated and can not be set together with `indexFilterable` or `indexSearchable`",
//								})
//								continue
//							}
//						}
//
//						if searchable != nil && *searchable {
//							if dataType != schema.DataTypeText {
//								testCases = append(testCases, testCase{
//									propName:        propName(dataType, inverted, filterable, searchable),
//									dataType:        dataType,
//									indexInverted:   inverted,
//									indexFilterable: filterable,
//									indexSearchable: searchable,
//									expectedErrMsg:  "`indexSearchable` is not allowed for other than text/text[] data types",
//								})
//								continue
//							}
//						}
//
//						testCases = append(testCases, testCase{
//							propName:        propName(dataType, inverted, filterable, searchable),
//							dataType:        dataType,
//							indexInverted:   inverted,
//							indexFilterable: filterable,
//							indexSearchable: searchable,
//							expectedErrMsg:  "",
//						})
//					}
//				}
//			}
//		}
//
//		mgr := newSchemaManager()
//		for _, tc := range testCases {
//			t.Run(tc.propName, func(t *testing.T) {
//				err := mgr.validatePropertyIndexing(&models.Property{
//					Name:            tc.propName,
//					DataType:        tc.dataType.PropString(),
//					IndexInverted:   tc.indexInverted,
//					IndexFilterable: tc.indexFilterable,
//					IndexSearchable: tc.indexSearchable,
//				})
//
//				if tc.expectedErrMsg != "" {
//					require.NotNil(t, err)
//					assert.EqualError(t, err, tc.expectedErrMsg)
//				} else {
//					require.Nil(t, err)
//				}
//			})
//		}
//	})
//}

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
