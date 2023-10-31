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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func Test_GetSchema(t *testing.T) {
	t.Parallel()
	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		fut := shutdown()
		require.Nil(t, fut.Error())
	}()

	sch, err := handler.GetSchema(nil)
	assert.Nil(t, err)
	assert.NotNil(t, sch)
}

func Test_AddClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		fut := shutdown()
		require.Nil(t, fut.Error())
	}()

	t.Run("happy path", func(t *testing.T) {
		class := models.Class{
			Class: "NewClass",
			Properties: []*models.Property{
				{DataType: []string{"text"}, Name: "textProp"},
				{DataType: []string{"int"}, Name: "intProp"},
			},
			Vectorizer: "none",
		}
		err := handler.AddClass(ctx, nil, &class)
		assert.Nil(t, err)
		defer func() {
			require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
		}()

		sch := handler.GetSchemaSkipAuth()
		require.Nil(t, err)
		require.NotNil(t, sch)
		require.NotNil(t, sch.Objects)
		require.Len(t, sch.Objects.Classes, 1)
		assert.Equal(t, class.Class, sch.Objects.Classes[0].Class)
		assert.Equal(t, class.Properties, sch.Objects.Classes[0].Properties)
	})

	t.Run("with empty class name", func(t *testing.T) {
		class := models.Class{}
		err := handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, "'' is not a valid class name")
	})

	t.Run("with permuted-casing class names", func(t *testing.T) {
		class1 := models.Class{Class: "NewClass", Vectorizer: "none"}
		err := handler.AddClass(ctx, nil, &class1)
		require.Nil(t, err)
		assert.Nil(t, err)
		defer func() {
			require.Nil(t, handler.DeleteClass(ctx, nil, class1.Class))
		}()

		class2 := models.Class{Class: "NewCLASS", Vectorizer: "none"}
		err = handler.AddClass(ctx, nil, &class2)
		assert.EqualError(t, err,
			`class name "NewCLASS" already exists as a permutation of: "NewClass". `+
				`class names must be unique when lowercased`)
	})

	t.Run("with default BM25 params", func(t *testing.T) {
		expectedBM25Config := &models.BM25Config{
			K1: config.DefaultBM25k1,
			B:  config.DefaultBM25b,
		}
		class := models.Class{
			Class:      "NewClass",
			Vectorizer: "none",
		}
		err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		defer func() {
			require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
		}()

		sch := handler.GetSchemaSkipAuth()
		require.NotNil(t, sch)
		require.NotNil(t, sch.Objects)
		require.Len(t, sch.Objects.Classes, 1)
		require.Equal(t, "NewClass", sch.Objects.Classes[0].Class)
		assert.Equal(t, expectedBM25Config, sch.Objects.Classes[0].InvertedIndexConfig.Bm25)
	})

	t.Run("with customized BM25 params", func(t *testing.T) {
		expectedBM25Config := &models.BM25Config{
			K1: 1.88,
			B:  0.44,
		}
		class := models.Class{
			Class: "NewClass",
			InvertedIndexConfig: &models.InvertedIndexConfig{
				Bm25: expectedBM25Config,
			},
			Vectorizer: "none",
		}
		err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		defer func() {
			require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
		}()

		sch := handler.GetSchemaSkipAuth()
		require.NotNil(t, sch)
		require.NotNil(t, sch.Objects)
		require.Len(t, sch.Objects.Classes, 1)
		require.Equal(t, "NewClass", sch.Objects.Classes[0].Class)
		assert.Equal(t, expectedBM25Config, sch.Objects.Classes[0].InvertedIndexConfig.Bm25)
	})

	t.Run("with default Stopwords config", func(t *testing.T) {
		expectedStopwordConfig := &models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		}
		class := models.Class{
			Class:      "NewClass",
			Vectorizer: "none",
		}
		err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		defer func() {
			require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
		}()

		sch := handler.GetSchemaSkipAuth()
		require.NotNil(t, sch)
		require.NotNil(t, sch.Objects)
		require.Len(t, sch.Objects.Classes, 1)
		require.Equal(t, "NewClass", sch.Objects.Classes[0].Class)
		assert.Equal(t, expectedStopwordConfig, sch.Objects.Classes[0].InvertedIndexConfig.Stopwords)
	})

	t.Run("with customized Stopwords config", func(t *testing.T) {
		expectedStopwordConfig := &models.StopwordConfig{
			Preset:    "none",
			Additions: []string{"monkey", "zebra", "octopus"},
			Removals:  []string{"are"},
		}
		class := models.Class{
			Class: "NewClass",
			InvertedIndexConfig: &models.InvertedIndexConfig{
				Stopwords: expectedStopwordConfig,
			},
			Vectorizer: "none",
		}
		err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		defer func() {
			require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
		}()

		sch := handler.GetSchemaSkipAuth()
		require.NotNil(t, sch)
		require.NotNil(t, sch.Objects)
		require.Len(t, sch.Objects.Classes, 1)
		require.Equal(t, "NewClass", sch.Objects.Classes[0].Class)
		assert.Equal(t, expectedStopwordConfig, sch.Objects.Classes[0].InvertedIndexConfig.Stopwords)
	})

	t.Run("with tokenizations", func(t *testing.T) {
		type testCase struct {
			propName       string
			dataType       []string
			tokenization   string
			expectedErrMsg string
		}

		propName := func(dataType schema.DataType, tokenization string) string {
			dtStr := strings.ReplaceAll(string(dataType), "[]", "Array")
			tStr := "empty"
			if tokenization != "" {
				tStr = tokenization
			}
			return fmt.Sprintf("%s_%s", dtStr, tStr)
		}

		runTestCases := func(t *testing.T, testCases []testCase) {
			for i, tc := range testCases {
				t.Run(tc.propName, func(t *testing.T) {
					class := &models.Class{
						Class: fmt.Sprintf("NewClass_%d", i),
						Properties: []*models.Property{
							{
								Name:         tc.propName,
								DataType:     tc.dataType,
								Tokenization: tc.tokenization,
							},
						},
						Vectorizer: "none",
					}
					err := handler.AddClass(context.Background(), nil, class)
					if tc.expectedErrMsg == "" {
						require.Nil(t, err)
						sch := handler.GetSchemaSkipAuth()
						require.NotNil(t, sch.Objects)
						require.NotEmpty(t, sch.Objects.Classes)
						defer func() {
							require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
						}()
					} else {
						require.EqualError(t, err, tc.expectedErrMsg)
					}
				})
			}
		}

		t.Run("text/textArray and all tokenizations", func(t *testing.T) {
			var testCases []testCase
			for _, dataType := range []schema.DataType{
				schema.DataTypeText, schema.DataTypeTextArray,
			} {
				for _, tokenization := range append(helpers.Tokenizations, "") {
					testCases = append(testCases, testCase{
						propName:       propName(dataType, tokenization),
						dataType:       dataType.PropString(),
						tokenization:   tokenization,
						expectedErrMsg: "",
					})
				}

				tokenization := "non_existing"
				testCases = append(testCases, testCase{
					propName:       propName(dataType, tokenization),
					dataType:       dataType.PropString(),
					tokenization:   tokenization,
					expectedErrMsg: fmt.Sprintf("Tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
				})
			}

			runTestCases(t, testCases)
		})

		t.Run("non text/textArray and all tokenizations", func(t *testing.T) {
			var testCases []testCase
			for _, dataType := range schema.PrimitiveDataTypes {
				switch dataType {
				case schema.DataTypeText, schema.DataTypeTextArray:
					continue
				default:
					tokenization := ""
					testCases = append(testCases, testCase{
						propName:       propName(dataType, tokenization),
						dataType:       dataType.PropString(),
						tokenization:   tokenization,
						expectedErrMsg: "",
					})

					for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
						testCases = append(testCases, testCase{
							propName:       propName(dataType, tokenization),
							dataType:       dataType.PropString(),
							tokenization:   tokenization,
							expectedErrMsg: fmt.Sprintf("Tokenization is not allowed for data type '%s'", dataType),
						})
					}
				}
			}

			runTestCases(t, testCases)
		})

		t.Run("non text/textArray and all tokenizations", func(t *testing.T) {
			classes := []models.Class{
				{Class: "SomeClass", Vectorizer: "none"},
				{Class: "SomeOtherClass", Vectorizer: "none"},
				{Class: "YetAnotherClass", Vectorizer: "none"},
			}
			for _, class := range classes {
				require.Nil(t, handler.AddClass(ctx, nil, &class))
			}
			defer func() {
				for _, class := range classes {
					require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
				}
			}()

			var testCases []testCase
			for i, dataType := range [][]string{
				{"SomeClass"},
				{"SomeOtherClass", "YetAnotherClass"},
			} {
				testCases = append(testCases, testCase{
					propName:       fmt.Sprintf("RefProp_%d_empty", i),
					dataType:       dataType,
					tokenization:   "",
					expectedErrMsg: "",
				})

				for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
					testCases = append(testCases, testCase{
						propName:       fmt.Sprintf("RefProp_%d_%s", i, tokenization),
						dataType:       dataType,
						tokenization:   tokenization,
						expectedErrMsg: "Tokenization is not allowed for reference data type",
					})
				}
			}

			runTestCases(t, testCases)
		})

		t.Run("[deprecated string] string/stringArray and all tokenizations", func(t *testing.T) {
			var testCases []testCase
			for _, dataType := range []schema.DataType{
				schema.DataTypeString, schema.DataTypeStringArray,
			} {
				for _, tokenization := range []string{
					models.PropertyTokenizationWord, models.PropertyTokenizationField, "",
				} {
					testCases = append(testCases, testCase{
						propName:       propName(dataType, tokenization),
						dataType:       dataType.PropString(),
						tokenization:   tokenization,
						expectedErrMsg: "",
					})
				}

				for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
					switch tokenization {
					case models.PropertyTokenizationWord, models.PropertyTokenizationField:
						continue
					default:
						testCases = append(testCases, testCase{
							propName:       propName(dataType, tokenization),
							dataType:       dataType.PropString(),
							tokenization:   tokenization,
							expectedErrMsg: fmt.Sprintf("Tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
						})
					}
				}
			}

			runTestCases(t, testCases)
		})
	})
}

func Test_AddClass_DefaultsAndMigration(t *testing.T) {
	t.Parallel()
	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		fut := shutdown()
		require.Nil(t, fut.Error())
	}()

	t.Run("set defaults and migrate string|stringArray datatype and tokenization", func(t *testing.T) {
		type testCase struct {
			propName     string
			dataType     schema.DataType
			tokenization string

			expectedDataType     schema.DataType
			expectedTokenization string
		}

		propName := func(dataType schema.DataType, tokenization string) string {
			return strings.ReplaceAll(fmt.Sprintf("%s_%s", dataType, tokenization), "[]", "Array")
		}

		ctx := context.Background()
		className := "MigrationClass"

		var testCases []testCase
		for _, dataType := range []schema.DataType{
			schema.DataTypeText, schema.DataTypeTextArray,
		} {
			for _, tokenization := range helpers.Tokenizations {
				testCases = append(testCases, testCase{
					propName:             propName(dataType, tokenization),
					dataType:             dataType,
					tokenization:         tokenization,
					expectedDataType:     dataType,
					expectedTokenization: tokenization,
				})
			}
			tokenization := ""
			testCases = append(testCases, testCase{
				propName:             propName(dataType, tokenization),
				dataType:             dataType,
				tokenization:         tokenization,
				expectedDataType:     dataType,
				expectedTokenization: models.PropertyTokenizationWord,
			})
		}
		for _, dataType := range []schema.DataType{
			schema.DataTypeString, schema.DataTypeStringArray,
		} {
			for _, tokenization := range []string{
				models.PropertyTokenizationWord, models.PropertyTokenizationField, "",
			} {
				var expectedDataType schema.DataType
				switch dataType {
				case schema.DataTypeStringArray:
					expectedDataType = schema.DataTypeTextArray
				default:
					expectedDataType = schema.DataTypeText
				}

				var expectedTokenization string
				switch tokenization {
				case models.PropertyTokenizationField:
					expectedTokenization = models.PropertyTokenizationField
				default:
					expectedTokenization = models.PropertyTokenizationWhitespace
				}

				testCases = append(testCases, testCase{
					propName:             propName(dataType, tokenization),
					dataType:             dataType,
					tokenization:         tokenization,
					expectedDataType:     expectedDataType,
					expectedTokenization: expectedTokenization,
				})
			}
		}

		t.Run("create class with all properties", func(t *testing.T) {
			var properties []*models.Property
			for _, tc := range testCases {
				properties = append(properties, &models.Property{
					Name:         "created_" + tc.propName,
					DataType:     tc.dataType.PropString(),
					Tokenization: tc.tokenization,
				})
			}

			class := models.Class{
				Class:      className,
				Properties: properties,
				Vectorizer: "none",
			}
			err := handler.AddClass(ctx, nil, &class)
			require.Nil(t, err)
			sch := handler.GetSchemaSkipAuth()
			require.NotNil(t, sch.Objects)
			require.NotEmpty(t, sch.Objects.Classes)
			require.Equal(t, className, sch.Objects.Classes[0].Class)
		})

		t.Run("add properties to existing class", func(t *testing.T) {
			for _, tc := range testCases {
				t.Run("added_"+tc.propName, func(t *testing.T) {
					err := handler.AddClassProperty(ctx, nil, className, &models.Property{
						Name:         "added_" + tc.propName,
						DataType:     tc.dataType.PropString(),
						Tokenization: tc.tokenization,
					})

					require.Nil(t, err)
				})
			}
		})

		t.Run("verify defaults and migration", func(t *testing.T) {
			class := handler.GetSchemaSkipAuth().Objects.Classes[0]
			for _, tc := range testCases {
				t.Run("created_"+tc.propName, func(t *testing.T) {
					createdProperty, err := schema.GetPropertyByName(class, "created_"+tc.propName)

					require.Nil(t, err)
					assert.Equal(t, tc.expectedDataType.PropString(), createdProperty.DataType)
					assert.Equal(t, tc.expectedTokenization, createdProperty.Tokenization)
				})

				t.Run("added_"+tc.propName, func(t *testing.T) {
					addedProperty, err := schema.GetPropertyByName(class, "added_"+tc.propName)

					require.Nil(t, err)
					assert.Equal(t, tc.expectedDataType.PropString(), addedProperty.DataType)
					assert.Equal(t, tc.expectedTokenization, addedProperty.Tokenization)
				})
			}
		})
	})

	t.Run("set defaults and migrate IndexInverted to IndexFilterable + IndexSearchable", func(t *testing.T) {
		vFalse := false
		vTrue := true
		allBoolPtrs := []*bool{nil, &vFalse, &vTrue}

		type testCase struct {
			propName        string
			dataType        schema.DataType
			indexInverted   *bool
			indexFilterable *bool
			indexSearchable *bool

			expectedInverted   *bool
			expectedFilterable *bool
			expectedSearchable *bool
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

		ctx := context.Background()
		className := "MigrationClass"

		var testCases []testCase

		for _, dataType := range []schema.DataType{schema.DataTypeText, schema.DataTypeInt} {
			for _, inverted := range allBoolPtrs {
				for _, filterable := range allBoolPtrs {
					for _, searchable := range allBoolPtrs {
						if inverted != nil {
							if filterable != nil || searchable != nil {
								// invalid combination, indexInverted can not be set
								// together with indexFilterable or indexSearchable
								continue
							}
						}

						if searchable != nil && *searchable {
							if dataType != schema.DataTypeText {
								// invalid combination, indexSearchable can not be enabled
								// for non text/text[] data type
								continue
							}
						}

						switch dataType {
						case schema.DataTypeText:
							if inverted != nil {
								testCases = append(testCases, testCase{
									propName:           propName(dataType, inverted, filterable, searchable),
									dataType:           dataType,
									indexInverted:      inverted,
									indexFilterable:    filterable,
									indexSearchable:    searchable,
									expectedInverted:   nil,
									expectedFilterable: inverted,
									expectedSearchable: inverted,
								})
							} else {
								expectedFilterable := filterable
								if filterable == nil {
									expectedFilterable = &vTrue
								}
								expectedSearchable := searchable
								if searchable == nil {
									expectedSearchable = &vTrue
								}
								testCases = append(testCases, testCase{
									propName:           propName(dataType, inverted, filterable, searchable),
									dataType:           dataType,
									indexInverted:      inverted,
									indexFilterable:    filterable,
									indexSearchable:    searchable,
									expectedInverted:   nil,
									expectedFilterable: expectedFilterable,
									expectedSearchable: expectedSearchable,
								})
							}
						default:
							if inverted != nil {
								testCases = append(testCases, testCase{
									propName:           propName(dataType, inverted, filterable, searchable),
									dataType:           dataType,
									indexInverted:      inverted,
									indexFilterable:    filterable,
									indexSearchable:    searchable,
									expectedInverted:   nil,
									expectedFilterable: inverted,
									expectedSearchable: &vFalse,
								})
							} else {
								expectedFilterable := filterable
								if filterable == nil {
									expectedFilterable = &vTrue
								}
								expectedSearchable := searchable
								if searchable == nil {
									expectedSearchable = &vFalse
								}
								testCases = append(testCases, testCase{
									propName:           propName(dataType, inverted, filterable, searchable),
									dataType:           dataType,
									indexInverted:      inverted,
									indexFilterable:    filterable,
									indexSearchable:    searchable,
									expectedInverted:   nil,
									expectedFilterable: expectedFilterable,
									expectedSearchable: expectedSearchable,
								})
							}
						}
					}
				}
			}
			require.Nil(t, handler.DeleteClass(ctx, nil, className))
		}

		t.Run("create class with all properties", func(t *testing.T) {
			var properties []*models.Property
			for _, tc := range testCases {
				properties = append(properties, &models.Property{
					Name:            "created_" + tc.propName,
					DataType:        tc.dataType.PropString(),
					IndexInverted:   tc.indexInverted,
					IndexFilterable: tc.indexFilterable,
					IndexSearchable: tc.indexSearchable,
				})
			}

			class := models.Class{
				Class:      className,
				Properties: properties,
				Vectorizer: "none",
			}
			err := handler.AddClass(ctx, nil, &class)
			require.Nil(t, err)
			sch := handler.GetSchemaSkipAuth()
			require.NotNil(t, sch.Objects)
			require.NotEmpty(t, sch.Objects.Classes)
			require.Equal(t, className, sch.Objects.Classes[0].Class)
		})

		t.Run("add properties to existing class", func(t *testing.T) {
			for _, tc := range testCases {
				t.Run("added_"+tc.propName, func(t *testing.T) {
					err := handler.AddClassProperty(ctx, nil, className, &models.Property{
						Name:            "added_" + tc.propName,
						DataType:        tc.dataType.PropString(),
						IndexInverted:   tc.indexInverted,
						IndexFilterable: tc.indexFilterable,
						IndexSearchable: tc.indexSearchable,
					})

					require.Nil(t, err)
				})
			}
		})

		t.Run("verify migration", func(t *testing.T) {
			class := handler.GetSchemaSkipAuth().Objects.Classes[0]
			for _, tc := range testCases {
				t.Run("created_"+tc.propName, func(t *testing.T) {
					createdProperty, err := schema.GetPropertyByName(class, "created_"+tc.propName)

					require.Nil(t, err)
					assert.Equal(t, tc.expectedInverted, createdProperty.IndexInverted)
					assert.Equal(t, tc.expectedFilterable, createdProperty.IndexFilterable)
					assert.Equal(t, tc.expectedSearchable, createdProperty.IndexSearchable)
				})

				t.Run("added_"+tc.propName, func(t *testing.T) {
					addedProperty, err := schema.GetPropertyByName(class, "added_"+tc.propName)

					require.Nil(t, err)
					assert.Equal(t, tc.expectedInverted, addedProperty.IndexInverted)
					assert.Equal(t, tc.expectedFilterable, addedProperty.IndexFilterable)
					assert.Equal(t, tc.expectedSearchable, addedProperty.IndexSearchable)
				})
			}
		})
	})
}

func Test_Defaults_NestedProperties(t *testing.T) {
	t.Parallel()
	//handler, shutdown := newTestHandler(t, &fakeDB{})
	//defer func() {
	//	fut := shutdown()
	//	require.Nil(t, fut.Error())
	//}()

	for _, pdt := range schema.PrimitiveDataTypes {
		t.Run(pdt.String(), func(t *testing.T) {
			nestedProperties := []*models.NestedProperty{
				{
					Name:     "nested_" + pdt.String(),
					DataType: pdt.PropString(),
				},
			}

			for _, ndt := range schema.NestedDataTypes {
				t.Run(ndt.String(), func(t *testing.T) {
					propPrimitives := &models.Property{
						Name:             "objectProp",
						DataType:         ndt.PropString(),
						NestedProperties: nestedProperties,
					}
					propLvl2Primitives := &models.Property{
						Name:     "objectPropLvl2",
						DataType: ndt.PropString(),
						NestedProperties: []*models.NestedProperty{
							{
								Name:             "nested_object",
								DataType:         ndt.PropString(),
								NestedProperties: nestedProperties,
							},
						},
					}

					setPropertyDefaults(propPrimitives)
					setPropertyDefaults(propLvl2Primitives)

					t.Run("primitive data types", func(t *testing.T) {
						for _, np := range []*models.NestedProperty{
							propPrimitives.NestedProperties[0],
							propLvl2Primitives.NestedProperties[0].NestedProperties[0],
						} {
							switch pdt {
							case schema.DataTypeText, schema.DataTypeTextArray:
								require.NotNil(t, np.IndexFilterable)
								assert.True(t, *np.IndexFilterable)
								require.NotNil(t, np.IndexSearchable)
								assert.True(t, *np.IndexSearchable)
								assert.Equal(t, models.PropertyTokenizationWord, np.Tokenization)
							case schema.DataTypeBlob:
								require.NotNil(t, np.IndexFilterable)
								assert.False(t, *np.IndexFilterable)
								require.NotNil(t, np.IndexSearchable)
								assert.False(t, *np.IndexSearchable)
								assert.Equal(t, "", np.Tokenization)
							default:
								require.NotNil(t, np.IndexFilterable)
								assert.True(t, *np.IndexFilterable)
								require.NotNil(t, np.IndexSearchable)
								assert.False(t, *np.IndexSearchable)
								assert.Equal(t, "", np.Tokenization)
							}
						}
					})

					t.Run("nested data types", func(t *testing.T) {
						for _, indexFilterable := range []*bool{
							propPrimitives.IndexFilterable,
							propLvl2Primitives.IndexFilterable,
							propLvl2Primitives.NestedProperties[0].IndexFilterable,
						} {
							require.NotNil(t, indexFilterable)
							assert.True(t, *indexFilterable)
						}
						for _, indexSearchable := range []*bool{
							propPrimitives.IndexSearchable,
							propLvl2Primitives.IndexSearchable,
							propLvl2Primitives.NestedProperties[0].IndexSearchable,
						} {
							require.NotNil(t, indexSearchable)
							assert.False(t, *indexSearchable)
						}
						for _, tokenization := range []string{
							propPrimitives.Tokenization,
							propLvl2Primitives.Tokenization,
							propLvl2Primitives.NestedProperties[0].Tokenization,
						} {
							assert.Equal(t, "", tokenization)
						}
					})
				})
			}
		})
	}
}

func Test_Validation_ClassNames(t *testing.T) {
	t.Parallel()
	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		fut := shutdown()
		require.Nil(t, fut.Error())
	}()

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

	ctx := context.Background()

	t.Run("adding a class", func(t *testing.T) {
		t.Run("different class names without keywords or properties", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "none",
						Class:      test.input,
					}

					err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(handler)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
					require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
				})
			}
		})

		t.Run("different class names with valid keywords", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "none",
						Class:      test.input,
					}

					err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					classNames := testGetClassNames(handler)
					assert.Contains(t, classNames, test.storedAs, "class should be stored correctly")
					require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
				})
			}
		})
	})
}

func Test_Validation_PropertyNames(t *testing.T) {
	t.Parallel()
	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		fut := shutdown()
		require.Nil(t, fut.Error())
	}()

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

	ctx := context.Background()

	t.Run("when adding a new class", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					sch, _ := handler.GetSchema(nil)
					propName := sch.Objects.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
					require.Nil(t, handler.DeleteClass(context.Background(), nil, class.Class))
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					sch, _ := handler.GetSchema(nil)
					propName := sch.Objects.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
					require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
				})
			}
		})
	})

	t.Run("when updating an existing class with a new property", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{
							{
								Name:     "dummyPropSoWeDontRunIntoAllNoindexedError",
								DataType: schema.DataTypeText.PropString(),
							},
						},
					}

					err := handler.AddClass(context.Background(), nil, class)
					require.Nil(t, err)
					defer func() {
						require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
					}()

					property := &models.Property{
						DataType: schema.DataTypeText.PropString(),
						Name:     test.input,
					}
					err = handler.AddClassProperty(context.Background(), nil, "ValidName", property)
					t.Log(err)
					require.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					sch, _ := handler.GetSchema(nil)
					propName := sch.Objects.Classes[0].Properties[1].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					err := handler.AddClass(ctx, nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)

					// only proceed if input was supposed to be valid
					if test.valid == false {
						return
					}

					sch, _ := handler.GetSchema(nil)
					propName := sch.Objects.Classes[0].Properties[0].Name
					assert.Equal(t, propName, test.storedAs, "class should be stored correctly")
					require.Nil(t, handler.DeleteClass(ctx, nil, class.Class))
				})
			}
		})
	})
}

// As of now, most class settings are immutable, but we need to allow some
// specific updates, such as the vector index config
func Test_UpdateClass(t *testing.T) {
	t.Parallel()
	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		fut := shutdown()
		require.Nil(t, fut.Error())
	}()

	t.Run("a class which doesn't exist", func(t *testing.T) {
		err := handler.UpdateClass(context.Background(),
			nil, "WrongClass", &models.Class{})
		require.NotNil(t, err)
		assert.Equal(t, ErrNotFound, err)
	})

	t.Run("various immutable and mutable fields", func(t *testing.T) {
		type test struct {
			name          string
			initial       *models.Class
			update        *models.Class
			expectedError error
		}

		tests := []test{
			{
				name:    "attempting a name change",
				initial: &models.Class{Class: "InitialName", Vectorizer: "none"},
				update:  &models.Class{Class: "UpdatedName", Vectorizer: "none"},
				expectedError: fmt.Errorf(
					"class name is immutable: " +
						"attempted change from \"InitialName\" to \"UpdatedName\""),
			},
			{
				name:    "attempting to modify the vectorizer",
				initial: &models.Class{Class: "InitialName", Vectorizer: "model1"},
				update:  &models.Class{Class: "InitialName", Vectorizer: "model2"},
				expectedError: fmt.Errorf(
					"vectorizer is immutable: " +
						"attempted change from \"model1\" to \"model2\""),
			},
			{
				name:    "attempting to modify the vector index type",
				initial: &models.Class{Class: "InitialName", VectorIndexType: "hnsw", Vectorizer: "none"},
				update:  &models.Class{Class: "InitialName", VectorIndexType: "lsh", Vectorizer: "none"},
				expectedError: fmt.Errorf(
					"vector index type is immutable: " +
						"attempted change from \"hnsw\" to \"lsh\""),
			},
			{
				name:    "attempting to add a property",
				initial: &models.Class{Class: "InitialName", Vectorizer: "none"},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name: "newProp",
						},
					},
				},
				expectedError: fmt.Errorf(
					"properties cannot be updated through updating the class. Use the add " +
						"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
						"to add additional properties"),
			},
			{
				name: "leaving properties unchanged",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "aProp",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "aProp",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
				expectedError: nil,
			},
			{
				name: "attempting to rename a property",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "aProp",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "changedProp",
							DataType: schema.DataTypeText.PropString(),
						},
					},
				},
				expectedError: fmt.Errorf(
					"properties cannot be updated through updating the class. Use the add " +
						"property feature (e.g. \"POST /v1/schema/{className}/properties\") " +
						"to add additional properties"),
			},
			{
				name: "attempting to update the inverted index cleanup interval",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 17,
					},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Bm25: &models.BM25Config{
							K1: config.DefaultBM25k1,
							B:  config.DefaultBM25b,
						},
					},
				},
			},
			{
				name: "attempting to update the inverted index BM25 config",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Bm25: &models.BM25Config{
							K1: 1.012,
							B:  0.125,
						},
					},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Bm25: &models.BM25Config{
							K1: 1.012,
							B:  0.125,
						},
					},
				},
			},
			{
				name: "attempting to update the inverted index Stopwords config",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Stopwords: &models.StopwordConfig{
							Preset: "en",
						},
					},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					InvertedIndexConfig: &models.InvertedIndexConfig{
						CleanupIntervalSeconds: 18,
						Stopwords: &models.StopwordConfig{
							Preset:    "none",
							Additions: []string{"banana", "passionfruit", "kiwi"},
							Removals:  []string{"a", "the"},
						},
					},
				},
			},
			{
				name: "attempting to update module config",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					ModuleConfig: map[string]interface{}{
						"my-module1": map[string]interface{}{
							"my-setting": "some-value",
						},
					},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					ModuleConfig: map[string]interface{}{
						"my-module1": map[string]interface{}{
							"my-setting": "updated-value",
						},
					},
				},
				expectedError: fmt.Errorf("module config is immutable"),
			},
			{
				name: "updating vector index config",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					VectorIndexConfig: map[string]interface{}{
						"some-setting": "old-value",
					},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					VectorIndexConfig: map[string]interface{}{
						"some-setting": "new-value",
					},
				},
				expectedError: nil,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				ctx := context.Background()
				assert.Nil(t, handler.AddClass(ctx, nil, test.initial))
				defer func() {
					require.Nil(t, handler.DeleteClass(ctx, nil, test.initial.Class))
				}()
				err := handler.UpdateClass(ctx, nil, test.initial.Class, test.update)
				if test.expectedError == nil {
					assert.Nil(t, err)
				} else {
					require.NotNil(t, err, "update must error")
					assert.Equal(t, test.expectedError.Error(), err.Error())
				}
			})
		}
	})
}

func TestRestoreClass_WithCircularRefs(t *testing.T) {
	// When restoring a class, there could be circular refs between the classes,
	// thus any validation that checks if linked classes exist would fail on the
	// first class to import. Since we have no control over the order of imports
	// when restoring, we need to relax this validation.

	t.Parallel()
	handler, shutdown := newTestHandler(t, &fakeDB{})
	defer func() {
		fut := shutdown()
		require.Nil(t, fut.Error())
	}()

	classes := []*models.Class{
		{
			Class: "Class_A",
			Properties: []*models.Property{{
				Name:     "to_Class_B",
				DataType: []string{"Class_B"},
			}, {
				Name:     "to_Class_C",
				DataType: []string{"Class_C"},
			}},
			Vectorizer: "none",
		},

		{
			Class: "Class_B",
			Properties: []*models.Property{{
				Name:     "to_Class_A",
				DataType: []string{"Class_A"},
			}, {
				Name:     "to_Class_C",
				DataType: []string{"Class_C"},
			}},
			Vectorizer: "none",
		},

		{
			Class: "Class_C",
			Properties: []*models.Property{{
				Name:     "to_Class_A",
				DataType: []string{"Class_A"},
			}, {
				Name:     "to_Class_B",
				DataType: []string{"Class_B"},
			}},
			Vectorizer: "none",
		},
	}

	for _, classRaw := range classes {
		schemaBytes, err := json.Marshal(classRaw)
		require.Nil(t, err)

		// for this particular test the sharding state does not matter, so we can
		// initiate any new sharding state
		shardingConfig, err := sharding.ParseConfig(nil, 1)
		require.Nil(t, err)

		nodes := fakeNodes{[]string{"node1", "node2"}}
		shardingState, err := sharding.InitState(classRaw.Class, shardingConfig, nodes, 1, false)
		require.Nil(t, err)

		shardingBytes, err := shardingState.JSON()
		require.Nil(t, err)

		descriptor := backup.ClassDescriptor{Name: classRaw.Class, Schema: schemaBytes, ShardingState: shardingBytes}
		err = handler.RestoreClass(context.Background(), &descriptor, map[string]string{})
		assert.Nil(t, err, "class passes validation")
	}
}

// TODO: Fix me
//func TestRestoreClass_WithNodeMapping(t *testing.T) {
//	classes := []*models.Class{{Class: "Class_A"}}
//
//	mgr := newSchemaManager()
//
//	for _, classRaw := range classes {
//		schemaBytes, err := json.Marshal(classRaw)
//		require.Nil(t, err)
//
//		shardingConfig, err := sharding.ParseConfig(nil, 2)
//		require.Nil(t, err)
//
//		nodes := fakeNodes{[]string{"node1", "node2"}}
//		shardingState, err := sharding.InitState(classRaw.Class, shardingConfig, nodes, 2, false)
//		require.Nil(t, err)
//
//		shardingBytes, err := shardingState.JSON()
//		require.Nil(t, err)
//
//		descriptor := backup.ClassDescriptor{Name: classRaw.Class, Schema: schemaBytes, ShardingState: shardingBytes}
//		err = mgr.RestoreClass(context.Background(), &descriptor, map[string]string{"node1": "new-node1"})
//		assert.NoError(t, err)
//
//		// Ensure that sharding state has been updated with the new node names
//		for _, shard := range mgr.ShardingState {
//			for _, v := range shard.Physical {
//				for _, node := range v.BelongsToNodes {
//					assert.Contains(t, []string{"new-node1", "node2"}, node)
//				}
//			}
//		}
//	}
//}

func testGetClassNames(h *Handler) []string {
	var names []string
	sch, _ := h.GetSchema(nil)

	// Extract all names
	for _, class := range sch.SemanticSchemaFor().Classes {
		names = append(names, class.Class)
	}

	return names
}
