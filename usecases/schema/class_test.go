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
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

func Test_GetSchema(t *testing.T) {
	t.Parallel()
	handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
	fakeMetaHandler.On("ReadOnlySchema").Return(models.Schema{})

	sch, err := handler.GetSchema(nil)
	assert.Nil(t, err)
	assert.NotNil(t, sch)
	fakeMetaHandler.AssertExpectations(t)
}

func Test_AddClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})

		class := models.Class{
			Class: "NewClass",
			Properties: []*models.Property{
				{DataType: []string{"text"}, Name: "textProp"},
				{DataType: []string{"int"}, Name: "intProp"},
			},
			Vectorizer: "none",
		}
		fakeMetaHandler.On("AddClass", mock.Anything, mock.Anything).Return(nil)

		_, err := handler.AddClass(ctx, nil, &class)
		assert.Nil(t, err)

		fakeMetaHandler.AssertExpectations(t)
	})

	t.Run("with empty class name", func(t *testing.T) {
		handler, _ := newTestHandler(t, &fakeDB{})
		class := models.Class{}
		_, err := handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, "'' is not a valid class name")
	})

	t.Run("with default params", func(t *testing.T) {
		handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
		class := models.Class{
			Class:      "NewClass",
			Vectorizer: "none",
		}

		expectedBM25Config := &models.BM25Config{
			K1: config.DefaultBM25k1,
			B:  config.DefaultBM25b,
		}
		expectedStopwordConfig := &models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		}
		expectedClass := &class
		expectedClass.InvertedIndexConfig = &models.InvertedIndexConfig{
			Bm25:                   expectedBM25Config,
			CleanupIntervalSeconds: 60,
			Stopwords:              expectedStopwordConfig,
		}
		fakeMetaHandler.On("AddClass", expectedClass, mock.Anything).Return(nil)

		_, err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		fakeMetaHandler.AssertExpectations(t)
	})

	t.Run("with customized params", func(t *testing.T) {
		handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
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

		expectedStopwordConfig := &models.StopwordConfig{
			Preset:    "none",
			Additions: []string{"monkey", "zebra", "octopus"},
			Removals:  []string{"are"},
		}
		expectedClass := &class
		expectedClass.InvertedIndexConfig = &models.InvertedIndexConfig{
			Bm25:                   expectedBM25Config,
			CleanupIntervalSeconds: 60,
			Stopwords:              expectedStopwordConfig,
		}
		fakeMetaHandler.On("AddClass", expectedClass, mock.Anything).Return(nil)
		_, err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		fakeMetaHandler.AssertExpectations(t)
	})

	t.Run("with tokenizations", func(t *testing.T) {
		type testCase struct {
			propName       string
			dataType       []string
			tokenization   string
			expectedErrMsg string
			callReadOnly   bool
		}

		propName := func(dataType schema.DataType, tokenization string) string {
			dtStr := strings.ReplaceAll(string(dataType), "[]", "Array")
			tStr := "empty"
			if tokenization != "" {
				tStr = tokenization
			}
			return fmt.Sprintf("%s_%s", dtStr, tStr)
		}

		// These classes are necessary for tests using references
		classes := map[string]models.Class{
			"SomeClass":       {Class: "SomeClass", Vectorizer: "none"},
			"SomeOtherClass":  {Class: "SomeOtherClass", Vectorizer: "none"},
			"YetAnotherClass": {Class: "YetAnotherClass", Vectorizer: "none"},
		}

		runTestCases := func(t *testing.T, testCases []testCase) {
			for i, tc := range testCases {
				t.Run(tc.propName, func(t *testing.T) {
					handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})

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
					classes[class.Class] = *class

					if tc.callReadOnly {
						call := fakeMetaHandler.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(nil)
						call.RunFn = func(a mock.Arguments) {
							existedClass := classes[a.Get(0).(string)]
							call.ReturnArguments = mock.Arguments{&existedClass}
						}
					}

					// fakeMetaHandler.On("ReadOnlyClass", mock.Anything).Return(&models.Class{Class: classes[tc.dataType[0]].Class, Vectorizer: classes[tc.dataType[0]].Vectorizer})
					if tc.expectedErrMsg == "" {
						fakeMetaHandler.On("AddClass", mock.Anything, mock.Anything).Return(nil)
					}

					_, err := handler.AddClass(context.Background(), nil, class)
					if tc.expectedErrMsg == "" {
						require.Nil(t, err)
					} else {
						require.EqualError(t, err, tc.expectedErrMsg)
					}
					fakeMetaHandler.AssertExpectations(t)
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
					callReadOnly:   true,
				})

				for _, tokenization := range append(helpers.Tokenizations, "non_existing") {
					testCases = append(testCases, testCase{
						propName:       fmt.Sprintf("RefProp_%d_%s", i, tokenization),
						dataType:       dataType,
						tokenization:   tokenization,
						expectedErrMsg: "Tokenization is not allowed for reference data type",
						callReadOnly:   true,
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

	t.Run("with invalid settings", func(t *testing.T) {
		handler, _ := newTestHandler(t, &fakeDB{})

		// Vectorizer while VectorConfig exists
		_, err := handler.AddClass(ctx, nil, &models.Class{
			Class:      "NewClass",
			Vectorizer: "some",
			VectorConfig: map[string]models.VectorConfig{"custom": {
				VectorIndexType:   "hnsw",
				VectorIndexConfig: hnsw.UserConfig{},
				Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
			}},
		})
		assert.EqualError(t, err, "class.vectorizer \"some\" can not be set if class.vectorConfig is configured")

		// VectorIndexType while VectorConfig exists
		_, err = handler.AddClass(ctx, nil, &models.Class{
			Class:           "NewClass",
			VectorIndexType: "some",
			VectorConfig: map[string]models.VectorConfig{"custom": {
				VectorIndexType:   "hnsw",
				VectorIndexConfig: hnsw.UserConfig{},
				Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
			}},
		})
		assert.EqualError(t, err, "class.vectorIndexType \"some\" can not be set if class.vectorConfig is configured")

		// VectorConfig is invalid VectorIndexType
		_, err = handler.AddClass(ctx, nil, &models.Class{
			Class: "NewClass",
			VectorConfig: map[string]models.VectorConfig{"custom": {
				VectorIndexType:   "invalid",
				VectorIndexConfig: hnsw.UserConfig{},
				Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
			}},
		})
		assert.EqualError(t, err, "target vector \"custom\": unrecognized or unsupported vectorIndexType \"invalid\"")

		// VectorConfig is invalid Vectorizer
		_, err = handler.AddClass(ctx, nil, &models.Class{
			Class: "NewClass",
			VectorConfig: map[string]models.VectorConfig{"custom": {
				VectorIndexType:   "flat",
				VectorIndexConfig: hnsw.UserConfig{},
				Vectorizer:        map[string]interface{}{"invalid": nil},
			}},
		})
		assert.EqualError(t, err, "target vector \"custom\": vectorizer: invalid vectorizer \"invalid\"")
	})
}

func Test_AddClass_DefaultsAndMigration(t *testing.T) {
	t.Parallel()

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

		handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
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

		t.Run("create class with all properties", func(t *testing.T) {
			fakeMetaHandler.On("AddClass", mock.Anything, mock.Anything).Return(nil)
			fakeMetaHandler.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(nil)

			_, err := handler.AddClass(ctx, nil, &class)
			require.Nil(t, err)
		})

		t.Run("add properties to existing class", func(t *testing.T) {
			for _, tc := range testCases {
				fakeMetaHandler.On("AddClass", mock.Anything, mock.Anything).Return(nil)
				fakeMetaHandler.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(&class)
				fakeMetaHandler.On("AddProperty", mock.Anything, mock.Anything).Return(nil)
				t.Run("added_"+tc.propName, func(t *testing.T) {
					_, err := handler.AddClassProperty(ctx, nil, &class, false, &models.Property{
						Name:         "added_" + tc.propName,
						DataType:     tc.dataType.PropString(),
						Tokenization: tc.tokenization,
					})

					require.Nil(t, err)
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
		}

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
		t.Run("create class with all properties", func(t *testing.T) {
			handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
			fakeMetaHandler.On("AddClass", mock.Anything, mock.Anything).Return(nil)
			_, err := handler.AddClass(ctx, nil, &class)
			require.Nil(t, err)
			fakeMetaHandler.AssertExpectations(t)
		})

		t.Run("add properties to existing class", func(t *testing.T) {
			handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
			for _, tc := range testCases {
				t.Run("added_"+tc.propName, func(t *testing.T) {
					prop := &models.Property{
						Name:            "added_" + tc.propName,
						DataType:        tc.dataType.PropString(),
						IndexInverted:   tc.indexInverted,
						IndexFilterable: tc.indexFilterable,
						IndexSearchable: tc.indexSearchable,
					}
					fakeMetaHandler.On("AddProperty", className, []*models.Property{prop}).Return(nil)
					_, err := handler.AddClassProperty(ctx, nil, &class, false, prop)

					require.Nil(t, err)
				})
			}
			fakeMetaHandler.AssertExpectations(t)
		})
	})
}

func Test_Defaults_NestedProperties(t *testing.T) {
	t.Parallel()

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
					handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      test.input,
					}

					if test.valid {
						fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
					}
					_, err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeMetaHandler.AssertExpectations(t)
				})
			}
		})

		t.Run("different class names with valid keywords", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      test.input,
					}

					if test.valid {
						fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
					}
					_, err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeMetaHandler.AssertExpectations(t)
				})
			}
		})
	})
}

func Test_Validation_PropertyNames(t *testing.T) {
	t.Parallel()
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
					handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					if test.valid {
						fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
					}
					_, err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeMetaHandler.AssertExpectations(t)
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					if test.valid {
						fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
					}
					_, err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeMetaHandler.AssertExpectations(t)
				})
			}
		})
	})

	t.Run("when updating an existing class with a new property", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
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

					fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
					_, err := handler.AddClass(context.Background(), nil, class)
					require.Nil(t, err)

					property := &models.Property{
						DataType: schema.DataTypeText.PropString(),
						Name:     test.input,
					}
					if test.valid {
						fakeMetaHandler.On("AddProperty", class.Class, []*models.Property{property}).Return(nil)
					}
					_, err = handler.AddClassProperty(context.Background(), nil, class, false, property)
					t.Log(err)
					require.Equal(t, test.valid, err == nil)
					fakeMetaHandler.AssertExpectations(t)
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
					}

					if test.valid {
						fakeMetaHandler.On("AddClass", class, mock.Anything).Return(nil)
					}
					_, err := handler.AddClass(ctx, nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeMetaHandler.AssertExpectations(t)
				})
			}
		})
	})
}

// As of now, most class settings are immutable, but we need to allow some
// specific updates, such as the vector index config
func Test_UpdateClass(t *testing.T) {
	t.Run("ClassNotFound", func(t *testing.T) {
		handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
		fakeMetaHandler.On("ReadOnlyClass", "WrongClass", mock.Anything).Return(nil)
		fakeMetaHandler.On("UpdateClass", mock.Anything, mock.Anything).Return(ErrNotFound)

		err := handler.UpdateClass(context.Background(), nil, "WrongClass", &models.Class{})
		require.NotNil(t, err)
		assert.Equal(t, ErrNotFound, err)
		fakeMetaHandler.AssertExpectations(t)
	})

	t.Run("Fields validation", func(t *testing.T) {
		tests := []struct {
			name          string
			initial       *models.Class
			update        *models.Class
			expectedError error
		}{
			{
				name:    "ChangeName",
				initial: &models.Class{Class: "InitialName", Vectorizer: "none"},
				update:  &models.Class{Class: "UpdatedName", Vectorizer: "none"},
				expectedError: fmt.Errorf(
					"class name is immutable: " +
						"attempted change from \"InitialName\" to \"UpdatedName\""),
			},
			{
				name:    "ModifyVectorizer",
				initial: &models.Class{Class: "InitialName", Vectorizer: "model1"},
				update:  &models.Class{Class: "InitialName", Vectorizer: "model2"},
				expectedError: fmt.Errorf(
					"vectorizer is immutable: " +
						"attempted change from \"model1\" to \"model2\""),
			},
			{
				name:    "ModifyVectorIndexType",
				initial: &models.Class{Class: "InitialName", VectorIndexType: "hnsw", Vectorizer: "none"},
				update:  &models.Class{Class: "InitialName", VectorIndexType: "flat", Vectorizer: "none"},
				expectedError: fmt.Errorf(
					"vector index type is immutable: " +
						"attempted change from \"hnsw\" to \"flat\""),
			},
			{
				name:          "UnsupportedVectorIndex",
				initial:       &models.Class{Class: "InitialName", VectorIndexType: "hnsw", Vectorizer: "none"},
				update:        &models.Class{Class: "InitialName", VectorIndexType: "lsh", Vectorizer: "none"},
				expectedError: fmt.Errorf("unsupported vector"),
			},
			{
				name:    "AddProperty",
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
			store := NewFakeStore()
			t.Run(test.name, func(t *testing.T) {
				handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
				ctx := context.Background()

				fakeMetaHandler.On("AddClass", test.initial, mock.Anything).Return(nil)
				fakeMetaHandler.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)
				fakeMetaHandler.On("ReadOnlyClass", test.initial.Class, mock.Anything).Return(test.initial)
				if len(test.initial.Properties) > 0 {
					fakeMetaHandler.On("ReadOnlyClass", test.initial.Class, mock.Anything).Return(test.initial)
				}
				_, err := handler.AddClass(ctx, nil, test.initial)
				assert.Nil(t, err)
				store.AddClass(test.initial)

				fakeMetaHandler.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)
				err = handler.UpdateClass(ctx, nil, test.initial.Class, test.update)
				if err == nil {
					err = store.UpdateClass(test.update)
				}

				if test.expectedError == nil {
					assert.Nil(t, err)
				} else {
					require.NotNil(t, err, "update must error")
					assert.Contains(t, err.Error(), test.expectedError.Error())
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
	handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})

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
		shardingConfig, err := shardingConfig.ParseConfig(nil, 1)
		require.Nil(t, err)

		nodes := fakeNodes{[]string{"node1", "node2"}}
		shardingState, err := sharding.InitState(classRaw.Class, shardingConfig, nodes, 1, false)
		require.Nil(t, err)

		shardingBytes, err := shardingState.JSON()
		require.Nil(t, err)

		descriptor := backup.ClassDescriptor{Name: classRaw.Class, Schema: schemaBytes, ShardingState: shardingBytes}
		fakeMetaHandler.On("RestoreClass", mock.Anything, mock.Anything).Return(nil)
		err = handler.RestoreClass(context.Background(), &descriptor, map[string]string{})
		assert.Nil(t, err, "class passes validation")
		fakeMetaHandler.AssertExpectations(t)
	}
}

func TestRestoreClass_WithNodeMapping(t *testing.T) {
	classes := []*models.Class{{
		Class:      "Class_A",
		Vectorizer: "none",
	}}

	handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})

	for _, classRaw := range classes {
		schemaBytes, err := json.Marshal(classRaw)
		require.Nil(t, err)

		shardingConfig, err := shardingConfig.ParseConfig(nil, 2)
		require.Nil(t, err)

		nodes := fakeNodes{[]string{"node1", "node2"}}
		shardingState, err := sharding.InitState(classRaw.Class, shardingConfig, nodes, 2, false)
		require.Nil(t, err)

		shardingBytes, err := shardingState.JSON()
		require.Nil(t, err)

		descriptor := backup.ClassDescriptor{Name: classRaw.Class, Schema: schemaBytes, ShardingState: shardingBytes}
		expectedShardingState := shardingState
		expectedShardingState.ApplyNodeMapping(map[string]string{"node1": "new-node1"})
		expectedShardingState.SetLocalName("")
		fakeMetaHandler.On("RestoreClass", mock.Anything, shardingState).Return(nil)
		err = handler.RestoreClass(context.Background(), &descriptor, map[string]string{"node1": "new-node1"})
		assert.NoError(t, err)
	}
}

func Test_DeleteClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		classToDelete string
		expErr        bool
		expErrMsg     string
		existing      []*models.Class
		expected      []*models.Class
	}{
		{
			name:          "class exists",
			classToDelete: "C1",
			existing: []*models.Class{
				{Class: "C1", VectorIndexType: "hnsw"},
				{Class: "OtherClass", VectorIndexType: "hnsw"},
			},
			expected: []*models.Class{
				classWithDefaultsSet(t, "OtherClass"),
			},
			expErr: false,
		},
		{
			name:          "class does not exist",
			classToDelete: "C1",
			existing: []*models.Class{
				{Class: "OtherClass", VectorIndexType: "hnsw"},
			},
			expected: []*models.Class{
				classWithDefaultsSet(t, "OtherClass"),
			},
			expErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})

			fakeMetaHandler.On("DeleteClass", mock.Anything, mock.Anything, mock.Anything).Return(nil)

			err := handler.DeleteClass(ctx, nil, test.classToDelete)
			if test.expErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expErrMsg)
			} else {
				require.Nil(t, err)
			}
			fakeMetaHandler.AssertExpectations(t)
		})
	}
}

func classWithDefaultsSet(t *testing.T, name string) *models.Class {
	class := &models.Class{Class: name, VectorIndexType: "hnsw"}

	sc, err := shardingConfig.ParseConfig(map[string]interface{}{}, 1)
	require.Nil(t, err)

	class.ShardingConfig = sc

	class.VectorIndexConfig = fakeVectorConfig{}
	class.ReplicationConfig = &models.ReplicationConfig{Factor: 1}

	return class
}
