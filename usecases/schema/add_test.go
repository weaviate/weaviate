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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestAddClass(t *testing.T) {
	t.Run("with empty class name", func(t *testing.T) {
		err := newSchemaManager().AddClass(context.Background(),
			nil, &models.Class{})
		require.EqualError(t, err, "'' is not a valid class name")
	})

	t.Run("with permuted-casing class names", func(t *testing.T) {
		mgr := newSchemaManager()
		err := mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewClass"})
		require.Nil(t, err)
		err = mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewCLASS"})
		require.NotNil(t, err)
		require.Equal(t,
			"class name \"NewCLASS\" already exists as a permutation of: \"NewClass\". "+
				"class names must be unique when lowercased", err.Error())
	})

	t.Run("with default BM25 params", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedBM25Config := &models.BM25Config{
			K1: config.DefaultBM25k1,
			B:  config.DefaultBM25b,
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewClass"})
		require.Nil(t, err)

		require.NotNil(t, mgr.schemaCache.ObjectSchema)
		require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.schemaCache.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedBM25Config, mgr.schemaCache.ObjectSchema.Classes[0].InvertedIndexConfig.Bm25)
	})

	t.Run("with customized BM25 params", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedBM25Config := &models.BM25Config{
			K1: 1.88,
			B:  0.44,
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{
					Bm25: expectedBM25Config,
				},
			})
		require.Nil(t, err)

		require.NotNil(t, mgr.schemaCache.ObjectSchema)
		require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.schemaCache.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedBM25Config, mgr.schemaCache.ObjectSchema.Classes[0].InvertedIndexConfig.Bm25)
	})

	t.Run("with default Stopwords config", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedStopwordConfig := &models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewClass"})
		require.Nil(t, err)

		require.NotNil(t, mgr.schemaCache.ObjectSchema)
		require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.schemaCache.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedStopwordConfig, mgr.schemaCache.ObjectSchema.Classes[0].InvertedIndexConfig.Stopwords)
	})

	t.Run("with customized Stopwords config", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedStopwordConfig := &models.StopwordConfig{
			Preset:    "none",
			Additions: []string{"monkey", "zebra", "octopus"},
			Removals:  []string{"are"},
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{
					Stopwords: expectedStopwordConfig,
				},
			})
		require.Nil(t, err)

		require.NotNil(t, mgr.schemaCache.ObjectSchema)
		require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.schemaCache.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedStopwordConfig, mgr.schemaCache.ObjectSchema.Classes[0].InvertedIndexConfig.Stopwords)
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

		runTestCases := func(t *testing.T, testCases []testCase, mgr *Manager) {
			for i, tc := range testCases {
				t.Run(tc.propName, func(t *testing.T) {
					err := mgr.AddClass(context.Background(), nil, &models.Class{
						Class: fmt.Sprintf("NewClass_%d", i),
						Properties: []*models.Property{
							{
								Name:         tc.propName,
								DataType:     tc.dataType,
								Tokenization: tc.tokenization,
							},
						},
					})

					if tc.expectedErrMsg == "" {
						require.Nil(t, err)
						require.NotNil(t, mgr.schemaCache.ObjectSchema)
						require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
					} else {
						require.EqualError(t, err, tc.expectedErrMsg)
					}
				})
			}
		}

		t.Run("text/textArray and all tokenizations", func(t *testing.T) {
			testCases := []testCase{}
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

			runTestCases(t, testCases, newSchemaManager())
		})

		t.Run("non text/textArray and all tokenizations", func(t *testing.T) {
			testCases := []testCase{}
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

			runTestCases(t, testCases, newSchemaManager())
		})

		t.Run("non text/textArray and all tokenizations", func(t *testing.T) {
			ctx := context.Background()
			mgr := newSchemaManager()

			_, err := mgr.addClass(ctx, &models.Class{Class: "SomeClass"})
			require.Nil(t, err)
			_, err = mgr.addClass(ctx, &models.Class{Class: "SomeOtherClass"})
			require.Nil(t, err)
			_, err = mgr.addClass(ctx, &models.Class{Class: "YetAnotherClass"})
			require.Nil(t, err)

			testCases := []testCase{}
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

			runTestCases(t, testCases, mgr)
		})

		t.Run("[deprecated string] string/stringArray and all tokenizations", func(t *testing.T) {
			testCases := []testCase{}
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

			runTestCases(t, testCases, newSchemaManager())
		})
	})

	t.Run("with default vector distance metric", func(t *testing.T) {
		mgr := newSchemaManager()

		expected := fakeVectorConfig{raw: map[string]interface{}{"distance": "cosine"}}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewClass"})
		require.Nil(t, err)

		require.NotNil(t, mgr.schemaCache.ObjectSchema)
		require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.schemaCache.ObjectSchema.Classes[0].Class)
		require.Equal(t, expected, mgr.schemaCache.ObjectSchema.Classes[0].VectorIndexConfig)
	})

	t.Run("with default vector distance metric when class already has VectorIndexConfig", func(t *testing.T) {
		mgr := newSchemaManager()

		expected := fakeVectorConfig{raw: map[string]interface{}{
			"distance":               "cosine",
			"otherVectorIndexConfig": "1234",
		}}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				VectorIndexConfig: map[string]interface{}{
					"otherVectorIndexConfig": "1234",
				},
			})
		require.Nil(t, err)

		require.NotNil(t, mgr.schemaCache.ObjectSchema)
		require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.schemaCache.ObjectSchema.Classes[0].Class)
		require.Equal(t, expected, mgr.schemaCache.ObjectSchema.Classes[0].VectorIndexConfig)
	})

	t.Run("with customized distance metric", func(t *testing.T) {
		mgr := newSchemaManager()

		expected := fakeVectorConfig{
			raw: map[string]interface{}{"distance": "l2-squared"},
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				VectorIndexConfig: map[string]interface{}{
					"distance": "l2-squared",
				},
			})
		require.Nil(t, err)

		require.NotNil(t, mgr.schemaCache.ObjectSchema)
		require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.schemaCache.ObjectSchema.Classes[0].Class)
		require.Equal(t, expected, mgr.schemaCache.ObjectSchema.Classes[0].VectorIndexConfig)
	})

	t.Run("with two identical prop names", func(t *testing.T) {
		mgr := newSchemaManager()

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				Properties: []*models.Property{
					{
						Name:     "my_prop",
						DataType: []string{"text"},
					},
					{
						Name:     "my_prop",
						DataType: []string{"int"},
					},
				},
			})
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "conflict for property")
	})

	t.Run("trying to add an identical prop later", func(t *testing.T) {
		mgr := newSchemaManager()

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				Properties: []*models.Property{
					{
						Name:     "my_prop",
						DataType: []string{"text"},
					},
					{
						Name:     "otherProp",
						DataType: []string{"text"},
					},
				},
			})
		require.Nil(t, err)

		attempts := []string{
			"my_prop",   // lowercase, same casing
			"my_Prop",   // lowercase, different casing
			"otherProp", // mixed case, same casing
			"otherprop", // mixed case, all lower
			"OtHerProP", // mixed case, other casing
		}

		for _, propName := range attempts {
			t.Run(propName, func(t *testing.T) {
				err = mgr.AddClassProperty(context.Background(), nil, "NewClass",
					&models.Property{
						Name:     propName,
						DataType: []string{"int"},
					})
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), "conflict for property")
			})
		}
	})

	// To prevent a regression on
	// https://github.com/weaviate/weaviate/issues/2530
	t.Run("with two props that are identical when ignoring casing", func(t *testing.T) {
		mgr := newSchemaManager()

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				Properties: []*models.Property{
					{
						Name:     "my_prop",
						DataType: []string{"text"},
					},
					{
						Name:     "mY_PrOP",
						DataType: []string{"int"},
					},
				},
			})
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), "conflict for property")
	})

	t.Run("with multi tenancy enabled", func(t *testing.T) {
		t.Run("valid multiTenancyConfig", func(t *testing.T) {
			class := &models.Class{
				Class: "NewClass",
				Properties: []*models.Property{
					{
						Name:     "textProp",
						DataType: []string{"text"},
					},
				},
				MultiTenancyConfig: &models.MultiTenancyConfig{
					Enabled: true,
				},
			}
			mgr := newSchemaManager()
			err := mgr.AddClass(context.Background(), nil, class)
			require.Nil(t, err)
			require.NotNil(t, class.ShardingConfig)
			require.Zero(t, class.ShardingConfig.(sharding.Config).DesiredCount)
		})

		t.Run("multiTenancyConfig and shardingConfig both provided", func(t *testing.T) {
			mgr := newSchemaManager()
			err := mgr.AddClass(context.Background(),
				nil,
				&models.Class{
					Class: "NewClass",
					Properties: []*models.Property{
						{
							Name:     "uuidProp",
							DataType: []string{"uuid"},
						},
					},
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled: true,
					},
					ShardingConfig: map[string]interface{}{
						"desiredCount": 2,
					},
				},
			)
			require.NotNil(t, err)
			require.Equal(t, "cannot have both shardingConfig and multiTenancyConfig", err.Error())
		})

		t.Run("multiTenancyConfig and shardingConfig both provided but multi tenancy config is set to false", func(t *testing.T) {
			mgr := newSchemaManager()
			err := mgr.AddClass(context.Background(),
				nil,
				&models.Class{
					Class: "NewClass1",
					Properties: []*models.Property{
						{
							Name:     "uuidProp",
							DataType: []string{"uuid"},
						},
					},
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled: false,
					},
					ShardingConfig: map[string]interface{}{
						"desiredCount": 2,
					},
				},
			)
			require.Nil(t, err)
		})

		t.Run("multiTenancyConfig and shardingConfig both provided but multi tenancy config is empty", func(t *testing.T) {
			mgr := newSchemaManager()
			err := mgr.AddClass(context.Background(),
				nil,
				&models.Class{
					Class: "NewClass",
					Properties: []*models.Property{
						{
							Name:     "uuidProp",
							DataType: []string{"uuid"},
						},
					},
					MultiTenancyConfig: &models.MultiTenancyConfig{},
					ShardingConfig: map[string]interface{}{
						"desiredCount": 2,
					},
				},
			)
			require.Nil(t, err)
		})

		t.Run("multiTenancyConfig and shardingConfig both provided but multi tenancy is nil", func(t *testing.T) {
			mgr := newSchemaManager()
			err := mgr.AddClass(context.Background(),
				nil,
				&models.Class{
					Class: "NewClass",
					Properties: []*models.Property{
						{
							Name:     "uuidProp",
							DataType: []string{"uuid"},
						},
					},
					MultiTenancyConfig: nil,
					ShardingConfig: map[string]interface{}{
						"desiredCount": 2,
					},
				},
			)
			require.Nil(t, err)
		})
	})
}

func TestAddClass_DefaultsAndMigration(t *testing.T) {
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

		mgr := newSchemaManager()
		ctx := context.Background()
		className := "MigrationClass"

		testCases := []testCase{}
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
			properties := []*models.Property{}
			for _, tc := range testCases {
				properties = append(properties, &models.Property{
					Name:         "created_" + tc.propName,
					DataType:     tc.dataType.PropString(),
					Tokenization: tc.tokenization,
				})
			}

			err := mgr.AddClass(ctx, nil, &models.Class{
				Class:      className,
				Properties: properties,
			})

			require.Nil(t, err)
			require.NotNil(t, mgr.schemaCache.ObjectSchema)
			require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
			require.Equal(t, className, mgr.schemaCache.ObjectSchema.Classes[0].Class)
		})

		t.Run("add properties to existing class", func(t *testing.T) {
			for _, tc := range testCases {
				t.Run("added_"+tc.propName, func(t *testing.T) {
					err := mgr.addClassProperty(ctx, className, &models.Property{
						Name:         "added_" + tc.propName,
						DataType:     tc.dataType.PropString(),
						Tokenization: tc.tokenization,
					})

					require.Nil(t, err)
				})
			}
		})

		t.Run("verify defaults and migration", func(t *testing.T) {
			class := mgr.schemaCache.ObjectSchema.Classes[0]
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

		mgr := newSchemaManager()
		ctx := context.Background()
		className := "MigrationClass"

		testCases := []testCase{}

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

		t.Run("create class with all properties", func(t *testing.T) {
			properties := []*models.Property{}
			for _, tc := range testCases {
				properties = append(properties, &models.Property{
					Name:            "created_" + tc.propName,
					DataType:        tc.dataType.PropString(),
					IndexInverted:   tc.indexInverted,
					IndexFilterable: tc.indexFilterable,
					IndexSearchable: tc.indexSearchable,
				})
			}

			err := mgr.AddClass(ctx, nil, &models.Class{
				Class:      className,
				Properties: properties,
			})

			require.Nil(t, err)
			require.NotNil(t, mgr.schemaCache.ObjectSchema)
			require.NotEmpty(t, mgr.schemaCache.ObjectSchema.Classes)
			require.Equal(t, className, mgr.schemaCache.ObjectSchema.Classes[0].Class)
		})

		t.Run("add properties to existing class", func(t *testing.T) {
			for _, tc := range testCases {
				t.Run("added_"+tc.propName, func(t *testing.T) {
					err := mgr.addClassProperty(ctx, className, &models.Property{
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
			class := mgr.schemaCache.ObjectSchema.Classes[0]
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
