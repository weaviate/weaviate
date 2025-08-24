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

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modelsext"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

func Test_AddClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("happy path", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		class := &models.Class{
			Class: "NewClass",
			Properties: []*models.Property{
				{DataType: []string{"text"}, Name: "textProp"},
				{DataType: []string{"int"}, Name: "intProp"},
			},
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}
		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)

		_, _, err := handler.AddClass(ctx, nil, class)
		assert.Nil(t, err)

		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("happy path, named vectors", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

		class := &models.Class{
			Class: "NewClass",
			Properties: []*models.Property{
				{DataType: []string{"text"}, Name: "textProp"},
			},
			VectorConfig: map[string]models.VectorConfig{
				"vec1": {
					VectorIndexType: hnswT,
					Vectorizer: map[string]interface{}{
						"text2vec-contextionary": map[string]interface{}{},
					},
				},
			},
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}
		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)

		_, _, err := handler.AddClass(ctx, nil, class)
		require.NoError(t, err)

		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("mixed vector schema creation", func(t *testing.T) {
		handler, _ := newTestHandler(t, &fakeDB{})

		class := &models.Class{
			Class: "NewClass",
			Properties: []*models.Property{
				{DataType: []string{"text"}, Name: "textProp"},
			},
			Vectorizer:      "text2vec-contextionary",
			VectorIndexType: hnswT,
			VectorConfig: map[string]models.VectorConfig{
				"vec1": {
					VectorIndexType: hnswT,
					Vectorizer: map[string]interface{}{
						"text2vec-contextionary": map[string]interface{}{},
					},
				},
			},
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}

		_, _, err := handler.AddClass(ctx, nil, class)
		require.ErrorContains(t, err, "creating a class with both a class level vector index and named vectors is forbidden")
	})

	t.Run("with empty class name", func(t *testing.T) {
		handler, _ := newTestHandler(t, &fakeDB{})
		class := models.Class{ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
		_, _, err := handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, "'' is not a valid class name")
	})

	t.Run("with reserved class name", func(t *testing.T) {
		handler, _ := newTestHandler(t, &fakeDB{})
		class := models.Class{Class: config.DefaultRaftDir, ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
		_, _, err := handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, fmt.Sprintf("parse class name: class name `%s` is reserved", config.DefaultRaftDir))

		class = models.Class{Class: "rAFT", ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
		_, _, err = handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, fmt.Sprintf("parse class name: class name `%s` is reserved", config.DefaultRaftDir))

		class = models.Class{Class: "rAfT", ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
		_, _, err = handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, fmt.Sprintf("parse class name: class name `%s` is reserved", config.DefaultRaftDir))

		class = models.Class{Class: "RaFT", ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
		_, _, err = handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, fmt.Sprintf("parse class name: class name `%s` is reserved", config.DefaultRaftDir))

		class = models.Class{Class: "RAFT", ReplicationConfig: &models.ReplicationConfig{Factor: 1}}
		_, _, err = handler.AddClass(ctx, nil, &class)
		assert.EqualError(t, err, fmt.Sprintf("parse class name: class name `%s` is reserved", config.DefaultRaftDir))
	})

	t.Run("with default params", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		class := models.Class{
			Class:             "NewClass",
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
			UsingBlockMaxWAND:      config.DefaultUsingBlockMaxWAND,
		}
		fakeSchemaManager.On("AddClass", expectedClass, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		_, _, err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("with customized params", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		expectedBM25Config := &models.BM25Config{
			K1: 1.88,
			B:  0.44,
		}
		class := models.Class{
			Class: "NewClass",
			InvertedIndexConfig: &models.InvertedIndexConfig{
				Bm25:              expectedBM25Config,
				UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
			},
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
			UsingBlockMaxWAND:      config.DefaultUsingBlockMaxWAND,
		}
		fakeSchemaManager.On("AddClass", expectedClass, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		_, _, err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		fakeSchemaManager.AssertExpectations(t)
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
			"SomeClass":       {Class: "SomeClass", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
			"SomeOtherClass":  {Class: "SomeOtherClass", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
			"YetAnotherClass": {Class: "YetAnotherClass", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
		}

		runTestCases := func(t *testing.T, testCases []testCase) {
			for i, tc := range testCases {
				t.Run(tc.propName, func(t *testing.T) {
					handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

					class := &models.Class{
						Class: fmt.Sprintf("NewClass_%d", i),
						Properties: []*models.Property{
							{
								Name:         tc.propName,
								DataType:     tc.dataType,
								Tokenization: tc.tokenization,
							},
						},
						Vectorizer:        "none",
						ReplicationConfig: &models.ReplicationConfig{Factor: 1},
					}
					classes[class.Class] = *class

					if tc.callReadOnly {
						call := fakeSchemaManager.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(nil)
						call.RunFn = func(a mock.Arguments) {
							existedClass := classes[a.Get(0).(string)]
							call.ReturnArguments = mock.Arguments{&existedClass}
						}
					}

					// fakeSchemaManager.On("ReadOnlyClass", mock.Anything).Return(&models.Class{Class: classes[tc.dataType[0]].Class, Vectorizer: classes[tc.dataType[0]].Vectorizer})
					if tc.expectedErrMsg == "" {
						fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
						fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
					}

					_, _, err := handler.AddClass(context.Background(), nil, class)
					if tc.expectedErrMsg == "" {
						require.Nil(t, err)
					} else {
						require.EqualError(t, err, tc.expectedErrMsg)
					}
					fakeSchemaManager.AssertExpectations(t)
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
					expectedErrMsg: fmt.Sprintf("tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
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
							expectedErrMsg: fmt.Sprintf("tokenization is not allowed for data type '%s'", dataType),
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
						expectedErrMsg: "tokenization is not allowed for reference data type",
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
							expectedErrMsg: fmt.Sprintf("tokenization '%s' is not allowed for data type '%s'", tokenization, dataType),
						})
					}
				}
			}

			runTestCases(t, testCases)
		})
	})

	t.Run("with invalid settings", func(t *testing.T) {
		handler, _ := newTestHandler(t, &fakeDB{})

		_, _, err := handler.AddClass(ctx, nil, &models.Class{
			Class:             "NewClass",
			VectorIndexType:   "invalid",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		})
		assert.EqualError(t, err, `unrecognized or unsupported vectorIndexType "invalid"`)

		// VectorConfig is invalid VectorIndexType
		_, _, err = handler.AddClass(ctx, nil, &models.Class{
			Class: "NewClass",
			VectorConfig: map[string]models.VectorConfig{
				"custom": {
					VectorIndexType:   "invalid",
					VectorIndexConfig: hnsw.UserConfig{},
					Vectorizer:        map[string]interface{}{"none": map[string]interface{}{}},
				},
			},
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		})
		assert.EqualError(t, err, `target vector "custom": unrecognized or unsupported vectorIndexType "invalid"`)

		// VectorConfig is invalid Vectorizer
		_, _, err = handler.AddClass(ctx, nil, &models.Class{
			Class: "NewClass",
			VectorConfig: map[string]models.VectorConfig{
				"custom": {
					VectorIndexType:   "flat",
					VectorIndexConfig: hnsw.UserConfig{},
					Vectorizer:        map[string]interface{}{"invalid": nil},
				},
			},
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		})
		assert.EqualError(t, err, `target vector "custom": vectorizer: invalid vectorizer "invalid"`)
	})
}

func Test_AddClassWithLimits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("with max collections limit", func(t *testing.T) {
		tests := []struct {
			name          string
			existingCount int
			maxAllowed    int
			expectedError error
		}{
			{
				name:          "under the limit",
				existingCount: 5,
				maxAllowed:    10,
				expectedError: nil,
			},
			{
				name:          "at the limit",
				existingCount: 10,
				maxAllowed:    10,
				expectedError: fmt.Errorf("maximum number of collections (10) reached"),
			},
			{
				name:          "over the limit",
				existingCount: 11,
				maxAllowed:    10,
				expectedError: fmt.Errorf("maximum number of collections (10) reached"),
			},
			{
				name:          "no limit set",
				existingCount: 100,
				maxAllowed:    -1,
				expectedError: nil,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

				// Mock the schema count
				fakeSchemaManager.On("QueryCollectionsCount").Return(tt.existingCount, nil)

				// Set the max collections limit in config
				handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(tt.maxAllowed)

				class := &models.Class{
					Class:             "NewClass",
					Vectorizer:        "none",
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				}

				if tt.expectedError == nil {
					fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
					fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
				}

				_, _, err := handler.AddClass(ctx, nil, class)
				if tt.expectedError != nil {
					require.NotNil(t, err)
					assert.Contains(t, err.Error(), tt.expectedError.Error())
				} else {
					require.Nil(t, err)
				}
				fakeSchemaManager.AssertExpectations(t)
			})
		}
	})

	t.Run("adding dynamic index", func(t *testing.T) {
		for _, tt := range []struct {
			name                 string
			asyncIndexingEnabled bool

			expectError string
		}{
			{
				name:                 "async indexing disabled",
				asyncIndexingEnabled: false,

				expectError: "the dynamic index can only be created under async indexing environment (ASYNC_INDEXING=true)",
			},
			{
				name:                 "async indexing enabled",
				asyncIndexingEnabled: true,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				handler, schemaManager := newTestHandler(t, &fakeDB{})
				handler.asyncIndexingEnabled = tt.asyncIndexingEnabled

				if tt.expectError == "" {
					schemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
					schemaManager.On("QueryCollectionsCount").Return(0, nil)
					defer schemaManager.AssertExpectations(t)
				}

				assertError := func(err error) {
					if tt.expectError != "" {
						require.ErrorContains(t, err, tt.expectError)
					} else {
						require.NoError(t, err)
					}
				}

				_, _, err := handler.AddClass(ctx, nil, &models.Class{
					Class:             "NewClass",
					VectorIndexType:   "dynamic",
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				})
				assertError(err)

				_, _, err = handler.AddClass(ctx, nil, &models.Class{
					Class: "NewClass",
					VectorConfig: map[string]models.VectorConfig{
						"vec1": {
							VectorIndexType: "dynamic",
							Vectorizer:      map[string]any{"text2vec-contextionary": map[string]any{}},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				})
				assertError(err)
			})
		}
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

		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		var properties []*models.Property
		for _, tc := range testCases {
			properties = append(properties, &models.Property{
				Name:         "created_" + tc.propName,
				DataType:     tc.dataType.PropString(),
				Tokenization: tc.tokenization,
			})
		}

		class := models.Class{
			Class:             className,
			Properties:        properties,
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}

		t.Run("create class with all properties", func(t *testing.T) {
			fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
			fakeSchemaManager.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(nil)
			fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
			handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
			_, _, err := handler.AddClass(ctx, nil, &class)
			require.Nil(t, err)
		})

		t.Run("add properties to existing class", func(t *testing.T) {
			for _, tc := range testCases {
				fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
				fakeSchemaManager.On("ReadOnlyClass", mock.Anything, mock.Anything).Return(&class)
				fakeSchemaManager.On("AddProperty", mock.Anything, mock.Anything).Return(nil)
				t.Run("added_"+tc.propName, func(t *testing.T) {
					_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false, &models.Property{
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
			Class:             className,
			Properties:        properties,
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}
		t.Run("create class with all properties", func(t *testing.T) {
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
			fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
			fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
			_, _, err := handler.AddClass(ctx, nil, &class)
			require.Nil(t, err)
			fakeSchemaManager.AssertExpectations(t)
		})

		t.Run("add properties to existing class", func(t *testing.T) {
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
			for _, tc := range testCases {
				t.Run("added_"+tc.propName, func(t *testing.T) {
					prop := &models.Property{
						Name:            "added_" + tc.propName,
						DataType:        tc.dataType.PropString(),
						IndexInverted:   tc.indexInverted,
						IndexFilterable: tc.indexFilterable,
						IndexSearchable: tc.indexSearchable,
					}
					fakeSchemaManager.On("AddProperty", className, []*models.Property{prop}).Return(nil)
					_, _, err := handler.AddClassProperty(ctx, nil, &class, class.Class, false, prop)

					require.Nil(t, err)
				})
			}
			fakeSchemaManager.AssertExpectations(t)
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
					handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer:        "none",
						Class:             test.input,
						ReplicationConfig: &models.ReplicationConfig{Factor: 1},
					}

					if test.valid {
						fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
						fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
					}
					_, _, err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeSchemaManager.AssertExpectations(t)
				})
			}
		})

		t.Run("different class names with valid keywords", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer:        "none",
						Class:             test.input,
						ReplicationConfig: &models.ReplicationConfig{Factor: 1},
					}

					if test.valid {
						fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
						fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
					}
					_, _, err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeSchemaManager.AssertExpectations(t)
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
					handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
						ReplicationConfig: &models.ReplicationConfig{Factor: 1},
					}

					if test.valid {
						fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
						fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
					}
					handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
					_, _, err := handler.AddClass(context.Background(), nil, class)
					assert.Equal(t, test.valid, err == nil)
					fakeSchemaManager.AssertExpectations(t)
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
						ReplicationConfig: &models.ReplicationConfig{Factor: 1},
					}

					if test.valid {
						fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
						fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
					}
					_, _, err := handler.AddClass(context.Background(), nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeSchemaManager.AssertExpectations(t)
				})
			}
		})
	})

	t.Run("when updating an existing class with a new property", func(t *testing.T) {
		t.Run("different property names without keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{
							{
								Name:     "dummyPropSoWeDontRunIntoAllNoindexedError",
								DataType: schema.DataTypeText.PropString(),
							},
						},
						ReplicationConfig: &models.ReplicationConfig{Factor: 1},
					}

					fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
					fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
					_, _, err := handler.AddClass(context.Background(), nil, class)
					require.Nil(t, err)

					property := &models.Property{
						DataType: schema.DataTypeText.PropString(),
						Name:     test.input,
					}
					if test.valid {
						fakeSchemaManager.On("AddProperty", class.Class, []*models.Property{property}).Return(nil)
					}
					_, _, err = handler.AddClassProperty(context.Background(), nil, class, class.Class, false, property)
					t.Log(err)
					require.Equal(t, test.valid, err == nil)
					fakeSchemaManager.AssertExpectations(t)
				})
			}
		})

		t.Run("different property names  with valid keywords for the prop", func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name+" as thing class", func(t *testing.T) {
					handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
					class := &models.Class{
						Vectorizer: "none",
						Class:      "ValidName",
						Properties: []*models.Property{{
							DataType: schema.DataTypeText.PropString(),
							Name:     test.input,
						}},
						ReplicationConfig: &models.ReplicationConfig{Factor: 1},
					}

					if test.valid {
						fakeSchemaManager.On("AddClass", class, mock.Anything).Return(nil)
						fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
					}
					_, _, err := handler.AddClass(ctx, nil, class)
					t.Log(err)
					assert.Equal(t, test.valid, err == nil)
					fakeSchemaManager.AssertExpectations(t)
				})
			}
		})
	})
}

// As of now, most class settings are immutable, but we need to allow some
// specific updates, such as the vector index config
func Test_UpdateClass(t *testing.T) {
	t.Run("class not found", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		fakeSchemaManager.On("ReadOnlyClass", "WrongClass", mock.Anything).Return(nil)
		fakeSchemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(ErrNotFound)

		err := handler.UpdateClass(context.Background(), nil, "WrongClass", &models.Class{ReplicationConfig: &models.ReplicationConfig{Factor: 1}})
		require.ErrorIs(t, err, ErrNotFound)
		fakeSchemaManager.AssertExpectations(t)
	})

	t.Run("fields validation", func(t *testing.T) {
		tests := []struct {
			name          string
			initial       *models.Class
			update        *models.Class
			expectedError error
		}{
			{
				name:    "ChangeName",
				initial: &models.Class{Class: "InitialName", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				update:  &models.Class{Class: "UpdatedName", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				expectedError: fmt.Errorf(
					"class name is immutable: " +
						"attempted change from \"InitialName\" to \"UpdatedName\""),
			},
			{
				name:    "ModifyVectorizer",
				initial: &models.Class{Class: "InitialName", Vectorizer: "model1", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				update:  &models.Class{Class: "InitialName", Vectorizer: "model2", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				expectedError: fmt.Errorf(
					"vectorizer is immutable: " +
						"attempted change from \"model1\" to \"model2\""),
			},
			{
				name:    "ModifyVectorIndexType",
				initial: &models.Class{Class: "InitialName", VectorIndexType: "hnsw", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				update:  &models.Class{Class: "InitialName", VectorIndexType: "flat", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				expectedError: fmt.Errorf(
					"vector index type is immutable: " +
						"attempted change from \"hnsw\" to \"flat\""),
			},
			{
				name:          "UnsupportedVectorIndex",
				initial:       &models.Class{Class: "InitialName", VectorIndexType: "hnsw", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				update:        &models.Class{Class: "InitialName", VectorIndexType: "lsh", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				expectedError: fmt.Errorf("unsupported vector"),
			},
			{
				name:    "add property to an empty class",
				initial: &models.Class{Class: "InitialName", Vectorizer: "none", ReplicationConfig: &models.ReplicationConfig{Factor: 1}},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name: "newProp",
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: errPropertiesUpdatedInClassUpdate,
			},
			{
				name: "updating second property",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "prop1",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "prop2",
							DataType: schema.DataTypeText.PropString(),
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "prop1",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "prop2",
							DataType: schema.DataTypeInt.PropString(),
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: errPropertiesUpdatedInClassUpdate,
			},
			{
				name: "properties order should not matter",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "prop1",
							DataType: schema.DataTypeText.PropString(),
						},
						{
							Name:     "prop2",
							DataType: schema.DataTypeInt.PropString(),
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "prop2",
							DataType: schema.DataTypeInt.PropString(),
						},
						{
							Name:     "prop1",
							DataType: schema.DataTypeText.PropString(),
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: nil,
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
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: fmt.Errorf(
					"property fields other than description cannot be updated through updating the class. Use the add " +
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
						UsingBlockMaxWAND:      config.DefaultUsingBlockMaxWAND,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
						UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
						UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
						UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
						UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
						UsingBlockMaxWAND: config.DefaultUsingBlockMaxWAND,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
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
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					ModuleConfig: map[string]interface{}{
						"my-module1": map[string]interface{}{
							"my-setting": "updated-value",
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: fmt.Errorf("can only update generative and reranker module configs"),
			},
			{
				name: "adding new module configuration",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					ModuleConfig: map[string]interface{}{
						"my-module1": map[string]interface{}{
							"my-setting": "some-value",
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					ModuleConfig: map[string]interface{}{
						"my-module1": map[string]interface{}{
							"my-setting": "some-value",
						},
						"my-module2": map[string]interface{}{
							"my-setting": "some-value",
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: nil,
			},
			{
				name: "adding new module configuration for a property",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "text",
							DataType: schema.DataTypeText.PropString(),
							ModuleConfig: map[string]interface{}{
								"my-module1": map[string]interface{}{
									"my-setting": "some-value",
								},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "text",
							DataType: schema.DataTypeText.PropString(),
							ModuleConfig: map[string]interface{}{
								"my-module1": map[string]interface{}{
									"my-setting": "some-value",
								},
								"my-module2": map[string]interface{}{
									"my-setting": "some-value",
								},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: nil,
			},
			{
				name: "updating existing module configuration for a property",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "text",
							DataType: schema.DataTypeText.PropString(),
							ModuleConfig: map[string]interface{}{
								"my-module1": map[string]interface{}{
									"my-setting": "some-value",
								},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "text",
							DataType: schema.DataTypeText.PropString(),
							ModuleConfig: map[string]interface{}{
								"my-module1": map[string]interface{}{
									"my-setting": "new-value",
								},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: errors.New(`module "my-module1" configuration cannot be updated`),
			},
			{
				name: "removing existing module configuration for a property",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "text",
							DataType: schema.DataTypeText.PropString(),
							ModuleConfig: map[string]interface{}{
								"my-module1": map[string]interface{}{
									"my-setting": "some-value",
								},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					Properties: []*models.Property{
						{
							Name:     "text",
							DataType: schema.DataTypeText.PropString(),
							ModuleConfig: map[string]interface{}{
								"my-module2": map[string]interface{}{
									"my-setting": "new-value",
								},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: errors.New(`module "my-module1" configuration was removed`),
			},
			{
				name: "updating vector index config",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					VectorIndexConfig: map[string]interface{}{
						"some-setting": "old-value",
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					VectorIndexConfig: map[string]interface{}{
						"some-setting": "new-value",
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: nil,
			},
			{
				name: "try to turn MT on when it was previously off",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled: false,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled: true,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: fmt.Errorf("enabling multi-tenancy for an existing class is not supported"),
			},
			{
				name: "try to turn MT off when it was previously on",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled: true,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled: false,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: fmt.Errorf("disabling multi-tenancy for an existing class is not supported"),
			},
			{
				name: "change auto tenant creation after creating the class",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled: true,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled:            true,
						AutoTenantCreation: true,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: nil,
			},
			{
				name: "change auto tenant activation after creating the class",
				initial: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled: true,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:      "InitialName",
					Vectorizer: "none",
					MultiTenancyConfig: &models.MultiTenancyConfig{
						Enabled:              true,
						AutoTenantActivation: true,
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: nil,
			},
			{
				name: "adding named vector on a class with legacy index",
				initial: &models.Class{
					Class:             "InitialName",
					Vectorizer:        "text2vec-contextionary",
					VectorIndexType:   hnswT,
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:           "InitialName",
					Vectorizer:      "text2vec-contextionary",
					VectorIndexType: hnswT,
					VectorConfig: map[string]models.VectorConfig{
						"vec1": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: nil,
			},
			{
				name: "adding new vector to a class with named vectors",
				initial: &models.Class{
					Class: "InitialName",
					VectorConfig: map[string]models.VectorConfig{
						"initial": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class: "InitialName",
					VectorConfig: map[string]models.VectorConfig{
						"initial": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
						"new": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
			},
			{
				name: "adding legacy vector to a class with named vectors",
				initial: &models.Class{
					Class: "InitialName",
					VectorConfig: map[string]models.VectorConfig{
						"initial": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:           "InitialName",
					Vectorizer:      "text2vec-contextionary",
					VectorIndexType: hnswT,
					VectorConfig: map[string]models.VectorConfig{
						"initial": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: fmt.Errorf("vectorizer is immutable"),
			},
			{
				name: "removing existing named vector",
				initial: &models.Class{
					Class: "InitialName",
					VectorConfig: map[string]models.VectorConfig{
						"first": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
						"second": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class: "InitialName",
					VectorConfig: map[string]models.VectorConfig{
						"first": {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: fmt.Errorf(`missing config for vector "second"`),
			},
			{
				name: "removing existing legacy vector",
				initial: &models.Class{
					Class:             "InitialName",
					Vectorizer:        "text2vec-contextionary",
					VectorIndexType:   hnswT,
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:             "InitialName",
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: fmt.Errorf("vectorizer is immutable"),
			},
			{
				name: "adding named vector with reserved named on a collection with legacy index",
				initial: &models.Class{
					Class:             "InitialName",
					Vectorizer:        "text2vec-contextionary",
					VectorIndexType:   hnswT,
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				update: &models.Class{
					Class:           "InitialName",
					Vectorizer:      "text2vec-contextionary",
					VectorIndexType: hnswT,
					VectorConfig: map[string]models.VectorConfig{
						modelsext.DefaultNamedVectorName: {
							VectorIndexType: hnswT,
							Vectorizer: map[string]interface{}{
								"text2vec-contextionary": map[string]interface{}{},
							},
						},
					},
					ReplicationConfig: &models.ReplicationConfig{Factor: 1},
				},
				expectedError: fmt.Errorf("vector named %s cannot be created when collection level vector index is configured", modelsext.DefaultNamedVectorName),
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
				ctx := context.Background()

				store := NewFakeStore()
				store.parser = handler.parser

				fakeSchemaManager.On("AddClass", test.initial, mock.Anything).Return(nil)
				fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
				fakeSchemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)
				fakeSchemaManager.On("ReadOnlyClass", test.initial.Class, mock.Anything).Return(test.initial)
				fakeSchemaManager.On("QueryShardingState", mock.Anything).Return(nil, nil)
				if len(test.initial.Properties) > 0 {
					fakeSchemaManager.On("ReadOnlyClass", test.initial.Class, mock.Anything).Return(test.initial)
				}
				handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
				_, _, err := handler.AddClass(ctx, nil, test.initial)
				assert.Nil(t, err)
				store.AddClass(test.initial)

				fakeSchemaManager.On("UpdateClass", mock.Anything, mock.Anything).Return(nil)
				err = handler.UpdateClass(ctx, nil, test.initial.Class, test.update)
				if err == nil {
					err = store.UpdateClass(test.update)
				}

				if test.expectedError == nil {
					assert.NoError(t, err)
				} else {
					assert.ErrorContains(t, err, test.expectedError.Error())
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
	handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

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

		nodes := mocks.NewMockNodeSelector("node1", "node2")
		shardingState, err := sharding.InitState(classRaw.Class, shardingConfig, nodes.LocalName(), nodes.StorageCandidates(), 1, false)
		require.Nil(t, err)

		shardingBytes, err := shardingState.JSON()
		require.Nil(t, err)

		descriptor := backup.ClassDescriptor{Name: classRaw.Class, Schema: schemaBytes, ShardingState: shardingBytes}
		fakeSchemaManager.On("RestoreClass", mock.Anything, mock.Anything).Return(nil)
		err = handler.RestoreClass(context.Background(), &descriptor, map[string]string{})
		assert.Nil(t, err, "class passes validation")
		fakeSchemaManager.AssertExpectations(t)
	}
}

func TestRestoreClass_WithNodeMapping(t *testing.T) {
	classes := []*models.Class{{
		Class:      "Class_A",
		Vectorizer: "none",
	}}

	handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

	for _, classRaw := range classes {
		schemaBytes, err := json.Marshal(classRaw)
		require.Nil(t, err)

		shardingConfig, err := shardingConfig.ParseConfig(nil, 2)
		require.Nil(t, err)

		nodes := mocks.NewMockNodeSelector("node1", "node2")
		shardingState, err := sharding.InitState(classRaw.Class, shardingConfig, nodes.LocalName(), nodes.StorageCandidates(), 2, false)
		require.Nil(t, err)

		shardingBytes, err := shardingState.JSON()
		require.Nil(t, err)

		descriptor := backup.ClassDescriptor{Name: classRaw.Class, Schema: schemaBytes, ShardingState: shardingBytes}
		expectedShardingState := shardingState
		expectedShardingState.ApplyNodeMapping(map[string]string{"node1": "new-node1"})
		expectedShardingState.SetLocalName("")
		fakeSchemaManager.On("RestoreClass", mock.Anything, shardingState).Return(nil)
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
		{
			name:          "class delete should auto transform to GQL convention",
			classToDelete: "c1", // all lower case form
			existing: []*models.Class{
				{Class: "C1", VectorIndexType: "hnsw"}, // GQL form
				{Class: "OtherClass", VectorIndexType: "hnsw"},
			},
			expected: []*models.Class{
				classWithDefaultsSet(t, "OtherClass"), // should still delete `C1` class name
			},
			expErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

			// NOTE: mocking schema manager's `DeleteClass` (not handler's)
			// underlying schemaManager should still work with canonical class name.
			canonical := schema.UppercaseClassName(test.classToDelete)
			fakeSchemaManager.On("DeleteClass", canonical).Return(nil)

			// but layer above like handler's `DeleteClass` should work independent of case sensitivity.
			err := handler.DeleteClass(ctx, nil, test.classToDelete)
			if test.expErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expErrMsg)
			} else {
				require.Nil(t, err)
			}
			fakeSchemaManager.AssertExpectations(t)
		})
	}
}

func Test_GetConsistentClass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name       string
		classToGet string
		expErr     bool
		expErrMsg  string
		existing   []*models.Class
		expected   *models.Class
	}{
		{
			name:       "class exists",
			classToGet: "C1",
			existing: []*models.Class{
				{Class: "C1", VectorIndexType: "hnsw"},
				{Class: "OtherClass", VectorIndexType: "hnsw"},
			},
			expected: classWithDefaultsSet(t, "C1"),
			expErr:   false,
		},
		{
			name:       "class does not exist",
			classToGet: "C1",
			existing: []*models.Class{
				{Class: "OtherClass", VectorIndexType: "hnsw"},
			},
			expected: &models.Class{}, // empty
			expErr:   false,
		},
		{
			name:       "class get should auto transform to GQL convention",
			classToGet: "c1", // lowercase
			existing: []*models.Class{
				{Class: "C1", VectorIndexType: "hnsw"}, // original class is GQL form
				{Class: "OtherClass", VectorIndexType: "hnsw"},
			},
			expected: classWithDefaultsSet(t, "C1"),
			expErr:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})

			// underlying schemaManager should still work with canonical class name.
			canonical := schema.UppercaseClassName(test.classToGet)
			fakeSchemaManager.On("ReadOnlyClassWithVersion", mock.Anything, canonical, mock.Anything).Return(test.expected, nil)

			// but layer above like `GetConsistentClass` should work independent of case sensitivity.
			got, _, err := handler.GetConsistentClass(ctx, nil, test.classToGet, false)
			if test.expErr {
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.expErrMsg)
			} else {
				require.Nil(t, err)
				assert.Equal(t, got, test.expected)
			}
			fakeSchemaManager.AssertExpectations(t)
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

func Test_AddClass_MultiTenancy(t *testing.T) {
	ctx := context.Background()

	t.Run("with MT enabled and no optional settings", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		class := models.Class{
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			Class:              "NewClass",
			Vectorizer:         "none",
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		}

		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
		c, _, err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		assert.False(t, schema.AutoTenantCreationEnabled(c))
		assert.False(t, schema.AutoTenantActivationEnabled(c))
	})

	t.Run("with MT enabled and all optional settings", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		class := models.Class{
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled:              true,
				AutoTenantCreation:   true,
				AutoTenantActivation: true,
			},
			Class:             "NewClass",
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		}

		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
		c, _, err := handler.AddClass(ctx, nil, &class)
		require.Nil(t, err)
		assert.True(t, schema.AutoTenantCreationEnabled(c))
		assert.True(t, schema.AutoTenantActivationEnabled(c))
	})

	t.Run("with MT disabled, but auto tenant creation on", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		class := models.Class{
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false, AutoTenantCreation: true},
			Class:              "NewClass",
			Vectorizer:         "none",
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		}

		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
		_, _, err := handler.AddClass(ctx, nil, &class)
		require.NotNil(t, err)
	})

	t.Run("with MT disabled, but auto tenant activation on", func(t *testing.T) {
		handler, fakeSchemaManager := newTestHandler(t, &fakeDB{})
		class := models.Class{
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false, AutoTenantActivation: true},
			Class:              "NewClass",
			Vectorizer:         "none",
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		}

		fakeSchemaManager.On("AddClass", mock.Anything, mock.Anything).Return(nil)
		fakeSchemaManager.On("QueryCollectionsCount").Return(0, nil)
		handler.schemaConfig.MaximumAllowedCollectionsCount = runtime.NewDynamicValue(-1)
		_, _, err := handler.AddClass(ctx, nil, &class)
		require.NotNil(t, err)
	})
}

func Test_SetClassDefaults(t *testing.T) {
	globalCfg := replication.GlobalConfig{MinimumFactor: 3}
	tests := []struct {
		name           string
		class          *models.Class
		expectedError  string
		expectedFactor int64
	}{
		{
			name:           "ReplicationConfig is nil",
			class:          &models.Class{},
			expectedError:  "",
			expectedFactor: 3,
		},
		{
			name: "ReplicationConfig factor less than MinimumFactor",
			class: &models.Class{
				ReplicationConfig: &models.ReplicationConfig{
					Factor: 2,
				},
			},
			expectedError:  "invalid replication factor: setup requires a minimum replication factor of 3: got 2",
			expectedFactor: 2,
		},
		{
			name: "ReplicationConfig factor less than 1",
			class: &models.Class{
				ReplicationConfig: &models.ReplicationConfig{
					Factor: 0,
				},
			},
			expectedError:  "",
			expectedFactor: 3,
		},
		{
			name: "ReplicationConfig factor greater than or equal to MinimumFactor",
			class: &models.Class{
				ReplicationConfig: &models.ReplicationConfig{
					Factor: 4,
				},
			},
			expectedError:  "",
			expectedFactor: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _ := newTestHandler(t, &fakeDB{})
			err := handler.setClassDefaults(tt.class, globalCfg)
			if tt.expectedError != "" {
				assert.EqualError(t, err, tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedFactor, tt.class.ReplicationConfig.Factor)
		})
	}
}
