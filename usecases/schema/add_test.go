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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestAddClass(t *testing.T) {
	t.Run("with empty class name", func(t *testing.T) {
		err := newSchemaManager().AddClass(context.Background(),
			nil, &models.Class{})
		require.EqualError(t, err, "'' is not a valid class name")
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

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedBM25Config, mgr.state.ObjectSchema.Classes[0].InvertedIndexConfig.Bm25)
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

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedBM25Config, mgr.state.ObjectSchema.Classes[0].InvertedIndexConfig.Bm25)
	})

	t.Run("with default Stopwords config", func(t *testing.T) {
		mgr := newSchemaManager()

		expectedStopwordConfig := &models.StopwordConfig{
			Preset: stopwords.EnglishPreset,
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewClass"})
		require.Nil(t, err)

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedStopwordConfig, mgr.state.ObjectSchema.Classes[0].InvertedIndexConfig.Stopwords)
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

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expectedStopwordConfig, mgr.state.ObjectSchema.Classes[0].InvertedIndexConfig.Stopwords)
	})

	classNameFromDataTypeAndTokenization := func(dataType schema.DataType, tokenization string) string {
		dtStr := strings.ReplaceAll(string(dataType), "[]", "Array")
		tStr := "empty"
		if tokenization != "" {
			tStr = tokenization
		}

		return fmt.Sprintf("%s_%s", dtStr, tStr)
	}

	t.Run("with valid property tokenization", func(t *testing.T) {
		mgr := newSchemaManager()

		properties := []*models.Property{}
		for _, dt := range append(schema.PrimitiveDataTypes, schema.DeprecatedPrimitiveDataTypes...) {
			properties = append(properties, &models.Property{
				Name:     classNameFromDataTypeAndTokenization(dt, ""),
				DataType: dt.PropString(),
			})
		}
		for _, dt := range []schema.DataType{
			schema.DataTypeText, schema.DataTypeTextArray,
			schema.DataTypeString, schema.DataTypeStringArray,
		} {
			for _, tokenization := range helpers.Tokenizations {
				properties = append(properties, &models.Property{
					Name:         classNameFromDataTypeAndTokenization(dt, tokenization),
					DataType:     dt.PropString(),
					Tokenization: tokenization,
				})
			}
		}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class:      "NewClass",
				Properties: properties,
			})

		require.Nil(t, err)
		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
	})

	t.Run("with invalid property tokenization", func(t *testing.T) {
		type testCase struct {
			name         string
			dataType     []string
			tokenization string
			errorMsg     string
		}

		nonExistingTokenization := "non_existing"

		mgr := newSchemaManager()
		_, err := mgr.addClass(context.Background(), &models.Class{
			Class: "SomeClass",
		})
		require.Nil(t, err)
		_, err = mgr.addClass(context.Background(), &models.Class{
			Class: "SomeOtherClass",
		})
		require.Nil(t, err)
		_, err = mgr.addClass(context.Background(), &models.Class{
			Class: "YetAnotherClass",
		})
		require.Nil(t, err)

		testCases := []testCase{}
		for _, dt := range append(schema.PrimitiveDataTypes, schema.DeprecatedPrimitiveDataTypes...) {
			switch dt {
			case schema.DataTypeString, schema.DataTypeStringArray:
				fallthrough
			case schema.DataTypeText, schema.DataTypeTextArray:
				testCases = append(testCases, testCase{
					name:         classNameFromDataTypeAndTokenization(dt, nonExistingTokenization),
					dataType:     []string{string(dt)},
					tokenization: nonExistingTokenization,
					errorMsg:     fmt.Sprintf("Tokenization '%s' is not allowed for data type '%s'", nonExistingTokenization, dt),
				})
			default:
				for _, tokenization := range append(helpers.Tokenizations, nonExistingTokenization) {
					testCases = append(testCases, testCase{
						name:         classNameFromDataTypeAndTokenization(dt, tokenization),
						dataType:     []string{string(dt)},
						tokenization: tokenization,
						errorMsg:     fmt.Sprintf("Tokenization is not allowed for data type '%s'", dt),
					})
				}
			}
		}
		for i, dataType := range [][]string{
			{"SomeClass"},
			{"SomeOtherClass", "YetAnotherClass"},
		} {
			for _, tokenization := range append(helpers.Tokenizations, nonExistingTokenization) {
				testCases = append(testCases, testCase{
					name:         fmt.Sprintf("RefClass_%d_%s", i, tokenization),
					dataType:     dataType,
					tokenization: tokenization,
					errorMsg:     "Tokenization is not allowed for reference data type",
				})
			}
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := mgr.AddClass(context.Background(),
					nil, &models.Class{
						Class: "NewClass",
						Properties: []*models.Property{
							{
								Name:         tc.name,
								DataType:     tc.dataType,
								Tokenization: tc.tokenization,
							},
						},
					})

				require.EqualError(t, err, tc.errorMsg)
			})
		}
	})

	t.Run("with default vector distance metric", func(t *testing.T) {
		mgr := newSchemaManager()

		expected := fakeVectorConfig{raw: map[string]interface{}{"distance": "cosine"}}

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{Class: "NewClass"})
		require.Nil(t, err)

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expected, mgr.state.ObjectSchema.Classes[0].VectorIndexConfig)
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

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expected, mgr.state.ObjectSchema.Classes[0].VectorIndexConfig)
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

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
		require.Equal(t, "NewClass", mgr.state.ObjectSchema.Classes[0].Class)
		require.Equal(t, expected, mgr.state.ObjectSchema.Classes[0].VectorIndexConfig)
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
}
