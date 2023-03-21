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
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted/stopwords"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	t.Run("with valid property tokenization", func(t *testing.T) {
		mgr := newSchemaManager()

		err := mgr.AddClass(context.Background(),
			nil, &models.Class{
				Class: "NewClass",
				Properties: []*models.Property{
					{
						Name:     "stringDefault",
						DataType: []string{"string"},
					},
					{
						Name:         "stringEmpty",
						DataType:     []string{"string"},
						Tokenization: "",
					},
					{
						Name:         "stringWord",
						DataType:     []string{"string"},
						Tokenization: "word",
					},
					{
						Name:         "stringField",
						DataType:     []string{"string"},
						Tokenization: "field",
					},
					{
						Name:     "stringArrayDefault",
						DataType: []string{"string[]"},
					},
					{
						Name:         "stringArrayEmpty",
						DataType:     []string{"string[]"},
						Tokenization: "",
					},
					{
						Name:         "stringArrayWord",
						DataType:     []string{"string[]"},
						Tokenization: "word",
					},
					{
						Name:         "stringArrayField",
						DataType:     []string{"string[]"},
						Tokenization: "field",
					},
					{
						Name:     "textDefault",
						DataType: []string{"text"},
					},
					{
						Name:         "textEmpty",
						DataType:     []string{"text"},
						Tokenization: "",
					},
					{
						Name:         "textWord",
						DataType:     []string{"text"},
						Tokenization: "word",
					},
					{
						Name:     "textArrayDefault",
						DataType: []string{"text[]"},
					},
					{
						Name:         "textArrayEmpty",
						DataType:     []string{"text[]"},
						Tokenization: "",
					},
					{
						Name:         "textArrayWord",
						DataType:     []string{"text[]"},
						Tokenization: "word",
					},
					{
						Name:     "IntDefault",
						DataType: []string{"int"},
					},
					{
						Name:         "IntEmpty",
						DataType:     []string{"int"},
						Tokenization: "",
					},
					{
						Name:     "NumberDefault",
						DataType: []string{"number"},
					},
					{
						Name:         "NumberEmpty",
						DataType:     []string{"number"},
						Tokenization: "",
					},
					{
						Name:     "BoolDefault",
						DataType: []string{"boolean"},
					},
					{
						Name:         "BoolEmpty",
						DataType:     []string{"boolean"},
						Tokenization: "",
					},
					{
						Name:     "DateDefault",
						DataType: []string{"date"},
					},
					{
						Name:         "DateEmpty",
						DataType:     []string{"date"},
						Tokenization: "",
					},
					{
						Name:     "GeoDefault",
						DataType: []string{"geoCoordinates"},
					},
					{
						Name:         "GeoEmpty",
						DataType:     []string{"geoCoordinates"},
						Tokenization: "",
					},
					{
						Name:     "PhoneDefault",
						DataType: []string{"phoneNumber"},
					},
					{
						Name:         "PhoneEmpty",
						DataType:     []string{"phoneNumber"},
						Tokenization: "",
					},
					{
						Name:     "BlobDefault",
						DataType: []string{"blob"},
					},
					{
						Name:         "BlobEmpty",
						DataType:     []string{"blob"},
						Tokenization: "",
					},
					{
						Name:     "IntArrayDefault",
						DataType: []string{"int[]"},
					},
					{
						Name:         "IntArrayEmpty",
						DataType:     []string{"int[]"},
						Tokenization: "",
					},
					{
						Name:     "NumberArrayDefault",
						DataType: []string{"number[]"},
					},
					{
						Name:         "NumberArrayEmpty",
						DataType:     []string{"number[]"},
						Tokenization: "",
					},
					{
						Name:     "BoolArrayDefault",
						DataType: []string{"boolean[]"},
					},
					{
						Name:         "BoolArrayEmpty",
						DataType:     []string{"boolean[]"},
						Tokenization: "",
					},
					{
						Name:     "DateArrayDefault",
						DataType: []string{"date[]"},
					},
					{
						Name:         "DateArrayEmpty",
						DataType:     []string{"date[]"},
						Tokenization: "",
					},
				},
			})
		require.Nil(t, err)

		require.NotNil(t, mgr.state.ObjectSchema)
		require.NotEmpty(t, mgr.state.ObjectSchema.Classes)
	})

	t.Run("with invalid property tokenization", func(t *testing.T) {
		mgr := newSchemaManager()

		type testData struct {
			name         string
			dataType     []string
			tokenization string
			errorMsg     string
		}

		tests := []testData{
			{
				name:         "textField",
				dataType:     []string{"text"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'text'",
			},
			{
				name:         "textArrayField",
				dataType:     []string{"text[]"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'text[]'",
			},
			{
				name:         "textNotExisting",
				dataType:     []string{"text"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'text'",
			},
			{
				name:         "textArrayNotExisting",
				dataType:     []string{"text[]"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'text[]'",
			},
			{
				name:         "intWord",
				dataType:     []string{"int"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'int'",
			},
			{
				name:         "intField",
				dataType:     []string{"int"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'int'",
			},
			{
				name:         "intNotExisting",
				dataType:     []string{"int"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'int'",
			},
			{
				name:         "numberWord",
				dataType:     []string{"number"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'number'",
			},
			{
				name:         "numberField",
				dataType:     []string{"number"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'number'",
			},
			{
				name:         "numberNotExisting",
				dataType:     []string{"number"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'number'",
			},
			{
				name:         "boolWord",
				dataType:     []string{"boolean"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'boolean'",
			},
			{
				name:         "boolField",
				dataType:     []string{"boolean"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'boolean'",
			},
			{
				name:         "boolNotExisting",
				dataType:     []string{"boolean"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'boolean'",
			},
			{
				name:         "dateWord",
				dataType:     []string{"date"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'date'",
			},
			{
				name:         "dateField",
				dataType:     []string{"date"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'date'",
			},
			{
				name:         "dateNotExisting",
				dataType:     []string{"date"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'date'",
			},
			{
				name:         "geoWord",
				dataType:     []string{"geoCoordinates"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'geoCoordinates'",
			},
			{
				name:         "geoField",
				dataType:     []string{"geoCoordinates"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'geoCoordinates'",
			},
			{
				name:         "geoNotExisting",
				dataType:     []string{"geoCoordinates"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'geoCoordinates'",
			},
			{
				name:         "phoneWord",
				dataType:     []string{"phoneNumber"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'phoneNumber'",
			},
			{
				name:         "phoneField",
				dataType:     []string{"phoneNumber"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'phoneNumber'",
			},
			{
				name:         "phoneNotExisting",
				dataType:     []string{"phoneNumber"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'phoneNumber'",
			},
			{
				name:         "blobWord",
				dataType:     []string{"blob"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'blob'",
			},
			{
				name:         "blobField",
				dataType:     []string{"blob"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'blob'",
			},
			{
				name:         "blobNotExisting",
				dataType:     []string{"blob"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'blob'",
			},
			{
				name:         "intArrayWord",
				dataType:     []string{"int[]"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'int[]'",
			},
			{
				name:         "intArrayField",
				dataType:     []string{"int[]"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'int[]'",
			},
			{
				name:         "intArrayNotExisting",
				dataType:     []string{"int[]"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'int[]'",
			},
			{
				name:         "numberArrayWord",
				dataType:     []string{"number[]"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'number[]'",
			},
			{
				name:         "numberArrayField",
				dataType:     []string{"number[]"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'number[]'",
			},
			{
				name:         "numberArrayNotExisting",
				dataType:     []string{"number[]"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'number[]'",
			},
			{
				name:         "booleanArrayWord",
				dataType:     []string{"boolean[]"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'boolean[]'",
			},
			{
				name:         "booleanArrayField",
				dataType:     []string{"boolean[]"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'boolean[]'",
			},
			{
				name:         "booleanArrayNotExisting",
				dataType:     []string{"boolean[]"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'boolean[]'",
			},
			{
				name:         "dateArrayWord",
				dataType:     []string{"date[]"},
				tokenization: "word",
				errorMsg:     "Tokenization 'word' is not allowed for data type 'date[]'",
			},
			{
				name:         "dateArrayField",
				dataType:     []string{"date[]"},
				tokenization: "field",
				errorMsg:     "Tokenization 'field' is not allowed for data type 'date[]'",
			},
			{
				name:         "dateArrayNotExisting",
				dataType:     []string{"date[]"},
				tokenization: "notExisting",
				errorMsg:     "Tokenization 'notExisting' is not allowed for data type 'date[]'",
			},
		}

		for _, td := range tests {
			t.Run(td.name, func(t *testing.T) {
				err := mgr.AddClass(context.Background(),
					nil, &models.Class{
						Class: "NewClass",
						Properties: []*models.Property{
							{
								Name:         td.name,
								DataType:     td.dataType,
								Tokenization: td.tokenization,
							},
						},
					})

				require.EqualError(t, err, td.errorMsg)
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
