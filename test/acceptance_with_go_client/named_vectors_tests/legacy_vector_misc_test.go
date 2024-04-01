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

package named_vectors_tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func allLegacyTests(endpoint string) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("[legacy vector] schema validation", testLegacySchemaValidation(endpoint))
		t.Run("[legacy vector] create schema", testLegacyCreateSchema(endpoint))
	}
}

func testLegacySchemaValidation(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("fails with multiple vectorizers in class's module config on create class", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecOpenAI := "text2vec-openai"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				ModuleConfig: map[string]interface{}{
					text2vecOpenAI: map[string]interface{}{
						"vectorizeClassName": true,
					},
					text2vecCohere: map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.Error(t, err)
			assert.ErrorContains(t, err, "multiple vectorizers configured in class's moduleConfig")
			assert.ErrorContains(t, err, text2vecOpenAI)
			assert.ErrorContains(t, err, text2vecCohere)
			assert.ErrorContains(t, err, "class.vectorizer is set to \\\"text2vec-cohere\\\"")

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.Error(t, err)
			assert.Nil(t, classCreated)
		})

		t.Run("fails with different vectorizer set in class's module config on create class", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecOpenAI := "text2vec-openai"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				ModuleConfig: map[string]interface{}{
					text2vecOpenAI: map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.Error(t, err)
			assert.ErrorContains(t, err, "multiple vectorizers configured in class's moduleConfig")
			assert.ErrorContains(t, err, text2vecOpenAI)
			assert.ErrorContains(t, err, text2vecCohere)
			assert.ErrorContains(t, err, "class.vectorizer is set to \\\"text2vec-cohere\\\"")

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.Error(t, err)
			assert.Nil(t, classCreated)
		})

		t.Run("succeeds with correct vectorizer set in class's module config on create class", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				ModuleConfig: map[string]interface{}{
					text2vecCohere: map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			assert.NotNil(t, classCreated)
		})

		t.Run("fails with multiple vectorizers in prop's module config on create class", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecOpenAI := "text2vec-openai"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
						ModuleConfig: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{
								"vectorizePropertyName": true,
							},
							text2vecCohere: map[string]interface{}{
								"vectorizePropertyName": true,
							},
						},
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.Error(t, err)
			assert.ErrorContains(t, err, "multiple vectorizers configured in property's \\\"text\\\" moduleConfig")
			assert.ErrorContains(t, err, text2vecOpenAI)
			assert.ErrorContains(t, err, text2vecCohere)
			assert.ErrorContains(t, err, "class.vectorizer is set to \\\"text2vec-cohere\\\"")

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.Error(t, err)
			assert.Nil(t, classCreated)
		})

		t.Run("fails with different vectorizer set in prop's module config on create class", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecOpenAI := "text2vec-openai"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
						ModuleConfig: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{
								"vectorizePropertyName": true,
							},
						},
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.Error(t, err)
			assert.ErrorContains(t, err, "multiple vectorizers configured in property's \\\"text\\\" moduleConfig")
			assert.ErrorContains(t, err, text2vecOpenAI)
			assert.ErrorContains(t, err, text2vecCohere)
			assert.ErrorContains(t, err, "class.vectorizer is set to \\\"text2vec-cohere\\\"")

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.Error(t, err)
			assert.Nil(t, classCreated)
		})

		t.Run("succeeds with correct vectorizer set in prop's module config on create class", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
						ModuleConfig: map[string]interface{}{
							text2vecCohere: map[string]interface{}{
								"vectorizePropertyName": true,
							},
						},
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			assert.NotNil(t, classCreated)
		})

		t.Run("fails with multiple vectorizers in prop's module config on add property", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecOpenAI := "text2vec-openai"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}
			property := &models.Property{
				Name:     "otherText",
				DataType: schema.DataTypeText.PropString(),
				ModuleConfig: map[string]interface{}{
					text2vecOpenAI: map[string]interface{}{
						"vectorizePropertyName": true,
					},
					text2vecCohere: map[string]interface{}{
						"vectorizePropertyName": true,
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			err = client.Schema().PropertyCreator().WithClassName(className).WithProperty(property).Do(ctx)
			require.Error(t, err)
			assert.ErrorContains(t, err, "multiple vectorizers configured in property's \\\"otherText\\\" moduleConfig")
			assert.ErrorContains(t, err, text2vecOpenAI)
			assert.ErrorContains(t, err, text2vecCohere)
			assert.ErrorContains(t, err, "class.vectorizer is set to \\\"text2vec-cohere\\\"")

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			require.Len(t, classCreated.Properties, 1)
			require.NotNil(t, classCreated.Properties[0])
			assert.Equal(t, classCreated.Properties[0].Name, "text")
		})

		t.Run("fails with different vectorizer set in prop's module config on add property", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecOpenAI := "text2vec-openai"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}
			property := &models.Property{
				Name:     "otherText",
				DataType: schema.DataTypeText.PropString(),
				ModuleConfig: map[string]interface{}{
					text2vecOpenAI: map[string]interface{}{
						"vectorizePropertyName": true,
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			err = client.Schema().PropertyCreator().WithClassName(className).WithProperty(property).Do(ctx)
			require.Error(t, err)
			assert.ErrorContains(t, err, "multiple vectorizers configured in property's \\\"otherText\\\" moduleConfig")
			assert.ErrorContains(t, err, text2vecOpenAI)
			assert.ErrorContains(t, err, text2vecCohere)
			assert.ErrorContains(t, err, "class.vectorizer is set to \\\"text2vec-cohere\\\"")

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			require.Len(t, classCreated.Properties, 1)
			require.NotNil(t, classCreated.Properties[0])
			assert.Equal(t, classCreated.Properties[0].Name, "text")
		})

		t.Run("succeeds with correct vectorizer set in prop's module config on add property", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVector"
			text2vecCohere := "text2vec-cohere"

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}
			property := &models.Property{
				Name:     "otherText",
				DataType: schema.DataTypeText.PropString(),
				ModuleConfig: map[string]interface{}{
					text2vecCohere: map[string]interface{}{
						"vectorizePropertyName": true,
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			err = client.Schema().PropertyCreator().WithClassName(className).WithProperty(property).Do(ctx)
			require.NoError(t, err)

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			require.Len(t, classCreated.Properties, 2)
			require.NotNil(t, classCreated.Properties[0])
			assert.Equal(t, classCreated.Properties[0].Name, "text")
			require.NotNil(t, classCreated.Properties[1])
			assert.Equal(t, classCreated.Properties[1].Name, "otherText")
		})
	}
}

func testLegacyCreateSchema(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("defaults for vectorizer configs", func(t *testing.T) {
			defer cleanup()

			className := "LegacyVectorDefaults"
			text2vecCohere := "text2vec-cohere"
			classOptions := []string{"vectorizeClassName"}
			propOptions := []string{"vectorizePropertyName", "skip"}

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecCohere,
				VectorIndexType: "hnsw",
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			assert.Equal(t, class.Class, classCreated.Class)

			// class defaults
			assert.Equal(t, class.Vectorizer, classCreated.Vectorizer)
			assert.Equal(t, class.VectorIndexType, classCreated.VectorIndexType)
			assert.NotEmpty(t, classCreated.VectorIndexConfig)

			require.NotNil(t, classCreated.ModuleConfig)
			vectorizers, ok := classCreated.ModuleConfig.(map[string]interface{})
			require.True(t, ok)
			require.Len(t, vectorizers, 1)
			require.Contains(t, vectorizers, text2vecCohere)
			require.NotNil(t, vectorizers[text2vecCohere])

			vectorizerConfig, ok := vectorizers[text2vecCohere].(map[string]interface{})
			require.True(t, ok)
			require.Len(t, vectorizerConfig, len(classOptions))
			for _, classOption := range classOptions {
				assert.Contains(t, vectorizerConfig, classOption)
			}
			assert.Nil(t, classCreated.VectorConfig)

			// props defaults
			for _, prop := range classCreated.Properties {
				require.NotNil(t, prop.ModuleConfig)
				vectorizers, ok := prop.ModuleConfig.(map[string]interface{})
				require.True(t, ok)
				require.Len(t, vectorizers, 1)
				require.Contains(t, vectorizers, text2vecCohere)
				require.NotNil(t, vectorizers[text2vecCohere])

				vectorizerConfig, ok := vectorizers[text2vecCohere].(map[string]interface{})
				require.True(t, ok)
				require.Len(t, vectorizerConfig, len(propOptions))
				for _, propOption := range propOptions {
					assert.Contains(t, vectorizerConfig, propOption)
				}
			}
		})
	}
}
