//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test_suits

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/schema"
)

func testMixedVectorsCreateSchema(host string) func(t *testing.T) {
	return func(t *testing.T) {
		var (
			ctx = context.Background()

			className      = "MixedVectorDefaults"
			text2vecCohere = "text2vec-cohere"
			text2vecOpenAI = "text2vec-openai"

			vectorizerClassOptions = map[string][]string{
				text2vecOpenAI: {"vectorizeClassName", "baseURL", "model"},
				text2vecCohere: {"vectorizeClassName", "baseUrl", "model", "truncate"},
			}
			propOptions = []string{"vectorizePropertyName", "skip"}

			assertVectorizer = func(moduleConfig interface{}, module string) {
				vectorizers, ok := moduleConfig.(map[string]interface{})
				require.True(t, ok)
				require.Len(t, vectorizers, 1)
				require.Contains(t, vectorizers, module)
				require.NotNil(t, vectorizers[module])

				vectorizerConfig, ok := vectorizers[module].(map[string]interface{})
				require.True(t, ok)

				expectClassOptions := vectorizerClassOptions[module]
				require.Len(t, vectorizerConfig, len(expectClassOptions))
				for _, classOption := range expectClassOptions {
					assert.Contains(t, vectorizerConfig, classOption)
				}
			}
		)

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.NoError(t, err)

		t.Run("defaults for vectorizer configs when adding named vector", func(t *testing.T) {
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer: text2vecOpenAI,
				VectorConfig: map[string]models.VectorConfig{
					"openai": {
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{},
						},
						VectorIndexType: "hnsw",
					},
				},
			}
			createMixedVectorsSchemaHelper(t, client, class)

			class.VectorConfig["cohere"] = models.VectorConfig{
				Vectorizer: map[string]interface{}{
					text2vecCohere: map[string]interface{}{
						"vectorizeClassName": true,
					},
				},
				VectorIndexType: "flat",
			}
			require.NoError(t, client.Schema().ClassUpdater().WithClass(class).Do(ctx))

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			assert.Equal(t, class.Class, classCreated.Class)

			require.Len(t, classCreated.VectorConfig, 2)
			for _, targetVector := range []struct{ name, module, indexType string }{
				{name: "openai", module: text2vecOpenAI, indexType: "hnsw"},
				{name: "cohere", module: text2vecCohere, indexType: "flat"},
			} {
				config := classCreated.VectorConfig[targetVector.name]

				assert.Equal(t, targetVector.indexType, config.VectorIndexType)
				assert.NotEmpty(t, config.VectorIndexConfig)
				assertVectorizer(config.Vectorizer, targetVector.module)
			}

			// props defaults
			for _, prop := range classCreated.Properties {
				require.NotNil(t, prop.ModuleConfig)
				vectorizers, ok := prop.ModuleConfig.(map[string]interface{})
				require.True(t, ok)

				require.Len(t, vectorizers, 2)
				for _, vectorizer := range []string{text2vecOpenAI, text2vecCohere} {
					require.Contains(t, vectorizers, vectorizer)

					vectorizerConfig, ok := vectorizers[vectorizer].(map[string]interface{})
					require.True(t, ok)
					require.Len(t, vectorizerConfig, len(propOptions))
					for _, propOption := range propOptions {
						assert.Contains(t, vectorizerConfig, propOption)
					}
				}
			}
		})

		t.Run("cannot create mixed vector schema", func(t *testing.T) {
			require.NoError(t, client.Schema().AllDeleter().Do(context.Background()))

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecOpenAI,
				VectorIndexType: "hnsw",
				VectorConfig: map[string]models.VectorConfig{
					"openai": {
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{},
						},
						VectorIndexType: "hnsw",
					},
					"cohere": {
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{},
						},
						VectorIndexType: "hnsw",
					},
				},
			}
			err = client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.ErrorContains(t, err, "creating a class with both a class level vector index and named vectors is forbidden")
		})

		t.Run("cannot remove named vector", func(t *testing.T) {
			require.NoError(t, client.Schema().AllDeleter().Do(context.Background()))

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"openai": {
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{},
						},
						VectorIndexType: "hnsw",
					},
					"cohere": {
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{},
						},
						VectorIndexType: "hnsw",
					},
				},
			}
			require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

			delete(class.VectorConfig, "cohere")
			err = client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.ErrorContains(t, err, `missing config for vector \"cohere\"`)
		})

		t.Run("cannot add legacy vector on a collection with named vectors", func(t *testing.T) {
			require.NoError(t, client.Schema().AllDeleter().Do(context.Background()))

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"openai": {
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{},
						},
						VectorIndexType: "hnsw",
					},
				},
			}
			require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

			class.Vectorizer = text2vecContextionary
			class.VectorIndexType = "hnsw"

			err = client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.ErrorContains(t, err, "422")
			require.ErrorContains(t, err, "parse class update")
		})

		t.Run("cannot add named vector called default when legacy schema present", func(t *testing.T) {
			require.NoError(t, client.Schema().AllDeleter().Do(context.Background()))

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				Vectorizer:      text2vecContextionary,
				VectorIndexType: "hnsw",
			}
			require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

			class.VectorConfig = map[string]models.VectorConfig{
				modelsext.DefaultNamedVectorName: {
					Vectorizer:      map[string]interface{}{text2vecContextionary: map[string]interface{}{}},
					VectorIndexType: "hnsw",
				},
			}

			err = client.Schema().ClassUpdater().WithClass(class).Do(ctx)
			require.ErrorContains(t, err, "422")
			require.ErrorContains(t, err, fmt.Sprintf("vector named %s cannot be created when collection level vector index is configured", modelsext.DefaultNamedVectorName))
		})
	}
}

// createMixedVectorsSchemaHelper is a function which creates a mixed vector schema by first creating a
// schema with specified legacy vector and the updates the schema with specified named vectors.
func createMixedVectorsSchemaHelper(t *testing.T, client *wvt.Client, class *models.Class) *models.Class {
	require.NoError(t, client.Schema().ClassDeleter().WithClassName(class.Class).Do(context.Background()))

	capturedNamedVectors := class.VectorConfig
	class.VectorConfig = nil

	require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(context.Background()))

	class.VectorConfig = capturedNamedVectors
	require.NoError(t, client.Schema().ClassUpdater().WithClass(class).Do(context.Background()))

	fetchedSchema, err := client.Schema().ClassGetter().WithClassName(class.Class).Do(context.Background())
	require.NoError(t, err)

	return fetchedSchema
}
