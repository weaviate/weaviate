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

func testCreateSchema(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("one named vector", func(t *testing.T) {
			cleanup()
			className := "NamedVector"
			text2vecOpenAI := "text2vec-openai"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name: "text", DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					"openai": {
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "hnsw",
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			cls, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			assert.Equal(t, class.Class, cls.Class)
			require.NotEmpty(t, cls.VectorConfig)
			require.Len(t, cls.VectorConfig, 1)
			require.NotEmpty(t, cls.VectorConfig["openai"])
			assert.Equal(t, class.VectorConfig["openai"].VectorIndexType, cls.VectorConfig["openai"].VectorIndexType)
			vectorizerConfig, ok := cls.VectorConfig["openai"].Vectorizer.(map[string]interface{})
			require.True(t, ok)
			require.NotEmpty(t, vectorizerConfig[text2vecOpenAI])
		})

		t.Run("multiple named vectors", func(t *testing.T) {
			cleanup()
			className := "NamedVectors"
			name1 := "openai"
			name2 := "cohere"
			text2vecOpenAI := "text2vec-openai"
			text2vecCohere := "text2vec-cohere"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name: "text", DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					name1: {
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "hnsw",
					},
					name2: {
						Vectorizer: map[string]interface{}{
							text2vecCohere: map[string]interface{}{
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "flat",
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			cls, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			assert.Equal(t, class.Class, cls.Class)
			require.NotEmpty(t, cls.VectorConfig)
			require.Len(t, cls.VectorConfig, 2)
			for _, name := range []string{name1, name2} {
				require.NotEmpty(t, cls.VectorConfig[name])
				assert.Equal(t, class.VectorConfig[name].VectorIndexType, cls.VectorConfig[name].VectorIndexType)
				vectorizerConfig, ok := cls.VectorConfig[name].Vectorizer.(map[string]interface{})
				require.True(t, ok)
				vectorizerName := text2vecOpenAI
				if name == name2 {
					vectorizerName = text2vecCohere
				}
				require.NotEmpty(t, vectorizerConfig[vectorizerName])
			}
		})

		t.Run("defaults for vectorizer configs", func(t *testing.T) {
			cleanup()

			className := "NamedVectorsDefaults"
			nameOpenAI := "openai"
			nameCohere := "cohere"
			text2vecOpenAI := "text2vec-openai"
			text2vecCohere := "text2vec-cohere"

			vectorizerConfigData := map[string]struct {
				name         string
				classOptions []string
				propOptions  []string
			}{
				nameOpenAI: {
					name:         text2vecOpenAI,
					classOptions: []string{"vectorizeClassName", "baseURL", "model"},
					propOptions:  []string{"vectorizePropertyName", "skip"},
				},
				nameCohere: {
					name:         text2vecCohere,
					classOptions: []string{"vectorizeClassName"},
					propOptions:  []string{"vectorizePropertyName", "skip"},
				},
			}

			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "text",
						DataType: schema.DataTypeText.PropString(),
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					nameOpenAI: {
						VectorIndexType: "hnsw",
						Vectorizer: map[string]interface{}{
							text2vecOpenAI: map[string]interface{}{},
						},
					},
					nameCohere: {
						VectorIndexType: "flat",
						Vectorizer: map[string]interface{}{
							text2vecCohere: map[string]interface{}{},
						},
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			assert.Equal(t, class.Class, classCreated.Class)

			// class defaults
			require.NotNil(t, classCreated.VectorConfig)
			require.Len(t, classCreated.VectorConfig, len(vectorizerConfigData))
			for vectorName, vectorizer := range vectorizerConfigData {
				require.Contains(t, classCreated.VectorConfig, vectorName)

				vectorConfig := class.VectorConfig[vectorName]
				vectorConfigCreated := classCreated.VectorConfig[vectorName]

				require.NotNil(t, vectorConfigCreated)
				assert.Equal(t, vectorConfig.VectorIndexType, vectorConfigCreated.VectorIndexType)
				assert.NotEmpty(t, vectorConfigCreated.VectorIndexConfig)

				require.NotNil(t, vectorConfigCreated.Vectorizer)
				vectorizers, ok := vectorConfigCreated.Vectorizer.(map[string]interface{})
				require.True(t, ok)
				require.Len(t, vectorizers, 1)
				require.Contains(t, vectorizers, vectorizer.name)
				require.NotNil(t, vectorizers[vectorizer.name])

				vectorizerConfig, ok := vectorizers[vectorizer.name].(map[string]interface{})
				require.True(t, ok)
				require.Len(t, vectorizerConfig, len(vectorizer.classOptions))
				for _, classOption := range vectorizer.classOptions {
					assert.Contains(t, vectorizerConfig, classOption)
				}
			}
			assert.Nil(t, class.ModuleConfig)
			assert.Empty(t, class.Vectorizer)
			assert.Empty(t, class.VectorIndexType)
			assert.Nil(t, class.VectorIndexConfig)

			// props defaults
			for _, prop := range classCreated.Properties {
				require.NotNil(t, prop.ModuleConfig)

				propModuleConfig, ok := prop.ModuleConfig.(map[string]interface{})
				require.True(t, ok)
				require.Len(t, propModuleConfig, len(vectorizerConfigData))

				for _, vectorizer := range vectorizerConfigData {
					require.Contains(t, propModuleConfig, vectorizer.name)
					require.NotNil(t, propModuleConfig[vectorizer.name])

					vectorizerConfig, ok := propModuleConfig[vectorizer.name].(map[string]interface{})
					require.True(t, ok)
					require.Len(t, vectorizerConfig, len(vectorizer.propOptions))
					for _, propOption := range vectorizer.propOptions {
						assert.Contains(t, vectorizerConfig, propOption)
					}
				}
			}
		})
	}
}
