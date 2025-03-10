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

package test_suits

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testMixedVectorsCreateSchema(host string) func(t *testing.T) {
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

			var (
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
					"cohere": {
						Vectorizer: map[string]interface{}{
							text2vecCohere: map[string]interface{}{
								"vectorizeClassName": true,
							},
						},
						VectorIndexType: "flat",
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)

			classCreated, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.NoError(t, err)
			assert.Equal(t, class.Class, classCreated.Class)

			// class defaults
			assert.Equal(t, class.Vectorizer, classCreated.Vectorizer)
			assert.Equal(t, "hnsw", classCreated.VectorIndexType)
			assert.NotEmpty(t, classCreated.VectorIndexConfig)

			assertVectorizer(classCreated.ModuleConfig, text2vecOpenAI)

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
	}
}
