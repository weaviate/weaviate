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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testSchemaValidation(t *testing.T, host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("none existent name of the vectorizer module", func(t *testing.T) {
			cleanup()
			className := "NamedVector"
			nonExistenVectorizer := "none_existent_vectorizer_module"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name: "text", DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					nonExistenVectorizer: {
						Vectorizer: map[string]interface{}{
							nonExistenVectorizer: map[string]interface{}{
								"vectorizeClassName": false,
							},
						},
						VectorIndexType: "hnsw",
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.Error(t, err)
			assert.ErrorContains(t, err, fmt.Sprintf("class.VectorConfig.Vectorizer module with name %s doesn't exist", nonExistenVectorizer))
		})

		t.Run("properties check", func(t *testing.T) {
			cleanup()
			className := "NamedVector"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name: "text", DataType: []string{schema.DataTypeText.String()},
					},
				},
				VectorConfig: map[string]models.VectorConfig{
					c11y: {
						Vectorizer: map[string]interface{}{
							text2vecContextionary: map[string]interface{}{
								"vectorizeClassName": false,
								"properties":         []interface{}{"text", 1111},
							},
						},
						VectorIndexType: "hnsw",
					},
				},
			}

			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.Error(t, err)
			assert.ErrorContains(t, err, "properties field value: 1111 must be a string")
		})

		t.Run("very long target vector name", func(t *testing.T) {
			tests := []struct {
				targetVectorName   string
				validationErrorMsg string
			}{
				{
					targetVectorName:   fmt.Sprintf("%s1", transformers_bq_very_long_230_chars),
					validationErrorMsg: "Target vector name should not be longer than 230 characters",
				},
				{
					targetVectorName:   "invalid-characters",
					validationErrorMsg: "Weaviate target vector names are restricted to valid GraphQL names",
				},
			}
			for _, tt := range tests {
				cleanup()
				class := &models.Class{
					Class: className,
					Properties: []*models.Property{
						{
							Name: "text", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorConfig: map[string]models.VectorConfig{
						tt.targetVectorName: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType:   "flat",
							VectorIndexConfig: bqFlatIndexConfig(),
						},
					},
				}

				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.validationErrorMsg)
			}
		})
	}
}
