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

func testMixedVectorsCreateObject(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.NoError(t, err)

		require.NoError(t, client.Schema().AllDeleter().Do(context.Background()))

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:     "text",
					DataType: schema.DataTypeText.PropString(),
				},
			},
			Vectorizer: text2vecContextionary,
			ModuleConfig: map[string]interface{}{
				text2vecContextionary: map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"contextionary": {
					Vectorizer: map[string]interface{}{
						text2vecContextionary: map[string]interface{}{
							"vectorizeClassName": true,
						},
					},
					VectorIndexType: "hnsw",
				},
				"contextionary_without_class_name": {
					Vectorizer: map[string]interface{}{
						text2vecContextionary: map[string]interface{}{
							"vectorizeClassName": false,
						},
					},
					VectorIndexType: "hnsw",
				},
				"transformers": {
					Vectorizer: map[string]interface{}{
						text2vecTransformers: map[string]interface{}{},
					},
					VectorIndexType: "flat",
				},
			},
		}
		require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

		objWrapper, err := client.Data().Creator().
			WithClassName(className).
			WithID(id1).
			WithProperties(map[string]interface{}{
				"text": "Lorem ipsum dolor sit amet",
			}).
			Do(ctx)
		require.NoError(t, err)

		obj := objWrapper.Object
		require.NotNil(t, obj)

		assert.Len(t, obj.Vector, 300)

		require.Len(t, obj.Vectors, 3)
		assert.Equal(t, []float32(obj.Vector), obj.Vectors["contextionary"].([]float32))
		assert.Len(t, obj.Vectors["contextionary_without_class_name"], 300)
		assert.Len(t, obj.Vectors["transformers"], 384)

		// as these vectors were made using different module parameters, they should be different
		assert.NotEqual(t, obj.Vector, obj.Vectors["contextionary_without_class_name"])
	}
}
