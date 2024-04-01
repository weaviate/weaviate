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

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	UUID3 = "00000000-0000-0000-0000-000000000001"
	UUID4 = "00000000-0000-0000-0000-000000000002"
)

func testCreateSchemaWithVectorizerAndBYOV(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)
		require.Nil(t, client.Schema().AllDeleter().Do(context.Background()))

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		defer cleanup()

		className := "BYOVwithVectorizer"

		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name: "text", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				"byov": {
					Vectorizer: map[string]interface{}{
						"text2vec-contextionary": map[string]interface{}{
							"vectorizeClassName": false,
						},
					},
					VectorIndexType: "hnsw",
				},
				"generate": {
					Vectorizer: map[string]interface{}{
						"text2vec-contextionary": map[string]interface{}{
							"vectorizeClassName": false,
						},
					},
					VectorIndexType: "hnsw",
				},
			},
		}

		require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

		_, err = client.Data().Creator().
			WithClassName(className).
			WithID(UUID3).
			WithProperties(map[string]interface{}{
				"text": "banana",
			}).
			Do(ctx)
		require.NoError(t, err)

		objWithoutVector, err := client.Data().ObjectsGetter().
			WithClassName(className).
			WithID(UUID3).
			WithVector().
			Do(ctx)
		require.NoError(t, err)
		require.Len(t, objWithoutVector, 1)
		require.Len(t, objWithoutVector[0].Vectors["byov"], 300)

		// add an object with the same vector but different properties
		_, err = client.Data().Creator().
			WithClassName(className).
			WithID(UUID4).
			WithProperties(map[string]interface{}{
				"text": "apple",
			}).WithVectors(models.Vectors{"byov": objWithoutVector[0].Vectors["byov"]}).
			Do(ctx)

		// vector "byov" must be the same as the same vector was explicitly given to the second object
		// vector "generated" must be different because it gets generated on different data for both objects
		objWithVector, err := client.Data().ObjectsGetter().
			WithClassName(className).
			WithID(UUID4).
			WithVector().
			Do(ctx)
		require.NoError(t, err)
		require.Len(t, objWithoutVector, 1)
		require.Equal(t, objWithVector[0].Vectors["byov"], objWithoutVector[0].Vectors["byov"])
		require.NotEqual(t, objWithVector[0].Vectors["generate"], objWithoutVector[0].Vectors["generate"])
	}
}
