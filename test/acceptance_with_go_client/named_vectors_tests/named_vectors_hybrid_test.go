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
	acceptance_with_go_client "acceptance_tests_with_client"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testHybrid(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		cleanup()

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name: "text", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				transformers: {
					Vectorizer: map[string]interface{}{
						text2vecTransformers: map[string]interface{}{
							"vectorizeClassName": false,
						},
					},
					VectorIndexType: "flat",
				},
			},
		}
		t.Run("hybrid with 1 vectorizer", func(t *testing.T) {
			id := "1aa6fbff-461b-4ca5-9ff7-47ccd6d07519"
			// create class
			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)
			// insert object
			objWrapper, err := client.Data().Creator().
				WithClassName(class.Class).
				WithID(id).
				WithProperties(map[string]interface{}{
					"text": "Some text goes here",
				}).
				Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, objWrapper)
			assert.Len(t, objWrapper.Object.Vectors, 1)

			field := graphql.Field{
				Name: "_additional",
				Fields: []graphql.Field{
					{Name: "id"},
				},
			}

			resp, err := client.GraphQL().Get().
				WithClassName(class.Class).
				WithHybrid(client.GraphQL().
					HybridArgumentBuilder().
					WithQuery("Some text goes here").
					WithAlpha(0.5)).
				WithFields(field).
				Do(ctx)
			require.NoError(t, err)
			ids := acceptance_with_go_client.GetIds(t, resp, class.Class)
			require.ElementsMatch(t, ids, []string{id})
		})
	}
}
