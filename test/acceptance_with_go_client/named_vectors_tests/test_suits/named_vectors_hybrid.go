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
	"time"

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/liutizhong/weaviate-go-client/v4/weaviate"
	"github.com/liutizhong/weaviate-go-client/v4/weaviate/graphql"
	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/schema"
)

func testHybrid(host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		class := &models.Class{
			Class: "TestClass",
			Properties: []*models.Property{
				{
					Name: "text", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "text2", DataType: []string{schema.DataTypeText.String()},
				},
			},
			VectorConfig: map[string]models.VectorConfig{
				transformers: {
					Vectorizer: map[string]interface{}{
						text2vecTransformers: map[string]interface{}{
							"vectorizeClassName": false,
							"sourceProperties":   []string{"text"},
						},
					},
					VectorIndexType: "flat",
				},
				contextionary: {
					Vectorizer: map[string]interface{}{
						text2vecContextionary: map[string]interface{}{
							"vectorizeClassName": false,
							"sourceProperties":   []string{"text2"},
						},
					},
					VectorIndexType: "flat",
				},
			},
		}

		cleanup := func() {
			err := client.Schema().ClassDeleter().WithClassName(class.Class).Do(context.Background())
			require.Nil(t, err)
		}

		field := graphql.Field{
			Name: "_additional",
			Fields: []graphql.Field{
				{Name: "id"},
			},
		}
		id := "1aa6fbff-461b-4ca5-9ff7-47ccd6d07519"
		id2 := "1aa6fbff-463b-4ca5-9ff7-47ccd6d07519"

		t.Run("hybrid with 1 vectorizer", func(t *testing.T) {
			// create class
			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			defer cleanup()
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
			assert.Len(t, objWrapper.Object.Vectors, 2)

			resp, err := client.GraphQL().Get().
				WithClassName(class.Class).
				WithHybrid(client.GraphQL().
					HybridArgumentBuilder().
					WithQuery("Some text goes here").
					WithAlpha(0.5).WithTargetVectors(transformers)).
				WithFields(field).
				Do(ctx)
			require.NoError(t, err)
			ids := acceptance_with_go_client.GetIds(t, resp, class.Class)
			require.ElementsMatch(t, ids, []string{id})
		})

		t.Run("hybrid with 2 vectorizers", func(t *testing.T) {
			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			defer cleanup()
			require.NoError(t, err)
			// insert object
			objWrapper, err := client.Data().Creator().
				WithClassName(class.Class).
				WithID(id).
				WithProperties(map[string]interface{}{
					"text":  "apple",
					"text2": "mountain",
				}).
				Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, objWrapper)
			assert.Len(t, objWrapper.Object.Vectors, 2)

			_, err = client.Data().Creator().
				WithClassName(class.Class).
				WithID(id2).
				WithProperties(map[string]interface{}{
					"text":  "mountain",
					"text2": "apple",
				}).
				Do(ctx)
			require.NoError(t, err)

			var ids []string
			require.Eventually(t, func() bool {
				resp, err := client.GraphQL().Get().
					WithClassName(class.Class).
					WithHybrid(client.GraphQL().
						HybridArgumentBuilder().
						WithQuery("apple").
						WithAlpha(1).WithTargetVectors(transformers, contextionary)).
					WithFields(field).
					Do(ctx)
				require.NoError(t, err)
				ids = acceptance_with_go_client.GetIds(t, resp, class.Class)
				return len(ids) == 2
			}, 5*time.Second, 1*time.Second)

			require.ElementsMatch(t, ids, []string{id, id2})
		})
	}
}
