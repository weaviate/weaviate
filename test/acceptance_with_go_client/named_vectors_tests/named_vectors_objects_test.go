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

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	vectorIndex "github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func testCreateObject(t *testing.T, host string) func(t *testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("multiple named vectors", func(t *testing.T) {
			cleanup()

			className := "NamedVectors"
			text2vecContextionaryName1 := "c11y1"
			text2vecContextionaryName2 := "c11y2"
			text2vecContextionaryName3 := "c11y3"
			text2vecContextionary := "text2vec-contextionary"
			id1 := "00000000-0000-0000-0000-000000000001"

			t.Run("create schema", func(t *testing.T) {
				class := &models.Class{
					Class: className,
					Properties: []*models.Property{
						{
							Name: "text", DataType: []string{schema.DataTypeText.String()},
						},
					},
					VectorConfig: map[string]models.VectorConfig{
						text2vecContextionaryName1: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType:   "hnsw",
							VectorIndexConfig: hnsw.UserConfig{Distance: vectorIndex.DefaultDistanceMetric},
						},
						text2vecContextionaryName2: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "hnsw",
						},
						text2vecContextionaryName3: {
							Vectorizer: map[string]interface{}{
								text2vecContextionary: map[string]interface{}{
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "hnsw",
						},
					},
					Vectorizer: text2vecContextionary,
				}

				err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
				require.NoError(t, err)

				cls, err := client.Schema().ClassGetter().WithClassName(className).Do(ctx)
				require.NoError(t, err)
				assert.Equal(t, class.Class, cls.Class)
				require.NotEmpty(t, cls.VectorConfig)
				require.Len(t, cls.VectorConfig, 3)
				for _, name := range []string{text2vecContextionaryName1, text2vecContextionaryName2, text2vecContextionaryName3} {
					require.NotEmpty(t, cls.VectorConfig[name])
					assert.Equal(t, class.VectorConfig[name].VectorIndexType, cls.VectorConfig[name].VectorIndexType)
					vectorizerConfig, ok := cls.VectorConfig[name].Vectorizer.(map[string]interface{})
					require.True(t, ok)
					require.NotEmpty(t, vectorizerConfig[text2vecContextionary])
				}
			})

			t.Run("create object", func(t *testing.T) {
				objWrapper, err := client.Data().Creator().
					WithClassName(className).
					WithID(id1).
					WithProperties(map[string]interface{}{
						"text": "I like reading books",
					}).
					Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, objWrapper)
				assert.Len(t, objWrapper.Object.Vectors, 3)

				objs, err := client.Data().ObjectsGetter().
					WithClassName(className).
					WithID(id1).
					WithVector().
					Do(ctx)
				require.NoError(t, err)
				require.Len(t, objs, 1)
				require.NotNil(t, objs[0])
				assert.Len(t, objs[0].Vectors, 3)
			})

			t.Run("GraphQL get vectors", func(t *testing.T) {
				where := filters.Where().
					WithPath([]string{"id"}).
					WithOperator(filters.Equal).
					WithValueText(id1)
				field := graphql.Field{
					Name: "_additional",
					Fields: []graphql.Field{
						{Name: "id"},
						{Name: fmt.Sprintf("vectors{%s}", text2vecContextionaryName1)},
					},
				}
				resp, err := client.GraphQL().Get().
					WithClassName(className).
					WithWhere(where).
					WithFields(field).
					Do(ctx)
				require.NoError(t, err)

				ids := acceptance_with_go_client.GetIds(t, resp, className)
				require.ElementsMatch(t, ids, []string{id1})

				resultVectors := acceptance_with_go_client.GetVectors(t, resp, className, text2vecContextionaryName1)
				require.NotEmpty(t, resultVectors[text2vecContextionaryName1])
			})
		})
	}
}
