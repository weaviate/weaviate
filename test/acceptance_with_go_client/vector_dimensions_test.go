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

package acceptance_with_go_client

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestEdgeVectorDimensions(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	ctx := context.Background()

	objID1 := "00000000-0000-0000-0000-000000000001"
	objID2 := "00000000-0000-0000-0000-000000000002"
	className := "VectorDimensions65k"
	propName := "title"

	// 65535 is the max value for uint16
	maxUint16 := uint16(65535)

	for _, vectorLength := range []uint16{1, 50000, maxUint16} {
		t.Run(fmt.Sprintf("%v vector dimensions", vectorLength), func(t *testing.T) {
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: propName, DataType: []string{string(schema.DataTypeText)}, IndexInverted: &vTrue},
				},
				Vectorizer: "none",
			}

			// delete class if exists and cleanup after test
			c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
			require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
			defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

			t.Run("insert vectors", func(t *testing.T) {
				generateVector := func(dims uint16) []float32 {
					vector := make([]float32, dims)
					for i := range vector {
						vector[i] = r.Float32()
					}
					return vector
				}
				for i, objID := range []string{objID1, objID2} {
					_, err := c.Data().Creator().
						WithClassName(className).WithID(objID).
						WithProperties(map[string]interface{}{
							propName: fmt.Sprintf("title %v", i),
						}).
						WithVector(generateVector(vectorLength)).
						Do(ctx)
					require.Nil(t, err)
				}
			})

			t.Run("check Aggregate", func(t *testing.T) {
				getCount := func(t *testing.T, result *models.GraphQLResponse) int {
					aggregate, ok := result.Data["Aggregate"].(map[string]interface{})
					require.True(t, ok)
					require.NotNil(t, aggregate)
					class, ok := aggregate[className].([]interface{})
					require.True(t, ok)
					require.Len(t, class, 1)
					title, ok := class[0].(map[string]interface{})
					require.True(t, ok)
					require.NotNil(t, title)
					count, ok := title["title"].(map[string]interface{})
					require.True(t, ok)
					require.NotNil(t, count)
					titleCount, ok := count["count"].(float64)
					require.True(t, ok)
					return int(titleCount)
				}
				result, err := c.GraphQL().Aggregate().WithClassName(className).WithFields(graphql.Field{
					Name: propName, Fields: []graphql.Field{
						{Name: "count"},
					},
				}).Do(ctx)
				require.Nil(t, err)
				require.Empty(t, result.Errors)
				assert.Equal(t, 2, getCount(t, result))
			})

			t.Run("check nearObject", func(t *testing.T) {
				nearObject := c.GraphQL().NearObjectArgBuilder().WithID(objID1)
				result, err := c.GraphQL().Get().
					WithClassName(className).
					WithNearObject(nearObject).
					WithFields(graphql.Field{Name: propName}).
					Do(ctx)
				require.Nil(t, err)
				require.Empty(t, result.Errors)
				get, ok := result.Data["Get"].(map[string]interface{})
				require.True(t, ok)
				require.NotNil(t, get)
				class, ok := get[className].([]interface{})
				require.True(t, ok)
				require.Len(t, class, 2)
			})
		})
	}
}
