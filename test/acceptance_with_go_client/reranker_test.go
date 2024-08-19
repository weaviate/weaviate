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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestReRanker(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := "BigFurryMonsterDog"
	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	classCreator := c.Schema().ClassCreator()
	class := models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "first",
				DataType: []string{string(schema.DataTypeText)},
			},
			{
				Name:     "second",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		ModuleConfig: map[string]interface{}{
			"reranker-dummy": map[string]interface{}{},
		},
	}
	require.Nil(t, classCreator.WithClass(&class).Do(ctx))
	uids := []string{uuid.New().String(), uuid.New().String()}
	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"first": "apple", "second": "longlong"},
	).WithID(uids[0]).Do(ctx)
	require.Nil(t, err)

	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"first": "apple", "second": "longlonglong"},
	).WithID(uids[1]).Do(ctx)
	require.Nil(t, err)

	t.Run("Rerank", func(t *testing.T) {
		fields := []graphql.Field{
			{Name: "_additional{id}"},
			{Name: "_additional{rerank(property: \"second\",query: \"apple\" ){score}}"},
		}
		result, err := c.GraphQL().Get().WithClassName(className).WithFields(fields...).Do(ctx)
		require.Nil(t, err)

		expected := []float64{12, 8}
		for i := 0; i < 2; i++ {
			rerankScore := result.Data["Get"].(map[string]interface{})[className].([]interface{})[i].(map[string]interface{})["_additional"].(map[string]interface{})["rerank"].([]interface{})[0].(map[string]interface{})["score"].(float64)
			require.Equal(t, rerankScore, expected[i])
		}
	})
}
