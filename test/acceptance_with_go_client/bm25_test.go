//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

var paragraphs = []string{
	"Some random text",
	"Other text",
	"completely unrelated",
	"this has nothing to do with the rest",
}

func TestBm25(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	truePointer := true

	cases := []struct{ datatype string }{{datatype: "text[]"}, {datatype: "string[]"}}
	for _, tt := range cases {
		t.Run("arrays "+tt.datatype, func(t *testing.T) {
			className := "Paragraph15845"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{Name: "contents", DataType: []string{tt.datatype}, Tokenization: "word", IndexInverted: &truePointer},
					{Name: "num", DataType: []string{"int"}},
				},
				InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: 1.2, B: 0.75}},
				Vectorizer:          "none",
			}
			require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
			defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

			creator := c.Data().Creator()
			_, err := creator.WithClassName(className).WithProperties(
				map[string]interface{}{"contents": []string{"what a nice day", "what a rainy day"}, "num": 0}).Do(ctx)
			require.Nil(t, err)
			_, err = creator.WithClassName(className).WithProperties(
				map[string]interface{}{"contents": []string{"rain all day", "snow and sun at the same time? How nice"}, "num": 1}).Do(ctx)
			require.Nil(t, err)

			builder := c.GraphQL().Bm25ArgBuilder().WithQuery("nice").WithProperties("contents")
			results, err := c.GraphQL().Get().WithClassName(className).WithBM25(builder).WithFields(graphql.Field{Name: "num"}).Do(ctx)
			require.Nil(t, err)
			result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
			require.Len(t, result, 2)
			require.Equal(t, 0., result[0].(map[string]interface{})["num"])
			require.Equal(t, 1., result[1].(map[string]interface{})["num"])
		})
	}
}
