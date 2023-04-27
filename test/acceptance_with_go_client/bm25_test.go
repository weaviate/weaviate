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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

var paragraphs = []string{
	"Some random text",
	"Other text",
	"completely unrelated",
	"this has nothing to do with the rest",
}

var (
	TRUE = true
	ctx  = context.Background()
)

func AddClassAndObjectsArray(t *testing.T, className string, datatype string, c *client.Client) {
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "contents", DataType: []string{datatype}, Tokenization: "word", IndexInverted: &TRUE},
			{Name: "num", DataType: []string{"int"}},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: 1.2, B: 0.75}},
		Vectorizer:          "none",
	}
	require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	creator := c.Data().Creator()
	_, err := creator.WithClassName(className).WithProperties(
		map[string]interface{}{"contents": []string{"nice", "what a rain day"}, "num": 0}).Do(ctx)
	require.Nil(t, err)
	_, err = creator.WithClassName(className).WithProperties(
		map[string]interface{}{"contents": []string{"rain", "snow and sun at once? nice"}, "num": 1}).Do(ctx)
	require.Nil(t, err)
	_, err = creator.WithClassName(className).WithProperties(
		map[string]interface{}{"contents": []string{
			"super long text to get the score down",
			"snow and sun at the same time? How nice",
			"long text without any meaning",
			"just ignore this",
			"this too, it doesn't matter",
		}, "num": 2}).Do(ctx)
	_, err = creator.WithClassName(className).WithProperties(
		map[string]interface{}{"contents": []string{
			"super long text to get the score down",
			"rain is necessary",
			"long text without any meaning",
			"just ignore this",
			"this too, it doesn't matter",
		}, "num": 3}).Do(ctx)
	require.Nil(t, err)
}

func AddClassAndObjectsString(t *testing.T, className string, c *client.Client) {
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "contents", DataType: []string{"text"}, Tokenization: "word", IndexInverted: &TRUE},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: 1.2, B: 0.75}},
		Vectorizer:          "none",
	}
	require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	creator := c.Data().Creator()
	_, err := creator.WithClassName(className).WithProperties(
		map[string]interface{}{"contents": "nice day", "num": 0}).WithVector([]float32{1, 2, 3}).Do(ctx)
	require.Nil(t, err)
	_, err = creator.WithClassName(className).WithProperties(
		map[string]interface{}{"contents": "nice sunny day", "num": 1}).WithVector([]float32{1.1, 2.1, 3.1}).Do(ctx)
	require.Nil(t, err)
	_, err = creator.WithClassName(className).WithVector([]float32{4, 5, 6}).WithProperties(
		map[string]interface{}{"contents": "many words without content. does not matter at all. this is suuuper long and only has one word: nice", "num": 2}).Do(ctx)
	_, err = creator.WithClassName(className).WithVector([]float32{4.1, 4.2, 4.3}).WithProperties(
		map[string]interface{}{"contents": "many words without content. does not matter at all. this is also suuuper long and only has one word: day", "num": 3}).Do(ctx)
	require.Nil(t, err)
}

func TestBm25(t *testing.T) {
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "Paragraph15845"

	cases := []struct{ datatype string }{
		{datatype: string(schema.DataTypeTextArray)},
		// deprecated string
		{datatype: string(schema.DataTypeStringArray)},
	}
	for _, tt := range cases {
		t.Run("arrays "+tt.datatype, func(t *testing.T) {
			AddClassAndObjectsArray(t, className, tt.datatype, c)
			defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

			builder := c.GraphQL().Bm25ArgBuilder().WithQuery("nice rain").WithProperties("contents")
			results, err := c.GraphQL().Get().WithClassName(className).WithBM25(builder).WithFields(graphql.Field{Name: "num"}).Do(ctx)
			require.Nil(t, err)
			result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
			require.Len(t, result, 4)
			require.Equal(t, 0., result[0].(map[string]interface{})["num"])
			require.Equal(t, 1., result[1].(map[string]interface{})["num"])
		})
	}
}

func TestBm25Autocut(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "Paragraph453745"

	AddClassAndObjectsArray(t, className, string(schema.DataTypeTextArray), c)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	cases := []struct {
		autocut    int
		numResults int
	}{
		{autocut: 1, numResults: 2}, {autocut: 2, numResults: 4}, {autocut: -1, numResults: 4 /*disabled*/},
	}
	for _, tt := range cases {
		t.Run("autocut "+fmt.Sprint(tt.autocut), func(t *testing.T) {
			results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(bm25:{query:\"rain nice\", autocut: %d, properties: [\"contents\"]}){num}}}", className, tt.autocut)).Do(ctx)
			require.Nil(t, err)
			result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
			require.Len(t, result, tt.numResults)
			require.Equal(t, 0., result[0].(map[string]interface{})["num"])
			require.Equal(t, 1., result[1].(map[string]interface{})["num"])
		})
	}
}

func TestHybridAutocut(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "Paragraph3936"

	AddClassAndObjectsString(t, className, c)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	cases := []struct {
		autocut    int
		numResults int
	}{
		{autocut: 1, numResults: 2}, {autocut: 2, numResults: 4}, {autocut: -1, numResults: 4 /*disabled*/},
	}
	for _, tt := range cases {
		t.Run("autocut "+fmt.Sprint(tt.autocut), func(t *testing.T) {
			results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(bm25:{query:\"rain nice\", autocut: %d, properties: [\"contents\"]}){num}}}", className, tt.autocut)).Do(ctx)
			require.Nil(t, err)
			result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
			require.Len(t, result, tt.numResults)
			require.Equal(t, 0., result[0].(map[string]interface{})["num"])
			require.Equal(t, 1., result[1].(map[string]interface{})["num"])
		})
	}
}
