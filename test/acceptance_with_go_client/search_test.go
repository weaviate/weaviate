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

func AddClassAndObjects(t *testing.T, className string, datatype string, c *client.Client) {
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "contents", DataType: []string{datatype}, Tokenization: "word", IndexFilterable: &TRUE, IndexSearchable: &TRUE},
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
}

func TestSearchOnArrays(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	c.Schema().AllDeleter().Do(ctx)

	cases := []struct {
		datatype  schema.DataType
		useHybrid bool // bm25 if not
	}{
		{datatype: schema.DataTypeTextArray, useHybrid: true},
		{datatype: schema.DataTypeTextArray, useHybrid: false},
		// deprecated string
		{datatype: schema.DataTypeStringArray, useHybrid: false},
	}
	for _, tt := range cases {
		t.Run("arrays "+tt.datatype.String(), func(t *testing.T) {
			className := "Paragraph15845"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:            "contents",
						DataType:        tt.datatype.PropString(),
						Tokenization:    models.PropertyTokenizationWord,
						IndexFilterable: &vFalse,
						IndexSearchable: &vTrue,
					},
					{
						Name:            "num",
						DataType:        schema.DataTypeInt.PropString(),
						IndexFilterable: &vTrue,
					},
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

			var results *models.GraphQLResponse
			if tt.useHybrid {
				builder := c.GraphQL().HybridArgumentBuilder().WithQuery("nice").WithAlpha(0)
				results, err = c.GraphQL().Get().WithClassName(className).WithHybrid(builder).WithFields(graphql.Field{Name: "num"}).Do(ctx)
				require.Nil(t, err)
			} else {
				builder := c.GraphQL().Bm25ArgBuilder().WithQuery("nice").WithProperties("contents")
				results, err = c.GraphQL().Get().WithClassName(className).WithBM25(builder).WithFields(graphql.Field{Name: "num"}).Do(ctx)
				require.Nil(t, err)
			}
			result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
			require.Len(t, result, 2)
			require.Equal(t, 0., result[0].(map[string]interface{})["num"])
			require.Equal(t, 1., result[1].(map[string]interface{})["num"])
		})
	}
}

func TestSearchOnSomeProperties(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	c.Schema().AllDeleter().Do(ctx)

	// only one property contains the search term
	cases := []struct {
		queryType string // hybrid or bm25
		property  string
		results   int
	}{
		{queryType: "bm25", property: "one", results: 1},
		{queryType: "hybrid", property: "one", results: 1},
		{queryType: "bm25", property: "two", results: 0},
		{queryType: "hybrid", property: "two", results: 0},
	}
	for _, tt := range cases {
		t.Run("search on some properties "+tt.queryType, func(t *testing.T) {
			className := "Paragraph15845"
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:            "one",
						DataType:        schema.DataTypeText.PropString(),
						Tokenization:    models.PropertyTokenizationWord,
						IndexFilterable: &vFalse,
						IndexSearchable: &vTrue,
					},
					{
						Name:            "two",
						DataType:        schema.DataTypeText.PropString(),
						Tokenization:    models.PropertyTokenizationWord,
						IndexFilterable: &vFalse,
						IndexSearchable: &vTrue,
					},
				},
				InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: 1.2, B: 0.75}},
				Vectorizer:          "none",
			}
			require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
			defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

			creator := c.Data().Creator()
			_, err := creator.WithClassName(className).WithProperties(
				map[string]interface{}{"one": "hello", "two": "world"}).Do(ctx)
			require.Nil(t, err)

			alpha := ""
			if tt.queryType == "hybrid" {
				alpha = "alpha:0" // exclude vector search, it doesn't matter for this testcase
			}

			results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(%s:{query:\"hello\", properties: [\"%s\"] %s} ){_additional{id}}}}", className, tt.queryType, tt.property, alpha)).Do(ctx)
			result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
			require.Len(t, result, tt.results)
		})
	}
}

func TestAutocut(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "Paragraph453745"

	AddClassAndObjects(t, className, string(schema.DataTypeTextArray), c)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	searchQuery := []string{"hybrid:{query:\"rain nice\", alpha: 0, fusion_type: relative_score_fusion", "bm25:{query:\"rain nice\""}
	cases := []struct {
		autocut    int
		numResults int
	}{
		{autocut: 1, numResults: 2}, {autocut: 2, numResults: 4}, {autocut: -1, numResults: 4 /*disabled*/},
	}
	for _, tt := range cases {
		for _, search := range searchQuery {
			t.Run("autocut "+fmt.Sprint(tt.autocut, " ", search), func(t *testing.T) {
				results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(%s, autocut: %d, properties: [\"contents\"]}){num}}}", className, search, tt.autocut)).Do(ctx)
				require.Nil(t, err)
				result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
				require.Len(t, result, tt.numResults)
				require.Equal(t, 0., result[0].(map[string]interface{})["num"])
				require.Equal(t, 1., result[1].(map[string]interface{})["num"])
			})
		}
	}
}
