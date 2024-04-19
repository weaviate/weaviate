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
	"testing"

	"github.com/google/uuid"
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

func AddClassAndObjects(t *testing.T, className string, datatype string, c *client.Client, vectorizer string) {
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "contents", DataType: []string{datatype}, Tokenization: "word", IndexFilterable: &TRUE, IndexSearchable: &TRUE},
			{Name: "num", DataType: []string{"int"}},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{Bm25: &models.BM25Config{K1: 1.2, B: 0.75}},
		Vectorizer:          vectorizer,
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
			result_int := results.Data["Get"].(map[string]interface{})[className]
			result := result_int.([]interface{})
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

			results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(%s:{query:\"hello\", properties: [\"%s\"] %s} ){_additional{id score}}}}", className, tt.queryType, tt.property, alpha)).Do(ctx)
			result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
			require.Len(t, result, tt.results)

			if len(result) > 0 && result[0].(map[string]interface{})["score"] != nil {
				val, err := result[0].(map[string]interface{})["score"].(float64)
				require.Nil(t, err)
				require.Greater(t, val, 0.0)
			}
		})
	}
}

func TestAutocut(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "Paragraph453745"

	AddClassAndObjects(t, className, string(schema.DataTypeTextArray), c, "none")
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	searchQuery := []string{"hybrid:{query:\"rain nice\", alpha: 0.0, fusionType: relativeScoreFusion", "bm25:{query:\"rain nice\""}
	cases := []struct {
		autocut    int
		numResults int
	}{
		{autocut: 1, numResults: 2}, {autocut: 2, numResults: 4}, {autocut: -1, numResults: 4 /*disabled*/},
	}
	for _, tt := range cases {
		for _, search := range searchQuery {
			t.Run("autocut "+fmt.Sprint(tt.autocut, " ", search), func(t *testing.T) {
				results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(%s, properties: [\"contents\"]}, autocut: %d){num}}}", className, search, tt.autocut)).Do(ctx)
				require.Nil(t, err)
				result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
				require.Len(t, result, tt.numResults)
				require.Equal(t, 0., result[0].(map[string]interface{})["num"])
				require.Equal(t, 1., result[1].(map[string]interface{})["num"])
			})
		}
	}
}

func TestHybridWithPureVectorSearch(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "ParagraphWithManyWords"

	AddClassAndObjects(t, className, string(schema.DataTypeTextArray), c, "text2vec-contextionary")
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(hybrid: {query: \"rain nice\" properties: [\"contents\"], alpha:1}, autocut: -1){num}}}", className)).Do(ctx)
	require.Nil(t, err)
	result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
	require.Len(t, result, 4)
}

func TestHybridWithNearTextSubsearch(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "ParagraphWithManyWords"

	AddClassAndObjects(t, className, string(schema.DataTypeTextArray), c, "text2vec-contextionary")
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(hybrid: { searches: { nearText: {concepts: [\"rain\", \"nice\"]}},  properties: [\"contents\"], alpha:1}, autocut: -1){num}}}", className)).Do(ctx)
	require.Nil(t, err)
	result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
	require.Len(t, result, 4)
}

func TestHybridWithOnlyVectorSearch(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)

	className := "HybridVectorOnlySearch"
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		Vectorizer: "text2vec-contextionary",
	}
	require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	creator := c.Data().Creator()
	model, err := creator.WithClassName(className).WithProperties(
		map[string]interface{}{"text": "how much wood can a woodchuck chuck?"}).Do(ctx)
	require.Nil(t, err)

	results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(hybrid:{searches: { nearVector: {vector:%v}}}){text}}}", className, model.Object.Vector)).Do(ctx)
	require.Nil(t, err)
	result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
	require.Len(t, result, 1)
}

func TestHybridWithVectorSubsearch(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)

	className := "HybridVectorOnlySearch"
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: []string{"text"}},
		},
		Vectorizer: "text2vec-contextionary",
	}
	require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))

	creator := c.Data().Creator()
	model, err := creator.WithClassName(className).WithProperties(
		map[string]interface{}{"text": "how much wood can a woodchuck chuck?"}).Do(ctx)
	require.Nil(t, err)

	results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(hybrid:{searches: { nearVector: { vector:%v}}}){text}}}", className, model.Object.Vector)).Do(ctx)
	require.Nil(t, err)
	result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
	require.Len(t, result, 1)
}

func TestNearVectorAndObjectAutocut(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "YellowAndBlueTrain"

	class := &models.Class{
		Class:      className,
		Vectorizer: "none",
	}
	require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	var uuids []string
	creator := c.Data().Creator()
	vectorNumbers := []float32{1, 1.1, 1.2, 2.0, 2.1, 2.2, 3.1, 3.2, 3.2}
	for _, vectorNumber := range vectorNumbers {
		uuids = append(uuids, uuid.New().String())
		_, err := creator.WithClassName(className).WithVector([]float32{1, 1, 1, 1, 1, vectorNumber}).WithID(uuids[len(uuids)-1]).Do(ctx)
		require.Nil(t, err)
	}

	t.Run("near vector", func(t *testing.T) {
		cases := []struct {
			autocut    int
			numResults int
		}{
			{autocut: 1, numResults: 3}, {autocut: 2, numResults: 6}, {autocut: -1, numResults: 9 /*disabled*/},
		}
		for _, tt := range cases {
			t.Run("autocut "+fmt.Sprint(tt.autocut), func(t *testing.T) {
				results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(nearVector:{vector:[1, 1, 1, 1, 1, 1]}, autocut: %d){_additional{vector}}}}", className, tt.autocut)).Do(ctx)
				require.Nil(t, err)
				result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
				require.Len(t, result, tt.numResults)
			})
		}
	})

	t.Run("near object", func(t *testing.T) {
		cases := []struct {
			autocut    int
			numResults int
		}{
			{autocut: 1, numResults: 3}, {autocut: 2, numResults: 6}, {autocut: -1, numResults: 9 /*disabled*/},
		}
		for _, tt := range cases {
			t.Run("autocut "+fmt.Sprint(tt.autocut), func(t *testing.T) {
				results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(nearObject:{id:%q}, autocut: %d){_additional{vector}}}}", className, uuids[0], tt.autocut)).Do(ctx)
				require.Nil(t, err)
				result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
				require.Len(t, result, tt.numResults)
			})
		}
	})
}

func TestHybridExplainScore(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "ParagraphWithManyWords"

	AddClassAndObjects(t, className, string(schema.DataTypeTextArray), c, "text2vec-contextionary")
	creator := c.Data().Creator()
	creator.WithClassName(className).WithProperties(
		map[string]interface{}{"contents": []string{
			"specific",
			"hybrid",
			"search",
			"object",
		}, "num": 4}).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	t.Run("hybrid explainscore 1", func(t *testing.T) {
		results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(hybrid:{query:\"rain nice\", fusionType: rankedFusion, alpha: 0.5, properties: [\"contents\"]}){num _additional { score explainScore id }}}}", className)).Do(ctx)

		require.Nil(t, err)
		result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
		require.Len(t, result, 5)
		for _, r := range result {
			score := r.(map[string]interface{})["_additional"].(map[string]interface{})["score"]
			require.NotNil(t, score)
		}
		explainScore := result[0].(map[string]interface{})["_additional"].(map[string]interface{})["explainScore"].(string)
		require.Contains(t, explainScore, "contributed 0.008333334 to the score")
		require.Contains(t, explainScore, "contributed 0.008196721 to the score")
	})
	t.Run("hybrid explainscore 2", func(t *testing.T) {
		results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(hybrid:{query:\"rain snow sun score\",fusionType: rankedFusion, properties: [\"contents\"]}){num _additional { score explainScore }}}}", className)).Do(ctx)
		require.Nil(t, err)
		result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
		require.Len(t, result, 5)
		for _, r := range result {
			score := r.(map[string]interface{})["_additional"].(map[string]interface{})["score"]
			require.NotNil(t, score)
		}
		explainScore := result[0].(map[string]interface{})["_additional"].(map[string]interface{})["explainScore"].(string)
		require.Contains(t, explainScore, "contributed 0.004166667 to the score")
		require.Contains(t, explainScore, "contributed 0.0125 to the score")
	})
	t.Run("hybrid explainscore relative score fusion", func(t *testing.T) {
		results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(hybrid:{query:\"rain snow sun score\", fusionType: relativeScoreFusion, properties: [\"contents\"]}){num _additional { score explainScore }}}}", className)).Do(ctx)
		require.Nil(t, err)
		result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
		require.Len(t, result, 5)
		for _, r := range result {
			score := r.(map[string]interface{})["_additional"].(map[string]interface{})["score"]
			require.NotNil(t, score)
		}
		explainScore := result[0].(map[string]interface{})["_additional"].(map[string]interface{})["explainScore"].(string)
		require.Contains(t, explainScore, "normalized score: 0.75")
		require.Contains(t, explainScore, "normalized score: 0.25")
	})
}

func TestNearTextAutocut(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})
	c.Schema().AllDeleter().Do(ctx)
	className := "YellowAndBlueSub"

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:         "text",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
		},
		Vectorizer: "text2vec-contextionary",
	}
	require.Nil(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	creator := c.Data().Creator()

	texts := []string{"word", "another word", "another word and", "completely unrelated"}
	for _, text := range texts {
		_, err := creator.WithClassName(className).WithProperties(map[string]interface{}{"text": text}).Do(ctx)
		require.Nil(t, err)
	}
	cases := []struct {
		autocut    int
		numResults int
	}{
		{autocut: 1, numResults: 3}, {autocut: -1, numResults: 4 /*disabled*/},
	}
	for _, tt := range cases {
		t.Run("autocut "+fmt.Sprint(tt.autocut), func(t *testing.T) {
			results, err := c.GraphQL().Raw().WithQuery(fmt.Sprintf("{Get{%s(nearText:{concepts: \"word\"}, autocut: %d){_additional{vector}}}}", className, tt.autocut)).Do(ctx)
			require.Nil(t, err)
			result := results.Data["Get"].(map[string]interface{})[className].([]interface{})
			require.Len(t, result, tt.numResults)
		})
	}
}
