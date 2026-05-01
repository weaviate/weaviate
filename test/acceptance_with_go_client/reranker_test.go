//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
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
		ModuleConfig: map[string]any{
			"reranker-dummy": map[string]any{},
		},
	}
	require.Nil(t, classCreator.WithClass(&class).Do(ctx))
	uids := []string{uuid.New().String(), uuid.New().String()}
	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]any{"first": "apple", "second": "longlong"},
	).WithID(uids[0]).WithVector([]float32{1, 0}).Do(ctx)
	require.Nil(t, err)

	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]any{"first": "apple", "second": "longlonglong"},
	).WithID(uids[1]).WithVector([]float32{1, 0}).Do(ctx)
	require.Nil(t, err)
	nv := graphql.NearVectorArgumentBuilder{}

	// vector search and non-vector search take different codepaths to the storage object. We need to make sure that
	// for both paths all the necessary properties are unmarshalled from binary, even if they are not requested by the
	// user.
	t.Run("Rerank with vector search", func(t *testing.T) {
		fields := []graphql.Field{
			{Name: "_additional{id}"},
			{Name: "_additional{rerank(property: \"second\",query: \"apple\" ){score}}"},
		}
		result, err := c.GraphQL().Get().WithClassName(className).WithNearVector(nv.WithVector([]float32{1, 0})).WithFields(fields...).Do(ctx)
		require.Nil(t, err)

		expected := []float64{12, 8}
		for i := 0; i < 2; i++ {
			rerankScore := result.Data["Get"].(map[string]any)[className].([]any)[i].(map[string]any)["_additional"].(map[string]any)["rerank"].([]any)[0].(map[string]any)["score"].(float64)
			require.Equal(t, rerankScore, expected[i])
		}
	})

	t.Run("Rerank without vector search", func(t *testing.T) {
		fields := []graphql.Field{
			{Name: "_additional{id}"},
			{Name: "_additional{rerank(property: \"second\",query: \"apple\" ){score}}"},
		}
		result, err := c.GraphQL().Get().WithClassName(className).WithFields(fields...).Do(ctx)
		require.Nil(t, err)

		expected := []float64{12, 8}
		for i := 0; i < 2; i++ {
			rerankScore := result.Data["Get"].(map[string]any)[className].([]any)[i].(map[string]any)["_additional"].(map[string]any)["rerank"].([]any)[0].(map[string]any)["score"].(float64)
			require.Equal(t, rerankScore, expected[i])
		}
	})
}

func TestReRanker_WithHybrid_Search(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := "HybridRerankerTest"
	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	// defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	// 1. Create a collection with reranker module enabled, with 2 properties: title and description.
	// A named vector "title" is defined using text2vec-contextionary, vectorizing both
	// title and description properties.
	classCreator := c.Schema().ClassCreator()
	class := models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{string(schema.DataTypeText)},
			},
			{
				Name:     "description",
				DataType: []string{string(schema.DataTypeText)},
			},
		},
		ModuleConfig: map[string]any{
			"reranker-dummy": map[string]any{},
		},
		VectorConfig: map[string]models.VectorConfig{
			"title": {
				Vectorizer: map[string]any{
					"text2vec-contextionary": map[string]any{
						"properties":         []string{"title", "description"},
						"vectorizeClassName": false,
					},
				},
				VectorIndexType: "hnsw",
			},
		},
	}
	require.Nil(t, classCreator.WithClass(&class).Do(ctx))

	// 2. Generate 10 objects and insert them into Weaviate
	testData := []struct {
		title       string
		description string
	}{
		{"Python Programming", "Learn Python programming from scratch to advanced concepts"},
		{"JavaScript Basics", "Introduction to JavaScript for web development"},
		{"Go Web Services", "Building REST APIs with Go programming language"},
		{"Python Data Science", "Data analysis and machine learning with Python"},
		{"JavaScript Advanced", "Advanced JavaScript patterns and best practices"},
		{"Go Concurrency", "Mastering concurrent programming in Go"},
		{"Python Automation", "Automate tasks with Python scripts"},
		{"JavaScript Frameworks", "React, Vue, and Angular frameworks explained"},
		{"Go Microservices", "Building scalable microservices with Go"},
	}

	uids := make([]string, len(testData))
	for i, data := range testData {
		uids[i] = uuid.New().String()
		_, err = c.Data().Creator().
			WithClassName(className).
			WithProperties(map[string]any{
				"title":       data.title,
				"description": data.description,
			}).
			WithID(uids[i]).
			Do(ctx)
		require.Nil(t, err)
	}

	// 3. Perform hybrid search with some title and perform rerank on it
	t.Run("Hybrid search with rerank", func(t *testing.T) {
		// First do a hybrid search
		hybridBuilder := c.GraphQL().HybridArgumentBuilder().WithQuery("programming").WithProperties([]string{"title"}).WithAlpha(0.5)

		fields := []graphql.Field{
			{Name: "_additional{id}"},
			{Name: "title"},
			{Name: "description"},
			{Name: "_additional{rerank(property: \"title\", query: \"Python\"){score}}"},
		}

		result, err := c.GraphQL().Get().
			WithClassName(className).
			WithHybrid(hybridBuilder).
			WithFields(fields...).
			Do(ctx)
		require.Nil(t, err)
		require.NotNil(t, result)

		// 4. Check that we get some results back
		getResult, ok := result.Data["Get"].(map[string]any)
		require.True(t, ok, "Get result should be a map")

		classResult, ok := getResult[className].([]any)
		require.True(t, ok, "Class result should be an array")
		require.GreaterOrEqual(t, len(classResult), 1, "Should have at least 1 result")

		// Check that rerank scores are present
		firstResult := classResult[0].(map[string]any)
		additional, ok := firstResult["_additional"].(map[string]any)
		require.True(t, ok, "_additional should be a map")

		// Check rerank field exists
		rerank, ok := additional["rerank"].([]any)
		require.True(t, ok, "rerank should be an array")
		require.GreaterOrEqual(t, len(rerank), 1, "Should have at least 1 rerank result")

		// Verify the score exists
		rerankResult := rerank[0].(map[string]any)
		_, hasScore := rerankResult["score"]
		require.True(t, hasScore, "Rerank result should have a score")

		// Check all results if they contain score
		t.Logf("Hybrid search with rerank returned %d results", len(classResult))
		for i, r := range classResult {
			item := r.(map[string]any)
			title := item["title"]
			add := item["_additional"].(map[string]any)
			rerankResults := add["rerank"].([]any)
			require.True(t, len(rerankResults) > 0)
			score := rerankResults[0].(map[string]any)["score"]
			assert.NotEmpty(t, fmt.Sprintf("%v", score))
			t.Logf("Result %d: %v, score: %v", i+1, title, score)
		}
	})
}
