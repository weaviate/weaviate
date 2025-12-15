//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package acceptance_with_go_client

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestGenerative(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := "BigScaryMonsterDog"
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
			"generative-dummy": map[string]interface{}{},
		},
	}
	require.Nil(t, classCreator.WithClass(&class).Do(ctx))
	uids := []string{uuid.New().String(), uuid.New().String()}
	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"first": "one", "second": "two"},
	).WithID(uids[0]).WithVector([]float32{1, 0}).Do(ctx)
	require.Nil(t, err)

	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"first": "three", "second": "four"},
	).WithID(uids[1]).WithVector([]float32{1, 0}).Do(ctx)
	require.Nil(t, err)
	nv := graphql.NearVectorArgumentBuilder{}

	t.Run("single result", func(t *testing.T) {
		gs := graphql.NewGenerativeSearch().SingleResult("Input: {first} and {second}")

		fields := graphql.Field{
			Name: "_additional{id}",
		}

		result, err := c.GraphQL().Get().WithClassName(className).WithNearVector(nv.WithVector([]float32{1, 0})).WithGenerativeSearch(gs).WithFields(fields).Do(ctx)
		require.Nil(t, err)

		expected := map[string]string{uids[0]: "Input: one and two", uids[1]: "Input: three and four"}
		for i := 0; i < 2; i++ {
			uidReturn := result.Data["Get"].(map[string]interface{})[className].([]interface{})[i].(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string)
			returnString := result.Data["Get"].(map[string]interface{})[className].([]interface{})[i].(map[string]interface{})["_additional"].(map[string]interface{})["generate"].(map[string]interface{})["singleResult"].(string)
			require.NotNil(t, returnString)
			require.True(t, strings.Contains(returnString, expected[uidReturn]), "expected %s to contain %s", returnString, expected[uidReturn])
		}
	})

	t.Run("grouped result with all properties", func(t *testing.T) {
		gs := graphql.NewGenerativeSearch().GroupedResult("summarize")

		result, err := c.GraphQL().Get().WithClassName(className).WithNearVector(nv.WithVector([]float32{1, 0})).WithGenerativeSearch(gs).Do(ctx)
		require.Nil(t, err)

		returnString := result.Data["Get"].(map[string]interface{})[className].([]interface{})[0].(map[string]interface{})["_additional"].(map[string]interface{})["generate"].(map[string]interface{})["groupedResult"].(string)
		require.NotNil(t, returnString)
		expected := "summarize"
		require.True(t, strings.Contains(returnString, expected), "expected %s to contain %s", returnString, expected)

		// order is not guaranteed
		require.True(t, strings.Contains(returnString, "{\"first\":\"one\",\"second\":\"two\"}"), "got &v", returnString)
		require.True(t, strings.Contains(returnString, "{\"first\":\"three\",\"second\":\"four\"}"), "got &v", returnString)
	})

	t.Run("grouped result with selected properties", func(t *testing.T) {
		gs := graphql.NewGenerativeSearch().GroupedResult("summarize", "first")

		result, err := c.GraphQL().Get().WithClassName(className).WithNearVector(nv.WithVector([]float32{1, 0})).WithGenerativeSearch(gs).Do(ctx)
		require.Nil(t, err)

		returnString := result.Data["Get"].(map[string]interface{})[className].([]interface{})[0].(map[string]interface{})["_additional"].(map[string]interface{})["generate"].(map[string]interface{})["groupedResult"].(string)
		require.NotNil(t, returnString)
		expected := "summarize:"
		require.True(t, strings.Contains(returnString, expected), "expected %s to contain %s", returnString, expected)

		// order is not guaranteed
		require.True(t, strings.Contains(returnString, "{\"first\":\"one\"}"), "got &v", returnString)
		require.True(t, strings.Contains(returnString, "{\"first\":\"three\"}"), "got &v", returnString)
		// only "first" was requested, so "second" should not be in the result
		require.False(t, strings.Contains(returnString, "second"), "got &v", returnString)
	})
}

func TestGenerativeUpdate(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := "LionsAreKittyCats"
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
		},
		ModuleConfig: map[string]interface{}{
			"generative-dummy": map[string]interface{}{"first": "second"},
		},
	}

	require.Nil(t, classCreator.WithClass(&class).Do(ctx))
	uids := []string{uuid.New().String(), uuid.New().String()}
	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"first": "one"},
	).WithID(uids[0]).Do(ctx)
	require.Nil(t, err)

	gs := graphql.NewGenerativeSearch().SingleResult("Input: {first}")

	fields := graphql.Field{Name: "_additional{id}"}
	res, err := c.GraphQL().Get().WithClassName(className).WithGenerativeSearch(gs).WithFields(fields).Do(ctx)
	require.NoError(t, err)
	singelResults := res.Data["Get"].(map[string]interface{})[className].([]interface{})[0].(map[string]interface{})["_additional"].(map[string]interface{})["generate"].(map[string]interface{})["singleResult"]
	require.NotNil(t, singelResults)
	require.Contains(t, singelResults, "first")
	require.Contains(t, singelResults, "second")

	class.ModuleConfig = map[string]interface{}{"generative-dummy": map[string]interface{}{"third": "fourth"}}
	require.NoError(t, c.Schema().ClassUpdater().WithClass(&class).Do(ctx))

	res, err = c.GraphQL().Get().WithClassName(className).WithGenerativeSearch(gs).WithFields(fields).Do(ctx)
	require.NoError(t, err)
	require.Nil(t, res.Errors)
	singelResults2 := res.Data["Get"].(map[string]interface{})[className].([]interface{})[0].(map[string]interface{})["_additional"].(map[string]interface{})["generate"].(map[string]interface{})["singleResult"]
	require.NotNil(t, singelResults2)
	require.Contains(t, singelResults2, "third")
	require.Contains(t, singelResults2, "fourth")
	require.NotContains(t, singelResults2, "first")
	require.NotContains(t, singelResults2, "second")
}
