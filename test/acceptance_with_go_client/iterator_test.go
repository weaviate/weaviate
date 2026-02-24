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
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestAfterUnsetVsEmpty(t *testing.T) {
	c, className := createClientWithClassName(t)

	class := models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "bool", DataType: []string{string(schema.DataTypeBoolean)}},
			{Name: "counter", DataType: []string{string(schema.DataTypeInt)}},
		},
		Vectorizer: "none",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

	numObjs := 100
	for i := 0; i < numObjs; i++ {
		_, err := c.Data().Creator().WithClassName(className).WithProperties(
			map[string]interface{}{"counter": i, "bool": i%2 == 0},
		).Do(ctx)
		require.NoError(t, err)
	}

	getExplicitEmpty, err := c.GraphQL().Get().WithClassName(className).WithLimit(10).WithFields(
		graphql.Field{
			Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
		},
		graphql.Field{Name: "counter"},
	).WithAfter("").Do(ctx)
	require.NoError(t, err)
	require.Nil(t, getExplicitEmpty.Errors)
	objExplicitEmpty := getExplicitEmpty.Data["Get"].(map[string]interface{})[className].([]interface{})

	getUnset, err := c.GraphQL().Get().WithClassName(className).WithLimit(10).WithFields(
		graphql.Field{
			Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
		},
		graphql.Field{Name: "counter"},
	).Do(ctx)
	require.NoError(t, err)
	require.Nil(t, getUnset.Errors)
	objUnset := getUnset.Data["Get"].(map[string]interface{})[className].([]interface{})

	require.Equal(t, objExplicitEmpty, objUnset)
}

func TestIteratorWithFilter(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := "GoldenSunsetFlower"
	require.NoError(t, c.Schema().ClassDeleter().WithClassName(className).Do(ctx))

	class := models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "bool", DataType: []string{string(schema.DataTypeBoolean)}},
			{Name: "counter", DataType: []string{string(schema.DataTypeInt)}},
		},
		Vectorizer: "none",
	}
	require.NoError(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

	trueUUIDs := make(map[string]struct{}, 0)
	numObjs := 100
	for i := 0; i < numObjs; i++ {
		obj, err := c.Data().Creator().WithClassName(className).WithProperties(
			map[string]interface{}{"counter": i, "bool": i%2 == 0},
		).Do(ctx)
		require.NoError(t, err)
		if i%2 == 0 {
			trueUUIDs[string(obj.Object.ID)] = struct{}{}
		}
	}

	found := 0
	var after string
	for {
		get, err := c.GraphQL().Get().WithClassName(className).WithWhere(filters.Where().
			WithPath([]string{"bool"}).
			WithOperator(filters.Equal).
			WithValueBoolean(true)).WithLimit(10).
			WithFields(
				graphql.Field{
					Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
				},
				graphql.Field{Name: "counter"},
			).WithAfter(after).Do(ctx)
		require.NoError(t, err)
		require.Nil(t, get.Errors)
		objs := get.Data["Get"].(map[string]interface{})[className].([]interface{})
		if len(objs) == 0 {
			break
		}
		found += len(objs)

		for _, obj := range objs {
			props := obj.(map[string]interface{})
			require.True(t, int(props["counter"].(float64))%2 == 0)
			id := props["_additional"].(map[string]interface{})["id"].(string)
			_, ok := trueUUIDs[id]
			require.True(t, ok, "Expected to find UUID %s in trueUUIDs")
			delete(trueUUIDs, id) // Make sure each object is only counted once
		}

		after = objs[len(objs)-1].(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string)
	}

	require.Equal(t, numObjs/2, found)
}
