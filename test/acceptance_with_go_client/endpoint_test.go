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

	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestUpdatingPropertiesWithNil(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := "RandomPinkFlower"
	upperPropName := "SomeProp"
	lowerPropName := "someProp"
	cases := []struct {
		prop1 string
		prop2 string
	}{
		{prop1: upperPropName, prop2: upperPropName},
		{prop1: lowerPropName, prop2: lowerPropName},
		{prop1: upperPropName, prop2: lowerPropName},
		{prop1: lowerPropName, prop2: upperPropName},
	}
	for _, tt := range cases {
		t.Run(tt.prop1+" "+tt.prop2, func(t *testing.T) {
			c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
			defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
			classCreator := c.Schema().ClassCreator()
			class := models.Class{
				Class: className,
				Properties: []*models.Property{{
					Name: tt.prop1,
					// TODO change to method call
					DataType: []string{string(schema.DataTypeText)},
					// TODO change to constant
					Tokenization: "whitespace",
				}},
				InvertedIndexConfig: &models.InvertedIndexConfig{IndexNullState: true},
			}
			require.Nil(t, classCreator.WithClass(&class).Do(ctx))

			_, err := c.Data().Creator().WithClassName(className).WithProperties(
				map[string]interface{}{tt.prop1: "SomeText"},
			).WithID(UUID1).Do(ctx)
			require.Nil(t, err)

			require.Nil(t, c.Data().Updater().WithClassName(className).WithProperties(map[string]interface{}{tt.prop2: nil}).WithID(UUID1).WithMerge().Do(ctx))

			// update should have cleared the object
			getter := c.Data().ObjectsGetter()
			objAfterUpdate, err := getter.WithID(UUID1).WithClassName(className).Do(ctx)
			require.Nil(t, err)
			require.Len(t, objAfterUpdate[0].Properties, 0)

			// test that II has been updated:
			// a) no results for when filtering for old value
			// b) one result when filtering for null values
			filter := filters.Where()
			filter.WithValueString("SomeText")
			filter.WithOperator(filters.Equal)
			filter.WithPath([]string{lowerPropName})
			resultFilter, err := c.GraphQL().Get().WithClassName(className).WithWhere(filter).WithFields(graphql.Field{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}}).Do(ctx)
			require.Nil(t, err)
			require.Len(t, resultFilter.Data["Get"].(map[string]interface{})[className], 0)

			filter = filters.Where()
			filter.WithValueBoolean(true)
			filter.WithOperator("IsNull") // replace with real operator after updating go client
			filter.WithPath([]string{lowerPropName})
			resultFilter, err = c.GraphQL().Get().WithClassName(className).WithWhere(filter).WithFields(graphql.Field{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}}).Do(ctx)
			require.Nil(t, err)
			require.Len(t, resultFilter.Data["Get"].(map[string]interface{})[className], 1)

			// Property is still part of the class
			schemaClass, err := c.Schema().ClassGetter().WithClassName(className).Do(ctx)
			require.Nil(t, err)
			require.Len(t, schemaClass.Properties, 1)
		})
	}
}

func TestUpdateWithVectorVectorizer(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := "TestUpdateWithVectorWithVec"
	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	classCreator := c.Schema().ClassCreator()
	class := models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name: "prop", DataType: []string{string(schema.DataTypeText)},
		}},
		Vectorizer: "text2vec-contextionary",
	}
	require.Nil(t, classCreator.WithClass(&class).Do(ctx))

	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"},
	).WithID(UUID1).Do(ctx)
	require.Nil(t, err)

	// get object to have vector
	objBeforeUpdate, err := c.Data().ObjectsGetter().WithID(UUID1).WithVector().WithClassName(className).Do(ctx)
	require.Nil(t, c.Data().Updater().WithClassName(className).WithVector(objBeforeUpdate[0].Vector).WithProperties(map[string]interface{}{"prop": "Other text"}).WithID(UUID1).WithMerge().Do(ctx))

	// update should not have changed the vector
	objAfterUpdate, err := c.Data().ObjectsGetter().WithID(UUID1).WithVector().WithClassName(className).Do(ctx)
	require.Equal(t, objBeforeUpdate[0].Vector, objAfterUpdate[0].Vector)
}

func TestUpdateWithVectorVectorizerNone(t *testing.T) {
	ctx := context.Background()
	c, err := client.NewClient(client.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	className := "TestUpdateWithVectorNoVec"
	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	classCreator := c.Schema().ClassCreator()
	class := models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name: "prop", DataType: []string{string(schema.DataTypeText)},
		}},
		Vectorizer: "none",
	}
	require.Nil(t, classCreator.WithClass(&class).Do(ctx))

	vecBefore := []float32{0.1, 0.2, 0.3}

	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"},
	).WithID(UUID1).WithVector(vecBefore).Do(ctx)
	require.Nil(t, err)

	// update without vector should not change the vector
	require.Nil(t, c.Data().Updater().WithClassName(className).WithProperties(map[string]interface{}{"prop": "Other text"}).WithID(UUID1).WithMerge().Do(ctx))
	objAfterUpdateNoVector, err := c.Data().ObjectsGetter().WithID(UUID1).WithVector().WithClassName(className).Do(ctx)
	require.ElementsMatch(t, vecBefore, objAfterUpdateNoVector[0].Vector)

	// update with vector should change the vector
	vecAfter := []float32{0.1, 0.2, 0.4}
	require.Nil(t, c.Data().Updater().WithClassName(className).WithProperties(map[string]interface{}{"prop": "Other text"}).WithID(UUID1).WithMerge().WithVector(vecAfter).Do(ctx))
	objAfterUpdateWithVector, err := c.Data().ObjectsGetter().WithID(UUID1).WithVector().WithClassName(className).Do(ctx)
	require.ElementsMatch(t, vecAfter, objAfterUpdateWithVector[0].Vector)
}
