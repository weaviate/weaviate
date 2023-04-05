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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/vector_errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestExploreSingleClassNoVector(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})

	className := "PurpleNavigationDevice"
	id := "0a21ae23-3f22-4f34-b04a-199e701886ef"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	class := classWithoutVectors(className)
	require.Nil(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

	_, err := c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id).Do(ctx)
	require.Nil(t, err)

	no := graphql.NearObjectArgumentBuilder{}
	res, err := c.GraphQL().Explore().WithNearObject(no.WithID(id).WithCertainty(0.8)).WithFields(graphql.Certainty).Do(ctx)
	require.Nil(t, err)
	require.Contains(t, res.Errors[0].Message, vector_errors.ErrNoVectorSearch.Error())
}

func TestExploreSingleClassVector(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})

	className := "GreenOnionSalad"
	id := "0a21ae23-3f22-4f34-b04a-199e701886ef"
	id2 := "0a21ae23-3f22-4f34-b04a-199e701886ff"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := classWithVectors(className)
	require.Nil(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))

	_, err := c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id).WithVector([]float32{1, 2, 3}).Do(ctx)
	require.Nil(t, err)
	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id2).WithVector([]float32{1.1, 2, 3}).Do(ctx)
	require.Nil(t, err)

	no := graphql.NearObjectArgumentBuilder{}
	res, err := c.GraphQL().Explore().WithNearObject(no.WithID(id).WithCertainty(0.8)).WithFields(graphql.Certainty, graphql.Beacon).Do(ctx)
	require.Nil(t, err)
	require.Nil(t, res.Errors)

	// return both objects
	require.Contains(t, res.Data["Explore"].([]interface{})[0].(map[string]interface{})["beacon"], id)
	require.Contains(t, res.Data["Explore"].([]interface{})[1].(map[string]interface{})["beacon"], id2)
}

func TestExploreMultipleClasses(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})

	className := "RedApples"
	className2 := "GreenApples"
	id1 := "0a21ae23-3f22-4f34-b04a-199e700886ef"
	id2 := "0a21ae23-3f22-4f34-b04a-199e701886ff"
	id3 := "0a21ae23-3f22-4f34-b04a-199e702886ef"
	id4 := "0a21ae23-3f22-4f34-b04a-199e703886ff"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	c.Schema().ClassDeleter().WithClassName(className2).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className2).Do(ctx)

	class := classWithVectors(className)
	require.Nil(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))
	class2 := classWithoutVectors(className2)
	require.Nil(t, c.Schema().ClassCreator().WithClass(&class2).Do(ctx))

	_, err := c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id1).WithVector([]float32{1, 2, 3}).Do(ctx)
	require.Nil(t, err)
	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id2).WithVector([]float32{1.1, 2, 3}).Do(ctx)
	require.Nil(t, err)

	_, err = c.Data().Creator().WithClassName(className2).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id3).Do(ctx)
	require.Nil(t, err)
	_, err = c.Data().Creator().WithClassName(className2).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id4).Do(ctx)
	require.Nil(t, err)

	no := graphql.NearObjectArgumentBuilder{}
	res, err := c.GraphQL().Explore().WithNearObject(no.WithID(id1).WithCertainty(0.8)).WithFields(graphql.Certainty, graphql.Beacon, graphql.ClassName).Do(ctx)
	require.Nil(t, err)
	require.Nil(t, res.Errors)

	// return both objects from class with vectors
	require.Contains(t, res.Data["Explore"].([]interface{})[0].(map[string]interface{})["beacon"], id1)
	require.Equal(t, res.Data["Explore"].([]interface{})[0].(map[string]interface{})["className"], className)
	require.Contains(t, res.Data["Explore"].([]interface{})[1].(map[string]interface{})["beacon"], id2)
	require.Equal(t, res.Data["Explore"].([]interface{})[1].(map[string]interface{})["className"], className)
}

func TestExploreMultipleClassesDiffVectorLength(t *testing.T) {
	ctx := context.Background()
	c := client.New(client.Config{Scheme: "http", Host: "localhost:8080"})

	className := "RedOranges"
	className2 := "GreenOranges"
	id1 := "0a21ae23-3f22-4f34-b04a-199e700886ef"
	id2 := "0a21ae23-3f22-4f34-b04a-199e701886ff"
	id3 := "0a21ae23-3f22-4f34-b04a-199e702886ef"
	id4 := "0a21ae23-3f22-4f34-b04a-199e703886ff"

	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	c.Schema().ClassDeleter().WithClassName(className2).Do(ctx)
	defer c.Schema().ClassDeleter().WithClassName(className2).Do(ctx)

	class := classWithVectors(className)
	require.Nil(t, c.Schema().ClassCreator().WithClass(&class).Do(ctx))
	class2 := classWithVectors(className2)
	require.Nil(t, c.Schema().ClassCreator().WithClass(&class2).Do(ctx))

	_, err := c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id1).WithVector([]float32{1, 2, 3}).Do(ctx)
	require.Nil(t, err)
	_, err = c.Data().Creator().WithClassName(className).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithID(id2).WithVector([]float32{1.1, 2, 3}).Do(ctx)
	require.Nil(t, err)

	_, err = c.Data().Creator().WithClassName(className2).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithVector([]float32{1, 2, 3, 4}).WithID(id3).Do(ctx)
	require.Nil(t, err)
	_, err = c.Data().Creator().WithClassName(className2).WithProperties(
		map[string]interface{}{"prop": "SomeText"}).WithVector([]float32{1, 2, 3, 4.1}).WithID(id4).Do(ctx)
	require.Nil(t, err)

	// search for vector length of first class
	no := graphql.NearObjectArgumentBuilder{}
	res, err := c.GraphQL().Explore().WithNearObject(no.WithID(id1).WithCertainty(0.8)).WithFields(graphql.Certainty, graphql.Beacon, graphql.ClassName).Do(ctx)
	require.Nil(t, err)
	require.Nil(t, res.Errors)

	// return both objects from class with vectors
	require.Contains(t, res.Data["Explore"].([]interface{})[0].(map[string]interface{})["beacon"], id1)
	require.Equal(t, res.Data["Explore"].([]interface{})[0].(map[string]interface{})["className"], className)
	require.Contains(t, res.Data["Explore"].([]interface{})[1].(map[string]interface{})["beacon"], id2)
	require.Equal(t, res.Data["Explore"].([]interface{})[1].(map[string]interface{})["className"], className)

	// search for vector length of second class
	no = graphql.NearObjectArgumentBuilder{}
	res, err = c.GraphQL().Explore().WithNearObject(no.WithID(id3).WithCertainty(0.8)).WithFields(graphql.Certainty, graphql.Beacon, graphql.ClassName).Do(ctx)
	require.Nil(t, err)
	require.Nil(t, res.Errors)

	// return both objects from class with vectors
	require.Contains(t, res.Data["Explore"].([]interface{})[0].(map[string]interface{})["beacon"], id3)
	require.Equal(t, res.Data["Explore"].([]interface{})[0].(map[string]interface{})["className"], className2)
	require.Contains(t, res.Data["Explore"].([]interface{})[1].(map[string]interface{})["beacon"], id4)
	require.Equal(t, res.Data["Explore"].([]interface{})[1].(map[string]interface{})["className"], className2)
}

func classWithVectors(className string) models.Class {
	return models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name:     "prop",
			DataType: []string{string(schema.DataTypeText)},
		}},
		Vectorizer: "none",
	}
}

func classWithoutVectors(className string) models.Class {
	return models.Class{
		Class: className,
		Properties: []*models.Property{{
			Name:     "prop",
			DataType: []string{string(schema.DataTypeText)},
		}},
		Vectorizer: "none",
		VectorIndexConfig: map[string]interface{}{
			"skip": true,
		},
	}
}
