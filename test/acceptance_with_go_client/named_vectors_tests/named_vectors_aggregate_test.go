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

package named_vectors_tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

const (
	UUID1 = "f47ac10b-58cc-0372-8567-0e02b2c3d479"
	UUID2 = "f47ac10b-58cc-0372-8567-0e02b2c3d480"
)

func TestAggregate(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)
	ctx := context.Background()

	classAggregate := "NamedAggregateTest"

	// delete class if exists and cleanup after test
	err = client.Schema().ClassDeleter().WithClassName(classAggregate).Do(ctx)
	require.Nil(t, err)

	defer client.Schema().ClassDeleter().WithClassName(classAggregate).Do(ctx)

	// create class and objects
	class := &models.Class{
		Class: classAggregate,
		Properties: []*models.Property{
			{
				Name: "first", DataType: []string{schema.DataTypeText.String()},
			},
			{
				Name: "second", DataType: []string{schema.DataTypeText.String()},
			},
			{
				Name: "number", DataType: []string{schema.DataTypeInt.String()},
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			"first": {
				Vectorizer: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizeClassName": false,
						"properties":         []string{"first"},
					},
				},
				VectorIndexType: "hnsw",
			},
			"second": {
				Vectorizer: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizeClassName": false,
						"properties":         []string{"second"},
					},
				},
				VectorIndexType: "hnsw",
			},
		},
	}
	require.Nil(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

	creator := client.Data().Creator()
	_, err = creator.WithClassName(classAggregate).WithProperties(
		map[string]interface{}{"first": "Hello", "second": "World", "number": 1}).WithID(UUID1).Do(ctx)
	require.Nil(t, err)
	_, err = creator.WithClassName(classAggregate).WithProperties(
		map[string]interface{}{"first": "World", "second": "Hello", "number": 2}).WithID(UUID2).Do(ctx)
	require.Nil(t, err)

	// aggregate
	no := &graphql.NearObjectArgumentBuilder{}
	no.WithTargetVectors("first").WithID(UUID1).WithCertainty(0.9)
	agg, err := client.GraphQL().Aggregate().WithClassName(classAggregate).WithNearObject(no).WithFields(graphql.Field{Name: "number", Fields: []graphql.Field{{Name: "maximum"}}}).Do(ctx)
	require.Nil(t, err)

	require.NotNil(t, agg)
	require.Nil(t, agg.Errors)
	require.NotNil(t, agg.Data)

	require.Equal(t, agg.Data["Aggregate"].(map[string]interface{})[classAggregate].([]interface{})[0].(map[string]interface{})["number"].(map[string]interface{})["maximum"], float64(1))

	// aggregate without needed a target vector
	agg, err = client.GraphQL().Aggregate().WithClassName(classAggregate).WithFields(graphql.Field{Name: "meta", Fields: []graphql.Field{{Name: "count"}}}).Do(ctx)
	require.Nil(t, err)
	require.NotNil(t, agg)
	require.Nil(t, agg.Errors)
	require.NotNil(t, agg.Data)
	require.Equal(t, agg.Data["Aggregate"].(map[string]interface{})[classAggregate].([]interface{})[0].(map[string]interface{})["meta"].(map[string]interface{})["count"], float64(2))
}
