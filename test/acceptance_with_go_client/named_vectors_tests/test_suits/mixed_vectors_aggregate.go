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

package test_suits

import (
	"context"
	"fmt"
	"testing"

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testMixedVectorsAggregate(host string) func(t *testing.T) {
	return func(t *testing.T) {
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)
		ctx := context.Background()

		classAggregate := "NamedAggregateTest"

		// delete class if exists and cleanup after test
		err = client.Schema().ClassDeleter().WithClassName(classAggregate).Do(ctx)
		require.Nil(t, err)

		defer client.Schema().ClassDeleter().WithClassName(classAggregate).Do(ctx)

		contextionaryConfig := map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": false,
				"properties":         []string{"text"},
			},
		}

		// create class and objects
		class := &models.Class{
			Class: classAggregate,
			Properties: []*models.Property{
				{
					Name: "text", DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name: "number", DataType: []string{schema.DataTypeInt.String()},
				},
			},
			Vectorizer:      text2vecContextionary,
			VectorIndexType: "hnsw",
			ModuleConfig:    contextionaryConfig,
			VectorConfig: map[string]models.VectorConfig{
				"first": {
					Vectorizer:      contextionaryConfig,
					VectorIndexType: "hnsw",
				},
				"second": {
					Vectorizer: map[string]interface{}{
						text2vecTransformers: map[string]interface{}{},
					},
					VectorIndexType: "hnsw",
				},
			},
		}
		require.NoError(t, client.Schema().ClassCreator().WithClass(class).Do(ctx))

		_, err = client.Schema().ClassGetter().WithClassName(classAggregate).Do(ctx)
		require.NoError(t, err)

		creator := client.Data().Creator()
		_, err = creator.WithClassName(classAggregate).WithID(id1).
			WithProperties(map[string]any{"text": "Hello", "number": 1}).
			Do(ctx)
		require.NoError(t, err)

		_, err = creator.WithClassName(classAggregate).WithID(id2).
			WithProperties(map[string]any{"text": "World", "number": 2}).
			Do(ctx)
		require.NoError(t, err)

		testAllObjectsIndexed(t, client, classAggregate)
		for _, targetVector := range []string{"", "first"} {
			t.Run(fmt.Sprintf("vector=%q", targetVector), func(t *testing.T) {
				no := &graphql.NearObjectArgumentBuilder{}
				no = no.WithID(id1).WithCertainty(0.9)
				if targetVector != "" {
					no = no.WithTargetVectors(targetVector)
				}

				agg, err := client.GraphQL().
					Aggregate().
					WithClassName(classAggregate).
					WithNearObject(no).
					WithFields(graphql.Field{Name: "number", Fields: []graphql.Field{{Name: "maximum"}}}).
					Do(ctx)
				require.NoError(t, err)

				maximums := acceptance_with_go_client.ExtractGraphQLField[float64](t, agg, "Aggregate", classAggregate, "number", "maximum")
				assert.Equal(t, []float64{1}, maximums)
			})
		}
	}
}
