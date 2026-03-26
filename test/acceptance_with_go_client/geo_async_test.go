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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
)

func TestGeoFilterAsyncIndexing(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_INDEXING_STALE_TIMEOUT", "1s").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	client, err := wvt.NewClient(wvt.Config{
		Scheme: "http",
		Host:   compose.GetWeaviate().URI(),
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	className := "GeoCityAsync"

	type city struct {
		name string
		lat  float32
		lon  float32
	}
	cities := []city{
		{"Berlin", 52.52, 13.405},
		{"Paris", 48.8566, 2.3522},
		{"London", 51.5074, -0.1278},
		{"Madrid", 40.4168, -3.7038},
		{"Rome", 41.9028, 12.4964},
		{"Amsterdam", 52.3676, 4.9041},
		{"Vienna", 48.2082, 16.3738},
		{"Warsaw", 52.2297, 21.0122},
		{"Potsdam", 52.3906, 13.0645},
		{"Falkensee", 52.5588, 13.0926},
	}

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
			{
				Name:     "location",
				DataType: schema.DataTypeGeoCoordinates.PropString(),
			},
		},
		Vectorizer: "none",
	}
	err = client.Schema().ClassCreator().WithClass(class).Do(ctx)
	require.NoError(t, err)

	for i, c := range cities {
		_, err := client.Data().Creator().
			WithID(fmt.Sprintf("00000000-0000-0000-0000-0000000000%02d", i)).
			WithClassName(className).
			WithProperties(map[string]any{
				"name": c.name,
				"location": map[string]any{
					"latitude":  c.lat,
					"longitude": c.lon,
				},
			}).
			Do(ctx)
		require.NoError(t, err)
	}

	// Wait for async indexing to complete by polling nodes API for queue to drain
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := client.Cluster().NodesStatusGetter().
			WithClass(className).
			WithOutput("verbose").
			Do(ctx)
		require.NoError(ct, err)
		require.NotEmpty(ct, resp.Nodes)
		for _, node := range resp.Nodes {
			for _, shard := range node.Shards {
				assert.Equal(ct, int64(0), shard.VectorQueueLength,
					"geo queue should be fully drained")
				assert.Equal(ct, "READY", shard.VectorIndexingStatus)
			}
		}
	}, 30*time.Second, 500*time.Millisecond, "async geo indexing did not complete in time")

	// Verify all objects inserted
	aggResp, err := client.GraphQL().Aggregate().
		WithClassName(className).
		WithFields(graphql.Field{Name: "meta", Fields: []graphql.Field{{Name: "count"}}}).
		Do(ctx)
	require.NoError(t, err)
	require.Empty(t, aggResp.Errors)
	count := aggResp.Data["Aggregate"].(map[string]any)[className].([]any)[0].(map[string]any)["meta"].(map[string]any)["count"].(float64)
	require.Equal(t, float64(len(cities)), count)

	// Geo filter: 200km from Berlin
	where := filters.Where().
		WithPath([]string{"location"}).
		WithOperator(filters.WithinGeoRange).
		WithValueGeoRange(&filters.GeoCoordinatesParameter{
			Latitude:    52.52,
			Longitude:   13.405,
			MaxDistance: 200 * 1000,
		})

	geoResp, err := client.GraphQL().Get().
		WithClassName(className).
		WithWhere(where).
		WithFields(
			graphql.Field{Name: "name"},
			graphql.Field{Name: "_additional{id}"},
		).
		Do(ctx)
	require.NoError(t, err)
	ids := GetIds(t, geoResp, className)
	require.NotEmpty(t, ids, "geo filter with async indexing should return results")

	// Without max distance
	whereNoDist := filters.Where().
		WithPath([]string{"location"}).
		WithOperator(filters.WithinGeoRange).
		WithValueGeoRange(&filters.GeoCoordinatesParameter{
			Latitude:  52.52,
			Longitude: 13.405,
		})

	geoResp, err = client.GraphQL().Get().
		WithClassName(className).
		WithWhere(whereNoDist).
		WithFields(
			graphql.Field{Name: "name"},
			graphql.Field{Name: "_additional{id}"},
		).
		Do(ctx)
	require.NoError(t, err)
	idsNoDist := GetIds(t, geoResp, className)
	require.NotEmpty(t, idsNoDist, "geo filter without max distance should return results")
}
