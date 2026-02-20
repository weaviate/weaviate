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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
)

func TestGeoFilterAfterDeleteAndRecreate(t *testing.T) {
	ctx := context.Background()

	var compose *docker.DockerCompose
	var err error
	if os.Getenv("TEST_WEAVIATE_IMAGE") != "" {
		compose, err = docker.New().
			WithWeaviate().
			Start(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, compose.Terminate(ctx))
		}()
	}

	weaviateURI := func(c *docker.DockerCompose) string {
		if c == nil {
			return "localhost:8080"
		}
		return c.GetWeaviate().URI()
	}

	client, err := wvt.NewClient(wvt.Config{
		Scheme: "http",
		Host:   weaviateURI(compose),
	})
	require.NoError(t, err)
	require.NotNil(t, client)

	className := "GeoCity"

	type city struct {
		name string
		lat  float32
		lon  float32
	}
	cities := []city{
		// Original 5 cities
		{"Berlin", 52.52, 13.405},
		{"Paris", 48.8566, 2.3522},
		{"London", 51.5074, -0.1278},
		{"Madrid", 40.4168, -3.7038},
		{"Rome", 41.9028, 12.4964},
		// 5 more cities
		{"Amsterdam", 52.3676, 4.9041},
		{"Vienna", 48.2082, 16.3738},
		{"Warsaw", 52.2297, 21.0122},
		{"Stockholm", 59.3293, 18.0686},
		{"Lisbon", 38.7223, -9.1393},
		// 10 cities near Berlin (~5-50km away)
		{"Potsdam", 52.3906, 13.0645},
		{"Oranienburg", 52.7546, 13.2374},
		{"Bernau", 52.6788, 13.5871},
		{"Königs Wusterhausen", 52.3005, 13.6332},
		{"Falkensee", 52.5588, 13.0926},
		{"Teltow", 52.4023, 13.2710},
		{"Hennigsdorf", 52.6363, 13.2044},
		{"Erkner", 52.4241, 13.7510},
		{"Strausberg", 52.5786, 13.8822},
		{"Ludwigsfelde", 52.3013, 13.2547},
	}

	createClass := func() {
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
		err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
		require.NoError(t, err)
	}

	insertCities := func() {
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
	}

	queryGeoFilterWithDist := func(maxDistMeters float32) []string {
		where := filters.Where().
			WithPath([]string{"location"}).
			WithOperator(filters.WithinGeoRange).
			WithValueGeoRange(&filters.GeoCoordinatesParameter{
				Latitude:    52.52,
				Longitude:   13.405,
				MaxDistance: maxDistMeters,
			})

		resp, err := client.GraphQL().Get().
			WithClassName(className).
			WithWhere(where).
			WithFields(
				graphql.Field{Name: "name"},
				graphql.Field{Name: "_additional{id}"},
			).
			Do(ctx)
		require.NoError(t, err)
		return GetIds(t, resp, className)
	}

	queryGeoFilterNoDist := func() []string {
		where := filters.Where().
			WithPath([]string{"location"}).
			WithOperator(filters.WithinGeoRange).
			WithValueGeoRange(&filters.GeoCoordinatesParameter{
				Latitude:  52.52,
				Longitude: 13.405,
			})

		resp, err := client.GraphQL().Get().
			WithClassName(className).
			WithWhere(where).
			WithFields(
				graphql.Field{Name: "name"},
				graphql.Field{Name: "_additional{id}"},
			).
			Do(ctx)
		require.NoError(t, err)
		return GetIds(t, resp, className)
	}

	aggregateCount := func() float64 {
		resp, err := client.GraphQL().Aggregate().
			WithClassName(className).
			WithFields(graphql.Field{Name: "meta", Fields: []graphql.Field{{Name: "count"}}}).
			Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.Errors)
		return resp.Data["Aggregate"].(map[string]any)[className].([]any)[0].(map[string]any)["meta"].(map[string]any)["count"].(float64)
	}

	// First round: create, insert, query
	createClass()
	insertCities()

	require.Equal(t, float64(len(cities)), aggregateCount(), "should have %d objects after insert", len(cities))

	idsNoDist := queryGeoFilterNoDist()
	require.NotEmpty(t, idsNoDist, "first query without max distance should return results")

	ids := queryGeoFilterWithDist(200 * 1000) // 200km from Berlin — should include Berlin + nearby towns
	require.NotEmpty(t, ids, "first query with max distance should return results")

	// Delete all data via batch delete (keep the class)
	_, err = client.Batch().ObjectsBatchDeleter().
		WithClassName(className).
		WithWhere(filters.Where().
			WithOperator(filters.Like).
			WithPath([]string{"name"}).
			WithValueText("*")).
		Do(ctx)
	require.NoError(t, err)

	require.Equal(t, float64(0), aggregateCount(), "should have 0 objects after batch delete")

	// Second round: re-insert same data, query again
	insertCities()

	require.Equal(t, float64(len(cities)), aggregateCount(), "should have %d objects after re-insert", len(cities))

	idsNoDist = queryGeoFilterNoDist()
	require.NotEmpty(t, idsNoDist, "query without max distance after re-insert should return results")

	ids = queryGeoFilterWithDist(200 * 1000)
	require.NotEmpty(t, ids, "query with max distance after re-insert should return results (geo filter bug)")

	// Third round: restart Weaviate, query again
	require.NoError(t, compose.Stop(ctx, compose.GetWeaviate().Name(), nil))
	require.NoError(t, compose.Start(ctx, compose.GetWeaviate().Name()))

	// Recreate client since the port may have changed after restart
	client, err = wvt.NewClient(wvt.Config{
		Scheme: "http",
		Host:   weaviateURI(compose),
	})
	require.NoError(t, err)

	require.Equal(t, float64(len(cities)), aggregateCount(), "should have %d objects after restart", len(cities))

	idsNoDist = queryGeoFilterNoDist()
	require.NotEmpty(t, idsNoDist, "query without max distance after restart should return results")

	ids = queryGeoFilterWithDist(200 * 1000)
	require.NotEmpty(t, ids, "query with max distance after restart should return results")
}
