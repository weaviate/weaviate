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

package acceptance_with_go_v6_client

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate-go-client/v6/modules/selfprovided"
	"github.com/weaviate/weaviate-go-client/v6/query/filter"
	"github.com/weaviate/weaviate/test/docker"
)

// TestGeoCollectionAfterDeleteAndRecreate is the reachable part of the v5
// suite's TestGeoFilterAfterDeleteAndRecreate: a collection holding a
// geoCoordinates property is emptied by a batch delete, refilled, and its node
// restarted.
//
// The v5 test asserted after each round that a WithinGeoRange filter still
// returned the cities around Berlin, which is what the original bug broke. The
// v6 client cannot express that query: filter.Operator has no WithinGeoRange,
// and the filter marshaller panics on a geo value. Nor can this test read the
// coordinates back and compare them itself -- the response decoder maps geo
// values (and every array property) to nil. Until the client covers geo, the
// assertions below are limited to object counts.
//
// This test links the weaviate module (test/docker) and the v6 client into one
// binary, and both register the Weaviate gRPC descriptors under the same proto
// file paths. Without GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn the binary
// panics during package init, before any test runs. test/run.sh sets it.
func TestGeoCollectionAfterDeleteAndRecreate(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, compose.Terminate(ctx)) })

	c := NewClientForContainer(t, ctx, compose.GetWeaviate())

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

	createCollection := func(c *client.Client) *collections.Handle {
		h, err := c.Collections.Create(ctx, collections.Collection{
			Name: className,
			Properties: []collections.Property{
				{Name: "name", DataType: collections.DataTypeText},
				{Name: "location", DataType: collections.DataTypeGeoCoordinates},
			},
			Vectors: map[string]collections.VectorConfig{
				"default": {Vectorizer: selfprovided.Vectorizer},
			},
		})
		require.NoError(t, err)
		return h
	}

	insertCities := func(h *collections.Handle) {
		objects := make([]*data.Object, len(cities))
		for i, city := range cities {
			id := uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-0000000000%02d", i))
			objects[i] = &data.Object{
				UUID: &id,
				Properties: map[string]any{
					"name": city.name,
					"location": map[string]any{
						"latitude":  city.lat,
						"longitude": city.lon,
					},
				},
			}
		}
		Insert(t, ctx, h, objects...)
	}

	count := func(h *collections.Handle) int64 {
		n, err := h.Count(ctx)
		require.NoError(t, err)
		return n
	}

	// First round: create, insert
	h := createCollection(c)
	insertCities(h)
	require.EqualValues(t, len(cities), count(h), "objects after insert")

	// Delete all data via batch delete (keep the collection)
	_, err = h.Data.DeleteSelected(ctx, data.DeleteSelected{
		Filter: &filter.Cond{Operator: filter.Like, Target: "name", Value: "*"},
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, count(h), "objects after batch delete")

	// Second round: re-insert the same data
	insertCities(h)
	require.EqualValues(t, len(cities), count(h), "objects after re-insert")

	// Third round: restart Weaviate
	require.NoError(t, compose.Stop(ctx, compose.GetWeaviate().Name(), nil))
	require.NoError(t, compose.Start(ctx, compose.GetWeaviate().Name()))

	// Reconnect, the published ports change when the container restarts.
	c = NewClientForContainer(t, ctx, compose.GetWeaviate())
	h = c.Collections.Use(className)
	require.EqualValues(t, len(cities), count(h), "objects after restart")

	config, err := c.Collections.GetConfig(ctx, className)
	require.NoError(t, err)
	dataTypes := make(map[string]collections.DataType, len(config.Properties))
	for _, p := range config.Properties {
		dataTypes[p.Name] = p.DataType
	}
	require.Equal(t, collections.DataTypeGeoCoordinates, dataTypes["location"],
		"geo property survives the restart")
}
