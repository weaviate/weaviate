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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
)

func TestGeoPropertyUpdate(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		With3NodeCluster().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	httpUri := compose.GetWeaviateNode(2).GetEndpoint(docker.HTTP)

	config := wvt.Config{
		Scheme: "http", Host: httpUri,
		StartupTimeout: 30 * time.Second,
	}
	client, err := wvt.NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, client)

	className := "GeoUpdateIssue"

	// clean DB
	client.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "geo",
				DataType: schema.DataTypeGeoCoordinates.PropString(),
			},
		},
	}
	err = client.Schema().ClassCreator().WithClass(class).Do(ctx)
	require.NoError(t, err)

	for i := range 10 {
		_, err = client.Data().Creator().
			WithID(fmt.Sprintf("00000000-0000-0000-0000-00000000000%v", i)).
			WithClassName(className).
			WithProperties(map[string]any{
				"geo": map[string]any{
					"latitude":  i,
					"longitude": i,
				},
			}).
			Do(ctx)
		require.NoError(t, err)
	}

	for i := range 10 {
		err = client.Data().Updater().
			WithID(fmt.Sprintf("00000000-0000-0000-0000-00000000000%v", i)).
			WithClassName(className).
			WithProperties(map[string]any{
				"geo": map[string]any{
					"latitude":  1 + i,
					"longitude": 2 + i,
				},
			}).
			WithMerge().
			Do(ctx)
		require.NoError(t, err)
	}
}
