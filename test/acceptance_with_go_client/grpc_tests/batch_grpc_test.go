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

package grpc_tests

import (
	"context"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/grpc"
	"github.com/weaviate/weaviate/entities/models"
)

func TestGRPC_Batch(t *testing.T) {
	ctx := context.Background()
	config := wvt.Config{Scheme: "http", Host: "localhost:8080", GrpcConfig: &grpc.Config{Host: "localhost:50051"}}
	client, err := wvt.NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, client)
	// clean DB
	err = client.Schema().AllDeleter().Do(ctx)
	require.NoError(t, err)
	t.Run("all properties", func(t *testing.T) {
		class := fixtures.AllPropertiesClass
		className := fixtures.AllPropertiesClassName
		id1 := fixtures.AllPropertiesID1
		properties := fixtures.AllPropertiesProperties
		t.Run("create schema", func(t *testing.T) {
			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)
		})
		t.Run("grpc batch import", func(t *testing.T) {
			objects := []*models.Object{
				{
					Class:      className,
					ID:         strfmt.UUID(id1),
					Properties: properties,
				},
			}
			resp, err := client.Batch().ObjectsBatcher().WithObjects(objects...).Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp, 1)
			require.NotNil(t, resp[0].Result)
			assert.Nil(t, resp[0].Result.Errors)
		})
		t.Run("check", func(t *testing.T) {
			objs, err := client.Data().ObjectsGetter().WithClassName(className).WithID(id1).Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, objs)
			require.Len(t, objs, 1)
			assert.Equal(t, className, objs[0].Class)
			props, ok := objs[0].Properties.(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, len(properties), len(props))
		})
	})
}
