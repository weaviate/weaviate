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

package grpc_tests

import (
	"context"
	"testing"
	"time"

	"acceptance_tests_with_client/fixtures"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
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
	t.Run("all properties", testGRPCBatchAPI(ctx, client))
}

func TestGRPC_Batch_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		WithText2VecContextionary().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	httpUri := compose.GetWeaviateNode(2).GetEndpoint(docker.HTTP)
	grpcUri := compose.GetWeaviateNode(2).GetEndpoint(docker.GRPC)

	config := wvt.Config{
		Scheme: "http", Host: httpUri,
		GrpcConfig:     &grpc.Config{Host: grpcUri},
		StartupTimeout: 30 * time.Second,
	}
	client, err := wvt.NewClient(config)
	require.NoError(t, err)
	require.NotNil(t, client)

	// clean DB
	err = client.Schema().AllDeleter().Do(ctx)
	require.NoError(t, err)

	t.Run("all properties", testGRPCBatchAPI(ctx, client))
	t.Run("multi vectors", testGRPCBatchImportColBERT(ctx, client))
}

func testGRPCBatchAPI(ctx context.Context, client *wvt.Client) func(t *testing.T) {
	return func(t *testing.T) {
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
					Class: className,
					ID:    strfmt.UUID(id1),
					// These properties slightly differ from the one defined in the class. We are laying them all out as
					// "base" properties and not as nested properties.
					// This will cause auto schema to kick in and will make the follower have eventual consistency on the
					// schema changes. With proper internal retry mechanism this will be transparent to the client.
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
			// We check that the properties are layed out and not nested. This indicates auto schema did it's job.
			objectProperties, ok := objs[0].Properties.(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, len(properties), len(objectProperties))
		})
	}
}

func testGRPCBatchImportColBERT(ctx context.Context, client *wvt.Client) func(t *testing.T) {
	return func(t *testing.T) {
		className := fixtures.BringYourOwnColBERTClassName
		class := fixtures.BringYourOwnColBERTClass(className)
		objects := fixtures.BringYourOwnColBERTObjects
		byoc := fixtures.BringYourOwnColBERTNamedVectorName
		t.Run("create schema", func(t *testing.T) {
			err := client.Schema().ClassCreator().WithClass(class).Do(ctx)
			require.NoError(t, err)
		})
		t.Run("batch import", func(t *testing.T) {
			objs := []*models.Object{}
			for _, o := range objects {
				obj := &models.Object{
					Class: className,
					ID:    strfmt.UUID(o.ID),
					Properties: map[string]interface{}{
						"name": o.Name,
					},
					Vectors: models.Vectors{
						byoc: o.Vector,
					},
				}
				objs = append(objs, obj)
			}
			resp, err := client.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Len(t, resp, len(objs))
			for i := range objs {
				require.NotNil(t, resp[i].Result)
				assert.Nil(t, resp[i].Result.Errors)
			}
		})
		t.Run("check", func(t *testing.T) {
			for _, o := range objects {
				objs, err := client.Data().ObjectsGetter().WithClassName(className).WithID(o.ID).WithVector().Do(ctx)
				require.NoError(t, err)
				require.NotNil(t, objs)
				require.Len(t, objs, 1)
				assert.Equal(t, className, objs[0].Class)
				require.Len(t, objs[0].Vectors, 1)
				require.IsType(t, [][]float32{}, objs[0].Vectors[byoc])
				assert.Equal(t, o.Vector, objs[0].Vectors[byoc])
			}
		})
	}
}
