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

package acceptance_with_go_client

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/fault"
	weaviateGrpc "github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"google.golang.org/grpc/status"
)

func TestReadOnlyMode(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("OPERATIONAL_MODE", "ReadOnly").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().GetEndpoint(docker.HTTP))
	client, err := client.NewClient(client.Config{
		Scheme: "http",
		Host:   compose.GetWeaviate().GetEndpoint(docker.HTTP),
		GrpcConfig: &weaviateGrpc.Config{
			Host: compose.GetWeaviate().GetEndpoint(docker.GRPC),
		},
	})
	require.NoError(t, err)

	t.Run("rest: is in read-only mode", func(t *testing.T) {
		health, err := helper.Client(t).Nodes.NodesGet(
			nodes.NewNodesGetParams(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, health.Payload)
		require.Equal(t, "ReadOnly", health.Payload.Nodes[0].OperationalMode)
	})

	t.Run("rest: reject schema creation", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassCreator().WithClass(paragraphs).Do(ctx)
		assertRestErr(t, err)
	})

	t.Run("rest: accept schema retrieval", func(t *testing.T) {
		schema, err := client.Schema().Getter().Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, schema)
	})

	t.Run("rest: reject schema update", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassUpdater().WithClass(paragraphs).Do(ctx)
		assertRestErr(t, err)
	})

	t.Run("rest: reject schema deletion", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassDeleter().WithClassName(paragraphs.Class).Do(ctx)
		assertRestErr(t, err)
	})

	t.Run("grpc: reject BatchObjects", func(t *testing.T) {
		_, err := client.Batch().ObjectsBatcher().WithObjects(&models.Object{
			Properties: map[string]interface{}{},
		}).Do(ctx)
		assertGrpcErr(t, err)
	})

	t.Run("grpc: accept Search", func(t *testing.T) {
		_, err := client.Experimental().Search().WithCollection("Whatever").Do(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not find class Whatever in schema")
	})

	t.Run("rest: reject replicate", func(t *testing.T) {
		res, err := http.Post(
			"http://"+compose.GetWeaviate().GetEndpoint(docker.HTTP)+"/v1/replication/replicate",
			"application/json",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusServiceUnavailable, res.StatusCode)
	})
}

func TestScaleOutMode(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("OPERATIONAL_MODE", "ScaleOut").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().GetEndpoint(docker.HTTP))
	client, err := client.NewClient(client.Config{
		Scheme: "http",
		Host:   compose.GetWeaviate().GetEndpoint(docker.HTTP),
		GrpcConfig: &weaviateGrpc.Config{
			Host: compose.GetWeaviate().GetEndpoint(docker.GRPC),
		},
	})
	require.NoError(t, err)

	t.Run("rest: is in scale-out mode", func(t *testing.T) {
		health, err := helper.Client(t).Nodes.NodesGet(
			nodes.NewNodesGetParams(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, health.Payload)
		require.Equal(t, "ScaleOut", health.Payload.Nodes[0].OperationalMode)
	})

	t.Run("rest: reject schema creation", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassCreator().WithClass(paragraphs).Do(ctx)
		assertRestErr(t, err)
	})

	t.Run("rest: accept schema retrieval", func(t *testing.T) {
		schema, err := client.Schema().Getter().Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, schema)
	})

	t.Run("rest: reject schema update", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassUpdater().WithClass(paragraphs).Do(ctx)
		assertRestErr(t, err)
	})

	t.Run("rest: reject schema deletion", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassDeleter().WithClassName(paragraphs.Class).Do(ctx)
		assertRestErr(t, err)
	})

	t.Run("grpc: reject BatchObjects", func(t *testing.T) {
		_, err := client.Batch().ObjectsBatcher().WithObjects(&models.Object{
			Properties: map[string]interface{}{},
		}).Do(ctx)
		assertGrpcErr(t, err)
	})

	t.Run("grpc: accept Search", func(t *testing.T) {
		_, err := client.Experimental().Search().WithCollection("Whatever").Do(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not find class Whatever in schema")
	})

	t.Run("rest: accept delete replication op", func(t *testing.T) {
		_, err := helper.Client(t).Replication.DeleteReplication(
			replication.NewDeleteReplicationParams().WithID("00000000-0000-0000-0000-000000000000"),
			nil,
		)
		require.NoError(t, err)
	})
}

func TestWriteOnlyMode(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("OPERATIONAL_MODE", "WriteOnly").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().GetEndpoint(docker.HTTP))
	client, err := client.NewClient(client.Config{
		Scheme: "http",
		Host:   compose.GetWeaviate().GetEndpoint(docker.HTTP),
		GrpcConfig: &weaviateGrpc.Config{
			Host: compose.GetWeaviate().GetEndpoint(docker.GRPC),
		},
	})
	require.NoError(t, err)

	t.Run("rest: is in write-only mode", func(t *testing.T) {
		health, err := helper.Client(t).Nodes.NodesGet(
			nodes.NewNodesGetParams(),
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, health.Payload)
		require.Equal(t, "WriteOnly", health.Payload.Nodes[0].OperationalMode)
	})

	t.Run("rest: accept schema creation", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassCreator().WithClass(paragraphs).Do(ctx)
		require.NoError(t, err)
	})

	t.Run("rest: reject schema retrieval", func(t *testing.T) {
		_, err := client.Schema().Getter().Do(ctx)
		require.Error(t, err)
		assertRestErr(t, err)
	})

	t.Run("rest: accept schema update", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassUpdater().WithClass(paragraphs).Do(ctx)
		require.NoError(t, err)
	})

	t.Run("grpc: accept BatchObjects", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		_, err := client.Batch().ObjectsBatcher().WithObjects(&models.Object{
			Class:      paragraphs.Class,
			Properties: map[string]interface{}{},
		}).Do(ctx)
		require.NoError(t, err)
	})

	t.Run("rest: accept schema deletion", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		err := client.Schema().ClassDeleter().WithClassName(paragraphs.Class).Do(ctx)
		require.NoError(t, err)
	})

	t.Run("grpc: reject Search", func(t *testing.T) {
		_, err := client.Experimental().Search().WithCollection("Whatever").Do(ctx)
		require.Error(t, err)
		assertGrpcErr(t, err)
	})

	t.Run("rest: accept delete replication op", func(t *testing.T) {
		_, err := helper.Client(t).Replication.DeleteReplication(
			replication.NewDeleteReplicationParams().WithID("00000000-0000-0000-0000-000000000000"),
			nil,
		)
		require.NoError(t, err)
	})
}

func assertRestErr(t *testing.T, err error) {
	var e *fault.WeaviateClientError
	if errors.As(err, &e) {
		require.Equal(t, 503, e.StatusCode)
	} else {
		t.Fatalf("expected error to be of type WeaviateClientError, but was %T", err)
	}
}

func assertGrpcErr(t *testing.T, err error) {
	st := status.Convert(err)
	// unavailable status code in grpc
	require.Equal(t, 14, int(st.Code()))
}
