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
	"testing"

	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/fault"
	weaviateGrpc "github.com/weaviate/weaviate-go-client/v5/weaviate/grpc"
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
		WithWeaviateEnv("READ_ONLY_MODE", "true").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	client, err := client.NewClient(client.Config{
		Scheme: "http",
		Host:   compose.GetWeaviate().GetEndpoint(docker.HTTP),
		GrpcConfig: &weaviateGrpc.Config{
			Host: compose.GetWeaviate().GetEndpoint(docker.GRPC),
		},
	})
	require.NoError(t, err)

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

	t.Run("rest: replicate", func(t *testing.T) {
		_, err := helper.Client(t).Replication.Replicate(
			replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{}),
			nil,
		)
		var e *replication.ReplicateUnprocessableEntity
		if !errors.As(err, &e) {
			// test that we get expected response here, which is a 422 due to bad params input but not 503
			t.Fatalf("expected error to be of type ReplicateUnprocessableEntity, but was %T", err)
		}
	})

	t.Run("rest: delete replication op", func(t *testing.T) {
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
