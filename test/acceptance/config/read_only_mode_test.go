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

package config

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"google.golang.org/grpc"
)

func TestReadOnlyMode(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		WithWeaviateEnv("READ_ONLY_MODE", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	// grpcClient, _ := grpcClient(t, compose.GetWeaviate().GrpcURI())

	t.Run("rest: reject schema creation", func(t *testing.T) {
		paragraphs := articles.ParagraphsClass()
		_, err := helper.Client(t).Schema.SchemaObjectsCreate(schema.NewSchemaObjectsCreateParams().WithObjectClass(paragraphs), nil)
		require.IsType(t, err, schema.NewSchemaObjectsCreateServiceUnavailable())
	})
}

func grpcClient(t *testing.T, host string) (protocol.WeaviateClient, *grpc.ClientConn) {
	conn, err := helper.CreateGrpcConnectionClient(host)
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := helper.CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)
	return grpcClient, conn
}
