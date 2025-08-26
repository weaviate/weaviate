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

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/books"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestGRPCDeprecated(t *testing.T) {
	conn, err := helper.CreateGrpcConnectionClient(":50051")
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := helper.CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)

	// create Books class
	booksClass := books.ClassContextionaryVectorizer()
	helper.CreateClass(t, booksClass)
	defer helper.DeleteClass(t, booksClass.Class)

	t.Run("Health Check", func(t *testing.T) {
		client := grpc_health_v1.NewHealthClient(conn)
		check, err := client.Check(context.TODO(), &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)
		require.NotNil(t, check)
		assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING.Enum().Number(), check.Status.Number())
	})

	t.Run("Batch import", func(t *testing.T) {
		resp, err := grpcClient.BatchObjects(context.TODO(), &pb.BatchObjectsRequest{
			Objects: books.BatchObjects(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("gRPC Search removed", func(t *testing.T) {
		_, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{})
		require.NotNil(t, err)
	})
}
