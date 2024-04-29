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

func TestGRPC_NamedVectors(t *testing.T) {
	grpcClient, conn := newClient(t)

	// delete if exists and then re-create Books class
	booksClass := books.ClassNamedContextionaryVectorizer()
	helper.DeleteClass(t, booksClass.Class)
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

	t.Run("Search with hybrid", func(t *testing.T) {
		resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
			Collection: booksClass.Class,
			HybridSearch: &pb.Hybrid{
				Query:         "Dune",
				TargetVectors: []string{"all"},
			},
			Uses_123Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Results)
		require.Equal(t, resp.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue(), "Dune")
	})

	t.Run("Search with hybrid and group by", func(t *testing.T) {
		resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
			Collection: booksClass.Class,
			GroupBy: &pb.GroupBy{
				Path:            []string{"title"},
				NumberOfGroups:  1,
				ObjectsPerGroup: 1,
			},
			HybridSearch: &pb.Hybrid{
				Query:         "Dune",
				TargetVectors: []string{"all"},
			},
			Uses_123Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.GroupByResults)
		require.Len(t, resp.GroupByResults, 1)
	})

	t.Run("Search with hybrid near text and group by", func(t *testing.T) {
		resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
			Collection: booksClass.Class,
			GroupBy: &pb.GroupBy{
				Path:            []string{"title"},
				NumberOfGroups:  1,
				ObjectsPerGroup: 1,
			},
			HybridSearch: &pb.Hybrid{
				Alpha: 0.5,
				NearText: &pb.NearTextSearch{
					Query:         []string{"Dune"},
					TargetVectors: []string{"all"},
				},
			},
			Uses_123Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.GroupByResults)
		require.Len(t, resp.GroupByResults, 1)
	})

	t.Run("Search with near text", func(t *testing.T) {
		resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
			Collection: booksClass.Class,
			NearText: &pb.NearTextSearch{
				Query:         []string{"Dune"},
				TargetVectors: []string{"all"},
			},
			Uses_123Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Results)
		require.Equal(t, resp.Results[0].Properties.NonRefProps.Fields["title"].GetTextValue(), "Dune")
	})

	t.Run("Search with near text and group by", func(t *testing.T) {
		resp, err := grpcClient.Search(context.TODO(), &pb.SearchRequest{
			Collection: booksClass.Class,
			GroupBy: &pb.GroupBy{
				Path:            []string{"title"},
				NumberOfGroups:  1,
				ObjectsPerGroup: 1,
			},
			NearText: &pb.NearTextSearch{
				Query:         []string{"Dune"},
				TargetVectors: []string{"all"},
			},
			Uses_123Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.GroupByResults)
		require.Len(t, resp.GroupByResults, 1)
	})
}
