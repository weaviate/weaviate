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

package profiling

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/byteops"
)

const className = "ProfileTest"

func TestQueryProfiling(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	grpcConn, err := helper.CreateGrpcConnectionClient(compose.GetWeaviate().GrpcURI())
	require.NoError(t, err)
	defer grpcConn.Close()
	grpcClient := helper.CreateGrpcWeaviateClient(grpcConn)

	// Create collection with 3 shards (one per node in the 3-node cluster).
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{Name: "text", DataType: schema.DataTypeText.PropString()},
			{Name: "num", DataType: schema.DataTypeInt.PropString()},
		},
		Vectorizer: "none",
		ShardingConfig: map[string]interface{}{
			"desiredCount": float64(3),
		},
	}
	helper.CreateClass(t, class)
	defer helper.DeleteClass(t, className)

	objects := make([]*models.Object, 30)
	for i := range objects {
		objects[i] = &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"text": fmt.Sprintf("hello world document about topic %d", i%5),
				"num":  i,
			},
			Vector: []float32{float32(i) * 0.1, float32(i) * 0.2, float32(i)*0.05 + 0.1},
		}
	}
	helper.CreateObjectsBatch(t, objects)

	vecBytes := byteops.Fp32SliceToBytes([]float32{0.5, 0.3, 0.2})

	t.Run("vector search returns profile", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      5,
			Metadata:   &pb.MetadataRequest{Uuid: true, Distance: true, Profile: true},
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{{
					VectorBytes: vecBytes,
					Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results)
		require.NotNil(t, resp.Profile, "profile should be present when requested")
		require.NotEmpty(t, resp.Profile.Shards)

		for _, shard := range resp.Profile.Shards {
			assert.NotEmpty(t, shard.Name, "shard name should be set")
			assert.NotEmpty(t, shard.Node, "node name should be set")

			vecSearch, ok := shard.Searches["vector"]
			require.True(t, ok, "should have vector search profile")
			assert.NotEmpty(t, vecSearch.Details["total_took"])
			assert.NotEmpty(t, vecSearch.Details["vector_search_took"])
		}
	})

	t.Run("BM25 keyword search returns profile", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      5,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true, Profile: true},
			Bm25Search: &pb.BM25{
				Query:      "hello world document",
				Properties: []string{"text"},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results)
		require.NotNil(t, resp.Profile)
		require.NotEmpty(t, resp.Profile.Shards)

		for _, shard := range resp.Profile.Shards {
			assert.NotEmpty(t, shard.Name)
			assert.NotEmpty(t, shard.Node)

			kwdSearch, ok := shard.Searches["keyword"]
			require.True(t, ok, "should have keyword search profile")
			assert.NotEmpty(t, kwdSearch.Details["total_took"])
		}
	})

	t.Run("hybrid search returns both vector and keyword profiles", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      5,
			Metadata:   &pb.MetadataRequest{Uuid: true, Score: true, Profile: true},
			HybridSearch: &pb.Hybrid{
				Query:      "hello world document",
				Alpha:      0.5,
				Properties: []string{"text"},
				NearVector: &pb.NearVector{
					Vectors: []*pb.Vectors{{
						VectorBytes: vecBytes,
						Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
					}},
				},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results)
		require.NotNil(t, resp.Profile)
		require.NotEmpty(t, resp.Profile.Shards)

		for _, shard := range resp.Profile.Shards {
			assert.NotEmpty(t, shard.Name)
			assert.NotEmpty(t, shard.Node)

			_, hasVector := shard.Searches["vector"]
			_, hasKeyword := shard.Searches["keyword"]
			assert.True(t, hasVector, "hybrid should have vector search profile for shard %s", shard.Name)
			assert.True(t, hasKeyword, "hybrid should have keyword search profile for shard %s", shard.Name)
		}
	})

	t.Run("profile absent when not requested", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      5,
			Metadata:   &pb.MetadataRequest{Uuid: true, Distance: true},
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{{
					VectorBytes: vecBytes,
					Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results)
		assert.Nil(t, resp.Profile, "profile should be nil when not requested")
	})

	t.Run("multi-node profiles include different node names", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      5,
			Metadata:   &pb.MetadataRequest{Uuid: true, Distance: true, Profile: true},
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{{
					VectorBytes: vecBytes,
					Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp.Profile)
		require.NotEmpty(t, resp.Profile.Shards)

		nodes := make(map[string]bool)
		for _, shard := range resp.Profile.Shards {
			require.NotEmpty(t, shard.Node)
			nodes[shard.Node] = true
		}
		assert.GreaterOrEqual(t, len(nodes), 2,
			"with 3 shards on 3 nodes, expected at least 2 distinct node names, got: %v", nodes)
	})

	t.Run("vector search with filter returns filter details", func(t *testing.T) {
		resp, err := grpcClient.Search(ctx, &pb.SearchRequest{
			Collection: className,
			Limit:      5,
			Metadata:   &pb.MetadataRequest{Uuid: true, Distance: true, Profile: true},
			NearVector: &pb.NearVector{
				Vectors: []*pb.Vectors{{
					VectorBytes: vecBytes,
					Type:        pb.Vectors_VECTOR_TYPE_SINGLE_FP32,
				}},
			},
			Filters: &pb.Filters{
				Operator:  pb.Filters_OPERATOR_GREATER_THAN,
				TestValue: &pb.Filters_ValueInt{ValueInt: 10},
				Target:    &pb.FilterTarget{Target: &pb.FilterTarget_Property{Property: "num"}},
			},
			Uses_127Api: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Results)
		require.NotNil(t, resp.Profile)

		for _, shard := range resp.Profile.Shards {
			vecSearch := shard.Searches["vector"]
			assert.NotEmpty(t, vecSearch.Details["total_took"])
			assert.NotEmpty(t, vecSearch.Details["filters_build_allow_list_took"],
				"filtered search should include filter timing for shard %s", shard.Name)
		}
	})
}
