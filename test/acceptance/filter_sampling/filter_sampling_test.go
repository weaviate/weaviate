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

package filter_sampling

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"google.golang.org/grpc"
)

const (
	testClassName = "FilterSamplingTest"
)

func client(t *testing.T, host string) (pb.WeaviateClient, *grpc.ClientConn) {
	conn, err := helper.CreateGrpcConnectionClient(host)
	require.NoError(t, err)
	require.NotNil(t, conn)
	grpcClient := helper.CreateGrpcWeaviateClient(conn)
	require.NotNil(t, grpcClient)
	return grpcClient, conn
}

func createTestClass(t *testing.T) {
	class := &models.Class{
		Class: testClassName,
		Properties: []*models.Property{
			{
				Name:            "category",
				DataType:        []string{"text"},
				Tokenization:    "field",
				IndexFilterable: func() *bool { b := true; return &b }(),
			},
			{
				Name:            "count",
				DataType:        []string{"int"},
				IndexFilterable: func() *bool { b := true; return &b }(),
			},
			{
				Name:            "price",
				DataType:        []string{"number"},
				IndexFilterable: func() *bool { b := true; return &b }(),
			},
			{
				Name:            "active",
				DataType:        []string{"boolean"},
				IndexFilterable: func() *bool { b := true; return &b }(),
			},
			{
				Name:            "nonIndexed",
				DataType:        []string{"text"},
				IndexFilterable: func() *bool { b := false; return &b }(),
			},
		},
		Vectorizer: "none",
	}

	helper.DeleteClass(t, class.Class)
	helper.CreateClass(t, class)
}

func createShardedTestClass(t *testing.T) {
	class := &models.Class{
		Class: testClassName,
		Properties: []*models.Property{
			{
				Name:            "category",
				DataType:        []string{"text"},
				Tokenization:    "field",
				IndexFilterable: func() *bool { b := true; return &b }(),
			},
			{
				Name:            "count",
				DataType:        []string{"int"},
				IndexFilterable: func() *bool { b := true; return &b }(),
			},
		},
		Vectorizer: "none",
		ShardingConfig: map[string]interface{}{
			"desiredCount": 3,
		},
		ReplicationConfig: &models.ReplicationConfig{
			Factor:       1,
			AsyncEnabled: false,
		},
	}

	helper.DeleteClass(t, class.Class)
	helper.CreateClass(t, class)
}

func insertTestData(t *testing.T) {
	// Insert objects with various categories
	testData := []struct {
		category string
		count    int64
		price    float64
		active   bool
	}{
		{"electronics", 10, 99.99, true},
		{"electronics", 20, 199.99, true},
		{"electronics", 30, 299.99, false},
		{"clothing", 5, 49.99, true},
		{"clothing", 15, 79.99, true},
		{"furniture", 2, 499.99, false},
		{"furniture", 8, 899.99, true},
		{"books", 100, 19.99, true},
		{"books", 200, 29.99, true},
		{"books", 50, 14.99, false},
	}

	for _, data := range testData {
		obj := &models.Object{
			Class: testClassName,
			Properties: map[string]interface{}{
				"category": data.category,
				"count":    data.count,
				"price":    data.price,
				"active":   data.active,
			},
		}
		require.NoError(t, helper.CreateObject(t, obj))
	}
}

func insertShardedTestData(t *testing.T) {
	// Insert objects with various categories - more data for sharding test
	testData := []struct {
		category string
		count    int64
	}{
		{"electronics", 10},
		{"electronics", 20},
		{"electronics", 30},
		{"clothing", 5},
		{"clothing", 15},
		{"furniture", 2},
		{"furniture", 8},
		{"books", 100},
		{"books", 200},
		{"books", 50},
		// More data to ensure distribution across shards
		{"electronics", 40},
		{"clothing", 25},
		{"furniture", 12},
		{"books", 75},
		{"toys", 1},
		{"toys", 2},
		{"toys", 3},
		{"sports", 10},
		{"sports", 20},
		{"sports", 30},
	}

	for _, data := range testData {
		obj := &models.Object{
			Class: testClassName,
			Properties: map[string]interface{}{
				"category": data.category,
				"count":    data.count,
			},
		}
		require.NoError(t, helper.CreateObject(t, obj))
	}
}

func TestFilterSampling_SingleNode(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	grpcClient, conn := client(t, compose.GetWeaviate().GrpcURI())
	defer conn.Close()

	createTestClass(t)
	insertTestData(t)
	defer helper.DeleteClass(t, testClassName)

	t.Run("sample text property", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "category",
			SampleCount: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// We have 4 unique categories: electronics (3), clothing (2), furniture (2), books (3)
		assert.Len(t, resp.Samples, 4)
		assert.Equal(t, uint64(10), resp.TotalObjects)

		// Samples should be sorted by cardinality descending
		// electronics and books both have 3, then clothing and furniture have 2
		categoryCount := make(map[string]uint64)
		for _, sample := range resp.Samples {
			categoryCount[sample.GetTextValue()] = sample.Cardinality
		}

		assert.Equal(t, uint64(3), categoryCount["electronics"])
		assert.Equal(t, uint64(3), categoryCount["books"])
		assert.Equal(t, uint64(2), categoryCount["clothing"])
		assert.Equal(t, uint64(2), categoryCount["furniture"])
	})

	t.Run("sample int property", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "count",
			SampleCount: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// We have 10 unique count values, each with cardinality 1
		assert.Len(t, resp.Samples, 10)
		assert.Equal(t, uint64(10), resp.TotalObjects)

		// Each sample should have cardinality 1
		for _, sample := range resp.Samples {
			assert.Equal(t, uint64(1), sample.Cardinality)
			assert.NotZero(t, sample.GetIntValue())
		}
	})

	t.Run("sample boolean property", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "active",
			SampleCount: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Boolean has 2 unique values: true (7 times), false (3 times)
		assert.Len(t, resp.Samples, 2)
		assert.Equal(t, uint64(10), resp.TotalObjects)

		// Check cardinalities
		trueCount := uint64(0)
		falseCount := uint64(0)
		for _, sample := range resp.Samples {
			if sample.GetBoolValue() {
				trueCount = sample.Cardinality
			} else {
				falseCount = sample.Cardinality
			}
		}
		assert.Equal(t, uint64(7), trueCount)
		assert.Equal(t, uint64(3), falseCount)
	})

	t.Run("sample with limit", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "category",
			SampleCount: 2,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should only return 2 samples (limited)
		assert.Len(t, resp.Samples, 2)
		assert.Equal(t, uint64(10), resp.TotalObjects)

		// Samples should be sorted by cardinality descending
		// (QuantileKeys samples evenly from keyspace, so we can't predict which values)
		if len(resp.Samples) >= 2 {
			assert.GreaterOrEqual(t, resp.Samples[0].Cardinality, resp.Samples[1].Cardinality)
		}
	})

	t.Run("error on non-indexed property", func(t *testing.T) {
		_, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "nonIndexed",
			SampleCount: 10,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "filterable index")
	})

	t.Run("error on non-existent property", func(t *testing.T) {
		_, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "nonExistent",
			SampleCount: 10,
		})
		require.Error(t, err)
	})

	t.Run("error on non-existent collection", func(t *testing.T) {
		_, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  "NonExistentClass",
			Property:    "category",
			SampleCount: 10,
		})
		require.Error(t, err)
	})

	t.Run("error on sample_count exceeding limit", func(t *testing.T) {
		// Default limit is 10000, so requesting 20000 should fail
		_, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "category",
			SampleCount: 20000,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sample_count must be <= 10000")
	})

	t.Run("took_seconds is populated", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "category",
			SampleCount: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// TookSeconds should be greater than 0
		assert.Greater(t, resp.TookSeconds, float32(0))
	})

	t.Run("percent covered is calculated", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "category",
			SampleCount: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// All 10 objects are covered (4 categories covering all 10 objects)
		// electronics(3) + books(3) + clothing(2) + furniture(2) = 10
		assert.Equal(t, 100.0, resp.EstimatedPercentCovered)
	})
}

func TestFilterSampling_MultiNode(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	grpcClient, conn := client(t, compose.GetWeaviate().GrpcURI())
	defer conn.Close()

	createShardedTestClass(t)
	insertShardedTestData(t)
	defer helper.DeleteClass(t, testClassName)

	t.Run("sample text property across shards", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "category",
			SampleCount: 10,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// We have 6 unique categories: electronics (4), clothing (3), furniture (3), books (4), toys (3), sports (3)
		assert.Len(t, resp.Samples, 6)
		assert.Equal(t, uint64(20), resp.TotalObjects)

		// Check that cardinalities are aggregated correctly across shards
		categoryCount := make(map[string]uint64)
		for _, sample := range resp.Samples {
			categoryCount[sample.GetTextValue()] = sample.Cardinality
		}

		assert.Equal(t, uint64(4), categoryCount["electronics"])
		assert.Equal(t, uint64(4), categoryCount["books"])
		assert.Equal(t, uint64(3), categoryCount["clothing"])
		assert.Equal(t, uint64(3), categoryCount["furniture"])
		assert.Equal(t, uint64(3), categoryCount["toys"])
		assert.Equal(t, uint64(3), categoryCount["sports"])
	})

	t.Run("sample int property across shards", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "count",
			SampleCount: 20,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Check that total objects is correct
		assert.Equal(t, uint64(20), resp.TotalObjects)

		// We have some duplicate count values, so total samples should be less than 20
		// Values: 10 (3x), 20 (3x), 30 (2x), 5, 15, 2 (2x), 8, 100, 200, 50, 40, 25, 12, 75, 1, 3
		// Actually some are unique, some are duplicated
		assert.LessOrEqual(t, len(resp.Samples), 20)
	})

	t.Run("sample with limit across shards", func(t *testing.T) {
		resp, err := grpcClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
			Collection:  testClassName,
			Property:    "category",
			SampleCount: 3,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should only return 3 samples (limited), sorted by cardinality
		assert.Len(t, resp.Samples, 3)
		assert.Equal(t, uint64(20), resp.TotalObjects)

		// Top 3 should be electronics (4), books (4), and one of the 3s
		// Due to sorting, we should see the highest cardinalities first
		for i := 0; i < len(resp.Samples)-1; i++ {
			assert.GreaterOrEqual(t, resp.Samples[i].Cardinality, resp.Samples[i+1].Cardinality)
		}
	})

	t.Run("connect to different nodes", func(t *testing.T) {
		// Test connecting to different nodes in the cluster
		// GetWeaviateNode uses 1-based indexing
		for i := 1; i <= 3; i++ {
			node := compose.GetWeaviateNode(i)
			require.NotNil(t, node, "node %d should not be nil", i)
			nodeClient, nodeConn := client(t, node.GrpcURI())

			resp, err := nodeClient.FilterSampling(ctx, &pb.FilterSamplingRequest{
				Collection:  testClassName,
				Property:    "category",
				SampleCount: 10,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)

			// All nodes should return the same aggregated result
			assert.Len(t, resp.Samples, 6)
			assert.Equal(t, uint64(20), resp.TotalObjects)

			nodeConn.Close()
		}
	})
}
