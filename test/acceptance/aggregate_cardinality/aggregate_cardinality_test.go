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

package aggregate_cardinality_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	className = "AggCardinality"

	objectCount = 12
	// name is unique per object, category cycles
	distinctNames      = objectCount
	distinctCategories = 3
)

func boolPtr(b bool) *bool { return &b }

// Sizing the estimate: the whole fixture fits one memtable (and, if it flushes,
// segments too small to carry a bloom filter), both of which GetKeysCount
// counts exactly. The band absorbs a bloom-filter estimate should a sized
// segment win instead.
func assertEstimate(t *testing.T, agg *pb.AggregateReply_Aggregations_Aggregation, wantDistinct int) {
	t.Helper()
	require.NotNil(t, agg.ApproximateCardinality,
		"property %q carries no approximate cardinality", agg.GetProperty())
	got := agg.GetApproximateCardinality()
	assert.InDelta(t, int64(wantDistinct), got, 1,
		"property %q: estimate %d not near the %d distinct keys ingested",
		agg.GetProperty(), got, wantDistinct)
}

func singleAggregation(t *testing.T, resp *pb.AggregateReply, property string) *pb.AggregateReply_Aggregations_Aggregation {
	t.Helper()
	require.NotNil(t, resp.GetSingleResult(), "a non-group-by request must reply with a single result")
	aggs := resp.GetSingleResult().GetAggregations().GetAggregations()
	require.Len(t, aggs, 1)
	require.Equal(t, property, aggs[0].GetProperty())
	return aggs[0]
}

func setupSchema(t *testing.T) {
	t.Helper()

	helper.CreateClass(t, &models.Class{
		Class:      className,
		Vectorizer: "none",
		// one shard, so the coordinator's max-across-shards merge has a single
		// input and the estimate can be compared against the whole fixture
		ShardingConfig: map[string]any{"desiredCount": 1},
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: boolPtr(true),
				IndexSearchable: boolPtr(false),
			},
			{
				Name:            "category",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: boolPtr(true),
				IndexSearchable: boolPtr(false),
			},
		},
	})
}

func setupData(t *testing.T) {
	t.Helper()

	objects := make([]*models.Object, 0, objectCount)
	for i := 0; i < objectCount; i++ {
		objects = append(objects, &models.Object{
			Class: className,
			Properties: map[string]any{
				"name":     fmt.Sprintf("city-%02d", i),
				"category": fmt.Sprintf("cat-%d", i%distinctCategories),
			},
		})
	}
	helper.CreateObjectsBatch(t, objects)
}

func TestAggregateApproximateCardinality(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
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

	setupSchema(t)
	defer helper.DeleteClass(t, className)
	setupData(t)

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection:   className,
			ObjectsCount: true,
		})
		assert.NoError(ct, err)
		if resp == nil {
			return
		}
		assert.Equal(ct, int64(objectCount), resp.GetSingleResult().GetObjectsCount())
	}, 30*time.Second, 500*time.Millisecond)

	t.Run("cardinality-only property skips the object scan and still answers", func(t *testing.T) {
		resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection: className,
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{Property: "name", ApproximateCardinality: true},
			},
		})
		require.NoError(t, err)

		name := singleAggregation(t, resp, "name")
		assertEstimate(t, name, distinctNames)
		assert.Nil(t, name.GetAggregation(), "cardinality-only property must carry no typed aggregation")
	})

	t.Run("cardinality and a typed aggregation on the same property", func(t *testing.T) {
		resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection: className,
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{
					Property: "category",
					Aggregation: &pb.AggregateRequest_Aggregation_Text_{
						Text: &pb.AggregateRequest_Aggregation_Text{
							Count:         true,
							Type:          true,
							TopOccurences: true,
						},
					},
					ApproximateCardinality: true,
				},
			},
		})
		require.NoError(t, err)

		category := singleAggregation(t, resp, "category")

		text := category.GetText()
		require.NotNil(t, text, "the typed aggregation must survive alongside the estimate")
		assert.Equal(t, int64(objectCount), text.GetCount())
		assert.Equal(t, "text", text.GetType())
		require.Len(t, text.GetTopOccurences().GetItems(), distinctCategories)

		assertEstimate(t, category, distinctCategories)
	})

	t.Run("unknown property is rejected", func(t *testing.T) {
		_, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection: className,
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{Property: "doesNotExist", ApproximateCardinality: true},
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "doesNotExist")
	})
}
