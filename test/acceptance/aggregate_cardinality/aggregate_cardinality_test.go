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
	// name is unique per object, category cycles, population repeats in pairs
	distinctNames       = objectCount
	distinctCategories  = 3
	distinctPopulations = objectCount / 2
)

func boolPtr(b bool) *bool { return &b }

// Sizing the estimate: the whole fixture fits one memtable (and, if it flushes,
// segments too small to carry a bloom filter), both of which GetKeysCount
// counts exactly — so the expected value is the distinct key count. The band
// absorbs a bloom-filter estimate should a sized segment win instead.
func assertEstimate(t *testing.T, agg *pb.AggregateReply_Aggregations_Aggregation, wantDistinct int) {
	t.Helper()
	require.NotNil(t, agg.ApproximateCardinality,
		"property %q carries no approximate cardinality", agg.GetProperty())
	got := agg.GetApproximateCardinality()
	assert.InDelta(t, int64(wantDistinct), got, 1,
		"property %q: estimate %d not near the %d distinct keys ingested",
		agg.GetProperty(), got, wantDistinct)
}

func aggsByProperty(t *testing.T, aggs *pb.AggregateReply_Aggregations) map[string]*pb.AggregateReply_Aggregations_Aggregation {
	t.Helper()
	require.NotNil(t, aggs)
	out := map[string]*pb.AggregateReply_Aggregations_Aggregation{}
	for _, a := range aggs.GetAggregations() {
		out[a.GetProperty()] = a
	}
	return out
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
			{
				Name:            "population",
				DataType:        schema.DataTypeInt.PropString(),
				IndexFilterable: boolPtr(true),
			},
			{
				// default indexing: geo values land in the geo index, never in a
				// filterable or searchable LSM bucket
				Name:     "location",
				DataType: schema.DataTypeGeoCoordinates.PropString(),
			},
			{
				Name:              "rangeOnly",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   boolPtr(false),
				IndexRangeFilters: boolPtr(true),
			},
			{
				Name:            "unindexed",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: boolPtr(false),
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
				"name":       fmt.Sprintf("city-%02d", i),
				"category":   fmt.Sprintf("cat-%d", i%distinctCategories),
				"population": int64((i / 2) * 1000),
				"location":   map[string]any{"latitude": 52.0 + float64(i), "longitude": 4.0},
				"rangeOnly":  int64(i),
				"unindexed":  fmt.Sprintf("blob-%02d", i),
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
		require.NotNil(t, resp.GetSingleResult(), "a non-group-by request must reply with a single result")

		aggs := aggsByProperty(t, resp.GetSingleResult().GetAggregations())
		require.Len(t, aggs, 1)
		name := aggs["name"]
		require.NotNil(t, name)
		assertEstimate(t, name, distinctNames)
		assert.Nil(t, name.GetAggregation(), "cardinality-only property must carry no typed aggregation")
	})

	t.Run("several cardinality-only properties at once", func(t *testing.T) {
		resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection: className,
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{Property: "name", ApproximateCardinality: true},
				{Property: "category", ApproximateCardinality: true},
				{Property: "population", ApproximateCardinality: true},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp.GetSingleResult())

		aggs := aggsByProperty(t, resp.GetSingleResult().GetAggregations())
		require.Len(t, aggs, 3)
		assertEstimate(t, aggs["name"], distinctNames)
		assertEstimate(t, aggs["category"], distinctCategories)
		assertEstimate(t, aggs["population"], distinctPopulations)
	})

	t.Run("cardinality alongside objects count", func(t *testing.T) {
		resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection:   className,
			ObjectsCount: true,
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{Property: "category", ApproximateCardinality: true},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp.GetSingleResult())
		assert.Equal(t, int64(objectCount), resp.GetSingleResult().GetObjectsCount())

		aggs := aggsByProperty(t, resp.GetSingleResult().GetAggregations())
		assertEstimate(t, aggs["category"], distinctCategories)
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
		require.NotNil(t, resp.GetSingleResult())

		aggs := aggsByProperty(t, resp.GetSingleResult().GetAggregations())
		require.Len(t, aggs, 1)
		category := aggs["category"]
		require.NotNil(t, category)

		text := category.GetText()
		require.NotNil(t, text, "the typed aggregation must survive alongside the estimate")
		assert.Equal(t, int64(objectCount), text.GetCount())
		assert.Equal(t, "text", text.GetType())
		require.Len(t, text.GetTopOccurences().GetItems(), distinctCategories)

		assertEstimate(t, category, distinctCategories)
	})

	t.Run("cardinality on one property, typed aggregation on another", func(t *testing.T) {
		resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection: className,
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{Property: "name", ApproximateCardinality: true},
				{
					Property: "population",
					Aggregation: &pb.AggregateRequest_Aggregation_Int{
						Int: &pb.AggregateRequest_Aggregation_Integer{Count: true, Maximum: true},
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp.GetSingleResult())

		aggs := aggsByProperty(t, resp.GetSingleResult().GetAggregations())
		require.Len(t, aggs, 2)

		assertEstimate(t, aggs["name"], distinctNames)
		assert.Nil(t, aggs["name"].GetAggregation())

		population := aggs["population"]
		require.NotNil(t, population.GetInt())
		assert.Equal(t, int64(objectCount), population.GetInt().GetCount())
		assert.Equal(t, int64((distinctPopulations-1)*1000), population.GetInt().GetMaximum())
		assert.Nil(t, population.ApproximateCardinality, "estimate must only be attached where requested")
	})

	t.Run("filters do not narrow the estimate", func(t *testing.T) {
		resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection: className,
			Filters: &pb.Filters{
				Operator:  pb.Filters_OPERATOR_EQUAL,
				TestValue: &pb.Filters_ValueText{ValueText: "cat-0"},
				On:        []string{"category"},
			},
			ObjectsCount: true,
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{Property: "name", ApproximateCardinality: true},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp.GetSingleResult())
		assert.Less(t, resp.GetSingleResult().GetObjectsCount(), int64(objectCount),
			"the filter must actually narrow the object count")

		aggs := aggsByProperty(t, resp.GetSingleResult().GetAggregations())
		assertEstimate(t, aggs["name"], distinctNames)
	})

	t.Run("group_by ignores the flag", func(t *testing.T) {
		resp, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
			Collection: className,
			GroupBy: &pb.AggregateRequest_GroupBy{
				Collection: className,
				Property:   "category",
			},
			Aggregations: []*pb.AggregateRequest_Aggregation{
				{
					Property: "category",
					Aggregation: &pb.AggregateRequest_Aggregation_Text_{
						Text: &pb.AggregateRequest_Aggregation_Text{Count: true},
					},
					ApproximateCardinality: true,
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp.GetGroupedResults())
		require.Len(t, resp.GetGroupedResults().GetGroups(), distinctCategories)

		for _, group := range resp.GetGroupedResults().GetGroups() {
			aggs := aggsByProperty(t, group.GetAggregations())
			category := aggs["category"]
			require.NotNil(t, category)
			assert.Nil(t, category.ApproximateCardinality,
				"the estimate is whole-bucket, so it is not reported per group")
			assert.Equal(t, int64(objectCount/distinctCategories), category.GetText().GetCount())
		}
	})

	t.Run("rejected requests", func(t *testing.T) {
		tests := []struct {
			name     string
			property string
			contains string
		}{
			{
				name:     "unknown property",
				property: "doesNotExist",
				contains: "doesNotExist",
			},
			{
				name:     "geo coordinates have no filterable or searchable bucket",
				property: "location",
				contains: "location",
			},
			{
				name:     "rangeable-only numeric has no filterable or searchable bucket",
				property: "rangeOnly",
				contains: "rangeOnly",
			},
			{
				name:     "property with inverted indexing disabled",
				property: "unindexed",
				contains: "unindexed",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := grpcClient.Aggregate(ctx, &pb.AggregateRequest{
					Collection: className,
					Aggregations: []*pb.AggregateRequest_Aggregation{
						{Property: tt.property, ApproximateCardinality: true},
					},
				})
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.contains)
			})
		}
	})
}
