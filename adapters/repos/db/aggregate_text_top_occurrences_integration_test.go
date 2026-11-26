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

//go:build integrationTest

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const aggTextTopOccClass = "AggregateTextTopOccurrences"

// TestAggregateTextTopOccurrencesOnlyWhenRequested pins that a text
// aggregation returns the top occurrences only when asked for them, including
// as totalTrue by a coordinator older than this node.
func TestAggregateTextTopOccurrencesOnlyWhenRequested(t *testing.T) {
	ctx := context.Background()
	class := &models.Class{
		Class: aggTextTopOccClass,
		Properties: []*models.Property{{
			Name:         "title",
			DataType:     schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationField,
		}},
	}
	shardLike, _ := testShardWithSettings(t, ctx, class,
		hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false)
	s := concreteShard(t, shardLike)
	for _, title := range []string{"Dune", "Dune", "Emma"} {
		require.NoError(t, s.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(uuid.NewString()),
				Class:      aggTextTopOccClass,
				Properties: map[string]interface{}{"title": title},
			},
			Vector: []float32{1, 2, 3},
		}))
	}

	dune := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorEqual,
		On:       &filters.Path{Class: aggTextTopOccClass, Property: "title"},
		Value:    &filters.Value{Value: "Dune", Type: schema.DataTypeText},
	}}
	filterCases := []struct {
		name      string
		filter    *filters.LocalFilter
		wantCount int
		wantItems []aggregation.TextOccurrence
	}{
		{name: "unfiltered", wantCount: 3, wantItems: []aggregation.TextOccurrence{{Value: "Dune", Occurs: 2}, {Value: "Emma", Occurs: 1}}},
		{name: "filtered", filter: dune, wantCount: 2, wantItems: []aggregation.TextOccurrence{{Value: "Dune", Occurs: 2}}},
	}
	aggCases := []struct {
		name      string
		aggs      []aggregation.Aggregator
		wantItems bool
	}{
		{name: "count only", aggs: []aggregation.Aggregator{aggregation.CountAggregator}},
		{
			name:      "top occurrences",
			aggs:      []aggregation.Aggregator{aggregation.CountAggregator, aggregation.NewTopOccurrencesAggregator(nil)},
			wantItems: true,
		},
		{
			name:      "totalTrue from an older coordinator",
			aggs:      []aggregation.Aggregator{aggregation.CountAggregator, aggregation.TotalTrueAggregator},
			wantItems: true,
		},
	}

	for _, fc := range filterCases {
		for _, ac := range aggCases {
			t.Run(fc.name+"/"+ac.name, func(t *testing.T) {
				res, err := s.Aggregate(ctx, aggregation.Params{
					ClassName:  aggTextTopOccClass,
					Filters:    fc.filter,
					Properties: []aggregation.ParamProperty{{Name: "title", Aggregators: ac.aggs}},
				}, nil)
				require.NoError(t, err)

				want := aggregation.Text{Count: fc.wantCount}
				if ac.wantItems {
					want.Items = fc.wantItems
				}
				require.Equal(t, want, res.Groups[0].Properties["title"].TextAggregation)
			})
		}
	}
}
