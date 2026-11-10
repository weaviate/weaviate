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
	"fmt"
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

// TestAggregateTextCountOnlyKeepsTopOccurrencesUnlessOnlyRequested pins that a
// count-only text aggregation still returns the top occurrences unless the
// caller sets [aggregation.Params.OnlyRequestedAggregations].
func TestAggregateTextCountOnlyKeepsTopOccurrencesUnlessOnlyRequested(t *testing.T) {
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
		hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)
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
	testCases := []struct {
		name      string
		filter    *filters.LocalFilter
		wantCount int
		wantItems []aggregation.TextOccurrence
	}{
		{name: "unfiltered", wantCount: 3, wantItems: []aggregation.TextOccurrence{{Value: "Dune", Occurs: 2}, {Value: "Emma", Occurs: 1}}},
		{name: "filtered", filter: dune, wantCount: 2, wantItems: []aggregation.TextOccurrence{{Value: "Dune", Occurs: 2}}},
	}

	for _, tc := range testCases {
		for _, onlyRequested := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/onlyRequested=%t", tc.name, onlyRequested), func(t *testing.T) {
				res, err := s.Aggregate(ctx, aggregation.Params{
					ClassName: aggTextTopOccClass,
					Filters:   tc.filter,
					Properties: []aggregation.ParamProperty{{
						Name:        "title",
						Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
					}},
					OnlyRequestedAggregations: onlyRequested,
				}, nil)
				require.NoError(t, err)

				want := aggregation.Text{Count: tc.wantCount, Items: tc.wantItems}
				if onlyRequested {
					want.Items = nil
				}
				require.Equal(t, want, res.Groups[0].Properties["title"].TextAggregation)
			})
		}
	}
}
