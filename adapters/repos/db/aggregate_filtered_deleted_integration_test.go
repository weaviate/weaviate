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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const aggDeletedIDsClass = "AggregateDeletedIDs"

func aggCategoryFilter(operator filters.Operator, value string) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: operator,
		On: &filters.Path{
			Class:    schema.ClassName(aggDeletedIDsClass),
			Property: schema.PropertyName("category"),
		},
		Value: &filters.Value{Value: value, Type: schema.DataTypeText},
	}}
}

// TestFilteredAggregateMetaCountSkipsDeletedIDs pins that the meta count of a
// filtered aggregation only counts live objects. A negated filter builds its
// allow list by subtracting the matches from the bitmap factory's universe,
// and after a restart that universe is prefilled from the doc id counter's
// high-water mark — so it contains every deleted doc id. Counting the raw
// allow list therefore overcounts by the number of deleted objects.
func TestFilteredAggregateMetaCountSkipsDeletedIDs(t *testing.T) {
	const total, deleted = 5, 2

	limit := 5
	tests := []struct {
		name      string
		filter    *filters.LocalFilter
		withProps bool
	}{
		{
			name:   "negated filter, meta count only",
			filter: aggCategoryFilter(filters.OperatorNotEqual, "other"),
		},
		{
			name:      "negated filter, with property aggregation",
			filter:    aggCategoryFilter(filters.OperatorNotEqual, "other"),
			withProps: true,
		},
		{
			name:   "positive filter (control)",
			filter: aggCategoryFilter(filters.OperatorEqual, "keep"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			class := &models.Class{
				Class: aggDeletedIDsClass,
				Properties: []*models.Property{{
					Name:         "category",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				}},
			}
			shardLike, _ := testShardWithSettings(t, ctx, class,
				hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)
			s := concreteShard(t, shardLike)

			ids := make([]strfmt.UUID, total)
			for i := range ids {
				obj := &storobj.Object{
					MarshallerVersion: 1,
					Object: models.Object{
						ID:         strfmt.UUID(uuid.NewString()),
						Class:      aggDeletedIDsClass,
						Properties: map[string]interface{}{"category": "keep"},
					},
					Vector: []float32{1, 2, 3},
				}
				require.NoError(t, s.PutObject(ctx, obj))
				ids[i] = obj.Object.ID
			}
			for i := 0; i < deleted; i++ {
				require.NoError(t, s.DeleteObject(ctx, ids[i], time.Time{}))
			}

			// mimic a restart: shard init prefills the bitmap factory's universe
			// from the doc id counter's high-water mark (shard_init_lsm.go), which
			// brings the deleted doc ids back into the universe
			s.bitmapFactory = roaringset.NewBitmapFactory(s.bitmapBufPool,
				func() uint64 { return s.counter.Get() - 1 })

			params := aggregation.Params{
				ClassName:        schema.ClassName(aggDeletedIDsClass),
				Filters:          test.filter,
				IncludeMetaCount: true,
			}
			if test.withProps {
				params.Properties = []aggregation.ParamProperty{{
					Name:        schema.PropertyName("category"),
					Aggregators: []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(&limit)},
				}}
			}

			res, err := s.Aggregate(ctx, params, nil)
			require.NoError(t, err)
			require.Len(t, res.Groups, 1)
			require.Equal(t, total-deleted, res.Groups[0].Count,
				"the meta count must only include live objects")

			if test.withProps {
				items := res.Groups[0].Properties["category"].TextAggregation.Items
				require.Len(t, items, 1)
				require.Equal(t, total-deleted, items[0].Occurs,
					"the property aggregation must only include live objects")
			}
		})
	}
}

// TestGroupedAggregateCountSkipsDeletedIDs is the GroupBy mirror of
// TestFilteredAggregateMetaCountSkipsDeletedIDs. The grouped path receives
// the same deleted-id-polluted allow list from a negated filter, but its
// per-group counts self-correct BY CONSTRUCTION: grouping requires reading
// each object's grouped-by value (grouper.groupFiltered →
// docid.ScanObjectsLSM), the scan skips ids that no longer resolve to a
// stored object, and each group's Count is the number of ids that actually
// resolved — a deleted id can never be grouped. This test pins that
// property so a future refactor (e.g. counting the raw allow list per
// group) cannot silently reintroduce the overcount the filtered meta count
// had.
func TestGroupedAggregateCountSkipsDeletedIDs(t *testing.T) {
	const (
		keepTotal   = 5
		keepDeleted = 2
		dropTotal   = 3
		dropDeleted = 1
	)

	limit := 5
	tests := []struct {
		name   string
		filter *filters.LocalFilter
	}{
		{
			// both groups survive the NotEqual "other" filter; each must
			// count only its live objects
			name:   "negated filter",
			filter: aggCategoryFilter(filters.OperatorNotEqual, "other"),
		},
		{
			name:   "positive filter (control)",
			filter: aggCategoryFilter(filters.OperatorEqual, "keep"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			class := &models.Class{
				Class: aggDeletedIDsClass,
				Properties: []*models.Property{{
					Name:         "category",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				}},
			}
			shardLike, _ := testShardWithSettings(t, ctx, class,
				hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)
			s := concreteShard(t, shardLike)

			put := func(category string) strfmt.UUID {
				obj := &storobj.Object{
					MarshallerVersion: 1,
					Object: models.Object{
						ID:         strfmt.UUID(uuid.NewString()),
						Class:      aggDeletedIDsClass,
						Properties: map[string]interface{}{"category": category},
					},
					Vector: []float32{1, 2, 3},
				}
				require.NoError(t, s.PutObject(ctx, obj))
				return obj.Object.ID
			}

			keepIDs := make([]strfmt.UUID, keepTotal)
			for i := range keepIDs {
				keepIDs[i] = put("keep")
			}
			dropIDs := make([]strfmt.UUID, dropTotal)
			for i := range dropIDs {
				dropIDs[i] = put("drop")
			}
			for i := 0; i < keepDeleted; i++ {
				require.NoError(t, s.DeleteObject(ctx, keepIDs[i], time.Time{}))
			}
			for i := 0; i < dropDeleted; i++ {
				require.NoError(t, s.DeleteObject(ctx, dropIDs[i], time.Time{}))
			}

			// mimic a restart: shard init prefills the bitmap factory's universe
			// from the doc id counter's high-water mark (shard_init_lsm.go), which
			// brings the deleted doc ids back into the universe
			s.bitmapFactory = roaringset.NewBitmapFactory(s.bitmapBufPool,
				func() uint64 { return s.counter.Get() - 1 })

			res, err := s.Aggregate(ctx, aggregation.Params{
				ClassName:        schema.ClassName(aggDeletedIDsClass),
				Filters:          test.filter,
				IncludeMetaCount: true,
				GroupBy: &filters.Path{
					Class:    schema.ClassName(aggDeletedIDsClass),
					Property: schema.PropertyName("category"),
				},
				Properties: []aggregation.ParamProperty{{
					Name:        schema.PropertyName("category"),
					Aggregators: []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(&limit)},
				}},
			}, nil)
			require.NoError(t, err)

			countsByGroup := map[interface{}]int{}
			propOccursByGroup := map[interface{}]int{}
			for _, g := range res.Groups {
				countsByGroup[g.GroupedBy.Value] = g.Count
				items := g.Properties["category"].TextAggregation.Items
				require.Len(t, items, 1)
				propOccursByGroup[g.GroupedBy.Value] = items[0].Occurs
			}

			expected := map[interface{}]int{"keep": keepTotal - keepDeleted}
			if test.filter.Root.Operator == filters.OperatorNotEqual {
				expected["drop"] = dropTotal - dropDeleted
			}
			require.Equal(t, expected, countsByGroup,
				"each group must count only its live objects")
			require.Equal(t, expected, propOccursByGroup,
				"each group's property aggregation must only include live objects")
		})
	}
}
