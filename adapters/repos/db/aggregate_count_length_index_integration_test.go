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
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const aggLengthIndexClass = "AggregateCountLengthIndex"

// TestAggregateCountOfArraysWithLengthIndex pins what a count-only
// aggregation over an array property relies on when it is answered from the
// property length index: after inserts, updates and deletes the index holds
// every live object exactly once, and the counts match the stored arrays.
func TestAggregateCountOfArraysWithLengthIndex(t *testing.T) {
	ctx := context.Background()
	props := map[string]schema.DataType{
		"texts":   schema.DataTypeTextArray,
		"ints":    schema.DataTypeIntArray,
		"numbers": schema.DataTypeNumberArray,
		"dates":   schema.DataTypeDateArray,
	}
	class := &models.Class{Class: aggLengthIndexClass, InvertedIndexConfig: invertedConfig()}
	for name, dt := range props {
		class.Properties = append(class.Properties, &models.Property{Name: name, DataType: dt.PropString()})
	}
	shardLike, _ := testShardWithSettings(t, ctx, class,
		hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)
	s := concreteShard(t, shardLike)

	// arrays of n elements, an empty array for n == 0, no property for n < 0
	values := func(n int) map[string]interface{} {
		out := map[string]interface{}{}
		if n < 0 {
			return out
		}
		texts, ints, numbers, dates := []string{}, []float64{}, []float64{}, []string{}
		for j := range n {
			texts = append(texts, fmt.Sprintf("v%d", j%2))
			ints = append(ints, float64(j))
			numbers = append(numbers, float64(j)+0.5)
			dates = append(dates, time.Date(2020, 1, 1+j, 0, 0, 0, 0, time.UTC).Format(time.RFC3339))
		}
		out["texts"], out["ints"], out["numbers"], out["dates"] = texts, ints, numbers, dates
		return out
	}
	live := map[strfmt.UUID]int{}
	put := func(id strfmt.UUID, n int) {
		require.NoError(t, s.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object:            models.Object{ID: id, Class: aggLengthIndexClass, Properties: values(n)},
			Vector:            []float32{1, 2, 3},
		}))
		live[id] = n
	}

	ids := make([]strfmt.UUID, 40)
	for i := range ids {
		ids[i] = strfmt.UUID(uuid.NewString())
		put(ids[i], i%6-1)
	}
	for i := 0; i < 10; i++ {
		put(ids[i], (i+3)%5) // update, changing the length
	}
	for i := 10; i < 15; i++ {
		require.NoError(t, s.DeleteObject(ctx, ids[i], time.Time{}))
		delete(live, ids[i])
	}

	want := 0
	for _, n := range live {
		want += max(n, 0)
	}
	require.Positive(t, want)
	total, err := s.store.Bucket(helpers.ObjectsBucketLSM).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, len(live), total)

	for name, dt := range props {
		t.Run(string(dt), func(t *testing.T) {
			c := s.store.Bucket(helpers.BucketFromPropNameLengthLSM(name)).CursorRoaringSet()
			indexed := 0
			for k, bm := c.First(); k != nil; k, bm = c.Next() {
				indexed += bm.GetCardinality()
			}
			c.Close()
			require.Equal(t, total, indexed, "the length index must hold every live object once")

			res, err := s.Aggregate(ctx, aggregation.Params{
				ClassName: aggLengthIndexClass,
				Properties: []aggregation.ParamProperty{{
					Name:        schema.PropertyName(name),
					Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
				}},
				OnlyRequestedAggregations: true,
			}, nil)
			require.NoError(t, err)
			got := res.Groups[0].Properties[name]
			switch got.Type {
			case aggregation.PropertyTypeText:
				require.Equal(t, want, got.TextAggregation.Count)
			case aggregation.PropertyTypeNumerical:
				require.Equal(t, float64(want), got.NumericalAggregations["count"])
			case aggregation.PropertyTypeDate:
				require.Equal(t, int64(want), got.DateAggregations["count"])
			default:
				t.Fatalf("unexpected property type %s", got.Type)
			}
		})
	}
}

// TestAggregateCountOfArraysAfterDroppingLastInvertedIndex pins that a
// count-only aggregation reads the objects once the property's last inverted
// index is dropped: the length index stays loaded, but writes no longer update
// it, so it keeps the lengths from before the drop.
func TestAggregateCountOfArraysAfterDroppingLastInvertedIndex(t *testing.T) {
	ctx := context.Background()
	const className = aggLengthIndexClass + "Dropped"
	class := &models.Class{Class: className, InvertedIndexConfig: invertedConfig()}
	class.Properties = []*models.Property{{Name: "ints", DataType: schema.DataTypeIntArray.PropString()}}
	shardLike, _ := testShardWithSettings(t, ctx, class,
		hnsw.UserConfig{Distance: common.DefaultDistanceMetric}, false, false, false)
	s := concreteShard(t, shardLike)

	id := strfmt.UUID(uuid.NewString())
	put := func(ints []float64) {
		require.NoError(t, s.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object:            models.Object{ID: id, Class: className, Properties: map[string]interface{}{"ints": ints}},
			Vector:            []float32{1, 2, 3},
		}))
	}
	count := func() float64 {
		res, err := s.Aggregate(ctx, aggregation.Params{
			ClassName: className,
			Properties: []aggregation.ParamProperty{{
				Name:        "ints",
				Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
			}},
			OnlyRequestedAggregations: true,
		}, nil)
		require.NoError(t, err)
		return res.Groups[0].Properties["ints"].NumericalAggregations["count"].(float64)
	}

	put([]float64{1})
	require.Equal(t, float64(1), count())

	off := false
	prop := class.Properties[0]
	prop.IndexFilterable, prop.IndexSearchable, prop.IndexRangeFilters = &off, &off, &off
	require.NoError(t, s.index.updateProperty(ctx, prop))
	require.NotNil(t, s.store.Bucket(helpers.BucketFromPropNameLengthLSM("ints")),
		"precondition: the length index outlives the drop")

	put([]float64{1, 2, 3})
	require.Equal(t, float64(3), count())
}
