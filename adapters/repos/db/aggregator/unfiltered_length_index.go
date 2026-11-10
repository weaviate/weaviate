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

package aggregator

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/aggregation"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/schema"
)

// countFromLengthIndex counts the values of an array property from the
// property length index, which records every object under the length of its
// array, and an object without the array under 0, so the sum of length times
// objects is the number of values.
//
// It reports false, and the property is read from the objects instead, unless
// the request asks for nothing but the count and the index holds every object
// exactly once: objects written before the property was added, or before it
// had an inverted index, have no entry. Writes stop updating the index once
// the property has no inverted index left, so it also reports false then. A
// text[] count also needs a caller that reads only what it requests, as the
// index gives no top occurrences; see
// [aggregation.Params.OnlyRequestedAggregations].
func (ua unfilteredAggregator) countFromLengthIndex(ctx context.Context,
	prop aggregation.ParamProperty, dt schema.DataType,
) (int, bool, error) {
	if !ua.countsArrayLength(dt) || !ua.onlyCountRequested(prop.Aggregators) {
		return 0, false, nil
	}
	if dt == schema.DataTypeTextArray && !ua.params.OnlyRequestedAggregations {
		return 0, false, nil
	}
	class := ua.getSchema.ReadOnlyClass(ua.params.ClassName.String())
	if class == nil || class.InvertedIndexConfig == nil || !class.InvertedIndexConfig.IndexPropertyLength {
		return 0, false, nil
	}
	schemaProp, err := schema.GetPropertyByName(class, prop.Name.String())
	if err != nil || !inverted.HasAnyInvertedIndex(schemaProp) {
		return 0, false, nil
	}
	lengths := ua.store.Bucket(helpers.BucketFromPropNameLengthLSM(prop.Name.String()))
	objects := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if lengths == nil || objects == nil || lengths.Strategy() != lsmkv.StrategyRoaringSet {
		return 0, false, nil
	}

	values, indexed, err := ua.sumLengths(ctx, lengths)
	if err != nil {
		return 0, false, fmt.Errorf("read length index: %w", err)
	}
	total, err := objects.Count(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("count objects: %w", err)
	}
	if indexed != total {
		ua.logger.WithField("property", prop.Name).
			Debugf("length index holds %d of %d objects, reading the objects instead", indexed, total)
		return 0, false, nil
	}
	return values, true, nil
}

// sumLengths returns the sum of length times objects over the length index,
// and the number of objects in it.
func (ua unfilteredAggregator) sumLengths(ctx context.Context, b *lsmkv.Bucket) (values, objects int, err error) {
	c := b.CursorRoaringSetCtx(ctx)
	defer c.Close()

	for k, bm := c.First(); k != nil; k, bm = c.Next() {
		if err := ctx.Err(); err != nil {
			return 0, 0, err
		}
		if bm == nil {
			continue
		}
		length, err := entinverted.ParseLexicographicallySortableInt64(k)
		if err != nil {
			return 0, 0, fmt.Errorf("parse length key: %w", err)
		}
		n := bm.GetCardinality()
		objects += n
		values += int(length) * n
	}
	return values, objects, nil
}

// countsArrayLength reports whether the count of dt is the length of its
// array. boolean[] is left out: its result always carries the true and false
// totals, which the length index cannot give.
func (ua unfilteredAggregator) countsArrayLength(dt schema.DataType) bool {
	switch dt {
	case schema.DataTypeTextArray, schema.DataTypeIntArray, schema.DataTypeNumberArray, schema.DataTypeDateArray:
		return true
	default:
		return false
	}
}

func (ua unfilteredAggregator) onlyCountRequested(aggs []aggregation.Aggregator) bool {
	for _, agg := range aggs {
		if agg != aggregation.CountAggregator && agg != aggregation.TypeAggregator {
			return false
		}
	}
	return true
}

// countOnlyProperty builds the result the object scan builds for a request
// that asks for nothing but the count.
func (ua unfilteredAggregator) countOnlyProperty(aggType aggregation.PropertyType,
	aggs []aggregation.Aggregator, count int,
) *aggregation.Property {
	switch aggType {
	case aggregation.PropertyTypeNumerical:
		agg := newNumericalAggregator()
		agg.count = uint64(count)
		out := aggregation.Property{Type: aggType}
		addNumericalAggregations(&out, aggs, agg)
		return &out
	case aggregation.PropertyTypeDate:
		agg := newDateAggregator()
		agg.count = uint64(count)
		out := aggregation.Property{Type: aggType}
		addDateAggregations(&out, aggs, agg)
		return &out
	default:
		return &aggregation.Property{Type: aggType, TextAggregation: aggregation.Text{Count: count}}
	}
}
