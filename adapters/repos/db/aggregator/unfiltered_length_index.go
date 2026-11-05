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

	"github.com/weaviate/sroar"

	"github.com/weaviate/weaviate/adapters/repos/db/docid"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/aggregation"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// textLookupShare bounds the objects a text count looks up one by one to this
// share of all objects: past it, reading every object in order is cheaper.
const textLookupShare = 10

// countFromLengthIndex counts the values of a property from the property
// length index, which records every object under the length of its value, and
// an object without the value under 0. For an array the sum of length times
// objects is the number of values; for text see [unfilteredAggregator.countText].
//
// It reports false, and the property is read from the objects instead, unless
// the request asks for nothing but the count and the index holds every object
// exactly once: objects written before the property was added, or before it
// had an inverted index, have no entry. A text or text[] count also needs a
// caller that reads only what it requests, as the index gives no top
// occurrences; see [aggregation.Params.OnlyRequestedAggregations].
func (ua unfilteredAggregator) countFromLengthIndex(ctx context.Context,
	prop aggregation.ParamProperty, dt schema.DataType,
) (int, bool, error) {
	if !ua.countsFromLengthIndex(dt) || !ua.onlyCountRequested(prop.Aggregators) {
		return 0, false, nil
	}
	if (dt == schema.DataTypeText || dt == schema.DataTypeTextArray) && !ua.params.OnlyRequestedAggregations {
		return 0, false, nil
	}
	class := ua.getSchema.ReadOnlyClass(ua.params.ClassName.String())
	if class == nil || class.InvertedIndexConfig == nil || !class.InvertedIndexConfig.IndexPropertyLength {
		return 0, false, nil
	}
	lengths := ua.store.Bucket(helpers.BucketFromPropNameLengthLSM(prop.Name.String()))
	objects := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if lengths == nil || objects == nil || lengths.Strategy() != lsmkv.StrategyRoaringSet {
		return 0, false, nil
	}

	counts, err := ua.readLengths(ctx, lengths)
	if err != nil {
		return 0, false, fmt.Errorf("read length index: %w", err)
	}
	total, err := objects.Count(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("count objects: %w", err)
	}
	if counts.objects != total {
		ua.logger.WithField("property", prop.Name).
			Debugf("length index holds %d of %d objects, reading the objects instead", counts.objects, total)
		return 0, false, nil
	}
	if dt == schema.DataTypeText {
		return ua.countText(ctx, prop, counts, total)
	}
	return counts.values, true, nil
}

type lengthCounts struct {
	values   int // sum of length times objects
	objects  int
	nonEmpty int           // objects with a length above 0
	empty    *sroar.Bitmap // objects with length 0; nil when there are none
}

func (ua unfilteredAggregator) readLengths(ctx context.Context, b *lsmkv.Bucket) (lengthCounts, error) {
	c := b.CursorRoaringSetCtx(ctx)
	defer c.Close()

	var out lengthCounts
	for k, bm := c.First(); k != nil; k, bm = c.Next() {
		if err := ctx.Err(); err != nil {
			return lengthCounts{}, err
		}
		if bm == nil || bm.IsEmpty() {
			continue
		}
		length, err := entinverted.ParseLexicographicallySortableInt64(k)
		if err != nil {
			return lengthCounts{}, fmt.Errorf("parse length key: %w", err)
		}
		n := bm.GetCardinality()
		out.objects += n
		out.values += int(length) * n
		if length == 0 {
			out.empty = bm.Clone()
		} else {
			out.nonEmpty += n
		}
	}
	return out, nil
}

// countText counts the objects that hold a text property. The length index
// records "" under 0, as it does an object without the property, while the
// object scan counts "" as a value, so the objects under 0 are looked up to
// find the "" among them. It reports false when they are too many to look up
// one by one.
func (ua unfilteredAggregator) countText(ctx context.Context,
	prop aggregation.ParamProperty, counts lengthCounts, total int,
) (int, bool, error) {
	if counts.empty == nil {
		return counts.nonEmpty, true, nil
	}
	empty := counts.empty.GetCardinality()
	if empty*textLookupShare > total {
		ua.logger.WithField("property", prop.Name).
			Debugf("%d of %d objects have no text to count, reading the objects instead", empty, total)
		return 0, false, nil
	}

	name := prop.Name.String()
	emptyText := 0
	hasEmptyText := func(_ context.Context, props *models.PropertySchema, _ uint64) error {
		if values, ok := (*props).(map[string]any); ok {
			if _, ok := values[name]; ok {
				emptyText++
			}
		}
		return nil
	}
	if err := docid.ScanObjectsLSM(ctx, ua.store, counts.empty.ToArray(), hasEmptyText, []string{name}, ua.logger); err != nil {
		return 0, false, fmt.Errorf("look up objects without text: %w", err)
	}
	return counts.nonEmpty + emptyText, true, nil
}

// countsFromLengthIndex reports whether the length index can count dt.
// boolean[] is left out: its result always carries the true and false totals,
// which the length index cannot give.
func (ua unfilteredAggregator) countsFromLengthIndex(dt schema.DataType) bool {
	switch dt {
	case schema.DataTypeText, schema.DataTypeTextArray, schema.DataTypeIntArray,
		schema.DataTypeNumberArray, schema.DataTypeDateArray:
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
