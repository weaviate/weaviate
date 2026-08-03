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
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/aggregation"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (ua unfilteredAggregator) boolProperty(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type: aggregation.PropertyTypeBoolean,
	}

	b := ua.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name.String()))
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newBoolAggregator()

	// bool never has a frequency, so it's either a Set or RoaringSet
	if b.Strategy() == lsmkv.StrategyRoaringSet {
		extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
			return ua.parseAndAddBoolRowRoaringSet(agg, k, b)
		}

		if err := iteratorConcurrently(ctx, b, func() Cursor { return RoaringCursor{b.CursorRoaringSetCtx(ctx)} }, extract, ua.logger); err != nil {
			return nil, err
		}
	} else {
		extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
			return ua.parseAndAddBoolRowSet(agg, k, vv)
		}

		if err := iteratorConcurrently(ctx, b, func() Cursor { return SetCursor{b.SetCursor()} }, extract, ua.logger); err != nil {
			return nil, err
		}
	}

	out.BooleanAggregation = agg.Res()

	return &out, nil
}

func (ua unfilteredAggregator) boolArrayProperty(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type: aggregation.PropertyTypeBoolean,
	}

	b := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newBoolAggregator()

	extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
		return ua.parseAndAddBoolArrayRow(agg, v, prop.Name)
	}

	err := iteratorConcurrently(ctx, b, func() Cursor { return ReplaceCursor{b.Cursor()} }, extract, ua.logger)
	if err != nil {
		return nil, err
	}

	out.BooleanAggregation = agg.Res()

	return &out, nil
}

func (ua unfilteredAggregator) parseAndAddBoolRowSet(agg *boolAggregator, k []byte, v [][]byte) error {
	if len(k) != 1 {
		// we expect to see a single byte for a marshalled bool
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 1: got %d", len(k))
	}

	if err := agg.AddBoolRow(k, uint64(len(v))); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddBoolRowRoaringSet(agg *boolAggregator, k []byte, v *sroar.Bitmap) error {
	if len(k) != 1 {
		// we expect to see a single byte for a marshalled bool
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 1: got %d", len(k))
	}

	if err := agg.AddBoolRow(k, uint64(v.GetCardinality())); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddBoolArrayRow(agg *boolAggregator,
	v []byte, propName schema.PropertyName,
) error {
	items, ok, err := storobj.ParseAndExtractBoolArrayProp(v, propName.String())
	if err != nil {
		return errors.Wrap(err, "parse and extract prop")
	}

	if !ok {
		return nil
	}

	for i := range items {
		if err := agg.AddBool(items[i]); err != nil {
			return err
		}
	}

	return nil
}

func (ua unfilteredAggregator) floatProperty(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]interface{}{},
	}

	b := ua.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name.String()))
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newNumericalAggregator()

	// float never has a frequency, so it's either a Set or RoaringSet
	if b.Strategy() == lsmkv.StrategyRoaringSet {
		extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
			return ua.parseAndAddFloatRowRoaringSet(agg, k, b)
		}

		if err := iteratorConcurrently(ctx, b, func() Cursor { return RoaringCursor{b.CursorRoaringSetCtx(ctx)} }, extract, ua.logger); err != nil {
			return nil, err
		}
	} else {
		extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
			return ua.parseAndAddFloatRowSet(agg, k, vv)
		}

		if err := iteratorConcurrently(ctx, b, func() Cursor { return SetCursor{b.SetCursor()} }, extract, ua.logger); err != nil {
			return nil, err
		}
	}

	addNumericalAggregations(&out, prop.Aggregators, agg)

	return &out, nil
}

func (ua unfilteredAggregator) intProperty(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]interface{}{},
	}

	b := ua.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name.String()))
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newNumericalAggregator()

	// int never has a frequency, so it's either a Set or RoaringSet
	if b.Strategy() == lsmkv.StrategyRoaringSet {
		extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
			return ua.parseAndAddIntRowRoaringSet(agg, k, b)
		}

		if err := iteratorConcurrently(ctx, b, func() Cursor { return RoaringCursor{b.CursorRoaringSetCtx(ctx)} }, extract, ua.logger); err != nil {
			return nil, err
		}
	} else {
		extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
			return ua.parseAndAddIntRowSet(agg, k, vv)
		}

		if err := iteratorConcurrently(ctx, b, func() Cursor { return SetCursor{b.SetCursor()} }, extract, ua.logger); err != nil {
			return nil, err
		}
	}

	addNumericalAggregations(&out, prop.Aggregators, agg)

	return &out, nil
}

func (ua unfilteredAggregator) dateProperty(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:             aggregation.PropertyTypeDate,
		DateAggregations: map[string]interface{}{},
	}

	b := ua.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name.String()))
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newDateAggregator()

	// dates don't have frequency, so it's either a Set or RoaringSet
	if b.Strategy() == lsmkv.StrategyRoaringSet {
		extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
			return ua.parseAndAddDateRowRoaringSet(agg, k, b)
		}

		if err := iteratorConcurrently(ctx, b, func() Cursor { return RoaringCursor{b.CursorRoaringSetCtx(ctx)} }, extract, ua.logger); err != nil {
			return nil, err
		}
	} else {
		extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
			return ua.parseAndAddDateRowSet(agg, k, vv)
		}

		if err := iteratorConcurrently(ctx, b, func() Cursor { return SetCursor{b.SetCursor()} }, extract, ua.logger); err != nil {
			return nil, err
		}
	}

	addDateAggregations(&out, prop.Aggregators, agg)

	return &out, nil
}

func (ua unfilteredAggregator) parseAndAddDateRowSet(agg *dateAggregator, k []byte,
	v [][]byte,
) error {
	if len(k) != 8 {
		// dates are stored as epoch nanoseconds, we expect to see an int64
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if err := agg.AddTimestampRow(k, uint64(len(v))); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddDateRowRoaringSet(agg *dateAggregator, k []byte,
	v *sroar.Bitmap,
) error {
	if len(k) != 8 {
		// dates are stored as epoch nanoseconds, we expect to see an int64
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if err := agg.AddTimestampRow(k, uint64(v.GetCardinality())); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) dateArrayProperty(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:             aggregation.PropertyTypeDate,
		DateAggregations: map[string]interface{}{},
	}

	b := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newDateAggregator()

	extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
		return ua.parseAndAddDateArrayRow(agg, v, prop.Name)
	}

	err := iteratorConcurrently(ctx, b, func() Cursor { return ReplaceCursor{b.Cursor()} }, extract, ua.logger)
	if err != nil {
		return nil, err
	}

	addDateAggregations(&out, prop.Aggregators, agg)

	return &out, nil
}

func (ua unfilteredAggregator) parseAndAddDateArrayRow(agg *dateAggregator,
	v []byte, propName schema.PropertyName,
) error {
	items, ok, err := storobj.ParseAndExtractProperty(v, propName.String())
	if err != nil {
		return errors.Wrap(err, "parse and extract prop")
	}

	if !ok {
		return nil
	}

	for i := range items {
		if err := agg.AddTimestamp(items[i]); err != nil {
			return err
		}
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddFloatRowSet(agg *numericalAggregator, k []byte,
	v [][]byte,
) error {
	if len(k) != 8 {
		// we expect to see either an int64 or a float64, so any non-8 length
		// is unexpected
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if err := agg.AddFloat64Row(k, uint64(len(v))); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddFloatRowRoaringSet(agg *numericalAggregator, k []byte,
	v *sroar.Bitmap,
) error {
	if len(k) != 8 {
		// we expect to see either an int64 or a float64, so any non-8 length
		// is unexpected
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if err := agg.AddFloat64Row(k, uint64(v.GetCardinality())); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddIntRowSet(agg *numericalAggregator, k []byte,
	v [][]byte,
) error {
	if len(k) != 8 {
		// we expect to see either an int64 or a float64, so any non-8 length
		// is unexpected
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if err := agg.AddInt64Row(k, uint64(len(v))); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddIntRowRoaringSet(agg *numericalAggregator, k []byte,
	v *sroar.Bitmap,
) error {
	if len(k) != 8 {
		// we expect to see either an int64 or a float64, so any non-8 length
		// is unexpected
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if err := agg.AddInt64Row(k, uint64(v.GetCardinality())); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddNumberArrayRow(agg *numericalAggregator,
	v []byte, propName schema.PropertyName,
) error {
	items, ok, err := storobj.ParseAndExtractNumberArrayProp(v, propName.String())
	if err != nil {
		return errors.Wrap(err, "parse and extract prop")
	}

	if !ok {
		return nil
	}

	for i := range items {
		err := agg.AddNumberRow(items[i], 1)
		if err != nil {
			return err
		}
	}

	return nil
}

// propertyValuesFromInverted lists the property's values from its inverted
// index: the filterable roaringset bucket whenever the property has one
// (distinct values are its keys, live doc counts their bitmaps'
// cardinalities — exact under any churn), otherwise the searchable bucket's
// stored posting counts when provably churn-free, otherwise the object
// scan. Past prop.TopOccurrencesCutoff distinct values it reports
// CutoffExceeded instead of values; deleted values count toward the cutoff
// until compaction reclaims them. Keys decode per data type into the
// value's string form; for text they are the indexed terms, identical to
// stored values for untokenized properties.
func (ua unfilteredAggregator) propertyValuesFromInverted(ctx context.Context,
	prop aggregation.ParamProperty, dt schema.DataType,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:            aggregation.PropertyTypeText,
		SchemaType:      string(dt),
		TextAggregation: aggregation.Text{},
	}

	// every value the shard saw, not just the top ones: only the merged list
	// tells the combiner the collection's distinct count
	agg := newTextAggregator(int(prop.TopOccurrencesCutoff))
	emit := func(key []byte, docCount int) error {
		return agg.AddTextCount(decodeInvertedKey(key, dt), docCount)
	}
	complete := func() aggregation.Text {
		res := agg.Res()
		res.ValuesComplete = true
		return res
	}

	if b, release := ua.store.AcquireBucketForRead(helpers.BucketFromPropNameLSM(prop.Name.String())); b != nil {
		defer release()
		if b.Strategy() != lsmkv.StrategyRoaringSet {
			return ua.property(ctx, withoutCutoff(prop))
		}

		exceeded, err := b.RoaringSetEachDistinctKey(ctx, int(prop.TopOccurrencesCutoff), emit)
		if err != nil {
			return nil, err
		}
		if exceeded {
			out.TextAggregation = aggregation.Text{CutoffExceeded: true}
			return &out, nil
		}

		out.TextAggregation = complete()
		return &out, nil
	}

	if sb, sbRelease := ua.store.AcquireBucketForRead(helpers.BucketSearchableFromPropNameLSM(prop.Name.String())); sb != nil {
		exceeded, exact, err := sb.InvertedEachDistinctKey(ctx, int(prop.TopOccurrencesCutoff), emit)
		sbRelease()
		if err != nil {
			return nil, err
		}
		if exceeded {
			out.TextAggregation = aggregation.Text{CutoffExceeded: true}
			return &out, nil
		}
		if exact {
			out.TextAggregation = complete()
			return &out, nil
		}
		// churn voided the stored counts (agg untouched); scan objects instead
	}

	return ua.property(ctx, withoutCutoff(prop))
}

func withoutCutoff(prop aggregation.ParamProperty) aggregation.ParamProperty {
	prop.TopOccurrencesCutoff = 0
	return prop
}

// invertedValuesDecodable reports whether the type's filterable-bucket keys
// decode back into value strings.
func invertedValuesDecodable(dt schema.DataType) bool {
	switch dt {
	case schema.DataTypeText, schema.DataTypeTextArray,
		schema.DataTypeInt, schema.DataTypeIntArray,
		schema.DataTypeNumber, schema.DataTypeNumberArray,
		schema.DataTypeBoolean, schema.DataTypeBooleanArray,
		schema.DataTypeDate, schema.DataTypeDateArray,
		schema.DataTypeUUID, schema.DataTypeUUIDArray:
		return true
	default:
		return false
	}
}

// decodeInvertedKey renders a filterable-bucket key as the string form of the
// property value it indexes. Undecodable keys fall back to their raw bytes.
func decodeInvertedKey(key []byte, dt schema.DataType) string {
	switch dt {
	case schema.DataTypeInt, schema.DataTypeIntArray:
		if v, err := entinverted.ParseLexicographicallySortableInt64(key); err == nil {
			return strconv.FormatInt(v, 10)
		}
	case schema.DataTypeNumber, schema.DataTypeNumberArray:
		if v, err := entinverted.ParseLexicographicallySortableFloat64(key); err == nil {
			return strconv.FormatFloat(v, 'g', -1, 64)
		}
	case schema.DataTypeBoolean, schema.DataTypeBooleanArray:
		if len(key) == 1 {
			return strconv.FormatBool(key[0] != 0)
		}
	case schema.DataTypeDate, schema.DataTypeDateArray:
		if v, err := entinverted.ParseLexicographicallySortableInt64(key); err == nil {
			return time.Unix(0, v).UTC().Format(time.RFC3339Nano)
		}
	case schema.DataTypeUUID, schema.DataTypeUUIDArray:
		if v, err := uuid.FromBytes(key); err == nil {
			return v.String()
		}
	default:
	}
	return string(key)
}

func (ua unfilteredAggregator) textProperty(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:            aggregation.PropertyTypeText,
		TextAggregation: aggregation.Text{},
	}

	limit := extractLimitFromTopOccs(prop.Aggregators)

	b := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newTextAggregator(limit)

	// we're looking at the whole object, so this is neither a Set, nor a Map, but
	// a Replace strategy
	extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
		return ua.parseAndAddTextRow(agg, v, prop.Name)
	}

	err := iteratorConcurrently(ctx, b, func() Cursor { return ReplaceCursor{b.Cursor()} }, extract, ua.logger)
	if err != nil {
		return nil, err
	}

	out.TextAggregation = agg.Res()

	return &out, nil
}

func (ua unfilteredAggregator) numberArrayProperty(ctx context.Context,
	prop aggregation.ParamProperty,
) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]interface{}{},
	}

	b := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newNumericalAggregator()

	extract := func(k []byte, v []byte, vv [][]byte, b *sroar.Bitmap) error {
		return ua.parseAndAddNumberArrayRow(agg, v, prop.Name)
	}

	err := iteratorConcurrently(ctx, b, func() Cursor { return ReplaceCursor{b.Cursor()} }, extract, ua.logger)
	if err != nil {
		return nil, err
	}

	addNumericalAggregations(&out, prop.Aggregators, agg)

	return &out, nil
}
