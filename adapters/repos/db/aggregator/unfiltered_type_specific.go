//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregator

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (ua unfilteredAggregator) boolProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type: aggregation.PropertyTypeBoolean,
	}

	if err := ua.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.BucketFromPropName(prop.Name.String()))
		if b == nil {
			return fmt.Errorf("could not find bucket for prop %s", prop.Name)
		}

		agg := newBoolAggregator()

		if err := b.ForEach(func(k, v []byte) error {
			return ua.parseAndAddBoolRow(agg, k, v)
		}); err != nil {
			return err
		}

		out.BooleanAggregation = agg.Res()

		return nil
	}); err != nil {
		return nil, err
	}

	return &out, nil
}

func (ua unfilteredAggregator) parseAndAddBoolRow(agg *boolAggregator, k, v []byte) error {
	if len(k) != 1 {
		// we expect to see a single byte for a marshalled bool
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 1: got %d", len(k))
	}

	if len(v) < 8 {
		// we expect to see a at least a checksum (8 bytes) and a count
		// (uint64), if that's not the case, then the row is corrupt
		return fmt.Errorf("unexpected value length on inverted index, "+
			"expected at least 8: got %d", len(v))
	}

	if err := agg.AddBoolRow(k, v[8:16]); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) floatProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]float64{},
	}

	if err := ua.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.BucketFromPropName(prop.Name.String()))
		if b == nil {
			return fmt.Errorf("could not find bucket for prop %s", prop.Name)
		}

		agg := newNumericalAggregator()

		if err := b.ForEach(func(k, v []byte) error {
			return ua.parseAndAddFloatRow(agg, k, v)
		}); err != nil {
			return err
		}

		addNumericalAggregations(&out, prop.Aggregators, agg)

		return nil
	}); err != nil {
		return nil, err
	}

	return &out, nil
}

func (ua unfilteredAggregator) intProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]float64{},
	}

	if err := ua.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.BucketFromPropName(prop.Name.String()))
		if b == nil {
			return fmt.Errorf("could not find bucket for prop %s", prop.Name)
		}

		agg := newNumericalAggregator()

		if err := b.ForEach(func(k, v []byte) error {
			return ua.parseAndAddIntRow(agg, k, v)
		}); err != nil {
			return err
		}

		addNumericalAggregations(&out, prop.Aggregators, agg)

		return nil
	}); err != nil {
		return nil, err
	}

	return &out, nil
}

func (ua unfilteredAggregator) parseAndAddFloatRow(agg *numericalAggregator, k, v []byte) error {
	if len(k) != 8 {
		// we expect to see either an int64 or a float64, so any non-8 length
		// is unexpected
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if len(v) < 16 {
		// we expect to see a at least a checksum (8 bytes) and a count
		// (uint64), if that's not the case, then the row is corrupt
		return fmt.Errorf("unexpected value length on inverted index, "+
			"expected at least 16: got %d", len(v))
	}

	if err := agg.AddFloat64Row(k, v[8:16]); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) parseAndAddIntRow(agg *numericalAggregator, k, v []byte) error {
	if len(k) != 8 {
		// we expect to see either an int64 or a float64, so any non-8 length
		// is unexpected
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if len(v) < 16 {
		// we expect to see a at least a checksum (8 bytes) and a count
		// (uint64), if that's not the case, then the row is corrupt
		return fmt.Errorf("unexpected value length on inverted index, "+
			"expected at least 16: got %d", len(v))
	}

	if err := agg.AddInt64Row(k, v[8:16]); err != nil {
		return err
	}

	return nil
}

func (ua unfilteredAggregator) textProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:            aggregation.PropertyTypeText,
		TextAggregation: aggregation.Text{},
	}

	limit := extractLimitFromTopOccs(prop.Aggregators)

	if err := ua.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.ObjectsBucket)
		if b == nil {
			return fmt.Errorf("could not find bucket for prop %s", prop.Name)
		}

		agg := newTextAggregator(limit)

		if err := b.ForEach(func(_, v []byte) error {
			return ua.parseAndAddTextRow(agg, v, prop.Name)
		}); err != nil {
			return err
		}

		out.TextAggregation = agg.Res()

		return nil
	}); err != nil {
		return nil, err
	}

	return &out, nil
}
