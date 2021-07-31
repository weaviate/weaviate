//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregator

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (ua unfilteredAggregator) boolProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type: aggregation.PropertyTypeBoolean,
	}

	b := ua.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name.String()))
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newBoolAggregator()

	c := b.SetCursor() // bool never has a frequency, so it's always a Set
	defer c.Close()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		err := ua.parseAndAddBoolRow(agg, k, v)
		if err != nil {
			return nil, err
		}
	}

	out.BooleanAggregation = agg.Res()

	return &out, nil
}

func (ua unfilteredAggregator) parseAndAddBoolRow(agg *boolAggregator, k []byte, v [][]byte) error {
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

func (ua unfilteredAggregator) floatProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]float64{},
	}

	b := ua.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name.String()))
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newNumericalAggregator()

	c := b.SetCursor() // flat never has a frequency, so it's always a Set
	defer c.Close()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := ua.parseAndAddFloatRow(agg, k, v); err != nil {
			return nil, err
		}
	}

	addNumericalAggregations(&out, prop.Aggregators, agg)

	return &out, nil
}

func (ua unfilteredAggregator) intProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]float64{},
	}

	b := ua.store.Bucket(helpers.BucketFromPropNameLSM(prop.Name.String()))
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newNumericalAggregator()

	c := b.SetCursor() // int never has a frequency, so it's always a Set
	defer c.Close()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := ua.parseAndAddIntRow(agg, k, v); err != nil {
			return nil, err
		}
	}

	addNumericalAggregations(&out, prop.Aggregators, agg)

	return &out, nil
}

func (ua unfilteredAggregator) parseAndAddFloatRow(agg *numericalAggregator, k []byte,
	v [][]byte) error {
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

func (ua unfilteredAggregator) parseAndAddIntRow(agg *numericalAggregator, k []byte,
	v [][]byte) error {
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

func (ua unfilteredAggregator) parseAndAddNumberArrayRow(agg *numericalAggregator,
	v []byte, propName schema.PropertyName) error {
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

func (ua unfilteredAggregator) textProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
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
	c := b.Cursor()
	defer c.Close()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := ua.parseAndAddTextRow(agg, v, prop.Name); err != nil {
			return nil, err
		}
	}

	out.TextAggregation = agg.Res()

	return &out, nil
}

func (ua unfilteredAggregator) numberArrayProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]float64{},
	}

	b := ua.store.Bucket(helpers.ObjectsBucketLSM)
	if b == nil {
		return nil, errors.Errorf("could not find bucket for prop %s", prop.Name)
	}

	agg := newNumericalAggregator()

	c := b.Cursor()
	defer c.Close()

	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := ua.parseAndAddNumberArrayRow(agg, v, prop.Name); err != nil {
			return nil, err
		}
	}

	addNumericalAggregations(&out, prop.Aggregators, agg)

	return &out, nil
}
