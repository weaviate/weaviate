package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (a *Aggregator) floatProperty(ctx context.Context,
	prop traverser.AggregateProperty) (aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]float64{},
	}

	if err := a.shard.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.BucketFromPropName(prop.Name.String()))
		if b == nil {
			return fmt.Errorf("could not find bucket for prop %s", prop.Name)
		}

		agg := newNumericalAggregator()

		if err := b.ForEach(func(k, v []byte) error {
			return a.parseAndAddFloatRow(agg, k, v)
		}); err != nil {
			return err
		}

		a.addNumericalAggregations(&out, prop.Aggregators, agg)

		return nil
	}); err != nil {
		return out, err
	}

	return out, nil
}

func (a *Aggregator) intProperty(ctx context.Context,
	prop traverser.AggregateProperty) (aggregation.Property, error) {
	out := aggregation.Property{
		Type:                  aggregation.PropertyTypeNumerical,
		NumericalAggregations: map[string]float64{},
	}

	if err := a.shard.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.BucketFromPropName(prop.Name.String()))
		if b == nil {
			return fmt.Errorf("could not find bucket for prop %s", prop.Name)
		}

		agg := newNumericalAggregator()

		if err := b.ForEach(func(k, v []byte) error {
			return a.parseAndAddIntRow(agg, k, v)
		}); err != nil {
			return err
		}

		a.addNumericalAggregations(&out, prop.Aggregators, agg)

		return nil
	}); err != nil {
		return out, err
	}

	return out, nil
}

func (a *Aggregator) parseAndAddFloatRow(agg *numericalAggregator, k, v []byte) error {
	if len(k) != 8 {
		// we expect to see either an int64 or a float64, so any non-8 length
		// is unexpected
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if len(v) < 8 {
		// we expect to see a at least a checksum (4 bytes) and a count
		// (uint32), if that's not the case, then the row is corrupt
		return fmt.Errorf("unexpected value length on inverted index, "+
			"expected at least 8: got %d", len(k))
	}

	if err := agg.AddFloat64(k, v[4:8]); err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) parseAndAddIntRow(agg *numericalAggregator, k, v []byte) error {
	if len(k) != 8 {
		// we expect to see either an int64 or a float64, so any non-8 length
		// is unexpected
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 8: got %d", len(k))
	}

	if len(v) < 8 {
		// we expect to see a at least a checksum (4 bytes) and a count
		// (uint32), if that's not the case, then the row is corrupt
		return fmt.Errorf("unexpected value length on inverted index, "+
			"expected at least 8: got %d", len(k))
	}

	if err := agg.AddInt64(k, v[4:8]); err != nil {
		return err
	}

	return nil
}

func (a *Aggregator) addNumericalAggregations(prop *aggregation.Property,
	aggs []traverser.Aggregator, agg *numericalAggregator) {
	for _, aProp := range aggs {
		switch aProp {
		case traverser.MeanAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Mean()
		case traverser.MinimumAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Min()
		case traverser.MaximumAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Max()
		case traverser.MedianAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Median()
		case traverser.ModeAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Mode()
		case traverser.SumAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Sum()
		case traverser.CountAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Count()

		default:
			continue
		}
	}
}

func newNumericalAggregator() *numericalAggregator {
	return &numericalAggregator{
		min: math.MaxFloat64,
		max: math.SmallestNonzeroFloat64,
	}
}

type numericalAggregator struct {
	count    uint32
	min      float64
	max      float64
	sum      float64
	maxCount uint32
	mode     float64
	pairs    []floatCountPair // for median calculation
}

type floatCountPair struct {
	value float64
	count uint32
}

func (a *numericalAggregator) AddFloat64(number, count []byte) error {
	var countParsed uint32

	numberParsed, err := inverted.ParseLexicographicallySortableFloat64(number)
	if err != nil {
		return errors.Wrap(err, "read float64")
	}

	if err := binary.Read(bytes.NewReader(count), binary.LittleEndian,
		&countParsed); err != nil {
		return errors.Wrap(err, "read doc count")
	}

	if countParsed == 0 {
		// skip
		return nil
	}

	a.count += countParsed
	a.sum += numberParsed * float64(countParsed)
	if numberParsed < a.min {
		a.min = numberParsed
	} else if numberParsed > a.max {
		a.max = numberParsed
	}

	if countParsed > a.maxCount {
		a.maxCount = countParsed
		a.mode = numberParsed
	}

	a.pairs = append(a.pairs, floatCountPair{value: numberParsed, count: countParsed})

	return nil
}

func (a *numericalAggregator) AddInt64(number, count []byte) error {
	var countParsed uint32

	numberParsed, err := inverted.ParseLexicographicallySortableInt64(number)
	if err != nil {
		return errors.Wrap(err, "read float64")
	}

	asFloat := float64(numberParsed)

	if err := binary.Read(bytes.NewReader(count), binary.LittleEndian,
		&countParsed); err != nil {
		return errors.Wrap(err, "read doc count")
	}

	if countParsed == 0 {
		// skip
		return nil
	}

	a.count += countParsed
	a.sum += asFloat * float64(countParsed)
	if asFloat < a.min {
		a.min = asFloat
	} else if asFloat > a.max {
		a.max = asFloat
	}

	if countParsed > a.maxCount {
		a.maxCount = countParsed
		a.mode = asFloat
	}

	a.pairs = append(a.pairs, floatCountPair{value: asFloat, count: countParsed})

	return nil
}

func (a *numericalAggregator) Mean() float64 {
	if a.count == 0 {
		return 0
	}
	return a.sum / float64(a.count)
}

func (a *numericalAggregator) Max() float64 {
	return a.max
}

func (a *numericalAggregator) Min() float64 {
	return a.min
}

func (a *numericalAggregator) Sum() float64 {
	return a.sum
}

func (a *numericalAggregator) Count() float64 {
	return float64(a.count)
}

func (a *numericalAggregator) Mode() float64 {
	return a.mode
}

func (a *numericalAggregator) Median() float64 {
	var index uint32
	if a.count%2 == 0 {
		index = a.count / 2
	} else {
		index = a.count/2 + 1
	}

	// since the pairs are read from an inverted index, which is in turn
	// lexicographically sorted, we know that our pairs must also be sorted
	var median float64
	for _, pair := range a.pairs {
		if index <= pair.count {
			median = pair.value
			break
		}
		index -= pair.count
	}

	return median
}
