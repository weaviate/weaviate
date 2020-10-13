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

func (a *Aggregator) numericalProperty(ctx context.Context,
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

		// TODO: support int props, for now it just assumes all props are float
		agg := newNumericalAggregator()

		if err := b.ForEach(func(k, v []byte) error {
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
		}); err != nil {
			return err
		}

		// TODO: check which aggregators are actually requested
		out.NumericalAggregations[traverser.MeanAggregator.String()] = agg.Mean()

		return nil
	}); err != nil {
		return out, err
	}

	return out, nil
}

func newNumericalAggregator() *numericalAggregator {
	return &numericalAggregator{
		min: math.MaxFloat64,
		max: math.SmallestNonzeroFloat64,
	}
}

type numericalAggregator struct {
	count uint32
	min   float64
	max   float64
	sum   float64
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

	// TODO: compare min/max

	a.count += countParsed
	a.sum += numberParsed * float64(countParsed)
	return nil
}

func (a *numericalAggregator) Mean() float64 {
	// TODO: handle zero case
	return a.sum / float64(a.count)
}
