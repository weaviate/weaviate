package aggregator

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (a *Aggregator) boolProperty(ctx context.Context,
	prop traverser.AggregateProperty) (aggregation.Property, error) {
	out := aggregation.Property{
		Type: aggregation.PropertyTypeBoolean,
	}

	if err := a.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.BucketFromPropName(prop.Name.String()))
		if b == nil {
			return fmt.Errorf("could not find bucket for prop %s", prop.Name)
		}

		agg := newBoolAggregator()

		if err := b.ForEach(func(k, v []byte) error {
			return a.parseAndAddBoolRow(agg, k, v)
		}); err != nil {
			return err
		}

		out.BooleanAggregation = agg.Res()

		return nil
	}); err != nil {
		return out, err
	}

	return out, nil
}

func newBoolAggregator() *boolAggregator {
	return &boolAggregator{}
}

type boolAggregator struct {
	countTrue  uint32
	countFalse uint32
}

func (a *Aggregator) parseAndAddBoolRow(agg *boolAggregator, k, v []byte) error {
	if len(k) != 1 {
		// we expect to see a single byte for a marshalled bool
		return fmt.Errorf("unexpected key length on inverted index, "+
			"expected 1: got %d", len(k))
	}

	if len(v) < 8 {
		// we expect to see a at least a checksum (4 bytes) and a count
		// (uint32), if that's not the case, then the row is corrupt
		return fmt.Errorf("unexpected value length on inverted index, "+
			"expected at least 8: got %d", len(k))
	}

	if err := agg.AddBool(k, v[4:8]); err != nil {
		return err
	}

	return nil
}

func (a *boolAggregator) AddBool(value, count []byte) error {
	var countParsed uint32

	var valueParsed bool

	if err := binary.Read(bytes.NewReader(value), binary.LittleEndian,
		&valueParsed); err != nil {
		return errors.Wrap(err, "read bool")
	}

	if err := binary.Read(bytes.NewReader(count), binary.LittleEndian,
		&countParsed); err != nil {
		return errors.Wrap(err, "read doc count")
	}

	if countParsed == 0 {
		// skip
		return nil
	}

	if valueParsed {
		a.countTrue += countParsed
	} else {
		a.countFalse += countParsed
	}

	return nil
}

func (a *boolAggregator) Res() aggregation.Boolean {
	out := aggregation.Boolean{}

	count := int(a.countTrue) + int(a.countFalse)
	if count == 0 {
		return out
	}

	out.Count = count
	out.TotalFalse = int(a.countFalse)
	out.TotalTrue = int(a.countTrue)
	out.PercentageTrue = float64(a.countTrue) / float64(count)
	out.PercentageFalse = float64(a.countFalse) / float64(count)

	return out
}
