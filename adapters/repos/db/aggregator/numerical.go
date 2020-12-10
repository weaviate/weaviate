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
	"bytes"
	"encoding/binary"
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func addNumericalAggregations(prop *aggregation.Property,
	aggs []traverser.Aggregator, agg *numericalAggregator) {
	if prop.NumericalAggregations == nil {
		prop.NumericalAggregations = map[string]float64{}
	}

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
		min:          math.MaxFloat64,
		max:          math.SmallestNonzeroFloat64,
		valueCounter: map[float64]uint64{},
	}
}

type numericalAggregator struct {
	count        uint64
	min          float64
	max          float64
	sum          float64
	maxCount     uint64
	mode         float64
	pairs        []floatCountPair   // for row-based median calculation
	valueCounter map[float64]uint64 // for individual median calculation
}

type floatCountPair struct {
	value float64
	count uint64
}

func (a *numericalAggregator) AddFloat64(value float64) error {
	a.count++
	a.sum += value
	if value < a.min {
		a.min = value
	}

	if value > a.max {
		a.max = value
	}

	count := a.valueCounter[value]
	count++
	a.valueCounter[value] = count

	return nil
}

// turns the value counter into a sorted list, as well as identifying the mode
func (a *numericalAggregator) buildPairsFromCounts() {
	for value, count := range a.valueCounter {
		if count > a.maxCount {
			a.maxCount = count
			a.mode = value
		}
		a.pairs = append(a.pairs, floatCountPair{value: value, count: count})
	}

	sort.Slice(a.pairs, func(x, y int) bool {
		return a.pairs[x].value < a.pairs[y].value
	})
}

func (a *numericalAggregator) AddFloat64Row(number, count []byte) error {
	var countParsed uint64

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
	}
	if numberParsed > a.max {
		a.max = numberParsed
	}

	if countParsed > a.maxCount {
		a.maxCount = countParsed
		a.mode = numberParsed
	}

	a.pairs = append(a.pairs, floatCountPair{value: numberParsed, count: countParsed})

	return nil
}

func (a *numericalAggregator) AddInt64Row(number, count []byte) error {
	var countParsed uint64

	numberParsed, err := inverted.ParseLexicographicallySortableInt64(number)
	if err != nil {
		return errors.Wrap(err, "read int64")
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
	}
	if asFloat > a.max {
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

// Mode does not require preparation if build from rows, but requires a call of
// buildPairsFromCounts() if it was built using individual objects
func (a *numericalAggregator) Mode() float64 {
	return a.mode
}

// Median does not require preparation if build from rows, but requires a call of
// buildPairsFromCounts() if it was built using individual objects
func (a *numericalAggregator) Median() float64 {
	var index uint64
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
