//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package aggregator

import (
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/entities/aggregation"
)

func addNumericalAggregations(prop *aggregation.Property,
	aggs []aggregation.Aggregator, agg *numericalAggregator) {
	if prop.NumericalAggregations == nil {
		prop.NumericalAggregations = map[string]float64{}
	}

	for _, aProp := range aggs {
		switch aProp {
		case aggregation.MeanAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Mean()
		case aggregation.MinimumAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Min()
		case aggregation.MaximumAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Max()
		case aggregation.MedianAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Median()
		case aggregation.ModeAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Mode()
		case aggregation.SumAggregator:
			prop.NumericalAggregations[aProp.String()] = agg.Sum()
		case aggregation.CountAggregator:
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

func (a *numericalAggregator) AddFloat64Row(number []byte,
	count uint64) error {
	numberParsed, err := inverted.ParseLexicographicallySortableFloat64(number)
	if err != nil {
		return errors.Wrap(err, "read float64")
	}

	return a.AddNumberRow(numberParsed, count)
}

func (a *numericalAggregator) AddInt64Row(number []byte, count uint64) error {
	numberParsed, err := inverted.ParseLexicographicallySortableInt64(number)
	if err != nil {
		return errors.Wrap(err, "read int64")
	}

	return a.AddNumberRow(float64(numberParsed), count)
}

func (a *numericalAggregator) AddNumberRow(number float64, count uint64) error {
	if count == 0 {
		// skip
		return nil
	}

	a.count += count
	a.sum += number * float64(count)
	if number < a.min {
		a.min = number
	}
	if number > a.max {
		a.max = number
	}

	if count > a.maxCount {
		a.maxCount = count
		a.mode = number
	}

	a.pairs = append(a.pairs, floatCountPair{value: number, count: count})

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
