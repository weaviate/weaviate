//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package aggregator

import (
	"math"
	"sort"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/aggregation"
)

func addNumericalAggregations(prop *aggregation.Property,
	aggs []aggregation.Aggregator, agg *numericalAggregator,
) {
	if prop.NumericalAggregations == nil {
		prop.NumericalAggregations = map[string]interface{}{}
	}
	agg.buildPairsFromCounts()

	// if there are no elements to aggregate over because a filter does not match anything, calculating mean etc. makes
	// no sense. Non-existent entries evaluate to nil with an interface{} map
	if agg.count == 0 {
		for _, entry := range aggs {
			if entry == aggregation.CountAggregator {
				prop.NumericalAggregations["count"] = float64(agg.count)
				break
			}
		}
		return
	}

	// when combining the results from different shards, we need the raw numbers to recompute the mode, mean and median.
	// Therefore we add a reference later which needs to be cleared out before returning the results to a user
loop:
	for _, aProp := range aggs {
		switch aProp {
		case aggregation.ModeAggregator, aggregation.MedianAggregator, aggregation.MeanAggregator:
			prop.NumericalAggregations["_numericalAggregator"] = agg
			break loop
		}
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
		max:          -math.MaxFloat64,
		valueCounter: map[float64]uint64{},
		pairs:        make([]floatCountPair, 0),
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
	return a.AddNumberRow(value, 1)
}

// turns the value counter into a sorted list, as well as identifying the mode. Must be called before calling median etc
func (a *numericalAggregator) buildPairsFromCounts() {
	a.pairs = a.pairs[:0] // clear out old values in case this function called more than once
	a.pairs = append(a.pairs, make([]floatCountPair, 0, len(a.valueCounter))...)

	for value, count := range a.valueCounter {
		// get one with higher count or lower value if counts are equal
		if count > a.maxCount || (count == a.maxCount && value < a.mode) {
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
	count uint64,
) error {
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

	currentCount := a.valueCounter[number]
	currentCount += count
	a.valueCounter[number] = currentCount

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
// buildPairsFromCounts() if it was built using individual objects. The call will panic
// if called without adding at least one element or without calling buildPairsFromCounts()
//
// since the pairs are read from an inverted index, which is in turn
// lexicographically sorted, we know that our pairs must also be sorted
//
// There are two cases:
// a) There is an uneven number of elements, then the median element is at index N/2
// b) There is an even number of elements, then the median element is (elem_(N/2) + elem_(N/2+1))/2.
//
//	with two sub-cases:
//	  b1) element N/2 and N/2 + 1 are within the same pair, then the median is the value of this pair
//	  b2) element N/2 and N/2 are part of different pairs, then the average of these pairs is the median and the
//	      median value is not part of the collection itself
func (a *numericalAggregator) Median() float64 {
	middleIndex := a.count / 2
	count := uint64(0)
	for index, pair := range a.pairs {
		count += pair.count
		if a.count%2 == 1 && count > middleIndex {
			return pair.value // case a)
		} else if a.count%2 == 0 {
			if count == middleIndex {
				return (pair.value + a.pairs[index+1].value) / 2 // case b2)
			} else if count > middleIndex {
				return pair.value // case b1)
			}
		}
	}
	panic("Couldn't determine median. This should never happen. Did you add values and call buildRows before?")
}
