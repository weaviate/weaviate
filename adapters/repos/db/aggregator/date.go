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
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/aggregation"
)

func addDateAggregations(prop *aggregation.Property,
	aggs []aggregation.Aggregator, agg *dateAggregator,
) {
	if prop.DateAggregations == nil {
		prop.DateAggregations = map[string]interface{}{}
	}
	agg.buildPairsFromCounts()

	// if there are no elements to aggregate over because a filter does not match anything, calculating median etc. makes
	// no sense. Non-existent entries evaluate to nil with an interface{} map
	if agg.count == 0 {
		for _, entry := range aggs {
			if entry == aggregation.CountAggregator {
				prop.DateAggregations["count"] = int64(agg.count)
				break
			}
		}
		return
	}

	// when combining the results from different shards, we need the raw dates to recompute the mode and median.
	// Therefore we add a reference later which needs to be cleared out before returning the results to a user
	for _, aProp := range aggs {
		switch aProp {
		case aggregation.ModeAggregator, aggregation.MedianAggregator:
			prop.DateAggregations["_dateAggregator"] = agg
		}
	}

	for _, aProp := range aggs {
		switch aProp {
		case aggregation.MinimumAggregator:
			prop.DateAggregations[aProp.String()] = agg.Min()
		case aggregation.MaximumAggregator:
			prop.DateAggregations[aProp.String()] = agg.Max()
		case aggregation.ModeAggregator:
			prop.DateAggregations[aProp.String()] = agg.Mode()
		case aggregation.CountAggregator:
			prop.DateAggregations[aProp.String()] = agg.Count()
		case aggregation.MedianAggregator:
			prop.DateAggregations[aProp.String()] = agg.Median()

		default:
			continue
		}
	}
}

type dateAggregator struct {
	count        uint64
	maxCount     uint64
	min          timestamp
	max          timestamp
	mode         timestamp
	pairs        []timestampCountPair // for row-based median calculation
	valueCounter map[timestamp]uint64 // for individual median calculation
}

func newDateAggregator() *dateAggregator {
	return &dateAggregator{
		min:          timestamp{epochNano: math.MaxInt64},
		valueCounter: map[timestamp]uint64{},
		pairs:        make([]timestampCountPair, 0),
	}
}

// timestamp allows us to contain multiple representations of a datetime
// the nanosecs value is needed for the numerical comparisons, and the
// string value is what the user expects to see
type timestamp struct {
	epochNano int64
	rfc3339   string
}

func newTimestamp(epochNano int64) timestamp {
	return timestamp{
		epochNano: epochNano,
		rfc3339:   time.Unix(0, epochNano).UTC().Format(time.RFC3339Nano),
	}
}

type timestampCountPair struct {
	value timestamp
	count uint64
}

func (a *dateAggregator) AddTimestamp(rfc3339 string) error {
	t, err := time.Parse(time.RFC3339Nano, rfc3339)
	if err != nil {
		return fmt.Errorf("failed to parse timestamp: %s", err)
	}

	ts := timestamp{
		epochNano: t.UnixNano(),
		rfc3339:   rfc3339,
	}
	return a.addRow(ts, 1)
}

func (a *dateAggregator) AddTimestampRow(b []byte, count uint64) error {
	nsec, err := inverted.ParseLexicographicallySortableInt64(b)
	if err != nil {
		return errors.Wrap(err, "read int64")
	}

	ts := newTimestamp(nsec)

	return a.addRow(ts, count)
}

func (a *dateAggregator) addRow(ts timestamp, count uint64) error {
	if count == 0 {
		// skip
		return nil
	}

	a.count += count
	if ts.epochNano < a.min.epochNano {
		a.min = ts
	}
	if ts.epochNano > a.max.epochNano {
		a.max = ts
	}

	currentCount := a.valueCounter[ts]
	currentCount += count
	a.valueCounter[ts] = currentCount

	return nil
}

func (a *dateAggregator) Max() string {
	return a.max.rfc3339
}

func (a *dateAggregator) Min() string {
	return a.min.rfc3339
}

// Mode does not require preparation if build from rows, but requires a call of
// buildPairsFromCounts() if it was built using individual objects
func (a *dateAggregator) Mode() string {
	return a.mode.rfc3339
}

func (a *dateAggregator) Count() int64 {
	return int64(a.count)
}

// Median does not require preparation if build from rows, but requires a call of
// buildPairsFromCounts() if it was built using individual objects
//
// Check the numericalAggregator.Median() for details about the calculation
func (a *dateAggregator) Median() string {
	middleIndex := a.count / 2
	count := uint64(0)
	for index, pair := range a.pairs {
		count += pair.count
		if a.count%2 == 1 && count > middleIndex {
			return pair.value.rfc3339 // case a)
		} else if a.count%2 == 0 {
			if count == middleIndex {
				MedianEpochNano := pair.value.epochNano + (a.pairs[index+1].value.epochNano-pair.value.epochNano)/2
				return time.Unix(0, MedianEpochNano).UTC().Format(time.RFC3339Nano) // case b2)
			} else if count > middleIndex {
				return pair.value.rfc3339 // case b1)
			}
		}
	}
	panic("Couldn't determine median. This should never happen. Did you add values and call buildRows before?")
}

// turns the value counter into a sorted list, as well as identifying the mode
func (a *dateAggregator) buildPairsFromCounts() {
	a.pairs = a.pairs[:0] // clear out old values in case this function called more than once
	for value, count := range a.valueCounter {
		if count > a.maxCount {
			a.maxCount = count
			a.mode = value
		}
		a.pairs = append(a.pairs, timestampCountPair{value: value, count: count})
	}

	sort.Slice(a.pairs, func(x, y int) bool {
		return a.pairs[x].value.epochNano < a.pairs[y].value.epochNano
	})
}
