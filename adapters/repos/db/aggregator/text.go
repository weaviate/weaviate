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
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
)

func extractLimitFromTopOccs(aggs []aggregation.Aggregator) int {
	for _, agg := range aggs {
		if agg.Type == aggregation.TopOccurrencesType && agg.Limit != nil {
			return *agg.Limit
		}
	}

	// we couldn't extract a limit, default to something reasonable
	return 5
}

func wantsTopOccurrences(aggs []aggregation.Aggregator) bool {
	for _, agg := range aggs {
		if agg.Type == aggregation.TopOccurrencesType {
			return true
		}
	}
	return false
}

// newTextAggregator tracks how often each value occurs unless onlyRequested
// is set and aggs do not ask for the top occurrences; see
// [aggregation.Params.OnlyRequestedAggregations].
func newTextAggregator(aggs []aggregation.Aggregator, onlyRequested bool) *textAggregator {
	if onlyRequested && !wantsTopOccurrences(aggs) {
		return &textAggregator{}
	}
	return &textAggregator{itemCounter: map[string]int{}, max: extractLimitFromTopOccs(aggs)}
}

// textAggregator counts text values and is safe for concurrent use. It counts
// how often each value occurs only when the top occurrences are requested: with
// mostly unique values that map holds every value it is given.
type textAggregator struct {
	count atomic.Uint64

	mu  sync.Mutex
	max int
	// itemCounter is nil when the top occurrences are not requested.
	itemCounter map[string]int

	// always keep sorted, so we can cut off the last elem, when it grows larger
	// than max
	topPairs []aggregation.TextOccurrence
}

func (a *Aggregator) parseAndAddTextRow(agg *textAggregator,
	v []byte, propName schema.PropertyName,
) error {
	if agg.itemCounter == nil {
		n, err := storobj.CountPropValues(v, propName.String())
		if err != nil {
			return errors.Wrap(err, "parse and count prop")
		}
		agg.count.Add(uint64(n))
		return nil
	}

	items, ok, err := storobj.ParseAndExtractTextProp(v, propName.String())
	if err != nil {
		return errors.Wrap(err, "parse and extract prop")
	}

	if !ok {
		return nil
	}

	for i := range items {
		if err := agg.AddText(items[i]); err != nil {
			return err
		}
	}
	return nil
}

func (a *textAggregator) AddText(value string) error {
	a.count.Add(1)
	if a.itemCounter == nil {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.itemCounter[value]++
	return nil
}

func (a *textAggregator) insertOrdered(elem aggregation.TextOccurrence) {
	if len(a.topPairs) == 0 {
		a.topPairs = []aggregation.TextOccurrence{elem}
		return
	}

	added := false
	for i, pair := range a.topPairs {
		if pair.Occurs > elem.Occurs {
			continue
		}
		// if number of occurrences is the same,
		// skip if string is after one in topPairs
		if pair.Occurs == elem.Occurs && pair.Value < elem.Value {
			continue
		}

		// we have found the first one that's smaller so me must insert before i
		a.topPairs = append(
			a.topPairs[:i], append(
				[]aggregation.TextOccurrence{elem},
				a.topPairs[i:]...,
			)...,
		)

		added = true
		break
	}

	if len(a.topPairs) > a.max {
		a.topPairs = a.topPairs[:len(a.topPairs)-1]
	}

	if !added && len(a.topPairs) < a.max {
		a.topPairs = append(a.topPairs, elem)
	}
}

func (a *textAggregator) Res() aggregation.Text {
	out := aggregation.Text{}
	count := a.count.Load()
	if count == 0 {
		return out
	}
	out.Count = int(count)

	for value, count := range a.itemCounter {
		a.insertOrdered(aggregation.TextOccurrence{
			Value:  value,
			Occurs: count,
		})
	}

	out.Items = a.topPairs
	sort.SliceStable(out.Items, func(a, b int) bool {
		countA := out.Items[a].Occurs
		countB := out.Items[b].Occurs

		if countA != countB {
			return countA > countB
		}

		valueA := out.Items[a].Value
		valueB := out.Items[b].Value
		if len(valueA) == 0 || len(valueB) == 0 {
			return false // order doesn't matter in this case, just prevent a panic
		}

		return valueA[0] < valueB[0]
	})

	return out
}
