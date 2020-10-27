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
	"sort"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func extractLimitFromTopOccs(aggs []traverser.Aggregator) int {
	for _, agg := range aggs {
		if agg.Type == traverser.TopOccurrencesType && agg.Limit != nil {
			return *agg.Limit
		}
	}

	// we couldn't extract a limit, default to something reasonable
	return 5
}

func newTextAggregator(limit int) *textAggregator {
	return &textAggregator{itemCounter: map[string]int{}, max: limit}
}

type textAggregator struct {
	max   int
	count uint32

	itemCounter map[string]int

	// always keep sorted, so we can cut off the last elem, when it grows larger
	// than max
	topPairs []aggregation.TextOccurrence
}

func (a *Aggregator) parseAndAddTextRow(agg *textAggregator,
	v []byte, propName schema.PropertyName) error {
	obj, err := storobj.FromBinary(v)
	if err != nil {
		return errors.Wrap(err, "unmarshal object")
	}

	s := obj.Schema()
	if s == nil {
		return nil
	}

	item, ok := s.(map[string]interface{})[propName.String()]
	if !ok {
		return nil
	}

	return agg.AddText(item.(string))
}

func (a *textAggregator) AddText(value string) error {
	a.count++

	itemCount := a.itemCounter[value]
	itemCount++
	a.itemCounter[value] = itemCount
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
	if a.count == 0 {
		return out
	}

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

	out.Count = int(a.count)
	return out
}
