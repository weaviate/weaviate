package aggregator

import (
	"context"
	"fmt"
	"sort"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
)

func (a *Aggregator) textProperty(ctx context.Context,
	prop traverser.AggregateProperty) (*aggregation.Property, error) {
	out := aggregation.Property{
		Type:            aggregation.PropertyTypeText,
		TextAggregation: aggregation.Text{},
	}

	limit := extractLimitFromTopOccs(prop.Aggregators)

	if err := a.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(helpers.ObjectsBucket)
		if b == nil {
			return fmt.Errorf("could not find bucket for prop %s", prop.Name)
		}

		agg := newTextAggregator(limit)

		if err := b.ForEach(func(_, v []byte) error {
			return a.parseAndAddTextRow(agg, v, prop.Name)
		}); err != nil {
			return err
		}

		out.TextAggregation = agg.Res()

		return nil
	}); err != nil {
		return nil, err
	}

	return &out, nil
}

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
