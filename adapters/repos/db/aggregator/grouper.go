package aggregator

import (
	"context"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/adapters/repos/db/inverted"
	"github.com/semi-technologies/weaviate/adapters/repos/db/storobj"
	"github.com/semi-technologies/weaviate/entities/aggregation"
)

// grouper is the component which identifies the top-n groups for a specific
// group-by parameter. It is used as part of the goruped aggregator, which then
// additionally performs an aggregation for each group.
type grouper struct {
	*Aggregator
	values    map[interface{}][]uint32 // map[value]docIDs
	topGroups []group
	limit     int
}

func newGrouper(a *Aggregator, limit int) *grouper {
	return &grouper{
		Aggregator: a,
		values:     map[interface{}][]uint32{},
		limit:      limit,
	}
}

func (g *grouper) Do(ctx context.Context) ([]group, error) {
	if len(g.params.GroupBy.Slice()) > 1 {
		return nil, fmt.Errorf("grouping by cross-refs not supported")
	}

	g.db.View(func(tx *bolt.Tx) error {
		return ScanAll(tx, func(obj *storobj.Object) (bool, error) {
			return true, g.addElement(obj)
		})
	})

	return g.aggregateAndSelect()
}

func (g *grouper) addElement(obj *storobj.Object) error {
	s := obj.Schema()
	if s == nil {
		return nil
	}

	item, ok := s.(map[string]interface{})[g.params.GroupBy.Property.String()]
	if !ok {
		return nil
	}

	ids := g.values[item]
	ids = append(ids, obj.IndexID())
	g.values[item] = ids
	return nil
}

func (g *grouper) aggregateAndSelect() ([]group, error) {
	for value, ids := range g.values {
		g.insertOrdered(group{
			res: aggregation.Group{
				GroupedBy: &aggregation.GroupedBy{
					Path:  g.params.GroupBy.Slice(),
					Value: value,
				},
				Count: len(ids),
			},
			docIDs: ids,
		})
	}

	return g.topGroups, nil
}

func (g *grouper) insertOrdered(elem group) {
	if len(g.topGroups) == 0 {
		g.topGroups = []group{elem}
		return
	}

	added := false
	for i, existing := range g.topGroups {
		if existing.res.Count > elem.res.Count {
			continue
		}

		// we have found the first one that's smaller so me must insert before i
		g.topGroups = append(
			g.topGroups[:i], append(
				[]group{elem},
				g.topGroups[i:]...,
			)...,
		)

		added = true
		break
	}

	if len(g.topGroups) > g.limit {
		g.topGroups = g.topGroups[:len(g.topGroups)-1]
	}

	if !added && len(g.topGroups) < g.limit {
		g.topGroups = append(g.topGroups, elem)
	}
}

// ScanAll iterates over every row in the object buckets
// TODO: where should this live?
func ScanAll(tx *bolt.Tx, scan inverted.ObjectScanFn) error {
	b := tx.Bucket(helpers.ObjectsBucket)
	if b == nil {
		return fmt.Errorf("objects bucket not found")
	}

	b.ForEach(func(_, v []byte) error {
		elem, err := storobj.FromBinary(v)
		if err != nil {
			return errors.Wrapf(err, "unmarshal data object")
		}

		// scanAll has no abort, so we can ignore the first arg
		_, err = scan(elem)
		return err
	})

	return nil
}
