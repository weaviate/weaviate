package inverted

import (
	"fmt"
	"sort"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/filters"
)

type propValuePair struct {
	prop         string
	value        []byte
	operator     filters.Operator
	hasFrequency bool
	docIDs       docPointers
	children     []*propValuePair
}

func (pv *propValuePair) fetchDocIDs(tx *bolt.Tx, searcher *Searcher, limit int) error {
	if pv.operator.OnValue() {
		b := tx.Bucket(helpers.BucketFromPropName(pv.prop))
		if b == nil {
			return fmt.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		pointers, err := searcher.docPointers(pv.operator, b, pv.value, limit, pv.hasFrequency)
		if err != nil {
			return err
		}

		pv.docIDs = pointers
	} else {
		for i, child := range pv.children {
			err := child.fetchDocIDs(tx, searcher, limit)
			if err != nil {
				return errors.Wrapf(err, "nested child %d", i)
			}
		}
	}

	return nil
}

func (pv *propValuePair) mergeDocIDs() (*docPointers, error) {
	if pv.operator.OnValue() {
		return &pv.docIDs, nil
	}

	switch pv.operator {
	case filters.OperatorAnd:
		return mergeAnd(pv.children)
	case filters.OperatorOr:
		return mergeOr(pv.children)
	default:
		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
	}
}

func mergeAnd(children []*propValuePair) (*docPointers, error) {
	sets := make([]*docPointers, len(children))

	// retrieve child IDs
	for i, child := range children {
		docIDs, err := child.mergeDocIDs()
		if err != nil {
			return nil, errors.Wrapf(err, "retrieve doc ids of child %d", i)
		}

		sets[i] = docIDs
	}

	// merge AND
	found := map[uint32]int{} // map[id]count
	for _, set := range sets {
		for _, pointer := range set.docIDs {
			count, _ := found[pointer.id]
			count++
			found[pointer.id] = count
		}
	}

	var out docPointers
	for id, count := range found {
		if count != len(sets) {
			continue
		}

		out.docIDs = append(out.docIDs, docPointer{
			id: id,
		})
	}

	sort.Slice(out.docIDs, func(a, b int) bool {
		return out.docIDs[a].id < out.docIDs[b].id
	})

	return &out, nil
}

func mergeOr(children []*propValuePair) (*docPointers, error) {
	sets := make([]*docPointers, len(children))

	// retrieve child IDs
	for i, child := range children {
		docIDs, err := child.mergeDocIDs()
		if err != nil {
			return nil, errors.Wrapf(err, "retrieve doc ids of child %d", i)
		}

		sets[i] = docIDs
	}

	// merge AND
	found := map[uint32]int{} // map[id]count
	for _, set := range sets {
		for _, pointer := range set.docIDs {
			count, _ := found[pointer.id]
			count++
			found[pointer.id] = count
		}
	}

	var out docPointers
	for id := range found {
		// TODO: improve score if item was contained more often

		out.docIDs = append(out.docIDs, docPointer{
			id: id,
		})
	}

	sort.Slice(out.docIDs, func(a, b int) bool {
		return out.docIDs[a].id < out.docIDs[b].id
	})

	return &out, nil
}
