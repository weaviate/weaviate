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

package inverted

import (
	"bytes"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/repos/db/helpers"
	"github.com/semi-technologies/weaviate/entities/filters"
)

type propValuePair struct {
	prop     string
	operator filters.Operator

	// set for all values that can be served by an inverted index, i.e. anything
	// that's not a geoRange
	value []byte

	// only set if operator=OperatorWithinGeoRange, as that cannot be served by a
	// byte value from an inverted index
	valueGeoRange *filters.GeoRange
	hasFrequency  bool
	docIDs        docPointers
	children      []*propValuePair
}

func (pv *propValuePair) fetchDocIDs(tx *bolt.Tx, searcher *Searcher, limit int) error {
	if pv.operator.OnValue() {
		id := helpers.BucketFromPropName(pv.prop)
		b := tx.Bucket(id)
		if b == nil {
			return fmt.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		pointers, err := searcher.docPointers(id, b, limit, pv)
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

	if checksumsIdentical(sets) {
		// all children are identical, no need to merge, simply return the first
		// set
		return sets[0], nil
	}

	// merge AND
	found := map[uint64]int64{} // map[id]count
	for _, set := range sets {
		for _, pointer := range set.docIDs {
			count := found[pointer.id]
			count++
			found[pointer.id] = count
		}
	}

	var out docPointers
	var idsForChecksum []int64
	for id, count := range found {
		if count != int64(len(sets)) {
			continue
		}

		// TODO: optimize to use fixed length slice and cut off (should be
		// considerably cheaper on very long lists, such as we encounter during
		// large classification cases
		out.docIDs = append(out.docIDs, docPointer{
			id: id,
		})
		idsForChecksum = append(idsForChecksum, int64(id))
	}

	checksum, err := docPointerChecksum(idsForChecksum)
	if err != nil {
		return nil, errors.Wrapf(err, "calculate checksum")
	}

	out.checksum = checksum
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

	if checksumsIdentical(sets) {
		// all children are identical, no need to merge, simply return the first
		// set
		return sets[0], nil
	}

	// merge OR
	var checksums [][]byte
	found := map[uint64]int64{} // map[id]count
	for _, set := range sets {
		for _, pointer := range set.docIDs {
			count := found[pointer.id]
			count++
			found[pointer.id] = count
			checksums = append(checksums, set.checksum)
		}
	}

	var out docPointers
	for id := range found {
		// TODO: improve score if item was contained more often

		out.docIDs = append(out.docIDs, docPointer{
			id: id,
		})
	}

	checksum, err := combineChecksums(checksums)
	if err != nil {
		return nil, errors.Wrap(err, "combine checksums")
	}

	out.checksum = checksum
	return &out, nil
}

func checksumsIdentical(sets []*docPointers) bool {
	if len(sets) == 0 {
		return false
	}

	if len(sets) == 1 {
		return true
	}

	lastChecksum := sets[0].checksum
	for _, set := range sets {
		if !bytes.Equal(set.checksum, lastChecksum) {
			return false
		}
	}

	return true
}
