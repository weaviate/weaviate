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

package inverted

import (
	"bytes"
	"fmt"

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

func (pv *propValuePair) fetchDocIDs(s *Searcher, limit int,
	tolerateDuplicates bool,
) error {
	if pv.operator.OnValue() {
		id := helpers.BucketFromPropNameLSM(pv.prop)
		if pv.prop == filters.InternalPropBackwardsCompatID {
			// the user-specified ID is considered legacy. we
			// support backwards compatibility with this prop
			id = helpers.BucketFromPropNameLSM(filters.InternalPropID)
			pv.prop = filters.InternalPropID
			pv.hasFrequency = false
		}

		if pv.operator == filters.OperatorIsNull {
			id += filters.InternalNullIndex
		}

		b := s.store.Bucket(id)

		if b == nil && pv.operator == filters.OperatorIsNull {
			return errors.Errorf("Nullstate must be indexed to be filterable! " +
				"add `indexNullState: true` to the invertedIndexConfig")
		}

		if b == nil && (pv.prop == filters.InternalPropCreationTimeUnix ||
			pv.prop == filters.InternalPropLastUpdateTimeUnix) {
			return errors.Errorf("timestamps must be indexed to be filterable! " +
				"add `indexTimestaps: true` to the invertedIndexConfig")
		}

		if b == nil && pv.operator != filters.OperatorWithinGeoRange {
			// a nil bucket is ok for a WithinGeoRange filter, as this query is not
			// served by the inverted index, but propagated to a secondary index in
			// .docPointers()
			return errors.Errorf("bucket for prop %s not found - is it indexed?", pv.prop)
		}

		pointers, err := s.docPointers(id, b, limit, pv, tolerateDuplicates)
		if err != nil {
			return err
		}

		pv.docIDs = pointers
	} else {
		for i, child := range pv.children {
			// Explicitly set the limit to 0 (=unlimited) as this is a nested filter,
			// otherwise we run into situations where each subfilter on their own
			// runs into the limit, possibly yielding in "less than limit" results
			// after merging.
			err := child.fetchDocIDs(s, 0, tolerateDuplicates)
			if err != nil {
				return errors.Wrapf(err, "nested child %d", i)
			}
		}
	}

	return nil
}

// if duplicates are acceptable, simpler (and faster) algorithms can be used
// for merging
func (pv *propValuePair) mergeDocIDs(acceptDuplicates bool) (*docPointers, error) {
	if pv.operator.OnValue() {
		return &pv.docIDs, nil
	}

	switch pv.operator {
	case filters.OperatorAnd:
		return mergeAndOptimized(pv.children, acceptDuplicates)
	case filters.OperatorOr:
		return mergeOr(pv.children, acceptDuplicates)
	default:
		return nil, fmt.Errorf("unsupported operator: %s", pv.operator.Name())
	}
}

// TODO: Delete?
// This is only left so we can use it as a control or baselines in tests and
// benchmkarks against the newer optimized version.
func mergeAnd(children []*propValuePair, acceptDuplicates bool) (*docPointers, error) {
	sets := make([]*docPointers, len(children))

	// retrieve child IDs
	for i, child := range children {
		docIDs, err := child.mergeDocIDs(acceptDuplicates)
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
	found := map[uint64]uint64{} // map[id]count
	for _, set := range sets {
		for _, pointer := range set.docIDs {
			count := found[pointer]
			count++
			found[pointer] = count
		}
	}

	var out docPointers
	var idsForChecksum []uint64
	for id, count := range found {
		if count != uint64(len(sets)) {
			continue
		}

		// TODO: optimize to use fixed length slice and cut off (should be
		// considerably cheaper on very long lists, such as we encounter during
		// large classification cases
		out.docIDs = append(out.docIDs, id)
		idsForChecksum = append(idsForChecksum, id)
	}

	checksum, err := docPointerChecksum(idsForChecksum)
	if err != nil {
		return nil, errors.Wrapf(err, "calculate checksum")
	}

	out.checksum = checksum
	return &out, nil
}

func mergeOr(children []*propValuePair, acceptDuplicates bool) (*docPointers, error) {
	sets := make([]*docPointers, len(children))

	// retrieve child IDs
	for i, child := range children {
		docIDs, err := child.mergeDocIDs(acceptDuplicates)
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

	if acceptDuplicates {
		return mergeOrAcceptDuplicates(sets)
	}

	// merge OR
	var checksums [][]byte
	found := map[uint64]uint64{} // map[id]count
	for _, set := range sets {
		for _, pointer := range set.docIDs {
			count := found[pointer]
			count++
			found[pointer] = count
		}
		checksums = append(checksums, set.checksum)
	}

	var out docPointers
	for id := range found {
		out.docIDs = append(out.docIDs, id)
	}

	out.checksum = combineChecksums(checksums, filters.OperatorOr)
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
