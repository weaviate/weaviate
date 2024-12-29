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

package lsmkv

import (
	"bytes"
	"math"
	"sort"
)

// QuantileKeys returns an approximation of the keys that make up the specified
// quantiles. This can be used to start parallel cursors at fairly evenly
// distributed positions in the segment.
//
// To understand the approximation, checkout
// [lsmkv.segmentindex.DiskTree.QuantileKeys] that runs on each segment.
//
// Some things to keep in mind:
//
//  1. It may return fewer keys than requested (including 0) if the segment
//     contains fewer entries
//  2. It may return keys that do not exist, for example because they are
//     tombstoned. This is acceptable, as a key does not have to exist to be used
//     as part of .Seek() in a cursor.
//  3. It will never return duplicates, to make sure all parallel cursors
//     return unique values.
func (b *Bucket) QuantileKeys(q int) [][]byte {
	if q <= 0 {
		return nil
	}

	b.flushLock.RLock()
	defer b.flushLock.RUnlock()

	keys := b.disk.quantileKeys(q)
	return keys
}

func (sg *SegmentGroup) quantileKeys(q int) [][]byte {
	sg.maintenanceLock.RLock()
	defer sg.maintenanceLock.RUnlock()

	var keys [][]byte

	if len(sg.segments) == 0 {
		return keys
	}

	for _, s := range sg.segments {
		keys = append(keys, s.quantileKeys(q)...)
	}

	// re-sort keys
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// There could be duplicates if a key was modified in multiple segments, we
	// need to remove them. Since the list is sorted at this, this is fairly easy
	// to do:
	uniqueKeys := make([][]byte, 0, len(keys))
	for i := range keys {
		if i == 0 || !bytes.Equal(keys[i], keys[i-1]) {
			uniqueKeys = append(uniqueKeys, keys[i])
		}
	}

	return pickEvenlyDistributedKeys(uniqueKeys, q)
}

func (s *segment) quantileKeys(q int) [][]byte {
	return s.index.QuantileKeys(q)
}

// pickEvenlyDistributedKeys picks q keys from the input keys, trying to keep
// the distribution as even as possible. The input keys are assumed to be
// sorted. It never returns duplicates, see the unit test proving this.
//
// Important to keep in mind is that our input values do not contain the first
// and last elements, but rather the first quantile points.
// This is because they were obtained using
// [lsmkv.segmentindex.DiskTree.QuantileKeys] which traverses the binary tree
// to a certain depth. The first element in the list is the element you get
// from continuously following the left child until you hit the maximum
// traversal depth. Respectively, the last element is the element you get from
// continuously following the right child until you hit the maximum traversal
// depth.
// This means that when a cursor uses those keys, it will need to add two
// special cases:
//
//  1. It needs to start with the actual first element and read to the first
//     checkpoint
//  2. When reaching the last checkpoint, it needs to keep reading
//     until the cursor no longer returns elements.
//
// As a result our goal here is to keep the gaps as even as possible. For
// example, assume the keys ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
// and we want to pick 3 keys. We would return ["C", "F", "I"], thus keeping
// the spacing fairly even.
func pickEvenlyDistributedKeys(uniqueKeys [][]byte, q int) [][]byte {
	if q >= len(uniqueKeys) {
		// impossible to pick, simply return the input
		return uniqueKeys
	}

	// we now have the guarantee that q > len(uniqueKeys), which means it is
	// possible to pick q keys without overlap while keeping the distribution as
	// even as possible
	finalKeys := make([][]byte, q)
	stepSize := float64(len(uniqueKeys)) / float64(q)
	for i := range finalKeys {
		pos := int(math.Round(float64(i)*stepSize + 0.5*stepSize))

		finalKeys[i] = uniqueKeys[pos]
	}

	return finalKeys
}
