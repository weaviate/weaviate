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

package lsmkv

import (
	"bytes"
	"sort"
)

type binarySearchTreeMap struct {
	root *binarySearchNodeMap
}

func (t *binarySearchTreeMap) insert(key []byte, pair MapPair) {
	if t.root == nil {
		t.root = &binarySearchNodeMap{
			key:    key,
			values: []MapPair{pair},
		}
		return
	}

	t.root.insert(key, pair)
}

func (t *binarySearchTreeMap) get(key []byte) ([]MapPair, error) {
	if t.root == nil {
		return nil, NotFound
	}

	return t.root.get(key)
}

func (t *binarySearchTreeMap) flattenInOrder() []*binarySearchNodeMap {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

type binarySearchNodeMap struct {
	key    []byte
	values []MapPair
	left   *binarySearchNodeMap
	right  *binarySearchNodeMap
}

func (n *binarySearchNodeMap) insert(key []byte, pair MapPair) {
	if bytes.Equal(key, n.key) {
		n.values = append(n.values, pair)
		return
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			n.left.insert(key, pair)
			return
		} else {
			n.left = &binarySearchNodeMap{
				key:    key,
				values: []MapPair{pair},
			}
			return
		}
	} else {
		if n.right != nil {
			n.right.insert(key, pair)
			return
		} else {
			n.right = &binarySearchNodeMap{
				key:    key,
				values: []MapPair{pair},
			}
			return
		}
	}
}

func (n *binarySearchNodeMap) get(key []byte) ([]MapPair, error) {
	if bytes.Equal(n.key, key) {
		return sortAndDedupValues(n.values), nil
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left == nil {
			return nil, NotFound
		}

		return n.left.get(key)
	} else {
		if n.right == nil {
			return nil, NotFound
		}

		return n.right.get(key)
	}
}

func (n *binarySearchNodeMap) flattenInOrder() []*binarySearchNodeMap {
	var left []*binarySearchNodeMap
	var right []*binarySearchNodeMap

	if n.left != nil {
		left = n.left.flattenInOrder()
	}

	if n.right != nil {
		right = n.right.flattenInOrder()
	}

	// the values are sorted on read for performance reasons, the assumption is
	// that while a memtable is open writes a much more common, thus we write map
	// KVs unsorted and only sort/dedup them on read.
	n.values = sortAndDedupValues(n.values)

	right = append([]*binarySearchNodeMap{n}, right...)
	return append(left, right...)
}

// takes a list of MapPair and sorts it while keeping the original order. Then
// removes redundancies (from updates or deletes after previous inserts) using
// a simple deduplication process.
func sortAndDedupValues(in []MapPair) []MapPair {
	// use SliceStable so that we keep the insert order on duplicates. This is
	// important because otherwise we can't dedup them correctly if we don't know
	// in which order they came in.
	sort.SliceStable(in, func(a, b int) bool {
		return bytes.Compare(in[a].Key, in[b].Key) < 0
	})

	// now deduping is as simple as looking one key ahead - if it's the same key
	// simply skip the current element. Meaning "out" will be a subset of
	// (sorted) "in".
	out := make([]MapPair, len(in))

	outIndex := 0
	for inIndex, pair := range in {
		// look ahead
		if inIndex+1 < len(in) && bytes.Equal(in[inIndex+1].Key, pair.Key) {
			continue
		}

		out[outIndex] = pair
		outIndex++
	}

	return out[:outIndex]
}
