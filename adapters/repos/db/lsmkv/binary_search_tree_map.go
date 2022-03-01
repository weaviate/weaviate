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

import "bytes"

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
		n.values = insertSorted(n.values, pair)
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
		return n.values, nil
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

	right = append([]*binarySearchNodeMap{n}, right...)
	return append(left, right...)
}

// insertSorted will insert at the right position by key sorting. If the exact
// key exists, it will replace the elem. It only uses a linear search, as the
// assumption is that not too many keys will be held in the memtable per elem
func insertSorted(list []MapPair, newElem MapPair) []MapPair {
	bestPos := 0
	for i := range list {
		if bestPos == len(list) {
			return append(list, newElem)
		}

		cmp := bytes.Compare(newElem.Key, list[i].Key)

		if cmp == 0 {
			// entry exists already, replace
			list[i] = newElem
			return list
		}

		if cmp == -1 {
			break
		}

		bestPos++
	}

	if len(list) == bestPos {
		return append(list, newElem)
	}

	list = append(list[:bestPos+1], list[bestPos:]...)
	list[bestPos] = newElem

	return list
}
