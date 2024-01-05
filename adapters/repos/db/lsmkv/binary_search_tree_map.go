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
	"sort"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/rbtree"
	"github.com/weaviate/weaviate/entities/lsmkv"
)

type binarySearchTreeMap struct {
	root *binarySearchNodeMap
}

func (t *binarySearchTreeMap) insert(key []byte, pair MapPair) {
	if t.root == nil {
		t.root = &binarySearchNodeMap{
			key:         key,
			values:      []MapPair{pair},
			colourIsRed: false, // root node is always black
		}
		return
	}

	if newRoot := t.root.insert(key, pair); newRoot != nil {
		t.root = newRoot
	}
	t.root.colourIsRed = false // Can be flipped in the process of balancing, but root is always black
}

func (t *binarySearchTreeMap) get(key []byte) ([]MapPair, error) {
	if t.root == nil {
		return nil, lsmkv.NotFound
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
	key         []byte
	values      []MapPair
	left        *binarySearchNodeMap
	right       *binarySearchNodeMap
	parent      *binarySearchNodeMap
	colourIsRed bool
}

func (n *binarySearchNodeMap) Parent() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.parent
}

func (n *binarySearchNodeMap) SetParent(parent rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if parent == nil {
		n.parent = nil
		return
	}

	n.parent = parent.(*binarySearchNodeMap)
}

func (n *binarySearchNodeMap) Left() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.left
}

func (n *binarySearchNodeMap) SetLeft(left rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if left == nil {
		n.left = nil
		return
	}

	n.left = left.(*binarySearchNodeMap)
}

func (n *binarySearchNodeMap) Right() rbtree.Node {
	if n == nil {
		return nil
	}
	return n.right
}

func (n *binarySearchNodeMap) SetRight(right rbtree.Node) {
	if n == nil {
		addNewSearchNodeMapReceiver(&n)
	}

	if right == nil {
		n.right = nil
		return
	}

	n.right = right.(*binarySearchNodeMap)
}

func (n *binarySearchNodeMap) IsRed() bool {
	if n == nil {
		return false
	}
	return n.colourIsRed
}

func (n *binarySearchNodeMap) SetRed(isRed bool) {
	n.colourIsRed = isRed
}

func (n *binarySearchNodeMap) IsNil() bool {
	return n == nil
}

func addNewSearchNodeMapReceiver(nodePtr **binarySearchNodeMap) {
	*nodePtr = &binarySearchNodeMap{}
}

func (n *binarySearchNodeMap) insert(key []byte, pair MapPair) *binarySearchNodeMap {
	if bytes.Equal(key, n.key) {
		n.values = append(n.values, pair)
		return nil // tree root does not change when replacing node
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			return n.left.insert(key, pair)
		} else {
			n.left = &binarySearchNodeMap{
				key:         key,
				parent:      n,
				colourIsRed: true,
				values:      []MapPair{pair},
			}
			return binarySearchNodeMapFromRB(rbtree.Rebalance(n.left))
		}
	} else {
		if n.right != nil {
			return n.right.insert(key, pair)
		} else {
			n.right = &binarySearchNodeMap{
				key:         key,
				parent:      n,
				colourIsRed: true,
				values:      []MapPair{pair},
			}
			return binarySearchNodeMapFromRB(rbtree.Rebalance(n.right))
		}
	}
}

func (n *binarySearchNodeMap) get(key []byte) ([]MapPair, error) {
	if bytes.Equal(n.key, key) {
		return sortAndDedupValues(n.values), nil
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left == nil {
			return nil, lsmkv.NotFound
		}

		return n.left.get(key)
	} else {
		if n.right == nil {
			return nil, lsmkv.NotFound
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
	right = append([]*binarySearchNodeMap{{
		key:         n.key,
		values:      sortAndDedupValues(n.values),
		colourIsRed: n.colourIsRed,
	}}, right...)
	return append(left, right...)
}

// takes a list of MapPair and sorts it while keeping the original order. Then
// removes redundancies (from updates or deletes after previous inserts) using
// a simple deduplication process.
func sortAndDedupValues(in []MapPair) []MapPair {
	out := make([]MapPair, len(in))
	copy(out, in)

	// use SliceStable so that we keep the insert order on duplicates. This is
	// important because otherwise we can't dedup them correctly if we don't know
	// in which order they came in.
	sort.SliceStable(out, func(a, b int) bool {
		return bytes.Compare(out[a].Key, out[b].Key) < 0
	})

	// now deduping is as simple as looking one key ahead - if it's the same key
	// simply skip the current element. Meaning "out" will be a subset of
	// (sorted) "in".
	outIndex := 0
	for inIndex, pair := range out {
		// look ahead
		if inIndex+1 < len(out) && bytes.Equal(out[inIndex+1].Key, pair.Key) {
			continue
		}

		out[outIndex] = pair
		outIndex++
	}

	return out[:outIndex]
}

func binarySearchNodeMapFromRB(rbNode rbtree.Node) (bsNode *binarySearchNodeMap) {
	if rbNode == nil {
		bsNode = nil
		return
	}
	bsNode = rbNode.(*binarySearchNodeMap)
	return
}
