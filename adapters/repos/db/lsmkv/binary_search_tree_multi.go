//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package lsmkv

import "bytes"

type binarySearchTreeMulti struct {
	root *binarySearchNodeMulti
}

type value struct {
	value     []byte
	tombstone bool
}

func (t *binarySearchTreeMulti) insert(key []byte, values []value) {
	if t.root == nil {
		t.root = &binarySearchNodeMulti{
			key:    key,
			values: values,
		}
		return
	}

	t.root.insert(key, values)
}

func (t *binarySearchTreeMulti) get(key []byte) ([]value, error) {
	if t.root == nil {
		return nil, NotFound
	}

	return t.root.get(key)
}

// // set Tombstone for the entire entry, i.e. all values for this key
// func (t *binarySearchTreeMulti) setTombstone(key []byte) {
// 	if t.root == nil {
// 		// we need to actively insert a node with a tombstone, even if this node is
// 		// not present because we still need to propagate the delete into the disk
// 		// segments. It could refer to an entity which was created in a previous
// 		// segment and is thus unknown to this memtable
// 		t.root = &binarySearchNodeMulti{
// 			key:       key,
// 			value:     nil,
// 			tombstone: true,
// 		}
// 	}

// 	t.root.setTombstone(key)
// }

func (t *binarySearchTreeMulti) flattenInOrder() []*binarySearchNodeMulti {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

type binarySearchNodeMulti struct {
	key    []byte
	values []value
	left   *binarySearchNodeMulti
	right  *binarySearchNodeMulti
}

func (n *binarySearchNodeMulti) insert(key []byte, values []value) {
	if bytes.Equal(key, n.key) {
		n.values = append(n.values, values...)
		return
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			n.left.insert(key, values)
			return
		} else {
			n.left = &binarySearchNodeMulti{
				key:    key,
				values: values,
			}
			return
		}
	} else {
		if n.right != nil {
			n.right.insert(key, values)
			return
		} else {
			n.right = &binarySearchNodeMulti{
				key:    key,
				values: values,
			}
			return
		}
	}
}

func (n *binarySearchNodeMulti) get(key []byte) ([]value, error) {
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

// func (n *binarySearchNodeMulti) setTombstone(key []byte) {
// 	if bytes.Equal(n.key, key) {
// 		n.value = nil
// 		n.tombstone = true
// 	}

// 	if bytes.Compare(key, n.key) < 0 {
// 		if n.left == nil {
// 			n.left = &binarySearchNodeMulti{
// 				key:       key,
// 				value:     nil,
// 				tombstone: true,
// 			}
// 			return
// 		}

// 		n.left.setTombstone(key)
// 		return
// 	} else {
// 		if n.right == nil {
// 			n.right = &binarySearchNodeMulti{
// 				key:       key,
// 				value:     nil,
// 				tombstone: true,
// 			}
// 			return
// 		}

// 		n.right.setTombstone(key)
// 		return
// 	}
// }

func (n *binarySearchNodeMulti) flattenInOrder() []*binarySearchNodeMulti {
	var left []*binarySearchNodeMulti
	var right []*binarySearchNodeMulti

	if n.left != nil {
		left = n.left.flattenInOrder()
	}

	if n.right != nil {
		right = n.right.flattenInOrder()
	}

	right = append([]*binarySearchNodeMulti{n}, right...)
	return append(left, right...)
}
