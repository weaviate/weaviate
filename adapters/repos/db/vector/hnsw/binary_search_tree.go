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

package hnsw

import (
	"fmt"
)

type binarySearchTreeGeneric struct {
	root *binarySearchNodeGeneric
}

func (t *binarySearchTreeGeneric) insert(index int, dist float32) {
	// before := time.Now()
	// defer m.addInserting(before)

	if t.root == nil {
		t.root = &binarySearchNodeGeneric{
			index: index,
			dist:  dist,
		}
		return
	}

	t.root.insert(index, dist)
}

func (t *binarySearchTreeGeneric) printInOrder() {
	t.root.printInOrder()
}

func (t *binarySearchTreeGeneric) contains(index int, dist float32) bool {
	// before := time.Now()
	// defer m.addContains(before)

	if t.root == nil {
		return false
	}

	return t.root.contains(index, dist)
}

func (t *binarySearchTreeGeneric) minimum() *binarySearchNodeGeneric {
	// before := time.Now()
	// defer m.addMinMax(before)
	return t.root.minimum()
}

func (t *binarySearchTreeGeneric) maximum() *binarySearchNodeGeneric {
	// before := time.Now()
	// defer m.addMinMax(before)
	return t.root.maximum()
}

func (t *binarySearchTreeGeneric) flattenInOrder() []*binarySearchNodeGeneric {
	// before := time.Now()
	// defer m.addFlattening(before)

	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

func (t *binarySearchTreeGeneric) delete(index int, dist float32) {
	// before := time.Now()
	// defer m.addDeleting(before)

	fakeParent := &binarySearchNodeGeneric{right: t.root, index: -1, dist: -999999}

	t.root.delete(index, dist, fakeParent)
	t.root = fakeParent.right
}

func (t *binarySearchTreeGeneric) len() int {
	if t.root == nil {
		return 0
	}

	return t.root.len()
}

type binarySearchNodeGeneric struct {
	index int
	dist  float32
	left  *binarySearchNodeGeneric
	right *binarySearchNodeGeneric
}

func (b binarySearchNodeGeneric) String() string {
	var left string
	var right string

	if b.left != nil {
		left = fmt.Sprintf("left child is %d", b.left.index)
	}

	if b.right != nil {
		right = fmt.Sprintf("right child is %d", b.right.index)
	}

	return fmt.Sprintf("%d (%f) %s %s \n", b.index, b.dist, left, right)
}

func (n *binarySearchNodeGeneric) insert(index int, dist float32) {
	// if n == nil {
	// 	n = &binarySearchNodeGeneric{
	// 		index: index,
	// 		dist:  dist,
	// 	}
	// }

	if index == n.index {
		// exact node is already present, ignore
		return
	}

	if dist < n.dist {
		if n.left != nil {
			n.left.insert(index, dist)
			return
		} else {
			n.left = &binarySearchNodeGeneric{
				index: index,
				dist:  dist,
			}
			return
		}
	} else {
		if n.right != nil {
			n.right.insert(index, dist)
			return
		} else {
			n.right = &binarySearchNodeGeneric{
				index: index,
				dist:  dist,
			}
			return
		}
	}
}

func (n *binarySearchNodeGeneric) printInOrder() {
	if n == nil {
		return
	}

	if n.left != nil {
		n.left.printInOrder()
	}

	if n.right != nil {
		n.right.printInOrder()
	}
}

func (n *binarySearchNodeGeneric) contains(index int, dist float32) bool {
	if n.index == index {
		return true
	}

	if dist < n.dist {
		if n.left == nil {
			return false
		}

		return n.left.contains(index, dist)
	} else {
		if n.right == nil {
			return false
		}

		return n.right.contains(index, dist)
	}
}

func (n *binarySearchNodeGeneric) minimum() *binarySearchNodeGeneric {
	if n.left == nil {
		return n
	}

	return n.left.minimum()
}

func (n *binarySearchNodeGeneric) maximum() *binarySearchNodeGeneric {
	if n.right == nil {
		return n
	}

	return n.right.maximum()
}

// maxAndParent() is a helper function for swapping while deleting
func (n *binarySearchNodeGeneric) maxAndParent(parent *binarySearchNodeGeneric) (*binarySearchNodeGeneric, *binarySearchNodeGeneric) {
	if n == nil {
		return nil, parent
	}

	if n.right == nil {
		return n, parent
	}

	return n.right.maxAndParent(n)
}

// minAndParent() is a helper function for swapping while deleting
func (n *binarySearchNodeGeneric) minAndParent(parent *binarySearchNodeGeneric) (*binarySearchNodeGeneric, *binarySearchNodeGeneric) {
	if n == nil {
		return nil, parent
	}

	if n.left == nil {
		return n, parent
	}

	return n.left.minAndParent(n)
}

func (n *binarySearchNodeGeneric) replaceNode(parent, replacement *binarySearchNodeGeneric) {
	if n == nil {
		panic("tried tor replace nil node")
	}

	if n == parent.left {
		// the current node is the parent's left, so we replace our parent's left
		// node, i.e. ourself
		parent.left = replacement
	} else if parent.right == n {
		// vice versa for right
		parent.right = replacement
	} else {
		panic("impossible replacement")
	}
}

// delete is inspired by the great explanation at https://appliedgo.net/bintree/
func (n *binarySearchNodeGeneric) delete(index int, dist float32, parent *binarySearchNodeGeneric) {
	if n == nil {
		panic(fmt.Sprintf("trying to delete nil node %v of parent %v", index, parent))
	}

	if n.dist == dist && n.index == index {
		// this is the node to be deleted

		if n.left == nil && n.right == nil {
			// node has no children, so deletion is a simple as removing this node
			n.replaceNode(parent, nil)
			return
		}

		// if the node has just one child, simply swap with it's child
		if n.left == nil {
			n.replaceNode(parent, n.right)
			return
		}
		if n.right == nil {
			n.replaceNode(parent, n.left)
			return
		}

		// node has two children
		if parent.right != nil && parent.right.index == n.index {
			// this node is a right child, so we need to delete max from left
			replacement, replParent := n.left.maxAndParent(n)
			n.index = replacement.index
			n.dist = replacement.dist

			replacement.delete(replacement.index, replacement.dist, replParent)
			return
		}

		if parent.left != nil && parent.left.index == n.index {
			// this node is a left child, so we need to delete min from right
			replacement, replParent := n.right.minAndParent(n)
			n.index = replacement.index
			n.dist = replacement.dist

			replacement.delete(replacement.index, replacement.dist, replParent)
			return
		}

		panic("this should be unreachable")
	}

	if dist < n.dist {
		n.left.delete(index, dist, n)
		return
	} else {
		n.right.delete(index, dist, n)
	}
}

func (n *binarySearchNodeGeneric) flattenInOrder() []*binarySearchNodeGeneric {
	var left []*binarySearchNodeGeneric
	var right []*binarySearchNodeGeneric

	if n.left != nil {
		left = n.left.flattenInOrder()
	}

	if n.right != nil {
		right = n.right.flattenInOrder()
	}

	right = append([]*binarySearchNodeGeneric{n}, right...)
	return append(left, right...)
}

func (n *binarySearchNodeGeneric) len() int {
	if n == nil {
		return 0
	}

	return n.left.len() + 1 + n.right.len()
}
