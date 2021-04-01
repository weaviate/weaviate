package lsmkv

import "bytes"

type binarySearchTree struct {
	root *binarySearchNode
}

func (t *binarySearchTree) insert(key, value []byte) {
	// before := time.Now()
	// defer m.addInserting(before)

	if t.root == nil {
		t.root = &binarySearchNode{
			key:   key,
			value: value,
		}
		return
	}

	t.root.insert(key, value)
}

func (t *binarySearchTree) get(key []byte) ([]byte, error) {
	// before := time.Now()
	// defer m.addContains(before)

	if t.root == nil {
		return nil, NotFound
	}

	return t.root.get(key)
}

func (t *binarySearchTree) setTombstone(key []byte) {
	if t.root == nil {
		// we need to actively insert a node with a tombstone, even if this node is
		// not present because we still need to propagate the delete into the disk
		// segments. It could refer to an entity which was created in a previous
		// segment and is thus unknown to this memtable
		t.root = &binarySearchNode{
			key:       key,
			value:     nil,
			tombstone: true,
		}
	}

	t.root.setTombstone(key)
}

// func (t *binarySearchTree) minimum() *binarySearchNode {
// 	// before := time.Now()
// 	// defer m.addMinMax(before)
// 	return t.root.minimum()
// }

// func (t *binarySearchTree) maximum() *binarySearchNode {
// 	// before := time.Now()
// 	// defer m.addMinMax(before)
// 	return t.root.maximum()
// }

func (t *binarySearchTree) flattenInOrder() []*binarySearchNode {
	// before := time.Now()
	// defer m.addFlattening(before)

	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

// func (t *binarySearchTree) delete(index uint64, dist float32) {
// 	// before := time.Now()
// 	// defer m.addDeleting(before)

// 	fakeParent := &binarySearchNode{right: t.root, index: math.MaxInt64, dist: -999999}

// 	t.root.delete(index, dist, fakeParent)
// 	t.root = fakeParent.right
// }

// func (t *binarySearchTree) len() int {
// 	if t.root == nil {
// 		return 0
// 	}

// 	return t.root.len()
// }

type binarySearchNode struct {
	key       []byte
	value     []byte
	left      *binarySearchNode
	right     *binarySearchNode
	tombstone bool
}

// func (b binarySearchNode) String() string {
// 	var left string
// 	var right string

// 	if b.left != nil {
// 		left = fmt.Sprintf("left child is %d", b.left.index)
// 	}

// 	if b.right != nil {
// 		right = fmt.Sprintf("right child is %d", b.right.index)
// 	}

// 	return fmt.Sprintf("%d (%f) %s %s \n", b.index, b.dist, left, right)
// }

func (n *binarySearchNode) insert(key, value []byte) {
	// if n == nil {
	// 	n = &binarySearchNode{
	// 		index: index,
	// 		dist:  dist,
	// 	}
	// }

	if bytes.Equal(key, n.key) {
		n.value = value

		// reset tombstone in case it had one
		n.tombstone = false
		return
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left != nil {
			n.left.insert(key, value)
			return
		} else {
			n.left = &binarySearchNode{
				key:   key,
				value: value,
			}
			return
		}
	} else {
		if n.right != nil {
			n.right.insert(key, value)
			return
		} else {
			n.right = &binarySearchNode{
				key:   key,
				value: value,
			}
			return
		}
	}
}

// func (n *binarysearchnode) contains(index uint64, dist float32) bool {
// 	if n.index == index {
// 		return true
// 	}

// 	if dist < n.dist {
// 		if n.left == nil {
// 			return false
// 		}

// 		return n.left.contains(index, dist)
// 	} else {
// 		if n.right == nil {
// 			return false
// 		}

// 		return n.right.contains(index, dist)
// 	}
// }

func (n *binarySearchNode) get(key []byte) ([]byte, error) {
	if bytes.Equal(n.key, key) {
		if !n.tombstone {
			return n.value, nil
		} else {
			return nil, Deleted
		}
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

func (n *binarySearchNode) setTombstone(key []byte) {
	if bytes.Equal(n.key, key) {
		n.value = nil
		n.tombstone = true
	}

	if bytes.Compare(key, n.key) < 0 {
		if n.left == nil {
			n.left = &binarySearchNode{
				key:       key,
				value:     nil,
				tombstone: true,
			}
			return
		}

		n.left.setTombstone(key)
		return
	} else {
		if n.right == nil {
			n.right = &binarySearchNode{
				key:       key,
				value:     nil,
				tombstone: true,
			}
			return
		}

		n.right.setTombstone(key)
		return
	}
}

// func (n *binarySearchNode) minimum() *binarySearchNode {
// 	if n.left == nil {
// 		return n
// 	}

// 	return n.left.minimum()
// }

// func (n *binarySearchNode) maximum() *binarySearchNode {
// 	if n.right == nil {
// 		return n
// 	}

// 	return n.right.maximum()
// }

// // maxAndParent() is a helper function for swapping while deleting
// func (n *binarySearchNode) maxAndParent(parent *binarySearchNode) (*binarySearchNode, *binarySearchNode) {
// 	if n == nil {
// 		return nil, parent
// 	}

// 	if n.right == nil {
// 		return n, parent
// 	}

// 	return n.right.maxAndParent(n)
// }

// // minAndParent() is a helper function for swapping while deleting
// func (n *binarySearchNode) minAndParent(parent *binarySearchNode) (*binarySearchNode, *binarySearchNode) {
// 	if n == nil {
// 		return nil, parent
// 	}

// 	if n.left == nil {
// 		return n, parent
// 	}

// 	return n.left.minAndParent(n)
// }

// func (n *binarySearchNode) replaceNode(parent, replacement *binarySearchNode) {
// 	if n == nil {
// 		panic("tried tor replace nil node")
// 	}

// 	if n == parent.left {
// 		// the current node is the parent's left, so we replace our parent's left
// 		// node, i.e. ourself
// 		parent.left = replacement
// 	} else if parent.right == n {
// 		// vice versa for right
// 		parent.right = replacement
// 	} else {
// 		panic("impossible replacement")
// 	}
// }

// // delete is inspired by the great explanation at https://appliedgo.net/bintree/
// func (n *binarySearchNode) delete(index uint64, dist float32, parent *binarySearchNode) {
// 	if n == nil {
// 		panic(fmt.Sprintf("trying to delete nil node %v of parent %v", index, parent))
// 	}

// 	if n.dist == dist && n.index == index {
// 		// this is the node to be deleted

// 		if n.left == nil && n.right == nil {
// 			// node has no children, so deletion is a simple as removing this node
// 			n.replaceNode(parent, nil)
// 			return
// 		}

// 		// if the node has just one child, simply swap with it's child
// 		if n.left == nil {
// 			n.replaceNode(parent, n.right)
// 			return
// 		}
// 		if n.right == nil {
// 			n.replaceNode(parent, n.left)
// 			return
// 		}

// 		// node has two children
// 		if parent.right != nil && parent.right.index == n.index {
// 			// this node is a right child, so we need to delete max from left
// 			replacement, replParent := n.left.maxAndParent(n)
// 			n.index = replacement.index
// 			n.dist = replacement.dist

// 			replacement.delete(replacement.index, replacement.dist, replParent)
// 			return
// 		}

// 		if parent.left != nil && parent.left.index == n.index {
// 			// this node is a left child, so we need to delete min from right
// 			replacement, replParent := n.right.minAndParent(n)
// 			n.index = replacement.index
// 			n.dist = replacement.dist

// 			replacement.delete(replacement.index, replacement.dist, replParent)
// 			return
// 		}

// 		panic("this should be unreachable")
// 	}

// 	if dist < n.dist {
// 		n.left.delete(index, dist, n)
// 		return
// 	} else {
// 		n.right.delete(index, dist, n)
// 	}
// }

func (n *binarySearchNode) flattenInOrder() []*binarySearchNode {
	var left []*binarySearchNode
	var right []*binarySearchNode

	if n.left != nil {
		left = n.left.flattenInOrder()
	}

	if n.right != nil {
		right = n.right.flattenInOrder()
	}

	right = append([]*binarySearchNode{n}, right...)
	return append(left, right...)
}

// func (n *binarySearchNode) len() int {
// 	if n == nil {
// 		return 0
// 	}

// 	return n.left.len() + 1 + n.right.len()
// }
