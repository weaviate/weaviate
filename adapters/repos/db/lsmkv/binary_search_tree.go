package lsmkv

import "bytes"

type binarySearchTree struct {
	root *binarySearchNode
}

func (t *binarySearchTree) insert(key, value []byte) {
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
		return
	}

	t.root.setTombstone(key)
}

func (t *binarySearchTree) flattenInOrder() []*binarySearchNode {
	if t.root == nil {
		return nil
	}

	return t.root.flattenInOrder()
}

type binarySearchNode struct {
	key       []byte
	value     []byte
	left      *binarySearchNode
	right     *binarySearchNode
	tombstone bool
}

func (n *binarySearchNode) insert(key, value []byte) {
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
