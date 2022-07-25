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

// This is a copy of adapters/repos/db/lsmkv/red_black_tree.go for maps

// This function rebalances and recolours trees to be valid RB trees. It needs to be called after each node that
// was added to the tree.
//
// Deletions are currently not supported as this is done through the tombstone flag and from the POV of the RB-tree
// tombstone-nodes are just normal nodes that get rebalanced the normal way.
//
// Throughout ths file the following relationships between nodes are used:
// GP = grandparent, P = parent, U = uncle, S = sibling, N = node that was just added
//
//     GP
//   /   \
//  U     P
//       / \
//      S   N
func rebalanceRedBlackTreeMulti(node *binarySearchNodeMulti) *binarySearchNodeMulti {
	for {
		parent := node.parent

		// if parent is black or the current node is the root node (== parent is nil) there is nothing to do
		if !isRedNodeMulti(parent) {
			return nil
		}

		grandparent := node.parent.parent
		var uncle *binarySearchNodeMulti
		if parent == grandparent.right {
			uncle = grandparent.left
		} else {
			uncle = grandparent.right
		}

		if isRedNodeMulti(uncle) {
			// if uncle is red, recoloring the tree up to the grandparent results in a valid RBtree.
			// The color of the grandfather changes to red, so there might be more fixes needed. Therefore
			// go up the tree and repeat.
			recolourNodesMulti(parent, grandparent, uncle)
			node = grandparent
		} else {
			// if uncle is black, there are four possible cases:
			//   parent is the right child grandparent:
			//    1) node is right child of parent => left rotate around GP
			//    2) node is left child of parent => right rotate around parent results in case 1
			//   For cases 3 and 4 just replace left and right in the two cases above
			//
			// In all of these cases the grandfather stays black and there is no need for further fixes up the tree
			var new_root *binarySearchNodeMulti
			if parent == grandparent.right {
				if node == parent.left {
					rightRotateMulti(parent)
					// node and parent switch places in the tree, update parent to recolour the current node
					parent = node
				}
				new_root = leftRotateMulti(grandparent)
			} else { // parent == grandparent.left
				if node == parent.right {
					leftRotateMulti(parent)
					parent = node
				}
				new_root = rightRotateMulti(grandparent)
			}
			recolourNodesMulti(grandparent, parent)
			return new_root
		}
	}
}

func recolourNodesMulti(nodes ...*binarySearchNodeMulti) {
	for _, n := range nodes {
		if n != nil {
			n.colourIsred = !n.colourIsred
		}
	}
}

func isRedNodeMulti(n *binarySearchNodeMulti) bool {
	return n != nil && n.colourIsred
}

// Rotate the tree left around the given node.
//
// After this rotation, the former right child (FC) will be the new parent and the former parent (FP) will
// be the left node of the new parent. The left child of the former child is transferred to the former parent.
//
//      FP                                FC
//   /      \         left rotate        /  \
//  FP_R     FC          =>            FP   FC_R
//          / \                       / \
//       FC_L   FC_R               FP_R  FC_L
//
// In case FP was the root of the tree, FC will be the new root of the tree.
func leftRotateMulti(rotation_node *binarySearchNodeMulti) *binarySearchNodeMulti {
	former_child := rotation_node.right
	rootRotate := rotation_node.parent == nil

	// former child node becomes new parent unless the rotation is around the root node
	if rootRotate {
		former_child.parent = nil
	} else {
		if rotation_node.parent.left == rotation_node {
			rotation_node.parent.left = former_child
		} else {
			rotation_node.parent.right = former_child
		}
		former_child.parent = rotation_node.parent
	}

	rotation_node.parent = former_child

	// Switch left child from former_child to rotation node
	rotation_node.right = former_child.left
	if former_child.left != nil {
		former_child.left.parent = rotation_node
	}
	former_child.left = rotation_node

	if rootRotate {
		return former_child
	} else {
		return nil
	}
}

// Same as leftRotate, just switch left and right everywhere
func rightRotateMulti(rotation_node *binarySearchNodeMulti) *binarySearchNodeMulti {
	former_child := rotation_node.left
	rootRotate := rotation_node.parent == nil

	if rootRotate {
		former_child.parent = nil
	} else {
		if rotation_node.parent.left == rotation_node {
			rotation_node.parent.left = former_child
		} else {
			rotation_node.parent.right = former_child
		}
		former_child.parent = rotation_node.parent
	}
	rotation_node.parent = former_child

	rotation_node.left = former_child.right
	if former_child.right != nil {
		former_child.right.parent = rotation_node
	}
	former_child.right = rotation_node

	if rootRotate {
		return former_child
	} else {
		return nil
	}
}
