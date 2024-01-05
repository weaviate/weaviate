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

package rbtree

type Node interface {
	Parent() Node
	SetParent(Node)
	Left() Node
	SetLeft(Node)
	Right() Node
	SetRight(Node)
	IsRed() bool
	SetRed(bool)
	IsNil() bool
}

// This function rebalances and recolours trees to be valid RB trees. It needs to be called after each node that
// was added to the tree.
//
// Deletions are currently not supported as this is done through the tombstone flag and from the POV of the RB-tree
// tombstone-nodes are just normal nodes that get rebalanced the normal way.
//
// Throughout this file the following relationships between nodes are used:
// GP = grandparent, P = parent, U = uncle, S = sibling, N = node that was just added
//
//	   GP
//	 /   \
//	U     P
//	     / \
//	    S   N
func Rebalance(node Node) Node {
	for {
		parent := node.Parent()

		// if parent is black or the current node is the root node (== parent is nil) there is nothing to do
		if !parent.IsRed() {
			return nil
		}

		grandparent := node.Parent().Parent()
		var uncle Node
		if parent == grandparent.Right() {
			uncle = grandparent.Left()
		} else {
			uncle = grandparent.Right()
		}

		if uncle.IsRed() {
			// if uncle is red, recoloring the tree up to the grandparent results in a valid RBtree.
			// The color of the grandfather changes to red, so there might be more fixes needed. Therefore
			// go up the tree and repeat.
			recolourNodes(parent, grandparent, uncle)
			node = grandparent
		} else {
			// if uncle is black, there are four possible cases:
			//   parent is the right child grandparent:
			//    1) node is right child of parent => left rotate around GP
			//    2) node is left child of parent => right rotate around parent results in case 1
			//   For cases 3 and 4 just replace left and right in the two cases above
			//
			// In all of these cases the grandfather stays black and there is no need for further fixes up the tree
			var newRoot Node
			if parent == grandparent.Right() {
				if node == parent.Left() {
					rightRotate(parent)
					// node and parent switch places in the tree, update parent to recolour the current node
					parent = node
				}
				newRoot = leftRotate(grandparent)
			} else { // parent == grandparent.left
				if node == parent.Right() {
					leftRotate(parent)
					parent = node
				}
				newRoot = rightRotate(grandparent)
			}
			recolourNodes(grandparent, parent)
			return newRoot
		}
	}
}

func recolourNodes(nodes ...Node) {
	for _, n := range nodes {
		if !n.IsNil() {
			if n.IsRed() {
				n.SetRed(false)
			} else {
				n.SetRed(true)
			}
		}
	}
}

// Rotate the tree left around the given node.
//
// After this rotation, the former right child (FC) will be the new parent and the former parent (FP) will
// be the left node of the new parent. The left child of the former child is transferred to the former parent.
//
//	    FP                                FC
//	 /      \         left rotate        /  \
//	FP_R     FC          =>            FP   FC_R
//	        / \                       / \
//	     FC_L   FC_R               FP_R  FC_L
//
// In case FP was the root of the tree, FC will be the new root of the tree.
func leftRotate(rotationNode Node) Node {
	formerChild := rotationNode.Right()
	rootRotate := rotationNode.Parent().IsNil()

	// former child node becomes new parent unless the rotation is around the root node
	if rootRotate {
		formerChild.SetParent(nil)
	} else {
		if rotationNode.Parent().Left() == rotationNode {
			rotationNode.Parent().SetLeft(formerChild)
		} else {
			rotationNode.Parent().SetRight(formerChild)
		}
		formerChild.SetParent(rotationNode.Parent())
	}

	rotationNode.SetParent(formerChild)

	// Switch left child from former_child to rotation node
	rotationNode.SetRight(formerChild.Left())
	if formerChild.Left() != nil {
		formerChild.Left().SetParent(rotationNode)
	}
	formerChild.SetLeft(rotationNode)

	if rootRotate {
		return formerChild
	} else {
		return nil
	}
}

// Same as leftRotate, just switch left and right everywhere
func rightRotate(rotationNode Node) Node {
	formerChild := rotationNode.Left()
	rootRotate := rotationNode.Parent().IsNil()

	if rootRotate {
		formerChild.SetParent(nil)
	} else {
		if rotationNode.Parent().Left() == rotationNode {
			rotationNode.Parent().SetLeft(formerChild)
		} else {
			rotationNode.Parent().SetRight(formerChild)
		}
		formerChild.SetParent(rotationNode.Parent())
	}
	rotationNode.SetParent(formerChild)

	rotationNode.SetLeft(formerChild.Right())
	if formerChild.Right() != nil {
		formerChild.Right().SetParent(rotationNode)
	}
	formerChild.SetRight(rotationNode)

	if rootRotate {
		return formerChild
	} else {
		return nil
	}
}
