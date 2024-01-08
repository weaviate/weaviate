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

package segmentindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/pkg/errors"
)

type Tree struct {
	nodes []*Node
}

type Node struct {
	Key   []byte
	Start uint64
	End   uint64
}

func NewTree(capacity int) Tree {
	return Tree{
		nodes: make([]*Node, 0, capacity),
	}
}

func NewBalanced(nodes []Node) Tree {
	t := Tree{nodes: make([]*Node, len(nodes))}

	if len(nodes) > 0 {
		// sort the slice just once
		sort.Slice(nodes, func(a, b int) bool {
			return bytes.Compare(nodes[a].Key, nodes[b].Key) < 0
		})

		t.buildBalanced(nodes, 0, 0, len(nodes)-1)
	}

	return t
}

func (t *Tree) buildBalanced(nodes []Node, targetPos, leftBound, rightBound int) {
	t.grow(targetPos)

	if leftBound > rightBound {
		return
	}

	mid := (leftBound + rightBound) / 2
	t.nodes[targetPos] = &nodes[mid]

	t.buildBalanced(nodes, t.left(targetPos), leftBound, mid-1)
	t.buildBalanced(nodes, t.right(targetPos), mid+1, rightBound)
}

func (t *Tree) Insert(key []byte, start, end uint64) {
	newNode := Node{
		Key:   key,
		Start: start,
		End:   end,
	}

	if len(t.nodes) == 0 {
		t.nodes = append(t.nodes, &newNode)
		return
	}

	t.insertAt(0, newNode)
}

func (t *Tree) insertAt(nodeID int, newNode Node) {
	if !t.exists(nodeID) {
		// we are at the target and can insert now
		t.grow(nodeID)
		t.nodes[nodeID] = &newNode
		return
	}

	if bytes.Equal(newNode.Key, t.nodes[nodeID].Key) {
		// this key already exists, which is an unexpected situation for an index
		// key
		panic(fmt.Sprintf("duplicate key %s", newNode.Key))
	}

	if bytes.Compare(newNode.Key, t.nodes[nodeID].Key) < 0 {
		t.insertAt(t.left(nodeID), newNode)
	} else {
		t.insertAt(t.right(nodeID), newNode)
	}
}

func (t *Tree) Get(key []byte) ([]byte, uint64, uint64) {
	if len(t.nodes) == 0 {
		return nil, 0, 0
	}

	return t.getAt(0, key)
}

func (t *Tree) getAt(nodeID int, key []byte) ([]byte, uint64, uint64) {
	if !t.exists(nodeID) {
		return nil, 0, 0
	}

	node := t.nodes[nodeID]
	if bytes.Equal(node.Key, key) {
		return node.Key, node.Start, node.End
	}

	if bytes.Compare(key, node.Key) < 0 {
		return t.getAt(t.left(nodeID), key)
	} else {
		return t.getAt(t.right(nodeID), key)
	}
}

func (t Tree) left(i int) int {
	return 2*i + 1
}

func (t Tree) right(i int) int {
	return 2*i + 2
}

func (t *Tree) exists(i int) bool {
	if i >= len(t.nodes) {
		return false
	}

	return t.nodes[i] != nil
}

// size calculates the exact size of this node on disk which is helpful to
// figure out the personal offset
func (n *Node) size() int {
	if n == nil {
		return 0
	}
	size := 0
	size += 4 // uint32 for key length
	size += len(n.Key)
	size += 8 // uint64 startPos
	size += 8 // uint64 endPos
	size += 8 // int64 pointer left child
	size += 8 // int64 pointer right child
	return size
}

func (t *Tree) grow(i int) {
	if i < len(t.nodes) {
		return
	}

	oldSize := len(t.nodes)
	newSize := oldSize
	for newSize <= i {
		newSize += oldSize
	}

	newNodes := make([]*Node, newSize)
	copy(newNodes, t.nodes)
	for i := range t.nodes {
		t.nodes[i] = nil
	}

	t.nodes = newNodes
}

func (t *Tree) MarshalBinary() ([]byte, error) {
	offsets, size := t.calculateDiskOffsets()

	buf := bytes.NewBuffer(nil)

	for i, node := range t.nodes {
		if node == nil {
			continue
		}

		var leftOffset int64
		var rightOffset int64

		if t.exists(t.left(i)) {
			leftOffset = int64(offsets[t.left(i)])
		} else {
			leftOffset = -1
		}

		if t.exists(t.right(i)) {
			rightOffset = int64(offsets[t.right(i)])
		} else {
			rightOffset = -1
		}

		if len(node.Key) > math.MaxUint32 {
			return nil, errors.Errorf("max key size is %d", math.MaxUint32)
		}

		keyLen := uint32(len(node.Key))
		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			return nil, err
		}
		if _, err := buf.Write(node.Key); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, node.Start); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, node.End); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, leftOffset); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, rightOffset); err != nil {
			return nil, err
		}
	}
	bytes := buf.Bytes()
	if size != len(bytes) {
		return nil, errors.Errorf("corrupt: wrote %d bytes with target %d", len(bytes), size)
	}

	return bytes, nil
}

func (t *Tree) MarshalBinaryInto(w io.Writer) (int64, error) {
	offsets, size := t.calculateDiskOffsets()

	// create buf just once and reuse for each iteration, each iteration
	// overwrites every single byte of the buffer, so no initializing or
	// resetting after a round is required.
	buf := make([]byte, 36) // 1x uint32 + 4x uint64

	for i, node := range t.nodes {
		if node == nil {
			continue
		}

		var leftOffset int64
		var rightOffset int64

		if t.exists(t.left(i)) {
			leftOffset = int64(offsets[t.left(i)])
		} else {
			leftOffset = -1
		}

		if t.exists(t.right(i)) {
			rightOffset = int64(offsets[t.right(i)])
		} else {
			rightOffset = -1
		}

		if len(node.Key) > math.MaxUint32 {
			return 0, errors.Errorf("max key size is %d", math.MaxUint32)
		}

		keyLen := uint32(len(node.Key))
		binary.LittleEndian.PutUint32(buf[0:4], keyLen)
		binary.LittleEndian.PutUint64(buf[4:12], node.Start)
		binary.LittleEndian.PutUint64(buf[12:20], node.End)
		binary.LittleEndian.PutUint64(buf[20:28], uint64(leftOffset))
		binary.LittleEndian.PutUint64(buf[28:36], uint64(rightOffset))

		if _, err := w.Write(buf[:4]); err != nil {
			return 0, err
		}
		if _, err := w.Write(node.Key); err != nil {
			return 0, err
		}
		if _, err := w.Write(buf[4:36]); err != nil {
			return 0, err
		}
	}

	return int64(size), nil
}

// returns individual offsets and total size, nil nodes are skipped
func (t *Tree) calculateDiskOffsets() ([]int, int) {
	current := 0
	out := make([]int, len(t.nodes))
	for i, node := range t.nodes {
		out[i] = current
		size := node.size()
		current += size
	}

	return out, current
}

func (t *Tree) Height() int {
	var highestElem int
	for i := len(t.nodes) - 1; i >= 0; i-- {
		if t.nodes[i] != nil {
			highestElem = i
			break
		}
	}

	return int(math.Ceil(math.Log2(float64(highestElem))))
}
