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
	"syscall"

	"github.com/weaviate/weaviate/usecases/byteops"

	"github.com/pkg/errors"
)

var pageSize = uint64(syscall.Getpagesize())

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
	treeWriter := NewTreeWriter(w, t.nodes)
	return treeWriter.Marshall()
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

func floatDivideAndRoundUp(a, b uint64) uint64 {
	return uint64(math.Ceil(float64(a) / float64(b)))
}

// TreeWriter writes the nodes into a binary file
// The nodes are ordered in a way that as many reads as possible are staying within a single page, minimizing how many
// reads from disk have to happen to find a given node. This is achieved by keeping children nodes withing a page and
// starting a new page when the next child would exceed the page size.
type TreeWriter struct {
	w                  io.Writer
	tmpBuffer          byteops.ReadWriter
	nodes              []*Node
	numNodes           uint64
	pageNodesReusable  []uint64
	pageNodesReusable2 []uint64
	pageNodesReusable3 []uint64
}

func NewTreeWriter(w io.Writer, nodes []*Node) TreeWriter {
	maxNodeSize := 0
	for i := range nodes {
		if nodes[i] != nil && nodes[i].size() > maxNodeSize {
			maxNodeSize = nodes[i].size()
		}
	}
	numNodes := uint64(len(nodes))
	minNodeSize := uint64(37)
	maxNodesPerPage := floatDivideAndRoundUp(pageSize, minNodeSize)
	return TreeWriter{
		w:                  w,
		tmpBuffer:          byteops.NewReadWriter(make([]byte, maxNodeSize)),
		nodes:              nodes,
		numNodes:           numNodes,
		pageNodesReusable:  make([]uint64, maxNodesPerPage),
		pageNodesReusable2: make([]uint64, maxNodesPerPage),
		pageNodesReusable3: make([]uint64, maxNodesPerPage),
	}
}

func (t *TreeWriter) Marshall() (int64, error) {
	offsets := make([]uint64, t.numNodes)
	firstNodeInPages := []uint64{0}
	var err error
	offset := uint64(0)
	nodesWritten := uint64(0)
	for {
		if len(firstNodeInPages) == 0 {
			break
		}
		node := firstNodeInPages[0]
		firstNodeInPages = firstNodeInPages[1:]
		offset, nodesWritten, err = t.writePage(node, offset, nodesWritten, &firstNodeInPages, offsets, false)
		if err != nil {
			return 0, err
		}
	}

	firstNodeInPages = []uint64{0}
	offset = uint64(0)
	nodesWritten = uint64(0)
	for {
		if len(firstNodeInPages) == 0 {
			break
		}
		node := firstNodeInPages[0]
		firstNodeInPages = firstNodeInPages[1:]
		offset, nodesWritten, err = t.writePage(node, offset, nodesWritten, &firstNodeInPages, offsets, true)
		if err != nil {
			return 0, err
		}
	}
	return int64(offset), nil
}

func (t *TreeWriter) nodesInPage(start uint64, ids []uint64) uint64 {
	ids[0] = start
	if len(t.pageNodesReusable3) == 0 {
		t.pageNodesReusable3 = append(t.pageNodesReusable3, start)
	} else {
		t.pageNodesReusable3[0] = start
		t.pageNodesReusable3 = t.pageNodesReusable3[:1]
	}
	ids = ids[:cap(ids)]

	count := uint64(1)
	offset := uint64(0)
	for {
		if len(t.pageNodesReusable3) == 0 {
			break
		}
		parentNode := t.pageNodesReusable3[0]
		t.pageNodesReusable3 = t.pageNodesReusable3[1:]
		rightChild := parentNode*2 + 1
		if rightChild >= t.numNodes {
			break
		}
		if t.nodes[rightChild] != nil {
			offset += uint64(t.nodes[rightChild].size())
			if offset > pageSize {
				break
			}
			ids[count] = rightChild
			t.pageNodesReusable3 = append(t.pageNodesReusable3, rightChild)
			count++
			if int(count) == len(ids) {
				break
			}
		}
		leftChild := parentNode*2 + 2
		if leftChild >= t.numNodes {
			break
		}
		if t.nodes[leftChild] != nil {
			offset += uint64(t.nodes[leftChild].size())
			if offset > pageSize {
				break
			}
			ids[count] = leftChild
			t.pageNodesReusable3 = append(t.pageNodesReusable3, leftChild)
			count++
			if int(count) == len(ids) {
				break
			}
		}
	}
	return count
}

func (t *TreeWriter) writePage(start, offset, nodesWritten uint64, nextPagesStart *[]uint64, offsets []uint64, writeFile bool) (uint64, uint64, error) {
	writeChild := func(highestNodeNumberInPage, nextNode uint64) uint64 {
		if nextNode >= t.numNodes || t.nodes[nextNode] == nil {
			return math.MaxUint64
		}
		if nextNode > highestNodeNumberInPage {
			*nextPagesStart = append(*nextPagesStart, nextNode)
		}
		return offsets[nextNode]
	}

	numNodesInPage := t.nodesInPage(start, t.pageNodesReusable)
	t.pageNodesReusable = t.pageNodesReusable[:numNodesInPage]
	highestNodeNumberInPage := t.pageNodesReusable[numNodesInPage-1]

	nodesCount := uint64(0)
	for _, id := range t.pageNodesReusable {
		left := writeChild(highestNodeNumberInPage, 2*id+1)  // left child
		right := writeChild(highestNodeNumberInPage, 2*id+2) // right child

		if !writeFile {
			offsets[id] = offset
		} else {
			t.tmpBuffer.ResetBuffer(t.tmpBuffer.Buffer)
			t.tmpBuffer.WriteUint32(uint32(len(t.nodes[id].Key)))
			t.tmpBuffer.CopyBytesToBuffer(t.nodes[id].Key)
			t.tmpBuffer.WriteUint64(t.nodes[id].Start)
			t.tmpBuffer.WriteUint64(t.nodes[id].End)
			t.tmpBuffer.WriteUint64(left)
			t.tmpBuffer.WriteUint64(right)

			t.w.Write(t.tmpBuffer.Buffer[:t.tmpBuffer.Position])
		}
		offset += uint64(t.nodes[id].size())
		nodesCount++
	}

	return offset, nodesWritten + nodesCount, nil
}
