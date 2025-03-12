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

package hashtree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/spaolacci/murmur3"
)

var (
	ErrIllegalArguments = errors.New("illegal arguments")
	ErrIllegalState     = errors.New("illegal state")
)

var _ AggregatedHashTree = (*HashTree)(nil)

// HashTree is a fixed-size hash tree with leaf aggregation and partial root recalculation.
type HashTree struct {
	height int

	nodes []Digest

	// syncState is a bit-tree of the same height as the associated hash tree
	// a zeroed bit in the associated node means the current value has changed since
	// previous root computation and thus, its parent node must be re-åcalculated
	syncState *Bitset

	hash murmur3.Hash128

	// pre-calculated value
	innerNodesCount int

	mux sync.Mutex
}

func NewHashTree(height int) (*HashTree, error) {
	if height < 0 {
		return nil, fmt.Errorf("%w: illegal height", ErrIllegalArguments)
	}

	nodesCount := NodesCount(height)
	innerNodesCount := InnerNodesCount(height)

	return &HashTree{
		height:          height,
		nodes:           make([]Digest, nodesCount),
		syncState:       NewBitset(nodesCount),
		hash:            murmur3.New128(),
		innerNodesCount: innerNodesCount,
	}, nil
}

func NodesCount(height int) int {
	return (1 << (height + 1)) - 1
}

func InnerNodesCount(height int) int {
	return 1<<height - 1
}

func LeavesCount(height int) int {
	return 1 << height
}

func nodesAtLevel(l int) int {
	return 1 << l
}

func (ht *HashTree) Height() int {
	return ht.height
}

func (ht *HashTree) AggregateLeafWith(i uint64, val []byte) error {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	ht.hash.Reset()
	ht.hash.Write(val)
	valh1, valh2 := ht.hash.Sum128()

	leaf := ht.innerNodesCount + int(i)

	if leaf >= len(ht.nodes) {
		return fmt.Errorf("leaf out of bounds")
	}

	// XOR with current leaf digest
	ht.nodes[leaf][0] ^= valh1
	ht.nodes[leaf][1] ^= valh2

	ht.syncState.Unset(leaf) // sync required
	ht.syncState.Unset(0)    // used to quickly determine if some sync required

	return nil
}

func (ht *HashTree) Reset() {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	for i := 0; i < len(ht.nodes); i++ {
		ht.nodes[i][0] = 0
		ht.nodes[i][1] = 0
	}

	ht.syncState.Reset()
}

func (ht *HashTree) syncRequired(node int) bool {
	return !ht.syncState.IsSet(node)
}

func writeDigest(w io.Writer, d Digest) {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:], d[0])
	binary.BigEndian.PutUint64(b[8:], d[1])
	w.Write(b[:])
}

func (ht *HashTree) Sync() {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	ht.sync()
}

func (ht *HashTree) sync() {
	if !ht.syncRequired(0) {
		return
	}

	// calculate the hash tree up to the root

	for l := ht.height - 1; l >= 0; l-- {
		firstNode := InnerNodesCount(l)

		// iterate over the nodes at level l
		for i := 0; i < nodesAtLevel(l); i++ {
			node := firstNode + i
			leftChild := node*2 + 1
			rightChild := node*2 + 2

			if !ht.syncRequired(leftChild) && !ht.syncRequired(rightChild) {
				// none of its children has been updated since last synchronization
				continue
			}

			ht.hash.Reset()
			writeDigest(ht.hash, ht.nodes[leftChild])
			writeDigest(ht.hash, ht.nodes[rightChild])
			leftH, rightH := ht.hash.Sum128()

			ht.nodes[node][0] = leftH
			ht.nodes[node][1] = rightH

			ht.syncState.Set(leftChild)
			ht.syncState.Set(rightChild)

			ht.syncState.Unset(node)
		}
	}

	ht.syncState.Set(0) // no sync is required
}

func (ht *HashTree) Root() Digest {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	return ht.root()
}

func (ht *HashTree) root() Digest {
	ht.sync()
	return ht.nodes[0]
}

func (ht *HashTree) Level(level int, discriminant *Bitset, digests []Digest) (n int, err error) {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	if level < 0 {
		return 0, fmt.Errorf("%w: invalid level(%d)", ErrIllegalArguments, level)
	}

	if level > ht.Height() {
		return 0, fmt.Errorf("%w: level(%d) is too high for current height(%d)", ErrIllegalState, level, ht.height)
	}

	if discriminant == nil {
		return 0, fmt.Errorf("%w: nil discriminant provided", ErrIllegalArguments)
	}

	if len(digests) < nodesAtLevel(level) {
		return 0, fmt.Errorf("%w: output buffer has not enough capacity", ErrIllegalArguments)
	}

	ht.sync()

	offset := InnerNodesCount(level)

	// TODO(jeroiraz): it may be more performant to iterate over the set positions in the discriminant
	for i := 0; i < nodesAtLevel(level); i++ {
		if discriminant.IsSet(offset + i) {
			digests[n] = ht.nodes[offset+i]
			n++
		}
	}

	return n, nil
}

func (ht *HashTree) Clone() AggregatedHashTree {
	ht.mux.Lock()
	defer ht.mux.Unlock()

	clone := &HashTree{
		height:          ht.height,
		nodes:           make([]Digest, len(ht.nodes)),
		syncState:       ht.syncState.Clone(),
		hash:            murmur3.New128(),
		innerNodesCount: ht.innerNodesCount,
	}

	copy(clone.nodes, ht.nodes)

	return clone
}
