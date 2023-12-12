//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hashtree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/spaolacci/murmur3"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrIllegalState = errors.New("illegal state")

type Digest [2]uint64

type HashTree struct {
	height          int
	innerNodesCount int
	nodes           []Digest
	syncState       *Bitset
	hash            murmur3.Hash128
}

func NewHashTree(height int) *HashTree {
	if height < 1 {
		panic("illegal height")
	}

	nodesCount := NodesCount(height)

	var innerNodesCount int

	if height > 1 {
		innerNodesCount = NodesCount(height - 1)
	}

	return &HashTree{
		height:          height,
		innerNodesCount: innerNodesCount,
		nodes:           make([]Digest, nodesCount),
		syncState:       NewBitset(nodesCount),
		hash:            murmur3.New128(),
	}
}

func NodesCount(height int) int {
	return 1<<height - 1
}

func LeavesCount(height int) int {
	return 1 << (height - 1)
}

func nodesAtLevel(l int) int {
	return 1 << l
}

func (ht *HashTree) Height() int {
	return ht.height
}

func (ht *HashTree) AggregateLeafWith(i int, val []byte) *HashTree {
	ht.hash.Reset()
	ht.hash.Write(val)
	valh1, valh2 := ht.hash.Sum128()

	leaf := ht.innerNodesCount + i

	// XOR with current leaf digest
	ht.nodes[leaf][0] ^= valh1
	ht.nodes[leaf][1] ^= valh2

	ht.syncState.Unset(leaf) // sync required
	ht.syncState.Unset(0)    // used to quickly determine if some sync required

	return ht
}

func (ht *HashTree) Reset() *HashTree {
	for i := 0; i < len(ht.nodes); i++ {
		ht.nodes[i][0] = 0
		ht.nodes[i][1] = 0
	}
	ht.syncState.Reset()

	return ht
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

func (ht *HashTree) sync() {
	if !ht.syncRequired(0) {
		return
	}

	// calculate the hash tree up to the root

	for l := ht.height - 2; l >= 0; l-- {
		firstNode := NodesCount(l)

		var parentSyncRequired bool

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

			parentSyncRequired = true
		}

		if !parentSyncRequired {
			break
		}
	}

	ht.syncState.Set(0) // no sync is required
}

func (ht *HashTree) Level(level int, discriminant *Bitset, digests []Digest) (n int, err error) {
	if level < 0 {
		return 0, fmt.Errorf("%w: invalid level(%d)", ErrIllegalArguments, level)
	}

	if level >= ht.Height() {
		return 0, fmt.Errorf("%w: level(%d) is too high for current height(%d)", ErrIllegalState, level, ht.height)
	}

	if discriminant == nil {
		return 0, fmt.Errorf("%w: nil discriminant provided", ErrIllegalArguments)
	}

	if len(digests) < nodesAtLevel(level) {
		return 0, fmt.Errorf("%w: output buffer has not enough capacity", ErrIllegalArguments)
	}

	ht.sync()

	var offset int

	if level > 0 {
		offset = NodesCount(level)
	}

	// TODO(jeroiraz): it may be more performant to iterate over the set positions in the discriminant
	for i := 0; i < nodesAtLevel(level); i++ {
		if discriminant.IsSet(i) {
			digests[n] = ht.nodes[offset+i]
			n++
		}
	}

	return n, nil
}
