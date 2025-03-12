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

import "io"

type AggregatedHashTree interface {
	Height() int
	AggregateLeafWith(i uint64, val []byte) error
	Sync()
	Root() Digest
	Level(level int, discriminant *Bitset, digests []Digest) (n int, err error)
	Reset()
	Clone() AggregatedHashTree

	Diff(ht AggregatedHashTree) (discriminant *Bitset, err error)
	DiffUsing(ht AggregatedHashTree, discriminant *Bitset, digests1, digests2 []Digest) error

	NewRangeReader(discriminant *Bitset) AggregatedHashTreeRangeReader

	Serialize(w io.Writer) (n int64, err error)
}

type AggregatedHashTreeRangeReader interface {
	Next() (uint64, uint64, error)
}

func LevelDiff(l int, discriminant *Bitset, digests1, digests2 []Digest) (levelDiffCount int) {
	offset := InnerNodesCount(l)

	n := 0

	for j := 0; j < nodesAtLevel(l); j++ {
		node := offset + j

		if !discriminant.IsSet(node) {
			continue
		}

		if digests1[n] == digests2[n] {
			n++
			discriminant.Unset(node)
			continue
		} else {
			levelDiffCount++
		}

		n++

		leftChild := 2*node + 1
		rightChild := 2*node + 2

		if discriminant.Size() <= rightChild {
			// node is a leaf
			continue
		}

		discriminant.Set(leftChild)
		discriminant.Set(rightChild)
	}

	return levelDiffCount
}
