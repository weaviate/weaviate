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

type BigHashTree interface {
	Height() int
	AggregateLeafWith(i uint64, val []byte) BigHashTree
	Sync() BigHashTree
	Level(level int, discriminant *Bitset, digests []Digest) (n int, err error)
	Reset() BigHashTree
	Clone() BigHashTree
}

type BigHashTreeDiffReader interface {
	Next() (uint64, uint64, error)
}
