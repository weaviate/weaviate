//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hashtree

import (
	"fmt"
	"testing"
)

// BenchmarkLevel and BenchmarkLevelLocal measure the per-call cost of retrieving
// digests at the leaf level (height=16, all nodes selected). Both functions do
// the same O(n) inner loop; the comparison establishes that LevelLocal does not
// introduce overhead relative to Level for the same workload.
func BenchmarkLevel(b *testing.B) {
	ht := buildBenchTree(b, 16)
	disc := NewBitset(NodesCount(16)).SetAll()
	digests := make([]Digest, LeavesCount(16))
	b.SetBytes(int64(LeavesCount(16)) * 16) // 16 bytes per Digest
	b.ResetTimer()
	var n int
	for i := 0; i < b.N; i++ {
		var err error
		n, err = ht.Level(16, disc, digests)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = n
}

func BenchmarkLevelLocal(b *testing.B) {
	ht := buildBenchTree(b, 16)
	disc := NewBitset(LeavesCount(16)).SetAll()
	digests := make([]Digest, LeavesCount(16))
	b.SetBytes(int64(LeavesCount(16)) * 16)
	b.ResetTimer()
	var n int
	for i := 0; i < b.N; i++ {
		var err error
		n, err = ht.LevelLocal(16, disc, digests)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = n
}

// BenchmarkFullTraversal* benchmarks a complete height+1 level traversal — the
// actual hashbeat call pattern. BenchmarkFullTraversalLevelLocal includes the
// ExtractSlice cost that ships the level-local discriminant, so this pair
// represents the end-to-end per-hashbeat CPU cost for the two approaches.
func BenchmarkFullTraversalLevel(b *testing.B) {
	height := 16
	ht := buildBenchTree(b, height)
	fullDisc := NewBitset(NodesCount(height)).SetAll()
	digests := make([]Digest, LeavesCount(height))
	b.ResetTimer()
	var n int
	for i := 0; i < b.N; i++ {
		for level := 0; level <= height; level++ {
			var err error
			n, err = ht.Level(level, fullDisc, digests)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	_ = n
}

func BenchmarkFullTraversalLevelLocal(b *testing.B) {
	height := 16
	ht := buildBenchTree(b, height)
	fullDisc := NewBitset(NodesCount(height)).SetAll()
	digests := make([]Digest, LeavesCount(height))
	b.ResetTimer()
	var n int
	for i := 0; i < b.N; i++ {
		for level := 0; level <= height; level++ {
			local := fullDisc.ExtractSlice(InnerNodesCount(level), LeavesCount(level))
			var err error
			n, err = ht.LevelLocal(level, local, digests)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	_ = n
}

func buildBenchTree(b *testing.B, height int) *HashTree {
	b.Helper()
	ht, err := NewHashTree(height)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < LeavesCount(height); i++ {
		if err := ht.AggregateLeafWith(uint64(i), fmt.Appendf(nil, "v%d", i)); err != nil {
			b.Fatal(err)
		}
	}
	return ht
}
