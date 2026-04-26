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

import "testing"

// BenchmarkExtractSlice* measure the cost of producing a level-local discriminant
// from the full-tree discriminant before each HashTreeLevel RPC. The height-16
// leaf-level case is the most expensive call in a hashbeat iteration; lower levels
// are included to show how cost scales with slice size.
//
// InnerNodesCount(l) = 2^l - 1 is never word-aligned for l >= 1, so all real
// call-site extractions exercise the shift path. BenchmarkExtractSliceWordAligned
// provides a synthetic aligned baseline for comparison.
func BenchmarkExtractSliceLevel16(b *testing.B) {
	// Largest slice in a default single-tenant hashbeat: leaf level of height-16.
	// offset=65535 (not word-aligned), count=65536.
	src := NewBitset(NodesCount(16)).SetAll()
	offset := InnerNodesCount(16) // 65535
	count := LeavesCount(16)      // 65536
	b.SetBytes(int64(count / 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = src.ExtractSlice(offset, count)
	}
}

func BenchmarkExtractSliceLevel8(b *testing.B) {
	// Mid-tree level: offset=255 (not word-aligned), count=256.
	src := NewBitset(NodesCount(16)).SetAll()
	offset := InnerNodesCount(8) // 255
	count := LeavesCount(8)      // 256
	b.SetBytes(int64(count / 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = src.ExtractSlice(offset, count)
	}
}

func BenchmarkExtractSliceWordAligned(b *testing.B) {
	// Synthetic word-aligned baseline (offset divisible by 64).
	// Uses the copy path instead of the shift loop.
	src := NewBitset(256).SetAll()
	b.SetBytes(128 / 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = src.ExtractSlice(64, 128)
	}
}
