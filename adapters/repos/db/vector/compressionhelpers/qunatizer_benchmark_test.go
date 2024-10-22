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

package compressionhelpers

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkBQFromCompressedBytes(b *testing.B) {
	bq := NewBinaryQuantizer(nil)
	iterations := 1000
	encoded := make([][]uint64, iterations)
	for i := range encoded {
		encoded[i] = make([]uint64, 6)
		for j := range encoded[i] {
			encoded[i][j] = uint64(j)
		}
	}

	compressed := make([][]byte, iterations)
	for i := range compressed {
		compressed[i] = make([]byte, 48)
		compressed[i] = bq.CompressedBytes(encoded[i])
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := range compressed {
			_ = bq.FromCompressedBytes(compressed[j])
		}
	}
}

func BenchmarkBQFromCompressedBytesWithSubsliceBuffer(b *testing.B) {
	bq := NewBinaryQuantizer(nil)
	iterations := 1000
	encoded := make([][]uint64, iterations)
	for i := range encoded {
		encoded[i] = make([]uint64, 6)
		for j := range encoded[i] {
			encoded[i][j] = uint64(j)
		}
	}

	compressed := make([][]byte, iterations)
	for i := range compressed {
		compressed[i] = make([]byte, 48)
		compressed[i] = bq.CompressedBytes(encoded[i])
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buffer []uint64
		for j := range compressed {
			_ = bq.FromCompressedBytesWithSubsliceBuffer(compressed[j], &buffer)
		}
	}
}

// This verifies that both the "normal" and the "with subslice buffer" version of
// FromCompressedBytes produce identical results
func TestBQFromCompressedBytes_SanityCheck(t *testing.T) {
	bq := NewBinaryQuantizer(nil)
	iterations := 10000
	encoded := make([][]uint64, iterations)
	for i := range encoded {
		encoded[i] = make([]uint64, 6)
		for j := range encoded[i] {
			encoded[i][j] = rand.Uint64()
		}
	}

	compressed := make([][]byte, iterations)
	for i := range compressed {
		compressed[i] = make([]byte, 48)
		compressed[i] = bq.CompressedBytes(encoded[i])
	}

	var buffer []uint64
	for j := range compressed {
		regular := bq.FromCompressedBytes(compressed[j])
		withBuffer := bq.FromCompressedBytesWithSubsliceBuffer(compressed[j], &buffer)
		assert.Equal(t, regular, withBuffer)
	}
}
