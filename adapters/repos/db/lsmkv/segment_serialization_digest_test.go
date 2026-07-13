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

package lsmkv

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/byteops"
)

func makeReplaceNode(valueSize, keySize, secondaryCount, secondaryKeySize int, tombstone bool) segmentReplaceNode {
	value := make([]byte, valueSize)
	for i := range value {
		value[i] = byte(i%251 + 1)
	}
	key := make([]byte, keySize)
	for i := range key {
		key[i] = byte(255 - i%251)
	}
	secondaryKeys := make([][]byte, secondaryCount)
	for j := range secondaryKeys {
		sk := make([]byte, secondaryKeySize)
		for i := range sk {
			sk[i] = byte((j + i) % 251)
		}
		secondaryKeys[j] = sk
	}
	return segmentReplaceNode{
		tombstone:           tombstone,
		value:               value,
		primaryKey:          key,
		secondaryIndexCount: uint16(secondaryCount),
		secondaryKeys:       secondaryKeys,
	}
}

// TestParseReplaceNodeDigest_ParityWithFull checks digest parsers match the full parser on key/tombstone/offset and retain exactly the requested value prefix.
func TestParseReplaceNodeDigest_ParityWithFull(t *testing.T) {
	cases := []struct {
		name             string
		valueSize        int
		keySize          int
		secondaryCount   int
		secondaryKeySize int
		tombstone        bool
	}{
		{"empty-value", 0, 16, 0, 0, false},
		{"value-below-prefix", 10, 16, 0, 0, false},
		{"value-equals-prefix", 42, 16, 0, 0, false},
		{"value-above-prefix", 100, 16, 0, 0, false},
		{"vector-sized-value", 4096, 16, 0, 0, false},
		{"one-secondary", 4096, 16, 1, 128, false},
		{"one-secondary-zero-len", 4096, 16, 1, 0, false},
		{"two-secondary", 4096, 16, 2, 64, false},
		{"tombstone", 4096, 16, 1, 128, true},
		{"large-key", 200, 512, 1, 128, false},
	}

	prefixes := []int{0, 8, 42, 1 << 20} // last exceeds every value -> clamps to full

	for _, tc := range cases {
		for _, prefix := range prefixes {
			t.Run(fmt.Sprintf("%s/prefix=%d", tc.name, prefix), func(t *testing.T) {
				node := makeReplaceNode(tc.valueSize, tc.keySize, tc.secondaryCount, tc.secondaryKeySize, tc.tombstone)
				var buf bytes.Buffer
				_, err := node.KeyIndexAndWriteTo(&buf)
				require.NoError(t, err)
				raw := buf.Bytes()
				secCount := uint16(tc.secondaryCount)

				var full segmentReplaceNode
				require.NoError(t, ParseReplaceNodeIntoPread(bytes.NewReader(raw), secCount, &full))

				wantPrefix := prefix
				if wantPrefix > tc.valueSize {
					wantPrefix = tc.valueSize
				}

				assertDigestParity := func(t *testing.T, got segmentReplaceNode) {
					require.Equal(t, full.tombstone, got.tombstone, "tombstone")
					require.Equal(t, full.offset, got.offset, "offset must match full parse")
					require.Equal(t, full.primaryKey, got.primaryKey, "primaryKey")
					require.Len(t, got.value, wantPrefix, "retained value length")
					require.True(t, bytes.Equal(full.value[:wantPrefix], got.value), "value prefix bytes")
				}

				t.Run("pread", func(t *testing.T) {
					var got segmentReplaceNode
					require.NoError(t, ParseReplaceNodeDigestIntoPread(bytes.NewReader(raw), secCount, prefix, &got))
					assertDigestParity(t, got)
				})

				t.Run("mmap", func(t *testing.T) {
					rw := byteops.NewReadWriter(raw)
					var got segmentReplaceNode
					require.NoError(t, ParseReplaceNodeDigestIntoMMAP(&rw, secCount, prefix, &got))
					assertDigestParity(t, got)
				})
			})
		}
	}
}

// TestParseReplaceNodeDigest_BufferReuse reuses one out node across shrinking/growing values to catch stale-buffer corruption in the retained prefix.
func TestParseReplaceNodeDigest_BufferReuse(t *testing.T) {
	const prefix = 42
	sizes := []int{4096, 10, 2048, 0, 100}

	var reusedPread segmentReplaceNode
	var reusedMMAP segmentReplaceNode
	for _, valueSize := range sizes {
		node := makeReplaceNode(valueSize, 16, 1, 128, false)
		var buf bytes.Buffer
		_, err := node.KeyIndexAndWriteTo(&buf)
		require.NoError(t, err)
		raw := buf.Bytes()

		wantPrefix := prefix
		if wantPrefix > valueSize {
			wantPrefix = valueSize
		}
		wantValue := node.value[:wantPrefix]

		require.NoError(t, ParseReplaceNodeDigestIntoPread(bytes.NewReader(raw), 1, prefix, &reusedPread))
		require.Equal(t, node.primaryKey, reusedPread.primaryKey)
		require.True(t, bytes.Equal(wantValue, reusedPread.value), "pread reuse value size=%d", valueSize)

		rw := byteops.NewReadWriter(raw)
		require.NoError(t, ParseReplaceNodeDigestIntoMMAP(&rw, 1, prefix, &reusedMMAP))
		require.Equal(t, node.primaryKey, reusedMMAP.primaryKey)
		require.True(t, bytes.Equal(wantValue, reusedMMAP.value), "mmap reuse value size=%d", valueSize)
	}
}

// BenchmarkParseReplaceNodeDigestVsFull_Pread shows the alloc drop from skipping the full value on the pread path.
func BenchmarkParseReplaceNodeDigestVsFull_Pread(b *testing.B) {
	node := makeReplaceNode(4096, 16, 1, 128, false)
	var buf bytes.Buffer
	_, err := node.KeyIndexAndWriteTo(&buf)
	require.NoError(b, err)
	raw := buf.Bytes()

	b.Run("full", func(b *testing.B) {
		var out segmentReplaceNode
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out = segmentReplaceNode{}
			_ = ParseReplaceNodeIntoPread(bytes.NewReader(raw), 1, &out)
		}
	})

	b.Run("digest", func(b *testing.B) {
		var out segmentReplaceNode
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out = segmentReplaceNode{}
			_ = ParseReplaceNodeDigestIntoPread(bytes.NewReader(raw), 1, 42, &out)
		}
	})
}
