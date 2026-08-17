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
	"bufio"
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

func serializeReplaceNode(t testing.TB, node segmentReplaceNode) []byte {
	t.Helper()
	var buf bytes.Buffer
	_, err := node.KeyIndexAndWriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
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
				raw := serializeReplaceNode(t, node)
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
					require.NoError(t, ParseReplaceNodeDigestIntoPread(bufio.NewReader(bytes.NewReader(raw)), secCount, prefix, &got))
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
		raw := serializeReplaceNode(t, node)

		wantPrefix := prefix
		if wantPrefix > valueSize {
			wantPrefix = valueSize
		}
		wantValue := node.value[:wantPrefix]

		require.NoError(t, ParseReplaceNodeDigestIntoPread(bufio.NewReader(bytes.NewReader(raw)), 1, prefix, &reusedPread))
		require.Equal(t, node.primaryKey, reusedPread.primaryKey)
		require.True(t, bytes.Equal(wantValue, reusedPread.value), "pread reuse value size=%d", valueSize)

		rw := byteops.NewReadWriter(raw)
		require.NoError(t, ParseReplaceNodeDigestIntoMMAP(&rw, 1, prefix, &reusedMMAP))
		require.Equal(t, node.primaryKey, reusedMMAP.primaryKey)
		require.True(t, bytes.Equal(wantValue, reusedMMAP.value), "mmap reuse value size=%d", valueSize)
	}
}

// preadReplaceParsers are the two parsers the reusable pread cursor drives, over the same wire format.
var preadReplaceParsers = []struct {
	name  string
	parse func(*bufio.Reader, uint16, *segmentReplaceNode) error
}{
	{"full", func(r *bufio.Reader, secCount uint16, out *segmentReplaceNode) error {
		return ParseReplaceNodeIntoPread(r, secCount, out)
	}},
	{"digest", func(r *bufio.Reader, secCount uint16, out *segmentReplaceNode) error {
		return ParseReplaceNodeDigestIntoPread(r, secCount, 42, out)
	}},
}

// TestPreadReplaceParsers_Truncated pins that a node cut short at any read or skip point returns an error, rather than reporting a short node as successfully parsed and desynchronising the cursor.
func TestPreadReplaceParsers_Truncated(t *testing.T) {
	const (
		valueSize    = 4096
		keySize      = 16
		secKeySize   = 128
		valueStart   = 9
		keyLenStart  = valueStart + valueSize
		keyStart     = keyLenStart + 4
		secLenStart  = keyStart + keySize
		secKeyStart  = secLenStart + 4
		nodeByteSize = secKeyStart + secKeySize
	)

	raw := serializeReplaceNode(t, makeReplaceNode(valueSize, keySize, 1, secKeySize, false))
	require.Len(t, raw, nodeByteSize)

	// the expected message identifies which read failed, so a skip whose error is
	// dropped surfaces as the next read's failure and fails the case
	cases := []struct {
		name       string
		keep       int
		wantFull   string
		wantDigest string
	}{
		{"empty", 0, "read tombstone and value length", "read tombstone and value length"},
		{"header", valueStart - 1, "read tombstone and value length", "read tombstone and value length"},
		{"value-prefix", valueStart + 41, "read value", "read value prefix"},
		{"value-remainder", valueStart + 43, "read value", "skip value remainder"},
		{"key-length", keyLenStart + 2, "read key length encoding", "read key length encoding"},
		{"key", keyStart + keySize/2, "read key", "read key"},
		{"secondary-key-length", secLenStart + 2, "read secondary key length encoding", "read secondary key length encoding"},
		{"secondary-key", secKeyStart + secKeySize/2, "read secondary key", "skip secondary key"},
	}

	for _, p := range preadReplaceParsers {
		for _, tc := range cases {
			t.Run(p.name+"/"+tc.name, func(t *testing.T) {
				want := tc.wantFull
				if p.name == "digest" {
					want = tc.wantDigest
				}
				var out segmentReplaceNode
				err := p.parse(bufio.NewReader(bytes.NewReader(raw[:tc.keep])), 1, &out)
				require.ErrorContains(t, err, want)
			})
		}
	}
}

// TestPreadReplaceParsers_NoAllocs pins both pread parsers at zero allocations per node once the out node's buffers are warm — the reusable cursor parses every node of every segment through them.
func TestPreadReplaceParsers_NoAllocs(t *testing.T) {
	cases := []struct {
		name             string
		valueSize        int
		secondaryCount   int
		secondaryKeySize int
	}{
		// value remainder and secondary key are both skipped in digest mode
		{"one-secondary", 4096, 1, 128},
		{"two-secondary", 4096, 2, 128},
		// nothing to skip in digest mode: prefix covers the value, secondary key is empty
		{"nothing-to-skip", 10, 1, 0},
		// skip spans several refills of the pooled read buffer
		{"skip-spans-refills", 8 * segmentCursorReaderBufSize, 1, 128},
		{"no-secondary", 4096, 0, 0},
	}

	for _, p := range preadReplaceParsers {
		for _, tc := range cases {
			t.Run(p.name+"/"+tc.name, func(t *testing.T) {
				raw := serializeReplaceNode(t, makeReplaceNode(tc.valueSize, 16, tc.secondaryCount, tc.secondaryKeySize, false))
				secCount := uint16(tc.secondaryCount)

				src := bytes.NewReader(raw)
				r := bufio.NewReaderSize(src, segmentCursorReaderBufSize)
				out := &segmentReplaceNode{}

				var parseErr error
				allocs := testing.AllocsPerRun(100, func() {
					src.Reset(raw)
					r.Reset(src)
					if err := p.parse(r, secCount, out); err != nil {
						parseErr = err
					}
				})
				require.NoError(t, parseErr)
				require.Zero(t, allocs, "parsing into a warm node must not allocate")
			})
		}
	}
}

// BenchmarkParseReplaceNodeDigestVsFull_Pread shows the alloc drop from skipping the full value on the pread path.
func BenchmarkParseReplaceNodeDigestVsFull_Pread(b *testing.B) {
	raw := serializeReplaceNode(b, makeReplaceNode(4096, 16, 1, 128, false))

	src := bytes.NewReader(raw)
	r := bufio.NewReader(src)
	rewind := func() {
		src.Reset(raw)
		r.Reset(src)
	}

	b.Run("full", func(b *testing.B) {
		var out segmentReplaceNode
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out = segmentReplaceNode{}
			rewind()
			_ = ParseReplaceNodeIntoPread(r, 1, &out)
		}
	})

	b.Run("digest", func(b *testing.B) {
		var out segmentReplaceNode
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			out = segmentReplaceNode{}
			rewind()
			_ = ParseReplaceNodeDigestIntoPread(r, 1, 42, &out)
		}
	})
}
