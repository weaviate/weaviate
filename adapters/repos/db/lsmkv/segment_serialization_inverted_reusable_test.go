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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

// buildInvertedNodes returns count postings (ascending docIDs) plus a matching
// propLengthsView, so the compaction encoders and their non-reusable
// counterparts read the same property lengths.
func buildInvertedNodes(count, base int) ([]MapPair, *propLengthsView) {
	nodes := make([]MapPair, count)
	ids := make([]uint64, count)
	lens := make([]uint32, count)
	for i := 0; i < count; i++ {
		docID := uint64(base + i*3 + 1)
		nodes[i] = NewMapPairFromDocIdAndTf(docID, float32(i%7+1), float32(i%5+1), false)
		ids[i] = docID
		lens[i] = uint32(i%5 + 1)
	}
	return nodes, &propLengthsView{ids: ids, lens: lens}
}

func encodeInvertedNode(t *testing.T, count, base int) []byte {
	t.Helper()
	nodes := make([]MapPair, count)
	for i := 0; i < count; i++ {
		nodes[i] = NewMapPairFromDocIdAndTf(uint64(base+i*3+1), float32(i%7+1), 1, false)
	}
	data, _ := createAndEncodeBlocksWithLengths(nodes,
		&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{}, 1.2, 0.75, 1.0)
	return data
}

func copyMapPairs(in []MapPair) []MapPair {
	out := make([]MapPair, len(in))
	for i, mp := range in {
		out[i] = MapPair{
			Key:       append([]byte(nil), mp.Key...),
			Value:     append([]byte(nil), mp.Value...),
			Tombstone: mp.Tombstone,
		}
	}
	return out
}

// decodeAndConvertFromBlocksReusable must decode byte-for-byte identically to
// the allocating decodeAndConvertFromBlocks, across the full-bytes path
// (count <= ENCODE_AS_FULL_BYTES), a single block, and multiple blocks.
func TestDecodeAndConvertFromBlocksReusable(t *testing.T) {
	// ENCODE_AS_FULL_BYTES=1, BLOCK_SIZE=128
	for _, count := range []int{1, 5, 200} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			data := encodeInvertedNode(t, count, 0)

			want, wantOff := decodeAndConvertFromBlocks(data)

			got, _, gotOff := decodeAndConvertFromBlocksReusable(data, nil, nil,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{})

			assert.Equal(t, wantOff, gotOff, "data-end offset")
			assert.Equal(t, want, got, "decoded MapPairs")
		})
	}
}

// Decoding a second node into the same buffers must produce the right result
// and must not corrupt the first node's data once the caller has copied it out.
func TestDecodeAndConvertFromBlocksReusable_BufferReuse(t *testing.T) {
	dataA := encodeInvertedNode(t, 200, 0)     // multi-block
	dataB := encodeInvertedNode(t, 50, 100000) // smaller, fits A's buffers

	wantA, _ := decodeAndConvertFromBlocks(dataA)
	wantB, _ := decodeAndConvertFromBlocks(dataB)

	delta := &varenc.VarIntDeltaEncoder{}
	tf := &varenc.VarIntEncoder{}

	gotA, arena, _ := decodeAndConvertFromBlocksReusable(dataA, nil, nil, delta, tf)
	copyA := copyMapPairs(gotA) // caller consumes/copies before reusing

	gotB, _, _ := decodeAndConvertFromBlocksReusable(dataB, gotA, arena, delta, tf)
	copyB := copyMapPairs(gotB)

	require.Equal(t, wantA, copyA, "first node intact after copy-out")
	require.Equal(t, wantB, copyB, "second node correct after buffer reuse")
}

// A caller may pass a pre-sized but zero-length buffer, e.g. make([]byte, 0, N);
// the reusable decoders must reslice it rather than index past len. Covers both
// the full-bytes and multi-block arena paths.
func TestReusableDecodeAcceptsZeroLenPresizedBuffers(t *testing.T) {
	for _, count := range []int{1, 200} { // full-bytes path, then multi-block path
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			data := encodeInvertedNode(t, count, 0)
			want, _ := decodeAndConvertFromBlocks(data)
			got, _, _ := decodeAndConvertFromBlocksReusable(data,
				make([]MapPair, 0, count), make([]byte, 0, count*16),
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{})
			assert.Equal(t, want, got)
		})
	}
}

// The reusable compaction encoders must produce byte-identical output to the
// allocating createAndEncodeBlocks, across the full-bytes, single-block, and
// multi-block paths.
func TestCreateAndEncodeBlocksCompaction(t *testing.T) {
	for _, count := range []int{1, 5, 200} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			nodes, lookupA := buildInvertedNodes(count, 0)
			_, lookupB := buildInvertedNodes(count, 0)

			want, _ := createAndEncodeBlocks(copyMapPairs(nodes), lookupA,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{}, 1.2, 0.75, 1.0)

			bufs := newCompactorInvertedBuffers()
			got := createAndEncodeBlocksCompaction(copyMapPairs(nodes), lookupB, &bufs,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{}, 1.2, 0.75, 1.0)

			assert.Equal(t, want, got)
		})
	}
}

// KeyIndexAndWriteToCompaction must write the same bytes as KeyIndexAndWriteTo
// and report the same end offset (as a KeyRedux).
func TestKeyIndexAndWriteToCompaction(t *testing.T) {
	for _, count := range []int{1, 5, 200} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			nodes, lookupA := buildInvertedNodes(count, 0)
			_, lookupB := buildInvertedNodes(count, 0)
			key := []byte("some-primary-key")

			var wantBuf bytes.Buffer
			wantKey, err := segmentInvertedNode{
				values: copyMapPairs(nodes), primaryKey: key, offset: 100, propLengths: lookupA,
			}.KeyIndexAndWriteTo(&wantBuf, &varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{}, 1.2, 0.75, 1.0)
			require.NoError(t, err)

			var gotBuf bytes.Buffer
			bufs := newCompactorInvertedBuffers()
			redux, err := segmentInvertedNode{
				values: copyMapPairs(nodes), primaryKey: key, offset: 100, propLengths: lookupB,
			}.KeyIndexAndWriteToCompaction(&gotBuf, make([]byte, 8), &bufs,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{}, 1.2, 0.75, 1.0)
			require.NoError(t, err)

			assert.Equal(t, wantBuf.Bytes(), gotBuf.Bytes(), "written bytes")
			assert.Equal(t, wantKey.ValueEnd, redux.ValueEnd, "ValueEnd")
			assert.Equal(t, wantKey.Key, redux.Key, "Key")
		})
	}
}
