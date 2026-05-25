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
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/varenc"
)

// makeInvertedPair builds a MapPair with the 8-byte inverted Value layout:
// Value[0:4] = TF (float32 bits), Value[4:8] = propLength (float32 bits).
func makeInvertedPair(docId uint64, tf, pl float32) MapPair {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, docId)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint32(value[0:4], math.Float32bits(tf))
	binary.LittleEndian.PutUint32(value[4:8], math.Float32bits(pl))
	return MapPair{Key: key, Value: value}
}

func plFromValue(v []byte) float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(v[4:8]))
}

// Sizes span the three encode paths:
//   - n=1: small (ENCODE_AS_FULL_BYTES) path
//   - n=2..128: single block
//   - n=129+: multi-block
var reusableTestSizes = []int{1, 2, 100, 128, 129, 256, 300}

// TestDecodeAndConvertFromBlocksReusable_MatchesNonReusable confirms that the
// reusable decoder produces the same MapPairs (key bytes and value bytes) as
// the canonical non-reusable decoder, across the small / single-block /
// multi-block paths.
func TestDecodeAndConvertFromBlocksReusable_MatchesNonReusable(t *testing.T) {
	for _, n := range reusableTestSizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			input := make([]MapPair, n)
			plm := make(map[uint64]uint32, n)
			for i := 0; i < n; i++ {
				input[i] = makeInvertedPair(uint64(i+1), float32(i+1), float32(i*2+1))
				plm[uint64(i+1)] = uint32(i*2 + 1)
			}
			encoded, _ := createAndEncodeBlocks(input, plm,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{},
				1.2, 0.75, 10.0)

			expected, _ := decodeAndConvertFromBlocks(encoded)

			got, _, _ := decodeAndConvertFromBlocksReusable(
				encoded, nil, nil,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{})

			require.Equal(t, len(expected), len(got))
			for i := range expected {
				assert.Equal(t, expected[i].Key, got[i].Key, "key mismatch at i=%d", i)
				assert.Equal(t, expected[i].Value, got[i].Value, "value mismatch at i=%d", i)
			}
		})
	}
}

// TestDecodeAndConvertFromBlocksReusable_PreservesPLZeroInvariant is the
// regression test for the arena-residue bug: when the kvArena is reused
// across decode calls (or pre-poisoned), Value[4:8] (the propLength slot)
// must be zero for every decoded pair, matching the prior make([]byte, 8)
// behaviour. The disk format does not carry propLengths in the value
// payload — they live in a separate map — so the decoded slot must be
// clean.
func TestDecodeAndConvertFromBlocksReusable_PreservesPLZeroInvariant(t *testing.T) {
	deltaEnc := &varenc.VarIntDeltaEncoder{}
	tfEnc := &varenc.VarIntEncoder{}

	encode := func(n int) []byte {
		input := make([]MapPair, n)
		plm := make(map[uint64]uint32, n)
		for i := 0; i < n; i++ {
			input[i] = makeInvertedPair(uint64(i+1), float32(i+1), float32(i+1))
			plm[uint64(i+1)] = uint32(i + 1)
		}
		out, _ := createAndEncodeBlocks(input, plm, deltaEnc, tfEnc, 1.2, 0.75, 10.0)
		return out
	}

	for _, n := range reusableTestSizes {
		t.Run(fmt.Sprintf("poisoned-arena/n=%d", n), func(t *testing.T) {
			// Pre-poison the arena with 0xFF so any unwritten byte in
			// Value[4:8] would show up as non-zero.
			arena := make([]byte, n*16)
			for i := range arena {
				arena[i] = 0xFF
			}

			got, _, _ := decodeAndConvertFromBlocksReusable(
				encode(n), nil, arena, deltaEnc, tfEnc)

			require.Len(t, got, n)
			for i, mp := range got {
				require.Equal(t, 8, len(mp.Value), "i=%d", i)
				assert.Equalf(t, float32(0), plFromValue(mp.Value),
					"Value[4:8] must be zero at i=%d (got %v)", i, plFromValue(mp.Value))
			}
		})
	}

	t.Run("reused-buffers-across-calls", func(t *testing.T) {
		// Two distinct payloads decoded with the same buffers. After the
		// second call, no byte in Value[4:8] may carry over from the first.
		data1 := encode(200)
		data2 := encode(150)

		var mapPairBuf []MapPair
		var arena []byte

		mapPairBuf, arena, _ = decodeAndConvertFromBlocksReusable(
			data1, mapPairBuf, arena, deltaEnc, tfEnc)
		for i, mp := range mapPairBuf {
			require.Equal(t, float32(0), plFromValue(mp.Value),
				"first call: PL must be zero at i=%d", i)
		}

		mapPairBuf, _, _ = decodeAndConvertFromBlocksReusable(
			data2, mapPairBuf, arena, deltaEnc, tfEnc)
		require.Len(t, mapPairBuf, 150)
		for i, mp := range mapPairBuf {
			assert.Equal(t, float32(0), plFromValue(mp.Value),
				"second call (reused arena): PL must be zero at i=%d", i)
		}
	})
}

// TestDecodeAndConvertFromBlocksReusable_Roundtrip drives encode → decode
// with reused mapPairBuf and kvArena across multiple distinct payloads,
// and confirms that on each decode every entry's docId and TF round-trip
// exactly and Value[4:8] is zero. The varenc TF encoder rounds floats to
// uint64, so the test uses integer TFs to keep the round-trip exact.
func TestDecodeAndConvertFromBlocksReusable_Roundtrip(t *testing.T) {
	deltaEnc := &varenc.VarIntDeltaEncoder{}
	tfEnc := &varenc.VarIntEncoder{}

	makePayload := func(n int, docIdBase uint64, tfBase float32) ([]byte, []MapPair) {
		input := make([]MapPair, n)
		plm := make(map[uint64]uint32, n)
		for i := 0; i < n; i++ {
			docId := docIdBase + uint64(i)
			tf := tfBase + float32(i)
			input[i] = makeInvertedPair(docId, tf, float32(i+1))
			plm[docId] = uint32(i + 1)
		}
		encoded, _ := createAndEncodeBlocks(input, plm, deltaEnc, tfEnc, 1.2, 0.75, 10.0)
		return encoded, input
	}

	// Sequence of payloads with distinct sizes so the arena/mapPairBuf
	// are sometimes reused at full size, sometimes truncated.
	payloads := []struct {
		n         int
		docIdBase uint64
		tfBase    float32
	}{
		{200, 1000, 10},
		{50, 5000, 1},
		{300, 7000, 20},
		{1, 9000, 7},
		{129, 11000, 3},
	}

	var mapPairBuf []MapPair
	var arena []byte

	for _, p := range payloads {
		t.Run(fmt.Sprintf("n=%d/docIdBase=%d", p.n, p.docIdBase), func(t *testing.T) {
			encoded, input := makePayload(p.n, p.docIdBase, p.tfBase)
			mapPairBuf, arena, _ = decodeAndConvertFromBlocksReusable(
				encoded, mapPairBuf, arena, deltaEnc, tfEnc)

			require.Len(t, mapPairBuf, p.n)
			for i, mp := range mapPairBuf {
				wantDocId := binary.BigEndian.Uint64(input[i].Key)
				gotDocId := binary.BigEndian.Uint64(mp.Key)
				assert.Equalf(t, wantDocId, gotDocId, "docId mismatch at i=%d", i)

				wantTF := math.Float32frombits(binary.LittleEndian.Uint32(input[i].Value[0:4]))
				gotTF := math.Float32frombits(binary.LittleEndian.Uint32(mp.Value[0:4]))
				assert.Equalf(t, wantTF, gotTF, "TF mismatch at i=%d (docId=%d)", i, gotDocId)

				assert.Equalf(t, float32(0), plFromValue(mp.Value),
					"Value[4:8] must be zero at i=%d", i)
			}
		})
	}
}

// TestCreateAndEncodeBlocksCompaction_MatchesNonReusable confirms that the
// compaction-optimised encoder produces byte-identical output to the
// canonical encoder when both receive the same input and a fully-populated
// propLengths map (the externalPropLengths==true path that compaction
// always takes).
func TestCreateAndEncodeBlocksCompaction_MatchesNonReusable(t *testing.T) {
	for _, n := range reusableTestSizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			input := make([]MapPair, n)
			for i := 0; i < n; i++ {
				input[i] = makeInvertedPair(uint64(i+1), float32(i+1), float32(i*2+1))
			}
			plmA := make(map[uint64]uint32, n)
			plmB := make(map[uint64]uint32, n)
			for i := 0; i < n; i++ {
				plmA[uint64(i+1)] = uint32(i*2 + 1)
				plmB[uint64(i+1)] = uint32(i*2 + 1)
			}

			expected, _ := createAndEncodeBlocks(input, plmA,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{},
				1.2, 0.75, 10.0)

			// createAndEncodeBlocksCompaction mutates its input slice
			// via filterTombstonesInPlace — pass a copy.
			inputCopy := make([]MapPair, n)
			copy(inputCopy, input)
			bufs := newCompactorInvertedBuffers()
			got := createAndEncodeBlocksCompaction(inputCopy, plmB, &bufs,
				&varenc.VarIntDeltaEncoder{}, &varenc.VarIntEncoder{},
				1.2, 0.75, 10.0)

			assert.Equal(t, expected, got)
		})
	}
}

// TestCreateAndEncodeBlocksCompaction_BufferReuse drives the same
// compactor buffers through multiple keys (the actual compaction
// workload) and checks the produced bytes are independent of buffer
// state from prior calls.
func TestCreateAndEncodeBlocksCompaction_BufferReuse(t *testing.T) {
	deltaEnc := &varenc.VarIntDeltaEncoder{}
	tfEnc := &varenc.VarIntEncoder{}

	makeInput := func(n int, tfBase float32) ([]MapPair, map[uint64]uint32) {
		input := make([]MapPair, n)
		plm := make(map[uint64]uint32, n)
		for i := 0; i < n; i++ {
			input[i] = makeInvertedPair(uint64(i+1), tfBase+float32(i), float32(i+1))
			plm[uint64(i+1)] = uint32(i + 1)
		}
		return input, plm
	}

	sizes := []int{200, 50, 300, 1, 129}

	bufs := newCompactorInvertedBuffers()
	for _, n := range sizes {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			input, plm := makeInput(n, 5.0)
			expected, _ := createAndEncodeBlocks(input, plm, deltaEnc, tfEnc, 1.2, 0.75, 10.0)

			inputCopy := make([]MapPair, n)
			copy(inputCopy, input)
			plmCopy := make(map[uint64]uint32, len(plm))
			for k, v := range plm {
				plmCopy[k] = v
			}

			got := createAndEncodeBlocksCompaction(inputCopy, plmCopy, &bufs,
				deltaEnc, tfEnc, 1.2, 0.75, 10.0)

			// The returned slice aliases bufs.encodeOutBuf for the
			// multi-block path; copy before the next iteration mutates it.
			gotCopy := append([]byte(nil), got...)
			assert.Equal(t, expected, gotCopy)
		})
	}
}

func TestFilterTombstonesInPlace(t *testing.T) {
	mk := func(id byte, tombstone bool) MapPair {
		return MapPair{Key: []byte{id}, Tombstone: tombstone}
	}

	tests := []struct {
		name     string
		input    []MapPair
		expected []MapPair
	}{
		{
			name:     "empty",
			input:    []MapPair{},
			expected: []MapPair{},
		},
		{
			name:     "none tombstoned",
			input:    []MapPair{mk('a', false), mk('b', false)},
			expected: []MapPair{mk('a', false), mk('b', false)},
		},
		{
			name:     "all tombstoned",
			input:    []MapPair{mk('a', true), mk('b', true), mk('c', true)},
			expected: []MapPair{},
		},
		{
			name: "interleaved",
			input: []MapPair{
				mk('a', false), mk('b', true), mk('c', false),
				mk('d', true), mk('e', false),
			},
			expected: []MapPair{mk('a', false), mk('c', false), mk('e', false)},
		},
		{
			name:     "leading tombstones",
			input:    []MapPair{mk('a', true), mk('b', true), mk('c', false)},
			expected: []MapPair{mk('c', false)},
		},
		{
			name:     "trailing tombstones",
			input:    []MapPair{mk('a', false), mk('b', true), mk('c', true)},
			expected: []MapPair{mk('a', false)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := filterTombstonesInPlace(tc.input)
			assert.Equal(t, tc.expected, got)
		})
	}
}
