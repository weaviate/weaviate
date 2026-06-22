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

package varenc

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// decodeReusableLegacy is a straightforward byte-by-byte varint decoder kept as
// a reference oracle. The reservoir / 64-bit-read decoder must stay bit-identical
// to it: the differential tests below require byte-identical output from both for
// every validly-encoded buffer, and fail if the two ever diverge.
func decodeReusableLegacy(deltas []uint64, packed []byte, deltaDiff bool) {
	if len(packed) < 8 {
		return
	}

	deltas[0] = binary.BigEndian.Uint64(packed[0:8])

	bitsNeeded := int((packed[8] >> 2) & 0x3F)
	if bitsNeeded == 0 || bitsNeeded > 64 {
		return
	}

	bitPos := 6
	bytePos := 8

	bitsLeft := 8 - bitPos
	bitBuffer := uint64(packed[bytePos] & ((1 << bitsLeft) - 1))

	bytePos++

	bitsMask := uint64((1 << bitsNeeded) - 1)

	for i := 1; i < len(deltas); i++ {
		for bitsLeft < bitsNeeded {
			if bytePos >= len(packed) {
				return
			}
			bitBuffer = (bitBuffer << 8) | uint64(packed[bytePos])
			bitsLeft += 8
			bytePos++
		}
		bitsLeft -= bitsNeeded
		deltas[i] = (bitBuffer >> bitsLeft) & bitsMask
		if deltaDiff {
			deltas[i] += deltas[i-1]
		}
	}
}

// diffSizes spans the reservoir/uint64 fast loops and their byte tails, plus the
// 8/9-byte guard boundary and several non-block-aligned counts.
var diffSizes = []int{1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 127, 128, 129, 255, 256, 511, 512}

// TestVarIntDecodeMatchesLegacyDecoder is the differential test pinning decoder
// bit-identity: for both codecs, every supported bit width, and a wide range of
// counts, the reservoir decoder must produce exactly the same values as the
// reference decoder AND round-trip the original input.
func TestVarIntDecodeMatchesLegacyDecoder(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x5eed, 0x1337))

	for _, deltaDiff := range []bool{false, true} {
		for bnu := 1; bnu <= maxSupportedBnu; bnu++ {
			maxVal := uint64(1)<<uint(bnu) - 1
			for _, n := range diffSizes {
				// In delta mode the cumulative sum must stay within uint64.
				if deltaDiff && maxVal != 0 && uint64(n) > ^uint64(0)/maxVal {
					continue
				}
				for trial := 0; trial < 6; trial++ {
					values := makeWidthValues(rng, n, bnu, maxVal, deltaDiff)

					var packed []byte
					if deltaDiff {
						enc := &VarIntDeltaEncoder{}
						enc.Init(n)
						packed = enc.Encode(values)
					} else {
						enc := &VarIntEncoder{}
						enc.Init(n)
						packed = enc.Encode(values)
					}

					gotNew := make([]uint64, n)
					gotOld := make([]uint64, n)
					decodeReusable(gotNew, packed, deltaDiff)
					decodeReusableLegacy(gotOld, packed, deltaDiff)

					require.Equalf(t, gotOld, gotNew,
						"new decoder diverged from legacy: deltaDiff=%v bnu=%d n=%d trial=%d packed=%x",
						deltaDiff, bnu, n, trial, packed)
					require.Equalf(t, values, gotNew,
						"decode did not round-trip: deltaDiff=%v bnu=%d n=%d trial=%d packed=%x",
						deltaDiff, bnu, n, trial, packed)
				}
			}
		}
	}
}

// makeWidthValues builds an n-element input whose deltas (or values, in
// non-delta mode) are bounded by bnu bits, anchoring the last delta to maxVal so
// the encoder records exactly bnu in the header.
func makeWidthValues(rng *rand.Rand, n, bnu int, maxVal uint64, deltaDiff bool) []uint64 {
	values := make([]uint64, n)
	if deltaDiff {
		values[0] = rng.Uint64N(1 << 30)
		for i := 1; i < n; i++ {
			var d uint64
			if maxVal != 0 {
				d = rng.Uint64N(maxVal + 1)
			}
			values[i] = values[i-1] + d
		}
		if n > 1 {
			values[n-1] = values[n-2] + maxVal
		}
		return values
	}
	values[0] = rng.Uint64()
	for i := 1; i < n; i++ {
		if maxVal == 0 {
			values[i] = 0
		} else {
			values[i] = rng.Uint64N(maxVal + 1)
		}
	}
	if n > 1 {
		values[n-1] = maxVal
	}
	return values
}

// TestVarIntDecodeShortBufferNoPanic pins the short-buffer guard: reading
// packed[8] requires len >= 9, so the len<9 guard must turn every sub-9-byte
// buffer into a safe no-op for both codecs. (A valid encoding is always >= 9
// bytes, so nothing legitimate is rejected — see
// TestVarIntBitsNeededHeaderHasExpectedValue.)
func TestVarIntDecodeShortBufferNoPanic(t *testing.T) {
	out := make([]uint64, 4)
	for l := 0; l <= 9; l++ {
		buf := make([]byte, l)
		require.NotPanicsf(t, func() { decodeReusable(out, buf, false) }, "len=%d non-delta", l)
		require.NotPanicsf(t, func() { decodeReusable(out, buf, true) }, "len=%d delta", l)
	}
}

// TestVarIntDecodeEmptyOutputNoPanic covers n==0: the deltas[0] write is guarded
// by an n==0 check that precedes it, so an empty output slice is a no-op rather
// than an index-out-of-range panic, even with a full-length packed buffer.
func TestVarIntDecodeEmptyOutputNoPanic(t *testing.T) {
	enc := &VarIntEncoder{}
	enc.Init(4)
	packed := enc.Encode([]uint64{10, 11, 12, 13})

	require.NotPanics(t, func() { decodeReusable([]uint64{}, packed, false) })
	require.NotPanics(t, func() { decodeReusable([]uint64{}, packed, true) })
}

// TestVarIntDecodeBnu59To63MatchesLegacy pins behavior in the unsupported width
// range [59,63]. Both decoders lose high bits there because the byte-refill tail
// overflows the 64-bit buffer (see the maxSupportedBnu comment); this range never
// occurs for BM25 doc-id/TF deltas. The reservoir decoder must stay byte-
// identical to the reference one across the full 6-bit header range. The NotEqual
// assertion documents that the range is genuinely lossy; if a future change makes
// it round-trip, this test will flag that the format/decoder moved.
func TestVarIntDecodeBnu59To63MatchesLegacy(t *testing.T) {
	for bnu := 59; bnu <= 63; bnu++ {
		t.Run(fmt.Sprintf("bnu=%d", bnu), func(t *testing.T) {
			v := uint64(1)<<uint(bnu-1) | 1 // exactly bnu significant bits
			require.Equal(t, bnu, bits.Len64(v))

			values := []uint64{0, v}
			enc := &VarIntEncoder{}
			enc.Init(2)
			packed := enc.Encode(values)
			require.Equal(t, bnu, int((packed[8]>>2)&0x3F), "header should record bnu")

			gotNew := make([]uint64, 2)
			gotOld := make([]uint64, 2)
			decodeReusable(gotNew, packed, false)
			decodeReusableLegacy(gotOld, packed, false)

			require.Equal(t, gotOld, gotNew, "new decoder must match legacy even in the lossy range")
			require.NotEqual(t, v, gotNew[1], "bnu>=59 is known-lossy; a round-trip here means the format/decoder changed")
		})
	}
}
