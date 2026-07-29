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
	"fmt"
	"math"
	"math/bits"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The encoded format stores bitsNeeded in a 6-bit header field, so the largest
// per-value bit width it can name is 63. The current decoder also loses bits
// for bnu >= 59 because its byte-refill loop overflows the 64-bit buffer. The
// tests below therefore cover bnu in [1, 58], which is the safely supported
// range. Realistic delta/TF values are far below this ceiling.
const maxSupportedBnu = 58

// roundtripSizes covers small, boundary, and block-aligned sizes plus a few
// large counts to exercise the fast and tail paths in decodeReusable.
var roundtripSizes = []int{1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 200, 511, 512}

func TestVarIntRoundtripFixedWidth(t *testing.T) {
	// For each bit width in [1, maxSupportedBnu], generate values that exactly
	// fit that width (including the boundary value 2^bnu - 1) and verify a
	// full encode/decode roundtrip.
	rng := rand.New(rand.NewPCG(0xdeadbeef, 0x12345678))

	for bnu := 1; bnu <= maxSupportedBnu; bnu++ {
		maxVal := uint64(1)<<uint(bnu) - 1
		for _, n := range roundtripSizes {
			t.Run(fmt.Sprintf("bnu=%d_n=%d", bnu, n), func(t *testing.T) {
				values := make([]uint64, n)
				values[0] = rng.Uint64() // first value is stored verbatim, can be anything
				for i := 1; i < n; i++ {
					if maxVal == 0 {
						values[i] = 0
					} else {
						values[i] = rng.Uint64N(maxVal + 1)
					}
				}
				// Force at least one boundary value so bitsNeeded is exactly bnu.
				if n > 1 {
					values[n-1] = maxVal
				}

				enc := &VarIntEncoder{}
				enc.Init(n)
				packed := enc.Encode(values)
				decoded := enc.Decode(packed)
				assert.Equal(t, values, decoded[:n])
			})
		}
	}
}

func TestVarIntDeltaRoundtripFixedWidth(t *testing.T) {
	// Same shape, but values are a non-decreasing cumulative sum so the deltas
	// have a known bit width.
	rng := rand.New(rand.NewPCG(0xc0ffee, 0xfeedface))

	for bnu := 1; bnu <= maxSupportedBnu; bnu++ {
		// Cap per-delta size by bnu and the count to keep the cumulative sum
		// within uint64.
		maxDelta := uint64(1)<<uint(bnu) - 1
		for _, n := range roundtripSizes {
			// Guard against overflow when n*maxDelta would wrap.
			if maxDelta != 0 && uint64(n) > ^uint64(0)/maxDelta {
				continue
			}
			t.Run(fmt.Sprintf("bnu=%d_n=%d", bnu, n), func(t *testing.T) {
				values := make([]uint64, n)
				values[0] = rng.Uint64N(1 << 30)
				for i := 1; i < n; i++ {
					var d uint64
					if maxDelta == 0 {
						d = 0
					} else {
						d = rng.Uint64N(maxDelta + 1)
					}
					values[i] = values[i-1] + d
				}
				if n > 1 {
					// Force one boundary delta so bitsNeeded = bnu.
					values[n-1] = values[n-2] + maxDelta
				}

				enc := &VarIntDeltaEncoder{}
				enc.Init(n)
				packed := enc.Encode(values)
				decoded := enc.Decode(packed)
				assert.Equal(t, values, decoded[:n])
			})
		}
	}
}

func TestVarIntEdgeCases(t *testing.T) {
	t.Run("single value", func(t *testing.T) {
		// Only the first value, no deltas; should roundtrip unchanged.
		for _, v := range []uint64{0, 1, 42, math.MaxUint32, math.MaxUint64} {
			enc := &VarIntEncoder{}
			enc.Init(1)
			packed := enc.Encode([]uint64{v})
			decoded := enc.Decode(packed)
			assert.Equal(t, []uint64{v}, decoded[:1])
		}
	})

	t.Run("all zero deltas", func(t *testing.T) {
		// Non-delta encoder: all-zero values means bitsNeeded=1 (encoder enforces a
		// minimum of 1 bit).
		n := 128
		values := make([]uint64, n)
		// First value can be anything; the rest are zero.
		values[0] = 1234

		enc := &VarIntEncoder{}
		enc.Init(n)
		packed := enc.Encode(values)
		decoded := enc.Decode(packed)
		assert.Equal(t, values, decoded[:n])
	})

	t.Run("delta encoder constant value", func(t *testing.T) {
		// All values equal → all deltas zero → bitsNeeded=1.
		n := 128
		values := make([]uint64, n)
		for i := range values {
			values[i] = 42
		}

		enc := &VarIntDeltaEncoder{}
		enc.Init(n)
		packed := enc.Encode(values)
		decoded := enc.Decode(packed)
		assert.Equal(t, values, decoded[:n])
	})

	t.Run("delta encoder strictly increasing by 1", func(t *testing.T) {
		// The bnu=1 case in the fast reservoir path: maximum loads-per-refill.
		n := 128
		values := make([]uint64, n)
		values[0] = 1000
		for i := 1; i < n; i++ {
			values[i] = values[i-1] + 1
		}

		enc := &VarIntDeltaEncoder{}
		enc.Init(n)
		packed := enc.Encode(values)
		decoded := enc.Decode(packed)
		assert.Equal(t, values, decoded[:n])
	})

	t.Run("delta encoder max representable deltas", func(t *testing.T) {
		// Deltas at the boundary of the supported bit width.
		n := 64
		maxDelta := uint64(1)<<maxSupportedBnu - 1
		values := make([]uint64, n)
		values[0] = 0
		for i := 1; i < n; i++ {
			values[i] = values[i-1] + maxDelta
		}

		enc := &VarIntDeltaEncoder{}
		enc.Init(n)
		packed := enc.Encode(values)
		decoded := enc.Decode(packed)
		assert.Equal(t, values, decoded[:n])
	})
}

func TestVarIntFastAndTailPathCoverage(t *testing.T) {
	// Force the decoder through both the reservoir fast path and the
	// byte-by-byte tail by choosing block sizes such that the 32-bit refill
	// runs out of room before processing every value.
	//
	// For bnu=8 and n=128, the encoded payload past the header is 127*8=1016
	// bits = 127 bytes. Reservoir refills 4 bytes at a time, so the last few
	// values are guaranteed to land in the tail. The roundtrip must still
	// succeed.
	for _, bnu := range []int{1, 2, 4, 8, 16, 24, 32} {
		t.Run(fmt.Sprintf("reservoir_bnu=%d", bnu), func(t *testing.T) {
			n := 128
			maxVal := uint64(1)<<uint(bnu) - 1
			rng := rand.New(rand.NewPCG(uint64(bnu), 0))
			values := make([]uint64, n)
			values[0] = rng.Uint64()
			for i := 1; i < n; i++ {
				values[i] = rng.Uint64N(maxVal + 1)
			}
			values[n-1] = maxVal // anchor bitsNeeded

			enc := &VarIntEncoder{}
			enc.Init(n)
			packed := enc.Encode(values)
			decoded := enc.Decode(packed)
			require.Equal(t, values, decoded[:n])
		})
	}

	// Same exercise for the uint64-load path (bnu in (32, 57]).
	for _, bnu := range []int{33, 40, 48, 56} {
		t.Run(fmt.Sprintf("uint64_load_bnu=%d", bnu), func(t *testing.T) {
			n := 64
			maxVal := uint64(1)<<uint(bnu) - 1
			rng := rand.New(rand.NewPCG(uint64(bnu), 1))
			values := make([]uint64, n)
			values[0] = rng.Uint64()
			for i := 1; i < n; i++ {
				values[i] = rng.Uint64N(maxVal + 1)
			}
			values[n-1] = maxVal

			enc := &VarIntEncoder{}
			enc.Init(n)
			packed := enc.Encode(values)
			decoded := enc.Decode(packed)
			require.Equal(t, values, decoded[:n])
		})
	}
}

func TestDecodeReusableReusesOutputBuffer(t *testing.T) {
	// The Reusable variants are expected to write into a caller-provided slice
	// without allocating. Verify two consecutive calls with different inputs
	// produce the correct outputs in the same buffer.
	enc := VarIntEncoder{}
	enc1 := &VarIntEncoder{}
	enc1.Init(32)

	v1 := make([]uint64, 32)
	v1[0] = 100
	for i := 1; i < 32; i++ {
		v1[i] = uint64(i * 3)
	}
	p1 := enc1.Encode(v1)

	v2 := make([]uint64, 32)
	v2[0] = 999_999
	for i := 1; i < 32; i++ {
		v2[i] = uint64(i)
	}
	p2 := enc1.Encode(v2)

	out := make([]uint64, 32)
	enc.DecodeReusable(p1, out)
	assert.Equal(t, v1, out)
	enc.DecodeReusable(p2, out)
	assert.Equal(t, v2, out)
}

func TestVarIntBitsNeededHeaderHasExpectedValue(t *testing.T) {
	// Confirm that the encoder writes the bitsNeeded value we expect for a
	// known input, and that the decoder reads it back identically. This pins
	// down the on-disk format so future format changes can't silently break
	// readers in other segments.
	for _, tc := range []struct {
		name      string
		values    []uint64
		expectBnu int
	}{
		{"all_zero_deltas", []uint64{0, 0, 0, 0, 0, 0, 0, 0}, 1},
		{"max_delta_3_bits", []uint64{0, 5, 7, 3, 0, 2}, 3},
		{"max_delta_8_bits", []uint64{0, 255, 100, 200}, 8},
		{"max_delta_16_bits", []uint64{0, 65535, 32000}, 16},
		{"max_delta_32_bits", []uint64{0, math.MaxUint32, 1234}, 32},
		{"max_delta_56_bits", []uint64{0, 1<<56 - 1, 0}, 56},
	} {
		t.Run(tc.name, func(t *testing.T) {
			enc := &VarIntEncoder{}
			enc.Init(len(tc.values))
			packed := enc.Encode(tc.values)

			// Header is the high 6 bits of packed[8].
			require.GreaterOrEqual(t, len(packed), 9)
			gotBnu := int((packed[8] >> 2) & 0x3F)
			if tc.expectBnu == 0 {
				// Encoder enforces a minimum of 1 bit even for all-zero deltas.
				assert.Equal(t, 1, gotBnu)
			} else {
				assert.Equal(t, tc.expectBnu, gotBnu)
			}

			decoded := enc.Decode(packed)
			assert.Equal(t, tc.values, decoded[:len(tc.values)])
		})
	}
}

// TestVarIntLargeBitWidthLimitation documents a known limitation in the
// encoded format: bitsNeeded values above 58 are not safely supported.
//   - The 6-bit header field can name at most 63 (so bnu=64 silently truncates
//     to 0 and the decoder returns nothing).
//   - For bnu in [59, 63] the decoder's byte-refill loop shifts a uint64 buffer
//     left by 8 when it already holds >= 56 bits, losing the oldest 2 bits.
//
// In practice deltas requiring >= 59 bits never occur in the BM25 inverted
// index use case (doc IDs and term frequencies fit comfortably below 2^40),
// so this is documented rather than fixed. If a future caller starts feeding
// values that need >= 59 bits, the format itself needs to change (wider
// header, different refill strategy).
func TestVarIntLargeBitWidthLimitation(t *testing.T) {
	t.Run("bnu_64_truncated_to_zero", func(t *testing.T) {
		// A delta requiring 64 bits would have bitsNeeded=64, which doesn't
		// fit in the 6-bit header.
		require.Equal(t, 64, bits.Len64(math.MaxUint64))

		values := []uint64{0, math.MaxUint64}
		enc := &VarIntEncoder{}
		enc.Init(2)
		packed := enc.Encode(values)

		// The encoder wrote 64 in 6 bits, which is 0.
		require.GreaterOrEqual(t, len(packed), 9)
		gotBnu := int((packed[8] >> 2) & 0x3F)
		assert.Equal(t, 0, gotBnu, "bnu=64 is truncated to 0 in the header")

		// The decoder treats bnu=0 as invalid and returns without touching the
		// remaining slots — only the first value comes through correctly.
		decoded := enc.Decode(packed)
		assert.Equal(t, uint64(0), decoded[0])
		assert.Equal(t, uint64(0), decoded[1])
	})
}
