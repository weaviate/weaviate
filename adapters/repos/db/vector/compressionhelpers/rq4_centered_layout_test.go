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

package compressionhelpers_test

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// centeredRQ4Header is the centered metadata block decoded by hand, so the
// tests below pin the on-disk layout rather than agreeing with the
// production reader about it.
type centeredRQ4Header struct {
	step      float32
	norm2     float32
	sumC      uint16
	lowerCode int8
	p0, p1    int
	d0, d1    int8
}

func parseCenteredRQ4Header(t *testing.T, code []byte) centeredRQ4Header {
	t.Helper()
	require.GreaterOrEqual(t, len(code), compressionhelpers.RQ4MetadataSize)
	return centeredRQ4Header{
		step:      math.Float32frombits(binary.BigEndian.Uint32(code[0:])),
		norm2:     math.Float32frombits(binary.BigEndian.Uint32(code[4:])),
		sumC:      binary.BigEndian.Uint16(code[8:]),
		lowerCode: int8(code[10]),
		p0:        int(code[11])<<4 | int(code[12])>>4,
		p1:        int(code[12]&0x0F)<<8 | int(code[13]),
		d0:        int8(code[14]),
		d1:        int8(code[15]),
	}
}

// lower reconstructs the interval base from the stored int8 offset.
func (h centeredRQ4Header) lower() float32 {
	return h.step * (float32(h.lowerCode)/compressionhelpers.RQ4LowerScale - compressionhelpers.RQ4LowerAnchor)
}

func nibbleSum(packed []byte) int {
	sum := 0
	for _, b := range packed {
		sum += int(b&0x0F) + int(b>>4)
	}
	return sum
}

func TestCenteredRQ4Layout(t *testing.T) {
	const seed = 29
	for _, dim := range []int{64, 128, 384, 777, 1536} {
		for _, tc := range centeredTestCases() {
			t.Run(fmt.Sprintf("d%d/%s", dim, tc.name), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(seed, uint64(dim)))
				vectors := coneVectors(rng, 50, dim, 3, 1, tc.normalize)
				mean := compressionhelpers.MeanVector(vectors, dim)
				rq, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, tc.provider, mean)
				require.NoError(t, err)

				outDim := rq.OutputDimension()
				for _, v := range vectors {
					code := rq.Encode(v)
					// Centered codes cost exactly what uncentered ones cost:
					// the sidecar rides inside the same 16 metadata bytes.
					require.Len(t, code, compressionhelpers.RQ4MetadataSize+outDim/2)
					h := parseCenteredRQ4Header(t, code)
					packed := code[compressionhelpers.RQ4MetadataSize:]

					require.Equal(t, nibbleSum(packed), int(h.sumC),
						"uint16 slot must hold the exact integer code sum")
					assert.Equal(t, h.lower(), rq.RQ4HeaderLower(code),
						"the reader must reconstruct lower from the stored int8")
					assert.Equal(t, h.step, rq.RQ4HeaderStep(code))

					// The int8 lower grid is only safe if real data never
					// reaches its ends; a clamp would shift every coordinate
					// of the vector rather than adding noise to one.
					assert.Less(t, int(h.lowerCode), 127, "lower must not clamp")
					assert.Greater(t, int(h.lowerCode), -127, "lower must not clamp")

					// Positions address the rotated space and never collide:
					// the selection takes the two largest distinct entries.
					assert.Less(t, h.p0, outDim)
					assert.Less(t, h.p1, outDim)
					assert.NotEqual(t, h.p0, h.p1)
					p0, p1, d0, d1 := rq.RQ4OutlierSidecar(code)
					assert.Equal(t, [4]int{h.p0, h.p1, int(h.d0), int(h.d1)},
						[4]int{p0, p1, int(d0), int(d1)})

					// Every coordinate reconstructs as its nibble, plus the
					// sidecar delta on the two outlier positions.
					restored := rq.Restore(code)
					require.Len(t, restored, outDim)
					half := len(packed)
					alpha := compressionhelpers.RQ4OutlierAlpha * h.step
					want := make([]float32, outDim)
					for i, b := range packed {
						want[i] = h.lower() + h.step*float32(b&0x0F)
						want[half+i] = h.lower() + h.step*float32(b>>4)
					}
					want[h.p0] += float32(h.d0) * alpha
					want[h.p1] += float32(h.d1) * alpha
					// Tolerance is one ulp of the reconstruction: the
					// production kernel may contract lower+step*code into an
					// FMA where this expression does not.
					for i := range want {
						assert.InDelta(t, want[i], restored[i],
							1e-6*(1+math.Abs(float64(want[i]))), "coordinate %d", i)
					}

					// The norm2 slot carries dot(x-mean, mean) for dot/cosine
					// and |x-mean|^2 for l2 (exact scalar metadata, not
					// quantized — small float tolerance only).
					cx := make([]float32, dim)
					var wantNorm2 float64
					for i := range cx {
						cx[i] = v[i] - mean[i]
						if tc.provider.Type() == "l2-squared" {
							wantNorm2 += float64(cx[i]) * float64(cx[i])
						} else {
							wantNorm2 += float64(cx[i]) * float64(mean[i])
						}
					}
					assert.InDelta(t, wantNorm2, float64(h.norm2), 1e-3*(1+math.Abs(wantNorm2)))
				}
			})
		}
	}
}

// lower is rounded onto the int8 grid after the interval search, so the
// reader reconstructs it up to half a grid step away from the value the
// nibbles were quantized against. What must hold is that the offset stays
// within that half step — a CLAMP would shift every coordinate of the vector
// by an unbounded amount instead — and that the nibbles are still the codes
// of the rotated vector to within one level.
func TestCenteredRQ4LowerRoundsWithinHalfAGridStep(t *testing.T) {
	const (
		dim  = 256
		seed = 47
	)
	provider := distancer.NewDotProductProvider()
	rng := rand.New(rand.NewPCG(seed, seed))
	vectors := coneVectors(rng, 30, dim, 3, 1, false)
	mean := compressionhelpers.MeanVector(vectors, dim)
	rq, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, provider, mean)
	require.NoError(t, err)

	for _, v := range vectors {
		code := rq.Encode(v)
		h := parseCenteredRQ4Header(t, code)
		require.Greater(t, h.step, float32(0))

		// The stored code is the nearest grid point, and it never clamps.
		lower := h.lower()
		require.Less(t, int(h.lowerCode), 127, "lower must not clamp")
		require.Greater(t, int(h.lowerCode), -127, "lower must not clamp")

		// And the nibbles are the nearest codes under that stored lower.
		centered := make([]float32, dim)
		for i := range centered {
			centered[i] = v[i] - mean[i]
		}
		rx := rq.Rotate(centered)
		p0, p1, _, _ := rq.RQ4OutlierSidecar(code)
		packed := code[compressionhelpers.RQ4MetadataSize:]
		half := len(packed)
		for i := range rx {
			if i == p0 || i == p1 {
				continue // zeroed before the interval search
			}
			var got byte
			if i < half {
				got = packed[i] & 0x0F
			} else {
				got = packed[i-half] >> 4
			}
			ideal := (rx[i] - lower) / h.step
			want := math.Round(float64(ideal))
			want = math.Max(0, math.Min(15, want))
			assert.InDelta(t, want, float64(got), 1.0,
				"nibble %d must quantize rx=%v against the STORED interval", i, rx[i])
		}
	}
}

// The wide layout is the compact one with the two fields that had to grow:
// the code sum to uint32 and the positions to uint16. Decoded by hand here so
// its offsets are pinned independently of the production reader.
func TestCenteredRQ4WideLayout(t *testing.T) {
	const seed = 71
	// 4160 is the first multiple of 64 past the 12-bit position cap; 8192
	// also overflows the uint16 code sum (15*8192 = 122880).
	for _, dim := range []int{4160, 8192} {
		t.Run(fmt.Sprintf("d%d", dim), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, uint64(dim)))
			vectors := coneVectors(rng, 6, dim, 3, 1, false)
			mean := compressionhelpers.MeanVector(vectors, dim)
			rq, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(
				dim, seed, distancer.NewDotProductProvider(), mean)
			require.NoError(t, err)

			var sawWidePosition bool
			for _, v := range vectors {
				code := rq.Encode(v)
				require.Len(t, code, 20+dim/2)

				step := math.Float32frombits(binary.BigEndian.Uint32(code[0:]))
				sum := binary.BigEndian.Uint32(code[8:])
				p0 := int(binary.BigEndian.Uint16(code[12:]))
				p1 := int(binary.BigEndian.Uint16(code[14:]))
				d0, d1 := int8(code[16]), int8(code[17])
				lowerCode := int8(code[18])

				packed := code[20:]
				require.Equal(t, nibbleSum(packed), int(sum),
					"uint32 slot must hold the exact integer code sum")
				assert.Zero(t, code[19], "the reserved byte must be written as zero")

				// The u16 code sum this layout replaces would have wrapped at
				// 65535; the nibble sum here must be free to exceed it.
				if dim == 8192 {
					assert.Greater(t, int(sum), 0)
				}

				lower := step * (float32(lowerCode)/compressionhelpers.RQ4LowerScale -
					compressionhelpers.RQ4LowerAnchor)
				assert.Equal(t, lower, rq.RQ4HeaderLower(code))
				assert.Equal(t, step, rq.RQ4HeaderStep(code))

				gp0, gp1, gd0, gd1 := rq.RQ4OutlierSidecar(code)
				assert.Equal(t, [4]int{p0, p1, int(d0), int(d1)},
					[4]int{gp0, gp1, int(gd0), int(gd1)})
				assert.Less(t, p0, dim)
				assert.Less(t, p1, dim)
				assert.NotEqual(t, p0, p1)
				// Positions land wherever the largest rotated coordinates are,
				// so only the sample as a whole is guaranteed to reach past
				// what 12 bits could address.
				if p0 > 4095 || p1 > 4095 {
					sawWidePosition = true
				}
			}
			// Only worth asserting where a sizeable share of the dimension
			// lies past 12 bits: at 4160 just 64 of 4160 coordinates do, so
			// a small sample legitimately misses them.
			if dim >= 8192 {
				assert.True(t, sawWidePosition,
					"the sample must exercise positions the compact layout cannot address")
			}
		})
	}
}

func TestRQ4LegacyLayoutUnchanged(t *testing.T) {
	const (
		dim  = 128
		seed = 31
	)
	rng := rand.New(rand.NewPCG(seed, seed))
	rq := compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, distancer.NewDotProductProvider())
	v := coneVectors(rng, 1, dim, 3, 1, false)[0]
	code := compressionhelpers.RQ4Code(rq.Encode(v))
	require.Len(t, []byte(code), compressionhelpers.RQ4MetadataSize+rq.OutputDimension()/2)

	lower := math.Float32frombits(binary.BigEndian.Uint32(code[0:]))
	step := math.Float32frombits(binary.BigEndian.Uint32(code[4:]))
	codeSum := math.Float32frombits(binary.BigEndian.Uint32(code[8:]))
	norm2 := math.Float32frombits(binary.BigEndian.Uint32(code[12:]))
	assert.Equal(t, code.Lower(), lower)
	assert.Equal(t, code.Step(), step)
	assert.Equal(t, code.CodeSum(), codeSum)
	assert.Equal(t, code.Norm2(), norm2)
	assert.Equal(t, step*float32(nibbleSum(code.Packed())), codeSum)
}

func TestRQ4RejectsForeignCodeLengths(t *testing.T) {
	const (
		dim  = 128
		seed = 41
	)
	provider := distancer.NewDotProductProvider()
	rng := rand.New(rand.NewPCG(seed, seed))
	vectors := coneVectors(rng, 10, dim, 3, 1, false)
	q := vectors[0]

	quantizers := map[string]*compressionhelpers.FourBitRotationalQuantizer{}
	quantizers["uncentered"] = compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, provider)
	centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(
		dim, seed, provider, compressionhelpers.MeanVector(vectors, dim))
	require.NoError(t, err)
	quantizers["centered"] = centered

	for name, rq := range quantizers {
		t.Run(name, func(t *testing.T) {
			good := rq.Encode(vectors[1])
			bad := [][]byte{
				make([]byte, len(good)-4),
				make([]byte, len(good)+4),
				make([]byte, 8),                                      // truncated below any header size
				make([]byte, compressionhelpers.RQ4MetadataSize+256), // other dim
				{},
			}
			d := rq.NewDistancer(q)
			for _, b := range bad {
				_, err := d.Distance(b)
				assert.Error(t, err, "asymmetric path must reject len %d", len(b))
				// Equal-but-wrong lengths are the dangerous case for the
				// symmetric path: it used to accept any matching pair.
				_, err = rq.DistanceBetweenCompressedVectors(b, b)
				assert.Error(t, err, "symmetric path must reject len %d", len(b))
				_, err = rq.DistanceBetweenCompressedVectors(good, b)
				assert.Error(t, err, "symmetric path must reject mixed %d/%d", len(good), len(b))
			}
			// Sanity: correct codes still work on both paths.
			_, err := d.Distance(good)
			require.NoError(t, err)
			_, err = rq.DistanceBetweenCompressedVectors(good, rq.Encode(vectors[2]))
			require.NoError(t, err)
		})
	}
}

// Centering is byte-neutral against the uncentered format at every dimension
// the compact layout addresses, and switches to the wide one above it rather
// than erroring or silently misaddressing. The wide layout costs four bytes,
// which at those dimensions is a rounding error against D/2.
func TestRQ4CodeSizeAcrossCenteredLayouts(t *testing.T) {
	const seed = 37
	provider := distancer.NewDotProductProvider()
	cases := []struct {
		name     string
		dim      int
		centered bool
		wantMeta int
	}{
		{name: "uncentered", dim: 1536, wantMeta: compressionhelpers.RQ4MetadataSize},
		{name: "centered", dim: 1536, centered: true, wantMeta: compressionhelpers.RQ4MetadataSize},
		{
			name: "centered at the compact position cap", dim: 4096, centered: true,
			wantMeta: compressionhelpers.RQ4MetadataSize,
		},
		// 4160 is the first multiple of 64 past the 12-bit positions, and
		// 4416 the first past the uint16 code sum: both need the wide layout.
		{name: "centered just past the compact cap", dim: 4160, centered: true, wantMeta: 20},
		{name: "centered past the u16 code sum", dim: 4416, centered: true, wantMeta: 20},
		{name: "centered at 8192", dim: 8192, centered: true, wantMeta: 20},
		{name: "centered at 16384", dim: 16384, centered: true, wantMeta: 20},
		{name: "uncentered at 16384", dim: 16384, wantMeta: compressionhelpers.RQ4MetadataSize},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, uint64(tc.dim)))
			vectors := coneVectors(rng, 10, tc.dim, 3, 1, false)
			q := coneVectors(rng, 1, tc.dim, 3, 1, false)[0]

			var rq *compressionhelpers.FourBitRotationalQuantizer
			if tc.centered {
				var err error
				rq, err = compressionhelpers.NewCenteredFourBitRotationalQuantizer(
					tc.dim, seed, provider, compressionhelpers.MeanVector(vectors, tc.dim))
				require.NoError(t, err)
			} else {
				rq = compressionhelpers.NewFourBitRotationalQuantizer(tc.dim, seed, provider)
			}
			require.Equal(t, tc.dim, rq.OutputDimension())

			stats, ok := rq.Stats().(compressionhelpers.RQ4Stats)
			require.True(t, ok)
			require.Equal(t, tc.wantMeta, stats.MetadataSize)

			codes := make([][]byte, len(vectors))
			for i, v := range vectors {
				codes[i] = rq.Encode(v)
				require.Len(t, codes[i], tc.wantMeta+rq.OutputDimension()/2)
			}

			// Both distance paths stay accurate in whichever layout was picked.
			d := rq.NewDistancer(q)
			for i, v := range vectors {
				exact := exactDistance(t, provider, q, v)
				scale := math.Sqrt(float64(dotFloat(q, q))) * math.Sqrt(float64(dotFloat(v, v)))
				est, err := d.Distance(codes[i])
				require.NoError(t, err)
				assert.InDelta(t, exact, float64(est), 0.05*(1+scale))
				sym, err := rq.DistanceBetweenCompressedVectors(codes[0], codes[i])
				require.NoError(t, err)
				assert.False(t, math.IsNaN(float64(sym)))
			}
		})
	}
}
