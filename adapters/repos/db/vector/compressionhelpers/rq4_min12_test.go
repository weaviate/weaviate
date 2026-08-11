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

type packedRQ4Header struct {
	step  float32
	sumC  uint16
	lower float32
	norm2 float32
}

func parsePackedRQ4Header(t *testing.T, code []byte) packedRQ4Header {
	t.Helper()
	require.GreaterOrEqual(t, len(code), compressionhelpers.RQ4PackedMetadataSize)
	return packedRQ4Header{
		step:  math.Float32frombits(binary.BigEndian.Uint32(code[0:])),
		sumC:  binary.BigEndian.Uint16(code[4:]),
		lower: math.Float32frombits(uint32(binary.BigEndian.Uint16(code[6:])) << 16),
		norm2: math.Float32frombits(binary.BigEndian.Uint32(code[8:])),
	}
}

func nibbleSum(packed []byte) int {
	sum := 0
	for _, b := range packed {
		sum += int(b&0x0F) + int(b>>4)
	}
	return sum
}

func TestCenteredRQ4PackedLayout(t *testing.T) {
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
					require.Len(t, code, compressionhelpers.RQ4PackedMetadataSize+outDim/2)
					h := parsePackedRQ4Header(t, code)
					packed := code[compressionhelpers.RQ4PackedMetadataSize:]

					require.Equal(t, nibbleSum(packed), int(h.sumC),
						"uint16 slot must hold the exact integer code sum")

					restored := rq.Restore(code)
					require.Len(t, restored, outDim)
					half := len(packed)
					for i, b := range packed {
						assert.Equal(t, h.lower+h.step*float32(b&0x0F), restored[i])
						assert.Equal(t, h.lower+h.step*float32(b>>4), restored[half+i])
					}

					// The norm2 slot carries dot(x-mean, mean) for dot/cosine
					// and |x-mean|^2 for l2 (exact scalar metadata, not
					// quantized — small float tolerance only).
					cx := make([]float32, dim)
					var want float64
					for i := range cx {
						cx[i] = v[i] - mean[i]
						if tc.provider.Type() == "l2-squared" {
							want += float64(cx[i]) * float64(cx[i])
						} else {
							want += float64(cx[i]) * float64(mean[i])
						}
					}
					assert.InDelta(t, want, float64(h.norm2), 1e-3*(1+math.Abs(want)))
				}
			})
		}
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
				make([]byte, len(good)-4), // other header layout, same dim
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

func TestRQ4MetadataSizeSelection(t *testing.T) {
	const seed = 37
	provider := distancer.NewDotProductProvider()
	cases := []struct {
		name     string
		dim      int
		centered bool
		wantMeta int
	}{
		{"uncentered", 1536, false, compressionhelpers.RQ4MetadataSize},
		{"centered", 1536, true, compressionhelpers.RQ4PackedMetadataSize},
		// 4352*15 = 65280 fits uint16; the next multiple of 64 (4416) does not.
		{"centered at boundary", 4352, true, compressionhelpers.RQ4PackedMetadataSize},
		{"centered above boundary", 4416, true, compressionhelpers.RQ4MetadataSize},
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
