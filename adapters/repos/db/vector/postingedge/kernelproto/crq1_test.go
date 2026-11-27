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

package kernelproto

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// randomQuery is a query of random values, with constants that make every
// term of the estimate count.
func randomQuery(t testing.TB, rng *rand.Rand, dims int) *Query {
	t.Helper()
	values := make([]uint8, dims)
	for i := range values {
		values[i] = uint8(rng.IntN(maxValue + 1))
	}
	q, err := NewQuery(values, 0.031, 0.8, 0.05, true)
	require.NoError(t, err)
	return q
}

// header packs a header word the way compressionhelpers does: the step as a
// float32, then the squared norm and ⟨μ,x-μ⟩ as bfloat16.
func header(step, norm2, dmu float32) uint64 {
	return uint64(math.Float32bits(step)) |
		uint64(math.Float32bits(norm2)>>16)<<32 |
		uint64(math.Float32bits(dmu)>>16)<<48
}

// randomCodes are codes of random bits behind a header of finite values, and
// the two extremes: every bit clear and every bit set.
func randomCodes(rng *rand.Rand, n, words int) [][]uint64 {
	codes := make([][]uint64, n)
	for c := range codes {
		codes[c] = make([]uint64, words+1)
		codes[c][0] = header(0.01+rng.Float32()/10, 0.5+rng.Float32(), rng.Float32()-0.5)
		for w := 1; w <= words; w++ {
			codes[c][w] = rng.Uint64()
		}
	}
	if n >= 2 {
		clear(codes[0][1:])
		for w := 1; w <= words; w++ {
			codes[1][w] = ^uint64(0)
		}
	}
	return codes
}

// fiveCalls is S the way the index computes it today: one SIMD Hamming
// distance per plane.
func fiveCalls(q *Query, x []uint64) int {
	s := 0
	for p := range q.Planes {
		s += int(compressionhelpers.HammingDistSIMD(q.Planes[p], x)) << p
	}
	return s
}

func TestFusedIsTheSameSAsFiveHammingDistances(t *testing.T) {
	tests := []struct {
		name string
		dims int
	}{
		{name: "one chunk", dims: 128},
		{name: "256 dims", dims: 256},
		{name: "768 dims", dims: 768},
		{name: "1536 dims", dims: 1536},
		{name: "the widest code of the NEON kernel", dims: 64 * chunkWords * maxFusedChunks},
		{name: "one chunk wider, in Go", dims: 64 * chunkWords * (maxFusedChunks + 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(tt.dims), 1))
			q := randomQuery(t, rng, tt.dims)
			for c, code := range randomCodes(rng, 200, tt.dims/64) {
				want := WeightedHamming(q, code[1:])
				require.Equalf(t, want, fiveCalls(q, code[1:]), "code %d", c)
				require.Equalf(t, want, FusedGo(q, code[1:]), "code %d", c)
				require.Equalf(t, want, Fused(q, code[1:]), "code %d", c)
				require.Equalf(t, q.estimate(code[0], want), q.Distance(code), "code %d", c)
			}
		})
	}

	t.Run("the extremes", func(t *testing.T) {
		values := make([]uint8, 768)
		for i := range values {
			values[i] = maxValue
		}
		q, err := NewQuery(values, 1, 1, 0, false)
		require.NoError(t, err)
		codes := randomCodes(rand.New(rand.NewPCG(3, 3)), 2, 12)
		// A query of 31 everywhere against a clear code: every plane differs
		// in every bit. Against a full code nothing differs.
		assert.Equal(t, 768*maxValue, Fused(q, codes[0][1:]))
		assert.Equal(t, 0, Fused(q, codes[1][1:]))
	})
}

func TestScanBlockIsTheSameSAsFiveHammingDistances(t *testing.T) {
	tests := []struct {
		name  string
		dims  int
		codes int
	}{
		{name: "one code of one chunk", dims: 128, codes: 1},
		{name: "a full block of 768 dims", dims: 768, codes: 32},
		{name: "the low half only", dims: 768, codes: 16},
		{name: "one code into the high half", dims: 768, codes: 17},
		{name: "a full block of 1536 dims", dims: 1536, codes: 32},
		{name: "the widest code of FastScan", dims: 2048, codes: 32},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(tt.dims), uint64(tt.codes)))
			q := randomQuery(t, rng, tt.dims)
			codes := randomCodes(rng, tt.codes, tt.dims/64)
			block, err := NewBlock(codes)
			require.NoError(t, err)
			require.Equal(t, tt.codes, block.Len())

			var neon, inGo [BlockCodes]uint16
			ScanBlock(q, block, &neon)
			ScanBlockGo(q, block, &inGo)
			distances := make([]float32, BlockCodes)
			q.BlockDistances(block, distances)
			for c, code := range codes {
				want := WeightedHamming(q, code[1:])
				require.Equalf(t, want, int(neon[c]), "code %d", c)
				require.Equalf(t, want, int(inGo[c]), "code %d", c)
				require.Equalf(t, q.Distance(code), distances[c], "code %d", c)
			}
		})
	}
}

func TestNewQueryRejects(t *testing.T) {
	tests := []struct {
		name   string
		values []uint8
		want   string
	}{
		{name: "no dimensions", values: nil, want: "chunks"},
		{name: "dimensions that do not fill a chunk", values: make([]uint8, 192), want: "chunks"},
		{name: "a value of 6 bits", values: append(make([]uint8, 127), 32), want: "does not fit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewQuery(tt.values, 1, 1, 0, true)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestNewBlockRejects(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 5))
	tests := []struct {
		name  string
		codes [][]uint64
		want  string
	}{
		{name: "no codes", codes: nil, want: "codes for a block"},
		{name: "a code too many", codes: randomCodes(rng, BlockCodes+1, 12), want: "codes for a block"},
		{name: "codes of two widths", codes: append(randomCodes(rng, 3, 12), randomCodes(rng, 1, 24)...), want: "words, the first"},
		{name: "a header and nothing else", codes: [][]uint64{{1}}, want: "do not fit"},
		{name: "codes too wide for 16-bit sums", codes: randomCodes(rng, 2, 36), want: "do not fit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewBlock(tt.codes)
			require.ErrorContains(t, err, tt.want)
		})
	}
}
