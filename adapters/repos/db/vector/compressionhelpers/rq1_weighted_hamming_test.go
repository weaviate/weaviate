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

package compressionhelpers

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// fiveHammingDistances is S the way the 1-bit RQ distancers computed it
// before weightedHamming: one Hamming distance per plane. The reference of the
// tests.
func fiveHammingDistances(c *RQMultiBitCode, x []uint64) int {
	return HammingDist(c.bits0, x) + HammingDist(c.bits1, x)<<1 + HammingDist(c.bits2, x)<<2 +
		HammingDist(c.bits3, x)<<3 + HammingDist(c.bits4, x)<<4
}

func randomWords(rng *rand.Rand, n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = rng.Uint64()
	}
	return out
}

func filledWords(n int, w uint64) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = w
	}
	return out
}

// Every kernel this machine runs gives the S of five Hamming distances, at
// every width: the NEON kernel splits codes at 31 chunks of 128 bits, the
// AVX2 kernel works in chunks of 256, and both finish the last words in Go.
func TestWeightedHammingIsFiveHammingDistances(t *testing.T) {
	widths := []int{0, 1, 2, 3, 4, 5, 7, 8, 12, 24, 61, 62, 63, 64, 124, 125, 126, 130}
	for name, kernel := range weightedHammingKernels() {
		for _, words := range widths {
			t.Run(fmt.Sprintf("%s/%d words", name, words), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(uint64(words), 1))
				cases := []struct {
					name  string
					query RQMultiBitCode
					x     []uint64
				}{
					{
						name: "random",
						query: RQMultiBitCode{
							bits0: randomWords(rng, words), bits1: randomWords(rng, words), bits2: randomWords(rng, words),
							bits3: randomWords(rng, words), bits4: randomWords(rng, words),
						},
						x: randomWords(rng, words),
					},
					{
						name: "every plane set against a clear code",
						query: RQMultiBitCode{
							bits0: filledWords(words, ^uint64(0)), bits1: filledWords(words, ^uint64(0)), bits2: filledWords(words, ^uint64(0)),
							bits3: filledWords(words, ^uint64(0)), bits4: filledWords(words, ^uint64(0)),
						},
						x: filledWords(words, 0),
					},
					{
						name: "every plane clear against a full code",
						query: RQMultiBitCode{
							bits0: filledWords(words, 0), bits1: filledWords(words, 0), bits2: filledWords(words, 0),
							bits3: filledWords(words, 0), bits4: filledWords(words, 0),
						},
						x: filledWords(words, ^uint64(0)),
					},
					{
						name: "a code longer than the query, read up to the query",
						query: RQMultiBitCode{
							bits0: randomWords(rng, words), bits1: randomWords(rng, words), bits2: randomWords(rng, words),
							bits3: randomWords(rng, words), bits4: randomWords(rng, words),
						},
						x: randomWords(rng, words+3),
					},
				}
				for _, tc := range cases {
					q := tc.query
					want := fiveHammingDistances(&q, tc.x[:words])
					got := kernel(q.bits0, q.bits1, q.bits2, q.bits3, q.bits4, tc.x)
					require.Equalf(t, want, got, "%s", tc.name)
				}
			})
		}
	}

	t.Run("the zero query", func(t *testing.T) {
		var q RQMultiBitCode
		require.Zero(t, q.weightedHamming(randomWords(rand.New(rand.NewPCG(2, 2)), 12)))
	})

	t.Run("a code shorter than the query", func(t *testing.T) {
		rng := rand.New(rand.NewPCG(3, 3))
		q := RQMultiBitCode{
			bits0: randomWords(rng, 12), bits1: randomWords(rng, 12), bits2: randomWords(rng, 12),
			bits3: randomWords(rng, 12), bits4: randomWords(rng, 12),
		}
		require.Panics(t, func() { q.weightedHamming(randomWords(rng, 11)) },
			"a short code must not be read past its end")
	})
}

// The distancers give the same distance as when they took five Hamming
// distances, bit for bit: S is the same integer, and 31·D - 2·S is exact in
// float32 at every supported width.
func TestRQ1DistancesAreUnchangedByTheFusedKernel(t *testing.T) {
	providers := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewL2SquaredProvider(),
		distancer.NewDotProductProvider(),
	}
	for _, dims := range []int{8, 64, 100, 256, 300, 511, 512, 768, 1000, 1536} {
		for _, provider := range providers {
			t.Run(fmt.Sprintf("%d dims/%s", dims, provider.Type()), func(t *testing.T) {
				rng := rand.New(rand.NewPCG(uint64(dims), 7))
				vector := func() []float32 {
					v := make([]float32, dims)
					for i := range v {
						v[i] = float32(rng.NormFloat64())
					}
					return v
				}
				mean := vector()
				for i := range mean {
					mean[i] /= 4
				}

				plain, err := NewBinaryRotationalQuantizer(dims, 42, provider)
				require.NoError(t, err)
				centered, err := NewCenteredBinaryRotationalQuantizer(dims, 42, mean, provider)
				require.NoError(t, err)

				for _, query := range [][]float32{vector(), make([]float32, dims)} {
					pd := plain.NewDistancer(query)
					cd := centered.NewDistancer(query)
					for range 20 {
						x := vector()

						code := plain.Encode(x)
						got, err := pd.Distance(code)
						require.NoError(t, err)
						cx := RQOneBitCode(code)
						dot := float32(31*pd.cq.Dimension - 2*fiveHammingDistances(&pd.cq, cx.Bits()))
						want := pd.l2*(cx.SquaredNorm()+pd.cq.SquaredNorm) + pd.cos - (1.0+pd.l2)*(pd.cq.Step*cx.Step()*dot)
						require.Equal(t, want, got, "plain 1-bit RQ")

						ccode := centered.Encode(x)
						got, err = cd.Distance(ccode)
						require.NoError(t, err)
						h := centered.header(ccode)
						var corr float32
						if centered.dotFam != 0 {
							corr = h.dmu + cd.corrQ
						}
						dot = float32(31*cd.cq.Dimension - 2*fiveHammingDistances(&cd.cq, ccode[oneBitFieldWords:]))
						want = centered.l2*(h.norm2+cd.cq.SquaredNorm) + centered.cos - (1.0+centered.l2)*(cd.cq.Step*h.step*dot) - corr
						require.Equal(t, want, got, "centered 1-bit RQ")
					}
				}
			})
		}
	}
}

// BenchmarkRQ1WeightedHamming sets the fused kernel against the five SIMD
// Hamming distances the distancers took before, at the widths of 768 and
// 1536 dimensions, on codes that sit in the L1 cache.
func BenchmarkRQ1WeightedHamming(b *testing.B) {
	for _, dims := range []int{768, 1536} {
		words := dims / 64
		rng := rand.New(rand.NewPCG(uint64(dims), 3))
		q := RQMultiBitCode{
			bits0: randomWords(rng, words), bits1: randomWords(rng, words), bits2: randomWords(rng, words),
			bits3: randomWords(rng, words), bits4: randomWords(rng, words),
		}
		const n = 64
		codes := randomWords(rng, n*words)
		var sink float32
		b.Run(fmt.Sprintf("dims=%d/five calls", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				x := codes[(i%n)*words : (i%n+1)*words]
				sink += HammingDistSIMD(q.bits0, x) + 2*HammingDistSIMD(q.bits1, x) + 4*HammingDistSIMD(q.bits2, x) +
					8*HammingDistSIMD(q.bits3, x) + 16*HammingDistSIMD(q.bits4, x)
			}
		})
		b.Run(fmt.Sprintf("dims=%d/fused", dims), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sink += float32(q.weightedHamming(codes[(i%n)*words : (i%n+1)*words]))
			}
		})
		_ = sink
	}
}
