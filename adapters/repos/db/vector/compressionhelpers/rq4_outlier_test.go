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
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// newOutlierQuantizer builds the centered quantizer over the mean of the
// given vectors. The sidecar is intrinsic to the centered layout; the A/B
// baseline below is the same quantizer's EncodeWithoutSidecar, which skips
// the outlier selection and leaves a zero correction.
func newOutlierQuantizer(t *testing.T, dim int, seed uint64, p distancer.Provider, vectors [][]float32) *compressionhelpers.FourBitRotationalQuantizer {
	t.Helper()
	rq, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(
		dim, seed, p, compressionhelpers.MeanVector(vectors, dim))
	require.NoError(t, err)
	return rq
}

// tailVectors are cone vectors with a few coordinates blown up per vector,
// the heavy-per-vector-tail regime the sidecar targets.
func tailVectors(rng *rand.Rand, n, dim int, normalize bool) [][]float32 {
	out := coneVectors(rng, n, dim, 3, 1, false)
	for _, v := range out {
		for k := 0; k < 3; k++ {
			v[rng.IntN(dim)] *= float32(6 + rng.Float64()*10)
		}
		if normalize {
			var norm float64
			for _, x := range v {
				norm += float64(x) * float64(x)
			}
			if norm > 0 {
				inv := float32(1 / math.Sqrt(norm))
				for i := range v {
					v[i] *= inv
				}
			}
		}
	}
	return out
}

// The sidecar stores exactly the two largest-magnitude rotated coordinates,
// and Restore reconstructs them far better than the 4-bit grid does.
func TestRQ4OutlierSelectsAndRestoresTopTwo(t *testing.T) {
	const (
		dim  = 256
		n    = 40
		seed = 53
	)
	for _, tc := range centeredTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))
			vectors := tailVectors(rng, n, dim, tc.normalize)
			rq := newOutlierQuantizer(t, dim, seed, tc.provider, vectors)
			mean := compressionhelpers.MeanVector(vectors, dim)

			var bulkErr, baseBulkErr float64
			var bulkN int
			for _, v := range vectors {
				centered := make([]float32, dim)
				for i := range centered {
					centered[i] = v[i] - mean[i]
				}
				rx := rq.Rotate(centered)

				// Expected top-2 by magnitude, ties toward the lower index.
				want0, want1 := 0, 1
				if math.Abs(float64(rx[1])) > math.Abs(float64(rx[0])) {
					want0, want1 = 1, 0
				}
				for i := 2; i < len(rx); i++ {
					a := math.Abs(float64(rx[i]))
					switch {
					case a > math.Abs(float64(rx[want0])):
						want0, want1 = i, want0
					case a > math.Abs(float64(rx[want1])):
						want1 = i
					}
				}

				code := rq.Encode(v)
				p0, p1, _, _ := rq.RQ4OutlierSidecar(code)
				require.Equal(t, want0, p0)
				require.Equal(t, want1, p1)

				// The stored delta puts the outliers on a 0.25*step grid, so
				// their reconstruction error is a small fraction of a step,
				// against up to half a step (usually much more, since they
				// were the coordinates stretching the interval) without it.
				restored := rq.Restore(code)
				step := rq.RQ4HeaderStep(code)
				for _, p := range []int{p0, p1} {
					assert.LessOrEqual(t, math.Abs(float64(restored[p]-rx[p])),
						float64(compressionhelpers.RQ4OutlierAlpha*step)/2+1e-4,
						"outlier reconstruction must land on the alpha*step grid")
				}

				// Zeroing the outliers tightens the interval, so the bulk
				// coordinates — everything the nibbles still carry — are
				// reconstructed more accurately than without the sidecar.
				baseRestored := rq.Restore(rq.EncodeWithoutSidecar(v))
				for i := range rx {
					if i == p0 || i == p1 {
						continue
					}
					bulkErr += math.Abs(float64(restored[i] - rx[i]))
					baseBulkErr += math.Abs(float64(baseRestored[i] - rx[i]))
					bulkN++
				}
			}
			require.NotZero(t, bulkN)
			assert.Less(t, bulkErr, baseBulkErr,
				"zeroing the outliers must lower the reconstruction error of the bulk coordinates")
		})
	}
}

// Removing the outliers from the nibble stream tightens the interval, and the
// sidecar correction restores them at query time: the estimator error must
// drop on tail-heavy data, and the asymmetric and symmetric paths must agree.
func TestRQ4OutlierEstimatorImprovesAndPathsAgree(t *testing.T) {
	const (
		dim     = 256
		n       = 200
		queries = 20
		seed    = 57
	)
	for _, tc := range centeredTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))
			vectors := tailVectors(rng, n, dim, tc.normalize)
			qs := tailVectors(rng, queries, dim, tc.normalize)
			rq := newOutlierQuantizer(t, dim, seed, tc.provider, vectors)

			baseCodes := make([][]byte, n)
			codes := make([][]byte, n)
			for i, v := range vectors {
				baseCodes[i] = rq.EncodeWithoutSidecar(v)
				codes[i] = rq.Encode(v)
			}

			var baseErr, sidecarErr float64
			for _, q := range qs {
				baseDist := rq.NewDistancer(q)
				dist := rq.NewDistancer(q)
				for i, v := range vectors {
					exact := exactDistance(t, tc.provider, q, v)
					got, err := baseDist.Distance(baseCodes[i])
					require.NoError(t, err)
					baseErr += math.Abs(float64(got - exact))
					got, err = dist.Distance(codes[i])
					require.NoError(t, err)
					sidecarErr += math.Abs(float64(got - exact))

					// The scalar fallback must apply the same correction.
					scalar, err := dist.DistanceScalar(codes[i])
					require.NoError(t, err)
					assert.InDelta(t, got, scalar, 1e-3*(1+math.Abs(float64(got))))
				}
			}
			assert.Less(t, sidecarErr, baseErr,
				"the sidecar must reduce the estimator error on tail-heavy data")

			// Asymmetric and symmetric estimates describe the same geometry;
			// they differ only by the query's own quantization.
			for i := 0; i < 20; i++ {
				x, y := vectors[i], vectors[(i*7+3)%n]
				dist := rq.NewDistancer(x)
				asym, err := dist.Distance(codes[(i*7+3)%n])
				require.NoError(t, err)
				sym, err := rq.DistanceBetweenCompressedVectors(codes[i], codes[(i*7+3)%n])
				require.NoError(t, err)
				exact := exactDistance(t, tc.provider, x, y)
				scale := 1 + math.Abs(float64(exact))
				assert.InDelta(t, asym, sym, 0.25*scale,
					"asymmetric and symmetric estimates must agree")
			}
		})
	}
}

// The two-sided correction (including its collision term) must make the
// compressed-compressed dot estimate exactly the dot product of the two
// reconstructions — the same identity the sidecar-free path satisfies. This
// is what pins the collision term: without it, codes sharing a sidecar
// position drift from the identity by delta_x*delta_y.
func TestRQ4OutlierSymmetricEstimateMatchesReconstructions(t *testing.T) {
	const (
		dim  = 128
		n    = 60
		seed = 59
	)
	for _, tc := range centeredTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))
			vectors := tailVectors(rng, n, dim, tc.normalize)
			rq := newOutlierQuantizer(t, dim, seed, tc.provider, vectors)

			codes := make([][]byte, n)
			restored := make([][]float32, n)
			for i, v := range vectors {
				codes[i] = rq.Encode(v)
				restored[i] = rq.Restore(codes[i])
			}

			check := func(i, j int) {
				t.Helper()
				got := rq.RQ4SymmetricDotEstimate(codes[i], codes[j])
				var want float64
				for k := range restored[i] {
					want += float64(restored[i][k]) * float64(restored[j][k])
				}
				assert.InDelta(t, want, float64(got), 1e-3*(1+math.Abs(want)),
					"symmetric estimate must equal dot(Restore(x), Restore(y)) for pair %d/%d", i, j)
			}

			// Full collision: a code against itself shares both positions.
			for i := range codes {
				check(i, i)
			}

			partial, disjoint := 0, 0
			for i := range codes {
				pi0, pi1, _, _ := rq.RQ4OutlierSidecar(codes[i])
				for j := i + 1; j < len(codes); j++ {
					pj0, pj1, _, _ := rq.RQ4OutlierSidecar(codes[j])
					if pi0 == pj0 || pi0 == pj1 || pi1 == pj0 || pi1 == pj1 {
						partial++
					} else {
						disjoint++
					}
					check(i, j)
				}
			}
			require.NotZero(t, disjoint, "the sample must contain non-colliding pairs")
			t.Logf("%d partially colliding pairs, %d disjoint pairs", partial, disjoint)
		})
	}
}

// A forced full collision on a single position: two vectors whose largest
// rotated coordinate is the same index must still satisfy the identity, and
// the collision term must be the thing that makes it hold.
func TestRQ4OutlierSymmetricCollisionTermIsRequired(t *testing.T) {
	const (
		dim  = 128
		seed = 60
	)
	rng := rand.New(rand.NewPCG(seed, seed))
	vectors := tailVectors(rng, 40, dim, false)
	rq := newOutlierQuantizer(t, dim, seed, distancer.NewDotProductProvider(), vectors)

	code := rq.Encode(vectors[0])
	p0, p1, d0, d1 := rq.RQ4OutlierSidecar(code)
	step := rq.RQ4HeaderStep(code)

	// Against itself both positions collide, contributing delta^2 terms that
	// a missing collision term would drop.
	got := rq.RQ4SymmetricDotEstimate(code, code)
	restored := rq.Restore(code)
	var want float64
	for _, x := range restored {
		want += float64(x) * float64(x)
	}
	require.InDelta(t, want, float64(got), 1e-3*(1+math.Abs(want)))

	dropped := math.Pow(float64(d0)*float64(compressionhelpers.RQ4OutlierAlpha*step), 2) +
		math.Pow(float64(d1)*float64(compressionhelpers.RQ4OutlierAlpha*step), 2)
	require.NotZero(t, dropped,
		"the sample vector must carry nonzero deltas at positions %d/%d", p0, p1)
	assert.Greater(t, dropped, 1e-3*(1+math.Abs(want)),
		"the collision term must be larger than the identity's tolerance, so the test can detect its absence")
}

// Degenerate and empty inputs must produce well-formed codes: the sidecar is
// present, its correction decodes to exactly zero, and no path returns NaN.
func TestRQ4OutlierDegenerateInputs(t *testing.T) {
	const (
		dim  = 128
		seed = 63
	)
	for _, tc := range centeredTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))
			vectors := tailVectors(rng, 20, dim, tc.normalize)
			rq := newOutlierQuantizer(t, dim, seed, tc.provider, vectors)
			wantLen := len(rq.Encode(vectors[0]))

			inputs := map[string][]float32{
				"zero vector":   make([]float32, dim),
				"empty":         {},
				"single spike":  append([]float32{5}, make([]float32, dim-1)...),
				"short input":   make([]float32, dim/2),
				"constant ones": constantVector(dim, 1),
			}
			for name, v := range inputs {
				t.Run(name, func(t *testing.T) {
					code := rq.Encode(v)
					require.Len(t, code, wantLen)
					_, _, d0, d1 := rq.RQ4OutlierSidecar(code)
					if rq.RQ4HeaderStep(code) == 0 {
						assert.Zero(t, d0, "degenerate codes must carry a zero correction")
						assert.Zero(t, d1, "degenerate codes must carry a zero correction")
					}

					for _, q := range [][]float32{vectors[0], make([]float32, dim), {}} {
						d := rq.NewDistancer(q)
						got, err := d.Distance(code)
						require.NoError(t, err)
						assert.False(t, math.IsNaN(float64(got)), "distance must not be NaN")
					}
					sym, err := rq.DistanceBetweenCompressedVectors(code, rq.Encode(vectors[1]))
					require.NoError(t, err)
					assert.False(t, math.IsNaN(float64(sym)), "symmetric distance must not be NaN")

					restored := rq.Restore(code)
					require.Len(t, restored, rq.OutputDimension())
					for _, x := range restored {
						require.False(t, math.IsNaN(float64(x)), "Restore must not produce NaN")
					}
				})
			}
		})
	}
}

func constantVector(dim int, x float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = x
	}
	return v
}

// Data() must reconstruct a quantizer that produces byte-identical codes and
// identical distances.
func TestRQ4OutlierRestoreRoundtrip(t *testing.T) {
	const (
		dim  = 192
		seed = 71
	)
	for _, tc := range centeredTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))
			vectors := tailVectors(rng, 30, dim, tc.normalize)
			rq := newOutlierQuantizer(t, dim, seed, tc.provider, vectors)

			data := rq.Data()
			require.NotEmpty(t, data.Mean, "Data must carry the centering mean")

			restored, err := compressionhelpers.RestoreFourBitRotationalQuantizer(
				int(data.InputDim), int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
				data.Rotation.Swaps, data.Rotation.Signs, data.Mean, tc.provider)
			require.NoError(t, err)

			q := vectors[0]
			d1 := rq.NewDistancer(q)
			d2 := restored.NewDistancer(q)
			for _, v := range vectors {
				c1 := rq.Encode(v)
				c2 := restored.Encode(v)
				require.Equal(t, c1, c2, "restored quantizer must encode identically")

				got1, err := d1.Distance(c1)
				require.NoError(t, err)
				got2, err := d2.Distance(c2)
				require.NoError(t, err)
				assert.Equal(t, got1, got2)
			}

			stats, ok := restored.Stats().(compressionhelpers.RQ4Stats)
			require.True(t, ok)
			assert.True(t, stats.Centering)
			// Centering and its sidecar are byte-neutral: the reported ratio
			// matches the uncentered tier at the same dimension.
			plain := compressionhelpers.RQ4Stats{Bits: 4, MetadataSize: stats.MetadataSize}
			assert.Equal(t, plain.CompressionRatio(dim), stats.CompressionRatio(dim))
		})
	}
}

// Codes must be identical whether encoded once or concurrently: Encode takes
// its scratch from a pool and the sidecar writes into it.
func TestRQ4OutlierEncodeIsConcurrencySafe(t *testing.T) {
	const (
		dim  = 256
		n    = 64
		seed = 83
	)
	rng := rand.New(rand.NewPCG(seed, seed))
	vectors := tailVectors(rng, n, dim, false)
	rq := newOutlierQuantizer(t, dim, seed, distancer.NewDotProductProvider(), vectors)

	want := make([][]byte, n)
	for i, v := range vectors {
		want[i] = rq.Encode(v)
	}

	got := make([][]byte, n)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < n; i += 8 {
				got[i] = rq.Encode(vectors[i])
			}
		}(w)
	}
	wg.Wait()
	for i := range want {
		require.Equal(t, want[i], got[i], fmt.Sprintf("vector %d", i))
	}
}
