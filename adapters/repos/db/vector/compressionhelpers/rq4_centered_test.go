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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// coneVectors generates vectors of the form base + noise: a shared mean
// component dominating the per-vector signal, which is the regime real
// embedding models produce and the one centering is designed for.
func coneVectors(rng *rand.Rand, n, dim int, meanScale, noiseScale float64, normalize bool) [][]float32 {
	base := make([]float64, dim)
	for i := range base {
		base[i] = rng.NormFloat64() * meanScale
	}
	out := make([][]float32, n)
	for v := range out {
		vec := make([]float32, dim)
		var norm float64
		for i := range vec {
			x := base[i] + rng.NormFloat64()*noiseScale
			vec[i] = float32(x)
			norm += x * x
		}
		if normalize && norm > 0 {
			inv := float32(1 / math.Sqrt(norm))
			for i := range vec {
				vec[i] *= inv
			}
		}
		out[v] = vec
	}
	return out
}

func exactDistance(t *testing.T, p distancer.Provider, x, y []float32) float32 {
	t.Helper()
	d, err := p.SingleDist(x, y)
	require.NoError(t, err)
	return d
}

type centeredTestCase struct {
	name      string
	provider  distancer.Provider
	normalize bool
}

func centeredTestCases() []centeredTestCase {
	return []centeredTestCase{
		{"dot", distancer.NewDotProductProvider(), false},
		{"cosine", distancer.NewCosineDistanceProvider(), true},
		{"l2", distancer.NewL2SquaredProvider(), false},
	}
}

// The centered estimator must (a) approximate the exact distance at least as
// well as the stock quantizer on mean-dominated data — substantially better
// for dot/cosine — and (b) agree between the asymmetric (query) and symmetric
// (compressed-compressed) paths.
func TestCenteredRQ4EstimatorAccuracy(t *testing.T) {
	const (
		dim     = 128
		n       = 200
		queries = 20
		seed    = 42
	)
	for _, tc := range centeredTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))
			vectors := coneVectors(rng, n, dim, 4, 1, tc.normalize)
			qs := coneVectors(rng, queries, dim, 4, 1, tc.normalize)
			mean := compressionhelpers.MeanVector(vectors, dim)

			stock := compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, tc.provider)
			centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, tc.provider, mean)
			require.NoError(t, err)

			stockCodes := make([][]byte, n)
			centeredCodes := make([][]byte, n)
			for i, v := range vectors {
				stockCodes[i] = stock.Encode(v)
				centeredCodes[i] = centered.Encode(v)
			}

			var stockErr, centeredErr float64
			for _, q := range qs {
				ds := stock.NewDistancer(q)
				dc := centered.NewDistancer(q)
				for i, v := range vectors {
					exact := exactDistance(t, tc.provider, q, v)
					est, err := ds.Distance(stockCodes[i])
					require.NoError(t, err)
					stockErr += math.Abs(float64(est - exact))
					est, err = dc.Distance(centeredCodes[i])
					require.NoError(t, err)
					centeredErr += math.Abs(float64(est - exact))
				}
			}
			total := float64(len(qs) * n)
			stockErr /= total
			centeredErr /= total
			// Centered must never be materially worse; on mean-dominated raw
			// (unnormalized) data it should be a large improvement.
			assert.LessOrEqual(t, centeredErr, stockErr*1.1,
				"centered mean abs err %f vs stock %f", centeredErr, stockErr)
			if !tc.normalize {
				assert.Less(t, centeredErr, stockErr/2,
					"expected a large accuracy win on mean-dominated data, got centered %f vs stock %f",
					centeredErr, stockErr)
			}

			// Symmetric path agrees with exact distances too.
			var symErr float64
			pairs := 0
			for i := 0; i < 50; i++ {
				x, y := rng.IntN(n), rng.IntN(n)
				exact := exactDistance(t, tc.provider, vectors[x], vectors[y])
				est, err := centered.DistanceBetweenCompressedVectors(centeredCodes[x], centeredCodes[y])
				require.NoError(t, err)
				symErr += math.Abs(float64(est - exact))
				pairs++
			}
			symErr /= float64(pairs)
			assert.Less(t, symErr, stockErr+centeredErr+0.1,
				"symmetric path error %f out of line with asymmetric errors", symErr)
		})
	}
}

// Under a zero mean the two tiers see the same rotated vectors, so they are
// directly comparable: same code length, and the centered tier — which
// spends five of those bytes on the outlier sidecar — must estimate at least
// as accurately as stock, not merely close to it.
func TestCenteredRQ4ZeroMeanIsAtLeastAsAccurateAsStock(t *testing.T) {
	const (
		dim  = 64
		seed = 7
	)
	for _, tc := range centeredTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))
			vectors := coneVectors(rng, 20, dim, 2, 1, tc.normalize)
			q := coneVectors(rng, 1, dim, 2, 1, tc.normalize)[0]

			stock := compressionhelpers.NewFourBitRotationalQuantizer(dim, seed, tc.provider)
			centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(
				dim, seed, tc.provider, make([]float32, dim))
			require.NoError(t, err)

			ds := stock.NewDistancer(q)
			dc := centered.NewDistancer(q)
			var stockErr, centeredErr float64
			for _, v := range vectors {
				cs := stock.Encode(v)
				cc := centered.Encode(v)
				require.Equal(t, len(cs), len(cc),
					"both layouts spend the same metadata bytes")
				exact := float64(exactDistance(t, tc.provider, q, v))
				a, err := ds.Distance(cs)
				require.NoError(t, err)
				b, err := dc.Distance(cc)
				require.NoError(t, err)
				stockErr += math.Abs(float64(a) - exact)
				centeredErr += math.Abs(float64(b) - exact)
			}
			assert.LessOrEqual(t, centeredErr, stockErr,
				"the centered tier's sidecar must not cost accuracy at a zero mean")
		})
	}
}

// Encoding the mean itself degenerates to the zero code, and its distance to
// any query must still be the exact algebraic value: the estimate of
// dot(mean, q) carried entirely by the scalar correction terms.
func TestCenteredRQ4MeanVectorDistanceIsExact(t *testing.T) {
	const (
		dim  = 96
		seed = 3
	)
	rng := rand.New(rand.NewPCG(seed, seed))
	provider := distancer.NewDotProductProvider()
	vectors := coneVectors(rng, 100, dim, 3, 1, false)
	mean := compressionhelpers.MeanVector(vectors, dim)
	q := coneVectors(rng, 1, dim, 3, 1, false)[0]

	centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, provider, mean)
	require.NoError(t, err)

	code := centered.Encode(mean)
	d := centered.NewDistancer(q)
	est, err := d.Distance(code)
	require.NoError(t, err)
	exact := exactDistance(t, provider, q, mean)
	// x - mean = 0 leaves no quantization error; only float rounding remains.
	assert.InDelta(t, exact, est, 1e-2)
}

// Persist/restore must reproduce identical distances, including the mean.
func TestCenteredRQ4RestoreRoundtrip(t *testing.T) {
	const (
		dim  = 64
		seed = 11
	)
	for _, tc := range centeredTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))
			vectors := coneVectors(rng, 30, dim, 3, 1, tc.normalize)
			q := coneVectors(rng, 1, dim, 3, 1, tc.normalize)[0]
			mean := compressionhelpers.MeanVector(vectors, dim)

			centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, tc.provider, mean)
			require.NoError(t, err)
			data := centered.Data()
			require.Equal(t, mean, data.Mean)

			restored, err := compressionhelpers.RestoreFourBitRotationalQuantizer(
				int(data.InputDim), int(data.Rotation.OutputDim), int(data.Rotation.Rounds),
				data.Rotation.Swaps, data.Rotation.Signs, data.Mean, tc.provider)
			require.NoError(t, err)

			d1 := centered.NewDistancer(q)
			d2 := restored.NewDistancer(q)
			for _, v := range vectors {
				c1 := centered.Encode(v)
				c2 := restored.Encode(v)
				assert.Equal(t, c1, c2, "restored quantizer must produce identical codes")
				a, err := d1.Distance(c1)
				require.NoError(t, err)
				b, err := d2.Distance(c2)
				require.NoError(t, err)
				assert.Equal(t, a, b)
			}
		})
	}
}

// The distancer keeps the original (uncentered) query for exact rescoring.
func TestCenteredRQ4DistanceToFloatUsesOriginalQuery(t *testing.T) {
	const (
		dim  = 32
		seed = 5
	)
	rng := rand.New(rand.NewPCG(seed, seed))
	provider := distancer.NewDotProductProvider()
	vectors := coneVectors(rng, 10, dim, 3, 1, false)
	mean := compressionhelpers.MeanVector(vectors, dim)
	q := coneVectors(rng, 1, dim, 3, 1, false)[0]

	centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, provider, mean)
	require.NoError(t, err)
	d := centered.NewDistancer(q)
	for _, v := range vectors {
		got, err := d.DistanceToFloat(v)
		require.NoError(t, err)
		assert.Equal(t, exactDistance(t, provider, q, v), got)
	}
}

// Decode adds the mean back so decoded vectors approximate the original.
func TestCenteredRQ4DecodeAddsMeanBack(t *testing.T) {
	const (
		dim  = 64
		seed = 13
	)
	rng := rand.New(rand.NewPCG(seed, seed))
	provider := distancer.NewL2SquaredProvider()
	vectors := coneVectors(rng, 50, dim, 4, 0.5, false)
	mean := compressionhelpers.MeanVector(vectors, dim)

	centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, provider, mean)
	require.NoError(t, err)
	v := vectors[0]
	decoded := centered.Decode(centered.Encode(v))
	require.Len(t, decoded, dim)
	var refNorm, errNorm float64
	for i := range v {
		refNorm += float64(v[i]) * float64(v[i])
		diff := float64(decoded[i] - v[i])
		errNorm += diff * diff
	}
	assert.Less(t, math.Sqrt(errNorm), 0.25*math.Sqrt(refNorm),
		"decoded vector should be a reasonable approximation of the original")
}

// An empty (abnormal) query must behave as the zero vector, exactly like the
// uncentered path: every distance estimates dot(x, 0) ~ 0.
func TestCenteredRQ4EmptyQueryBehavesAsZeroVector(t *testing.T) {
	const (
		dim  = 96
		seed = 17
	)
	rng := rand.New(rand.NewPCG(seed, seed))
	provider := distancer.NewDotProductProvider()
	vectors := coneVectors(rng, 50, dim, 4, 1, false)
	mean := compressionhelpers.MeanVector(vectors, dim)

	centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(dim, seed, provider, mean)
	require.NoError(t, err)
	d := centered.NewDistancer(nil)
	var sumDist, sumDmu float64
	for _, v := range vectors {
		dist, err := d.Distance(centered.Encode(v))
		require.NoError(t, err)
		var dmu float32
		for i, m := range mean {
			dmu += (v[i] - m) * m
		}
		sumDist += math.Abs(float64(dist))
		sumDmu += math.Abs(float64(dmu))
	}
	require.Less(t, sumDist, 0.5*sumDmu,
		"empty-query distances (sum %f) should be quantization noise, not the -dmu bias (sum %f)", sumDist, sumDmu)
}

func TestCenteredRQ4ConstructorValidation(t *testing.T) {
	provider := distancer.NewDotProductProvider()
	_, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(8, 1, provider, make([]float32, 4))
	assert.Error(t, err, "mean length mismatch must be rejected")
	_, err = compressionhelpers.RestoreFourBitRotationalQuantizer(8, 64, 3, nil, nil, make([]float32, 4), provider)
	assert.Error(t, err, "restore with mismatched mean length must be rejected")

	// The wide centered layout addresses outliers with uint16 positions, so
	// the output dimension cannot exceed 65535.
	tooWide := 65600
	_, err = compressionhelpers.RestoreFourBitRotationalQuantizer(
		tooWide, tooWide, 3, nil, nil, make([]float32, tooWide), provider)
	assert.Error(t, err, "centering above the uint16 position cap must be rejected")
}

func TestMeanVector(t *testing.T) {
	cases := []struct {
		name    string
		vectors [][]float32
		dim     int
		want    []float32
	}{
		{"empty input", nil, 3, []float32{0, 0, 0}},
		{"basic", [][]float32{{1, 2}, {3, 4}}, 2, []float32{2, 3}},
		{"short vector zero-pads", [][]float32{{2}, {4, 8}}, 2, []float32{3, 4}},
		{"long vector truncated", [][]float32{{1, 2, 99}}, 2, []float32{1, 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, compressionhelpers.MeanVector(tc.vectors, tc.dim))
		})
	}
}

// Stats must expose centering so operators can tell the tiers apart.
func TestCenteredRQ4Stats(t *testing.T) {
	provider := distancer.NewDotProductProvider()
	stock := compressionhelpers.NewFourBitRotationalQuantizer(16, 1, provider)
	centered, err := compressionhelpers.NewCenteredFourBitRotationalQuantizer(16, 1, provider, make([]float32, 16))
	require.NoError(t, err)
	for name, q := range map[string]interface {
		Stats() compressionhelpers.CompressionStats
	}{"stock": stock, "centered": centered} {
		stats, ok := q.Stats().(compressionhelpers.RQ4Stats)
		require.True(t, ok)
		assert.Equal(t, name == "centered", stats.Centering, fmt.Sprintf("quantizer %s", name))
	}
}
