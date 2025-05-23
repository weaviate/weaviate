//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func codebook(d int, bits int) [][]float32 {
	// All values (pre-normalization) in any one dimension.
	values := make([]float32, 1<<bits)
	for i := range 1 << bits {
		values[i] = -float32(int(1<<(bits-1))) + 0.5 + float32(i)
	}

	// Generate all codes.
	codes := make([][]float32, 0, 1<<bits)
	for _, v := range values {
		x := make([]float32, 1)
		x[0] = v
		codes = append(codes, x)
	}

	for i := range d - 1 {
		newCodes := make([][]float32, 0, len(codes)*(1<<bits))
		for j := range codes {
			for _, v := range values {
				x := make([]float32, i+2)
				copy(x, codes[j])
				x[i+1] = v
				newCodes = append(newCodes, x)
			}
		}
		codes = newCodes
	}

	// Normalize the codes to lie on the unit sphere.
	for _, c := range codes {
		l2norm := float32(norm(c))
		for j := range c {
			c[j] = c[j] / l2norm
		}
	}
	return codes
}

type VectorGenerator struct {
	rng *rand.Rand
}

func NewVectorGenerator() *VectorGenerator {
	return NewVectorGeneratorWithSeed(42)
}

func NewVectorGeneratorWithSeed(seed uint64) *VectorGenerator {
	return &VectorGenerator{
		rng: rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac)),
	}
}

func (vg *VectorGenerator) RandomUnitVector(d int) []float32 {
	x := make([]float32, d)
	for i := range x {
		x[i] = float32(vg.rng.NormFloat64())
	}
	xNorm := float32(norm(x))
	for i := range x {
		x[i] = x[i] / xNorm
	}
	return x
}

func findNearestCode(x []float32, codes [][]float32) []float32 {
	var minDist float64 = math.MaxFloat64
	var minIndex int
	for i, c := range codes {
		if dist := dist(x, c); dist < minDist {
			minDist = dist
			minIndex = i
		}
	}
	return codes[minIndex]
}

func TestRQEncodesToNearest(t *testing.T) {
	d := 3
	maxBits := 4
	n := 100

	var seed uint64 = 42
	rq := compressionhelpers.NewRotationalQuantizer(d, seed)
	vg := NewVectorGenerator()

	for i := range maxBits {
		bits := i + 1
		codes := codebook(d, bits)
		for range n {
			x := vg.RandomUnitVector(d)
			c := rq.Encode(x, bits)
			qx := rq.Decode(c)
			// Apply the same underlying rotation of x and find the nearest code point by brute force.
			rx := rq.Rotate(x)
			bx := findNearestCode(rx, codes)
			assert.Less(t, math.Abs(float64(dist(x, qx)-dist(x, bx))), 1e-6)
		}
	}
}

func TestRQEstimationConcentrationBounds(t *testing.T) {
	// Create a number of rotational quantizers. Construct two vectors with a given cosine similarity.
	// Verify that the estimator behaves according to the concentration bounds specified in the paper.
	d := 256
	m := 10
	rq := make([]*compressionhelpers.RotationalQuantizer, m)
	for i := range rq {
		rq[i] = compressionhelpers.NewRotationalQuantizer(d, uint64(i))
	}

	q := make([]float32, d)
	x := make([]float32, d)

	var alpha float32 = 0.5
	q[0] = 1.0
	x[0] = alpha
	x[1] = float32(math.Sqrt(1.0 - float64(alpha*alpha)))
	for i := range 1 {
		bits := i + 1

		// With probability > 0.999 the absolute error should be less than eps.
		// For d = 256 the error for b bits is 2^(-b) * 0.36, so 0.18, 0.9,
		// 0.045.. for b = 1, 2, 3,...
		eps := math.Pow(2.0, -float64(bits)) * 5.75 / math.Sqrt(float64(d))

		cos := make([]float32, m)
		for i := range rq {
			c := rq[i].Encode(x, bits)
			dist := rq[i].NewDistancer(q, distancer.NewDotProductProvider(), bits)
			estimate, _ := dist.Distance(c)
			cos[i] = -estimate // Holds for unit vectors.
			assert.Less(t, math.Abs(float64(cos[i]-alpha)), eps)
		}
	}
}

func TestRQDistancer(t *testing.T) {
	d := 256
	var alpha float32 = 0.5
	var qNorm float32 = 0.76
	var xNorm float32 = 3.14
	q := make([]float32, d)
	x := make([]float32, d)
	q[0] = qNorm
	x[0] = xNorm * alpha
	x[1] = xNorm * float32(math.Sqrt(1.0-float64(alpha*alpha)))

	bits := 8
	rq := compressionhelpers.NewRotationalQuantizer(d, 42)
	code := rq.Encode(x, bits)

	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}

	for _, m := range metrics {
		expected, _ := m.SingleDist(q, x)
		distancer := rq.NewDistancer(q, m, bits)
		estimated, _ := distancer.Distance(code)
		eps := 0.0002
		assert.Less(t, math.Abs(float64(estimated-expected)), eps)
	}
}

func TestRQRestore(t *testing.T) {
	d := 32
	vg := NewVectorGenerator()
	x := vg.RandomUnitVector(d)
	// Set some entries manually to ensure that it works for non-unit vectors.
	x[3] = -3.67
	x[9] = 4.99

	bits := 8
	rq := compressionhelpers.NewRotationalQuantizer(d, 42)
	code := rq.Encode(x, bits)
	xRestored := rq.Restore(code)
	t.Log(x)
	t.Log(xRestored)
	for i := range d {
		assert.Less(t, math.Abs(float64(x[i]-xRestored[i])), 0.015)
	}
}

func l2Squared(x []float32, y []float32) float32 {
	provider := distancer.NewL2SquaredProvider()
	dist, _ := provider.SingleDist(x, y)
	return dist
}

func TestRQRestoreWithCenters(t *testing.T) {
	d := 4
	numCenters := 25
	numVectors := 100
	bits := 6
	centers := make([][]float32, numCenters)
	vg := NewVectorGenerator()
	for i := range numCenters {
		centers[i] = vg.RandomUnitVector(d)
	}
	rq := compressionhelpers.NewRotationalQuantizerWithCenters(d, 42, centers)

	for range numVectors {
		v := vg.RandomUnitVector(d)
		c := rq.Encode(v, bits)
		vRestored := rq.Restore(c)
		var eps float32 = 1e-3
		assert.Less(t, l2Squared(v, vRestored), eps)
	}
}

func TestRQDistancerWithCentersSimple(t *testing.T) {
	d := 2
	bits := 4
	x := []float32{1.0, 0.0}
	q := []float32{-1.0, 0.0}
	centers := [][]float32{{0.5, 0.5}}
	rq := compressionhelpers.NewRotationalQuantizerWithCenters(d, 42, centers)
	distancer := rq.NewDistancer(q, distancer.NewDotProductProvider(), bits)
	c := rq.Encode(x, bits)
	dist, _ := distancer.Distance(c)
	assert.Less(t, math.Abs(float64(dist-1.0)), 0.002)
}

// Create vectors clustered around different centers.
// Verify that RQ with centers is at least as accurate as RQ without centers.
func TestRQDistancerWithCenters(t *testing.T) {
	d := 128
	bits := 8
	numCenters := 10
	numClusterVectors := 10

	vg := NewVectorGenerator()
	centers := make([][]float32, numCenters)
	for i := range centers {
		centers[i] = vg.RandomUnitVector(d)
	}

	rq := compressionhelpers.NewRotationalQuantizer(d, 42)
	rqCentered := compressionhelpers.NewRotationalQuantizerWithCenters(d, 42, centers)

	n := numCenters * numClusterVectors
	vectors := make([][]float32, n)
	codes := make([]compressionhelpers.RQEncoding, n)
	codesCentered := make([]compressionhelpers.RQEncoding, n)
	i := 0
	for _, c := range centers {
		for range numClusterVectors {
			// Generate a random point that is highly correlated with the current center.
			v := vg.RandomUnitVector(d)
			for s := range v {
				v[s] = 0.75*c[s] + 0.25*v[s]
			}
			vectors[i] = v
			i++
		}
	}

	for i := range vectors {
		codes[i] = rq.Encode(vectors[i], bits)
		codesCentered[i] = rqCentered.Encode(vectors[i], bits)
	}

	query := vg.RandomUnitVector(d)

	metrics := []distancer.Provider{
		distancer.NewCosineDistanceProvider(),
		distancer.NewDotProductProvider(),
		distancer.NewL2SquaredProvider(),
	}

	for _, m := range metrics {
		distancer := rq.NewDistancer(query, m, bits)
		distancerCentered := rqCentered.NewDistancer(query, m, bits)
		var errorSum float64
		var errorSumCentered float64
		for i := range vectors {
			expected, _ := m.SingleDist(query, vectors[i])
			estimated, _ := distancer.Distance(codes[i])
			estimatedCentered, _ := distancerCentered.Distance(codesCentered[i])
			absDiff := math.Abs(float64(expected - estimated))
			errorSum += absDiff
			absDiffCentered := math.Abs(float64(expected - estimatedCentered))
			errorSumCentered += absDiffCentered
			assert.Less(t, absDiff, 1e-2)
			assert.Less(t, absDiffCentered, 1e-2)
		}
		// Centering tends to reduce the error.
		assert.Less(t, errorSumCentered/errorSum, 0.5)
	}
}

func BenchmarkRQEncode(b *testing.B) {
	dimensions := []int{64, 256, 1024}
	encodingBits := []int{1, 2, 4, 8}
	vg := NewVectorGenerator()
	for _, dim := range dimensions {
		for _, bits := range encodingBits {
			rq := compressionhelpers.NewRotationalQuantizer(dim, 42)
			b.Run(fmt.Sprintf("RQ-d%d-b%d", dim, bits), func(b *testing.B) {
				for b.Loop() {
					b.StopTimer()
					x := vg.RandomUnitVector(dim)
					b.StartTimer()
					rq.Encode(x, bits)
				}
				b.ReportMetric(float64(b.Elapsed().Microseconds())/float64(b.N), "us/op")
			})
		}
	}
}

func BenchmarkRQNew(b *testing.B) {
	dimensions := []int{32, 128, 512, 1024, 1536}
	for _, dim := range dimensions {
		b.Run(fmt.Sprintf("RQ-d%d", dim), func(b *testing.B) {
			for b.Loop() {
				compressionhelpers.NewRotationalQuantizer(dim, 42)
			}
			b.ReportMetric(float64(b.Elapsed().Seconds())/float64(b.N), "sec/op")
		})
	}
}
