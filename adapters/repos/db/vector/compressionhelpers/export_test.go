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
	"github.com/tphakala/simd/f32"
)

// DistanceScalar exposes the pure Go packed byte-nibble distance path so
// benchmarks and tests can compare it against the SIMD-assisted Distance.
func (d *FourBitRQDistancer) DistanceScalar(x []byte) (float32, error) {
	return d.distanceScalar(x)
}

// DotByteNibble exposes the scalar packed kernel for benchmarks.
var DotByteNibble = dotByteNibbleImpl

// DotNibbleNibble exposes the scalar packed kernel for benchmarks.
var DotNibbleNibble = dotNibbleNibbleImpl

// SetRQ4ClipFactors overrides the clip factor candidate grid of Encode, for
// benchmarking recall sensitivity to the candidate count. Returns the
// previous grid.
func SetRQ4ClipFactors(f []float32) []float32 {
	prev := rq4ClipFactors
	rq4ClipFactors = f
	return prev
}

// SetRQ4ClipSearchSample overrides the sample size of the encode-time clip
// search, for parameter sweeps. Returns the previous value.
func SetRQ4ClipSearchSample(n int) int {
	prev := rq4ClipSearchSample
	rq4ClipSearchSample = n
	return prev
}

// PureRaBitQ4ScaleFactors is the candidate scale grid of the reference
// extended-RaBitQ encoder. The default 25-point grid approximates the paper's
// exact optimal-scale search; benchmarks can swap in a 10-point grid to
// compare encode cost at parity with Encode's candidate count.
var PureRaBitQ4ScaleFactors = func() []float32 {
	f := make([]float32, 25)
	for i := range f {
		f[i] = 0.40 + 0.025*float32(i)
	}
	return f
}()

// EncodePureRaBitQ4 is a reference implementation of pure extended RaBitQ at
// 4 bits, used only in benchmarks to compare grid parameterizations. It
// follows the extended RaBitQ recipe: a symmetric zero-centered uniform grid
// (reconstruction levels proportional to c - 7.5 for codes c in [0, 15]) with
// a single per-vector scale, chosen to maximize the cosine similarity between
// the rotated vector and its reconstruction, then rescaled by the
// least-squares factor. It differs from Encode only in the grid: one scale
// parameter centered at zero instead of the affine (lower, step)
// pair. The scale search uses a denser candidate set than Encode (25 vs 10)
// to approximate the paper's exact optimal-scale search. The output is a
// standard RQ4Code (with lower = -7.5*step), so the regular distancers apply
// unchanged.
func (rq *FourBitRotationalQuantizer) EncodePureRaBitQ4(x []float32) []byte {
	outDim := rq.OutputDimension()
	if len(x) == 0 {
		return ZeroRQ4Code(outDim)
	}
	if len(x) > outDim {
		x = x[:outDim]
	}

	rx := rq.rotation.Rotate(x)
	scratch := rq.scratch.Get().(*rq4Scratch)
	defer rq.scratch.Put(scratch)

	maxAbs := f32.MaxAbs(rx)
	if maxAbs <= 0 {
		return ZeroRQ4Code(outDim)
	}
	// The full-range step maps [-maxAbs, maxAbs] onto the 16 grid points.
	fullStep := 2 * maxAbs / rq4MaxCode

	sample := rx
	if len(sample) > rq4ClipSearchSample {
		sample = sample[:rq4ClipSearchSample]
	}
	sumSample := f32.Sum(sample)

	bestStep := fullStep
	var bestScore float32 = -1
	for _, f := range PureRaBitQ4ScaleFactors {
		step := f * fullStep
		s1, s2, _ := rq4Correlation(sample, sumSample, -7.5*step, step, scratch)
		if s2 <= 0 {
			continue
		}
		if score := s1 * s1 / s2; score > bestScore {
			bestScore, bestStep = score, step
		}
	}

	sumX := f32.Sum(rx)
	s1, s2, codeSum := rq4Correlation(rx, sumX, -7.5*bestStep, bestStep, scratch)
	t := float32(1)
	if s2 > 0 && s1/s2 > 0 {
		t = s1 / s2
	}

	code := NewRQ4Code(outDim)
	packed := code.Packed()
	half := len(packed)
	ci := scratch.ci
	for i := range packed {
		packed[i] = byte(ci[i]) | byte(ci[half+i])<<4
	}
	code.setLower(t * -7.5 * bestStep)
	code.setStep(t * bestStep)
	code.setCodeSum(t * bestStep * codeSum)
	code.setNorm2(f32.SumOfSquares(x))
	return code
}
