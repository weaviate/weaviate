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
	"sync"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/tphakala/simd/f32"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// FourBitRotationalQuantizer compresses vectors to 4 bits per dimension using
// a RaBitQ-style random rotation followed by scalar quantization of the
// rotated entries. It is the 4-bit sibling of the 8-bit RotationalQuantizer
// and shares its design decisions:
//
//   - Per-vector quantization intervals: each vector is quantized
//     over its own [min, max] range of rotated entries rather than a fixed
//     interval derived from the Gaussian distribution of rotated entries as in
//     the RaBitQ paper. This adapts to the actual per-vector distribution and
//     requires no training.
//   - Codes are stored as []byte so the existing byte-based vector cache can
//     be reused. Two dimensions are packed per byte in a plane layout: byte j
//     holds dimension j in its low nibble and dimension j+D/2 in its high
//     nibble. This lets the distance path unpack eight packed bytes at a time
//     into two contiguous nibble streams with two uint64 mask operations,
//     keeping the unpacked codes in natural dimension order. The rotation
//     output dimension is always a multiple of 64, so packing never needs
//     padding.
//
// Following (extended) RaBitQ, distance estimation is asymmetric: data
// vectors are stored at 4 bits per dimension, but queries are quantized at 8
// bits per dimension inside the distancer. This costs nothing in storage and
// recovers most of the accuracy lost to the coarse data codes.
type FourBitRotationalQuantizer struct {
	inputDim  uint32
	rotation  *compression.FastRotation
	distancer distancer.Provider

	// Pool of encode-time scratch buffers; Encode is called concurrently
	// during imports.
	scratch sync.Pool

	// Precomputed for faster distance computations.
	err error   // Precomputed error returned on unsupported distancers.
	cos float32 // Indicator for the cosine-dot distancer.
	l2  float32 // Indicator for the l2-squared distancer.

	// Centering state
	mean      []float32
	meanNorm2 float32 // dot(mean, mean), for the compressed-compressed path.
}

const (
	// RQ4MetadataSize is the number of metadata bytes at the start of a 4-bit
	// code: lower, step, codeSum, norm2 as big-endian float32.
	RQ4MetadataSize = 16
	rq4MaxCode      = 15
	rq4QueryBits    = 8
)

func NewFourBitRotationalQuantizer(inputDim int, seed uint64, distancer distancer.Provider) *FourBitRotationalQuantizer {
	// Three rotation rounds, same trade-off as the 8-bit quantizer.
	rotationRounds := 3
	rotation := NewFastRotation(inputDim, rotationRounds, seed)
	cos, l2, err := distancerIndicatorsAndError(distancer)
	rq := &FourBitRotationalQuantizer{
		inputDim:  uint32(inputDim),
		rotation:  rotation,
		distancer: distancer,
		err:       err,
		cos:       cos,
		l2:        l2,
	}
	rq.scratch.New = func() any { return newRQ4Scratch(int(rotation.OutputDim), 0) }
	return rq
}

func NewCenteredFourBitRotationalQuantizer(inputDim int, seed uint64, distancer distancer.Provider, mean []float32) (*FourBitRotationalQuantizer, error) {
	if len(mean) == 0 {
		return nil, errors.New("centering requires a non-empty mean vector")
	}
	if len(mean) != inputDim {
		return nil, errors.Errorf("centering mean length %d does not match input dimension %d", len(mean), inputDim)
	}
	rq := NewFourBitRotationalQuantizer(inputDim, seed, distancer)
	rq.mean = mean
	rq.meanNorm2 = dotProduct(mean, mean)
	rq.scratch.New = func() any { return newRQ4Scratch(rq.OutputDimension(), len(mean)) }
	return rq, nil
}

func RestoreFourBitRotationalQuantizer(inputDim int, outputDim int, rounds int, swaps [][]compression.Swap, signs [][]float32, mean []float32, distancer distancer.Provider) (*FourBitRotationalQuantizer, error) {
	// Normalize empty to nil: an uncentered quantizer must never reach the
	// SIMD dot below — the amd64 kernel faults on a nil/empty slice.
	if len(mean) == 0 {
		mean = nil
	}
	if mean != nil && len(mean) != inputDim {
		return nil, errors.Errorf("centering mean length %d does not match input dimension %d", len(mean), inputDim)
	}
	cos, l2, err := distancerIndicatorsAndError(distancer)
	rq := &FourBitRotationalQuantizer{
		inputDim:  uint32(inputDim),
		rotation:  RestoreFastRotation(outputDim, rounds, swaps, signs),
		distancer: distancer,
		err:       err,
		cos:       cos,
		l2:        l2,
		mean:      mean,
	}
	if mean != nil {
		rq.meanNorm2 = dotProduct(mean, mean)
	}
	rq.scratch.New = func() any { return newRQ4Scratch(outputDim, len(mean)) }
	return rq, nil
}

func (rq *FourBitRotationalQuantizer) OutputDimension() int {
	return int(rq.rotation.OutputDim)
}

// RQ4Code is the packed 4-bit code of a data vector: 16 bytes of metadata
// followed by OutputDim/2 bytes holding two 4-bit codes each.
type RQ4Code []byte

func (c RQ4Code) Lower() float32 {
	return getFloat32(c, 0)
}

func (c RQ4Code) setLower(x float32) {
	putFloat32(c, 0, x)
}

func (c RQ4Code) Step() float32 {
	return getFloat32(c, 4)
}

func (c RQ4Code) setStep(x float32) {
	putFloat32(c, 4, x)
}

// CodeSum returns step * (sum of the integer codes).
func (c RQ4Code) CodeSum() float32 {
	return getFloat32(c, 8)
}

func (c RQ4Code) setCodeSum(x float32) {
	putFloat32(c, 8, x)
}

// Norm2 returns the squared Euclidean norm of the original vector.
func (c RQ4Code) Norm2() float32 {
	return getFloat32(c, 12)
}

func (c RQ4Code) setNorm2(x float32) {
	putFloat32(c, 12, x)
}

// Packed returns the packed 4-bit codes without the metadata prefix.
func (c RQ4Code) Packed() []byte {
	return c[RQ4MetadataSize:]
}

// Code returns the integer code of dimension i. Dimensions [0, D/2) live in
// the low nibbles, dimensions [D/2, D) in the high nibbles (plane layout).
func (c RQ4Code) Code(i int) byte {
	half := len(c) - RQ4MetadataSize
	if i < half {
		return c[RQ4MetadataSize+i] & 0x0F
	}
	return c[RQ4MetadataSize+i-half] >> 4
}

func (c RQ4Code) Dimension() int {
	return 2 * (len(c) - RQ4MetadataSize)
}

func NewRQ4Code(d int) RQ4Code {
	return make([]byte, RQ4MetadataSize+d/2)
}

// ZeroRQ4Code is the code representing the zero vector. We also return this
// in case of abnormal input, such as a nil vector.
func ZeroRQ4Code(d int) RQ4Code {
	return NewRQ4Code(d)
}

func (c RQ4Code) String() string {
	packed := c.Packed()
	return fmt.Sprintf("RQ4Code{Lower: %.4f, Step: %.4f, CodeSum: %.4f, Norm2: %.4f, Packed[:5]: %v}",
		c.Lower(), c.Step(), c.CodeSum(), c.Norm2(), packed[:min(5, len(packed))])
}

// rq4ClipFactors are the candidate shrink factors for the per-vector
// quantization interval, in the spirit of extended RaBitQ. With only 16 code
// points, spending them on the full [min, max] range wastes resolution on a
// few outlier entries: clipping the interval and clamping the outliers gives
// the remaining entries finer resolution. Encode evaluates every candidate on
// the actual rotated entries and keeps the best; the 1.0 factor (plain
// min/max, no clipping) is always a candidate, so the result is never worse
// than the unclipped encoding.
//
// The grid is deliberately coarse: parameter sweeps on real datasets
// (BenchmarkRQ4ParamSweep) showed that denser grids (9 or 13 factors, or 25
// for the symmetric reference encoder) do not improve recall — the search
// optimizes reconstruction quality on a sample, and finer grids only overfit
// that proxy. A coarse grid paired with a larger scoring sample gives equal
// or better recall for the same total search work.
var rq4ClipFactors = []float32{0.6, 0.7, 0.8, 0.9}

// rq4Scratch holds the intermediate buffers of the encode-time interval
// search so a single allocation serves all candidate evaluations.
type rq4Scratch struct {
	ci []int32   // integer codes of the most recent quantization
	cf []float32 // residual-stage scratch (rq4r); plain rq4 no longer uses it
	rx []float32 // rotated input, output buffer for RotateInto
	cx []float32 // centered input (x - mean); empty unless centering is on
}

func newRQ4Scratch(d, meanDim int) *rq4Scratch {
	return &rq4Scratch{
		ci: make([]int32, d),
		cf: make([]float32, d),
		rx: make([]float32, d),
		cx: make([]float32, meanDim),
	}
}

// centerInto writes x - mean into dst and returns the centered slice along
// with dot(x-mean, mean). Inputs shorter than mean are zero-padded before
// centering; entries beyond len(mean) are ignored.
func centerInto(dst, x, mean []float32) ([]float32, float32) {
	dst = dst[:len(mean)]
	n := copy(dst, x)
	for i := n; i < len(dst); i++ {
		dst[i] = 0
	}
	var dmu float32
	for i, m := range mean {
		v := dst[i] - m
		dst[i] = v
		dmu += v * m
	}
	return dst, dmu
}

// rq4Correlation quantizes xs over [lower, lower + 15*step] with clamping and
// returns s1 = <xs, x̂>, s2 = <x̂, x̂> and the code sum, where x̂ is the
// reconstruction. sumX is the precomputed sum of the entries of xs. On return
// scratch.ci holds the integer codes of xs, so the caller can reuse the codes
// of the final evaluation for packing. Candidates are compared by s1²/s2: the
// squared norm of the projection of xs onto the reconstruction direction,
// i.e. maximizing the cosine similarity between xs and x̂ as in extended
// RaBitQ. This is equivalent to minimizing the residual after the
// reconstruction is rescaled by its least-squares factor t = s1/s2.
//
// The whole pass runs on a single fused SIMD kernel (rq4QuantCorrImpl:
// NEON/AVX2 with a pure Go fallback) that quantizes and accumulates the
// three reductions in registers. The quantization is dst =
// int32(clamp(v*invStep + (0.5 - lower*invStep), 0, 15)) with truncation and
// two separate float32 roundings (never FMA), identical on every
// architecture, keeping the integer codes deterministic across platforms.
// sumC and sumC2 are exact integer sums, so only the <xs, codes> dot product
// carries architecture-specific float accumulation order.
func rq4Correlation(xs []float32, sumX, lower, step float32, scratch *rq4Scratch) (s1, s2, sumC float32) {
	n := len(xs)
	invStep := 1 / step
	ci := scratch.ci[:n]
	sumXC, sumCi, sumC2i := rq4QuantCorrImpl(ci, xs, invStep, 0.5-lower*invStep)
	sumC = float32(sumCi)
	sumC2 := float32(sumC2i)
	s1 = lower*sumX + step*sumXC
	s2 = float32(n)*lower*lower + 2*lower*step*sumC + step*step*sumC2
	return s1, s2, sumC
}

// rq4ClipSearchSample bounds the number of rotated entries used to score the
// clip factor candidates. The rotation mixes all input dimensions into every
// output entry, so a prefix is statistically representative of the full
// vector; scoring on a sample cuts the candidate search cost for
// high-dimensional vectors with little effect on which candidate wins. The
// rescaling factor of the winning candidate is recomputed exactly on the full
// vector. Sized together with rq4ClipFactors (see BenchmarkRQ4ParamSweep):
// few factors scored on a large sample beat many factors on a small one at
// equal total work.
var rq4ClipSearchSample = 512

// rq4Interval selects the quantization interval for the rotated vector rx by
// evaluating min/max shrunk by each clip factor. It returns the (lower, step)
// pair with the highest correlation score, the least-squares rescaling factor
// t to apply to the reconstruction, and the code sum of the winning
// quantization. On return scratch.ci holds the integer codes of rx under the
// winning interval, ready for packing. A zero step signals a degenerate
// (zero) vector.
func rq4Interval(rx []float32, scratch *rq4Scratch) (float32, float32, float32, float32) {
	// One fused sweep for min, max and the full-vector sum (the sum feeds the
	// exact rescaling pass at the end).
	minV, maxV, sumX := rq4MinMaxSumImpl(rx)
	bestLower := minV
	bestStep := (maxV - minV) / rq4MaxCode
	if bestStep <= 0 {
		return bestLower, 0, 1, 0
	}

	sample, sumSample := rx, sumX
	if len(sample) > rq4ClipSearchSample {
		sample = sample[:rq4ClipSearchSample]
		sumSample = f32.Sum(sample)
	}
	s1, s2, _ := rq4Correlation(sample, sumSample, bestLower, bestStep, scratch)
	var bestScore float32
	if s2 > 0 {
		bestScore = s1 * s1 / s2
	}
	for _, f := range rq4ClipFactors {
		lower := f * minV
		step := (f*maxV - lower) / rq4MaxCode
		if step <= 0 {
			continue
		}
		s1, s2, _ := rq4Correlation(sample, sumSample, lower, step, scratch)
		if s2 <= 0 {
			continue
		}
		if score := s1 * s1 / s2; score > bestScore {
			bestScore, bestLower, bestStep = score, lower, step
		}
	}

	// Exact rescaling factor for the winning interval over the full vector.
	// This pass also leaves the final codes in scratch.ci.
	bestT := float32(1)
	s1, s2, sumC := rq4Correlation(rx, sumX, bestLower, bestStep, scratch)
	if s2 > 0 {
		bestT = s1 / s2
	}
	if !(bestT > 0) {
		bestT = 1
	}
	return bestLower, bestStep, bestT, sumC
}

func (rq *FourBitRotationalQuantizer) Encode(x []float32) []byte {
	outDim := rq.OutputDimension()
	if len(x) == 0 {
		return ZeroRQ4Code(outDim)
	}
	if len(x) > outDim {
		x = x[:outDim]
	}

	scratch := rq.scratch.Get().(*rq4Scratch)
	defer rq.scratch.Put(scratch)
	var dmu float32
	if rq.mean != nil {
		x, dmu = centerInto(scratch.cx, x, rq.mean)
	}
	rx := rq.rotation.RotateInto(x, scratch.rx)
	lower, step, t, codeSum := rq4Interval(rx, scratch)
	if step <= 0 {
		// The input was likely the zero vector or indistinguishable from it.
		// Still record the norm2-slot metadata: the scalar estimator terms
		// stay exact even when the codes degenerate to zero.
		code := RQ4Code(ZeroRQ4Code(outDim))
		code.setNorm2(rq.norm2Slot(x, dmu))
		return code
	}

	code := NewRQ4Code(outDim)
	packed := code.Packed()
	half := len(packed)
	// scratch.ci holds the codes of the winning interval; packing is a pure
	// byte shuffle.
	ci := scratch.ci
	for i := range packed {
		packed[i] = byte(ci[i]) | byte(ci[half+i])<<4
	}
	// Fold the least-squares rescaling factor of the reconstruction into the
	// affine parameters. The distance computations are linear in (lower, step,
	// codeSum), so no query-time work is needed.
	code.setLower(t * lower)
	code.setStep(t * step)
	code.setCodeSum(t * step * codeSum)
	code.setNorm2(rq.norm2Slot(x, dmu))
	return code
}

// norm2Slot computes the value stored in the code's norm2 metadata field. In
// centered dot/cosine mode the l2 estimator never reads it, so the slot
// carries the exact centering correction dot(x-mean, mean) instead — zero
// extra bytes per vector. In every other mode it is the squared norm of the
// (possibly centered) encode input, which for centered l2 is exactly what the
// translation-invariant l2 estimator needs.
func (rq *FourBitRotationalQuantizer) norm2Slot(x []float32, dmu float32) float32 {
	if rq.mean != nil && rq.l2 == 0 {
		return dmu
	}
	return dotProduct(x, x)
}

func (rq *FourBitRotationalQuantizer) Rotate(x []float32) []float32 {
	return rq.rotation.Rotate(x)
}

func (rq *FourBitRotationalQuantizer) UnRotate(x []float32) []float32 {
	return rq.rotation.UnRotate(x)
}

// Restore returns the rotated-space approximation of the encoded vector.
func (rq *FourBitRotationalQuantizer) Restore(b []byte) []float32 {
	c := RQ4Code(b)
	d := c.Dimension()
	x := make([]float32, d)
	lower, step := c.Lower(), c.Step()
	packed := c.Packed()
	half := len(packed)
	for i, v := range packed {
		x[i] = lower + step*float32(v&0x0F)
		x[half+i] = lower + step*float32(v>>4)
	}
	return x
}

func (rq *FourBitRotationalQuantizer) Decode(compressed []byte) []float32 {
	unrotated := rq.rotation.UnRotateInPlace(rq.Restore(compressed))
	if int(rq.inputDim) < len(unrotated) {
		unrotated = unrotated[:rq.inputDim]
	}
	// Centered codes hold x-mean; add the mean back so Decode returns an
	// approximation of the original vector.
	for i, m := range rq.mean {
		unrotated[i] += m
	}
	return unrotated
}

// rq4QueryCode is an 8-bit encoding of the query over the same rotation. It
// only lives inside a distancer and is never stored. The integer codes are
// kept in two representations: unpacked bytes (one code per dimension) for
// the scalar byte-nibble kernel, and offset int8 values (code - 128) for the
// SIMD int8 dot product. The offset is corrected algebraically during
// distance estimation:
//
//	sum_i c'_i*c_i = sum_i (c'_i - 128)*c_i + 128*sum_i c_i
//
// where 128*sum_i c_i folds into the stored codeSum of the data vector.
type rq4QueryCode struct {
	lower     float32
	step      float32
	codeSum   float32 // step * (sum of the integer codes)
	norm2     float32
	codes     []byte
	codesInt8 []int8 // codes[i] - 128, for i8.DotProduct.
}

func (rq *FourBitRotationalQuantizer) encodeQuery(q []float32) rq4QueryCode {
	outDim := rq.OutputDimension()
	if len(q) > outDim {
		q = q[:outDim]
	}
	if len(q) == 0 {
		return rq4QueryCode{
			codes:     make([]byte, outDim),
			codesInt8: make([]int8, outDim),
		}
	}
	cq := encodeRotatedQuery(rq.rotation.Rotate(q))
	cq.norm2 = dotProduct(q, q)
	return cq
}

// encodeRotatedQuery quantizes an already-rotated query to 8 bits per
// dimension over its min/max range. The norm2 field is left zero; callers
// that need it set it from the unrotated vector.
func encodeRotatedQuery(rx []float32) rq4QueryCode {
	codes := make([]byte, len(rx))
	codesInt8 := make([]int8, len(rx))

	var maxCode uint8 = (1 << rq4QueryBits) - 1
	lower, upper, _ := rq4MinMaxSumImpl(rx)
	step := (upper - lower) / float32(maxCode)
	if step <= 0 {
		return rq4QueryCode{codes: codes, codesInt8: codesInt8}
	}

	var codeSum float32
	for i, v := range rx {
		c := byte((v-lower)/step + 0.5)
		codeSum += float32(c)
		codes[i] = c
		codesInt8[i] = int8(c - 128)
	}
	return rq4QueryCode{
		lower:     lower,
		step:      step,
		codeSum:   step * codeSum,
		codes:     codes,
		codesInt8: codesInt8,
	}
}

// FourBitRQDistancer computes asymmetric distances between an 8-bit encoded
// query and 4-bit encoded data vectors. Distance itself only reads shared
// state (the fused nibble kernel needs no scratch), but the distancer still
// owns scratch buffers used by the residual extension's distance path, which
// make that path NOT safe for concurrent use.
type FourBitRQDistancer struct {
	distancer distancer.Provider
	rq        *FourBitRotationalQuantizer
	query     []float32
	cq        rq4QueryCode
	a         float32 // Dimension()*lower + codeSum, precomputed.
	b         float32 // lower + 128*step, precomputed offset correction.

	// Unpacking buffer for data codes. scratchInt8 is an unsafe view of
	// scratch for the int8 SIMD dot product; nibble values 0-15 have the same
	// bit pattern in both types.
	scratch     []byte
	scratchInt8 []int8

	err error
	cos float32
	l2  float32

	// Centered dot/cosine mode
	centeredDot bool
	qMeanDot    float32
}

// bytesAsInt8 reinterprets a byte slice as int8 for the SIMD int8 kernels.
// Values must stay in [0, 127] for the two views to agree.
func bytesAsInt8(b []byte) []int8 {
	return unsafe.Slice((*int8)(unsafe.Pointer(&b[0])), len(b))
}

func (rq *FourBitRotationalQuantizer) NewDistancer(q []float32) *FourBitRQDistancer {
	// The distancer quantizes the centered query but keeps the original in
	// d.query: DistanceToFloat must rescore against uncentered floats.
	cq4 := q
	var qMeanDot float32
	if rq.mean != nil {
		cq4, qMeanDot = centerInto(make([]float32, len(rq.mean)), q, rq.mean)
		// centerInto returns dot(q-mean, mean); the estimator needs
		// dot(mean, q) = dot(q-mean, mean) + dot(mean, mean).
		qMeanDot += rq.meanNorm2
	}
	cq := rq.encodeQuery(cq4)
	scratch := make([]byte, len(cq.codes))
	return &FourBitRQDistancer{
		distancer:   rq.distancer,
		rq:          rq,
		query:       q,
		cq:          cq,
		a:           float32(len(cq.codes))*cq.lower + cq.codeSum,
		b:           cq.lower + 128*cq.step,
		scratch:     scratch,
		scratchInt8: bytesAsInt8(scratch),
		err:         rq.err,
		cos:         rq.cos,
		l2:          rq.l2,
		centeredDot: rq.mean != nil && rq.l2 == 0,
		qMeanDot:    qMeanDot,
	}
}

// Distance estimates the distance between the query and a 4-bit code. Using
// x_i ~ lower_x + step_x*c_i and q_i ~ lower_q + step_q*c'_i the dot product
// expands to D*l_q*l_x + l_x*codeSum_q + l_q*codeSum_x + step_q*step_x*<c',c>.
// The integer dot product <c',c> runs on a fused SIMD kernel that unpacks the
// data nibbles in registers (see dotByteNibbleImpl).
func (d *FourBitRQDistancer) Distance(x []byte) (float32, error) {
	if len(x) != RQ4MetadataSize+len(d.cq.codes)/2 {
		return 0, errors.Errorf("4-bit code length doesn't match: %d vs %d",
			len(x), RQ4MetadataSize+len(d.cq.codes)/2)
	}
	cx := RQ4Code(x)
	dot := dotByteNibbleImpl(d.cq.codes, cx.Packed())
	dotEstimate := cx.Lower()*d.a + cx.CodeSum()*d.cq.lower +
		cx.Step()*d.cq.step*float32(dot)
	if d.centeredDot {
		// dot(x, q) = dot(x-mean, q-mean) + dot(x-mean, mean) + dot(mean, q);
		// the second term rides in the norm2 slot, the third is per-query.
		return d.cos - (dotEstimate + cx.Norm2() + d.qMeanDot), d.err
	}
	return d.l2*(cx.Norm2()+d.cq.norm2) + d.cos - (1.0+d.l2)*dotEstimate, d.err
}

// distanceScalar is the pure Go fallback using the packed byte-nibble kernel.
// It exists so benchmarks can compare it against the SIMD-assisted Distance.
func (d *FourBitRQDistancer) distanceScalar(x []byte) (float32, error) {
	if len(x) != RQ4MetadataSize+len(d.cq.codes)/2 {
		return 0, errors.Errorf("4-bit code length doesn't match: %d vs %d",
			len(x), RQ4MetadataSize+len(d.cq.codes)/2)
	}
	cx := RQ4Code(x)
	dotEstimate := cx.Lower()*d.a + cx.CodeSum()*d.cq.lower +
		cx.Step()*d.cq.step*float32(dotByteNibbleGo(d.cq.codes, cx.Packed()))
	if d.centeredDot {
		return d.cos - (dotEstimate + cx.Norm2() + d.qMeanDot), d.err
	}
	return d.l2*(cx.Norm2()+d.cq.norm2) + d.cos - (1.0+d.l2)*dotEstimate, d.err
}

func (d *FourBitRQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.query) > 0 {
		return d.distancer.SingleDist(d.query, x)
	}
	cx := d.rq.Encode(x)
	return d.Distance(cx)
}

func (rq *FourBitRotationalQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.Errorf("4-bit code lengths don't match: %d vs %d",
			len(x), len(y))
	}
	cx, cy := RQ4Code(x), RQ4Code(y)
	a := float32(cx.Dimension()) * cx.Lower() * cy.Lower()
	b := cx.Lower() * cy.CodeSum()
	c := cy.Lower() * cx.CodeSum()
	d := cx.Step() * cy.Step() * float32(dotNibbleNibbleImpl(cx.Packed(), cy.Packed()))
	dotEstimate := a + b + c + d
	if rq.mean != nil && rq.l2 == 0 {
		// dot(x, y) = dot(x', y') + dot(x', mean) + dot(y', mean) + |mean|^2
		// with x' = x-mean; both cross terms ride in the norm2 slots.
		return rq.cos - (dotEstimate + cx.Norm2() + cy.Norm2() + rq.meanNorm2), rq.err
	}
	return rq.l2*(cx.Norm2()+cy.Norm2()) + rq.cos - (1.0+rq.l2)*dotEstimate, rq.err
}

// fourBitRQCompressedDistancer computes distances from a stored 4-bit code,
// used e.g. when reconnecting the HNSW graph after deletes.
type fourBitRQCompressedDistancer struct {
	rq *FourBitRotationalQuantizer
	cq RQ4Code
}

func (d *fourBitRQCompressedDistancer) Distance(x []byte) (float32, error) {
	return d.rq.DistanceBetweenCompressedVectors(d.cq, x)
}

func (d *fourBitRQCompressedDistancer) DistanceToFloat(x []float32) (float32, error) {
	return d.rq.DistanceBetweenCompressedVectors(d.cq, d.rq.Encode(x))
}

func (rq *FourBitRotationalQuantizer) NewCompressedQuantizerDistancer(c []byte) quantizerDistancer[byte] {
	return &fourBitRQCompressedDistancer{rq: rq, cq: c}
}

func (rq *FourBitRotationalQuantizer) NewQuantizerDistancer(vec []float32) quantizerDistancer[byte] {
	return rq.NewDistancer(vec)
}

func (rq *FourBitRotationalQuantizer) ReturnQuantizerDistancer(distancer quantizerDistancer[byte]) {}

func (rq *FourBitRotationalQuantizer) CompressedBytes(compressed []byte) []byte {
	return compressed
}

func (rq *FourBitRotationalQuantizer) FromCompressedBytes(compressed []byte) []byte {
	return compressed
}

func (rq *FourBitRotationalQuantizer) FromCompressedBytesWithSubsliceBuffer(compressed []byte, buffer *[]byte) []byte {
	if len(*buffer) < len(compressed) {
		*buffer = make([]byte, len(compressed)*1000)
	}

	// take from end so we can address the start of the buffer
	out := (*buffer)[len(*buffer)-len(compressed):]
	copy(out, compressed)
	*buffer = (*buffer)[:len(*buffer)-len(compressed)]

	return out
}

// PersistCompression writes the rotation to the commit log so the quantizer
// can be reconstructed on startup. The record is the same AddRQ entry used by
// the 8-bit quantizer; the Bits field distinguishes the two on restore.
func (rq *FourBitRotationalQuantizer) PersistCompression(logger CommitLogger) {
	logger.AddRQCompression(rq.Data())
}

func (rq *FourBitRotationalQuantizer) Data() compression.RQData {
	return compression.RQData{
		InputDim: rq.inputDim,
		Bits:     4,
		Rotation: *rq.rotation,
		Mean:     rq.mean,
	}
}

type RQ4Stats struct {
	Bits      uint32 `json:"bits"`
	Centering bool   `json:"centering"`
}

func (s RQ4Stats) CompressionType() string {
	return "rq"
}

func (s RQ4Stats) CompressionRatio(dimensionality int) float64 {
	// Original size = dim * 4 bytes (float32). Compressed size = 16 bytes of
	// metadata + half a byte per dimension.
	originalSize := dimensionality * 4
	compressedSize := RQ4MetadataSize + dimensionality/2
	return float64(originalSize) / float64(compressedSize)
}

func (rq *FourBitRotationalQuantizer) Stats() CompressionStats {
	return RQ4Stats{Bits: 4, Centering: rq.mean != nil}
}
