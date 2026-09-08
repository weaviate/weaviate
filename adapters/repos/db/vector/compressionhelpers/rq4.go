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
	"encoding/binary"
	"fmt"
	"math"
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

	// Centering state.
	mean      []float32
	meanNorm2 float32 // dot(mean, mean), for the compressed-compressed path.
	metaSize  int
	layout    rq4CenteredLayout
}

const (
	RQ4MetadataSize = 16

	rq4CenteredWideMetadataSize = 20
	rq4CenteredCompactMaxDim    = 4096
	rq4CenteredMaxOutputDim     = math.MaxUint16

	rq4MaxCode      = 15
	rq4QueryBits    = 8
	rq4LowerAnchor  = 7.5
	rq4LowerScale   = 32
	rq4OutlierAlpha = 0.25
)

type rq4CenteredLayout struct {
	size     int
	norm2Off int
	sumOff   int
	sumWide  bool // code sum as uint32 rather than uint16
	lowerOff int
	posOff   int
	posWide  bool // positions as two uint16 rather than two 12-bit values
	deltaOff int
}

// step always sits at offset 0 in both variants; the fused header decode
// reads it first and every other field is scaled by it.
const rq4cStepOff = 0

var (
	// [0:4] step f32 | [4:8] norm2/dmu f32 | [8:10] nibble sum u16 |
	// [10:11] lower i8 | [11:14] two 12-bit positions | [14:16] two i8 deltas
	rq4CompactLayout = rq4CenteredLayout{
		size: RQ4MetadataSize, norm2Off: 4, sumOff: 8, lowerOff: 10,
		posOff: 11, deltaOff: 14,
	}
	// [0:4] step f32 | [4:8] norm2/dmu f32 | [8:12] nibble sum u32 |
	// [12:16] two u16 positions | [16:18] two i8 deltas | [18:19] lower i8 |
	// [19:20] reserved, written as zero
	rq4WideLayout = rq4CenteredLayout{
		size: rq4CenteredWideMetadataSize, norm2Off: 4, sumOff: 8, sumWide: true,
		posOff: 12, posWide: true, deltaOff: 16, lowerOff: 18,
	}
)

func rq4LayoutFor(outputDim int) rq4CenteredLayout {
	if outputDim > rq4CenteredCompactMaxDim {
		return rq4WideLayout
	}
	return rq4CompactLayout
}

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
		metaSize:  RQ4MetadataSize,
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
	if err := rq4CheckCenteredDim(rq.OutputDimension()); err != nil {
		return nil, err
	}
	rq.mean = mean
	rq.meanNorm2 = dotProduct(mean, mean)
	rq.layout = rq4LayoutFor(rq.OutputDimension())
	rq.metaSize = rq.layout.size
	rq.scratch.New = func() any { return newRQ4Scratch(rq.OutputDimension(), len(mean)) }
	return rq, nil
}

func rq4CheckCenteredDim(outputDim int) error {
	if outputDim > rq4CenteredMaxOutputDim {
		return errors.Errorf("centered 4-bit codes cap the output dimension at %d, got %d",
			rq4CenteredMaxOutputDim, outputDim)
	}
	return nil
}

func putUint32(b []byte, pos int, x uint32) {
	binary.BigEndian.PutUint32(b[pos:], x)
}

func getUint32(b []byte, pos int) uint32 {
	return binary.BigEndian.Uint32(b[pos:])
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
	if mean != nil {
		if err := rq4CheckCenteredDim(outputDim); err != nil {
			return nil, err
		}
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
		metaSize:  RQ4MetadataSize,
	}
	if mean != nil {
		rq.layout = rq4LayoutFor(outputDim)
		rq.metaSize = rq.layout.size
		rq.meanNorm2 = dotProduct(mean, mean)
	}
	rq.scratch.New = func() any { return newRQ4Scratch(outputDim, len(mean)) }
	return rq, nil
}

func (rq *FourBitRotationalQuantizer) OutputDimension() int {
	return int(rq.rotation.OutputDim)
}

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

func putUint16(b []byte, pos int, x uint16) {
	binary.BigEndian.PutUint16(b[pos:], x)
}

func getUint16(b []byte, pos int) uint16 {
	return binary.BigEndian.Uint16(b[pos:])
}

// rq4Header is the decoded per-code metadata, independent of the stored
// layout.
type rq4Header struct {
	lower   float32
	step    float32
	codeSum float32
	norm2   float32
}

func (rq *FourBitRotationalQuantizer) header(c []byte) rq4Header {
	if rq.centered() {
		l := rq.layout
		step := getFloat32(c, rq4cStepOff)
		sum := uint32(getUint16(c, l.sumOff))
		if l.sumWide {
			sum = getUint32(c, l.sumOff)
		}
		return rq4Header{
			lower:   rq4LowerFromCode(int8(c[l.lowerOff]), step),
			step:    step,
			codeSum: step * float32(sum),
			norm2:   getFloat32(c, l.norm2Off),
		}
	}
	cx := RQ4Code(c)
	return rq4Header{lower: cx.Lower(), step: cx.Step(), codeSum: cx.CodeSum(), norm2: cx.Norm2()}
}

func (rq *FourBitRotationalQuantizer) putHeader(c []byte, lower, step, sumC, norm2 float32) {
	if rq.centered() {
		l := rq.layout
		putFloat32(c, rq4cStepOff, step)
		putFloat32(c, l.norm2Off, norm2)
		if l.sumWide {
			putUint32(c, l.sumOff, uint32(sumC))
		} else {
			putUint16(c, l.sumOff, uint16(sumC))
		}
		c[l.lowerOff] = byte(rq4LowerCode(lower, step))
		return
	}
	cx := RQ4Code(c)
	cx.setLower(lower)
	cx.setStep(step)
	cx.setCodeSum(step * sumC)
	cx.setNorm2(norm2)
}

func (rq *FourBitRotationalQuantizer) newCode(d int) []byte {
	return make([]byte, rq.metaSize+d/2)
}

func (rq *FourBitRotationalQuantizer) centered() bool {
	return rq.mean != nil
}

func rq4LowerCode(lower, step float32) int8 {
	if !(step > 0) {
		return 0
	}
	q := (lower/step + rq4LowerAnchor) * rq4LowerScale
	if math.IsNaN(float64(q)) {
		return 0
	}
	if q >= 0 {
		q += 0.5
	} else {
		q -= 0.5
	}
	if q > 127 {
		return 127
	}
	if q < -127 {
		return -127
	}
	return int8(q)
}

func rq4LowerFromCode(c int8, step float32) float32 {
	return step * (float32(c)/rq4LowerScale - rq4LowerAnchor)
}

// readOutlierSidecar decodes the stored outlier positions and int8 deltas.
func (rq *FourBitRotationalQuantizer) readOutlierSidecar(c []byte) (p0, p1 int, d0, d1 int8) {
	l := rq.layout
	if l.posWide {
		p0, p1 = int(getUint16(c, l.posOff)), int(getUint16(c, l.posOff+2))
	} else {
		p0 = int(c[l.posOff])<<4 | int(c[l.posOff+1])>>4
		p1 = int(c[l.posOff+1]&0x0F)<<8 | int(c[l.posOff+2])
	}
	return p0, p1, int8(c[l.deltaOff]), int8(c[l.deltaOff+1])
}

// writeOutlierSidecar writes the two selected outliers into the metadata
// block. It must run after the nibbles and the header are in place: the
// deltas are residuals against the STORED reconstruction, read back out of
// the code so every rounding the reader will see is already inside them
// (mandatory — computing them against the unrounded parameters degrades the
// correction).
func (rq *FourBitRotationalQuantizer) writeOutlierSidecar(code []byte, p0, p1 int, v0, v1 float32) {
	l := rq.layout
	if l.posWide {
		putUint16(code, l.posOff, uint16(p0))
		putUint16(code, l.posOff+2, uint16(p1))
	} else {
		code[l.posOff] = byte(p0 >> 4)
		code[l.posOff+1] = byte(p0&0x0F)<<4 | byte(p1>>8)
		code[l.posOff+2] = byte(p1)
	}
	h := rq.header(code)
	code[l.deltaOff] = byte(rq4OutlierDelta(v0, rq.nibbleValue(code, h, p0), h.step))
	code[l.deltaOff+1] = byte(rq4OutlierDelta(v1, rq.nibbleValue(code, h, p1), h.step))
}

// rq4OutlierDelta quantizes the outlier residual v - rec onto the
// alpha*step int8 grid (round half away from zero, clamped to ±127).
// Degenerate steps and non-finite residuals encode to zero, so the
// correction decodes to exactly zero.
func rq4OutlierDelta(v, rec, step float32) int8 {
	if !(step > 0) {
		return 0
	}
	q := (v - rec) / (rq4OutlierAlpha * step)
	if math.IsNaN(float64(q)) {
		return 0
	}
	if q >= 0 {
		q += 0.5
	} else {
		q -= 0.5
	}
	if q > 127 {
		return 127
	}
	if q < -127 {
		return -127
	}
	return int8(q)
}

// rq4OutlierNaNFloor is the smallest sign-cleared float32 bit pattern that
// denotes a NaN. Magnitudes at or above it are excluded from outlier
// selection: a NaN coordinate must never be chosen, matching the
// quantizer's NaN-as-zero convention downstream.
const rq4OutlierNaNFloor = 0x7F800001

// rq4OutlierKey is the comparison key of a coordinate: its magnitude as a
// bit pattern, with NaN mapped to zero so it never wins. For same-sign
// floats the unsigned bit-pattern order is the numeric order, so integer
// compares rank magnitudes exactly.
func rq4OutlierKey(v float32) uint32 {
	k := math.Float32bits(v) &^ (1 << 31)
	if k >= rq4OutlierNaNFloor {
		return 0
	}
	return k
}

// rq4SelectOutliers records the two largest-magnitude coordinates of rx
// (ties break toward the lower index, NaN never selected) and zeroes them in
// place so the interval search runs on the remaining coordinates only. rx
// always has at least two entries (the rotation output dimension is a
// multiple of 64), and the two returned positions are always distinct.
//
// Three choices make this pass cheap enough to sit in front of every encode.
// Magnitudes are compared as sign-cleared bit patterns, so the hot loop
// issues integer compares rather than floating-point ones (~2x faster
// measured). The loop guards on the SMALLER of the two running maxima, so
// the common case is a single compare and a branch taken O(log n) times;
// testing against m0 first would double the compares and lengthen the
// loop-carried dependency (~4x slower measured). The NaN test then costs
// nothing on the hot path: raw keys are compared in the loop and NaN is
// filtered inside the rarely-taken branch, where it is the only key that can
// exceed the floor.
//
// The first two coordinates seed the maxima explicitly rather than starting
// from a sentinel. A sentinel of zero would leave both positions pointing at
// index 0 for a vector whose only nonzero coordinate is rx[0] (the seed
// shifts p0 into p1 before any later element can displace it), and a code
// with two sidecar entries on one coordinate would double-count that
// coordinate's correction.
func rq4SelectOutliers(rx []float32) (p0, p1 int, v0, v1 float32) {
	k0, k1 := rq4OutlierKey(rx[0]), rq4OutlierKey(rx[1])
	p0, p1 = 0, 1
	m0, m1 := k0, k1
	if k1 > k0 {
		p0, p1, m0, m1 = 1, 0, k1, k0
	}
	for i := 2; i < len(rx); i++ {
		a := math.Float32bits(rx[i]) &^ (1 << 31)
		if a > m1 {
			if a >= rq4OutlierNaNFloor {
				continue
			}
			if a > m0 {
				p1, m1 = p0, m0
				p0, m0 = i, a
			} else {
				p1, m1 = i, a
			}
		}
	}
	v0, v1 = rx[p0], rx[p1]
	rx[p0], rx[p1] = 0, 0
	return p0, p1, v0, v1
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
// centering; entries beyond len(mean) are ignored. dst must not alias x.
//
// Both passes are SIMD. Fusing them into one scalar loop looks cheaper — one
// pass instead of two — but the dmu accumulator then carries a loop-carried
// floating-point dependency, so the loop runs at FP-add latency per element
// rather than at load throughput: measured 4.4x slower at d1536 (1.36 vs
// 0.31 ns/elem), and centering was the whole encode gap against uncentered
// RQ4. Splitting the dot out lets it accumulate in parallel lanes; the
// reassociation moves dmu by ~2e-6 relative, far below the quantization
// noise the slot feeds.
func centerInto(dst, x, mean []float32) ([]float32, float32) {
	dst = dst[:len(mean)]
	if len(x) == len(mean) {
		f32.Sub(dst, x, mean)
	} else {
		// Short or over-long input: materialize the zero-padded copy first,
		// then subtract in place. Sub is not documented to support aliasing,
		// so the in-place form stays a plain loop.
		n := copy(dst, x)
		for i := n; i < len(dst); i++ {
			dst[i] = 0
		}
		for i, m := range mean {
			dst[i] -= m
		}
	}
	return dst, dotProduct(dst, mean)
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
	return rq.encode(x, rq.centered())
}

// encode is Encode with the outlier selection switchable. Production always
// passes rq.centered(): a centered code always carries its two outliers, in
// the five metadata bytes the layout reserves for them, and an uncentered one
// never does. The disabled arm exists so benchmarks can measure the outliers'
// contribution against an otherwise identical encode; it writes the same code
// length with a zero correction.
func (rq *FourBitRotationalQuantizer) encode(x []float32, withSidecar bool) []byte {
	outDim := rq.OutputDimension()
	if len(x) == 0 {
		return rq.newCode(outDim)
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
	// The outlier coordinates leave the nibble stream before the interval
	// search, so the interval — and every remaining coordinate's step —
	// tightens. The scalar metadata below still describes the ORIGINAL
	// centered vector; zeroing affects only the nibble codes.
	var op0, op1 int
	var ov0, ov1 float32
	if withSidecar {
		op0, op1, ov0, ov1 = rq4SelectOutliers(rx)
	}
	lower, step, t, codeSum := rq4Interval(rx, scratch)
	code := rq.newCode(outDim)
	if step <= 0 {
		// The input was likely the zero vector or indistinguishable from it.
		// Still record the norm2-slot metadata: the scalar estimator terms
		// stay exact even when the codes degenerate to zero.
		rq.putHeader(code, 0, 0, 0, rq.norm2Slot(x, dmu))
		if withSidecar {
			// Positions are recorded but the deltas encode to zero: a
			// step-relative grid cannot carry raw values. A degenerate vector
			// with meaningful outliers requires <=2 nonzero rotated
			// coordinates, which is pathological.
			rq.writeOutlierSidecar(code, op0, op1, ov0, ov1)
		}
		return code
	}

	packed := code[rq.metaSize : rq.metaSize+outDim/2]
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
	rq.putHeader(code, t*lower, t*step, codeSum, rq.norm2Slot(x, dmu))
	if withSidecar {
		rq.writeOutlierSidecar(code, op0, op1, ov0, ov1)
	}
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
	h := rq.header(b)
	packed := b[rq.metaSize:]
	half := len(packed)
	x := make([]float32, 2*half)
	for i, v := range packed {
		x[i] = h.lower + h.step*float32(v&0x0F)
		x[half+i] = h.lower + h.step*float32(v>>4)
	}
	if rq.centered() {
		// The sidecar coordinates reconstruct as the nibble value plus the
		// stored delta on the alpha*step grid.
		p0, p1, d0, d1 := rq.readOutlierSidecar(b)
		s := rq4OutlierAlpha * h.step
		x[p0] += float32(d0) * s
		x[p1] += float32(d1) * s
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

	// rquery is the rotated centered float query, kept only in centered mode:
	// the outlier correction reads it at the stored positions.
	rquery []float32
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
	var cq rq4QueryCode
	var rquery []float32
	if rq.centered() {
		// Rotate once and keep the float query: it feeds both the 8-bit
		// query encoding and the sidecar correction.
		qx := cq4
		if len(qx) > rq.OutputDimension() {
			qx = qx[:rq.OutputDimension()]
		}
		if len(qx) == 0 {
			cq = rq4QueryCode{
				codes:     make([]byte, rq.OutputDimension()),
				codesInt8: make([]int8, rq.OutputDimension()),
			}
			rquery = make([]float32, rq.OutputDimension())
		} else {
			rquery = rq.rotation.Rotate(qx)
			cq = encodeRotatedQuery(rquery)
			cq.norm2 = dotProduct(qx, qx)
		}
	} else {
		cq = rq.encodeQuery(cq4)
	}
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
		rquery:      rquery,
	}
}

// outlierCorrection is the stored outliers' contribution to the dot-product
// estimate: the int8 deltas decoded on the alpha*step grid times the rotated
// centered float query at the stored positions. It folds into dotEstimate,
// so the metric coefficient (1 on the centered-dot path, 1+l2 otherwise)
// applies exactly as it does to the nibble estimate.
func (d *FourBitRQDistancer) outlierCorrection(x []byte, step float32) float32 {
	p0, p1, d0, d1 := d.rq.readOutlierSidecar(x)
	s := rq4OutlierAlpha * step
	return s * (float32(d0)*d.rquery[p0] + float32(d1)*d.rquery[p1])
}

// Distance estimates the distance between the query and a 4-bit code. Using
// x_i ~ lower_x + step_x*c_i and q_i ~ lower_q + step_q*c'_i the dot product
// expands to D*l_q*l_x + l_x*codeSum_q + l_q*codeSum_x + step_q*step_x*<c',c>.
// The integer dot product <c',c> runs on a fused SIMD kernel that unpacks the
// data nibbles in registers (see dotByteNibbleImpl).
func (d *FourBitRQDistancer) Distance(x []byte) (float32, error) {
	half := len(d.cq.codes) / 2
	if len(x) != d.rq.metaSize+half {
		return 0, errors.Errorf("4-bit code length doesn't match: %d vs %d",
			len(x), d.rq.metaSize+half)
	}
	h := d.rq.header(x)
	dot := dotByteNibbleImpl(d.cq.codes, x[d.rq.metaSize:d.rq.metaSize+half])
	dotEstimate := h.lower*d.a + h.codeSum*d.cq.lower +
		h.step*d.cq.step*float32(dot)
	if d.rq.centered() {
		dotEstimate += d.outlierCorrection(x, h.step)
	}
	if d.centeredDot {
		// dot(x, q) = dot(x-mean, q-mean) + dot(x-mean, mean) + dot(mean, q);
		// the second term rides in the norm2 slot, the third is per-query.
		return d.cos - (dotEstimate + h.norm2 + d.qMeanDot), d.err
	}
	return d.l2*(h.norm2+d.cq.norm2) + d.cos - (1.0+d.l2)*dotEstimate, d.err
}

// distanceScalar is the pure Go fallback using the packed byte-nibble kernel.
// It exists so benchmarks can compare it against the SIMD-assisted Distance.
func (d *FourBitRQDistancer) distanceScalar(x []byte) (float32, error) {
	half := len(d.cq.codes) / 2
	if len(x) != d.rq.metaSize+half {
		return 0, errors.Errorf("4-bit code length doesn't match: %d vs %d",
			len(x), d.rq.metaSize+half)
	}
	h := d.rq.header(x)
	dotEstimate := h.lower*d.a + h.codeSum*d.cq.lower +
		h.step*d.cq.step*float32(dotByteNibbleGo(d.cq.codes, x[d.rq.metaSize:d.rq.metaSize+half]))
	if d.rq.centered() {
		dotEstimate += d.outlierCorrection(x, h.step)
	}
	if d.centeredDot {
		return d.cos - (dotEstimate + h.norm2 + d.qMeanDot), d.err
	}
	return d.l2*(h.norm2+d.cq.norm2) + d.cos - (1.0+d.l2)*dotEstimate, d.err
}

func (d *FourBitRQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.query) > 0 {
		return d.distancer.SingleDist(d.query, x)
	}
	cx := d.rq.Encode(x)
	return d.Distance(cx)
}

func (rq *FourBitRotationalQuantizer) DistanceBetweenCompressedVectors(x, y []byte) (float32, error) {
	dim := rq.OutputDimension()
	expected := rq.metaSize + dim/2
	if len(x) != expected || len(y) != expected {
		return 0, errors.Errorf("4-bit code lengths don't match quantizer: %d and %d, want %d",
			len(x), len(y), expected)
	}
	hx, hy := rq.header(x), rq.header(y)
	dotEstimate := rq.dotEstimateBetween(x, y, hx, hy)
	if rq.mean != nil && rq.l2 == 0 {
		// dot(x, y) = dot(x', y') + dot(x', mean) + dot(y', mean) + |mean|^2
		// with x' = x-mean; both cross terms ride in the norm2 slots.
		return rq.cos - (dotEstimate + hx.norm2 + hy.norm2 + rq.meanNorm2), rq.err
	}
	return rq.l2*(hx.norm2+hy.norm2) + rq.cos - (1.0+rq.l2)*dotEstimate, rq.err
}

// dotEstimateBetween estimates the dot product of two stored codes in the
// rotated (centered) space. For centered codes the result is exactly the dot
// product of the two reconstructions Restore returns, outliers included.
func (rq *FourBitRotationalQuantizer) dotEstimateBetween(x, y []byte, hx, hy rq4Header) float32 {
	dim := rq.OutputDimension()
	a := float32(dim) * hx.lower * hy.lower
	b := hx.lower * hy.codeSum
	c := hy.lower * hx.codeSum
	d := hx.step * hy.step * float32(dotNibbleNibbleImpl(x[rq.metaSize:rq.metaSize+dim/2], y[rq.metaSize:rq.metaSize+dim/2]))
	dotEstimate := a + b + c + d
	if rq.centered() {
		dotEstimate += rq.outlierCrossCorrection(x, y, hx, hy)
	}
	return dotEstimate
}

// outlierCrossCorrection is the two-sided outlier correction of the
// compressed-compressed dot estimate:
//
//	dot(x,y) ≈ dotEst + Σ_k δx_k·ŷ[px_k] + Σ_j δy_j·x̂[py_j] + Σ_{px_k=py_j} δx_k·δy_j
//
// with ŷ[p] the nibble reconstruction of coordinate p. The collision term
// makes the estimate exact when both codes store an outlier at the same
// coordinate: the reconstruction there is (nibble + δ) on both sides.
func (rq *FourBitRotationalQuantizer) outlierCrossCorrection(x, y []byte, hx, hy rq4Header) float32 {
	xp0, xp1, xq0, xq1 := rq.readOutlierSidecar(x)
	yp0, yp1, yq0, yq1 := rq.readOutlierSidecar(y)
	dx0 := float32(xq0) * rq4OutlierAlpha * hx.step
	dx1 := float32(xq1) * rq4OutlierAlpha * hx.step
	dy0 := float32(yq0) * rq4OutlierAlpha * hy.step
	dy1 := float32(yq1) * rq4OutlierAlpha * hy.step
	corr := dx0*rq.nibbleValue(y, hy, xp0) + dx1*rq.nibbleValue(y, hy, xp1) +
		dy0*rq.nibbleValue(x, hx, yp0) + dy1*rq.nibbleValue(x, hx, yp1)
	if xp0 == yp0 {
		corr += dx0 * dy0
	}
	if xp0 == yp1 {
		corr += dx0 * dy1
	}
	if xp1 == yp0 {
		corr += dx1 * dy0
	}
	if xp1 == yp1 {
		corr += dx1 * dy1
	}
	return corr
}

// nibbleValue reconstructs coordinate i of a code under its header (plane
// layout: dimensions [0, D/2) in the low nibbles, [D/2, D) in the high).
func (rq *FourBitRotationalQuantizer) nibbleValue(c []byte, h rq4Header, i int) float32 {
	half := rq.OutputDimension() / 2
	var v byte
	if i < half {
		v = c[rq.metaSize+i] & 0x0F
	} else {
		v = c[rq.metaSize+i-half] >> 4
	}
	return h.lower + h.step*float32(v)
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
	Bits         uint32 `json:"bits"`
	Centering    bool   `json:"centering"`
	MetadataSize int    `json:"metadataSize"`
}

func (s RQ4Stats) CompressionType() string {
	return "rq"
}

func (s RQ4Stats) CompressionRatio(dimensionality int) float64 {
	// Original size = dim * 4 bytes (float32). Compressed size = the code's
	// metadata prefix (16 bytes, or 20 for centered codes above the compact
	// layout's dimension) + half a byte per dimension.
	metaSize := s.MetadataSize
	if metaSize == 0 {
		metaSize = RQ4MetadataSize
	}
	originalSize := dimensionality * 4
	compressedSize := metaSize + dimensionality/2
	return float64(originalSize) / float64(compressedSize)
}

func (rq *FourBitRotationalQuantizer) Stats() CompressionStats {
	return RQ4Stats{Bits: 4, Centering: rq.centered(), MetadataSize: rq.metaSize}
}
