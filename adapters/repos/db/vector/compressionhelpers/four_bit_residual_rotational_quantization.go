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
	"math/bits"

	"github.com/pkg/errors"
	"github.com/tphakala/simd/f32"
	"github.com/tphakala/simd/i8"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// FourBitResidualQuantizer is an experimental extension of the 4-bit
// rotational quantizer with a TurboQuant-style 1-bit residual stage (see
// https://arxiv.org/abs/2504.19874). After the 4-bit primary quantization the
// residual r = rx - x̂ is rotated by an independent second random rotation
// and quantized to one bit per dimension as
//
//	r̂2_i = sigma * sign(r2_i), sigma = (sum_i |r2_i|) / D, r2 = R2*r
//
// where sigma is the least-squares scale of the sign vector. The second
// rotation is essential on real embeddings: the primary residual inherits
// heavy-tailed structure from the clamped outlier dimensions, which makes a
// same-basis sign quantizer improve the median estimate but fatten the error
// tail (measured: p99 +30% on dbpedia-openai). Rotating the residual
// re-Gaussianizes it, which is exactly the quantized Johnson-Lindenstrauss
// stage of TurboQuant.
//
// Distance estimation adds the correction <q̂2, r̂2> to the primary estimate,
// where q̂2 is an 8-bit encoding of the query in the residual basis. Storage
// cost: one extra bit per dimension plus eight metadata bytes (4.5 bits per
// dimension total, against 4.1 for the plain 4-bit quantizer at 1536
// dimensions).
type FourBitResidualQuantizer struct {
	*FourBitRotationalQuantizer
	rotation2 *compression.FastRotation
}

func NewFourBitResidualQuantizer(inputDim int, seed uint64, distancer distancer.Provider) *FourBitResidualQuantizer {
	base := NewFourBitRotationalQuantizer(inputDim, seed, distancer)
	// The residual rotation operates on the (padded) output basis of the
	// primary rotation and must use an independent seed.
	rotation2 := NewFastRotation(base.OutputDimension(), 3, seed^0x9e3779b97f4a7c15)
	return &FourBitResidualQuantizer{
		FourBitRotationalQuantizer: base,
		rotation2:                  rotation2,
	}
}

// RQ4RCode extends RQ4Code with a residual plane:
//
//	[RQ4Code: 16 metadata bytes + D/2 nibble bytes]
//	[sigma: float32][signPopcount: uint32]
//	[D/8 sign-bit bytes, bit j of byte i = sign of dimension 8i+j]
//
// The sign popcount is stored so the distance path can correct the -128
// offset of the query codes without counting bits per candidate.
type RQ4RCode []byte

const rq4ResidualMetadataSize = 8

// rq4rDimension recovers D from the total code length
// len = 16 + D/2 + 8 + D/8 = 24 + 5D/8.
func rq4rDimension(codeLen int) int {
	return (codeLen - RQ4MetadataSize - rq4ResidualMetadataSize) * 8 / 5
}

func rq4rCodeLen(d int) int {
	return RQ4MetadataSize + d/2 + rq4ResidualMetadataSize + d/8
}

// Base returns the embedded plain 4-bit code.
func (c RQ4RCode) Base() RQ4Code {
	d := rq4rDimension(len(c))
	return RQ4Code(c[:RQ4MetadataSize+d/2])
}

func (c RQ4RCode) Sigma() float32 {
	d := rq4rDimension(len(c))
	return getFloat32(c, RQ4MetadataSize+d/2)
}

func (c RQ4RCode) setSigma(x float32) {
	d := rq4rDimension(len(c))
	putFloat32(c, RQ4MetadataSize+d/2, x)
}

func (c RQ4RCode) SignPopcount() uint32 {
	d := rq4rDimension(len(c))
	return binary.BigEndian.Uint32(c[RQ4MetadataSize+d/2+4:])
}

func (c RQ4RCode) setSignPopcount(x uint32) {
	d := rq4rDimension(len(c))
	binary.BigEndian.PutUint32(c[RQ4MetadataSize+d/2+4:], x)
}

// SignBits returns the residual sign plane.
func (c RQ4RCode) SignBits() []byte {
	d := rq4rDimension(len(c))
	return c[RQ4MetadataSize+d/2+rq4ResidualMetadataSize:]
}

func (c RQ4RCode) Dimension() int {
	return rq4rDimension(len(c))
}

// bitExpandLUT maps a byte to eight 0x00/0x01 bytes, one per bit, in
// little-endian dimension order. Used to expand the residual sign plane for
// the int8 SIMD dot product.
var bitExpandLUT = func() [256]uint64 {
	var lut [256]uint64
	for b := range lut {
		var v uint64
		for j := range 8 {
			if b&(1<<j) != 0 {
				v |= 1 << (8 * j)
			}
		}
		lut[b] = v
	}
	return lut
}()

func (rq *FourBitResidualQuantizer) Encode(x []float32) []byte {
	outDim := rq.OutputDimension()
	code := make([]byte, rq4rCodeLen(outDim))
	rc := RQ4RCode(code)
	if len(x) == 0 {
		return code
	}
	if len(x) > outDim {
		x = x[:outDim]
	}

	rx := rq.rotation.Rotate(x)
	scratch := rq.scratch.Get().(*rq4Scratch)
	defer rq.scratch.Put(scratch)
	lower, step, t, codeSum := rq4Interval(rx, scratch)
	if step <= 0 {
		// Degenerate input: zero primary code and zero residual.
		return code
	}

	base := rc.Base()
	packed := base.Packed()
	half := len(packed)
	ci := scratch.ci
	for i := range packed {
		packed[i] = byte(ci[i]) | byte(ci[half+i])<<4
	}
	base.setLower(t * lower)
	base.setStep(t * step)
	base.setCodeSum(t * step * codeSum)
	base.setNorm2(f32.SumOfSquares(x))

	// Residual stage: r = rx - x̂ with x̂_i = t*(lower + step*ci_i). The
	// float view of the codes is still in scratch.cf from the winning
	// evaluation of rq4Interval.
	res := scratch.cf
	f32.Scale(res, res, t*step)
	f32.AddScalar(res, res, t*lower)
	f32.Sub(res, rx, res)

	// Rotate the residual into an independent basis before the 1-bit
	// quantization to spread the clamped-outlier energy uniformly.
	res2 := rq.rotation2.Rotate(res)

	var l1 float32
	var popcount uint32
	signBits := rc.SignBits()
	for i := range signBits {
		var sb byte
		for j := range 8 {
			r := res2[8*i+j]
			if r > 0 {
				sb |= 1 << j
				l1 += r
			} else {
				l1 -= r
			}
		}
		popcount += uint32(bits.OnesCount8(sb))
		signBits[i] = sb
	}
	rc.setSigma(l1 / float32(outDim))
	rc.setSignPopcount(popcount)
	return code
}

// Restore returns the primary-rotation-space approximation including the
// residual correction, which is un-rotated from the residual basis.
func (rq *FourBitResidualQuantizer) Restore(b []byte) []float32 {
	rc := RQ4RCode(b)
	x := rq.FourBitRotationalQuantizer.Restore(rc.Base())
	sigma := rc.Sigma()
	signBits := rc.SignBits()
	r2 := make([]float32, len(x))
	for i := range r2 {
		if signBits[i/8]&(1<<(i%8)) != 0 {
			r2[i] = sigma
		} else {
			r2[i] = -sigma
		}
	}
	r1 := rq.rotation2.UnRotateInPlace(r2)
	f32.Add(x, x, r1)
	return x
}

func (rq *FourBitResidualQuantizer) Decode(compressed []byte) []float32 {
	unrotated := rq.rotation.UnRotateInPlace(rq.Restore(compressed))
	if int(rq.inputDim) < len(unrotated) {
		return unrotated[:rq.inputDim]
	}
	return unrotated
}

// FourBitResidualRQDistancer extends the asymmetric 4-bit distancer with the
// residual correction. NOT safe for concurrent use (scratch buffers).
type FourBitResidualRQDistancer struct {
	base *FourBitRQDistancer

	// 8-bit encoding of the query in the residual basis, plus the
	// precomputed sum of its reconstructed entries.
	cq2 rq4QueryCode
	a2  float32

	// Expansion buffer for the residual sign plane. signScratchInt8 is an
	// unsafe view of signScratch, same trick as the nibble scratch.
	signScratch     []byte
	signScratchInt8 []int8
}

func (rq *FourBitResidualQuantizer) NewDistancer(q []float32) *FourBitResidualRQDistancer {
	base := rq.FourBitRotationalQuantizer.NewDistancer(q)
	outDim := rq.OutputDimension()

	// Encode the query in the residual basis. The rotations compose:
	// q2 = R2*(R1*q).
	var cq2 rq4QueryCode
	if len(q) == 0 {
		cq2 = rq4QueryCode{
			codes:     make([]byte, outDim),
			codesInt8: make([]int8, outDim),
		}
	} else {
		qq := q
		if len(qq) > outDim {
			qq = qq[:outDim]
		}
		cq2 = encodeRotatedQuery(rq.rotation2.Rotate(rq.rotation.Rotate(qq)))
	}

	signScratch := make([]byte, outDim)
	return &FourBitResidualRQDistancer{
		base:            base,
		cq2:             cq2,
		a2:              float32(outDim)*cq2.lower + cq2.codeSum,
		signScratch:     signScratch,
		signScratchInt8: bytesAsInt8(signScratch),
	}
}

// Distance estimates the distance between the query and a residual-augmented
// 4-bit code. The primary estimate follows FourBitRQDistancer.Distance; the
// residual correction is
//
//	<q̂, r̂> = sigma * (2*<q̂, s> - sum(q̂))
//
// with s the 0/1 sign indicators and
//
//	<q̂, s> = l_q*P + s_q*(sum_i (cq_i - 128)*s_i + 128*P)
//
// where P is the stored sign popcount. The masked code sum runs as an int8
// SIMD dot product against the LUT-expanded sign plane.
func (d *FourBitResidualRQDistancer) Distance(x []byte) (float32, error) {
	b := d.base
	dim := len(b.cq.codes)
	if len(x) != rq4rCodeLen(dim) {
		return 0, errors.Errorf("4-bit residual code length doesn't match: %d vs %d",
			len(x), rq4rCodeLen(dim))
	}
	rc := RQ4RCode(x)
	cx := rc.Base()

	// Primary 4-bit estimate (same as FourBitRQDistancer.Distance).
	packed := cx.Packed()
	half := len(packed)
	scratch := b.scratch
	const loMask = 0x0F0F0F0F0F0F0F0F
	j := 0
	for ; j+8 <= half; j += 8 {
		v := binary.LittleEndian.Uint64(packed[j:])
		binary.LittleEndian.PutUint64(scratch[j:], v&loMask)
		binary.LittleEndian.PutUint64(scratch[half+j:], (v>>4)&loMask)
	}
	for ; j < half; j++ {
		scratch[j] = packed[j] & 0x0F
		scratch[half+j] = packed[j] >> 4
	}
	dot := i8.DotProduct(b.cq.codesInt8, b.scratchInt8)
	dotEstimate := cx.Lower()*b.a + cx.CodeSum()*b.b +
		cx.Step()*b.cq.step*float32(dot)

	// Residual correction in the residual basis: <q̂2, r̂2> with
	// r̂2 = sigma*(2s - 1) for sign indicators s.
	sigma := rc.Sigma()
	if sigma > 0 {
		signBits := rc.SignBits()
		sign := d.signScratch
		for i, v := range signBits {
			binary.LittleEndian.PutUint64(sign[8*i:], bitExpandLUT[v])
		}
		maskedDot := i8.DotProduct(d.cq2.codesInt8, d.signScratchInt8)
		p := float32(rc.SignPopcount())
		qsDot := d.cq2.lower*p + d.cq2.step*(float32(maskedDot)+128*p)
		dotEstimate += sigma * (2*qsDot - d.a2)
	}

	return b.l2*(cx.Norm2()+b.cq.norm2) + b.cos - (1.0+b.l2)*dotEstimate, b.err
}

func (d *FourBitResidualRQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.base.query) > 0 {
		return d.base.distancer.SingleDist(d.base.query, x)
	}
	return 0, errors.New("residual distancer has no query vector")
}

type RQ4RStats struct {
	Bits uint32 `json:"bits"`
}

func (s RQ4RStats) CompressionType() string {
	return "rq"
}

func (s RQ4RStats) CompressionRatio(dimensionality int) float64 {
	originalSize := dimensionality * 4
	return float64(originalSize) / float64(rq4rCodeLen(dimensionality))
}

func (rq *FourBitResidualQuantizer) Stats() CompressionStats {
	return RQ4RStats{Bits: 5}
}
