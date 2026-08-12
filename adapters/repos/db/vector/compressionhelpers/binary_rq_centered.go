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
	"math"
	"math/rand/v2"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// CenteredBinaryRotationalQuantizer is the 1-bit rotational quantizer with
// dataset-mean centering: codes are the sign bits of R(x - mean). Centering
// is what makes 1-bit prefix and shared-store schemes viable on anisotropic
// embedding models (see docs/uncentered-binary-quantization.md); it has
// never shipped, so the centered code defines its own header layout from
// day one rather than extending the uncentered one:
//
//	word 0, bits  0..31: step  = ‖x-μ‖²/‖x-μ‖₁ of the rotated centered
//	                     vector, float32 (exact, it scales every estimate)
//	word 0, bits 32..47: squaredNorm = ‖x-μ‖², bfloat16
//	word 0, bits 48..63: ⟨μ, x⟩, bfloat16 (the additive centering
//	                     correction; RNE error ≤2⁻⁸ is far below 1-bit
//	                     estimator noise)
//	words 1..: sign bits, 64 dimensions per word
//
// The layout is dispatched by construction — this type writes and reads
// only the centered layout, BinaryRotationalQuantizer only the uncentered
// one — never sniffed from bytes, mirroring the rq4c centered pattern
// (trengrj/4bit-centering). Total header stays 8 bytes, so centered and
// uncentered codes of one width have identical sizes and stride math.
//
// Scope note: this type deliberately carries no config, activation or
// persistence plumbing; the mean arrives as a constructor parameter and the
// wiring (RQData.Mean, WAL record, compress() training pass) lands when the
// branch is rebased onto the rq4c centering infrastructure.
type CenteredBinaryRotationalQuantizer struct {
	inputDim    uint32 // padded to minCodeBits like the uncentered quantizer
	originalDim uint32
	rotation    *compression.FastRotation
	distancer   distancer.Provider
	rounding    []float32
	mean        []float32
	meanNorm2   float32 // ⟨μ,μ⟩, query-side constant of the correction

	l2     float32 // indicator: 1 for l2-squared
	cos    float32 // indicator: 1 for cosine-dot
	dotFam float32 // indicator: 1 for dot-based distancers, which need the
	// centering correction; l2 is translation-invariant and needs none
}

// NewCenteredBinaryRotationalQuantizer builds the centered 1-bit quantizer.
// mean must be the dataset mean over exactly inputDim dimensions; a nil
// mean is a contract error (use BinaryRotationalQuantizer for uncentered).
func NewCenteredBinaryRotationalQuantizer(inputDim int, seed uint64, mean []float32, distancer distancer.Provider) (*CenteredBinaryRotationalQuantizer, error) {
	if len(mean) != inputDim {
		return nil, errors.Errorf("centering mean has %d dims, input has %d", len(mean), inputDim)
	}

	originalDim := inputDim
	if inputDim < minCodeBits {
		inputDim = minCodeBits
	}
	rotation := NewFastRotation(inputDim, rotationRounds, seed)

	cos, l2, err := distancerIndicatorsAndError(distancer)
	if err != nil {
		return nil, err
	}

	// Same randomized rounding for the multi-bit query codes as the
	// uncentered quantizer, same derivation from the seed.
	rounding := make([]float32, rotation.OutputDim)
	rng := rand.New(rand.NewPCG(seed, 0x4f8ebf70e130707f))
	for i := range rounding {
		rounding[i] = rng.Float32()
	}

	var mu2 float32
	for _, m := range mean {
		mu2 += m * m
	}

	return &CenteredBinaryRotationalQuantizer{
		inputDim:    uint32(inputDim),
		originalDim: uint32(originalDim),
		rotation:    rotation,
		distancer:   distancer,
		rounding:    rounding,
		mean:        mean,
		meanNorm2:   mu2,
		cos:         cos,
		l2:          l2,
		dotFam:      1 - l2,
	}, nil
}

// crqHeader is the decoded per-code metadata, independent of the packed
// representation.
type crqHeader struct {
	step  float32
	norm2 float32 // ‖x-μ‖²
	muX   float32 // ⟨μ, x⟩
}

func (rq *CenteredBinaryRotationalQuantizer) putHeader(c []uint64, step, norm2, muX float32) {
	c[0] = uint64(math.Float32bits(step)) |
		uint64(float32ToBFloat16(norm2))<<32 |
		uint64(float32ToBFloat16(muX))<<48
}

func (rq *CenteredBinaryRotationalQuantizer) header(c []uint64) crqHeader {
	return crqHeader{
		step:  math.Float32frombits(uint32(c[0])),
		norm2: bfloat16ToFloat32(uint16(c[0] >> 32)),
		muX:   bfloat16ToFloat32(uint16(c[0] >> 48)),
	}
}

// centerInto writes x - mean into dst (allocating it) and returns it along
// with ⟨μ, x⟩ of the ORIGINAL vector, accumulated in one pass.
func (rq *CenteredBinaryRotationalQuantizer) centerInto(x []float32) ([]float32, float32) {
	n := len(x)
	if n > len(rq.mean) {
		n = len(rq.mean)
	}
	dst := make([]float32, len(x))
	var muX float32
	for i := 0; i < n; i++ {
		dst[i] = x[i] - rq.mean[i]
		muX += x[i] * rq.mean[i]
	}
	copy(dst[n:], x[n:])
	return dst, muX
}

func (rq *CenteredBinaryRotationalQuantizer) Encode(x []float32) []uint64 {
	cx, muX := rq.centerInto(x)
	rx := rq.rotation.Rotate(cx)
	d := len(rx)
	code := make([]uint64, oneBitFieldWords+d/64)
	codeBits := code[oneBitFieldWords:]
	blocks := d / 64
	var l2NormSquared float32
	var l1Norm float32
	i := 0
	for b := 0; b < blocks; b++ {
		var word uint64
		for bit := uint64(1); bit != 0; bit <<= 1 {
			if rx[i] > 0 {
				word |= bit
				l1Norm += rx[i]
			} else {
				l1Norm += -rx[i]
			}
			l2NormSquared += rx[i] * rx[i]
			i++
		}
		codeBits[b] = word
	}
	step := float32(0)
	if l1Norm > 0 {
		step = l2NormSquared / l1Norm
	}
	// The header is written even for the degenerate x == μ case: the
	// centering correction ⟨μ,x⟩ still applies to the distance.
	rq.putHeader(code, step, l2NormSquared, muX)
	return code
}

// Restore returns the rotated-space approximation of the CENTERED vector
// (±‖x-μ‖/√D per the sign bits).
func (rq *CenteredBinaryRotationalQuantizer) Restore(b []uint64) []float32 {
	h := rq.header(b)
	dim := 64 * (len(b) - oneBitFieldWords)
	avgNorm := float32(math.Sqrt(float64(h.norm2))) / float32(math.Sqrt(float64(dim)))
	x := make([]float32, dim)
	codeBits := b[oneBitFieldWords:]
	for i := 0; i < dim; i++ {
		if (codeBits[i/64] & (1 << (uint(i) % 64))) != 0 {
			x[i] = avgNorm
		} else {
			x[i] = -avgNorm
		}
	}
	return x
}

func (rq *CenteredBinaryRotationalQuantizer) Decode(compressed []uint64) []float32 {
	restored := rq.Restore(compressed)
	unrotated := rq.rotation.UnRotateInPlace(restored)
	out := unrotated[:rq.originalDim]
	for i := range out {
		if i < len(rq.mean) {
			out[i] += rq.mean[i]
		}
	}
	return out
}

// encodeQuery builds the 5-bit multi-bit code of the CENTERED query, same
// format and randomized rounding as the uncentered quantizer.
func (rq *CenteredBinaryRotationalQuantizer) encodeQuery(q []float32) RQMultiBitCode {
	cq, _ := rq.centerInto(q)
	rx := rq.rotation.Rotate(cq)
	abs := maxAbs(rx)
	if abs == 0 {
		return RQMultiBitCode{}
	}
	step := abs / 31
	blocks := len(rx) >> 6
	bits0 := make([]uint64, blocks)
	bits1 := make([]uint64, blocks)
	bits2 := make([]uint64, blocks)
	bits3 := make([]uint64, blocks)
	bits4 := make([]uint64, blocks)
	var squaredNorm float32
	i := 0
	for b := 0; b < blocks; b++ {
		var b0, b1, b2, b3, b4 uint64
		for bit := uint64(1); bit != 0; bit <<= 1 {
			squaredNorm += rx[i] * rx[i]
			c := uint64(((rx[i] + abs) / (2 * step)) + rq.rounding[i])
			if c&1 != 0 {
				b0 |= bit
			}
			if c&2 != 0 {
				b1 |= bit
			}
			if c&4 != 0 {
				b2 |= bit
			}
			if c&8 != 0 {
				b3 |= bit
			}
			if c&16 != 0 {
				b4 |= bit
			}
			i++
		}
		bits0[b] = b0
		bits1[b] = b1
		bits2[b] = b2
		bits3[b] = b3
		bits4[b] = b4
	}
	return RQMultiBitCode{
		Dimension:   len(rx),
		SquaredNorm: squaredNorm,
		Step:        step,
		bits0:       bits0,
		bits1:       bits1,
		bits2:       bits2,
		bits3:       bits3,
		bits4:       bits4,
	}
}

// CenteredBinaryRQDistancer computes distances between a centered multi-bit
// query code and centered 1-bit data codes.
type CenteredBinaryRQDistancer struct {
	query     []float32
	distancer distancer.Provider
	rq        *CenteredBinaryRotationalQuantizer
	cq        RQMultiBitCode
	corrQ     float32 // ⟨μ,q⟩ - ⟨μ,μ⟩, query-side constant of the correction
}

func (rq *CenteredBinaryRotationalQuantizer) NewDistancer(q []float32) *CenteredBinaryRQDistancer {
	var corrQ float32
	if len(q) > 0 {
		n := len(q)
		if n > len(rq.mean) {
			n = len(rq.mean)
		}
		var muQ float32
		for i := 0; i < n; i++ {
			muQ += q[i] * rq.mean[i]
		}
		corrQ = muQ - rq.meanNorm2
	}
	return &CenteredBinaryRQDistancer{
		query:     q,
		distancer: rq.distancer,
		rq:        rq,
		cq:        rq.encodeQuery(q),
		corrQ:     corrQ,
	}
}

// Distance estimates the distance between the query and a centered 1-bit
// code. The multi-bit/1-bit dot product estimates ⟨x-μ, q-μ⟩; for
// dot-based distancers the additive correction ⟨μ,x⟩ + ⟨μ,q⟩ - ⟨μ,μ⟩
// restores an estimate of ⟨x, q⟩. For l2 no correction is needed:
// distances are translation-invariant, so the centered norms and dot are
// used directly.
func (d *CenteredBinaryRQDistancer) Distance(x []uint64) (float32, error) {
	h := d.rq.header(x)
	codeBits := x[oneBitFieldWords:]
	corr := d.rq.dotFam * (h.muX + d.corrQ)

	const hammingDistSIMDThreshold = 512
	var dot float32
	if d.cq.Dimension < hammingDistSIMDThreshold {
		di := 31 * d.cq.Dimension
		di -= HammingDist(d.cq.bits0, codeBits) << 1
		di -= HammingDist(d.cq.bits1, codeBits) << 2
		di -= HammingDist(d.cq.bits2, codeBits) << 3
		di -= HammingDist(d.cq.bits3, codeBits) << 4
		di -= HammingDist(d.cq.bits4, codeBits) << 5
		dot = float32(di)
	} else {
		dot = float32(31 * d.cq.Dimension)
		dot -= 2 * HammingDistSIMD(d.cq.bits0, codeBits)
		dot -= 4 * HammingDistSIMD(d.cq.bits1, codeBits)
		dot -= 8 * HammingDistSIMD(d.cq.bits2, codeBits)
		dot -= 16 * HammingDistSIMD(d.cq.bits3, codeBits)
		dot -= 32 * HammingDistSIMD(d.cq.bits4, codeBits)
	}
	dotEstimate := d.cq.Step * h.step * dot
	return d.rq.l2*(h.norm2+d.cq.SquaredNorm) + d.rq.cos - (1.0+d.rq.l2)*dotEstimate - corr, nil
}

func (d *CenteredBinaryRQDistancer) DistanceToFloat(x []float32) (float32, error) {
	if len(d.query) > 0 {
		return d.distancer.SingleDist(d.query, x)
	}
	return d.Distance(d.rq.Encode(x))
}

// DistanceBetweenCompressedVectors estimates the distance between two
// centered 1-bit codes via the Hamming angle estimate on the centered
// vectors, plus the centering correction for dot-based distancers. The
// dimension comes from the code itself, never from the quantizer's rotation
// width.
func (rq *CenteredBinaryRotationalQuantizer) DistanceBetweenCompressedVectors(x, y []uint64) (float32, error) {
	if len(x) != len(y) {
		return 0, errors.Errorf("code lengths don't match: %d vs %d", len(x), len(y))
	}
	hx, hy := rq.header(x), rq.header(y)
	dim := 64 * (len(x) - oneBitFieldWords)
	fractionDiff := float64(HammingDist(x[oneBitFieldWords:], y[oneBitFieldWords:])) / float64(dim)
	cosEstimate := math.Cos(math.Pi * fractionDiff)
	dotEstimate := float32(math.Sqrt(float64(hx.norm2)) * math.Sqrt(float64(hy.norm2)) * cosEstimate)
	corr := rq.dotFam * (hx.muX + hy.muX - rq.meanNorm2)
	return rq.l2*(hx.norm2+hy.norm2) + rq.cos - (1.0+rq.l2)*dotEstimate - corr, nil
}
