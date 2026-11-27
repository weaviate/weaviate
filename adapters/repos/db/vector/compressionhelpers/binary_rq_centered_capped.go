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
	"github.com/pkg/errors"
)

// CappedCenteredBinaryRQ is a view of a centered 1-bit quantizer whose codes
// keep only the first bits of the sign bits. The rotation still runs at full
// width: a random rotation spreads the dot product evenly over the rotated
// dimensions, so the first bits of them estimate it without bias once scaled
// by width/bits. Fewer bits make the estimate noisier, not different.
//
// A capped code has the layout of a full one, with fewer words of sign bits:
//
//	word 0: the header of CenteredBinaryRotationalQuantizer, where step is
//	        computed over the kept dimensions only (it scales their dot
//	        product) and squaredNorm and ⟨μ, x-μ⟩ are those of the whole
//	        vector (they are exact corrections, independent of the bits)
//	words 1..: the first bits sign bits, 64 per word
//
// Capped at the full width, codes and distances are bit for bit those of the
// quantizer itself.
type CappedCenteredBinaryRQ struct {
	rq   *CenteredBinaryRotationalQuantizer
	bits int
}

// Capped returns the view of the quantizer that keeps bits sign bits per
// code. bits must be a positive multiple of 64 no larger than the width of
// the rotation.
func (rq *CenteredBinaryRotationalQuantizer) Capped(bits int) (*CappedCenteredBinaryRQ, error) {
	width := int(rq.rotation.OutputDim)
	if bits <= 0 || bits%64 != 0 || bits > width {
		return nil, errors.Errorf("capped rq1 keeps a positive multiple of 64 bits up to %d, got %d", width, bits)
	}
	return &CappedCenteredBinaryRQ{rq: rq, bits: bits}, nil
}

// Bits is the number of sign bits a code keeps.
func (c *CappedCenteredBinaryRQ) Bits() int {
	return c.bits
}

// Encode is the Encode of the quantizer, storing only the first sign bits.
func (c *CappedCenteredBinaryRQ) Encode(x []float32) []uint64 {
	rq := c.rq
	scratch := rq.scratch.Get().(*crq1Scratch)
	defer rq.scratch.Put(scratch)
	cx, dmu := centerInto(scratch.cx, x, rq.mean)
	rx := rq.rotation.RotateInto(cx, scratch.rx)

	blocks := c.bits / 64
	code := make([]uint64, oneBitFieldWords+blocks)
	codeBits := code[oneBitFieldWords:]
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
	// The norm of the whole vector: the dimensions that are not kept still
	// count towards it.
	fullNormSquared := l2NormSquared
	for ; i < len(rx); i++ {
		fullNormSquared += rx[i] * rx[i]
	}
	rq.putHeader(code, step, fullNormSquared, dmu)
	return code
}

// CappedCenteredBinaryRQDistancer estimates distances between a query and
// capped codes. The query is encoded at full width, like for full codes; only
// the part that faces the kept bits is used.
type CappedCenteredBinaryRQDistancer struct {
	full  *CenteredBinaryRQDistancer
	bits  int
	words int
	// scale takes the dot product of the kept dimensions to that of all of
	// them.
	scale float32
}

func (c *CappedCenteredBinaryRQ) NewDistancer(q []float32) *CappedCenteredBinaryRQDistancer {
	full := c.rq.NewDistancer(q)
	distancer := &CappedCenteredBinaryRQDistancer{
		full:  full,
		bits:  c.bits,
		words: c.bits / 64,
		scale: 1,
	}
	// A zero query encodes to an empty code, see encodeRotatedQuery.
	if full.cq.Dimension > 0 {
		distancer.scale = float32(full.cq.Dimension) / float32(c.bits)
	}
	return distancer
}

// Distance estimates the distance between the query and a capped code. It
// is the estimate of CenteredBinaryRQDistancer.Distance with the dot product
// taken over the kept dimensions and scaled up to the full width.
func (d *CappedCenteredBinaryRQDistancer) Distance(x []uint64) (float32, error) {
	if len(x) != oneBitFieldWords+d.words {
		return 0, errors.Errorf("capped rq1 code has %d words, expected %d", len(x), oneBitFieldWords+d.words)
	}
	rq, cq := d.full.rq, &d.full.cq
	h := rq.header(x)
	codeBits := x[oneBitFieldWords:]
	var corr float32
	if rq.dotFam != 0 {
		corr = h.dmu + d.full.corrQ
	}
	if cq.Dimension == 0 {
		// The zero query: nothing to take a dot product with.
		return rq.l2*h.norm2 + rq.cos - corr, nil
	}

	const hammingDistSIMDThreshold = 512
	var dot float32
	if d.bits < hammingDistSIMDThreshold {
		di := 31 * d.bits
		di -= HammingDist(cq.bits0[:d.words], codeBits) << 1
		di -= HammingDist(cq.bits1[:d.words], codeBits) << 2
		di -= HammingDist(cq.bits2[:d.words], codeBits) << 3
		di -= HammingDist(cq.bits3[:d.words], codeBits) << 4
		di -= HammingDist(cq.bits4[:d.words], codeBits) << 5
		dot = float32(di)
	} else {
		dot = float32(31 * d.bits)
		dot -= 2 * HammingDistSIMD(cq.bits0[:d.words], codeBits)
		dot -= 4 * HammingDistSIMD(cq.bits1[:d.words], codeBits)
		dot -= 8 * HammingDistSIMD(cq.bits2[:d.words], codeBits)
		dot -= 16 * HammingDistSIMD(cq.bits3[:d.words], codeBits)
		dot -= 32 * HammingDistSIMD(cq.bits4[:d.words], codeBits)
	}
	dotEstimate := cq.Step * h.step * dot * d.scale
	return rq.l2*(h.norm2+cq.SquaredNorm) + rq.cos - (1.0+rq.l2)*dotEstimate - corr, nil
}
