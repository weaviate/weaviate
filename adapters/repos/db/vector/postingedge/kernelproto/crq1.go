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

package kernelproto

import (
	"math"
	"math/bits"

	"github.com/pkg/errors"
)

const (
	// Planes is the number of bit planes of a query: its values have 5 bits.
	Planes = 5
	// maxValue is the largest query value.
	maxValue = 1<<Planes - 1
	// chunkWords is what one pass of the fused kernel reads of a code: 128
	// bits.
	chunkWords = 2
	// maxFusedChunks bounds the code width of the fused kernel, which counts
	// each plane in bytes: 8 per chunk, at most 255.
	maxFusedChunks = 31
	// BlockCodes is the number of codes of a FastScan block.
	BlockCodes = 32
	// groupBits is the number of dimensions a FastScan table covers.
	groupBits = 4
	// maxScanDims bounds the code width of FastScan, whose sums are 16 bits:
	// at most 31 per dimension.
	maxScanDims = math.MaxUint16 / maxValue
)

// Query is a 5-bit CRQ1 query code laid out for the kernels, with the
// query-side constants of the estimate.
type Query struct {
	// Words is the width of a data code in 64-bit words, without its header.
	Words int
	// Planes[p] holds bit p of the value of every dimension.
	Planes [Planes][]uint64
	// interleaved holds, for every 128 bits of a data code, the five planes
	// over those bits, plane after plane: what the fused kernel reads.
	interleaved []uint64
	// lut holds 16 bytes for every group of 4 dimensions: what the group
	// adds to S for each value its 4 code bits can take.
	lut []uint8

	dims                              int
	step, squaredNorm, corrQ, l2, cos float32
}

// NewQuery lays out a query of the given values, one per dimension, each at
// most 31. The dimensions must fill whole 128-bit chunks. The constants are
// those of CenteredBinaryRQDistancer: the step and squared norm of the
// query code, ⟨μ,q⟩ + ‖μ‖², and whether the distance is cosine (else l2).
func NewQuery(values []uint8, step, squaredNorm, corrQ float32, cosine bool) (*Query, error) {
	dims := len(values)
	if dims == 0 || dims%(64*chunkWords) != 0 {
		return nil, errors.Errorf("%d dimensions do not fill whole chunks of %d", dims, 64*chunkWords)
	}
	words := dims / 64
	q := &Query{Words: words, dims: dims, step: step, squaredNorm: squaredNorm, corrQ: corrQ}
	if cosine {
		q.cos = 1
	} else {
		q.l2 = 1
	}
	for p := range q.Planes {
		q.Planes[p] = make([]uint64, words)
	}
	for i, v := range values {
		if v > maxValue {
			return nil, errors.Errorf("value %d of dimension %d does not fit %d bits", v, i, Planes)
		}
		for p := range q.Planes {
			if v>>p&1 != 0 {
				q.Planes[p][i/64] |= 1 << (i % 64)
			}
		}
	}

	q.interleaved = make([]uint64, 0, Planes*words)
	for chunk := 0; chunk < words; chunk += chunkWords {
		for p := range q.Planes {
			q.interleaved = append(q.interleaved, q.Planes[p][chunk:chunk+chunkWords]...)
		}
	}

	q.lut = make([]uint8, dims/groupBits*16)
	for g := 0; g < dims/groupBits; g++ {
		for nibble := 0; nibble < 16; nibble++ {
			sum := 0
			for j := 0; j < groupBits; j++ {
				v := int(values[g*groupBits+j])
				if nibble>>j&1 != 0 {
					v = maxValue - v
				}
				sum += v
			}
			q.lut[g*16+nibble] = uint8(sum)
		}
	}
	return q, nil
}

// WeightedHamming is S computed the way CenteredBinaryRQDistancer does it,
// one Hamming distance per plane, in Go: the reference of the tests.
func WeightedHamming(q *Query, x []uint64) int {
	s := 0
	for p := range q.Planes {
		count := 0
		for w, plane := range q.Planes[p] {
			count += bits.OnesCount64(plane ^ x[w])
		}
		s += count << p
	}
	return s
}

// FusedGo is S in one pass over the code, in Go.
func FusedGo(q *Query, x []uint64) int {
	var c0, c1, c2, c3, c4 int
	p0, p1, p2, p3, p4 := q.Planes[0], q.Planes[1], q.Planes[2], q.Planes[3], q.Planes[4]
	x = x[:len(p0)]
	for w, word := range x {
		c0 += bits.OnesCount64(p0[w] ^ word)
		c1 += bits.OnesCount64(p1[w] ^ word)
		c2 += bits.OnesCount64(p2[w] ^ word)
		c3 += bits.OnesCount64(p3[w] ^ word)
		c4 += bits.OnesCount64(p4[w] ^ word)
	}
	return c0 + c1<<1 + c2<<2 + c3<<3 + c4<<4
}

// Fused is S in one pass over the code: NEON on arm64, FusedGo elsewhere.
// x is the code without its header word.
func Fused(q *Query, x []uint64) int {
	return fused(q, x[:q.Words])
}

// Distance is the estimate of CenteredBinaryRQDistancer.Distance for a code
// with its header word, with S from Fused: the same work, in one pass.
func (q *Query) Distance(code []uint64) float32 {
	return q.estimate(code[0], Fused(q, code[1:]))
}

// estimate turns S into the distance estimate, with the header word of the
// code: its step as a float32, then its squared norm and ⟨μ,x-μ⟩ as
// bfloat16, the layout of compressionhelpers.
func (q *Query) estimate(header uint64, s int) float32 {
	step := math.Float32frombits(uint32(header))
	norm2 := math.Float32frombits(uint32(header>>32) << 16)
	dmu := math.Float32frombits(uint32(header>>48) << 16)
	dot := float32(maxValue*q.dims - 2*s)
	return q.l2*(norm2+q.squaredNorm) + q.cos - (1+q.l2)*q.step*step*dot - (dmu + q.corrQ)
}

// Block is up to 32 data codes laid out for FastScan. For every group of 4
// dimensions it holds 16 bytes: byte j has the 4 code bits of the group of
// code j in its low half and those of code j+16 in its high half. The header
// words stay apart, one per code.
type Block struct {
	n       int
	groups  int
	nibbles []uint8
	headers [BlockCodes]uint64
}

// NewBlock lays out up to 32 codes of the same width, each with its header
// word. A block of fewer codes scores the missing ones as zero codes.
func NewBlock(codes [][]uint64) (*Block, error) {
	if len(codes) == 0 || len(codes) > BlockCodes {
		return nil, errors.Errorf("%d codes for a block of %d", len(codes), BlockCodes)
	}
	words := len(codes[0]) - 1
	if words <= 0 || 64*words > maxScanDims {
		return nil, errors.Errorf("codes of %d words do not fit FastScan", words)
	}
	b := &Block{n: len(codes), groups: 64 * words / groupBits}
	b.nibbles = make([]uint8, b.groups*16)
	for c, code := range codes {
		if len(code) != words+1 {
			return nil, errors.Errorf("code %d has %d words, the first %d", c, len(code), words+1)
		}
		b.headers[c] = code[0]
		for g := 0; g < b.groups; g++ {
			nibble := uint8(code[1+g/16] >> (groupBits * (g % 16)) & 0xF)
			if c < 16 {
				b.nibbles[g*16+c] |= nibble
			} else {
				b.nibbles[g*16+c-16] |= nibble << 4
			}
		}
	}
	return b, nil
}

// Len is the number of codes of the block.
func (b *Block) Len() int {
	return b.n
}

// ScanBlock writes S of every code of the block into s: NEON on arm64, Go
// elsewhere. The block and the query must have the same width.
func ScanBlock(q *Query, b *Block, s *[BlockCodes]uint16) {
	scanBlock(q.lut[:b.groups*16], b.nibbles, s)
}

// ScanBlockGo is ScanBlock in Go.
func ScanBlockGo(q *Query, b *Block, s *[BlockCodes]uint16) {
	scanBlockGo(q.lut[:b.groups*16], b.nibbles, s)
}

func scanBlockGo(lut, nibbles []uint8, s *[BlockCodes]uint16) {
	*s = [BlockCodes]uint16{}
	for g := 0; g < len(nibbles)/16; g++ {
		table := lut[g*16 : g*16+16]
		for j, packed := range nibbles[g*16 : g*16+16] {
			s[j] += uint16(table[packed&0xF])
			s[j+16] += uint16(table[packed>>4])
		}
	}
}

// BlockDistances writes the estimate of every code of the block into out.
func (q *Query) BlockDistances(b *Block, out []float32) {
	var s [BlockCodes]uint16
	ScanBlock(q, b, &s)
	for c := 0; c < b.n; c++ {
		out[c] = q.estimate(b.headers[c], int(s[c]))
	}
}
