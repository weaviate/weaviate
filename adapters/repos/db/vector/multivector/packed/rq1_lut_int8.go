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

package packed

// The full-precision-query RQ1 estimator: the same sign codes as rq1.go, the
// query kept at full precision, scored through int8 lookup tables over 4-bit
// groups. This file is the scalar definition the fast scan in rq1_lut_fs.go
// vectorizes, and it holds the helpers shared with it.
//
// # The estimator
//
//	est(q, t) = Step_t * <sign(x~_t), q~> = Step_t * sum_i s_i * q~_i
//
// where s_i = +1/-1 is document sign bit i and q~ is the rotated query. The
// rotated dimensions are split into groups, and a table
// T[g][c] = sum over the group of +-q~_j for each sign pattern c of group g
// turns the dot into a sum of one lookup per group. The tables are built once
// per query; the scan only reads them. Step cannot fold into the tables: an
// entry belongs to a query token and a sign pattern, while Step belongs to the
// document token, so it multiplies the summed lookups once per pair.
//
// The query is kept at full precision because in a lookup kernel its precision
// is free: it enters only the table build, once per query, and the per-pair
// work is the same lookups whatever the entries hold. Quantizing the query
// would add error and save nothing. (The house 5-bit query exists for the
// popcount kernel, where the query has to be bits.)
//
// # Why 4-bit groups
//
// TBL and PSHUFB look up sixteen bytes held in a vector register, so a group
// has to be four dimensions wide (2^4 = 16 sign patterns) and its entries have
// to be bytes. This file scores that representation in plain Go and is the
// reference the kernel is held to.
//
// # Int8 tables
//
// Entries are rounded to [-127, 127] with one scale per query token:
//
//	scale_q     = max over groups n of sum_{j in n} |q~_j| / 127
//	T4int[n][c] = round(T4[n][c] / scale_q)
//	est(q, t)   = (S * scale_q) * Step_t,   S = sum_n T4int[n][group n of the code]
//
// The rounding of the entries is the only new error; S is exact integer
// arithmetic. A per-group scale would be finer but would break the integer
// accumulation, so the scale is per query token. S fits int16 up to 258 groups
// (256 * 127 = 32512 at d=1024); this reference accumulates in int32 and any
// kernel must reproduce its integer sum exactly.
//
// # Verification
//
// This is a different estimator from RQ1Scorer's, so there is no bit-exact
// parity between the two, and there is no house implementation of it. The
// tests pin the sign convention and byte indexing against the house Distance
// (feeding the house's own quantized query through the tables must reproduce
// it), pin the tables to the exact sums they round (in float64), and pin the
// quantization by the bound it guarantees: |S * scale - dot| is at most
// groups * scale / 2 per pair, which no misindexed group or flipped sign
// satisfies. Accuracy against the 5-bit estimator is a corpus measurement.

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/tphakala/simd/f16"
)

// rq1Int8Levels is the largest magnitude a table entry is quantized to. The
// range is symmetric on purpose: a group's entries satisfy T[15-c] = -T[c]
// exactly (complementing every sign bit negates the sum), and a symmetric range
// with round-half-away keeps the quantized table antisymmetric too, so the
// rounding adds no bias.
const rq1Int8Levels = 127

// rq1Groups returns how many 8-dimension groups one code has under p. The
// rotation output is a multiple of 64 (compression.NewFastRotation), so there
// is never a partial group.
func rq1Groups(p *RQ1Params) int {
	return 8 * (p.codeWords - 1)
}

// rq1Nibbles returns how many 4-dimension groups one code has under p: two per
// code byte.
func rq1Nibbles(p *RQ1Params) int {
	return 2 * rq1Groups(p)
}

// rq1NibbleEntry returns one lookup-table entry: the signed sum of four rotated
// query coordinates under sign pattern c.
//
// Arguments:
//   - qs: the group's four rotated query coordinates (only qs[:4] is read).
//   - c: the 4-bit sign pattern. A set bit j adds qs[j], a cleared bit
//     subtracts it, matching Encode's convention that a set sign bit means the
//     rotated coordinate was strictly positive.
//
// It runs at scorer construction only, through rq1Int8Table; the per-pair
// loops read the finished tables. The sum runs in index order, j = 0..3. Every
// 4-bit table in this package is built by calling this, so different layouts
// hold identical float32 values before quantization.
func rq1NibbleEntry(qs []float32, c byte) float32 {
	var dot float32
	for j, v := range qs[:4] {
		if c&(1<<j) != 0 {
			dot += v
		} else {
			dot -= v
		}
	}
	return dot
}

// rq1Int8Table fills the int8 table of one rotated query token and returns its
// scale, the value of one table unit. It runs at scorer construction only, once
// per query token, from NewRQ1LUTInt8Scorer and NewRQ1LUTFastScanScorer.
//
// Arguments:
//   - rx: the rotated query token, at least 4*nibbles coordinates.
//   - nibbles: number of 4-dimension groups.
//   - tbl: the table to fill, nibbles*16 entries, indexed [n*16 + c].
//
// A zero query token rotates to the zero vector: every entry is zero, the scale
// is zero, and the estimate is exactly zero, which is what the 5-bit estimator
// does with it too (encodeQuery gives it a zero Step).
func rq1Int8Table(rx []float32, nibbles int, tbl []int8) float32 {
	// the largest entry of the whole table sets the scale; a group's largest
	// entry is the pattern where every sign agrees with the coordinate
	var maxAbs float32
	for n := range nibbles {
		var extreme float32
		for _, v := range rx[4*n : 4*n+4] {
			if v < 0 {
				extreme -= v
			} else {
				extreme += v
			}
		}
		if extreme > maxAbs {
			maxAbs = extreme
		}
	}
	if maxAbs == 0 {
		clear(tbl[:nibbles*16])
		return 0
	}

	scale := maxAbs / rq1Int8Levels
	for n := range nibbles {
		qs := rx[4*n : 4*n+4]
		base := n * 16
		for c := range 16 {
			// rounded in float64 so the division is not doubly rounded, and
			// clamped because the extreme entry divides to exactly the
			// boundary and float rounding may put it a hair past
			v := math.Round(float64(rq1NibbleEntry(qs, byte(c))) / float64(scale))
			if v > rq1Int8Levels {
				v = rq1Int8Levels
			} else if v < -rq1Int8Levels {
				v = -rq1Int8Levels
			}
			tbl[base+c] = int8(v)
		}
	}
	return scale
}

// widenRQ1Steps converts the binary16 scalar section of b to float32 and
// returns the result. It runs once per Distance call, one conversion per
// document token; the per-pair loops read the widened slice.
//
// Arguments:
//   - b: the blob whose Steps to widen.
//   - buf: scratch to write into, reallocated when too small.
//
// f16.ToFloat32Slice is not used: it wants a []uint16, and a blob handed over
// from an mmap'd segment has no alignment guarantee at an interior offset.
func widenRQ1Steps(b Blob, buf []float32) []float32 {
	tokens := int(b.Tokens)
	if cap(buf) < tokens {
		buf = make([]float32, tokens)
	}
	buf = buf[:tokens]
	scalars := b.Scalars()
	for i := range buf {
		buf[i] = f16.ToFloat32(binary.LittleEndian.Uint16(scalars[2*i:]))
	}
	return buf
}

// checkRQ1LUTBlob is checkRQ1Blob plus a stride check: the table scorers walk
// the code section groups bytes at a time, and groups comes from the scorer's
// parameters while the blob's stride comes from its header. The dims check
// already implies agreement today; one comparison per document keeps that from
// ever becoming an unstated assumption.
//
// Arguments:
//   - b, p, layout: as in checkRQ1Blob.
//   - groups: the scorer's code bytes per token.
func checkRQ1LUTBlob(b Blob, p *RQ1Params, groups int, layout Layout) error {
	if err := checkRQ1Blob(b, p, layout); err != nil {
		return err
	}
	if b.perToken != groups {
		return fmt.Errorf("packed: blob has %d code bytes per token, scorer built %d group tables", b.perToken, groups)
	}
	return nil
}

var _ Scorer = (*RQ1LUTInt8Scorer)(nil)

// RQ1LUTInt8Scorer scores token-major RQ1 blobs with int8 tables over 4-bit
// groups: the document walked once per query token, the two nibbles of each
// code byte looked up and summed as integers, the sum multiplied by the scale
// and then by the token's Step.
//
// It is the parity reference the fast scan in rq1_lut_fs.go is held to, bit for
// bit, and the scorer the accuracy gate is measured with.
//
// It is not safe for concurrent use, since Distance reuses the steps buffer.
type RQ1LUTInt8Scorer struct {
	p       *RQ1Params
	nq      int
	groups  int
	nibbles int

	// tables is one table per query token, indexed [q*tableLen + n*16 + c].
	tables   []int8
	tableLen int

	// scale is the value of one table unit, per query token.
	scale []float32

	// corr is <q, mu> per query token, as in every RQ1 scorer here.
	corr []float32

	// steps holds the document's Step per token, widened once per document.
	steps []float32
}

// NewRQ1LUTInt8Scorer prepares query for scoring token-major RQ1 blobs encoded
// under p. Each query token is rotated once (the same rotation the codes were
// encoded under) and expanded into its quantized 4-bit tables. The query itself
// is not retained.
//
// Arguments:
//   - query: one slice per query token, each exactly p.dims long.
//   - p: the parameters the blobs were encoded under.
func NewRQ1LUTInt8Scorer(query [][]float32, p *RQ1Params) (*RQ1LUTInt8Scorer, error) {
	if err := validateTokens(query, p.dims); err != nil {
		return nil, fmt.Errorf("packed: query: %w", err)
	}

	nibbles := rq1Nibbles(p)
	s := &RQ1LUTInt8Scorer{
		p:        p,
		nq:       len(query),
		groups:   rq1Groups(p),
		nibbles:  nibbles,
		tableLen: nibbles * 16,
		scale:    make([]float32, len(query)),
		corr:     make([]float32, len(query)),
	}
	s.tables = make([]int8, len(query)*s.tableLen)

	for q, token := range query {
		rx := p.rot.Rotate(token)
		s.scale[q] = rq1Int8Table(rx, nibbles, s.tables[q*s.tableLen:(q+1)*s.tableLen])
		for j, m := range p.mean {
			s.corr[q] += token[j] * m
		}
	}
	return s, nil
}

// Distance implements Scorer: the negated MaxSim estimate over int8 tables,
// with the <q, mu> correction applied per query token after its maximum.
func (s *RQ1LUTInt8Scorer) Distance(b Blob) (float32, error) {
	if err := checkRQ1LUTBlob(b, s.p, s.groups, LayoutTokenMajor); err != nil {
		return 0, err
	}
	s.steps = widenRQ1Steps(b, s.steps)

	tokens := int(b.Tokens)
	codes := b.Codes()
	groups := s.groups

	var sum float32
	for q := 0; q < s.nq; q++ {
		tbl := s.tables[q*s.tableLen : (q+1)*s.tableLen]
		scale := s.scale[q]
		// Seeded like every scorer here: an empty document saturates to the
		// largest finite distance per query token and sorts last.
		best := float32(-math.MaxFloat32)
		for t := 0; t < tokens; t++ {
			code := codes[t*groups : (t+1)*groups]
			// integer arithmetic, so the order of accumulation is free and a
			// kernel summing sixteen tokens at a time in vector lanes can be
			// held to this exactly. Code byte g holds nibble groups 2g (low)
			// and 2g+1 (high), whose tables start at 32*g and 32*g+16.
			var acc int32
			for g, c := range code {
				acc += int32(tbl[32*g+int(c&15)]) + int32(tbl[32*g+16+int(c>>4)])
			}
			// scale first, then Step: float multiplication is not
			// associative, and the kernels take the same order
			if v := (float32(acc) * scale) * s.steps[t]; v > best {
				best = v
			}
		}
		sum += -best - s.corr[q]
	}
	return sum, nil
}
