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

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// The full-precision-query estimator has no house implementation to be pinned
// against (it is the 5-bit estimator with the query quantization removed), so
// its verification is a chain:
//
//   - TestRQ1LUTHouseIdentity pins the conventions to the house Distance at
//     bit equality: extracting sign bits per byte, in the order the tables are
//     indexed, and the query levels by the house quantization rule reproduces
//     the house result exactly. A flipped sign convention or a misread code
//     byte fails here, against shipped code.
//   - TestRQ1NibbleEntry pins the table builder's float32 entries to the
//     exact signed sums, for every group and sign pattern.
//   - TestRQ1Int8Table pins the quantization: every entry within half a unit
//     of the exact value, the extreme entry at exactly +-127, the antisymmetry
//     T[15-c] = -T[c] surviving the rounding, and a zero query token giving a
//     zero scale and a zero table.
//   - TestRQ1LUTInt8PairBound pins the sign convention and the group
//     indexing against the mathematical estimator, per pair, through the
//     bound the quantization guarantees (groups * scale / 2).
//   - TestRQ1LUTInt8Distance pins Distance's own loop (maxima, correction,
//     negation) against a float64 sum over an independent group extraction.
//   - TestRQ1LUTInt8AgreesWithEstimator checks that the int8 estimate differs
//     from the exact estimator, computed in float64, by no more than the
//     rounding bound summed over query tokens.
//
// rq1_lut_fs_test.go then pins the fast scan to this scorer, bit for bit.
//
// Mutation testing: swapped nibbles, a wrong high-nibble table base, a
// reversed bit order, a flipped sign and a wrong scale rule are all caught.
// Applying Step before the scale is not caught here: the two orders differ in
// the last bit only, which every bound here covers. The fast scan's parity
// with this scorer at bit equality (rq1_lut_fs_test.go) is what pins it.

// signBit returns document sign s_i = +1 or -1 for rotated coordinate i of
// code, extracted per byte on purpose (the scorers read whole words), so the
// tests and the scorers reach the same bit through different arithmetic.
func signBit(code []byte, i int) float64 {
	if code[i/8]&(1<<(i%8)) != 0 {
		return 1
	}
	return -1
}

// nibbleAt returns 4-bit group n of code, extracted per byte and shift on
// purpose (the scorer indexes differently), so the tests and the scorer reach
// the same nibble by different arithmetic.
func nibbleAt(code []byte, n int) byte {
	b := code[n/2]
	if n%2 == 0 {
		return b & 15
	}
	return b >> 4
}

// TestRQ1LUTHouseIdentity pins the sign convention against shipped code, at
// bit equality. The house Distance computes the integer dot
// sum_i s_i * (2*c_i - 31) with c_i the query's 5-bit level; rebuilding its
// result from the document signs read the way the tables are indexed must
// reproduce it exactly, so the table convention (set bit adds, cleared bit
// subtracts) is the house's.
//
// The levels are rebuilt because the house code keeps its bit planes
// unexported: c_i = floor((q~_i + max|q~|) / (2*step) + r_i) with
// step = max|q~| / 31 and r_i the rounding offsets the params derive. The step
// is checked against the house's own.
//
// The house Distance switches from integer popcounts to a float32 SIMD dot at
// 512 dimensions, so the list crosses that threshold. The identity holds on
// both paths: every value on the float path is an integer that float32 holds
// exactly.
func TestRQ1LUTHouseIdentity(t *testing.T) {
	for _, dims := range []int{64, 100, 128, 256, 512, 1024} {
		t.Run(fmt.Sprintf("d=%d", dims), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(dims) + 77))
			p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
			if err != nil {
				t.Fatalf("NewRQ1Params: %v", err)
			}
			query := unitTokens(rng, 16, dims)
			doc := unitTokens(rng, 24, dims)

			outputDim := 8 * rq1Groups(p)
			for qi, q := range query {
				dist := p.brq.NewDistancer(q)
				qc := dist.QueryCode()

				// the query's 5-bit levels by the house rule, in the house's
				// float32 arithmetic. A zero query token has a zero-dimensional
				// code and a zero step; the loop bound makes both sides zero.
				rx := p.rot.Rotate(q)
				var abs float32
				for _, v := range rx {
					if v < 0 {
						v = -v
					}
					if v > abs {
						abs = v
					}
				}
				step := abs / 31
				if math.Float32bits(step) != math.Float32bits(qc.Step) {
					t.Fatalf("query %d: rebuilt step %v, house %v", qi, step, qc.Step)
				}
				levels := make([]int, qc.Dimension)
				for i := range levels {
					levels[i] = int(uint64(((rx[i] + abs) / (2 * step)) + p.rounding[i]))
				}

				for ti, tok := range doc {
					code := p.brq.Encode(tok)
					want, err := dist.Distance(code)
					if err != nil {
						t.Fatalf("house Distance: %v", err)
					}

					codeBytes := make([]byte, outputDim/8)
					for k, w := range compressionhelpers.RQOneBitCode(code).Bits() {
						for b := range 8 {
							codeBytes[8*k+b] = byte(w >> (8 * b))
						}
					}
					dot := 0
					for i := 0; i < qc.Dimension; i++ {
						if signBit(codeBytes, i) > 0 {
							dot += 2*levels[i] - 31
						} else {
							dot -= 2*levels[i] - 31
						}
					}

					est := qc.Step * compressionhelpers.RQOneBitCode(code).Step() * float32(dot)
					rebuilt := float32(0) - est
					if math.Float32bits(rebuilt) != math.Float32bits(want) {
						t.Fatalf("query %d doc %d: rebuilt %v (%#08x), house %v (%#08x)",
							qi, ti, rebuilt, math.Float32bits(rebuilt), want, math.Float32bits(want))
					}
				}
			}
		})
	}
}

// TestRQ1NibbleEntry pins the table builder's entries to the exact signed sums
// computed in float64 from the sign pattern's bits, for every group of a
// rotated query token and every pattern. The builder sums in float32, so the
// tolerance is its rounding.
func TestRQ1NibbleEntry(t *testing.T) {
	for _, dims := range []int{64, 128, 256, 1024} {
		t.Run(fmt.Sprintf("d=%d", dims), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(dims) + 131))
			p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
			if err != nil {
				t.Fatalf("NewRQ1Params: %v", err)
			}
			rx := p.rot.Rotate(unitTokens(rng, 1, dims)[0])

			for n := range rq1Nibbles(p) {
				qs := rx[4*n : 4*n+4]
				// the tolerance is relative to sum|q~_j| and cannot be
				// relative to the entry: the terms cancel, so an entry can be
				// orders smaller than the partial sums whose rounding
				// produced it
				var absSum float64
				for _, v := range qs {
					absSum += math.Abs(float64(v))
				}
				for c := range 16 {
					var want float64
					for j, v := range qs {
						if c&(1<<j) != 0 {
							want += float64(v)
						} else {
							want -= float64(v)
						}
					}
					got := float64(rq1NibbleEntry(qs, byte(c)))
					if tol := 4 * eps32 * absSum; math.Abs(got-want) > tol {
						t.Fatalf("group %d pattern %d: rq1NibbleEntry %v, exact %v (tol %g)",
							n, c, got, want, tol)
					}
				}
			}
		})
	}
}

// TestRQ1Int8Table pins the quantization: half-unit accuracy, the extreme entry
// at the end of the range, antisymmetry preserved, and the zero token's zero
// table.
func TestRQ1Int8Table(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(41))
	p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	nibbles := rq1Nibbles(p)
	tbl := make([]int8, nibbles*16)

	t.Run("quantization", func(t *testing.T) {
		rx := p.rot.Rotate(unitTokens(rng, 1, dims)[0])
		scale := rq1Int8Table(rx, nibbles, tbl)
		if scale <= 0 {
			t.Fatalf("scale = %v, want positive for a non-zero token", scale)
		}

		var sawExtreme bool
		for n := range nibbles {
			qs := rx[4*n : 4*n+4]
			for c := range 16 {
				e := tbl[n*16+c]
				if e == rq1Int8Levels || e == -rq1Int8Levels {
					sawExtreme = true
				}
				want := float64(rq1NibbleEntry(qs, byte(c)))
				got := float64(e) * float64(scale)
				if math.Abs(got-want) > float64(scale)/2*(1+8*eps32) {
					t.Fatalf("group %d pattern %d: %v, want %v (half unit is %v)",
						n, c, got, want, float64(scale)/2)
				}
				// T[15-c] = -T[c]: complementing every sign bit negates the sum,
				// and a symmetric range keeps that true after rounding.
				if opp := tbl[n*16+15-c]; opp != -e {
					t.Fatalf("group %d pattern %d: entry %d, complement %d, want negation",
						n, c, e, opp)
				}
			}
		}
		if !sawExtreme {
			t.Errorf("no entry reached +-%d: the scale is not set by the largest entry", rq1Int8Levels)
		}
	})

	t.Run("zero token", func(t *testing.T) {
		for i := range tbl {
			tbl[i] = 7 // pre-dirtied, so a table left unwritten fails
		}
		if scale := rq1Int8Table(make([]float32, p.rot.OutputDim), nibbles, tbl); scale != 0 {
			t.Fatalf("scale = %v, want 0 for the zero token", scale)
		}
		for i, e := range tbl {
			if e != 0 {
				t.Fatalf("entry %d = %d, want 0", i, e)
			}
		}
	})
}

// rq1Int8Fixture is one (query, document, parameters) triple with the
// document's token-major RQ1 blob, shared by the tests below.
type rq1Int8Fixture struct {
	p     *RQ1Params
	query [][]float32
	doc   [][]float32
	blob  Blob
}

// newRQ1Int8Fixture builds a fixture of nq query tokens and nd document tokens
// of dimensionality dims, drawn from seed. When centered is set the parameters
// carry a mean trained on separate random tokens.
func newRQ1Int8Fixture(t *testing.T, dims, nq, nd int, centered bool, seed int64) rq1Int8Fixture {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	query := unitTokens(rng, nq, dims)
	doc := unitTokens(rng, nd, dims)

	var mean []float32
	var id uint16
	var version uint16
	if centered {
		mean = tokenMean(unitTokens(rng, 64, dims), dims)
		id, version = 3, 2
	}
	p, err := NewRQ1Params(dims, 0x5eed, mean, id, version)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	raw, err := EncodeRQ1(doc, p)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	b, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return rq1Int8Fixture{p: p, query: query, doc: doc, blob: b}
}

// TestRQ1LUTInt8PairBound pins the tables and their indexing per pair: the
// int8 dot must land within groups * scale / 2 of the exact projection
// <sign(x~), q~>, computed in float64 from an independent per-byte sign
// extraction. That is the quantization's own guarantee, and any wrong group,
// table or sign violates it.
func TestRQ1LUTInt8PairBound(t *testing.T) {
	for _, dims := range []int{64, 128, 256} {
		t.Run(fmt.Sprintf("d=%d", dims), func(t *testing.T) {
			f := newRQ1Int8Fixture(t, dims, 8, 24, false, int64(dims)+17)
			s, err := NewRQ1LUTInt8Scorer(f.query, f.p)
			if err != nil {
				t.Fatalf("NewRQ1LUTInt8Scorer: %v", err)
			}
			outputDim := 8 * rq1Groups(f.p)
			bound := float64(s.nibbles) / 2

			var spent float64
			for q, token := range f.query {
				rx := f.p.rot.Rotate(token)
				tbl := s.tables[q*s.tableLen : (q+1)*s.tableLen]
				scale := float64(s.scale[q])

				for ti := range int(f.blob.Tokens) {
					code := f.blob.Code(ti)
					var want float64
					for i := range outputDim {
						want += signBit(code, i) * float64(rx[i])
					}
					var acc float64
					for n := range s.nibbles {
						acc += float64(tbl[n*16+int(nibbleAt(code, n))])
					}
					// a zero query token (every eighth token unitTokens draws)
					// has a zero scale and no units to be off by, so it is
					// asserted directly; the bound says nothing about it
					if scale == 0 {
						if acc != 0 || want != 0 {
							t.Fatalf("query %d token %d: zero query token gave int8 %v, exact %v",
								q, ti, acc, want)
						}
						continue
					}
					diff := math.Abs(acc - want/scale) // in table units
					if diff > bound*(1+8*eps32) {
						t.Fatalf("query %d token %d: int8 dot %v units, exact %v units, off by %v > %v",
							q, ti, acc, want/scale, diff, bound)
					}
					if diff > spent {
						spent = diff
					}
				}
			}
			// rounding errors are signed and largely cancel, so the worst
			// pair should spend a small fraction of the bound
			t.Logf("worst pair spent %.2f of %.1f table units (%d nibbles)", spent, bound, s.nibbles)
		})
	}
}

// TestRQ1LUTInt8Distance pins Distance itself (the per-query-token maximum,
// the Step rescale, the correction and the negation) against a float64 sum
// built from an independent group extraction over the same tables.
func TestRQ1LUTInt8Distance(t *testing.T) {
	for _, dims := range []int{64, 128, 256} {
		for _, centered := range []bool{false, true} {
			name := "uncentered"
			if centered {
				name = "centered"
			}
			t.Run(fmt.Sprintf("d=%d/%s", dims, name), func(t *testing.T) {
				f := newRQ1Int8Fixture(t, dims, 32, 124, centered, int64(dims)+29)
				s, err := NewRQ1LUTInt8Scorer(f.query, f.p)
				if err != nil {
					t.Fatalf("NewRQ1LUTInt8Scorer: %v", err)
				}
				got, err := s.Distance(f.blob)
				if err != nil {
					t.Fatalf("Distance: %v", err)
				}

				var want, maxTerm float64
				for q, token := range f.query {
					tbl := s.tables[q*s.tableLen : (q+1)*s.tableLen]
					scale := float64(s.scale[q])

					// corr computed exactly as the scorer computes it, so the
					// bound below need not cover its rounding
					var corr float32
					for j, m := range f.p.mean {
						corr += token[j] * m
					}

					best := -math.MaxFloat64
					for ti := range int(f.blob.Tokens) {
						code := f.blob.Code(ti)
						var acc float64
						for n := range s.nibbles {
							acc += float64(tbl[n*16+int(nibbleAt(code, n))])
						}
						est := acc * scale * float64(f.blob.Scalar(ti))
						if math.Abs(est) > maxTerm {
							maxTerm = math.Abs(est)
						}
						if est > best {
							best = est
						}
					}
					want += -best - float64(corr)
				}

				// each per-token estimate is one float32 product chain over an
				// exact integer sum, and the outer sum has one term per query
				// token; the factor 8 absorbs the maximum switching tokens
				// between precisions
				tol := 8 * float64(len(f.query)) * eps32 * maxTerm
				if diff := math.Abs(float64(got) - want); diff > tol {
					t.Fatalf("Distance = %v, float64 reference = %v, |diff| = %g > tol %g",
						got, want, diff, tol)
				}
			})
		}
	}
}

// exactRQ1MaxSim returns the negated MaxSim of query against b under the exact
// estimator, Step_t * sum_i s_i * q~_i per pair in float64 with the signs
// extracted per byte from the blob, the maxima and the corrected sum taken in
// float64. The correction is computed in float32 as the scorers compute it, so
// no bound has to cover its rounding.
//
// It also returns how far a float32 evaluation of the same estimator may sit
// from this value: each per-pair dot is a float32 sum of outputDim signed terms
// bounded by sum|q~_i|, times Step, and MaxSim adds one per query token; the
// factor 8 absorbs the maximum switching tokens between precisions and the
// final sum over query tokens.
func exactRQ1MaxSim(p *RQ1Params, query [][]float32, b Blob) (want, floatTol float64) {
	outputDim := 8 * rq1Groups(p)
	var maxAbsSum, maxStep float64
	for _, q := range query {
		rx := p.rot.Rotate(q)
		var absSum float64
		for _, v := range rx {
			absSum += math.Abs(float64(v))
		}
		if absSum > maxAbsSum {
			maxAbsSum = absSum
		}

		var corr float32
		for j, m := range p.mean {
			corr += q[j] * m
		}

		best := -math.MaxFloat64
		for ti := range int(b.Tokens) {
			code := b.Code(ti)
			var dot float64
			for i := range outputDim {
				dot += signBit(code, i) * float64(rx[i])
			}
			step := float64(b.Scalar(ti))
			if step > maxStep {
				maxStep = step
			}
			if est := step * dot; est > best {
				best = est
			}
		}
		want += -best - float64(corr)
	}
	floatTol = 8 * float64(len(query)) * float64(outputDim) * float64(eps32) * maxStep * maxAbsSum
	return want, floatTol
}

// rq1Int8RoundingBound returns how far the int8 scorer's answer may sit from
// the exact estimator on b: per query token the maxima can differ by at most
// the worst per-pair error, max_t Step_t * groups * scale_q / 2.
func rq1Int8RoundingBound(s *RQ1LUTInt8Scorer, b Blob) float64 {
	var maxStep float64
	for ti := range int(b.Tokens) {
		if v := float64(b.Scalar(ti)); v > maxStep {
			maxStep = v
		}
	}
	var bound float64
	for q := range s.nq {
		bound += maxStep * float64(s.nibbles) * float64(s.scale[q]) / 2
	}
	return bound
}

// TestRQ1LUTInt8AgreesWithEstimator holds the int8 scorer to the exact
// estimator in float64, within the bound the rounding guarantees plus the
// float32 rounding of the scorer's own arithmetic. It also logs how much of the
// bound is spent.
func TestRQ1LUTInt8AgreesWithEstimator(t *testing.T) {
	for _, dims := range []int{64, 128, 256, 1024} {
		for _, nd := range []int{0, 1, 32, 124, 1030} {
			t.Run(fmt.Sprintf("d=%d/n=%d", dims, nd), func(t *testing.T) {
				f := newRQ1Int8Fixture(t, dims, 32, nd, true, int64(dims*7+nd)+53)
				s, err := NewRQ1LUTInt8Scorer(f.query, f.p)
				if err != nil {
					t.Fatalf("NewRQ1LUTInt8Scorer: %v", err)
				}
				got, err := s.Distance(f.blob)
				if err != nil {
					t.Fatalf("RQ1LUTInt8Scorer.Distance: %v", err)
				}

				// an empty document saturates to +Inf, where a difference
				// is NaN and the bound says nothing
				if nd == 0 {
					if !math.IsInf(float64(got), 1) {
						t.Fatalf("empty document: int8 %v, want +Inf", got)
					}
					return
				}

				want, floatTol := exactRQ1MaxSim(f.p, f.query, f.blob)
				bound := rq1Int8RoundingBound(s, f.blob)
				diff := math.Abs(float64(got) - want)
				if diff > bound+floatTol {
					t.Fatalf("int8 %v, exact %v, |diff| = %g > rounding bound %g",
						got, want, diff, bound+floatTol)
				}
				t.Logf("|int8 - exact| = %.6g, bound %.6g, MaxSim %.6g", diff, bound, math.Abs(want))
			})
		}
	}
}

// edgeCaseParams builds the params the edge-case tests of both int8 table
// scorers encode under: uncentered, or centered on the mean of 64 random
// unit tokens.
//
// Arguments:
//   - rng: the source of the mean's tokens.
//   - dims: the dimensionality.
//   - centered: whether to center.
//
// It returns the name of the variant for the subtest and the params.
func edgeCaseParams(t *testing.T, rng *rand.Rand, dims int, centered bool) (string, *RQ1Params) {
	t.Helper()
	name := "uncentered"
	var mean []float32
	var id uint16
	var version uint16
	if centered {
		name = "centered"
		mean = tokenMean(unitTokens(rng, 64, dims), dims)
		id, version = 9, 1
	}
	p, err := NewRQ1Params(dims, 0x5eed, mean, id, version)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	return name, p
}

// TestRQ1LUTInt8EdgeCases covers the documents where the maximum is degenerate
// or the estimate is exactly zero, and the zero query token, whose table is
// zero and whose contribution must be exactly the correction. The answer is
// held to the exact estimator: bit for bit where no rounding is left, within
// the rounding bound elsewhere.
func TestRQ1LUTInt8EdgeCases(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(67))
	query := unitTokens(rng, 32, dims)
	query = append(query, make([]float32, dims)) // an explicit zero query token

	identical := unitTokens(rng, 1, dims)[0]
	allIdentical := make([][]float32, 16)
	for i := range allIdentical {
		allIdentical[i] = identical
	}
	allZero := make([][]float32, 12)
	for i := range allZero {
		allZero[i] = make([]float32, dims)
	}

	cases := []struct {
		name string
		doc  [][]float32
	}{
		{"empty", [][]float32{}},
		{"nil", nil},
		{"single token", unitTokens(rng, 1, dims)},
		{"all identical", allIdentical},
		{"all zero tokens", allZero},
		{"mixed zero tokens", unitTokens(rng, 24, dims)},
	}

	for _, centered := range []bool{false, true} {
		name, p := edgeCaseParams(t, rng, dims, centered)
		s, err := NewRQ1LUTInt8Scorer(query, p)
		if err != nil {
			t.Fatalf("NewRQ1LUTInt8Scorer: %v", err)
		}
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s/%s", name, tc.name), func(t *testing.T) {
				raw, err := EncodeRQ1(tc.doc, p)
				if err != nil {
					t.Fatalf("EncodeRQ1: %v", err)
				}
				b, err := Parse(raw)
				if err != nil {
					t.Fatalf("Parse: %v", err)
				}
				got, err := s.Distance(b)
				if err != nil {
					t.Fatalf("Distance: %v", err)
				}
				// Bit equality where there is no rounding left to differ over.
				// An empty document saturates to +Inf. A document whose every
				// Step is zero (the uncentered encoding of zero tokens)
				// estimates exactly zero per pair, so the answer is the
				// negated corrections summed in float32, as the scorer sums
				// them. Centering gives those tokens the code of -mu and a
				// real Step, so there they are ordinary tokens and take the
				// rounding bound instead.
				if b.Tokens == 0 {
					if !math.IsInf(float64(got), 1) {
						t.Fatalf("empty document: int8 %v, want +Inf", got)
					}
					return
				}
				allZero := true
				for ti := range int(b.Tokens) {
					if b.Scalar(ti) != 0 {
						allZero = false
					}
				}
				if allZero {
					var want float32
					for q := range query {
						want += -s.corr[q]
					}
					if math.Float32bits(got) != math.Float32bits(want) {
						t.Fatalf("int8 %v (%#08x), want %v (%#08x)",
							got, math.Float32bits(got), want, math.Float32bits(want))
					}
					return
				}
				want, floatTol := exactRQ1MaxSim(p, query, b)
				bound := rq1Int8RoundingBound(s, b)
				if diff := math.Abs(float64(got) - want); diff > bound+floatTol {
					t.Fatalf("int8 %v, exact %v, |diff| = %g > rounding bound %g",
						got, want, diff, bound+floatTol)
				}
			})
		}
	}
}

// TestRQ1LUTInt8EmptyQuery pins that a query with no tokens scores every
// document exactly 0: nothing to sum, as in every scorer here.
func TestRQ1LUTInt8EmptyQuery(t *testing.T) {
	f := newRQ1Int8Fixture(t, 128, 0, 12, true, 73)
	s, err := NewRQ1LUTInt8Scorer(f.query, f.p)
	if err != nil {
		t.Fatalf("NewRQ1LUTInt8Scorer: %v", err)
	}
	got, err := s.Distance(f.blob)
	if err != nil {
		t.Fatalf("Distance: %v", err)
	}
	if math.Float32bits(got) != 0 {
		t.Fatalf("empty query scored %v (%#08x), want exactly 0", got, math.Float32bits(got))
	}
}

// TestRQ1LUTInt8RejectsMismatchedBlobs pins that the int8 scorer refuses what
// the other RQ1 scorers refuse.
func TestRQ1LUTInt8RejectsMismatchedBlobs(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(71))
	query := unitTokens(rng, 4, dims)
	doc := unitTokens(rng, 8, dims)

	p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	other, err := NewRQ1Params(dims, 0x5eed, tokenMean(doc, dims), 4, 2)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	wideParams, err := NewRQ1Params(256, 0x5eed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}

	float32Blob, err := EncodeFloat32(doc, dims)
	if err != nil {
		t.Fatalf("EncodeFloat32: %v", err)
	}
	centeredBlob, err := EncodeRQ1(doc, other)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	wideBlob, err := EncodeRQ1(unitTokens(rng, 8, 256), wideParams)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}

	for _, tc := range []struct {
		name string
		blob []byte
	}{
		{"float32 encoding", float32Blob},
		{"different quantizer reference", centeredBlob},
		{"different dimensions", wideBlob},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := Parse(tc.blob)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			s, err := NewRQ1LUTInt8Scorer(query, p)
			if err != nil {
				t.Fatalf("NewRQ1LUTInt8Scorer: %v", err)
			}
			if _, err := s.Distance(b); err == nil {
				t.Errorf("RQ1LUTInt8Scorer accepted a %s blob", tc.name)
			}
		})
	}
}

// TestNewRQ1LUTInt8ScorerRejectsBadQueries pins the same query validation every
// other scorer applies.
func TestNewRQ1LUTInt8ScorerRejectsBadQueries(t *testing.T) {
	p, err := NewRQ1Params(128, 0x5eed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	for _, tc := range []struct {
		name  string
		query [][]float32
	}{
		{"short token", [][]float32{make([]float32, 127)}},
		{"long token", [][]float32{make([]float32, 129)}},
		{"nil token", [][]float32{nil}},
		{"ragged", [][]float32{make([]float32, 128), make([]float32, 64)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRQ1LUTInt8Scorer(tc.query, p); err == nil {
				t.Errorf("NewRQ1LUTInt8Scorer accepted a %s query", tc.name)
			}
		})
	}
}
