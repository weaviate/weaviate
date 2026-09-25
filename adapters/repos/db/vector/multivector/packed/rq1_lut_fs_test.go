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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// The fast scan changes the stored bytes and has hand-written kernels, so the
// two are verified apart:
//
//   - TestInterleaveBlock16 pins the layout as a permutation: a round trip
//     through an independently written inverse, the tail verbatim, the byte
//     positions asserted from the definition.
//   - TestEncodeRQ1Block16 pins the encoder to EncodeRQ1: same header except
//     the layout byte, same scalars, codes that de-interleave to exactly the
//     token-major ones.
//   - TestRQ1ScanBlocksKernels and TestRQ1ScanTileKernels pin every vector
//     kernel this build carries against the portable one at bit equality,
//     called directly so the shapes include ones no blob produces: no blocks,
//     one group, the int16 accumulators' exact boundary, extreme tables and
//     Steps of both signs.
//   - The parity tests pin the whole scorer to RQ1LUTInt8Scorer at bit
//     equality, which fixes the multiply order (S * scale) * Step that the
//     reference's own tests cannot, and cover the block/tail split, the empty
//     document, and the portable fallback above rq1ScanMaxNibbles.
//   - TestRQ1FastScanRejections pins the layout check in both directions. The
//     two layouts are the same length, so an unchecked mismatch scores
//     silently wrong.
//
// Mutation testing of the Go code and of every kernel caught every mutation
// except these, which are equivalent or unobservable: swapping the VMAXPS
// operand order (it differs only on the sign of zero, which sameLane admits);
// removing VZEROUPPER; stopping the tiled loop a tile early (the leftover query
// tokens fall through to the untiled loop); and removing the int16 fallback,
// since real unit-norm coordinates never approach the bound, which is why
// TestRQ1ScanFallbackGate asserts the rule directly. Applying Step before the
// scale is caught by the bit-equality tests only, as it differs in the last
// bit.

// sameLane reports whether two lane maxima are equal bit for bit, except that
// a zero of either sign counts as equal.
//
// That relaxation is the only latitude a kernel has here, and it is not
// observable through Scorer. Three conventions disagree about the sign of a
// zero maximum (ARM's FMAX returns +0 for +0 and -0, Intel's MAXPS returns its
// second operand, Go's `v > best` keeps the first), and the case is reachable
// in real data: a zero document token has Step = 0 and scores a zero of either
// sign against every query token. Distance negates the maximum and adds it to
// a sum that starts at +0, and +0 plus a zero of either sign is +0, so Distance
// is unaffected; the parity tests against RQ1LUTInt8Scorer compare Distance
// bit for bit and assert that.
func sameLane(a, b float32) bool {
	if a == 0 && b == 0 {
		return true
	}
	return math.Float32bits(a) == math.Float32bits(b)
}

// deinterleaveBlock16 inverts interleaveBlock16: it takes a code section in
// LayoutTokenBlock16 order (tokens tokens of perToken bytes) and returns it in
// token-major order. Written from the layout's definition, so a round trip
// through it is evidence.
func deinterleaveBlock16(codes []byte, tokens, perToken int) []byte {
	out := make([]byte, len(codes))
	blocks := tokens / blockTokens
	for b := 0; b < blocks; b++ {
		base := b * blockTokens * perToken
		for j := 0; j < blockTokens; j++ {
			for g := 0; g < perToken; g++ {
				out[base+j*perToken+g] = codes[base+g*blockTokens+j]
			}
		}
	}
	copy(out[blocks*blockTokens*perToken:], codes[blocks*blockTokens*perToken:])
	return out
}

// TestInterleaveBlock16 pins the interleave as a permutation: byte positions
// from the layout's definition, the tail verbatim, and a round trip through
// deinterleaveBlock16.
func TestInterleaveBlock16(t *testing.T) {
	for _, perToken := range []int{1, 8, 16, 32} {
		for _, tokens := range []int{0, 1, 15, 16, 17, 31, 32, 130} {
			t.Run(fmt.Sprintf("pt=%d/n=%d", perToken, tokens), func(t *testing.T) {
				rng := rand.New(rand.NewSource(int64(perToken*1000 + tokens)))
				codes := make([]byte, tokens*perToken)
				for i := range codes {
					codes[i] = byte(rng.Intn(256))
				}

				got := interleaveBlock16(codes, tokens, perToken)
				if len(got) != len(codes) {
					t.Fatalf("interleaved to %d bytes, want %d", len(got), len(codes))
				}

				// the permutation itself, from the layout's definition: within
				// a full block, token j's code byte g sits at g*16+j
				blocks := tokens / blockTokens
				for b := 0; b < blocks; b++ {
					base := b * blockTokens * perToken
					for j := 0; j < blockTokens; j++ {
						for g := 0; g < perToken; g++ {
							if got[base+g*blockTokens+j] != codes[base+j*perToken+g] {
								t.Fatalf("block %d token %d byte %d misplaced", b, j, g)
							}
						}
					}
				}
				// the remainder is a token-major tail, byte for byte
				tailAt := blocks * blockTokens * perToken
				if !bytes.Equal(got[tailAt:], codes[tailAt:]) {
					t.Fatalf("tail of %d bytes was permuted", len(codes)-tailAt)
				}
				if back := deinterleaveBlock16(got, tokens, perToken); !bytes.Equal(back, codes) {
					t.Fatal("the interleave does not round-trip")
				}
			})
		}
	}
}

// TestEncodeRQ1Block16 pins EncodeRQ1Block16 to EncodeRQ1: same header except
// the layout byte, same scalars, and codes that de-interleave to the
// token-major ones.
func TestEncodeRQ1Block16(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(5))
	p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}

	for _, n := range []int{0, 1, 15, 16, 17, 130} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			doc := unitTokens(rng, n, dims)
			plain, err := EncodeRQ1(doc, p)
			if err != nil {
				t.Fatalf("EncodeRQ1: %v", err)
			}
			blocked, err := EncodeRQ1Block16(doc, p)
			if err != nil {
				t.Fatalf("EncodeRQ1Block16: %v", err)
			}
			if len(plain) != len(blocked) {
				t.Fatalf("blocked blob is %d bytes, token-major is %d; the layout is a permutation",
					len(blocked), len(plain))
			}

			pb, err := Parse(plain)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			bb, err := Parse(blocked)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if bb.Layout != LayoutTokenBlock16 {
				t.Fatalf("layout = %d, want %d", uint8(bb.Layout), uint8(LayoutTokenBlock16))
			}
			want := pb.Header
			want.Layout = LayoutTokenBlock16
			if bb.Header != want {
				t.Fatalf("header = %+v, want %+v", bb.Header, want)
			}
			if !bytes.Equal(bb.Scalars(), pb.Scalars()) {
				t.Fatal("the scalar section differs; only the code section is permuted")
			}
			back := deinterleaveBlock16(bb.Codes(), n, bb.perToken)
			if !bytes.Equal(back, pb.Codes()) {
				t.Fatal("de-interleaved codes differ from the token-major ones")
			}
		})
	}
}

// TestRQ1ScanBlocksKernels holds every block kernel this build carries (each
// entry of rq1ScanBlocksVariants, since machines without the widest
// instruction set run the others) to the portable one at bit equality. It also
// asserts that the installed kernel is a listed one, so no kernel can be
// dispatched without passing here.
//
// The shapes are the kernel's, including ones no blob produces: no blocks at all,
// every block count modulo four so a kernel taking blocks two or four at a
// time meets each remainder with and without a full pass before it, the int16
// accumulators' exact boundary (258 nibble groups at +-127 in every lane), a
// single group, Steps of both signs and zero, and a zero scale. On a build
// with no vector kernel the list is empty and the portable implementation
// must be what is installed.
func TestRQ1ScanBlocksKernels(t *testing.T) {
	installed := reflect.ValueOf(rq1ScanBlocksImpl).Pointer()
	if len(rq1ScanBlocksVariants) == 0 {
		if installed != reflect.ValueOf(rq1ScanBlocksGo).Pointer() {
			t.Fatal("a kernel is installed but rq1ScanBlocksVariants lists none")
		}
		t.Skip("no vector kernel on this build")
	}
	listed := false
	for _, v := range rq1ScanBlocksVariants {
		if reflect.ValueOf(v.fn).Pointer() == installed {
			listed = true
		}
	}
	if !listed {
		t.Fatal("the installed kernel is not in rq1ScanBlocksVariants")
	}

	for _, v := range rq1ScanBlocksVariants {
		t.Run(v.name, func(t *testing.T) {
			testRQ1ScanBlocksKernel(t, v.fn)
		})
	}
}

// testRQ1ScanBlocksKernel is TestRQ1ScanBlocksKernels's body for one kernel:
// it runs kernel and rq1ScanBlocksGo over the same inputs and compares the
// lane maxima with sameLane.
func testRQ1ScanBlocksKernel(t *testing.T, kernel func(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32)) {
	t.Helper()
	for _, groups := range []int{1, 2, 8, 16, 17, 129} {
		for _, blocks := range []int{0, 1, 2, 3, 4, 7, 9} {
			for _, extreme := range []bool{false, true} {
				name := fmt.Sprintf("g=%d/b=%d/extreme=%v", groups, blocks, extreme)
				t.Run(name, func(t *testing.T) {
					rng := rand.New(rand.NewSource(int64(groups*100 + blocks)))
					tbl := make([]int8, 32*groups)
					for i := range tbl {
						if extreme {
							// every lookup at the accumulator's limit, so
							// groups=129 lands exactly on 258*127 = 32766
							tbl[i] = rq1Int8Levels
						} else {
							tbl[i] = int8(rng.Intn(255) - 127)
						}
					}
					codes := make([]byte, blocks*blockTokens*groups)
					for i := range codes {
						codes[i] = byte(rng.Intn(256))
					}
					steps := make([]float32, blocks*blockTokens)
					for i := range steps {
						switch i % 5 {
						case 0:
							steps[i] = 0
						case 1:
							steps[i] = float32(-rng.Float64())
						default:
							steps[i] = float32(rng.Float64())
						}
					}

					for _, scale := range []float32{0, 1, 0.10546875, float32(rng.Float64())} {
						want := make([]float32, blockTokens)
						got := make([]float32, blockTokens)
						for i := range want {
							want[i] = -math.MaxFloat32
							got[i] = -math.MaxFloat32
						}
						rq1ScanBlocksGo(tbl, codes, steps, want, groups, blocks, scale)
						kernel(tbl, codes, steps, got, groups, blocks, scale)
						for i := range want {
							if !sameLane(got[i], want[i]) {
								t.Fatalf("scale %v lane %d: kernel = %v (%#08x), portable = %v (%#08x)",
									scale, i, got[i], math.Float32bits(got[i]),
									want[i], math.Float32bits(want[i]))
							}
						}
					}
				})
			}
		}
	}
}

// rq1ScanTileGo is the parity reference for the tiled vector kernels: the
// portable block kernel run once per query token. Only the tests call it; a
// scorer without a tiled vector kernel scores query tokens one at a time.
// Arguments are rq1ScanTileImpl's plus tile, the number of query tokens.
func rq1ScanTileGo(tbl []int8, tblStride int, codes []byte, steps, best, scales []float32, groups, blocks, tile int) {
	for t := range tile {
		rq1ScanBlocksGo(tbl[t*tblStride:], codes, steps, best[t*blockTokens:], groups, blocks, scales[t])
	}
}

// TestRQ1ScanTileKernels holds the tiled kernel this build installed to
// rq1ScanTileGo at bit equality, over the shapes TestRQ1ScanBlocksKernels
// uses. The reference is the one-token kernel run once per query token, so
// this asserts exactly the tile's claim: tiling changes only when the
// arithmetic happens.
//
// Scales differ per query token and the tables are independent, which catches
// a kernel that reads one query token's table or scale for another, the
// failure only a tile can have. On a build with no tiled kernel this is
// skipped and TestRQ1ScanTileWidth holds instead.
func TestRQ1ScanTileKernels(t *testing.T) {
	if rq1ScanTileImpl == nil {
		t.Skip("no tiled kernel on this build")
	}
	width := rq1ScanTileWidth
	for _, groups := range []int{1, 2, 8, 16, 17, 129} {
		for _, blocks := range []int{1, 2, 9} {
			for _, extreme := range []bool{false, true} {
				name := fmt.Sprintf("g=%d/b=%d/extreme=%v", groups, blocks, extreme)
				t.Run(name, func(t *testing.T) {
					rng := rand.New(rand.NewSource(int64(groups*100 + blocks)))
					stride := 32 * groups
					tbl := make([]int8, width*stride)
					for i := range tbl {
						if extreme {
							tbl[i] = rq1Int8Levels
						} else {
							tbl[i] = int8(rng.Intn(255) - 127)
						}
					}
					codes := make([]byte, blocks*blockTokens*groups)
					for i := range codes {
						codes[i] = byte(rng.Intn(256))
					}
					steps := make([]float32, blocks*blockTokens)
					for i := range steps {
						switch i % 5 {
						case 0:
							steps[i] = 0
						case 1:
							steps[i] = float32(-rng.Float64())
						default:
							steps[i] = float32(rng.Float64())
						}
					}
					// one scale per query token, all different and one of them
					// zero: a kernel broadcasting one scale over the tile
					// would pass with equal scales and fails here
					scales := make([]float32, width)
					for i := range scales {
						scales[i] = float32(rng.Float64())
					}
					scales[width-1] = 0

					want := make([]float32, width*blockTokens)
					got := make([]float32, width*blockTokens)
					for i := range want {
						want[i] = -math.MaxFloat32
						got[i] = -math.MaxFloat32
					}
					rq1ScanTileGo(tbl, stride, codes, steps, want, scales, groups, blocks, width)
					rq1ScanTileImpl(tbl, stride, codes, steps, got, scales, groups, blocks)
					for i := range want {
						if !sameLane(got[i], want[i]) {
							t.Fatalf("query token %d lane %d: kernel = %v (%#08x), portable = %v (%#08x)",
								i/blockTokens, i%blockTokens, got[i], math.Float32bits(got[i]),
								want[i], math.Float32bits(want[i]))
						}
					}
				})
			}
		}
	}
}

// TestRQ1ScanTileWidth pins the scorer's tile width against what the build
// installed, including the int16 fallback dropping the tile: the tiled kernels
// accumulate in int16 like the block ones, so a scorer that fell back for the
// width-1 path and kept tiling would wrap silently above the bound.
func TestRQ1ScanTileWidth(t *testing.T) {
	if (rq1ScanTileImpl == nil) != (rq1ScanTileWidth == 1) {
		t.Fatalf("tile width %d with impl nil = %v", rq1ScanTileWidth, rq1ScanTileImpl == nil)
	}

	rng := rand.New(rand.NewSource(41))
	for _, dims := range []int{128, 1024, 1088} {
		t.Run(fmt.Sprintf("d=%d", dims), func(t *testing.T) {
			p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
			if err != nil {
				t.Fatalf("NewRQ1Params: %v", err)
			}
			s, err := NewRQ1LUTFastScanScorer(unitTokens(rng, 8, dims), p)
			if err != nil {
				t.Fatalf("NewRQ1LUTFastScanScorer: %v", err)
			}
			want := rq1ScanTileWidth
			if s.nibbles > rq1ScanMaxNibbles {
				want = 1
			}
			if s.tile != want {
				t.Fatalf("%d nibble groups: tile = %d, want %d", s.nibbles, s.tile, want)
			}
			if s.tile > 1 && len(s.tileLanes) != s.tile*blockTokens {
				t.Fatalf("tile %d with %d lanes of scratch", s.tile, len(s.tileLanes))
			}
		})
	}
}

// TestRQ1ScanFallbackGate pins the int16 accumulators' limit and the scorer's
// switch at it. Neither is reachable from a parity test: the kernels agree at
// the boundary (TestRQ1ScanBlocksKernels drives 258 nibble groups to exactly
// 32766), and real tokens never approach it (272 groups of unit-norm
// coordinates sum to a few hundred, so a scorer with no fallback still agrees
// with the reference at d=1088). So the rule itself is asserted.
func TestRQ1ScanFallbackGate(t *testing.T) {
	if rq1ScanMaxNibbles*rq1Int8Levels > math.MaxInt16 {
		t.Fatalf("%d nibble groups at +-%d overflow int16", rq1ScanMaxNibbles, rq1Int8Levels)
	}
	if (rq1ScanMaxNibbles+1)*rq1Int8Levels <= math.MaxInt16 {
		t.Fatalf("%d nibble groups still fit int16; the limit is too low", rq1ScanMaxNibbles+1)
	}

	rng := rand.New(rand.NewSource(37))
	for _, dims := range []int{128, 1024, 1088} {
		t.Run(fmt.Sprintf("d=%d", dims), func(t *testing.T) {
			p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
			if err != nil {
				t.Fatalf("NewRQ1Params: %v", err)
			}
			s, err := NewRQ1LUTFastScanScorer(unitTokens(rng, 4, dims), p)
			if err != nil {
				t.Fatalf("NewRQ1LUTFastScanScorer: %v", err)
			}
			want := rq1ScanBlocksImpl
			if s.nibbles > rq1ScanMaxNibbles {
				want = rq1ScanBlocksGo
			}
			if reflect.ValueOf(s.scan).Pointer() != reflect.ValueOf(want).Pointer() {
				t.Fatalf("%d nibble groups: the scorer bound the wrong kernel", s.nibbles)
			}
		})
	}
}

// assertRQ1FastScanParity scores every document of docs against query with
// RQ1LUTInt8Scorer over token-major bytes and with RQ1LUTFastScanScorer over
// the interleaved ones, both under p, and requires bit equality. label names
// the case in failures. The scorers are reused across documents of unsorted
// sizes, which exercises the widened-Step and lane-maximum scratch.
func assertRQ1FastScanParity(t *testing.T, query [][]float32, docs [][][]float32, p *RQ1Params, label string) {
	t.Helper()

	ref, err := NewRQ1LUTInt8Scorer(query, p)
	if err != nil {
		t.Fatalf("%s: NewRQ1LUTInt8Scorer: %v", label, err)
	}
	fs, err := NewRQ1LUTFastScanScorer(query, p)
	if err != nil {
		t.Fatalf("%s: NewRQ1LUTFastScanScorer: %v", label, err)
	}

	for i, doc := range docs {
		plain, err := EncodeRQ1(doc, p)
		if err != nil {
			t.Fatalf("%s doc %d: EncodeRQ1: %v", label, i, err)
		}
		blocked, err := EncodeRQ1Block16(doc, p)
		if err != nil {
			t.Fatalf("%s doc %d: EncodeRQ1Block16: %v", label, i, err)
		}
		pb, err := Parse(plain)
		if err != nil {
			t.Fatalf("%s doc %d: Parse: %v", label, i, err)
		}
		bb, err := Parse(blocked)
		if err != nil {
			t.Fatalf("%s doc %d: Parse: %v", label, i, err)
		}

		want, err := ref.Distance(pb)
		if err != nil {
			t.Fatalf("%s doc %d: RQ1LUTInt8Scorer.Distance: %v", label, i, err)
		}
		got, err := fs.Distance(bb)
		if err != nil {
			t.Fatalf("%s doc %d: RQ1LUTFastScanScorer.Distance: %v", label, i, err)
		}
		// compared through the bit patterns so a NaN, or a zero of the wrong
		// sign, fails instead of comparing equal or being skipped
		if math.Float32bits(got) != math.Float32bits(want) {
			t.Errorf("%s doc %d (%d tokens): fast scan = %v (%#08x), RQ1LUTInt8Scorer = %v (%#08x)",
				label, i, len(doc), got, math.Float32bits(got), want, math.Float32bits(want))
		}
	}
}

// TestRQ1FastScanParity covers the grid of dimensionalities and document
// sizes. d=1088 has 272 nibble groups, past rq1ScanMaxNibbles, so it is the row
// that runs the portable fallback; the rest run whatever kernel this build
// installed. Document sizes straddle every block/tail split of
// blockTokens = 16.
func TestRQ1FastScanParity(t *testing.T) {
	for _, dims := range []int{64, 100, 128, 256, 1024, 1088} {
		for _, centered := range []bool{false, true} {
			name := "uncentered"
			if centered {
				name = "centered"
			}
			t.Run(fmt.Sprintf("d=%d/%s", dims, name), func(t *testing.T) {
				rng := rand.New(rand.NewSource(int64(dims)))
				query := unitTokens(rng, 32, dims)

				// sizes out of order on purpose, so scratch sized for the
				// largest document is reused for a tiny one and grown again
				docs := make([][][]float32, 0, 10)
				for _, n := range []int{124, 130, 0, 1, 15, 16, 17, 180, 31, 6} {
					docs = append(docs, unitTokens(rng, n, dims))
				}

				var mean []float32
				var id uint16
				var version uint16
				if centered {
					mean = tokenMean(docs[0], dims)
					id, version = 7, 3
				}
				p, err := NewRQ1Params(dims, 0x5eed, mean, id, version)
				if err != nil {
					t.Fatalf("NewRQ1Params: %v", err)
				}
				assertRQ1FastScanParity(t, query, docs, p, name)
			})
		}
	}
}

// TestRQ1FastScanQueryShapes covers the query side, which is tiled: query
// tokens are scored rq1ScanTileWidth at a time and the leftover ones one at a
// time. The shapes split differently (none, fewer than a tile, exactly a tile,
// a tile and a remainder, and the same around two tiles), so whatever width a
// build installs, some of them exercise both paths and their boundary. At
// width 4 that is 3 (no tile), 4 (one, no remainder), 5 and 9 (tile plus one),
// 7 (tile plus three).
func TestRQ1FastScanQueryShapes(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(13))
	doc := unitTokens(rng, 130, dims)
	p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	for _, nq := range []int{0, 1, 3, 4, 5, 7, 8, 9, 32, 33} {
		t.Run(fmt.Sprintf("nq=%d", nq), func(t *testing.T) {
			assertRQ1FastScanParity(t, unitTokens(rng, nq, dims), [][][]float32{doc}, p, "query shape")
		})
	}
}

// TestRQ1FastScanEdgeCases covers the documents where the maximum is degenerate
// or the estimate is exactly zero. "all negative" is the one that pins the lane
// maxima's seed: every estimate is below zero, so a kernel seeding its lanes at
// zero and never reading the caller's -MaxFloat32 still reports zero.
func TestRQ1FastScanEdgeCases(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(29))
	query := unitTokens(rng, 32, dims)

	identical := unitTokens(rng, 1, dims)[0]
	allIdentical := make([][]float32, 20)
	for i := range allIdentical {
		allIdentical[i] = identical
	}
	allZero := make([][]float32, 18)
	for i := range allZero {
		allZero[i] = make([]float32, dims)
	}
	// every document token is the negation of a query token, so every
	// estimate is negative
	allNegative := make([][]float32, 17)
	for i := range allNegative {
		t := make([]float32, dims)
		for j, v := range query[i%len(query)] {
			t[j] = -v
		}
		allNegative[i] = t
	}

	cases := []struct {
		name string
		doc  [][]float32
	}{
		{"empty", [][]float32{}},
		{"nil", nil},
		{"single token", unitTokens(rng, 1, dims)},
		{"one exact block", unitTokens(rng, 16, dims)},
		{"tail only", unitTokens(rng, 9, dims)},
		{"all identical", allIdentical},
		{"all zero tokens", allZero},
		{"all negative", allNegative},
		{"mixed zero tokens", unitTokens(rng, 24, dims)},
	}

	for _, centered := range []bool{false, true} {
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
		for _, tc := range cases {
			t.Run(fmt.Sprintf("%s/%s", name, tc.name), func(t *testing.T) {
				assertRQ1FastScanParity(t, query, [][][]float32{tc.doc}, p, tc.name)
			})
		}
	}
}

// TestRQ1FastScanRejections pins the layout check in both directions. The two
// layouts hold the same bytes in a different order and are exactly the same
// length, so nothing but the header byte can tell them apart and an unchecked
// mismatch scores silently wrong.
func TestRQ1FastScanRejections(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(31))
	query := unitTokens(rng, 8, dims)
	doc := unitTokens(rng, 40, dims)

	p, err := NewRQ1Params(dims, 0x5eed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	other, err := NewRQ1Params(dims, 0x5eed, tokenMean(doc, dims), 4, 1)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}

	plain, err := EncodeRQ1(doc, p)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	blocked, err := EncodeRQ1Block16(doc, p)
	if err != nil {
		t.Fatalf("EncodeRQ1Block16: %v", err)
	}
	f32Blob, err := EncodeFloat32(doc, dims)
	if err != nil {
		t.Fatalf("EncodeFloat32: %v", err)
	}

	fs, err := NewRQ1LUTFastScanScorer(query, p)
	if err != nil {
		t.Fatalf("NewRQ1LUTFastScanScorer: %v", err)
	}
	wrongParams, err := NewRQ1LUTFastScanScorer(query, other)
	if err != nil {
		t.Fatalf("NewRQ1LUTFastScanScorer: %v", err)
	}

	for _, tt := range []struct {
		name   string
		scorer Scorer
		blob   []byte
	}{
		{"token-major blob", fs, plain},
		{"float32 blob", fs, f32Blob},
		{"another parameter set", wrongParams, blocked},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b, err := Parse(tt.blob)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if _, err := tt.scorer.Distance(b); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}

	// and the other direction: every scorer over token-major RQ1 must refuse
	// these bytes, which it would otherwise walk at the wrong stride
	b, err := Parse(blocked)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ref, err := NewRQ1LUTInt8Scorer(query, p)
	if err != nil {
		t.Fatalf("NewRQ1LUTInt8Scorer: %v", err)
	}
	plainScorer, err := NewRQ1Scorer(query, p)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}
	for _, tt := range []struct {
		name   string
		scorer Scorer
	}{
		{"RQ1LUTInt8Scorer", ref},
		{"RQ1Scorer", plainScorer},
	} {
		t.Run(tt.name+" refuses block16", func(t *testing.T) {
			if _, err := tt.scorer.Distance(b); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// TestGoldenRQ1Block16 checks the committed v1 blob in the interleaved layout.
// It pins what TestGoldenRQ1 pins (the format and the rotation) plus the
// permutation itself, which is why it has more than one block: at 20 tokens it
// covers a full block and a four-token tail. Regenerate with -update only when
// the format version is bumped.
func TestGoldenRQ1Block16(t *testing.T) {
	const dims = 128
	const seed = 42
	goldenTokens := fixedTokens(20, dims)

	p, err := NewRQ1Params(dims, seed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	blob, err := EncodeRQ1Block16(goldenTokens, p)
	if err != nil {
		t.Fatalf("EncodeRQ1Block16: %v", err)
	}

	path := filepath.Join("testdata", "v1_rq1_block16.blob")
	if *update {
		if err := os.WriteFile(path, blob, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	onDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(onDisk, blob) {
		t.Fatalf("%s differs from what this build writes; the format, the interleave or the "+
			"rotation changed without a version bump", path)
	}

	parsed, err := Parse(onDisk)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := Header{
		Version: 1, Encoding: EncodingRQ1, Layout: LayoutTokenBlock16,
		ScalarKind: ScalarFloat16, Dims: dims, Tokens: 20,
	}
	if parsed.Header != want {
		t.Fatalf("header = %+v, want %+v", parsed.Header, want)
	}
	if got := parsed.Scalar(1); got != 0 {
		t.Fatalf("the zero token's Step = %v, want exactly 0", got)
	}
}
