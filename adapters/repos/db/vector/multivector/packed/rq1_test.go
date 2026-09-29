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
	"strings"
	"testing"

	"github.com/tphakala/simd/f16"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// TestRQ1BlobSize pins the exact size of an RQ1 blob at every shape: the
// 24-byte header, then per token ceil(dims/64)*8 code bytes and a 2-byte
// scalar, including the round-up of the code width at dimensions that are not
// a multiple of 64.
func TestRQ1BlobSize(t *testing.T) {
	for _, tc := range []struct {
		dims, codeBytes int
	}{
		{8, 8}, {100, 16}, {128, 16}, {256, 32}, {1024, 128},
	} {
		p, err := NewRQ1Params(tc.dims, 42, nil, 0, 0)
		if err != nil {
			t.Fatalf("NewRQ1Params: %v", err)
		}
		for _, n := range []int{0, 1, 32, 124, 180, 1030} {
			blob, err := EncodeRQ1(fixedTokens(n, tc.dims), p)
			if err != nil {
				t.Fatalf("EncodeRQ1 d=%d n=%d: %v", tc.dims, n, err)
			}
			if want := headerLenV1 + n*(tc.codeBytes+2); len(blob) != want {
				t.Fatalf("d=%d n=%d: blob is %d bytes, want %d", tc.dims, n, len(blob), want)
			}
		}
	}
}

// narrowStep replaces the float32 Step of a house RQ code with the value the
// blob's binary16 scalar section restores, in place, and returns the code. The
// house distancer then computes with exactly the Step the packed scorer reads.
// Word zero holds Step in its low half and SquaredNorm in its high half (see
// RQOneBitCode).
func narrowStep(code []uint64) []uint64 {
	narrowed := f16.ToFloat32(f16.FromFloat32(compressionhelpers.RQOneBitCode(code).Step()))
	const lower32 = (uint64(1) << 32) - 1
	code[0] = (code[0] &^ lower32) | uint64(math.Float32bits(narrowed))
	return code
}

// referenceBRQMaxSim is the MaxSim loop run over the house quantizer: the
// packed scorer's answer computed by compressionhelpers' own arithmetic.
//
// Arguments:
//   - brq: the house quantizer to build one BinaryRQDistancer per query token.
//   - query: the query tokens.
//   - codes: the document's house codes, one per token.
//   - corr: the <q, mu> correction per query token, subtracted after the
//     minimum; all zero when uncentered.
func referenceBRQMaxSim(t *testing.T, brq *compressionhelpers.BinaryRotationalQuantizer, query [][]float32, codes [][]uint64, corr []float32) float32 {
	t.Helper()
	var sum float32
	for qi, q := range query {
		dist := brq.NewDistancer(q)
		best := float32(math.MaxFloat32)
		for _, code := range codes {
			d, err := dist.Distance(code)
			if err != nil {
				t.Fatalf("BinaryRQDistancer.Distance: %v", err)
			}
			if d < best {
				best = d
			}
		}
		sum += best - corr[qi]
	}
	return sum
}

// TestRQ1ParityWithBRQ holds RQ1Scorer to BinaryRotationalQuantizer bit for
// bit. The only difference is the binary16 Step, which narrowStep applies to
// the reference side too, so the assertion is exact equality.
//
// The scorer delegates to BinaryRQDistancer, so what these cases pin is
// everything around that delegation: the blob round trip (sign words and
// binary16 Step written by EncodeRQ1 and rebuilt into house codes by the
// scorer must score the same as the codes Encode returns directly) and the
// MaxSim loop with its correction order.
//
// The reference quantizer is built two ways. At and above 256 dimensions
// NewBinaryRotationalQuantizer derives everything from the seed on its own, so
// those cases also pin NewRQ1Params' construction (the unpadded rotation and
// the mirrored rounding derivation) against the house one. Below 256 the
// from-seed constructor would pad the code to 256 bits, which the packed
// format does not (see EncodingRQ1), so there the reference is the params' own
// restored quantizer and the cases pin the round trip and the loop at the
// native width.
//
// The centered variant has no counterpart in compressionhelpers, so the
// reference is the same quantizer fed pre-centered tokens, with the <q, mu>
// correction applied in the same order as the scorer applies it.
func TestRQ1ParityWithBRQ(t *testing.T) {
	shapes := []struct {
		dims, nDoc, nQ int
	}{
		{128, 124, 32}, // a typical text document, via the restored reference
		{128, 1, 32},
		{128, 12, 0},  // empty query: nothing to sum, the distance is 0
		{100, 57, 32}, // code width rounds up to 128 bits
		{256, 124, 32},
		{256, 1, 32},
		{256, 0, 32}, // empty document: saturates to MaxFloat32 per query token
		{320, 57, 32},
		{1024, 33, 8},
	}
	for _, tc := range shapes {
		for _, centered := range []bool{false, true} {
			name := fmt.Sprintf("d%d_n%d_nq%d_centered=%v", tc.dims, tc.nDoc, tc.nQ, centered)
			t.Run(name, func(t *testing.T) {
				rng := rand.New(rand.NewSource(int64(tc.dims*100 + tc.nDoc)))
				doc := unitTokens(rng, tc.nDoc, tc.dims)
				query := unitTokens(rng, tc.nQ, tc.dims)
				seed := uint64(tc.dims*1000 + tc.nDoc)

				var mean []float32
				var id uint16
				var version uint16
				if centered {
					mean = tokenMean(doc, tc.dims)
					id, version = 7, 3
				}
				params, err := NewRQ1Params(tc.dims, seed, mean, id, version)
				if err != nil {
					t.Fatalf("NewRQ1Params: %v", err)
				}
				got := rq1Distance(t, doc, query, params)

				// below 256 dimensions the from-seed constructor would pad
				// to 256 bits, so the reference is the params' own restored
				// quantizer; at 256 and above an independently built one also
				// pins NewRQ1Params' construction against the house one
				brq := params.brq
				if tc.dims >= 256 {
					brq, err = compressionhelpers.NewBinaryRotationalQuantizer(tc.dims, seed, distancer.NewDotProductProvider())
					if err != nil {
						t.Fatalf("NewBinaryRotationalQuantizer: %v", err)
					}
				}
				corr := make([]float32, tc.nQ)
				encodeInput := doc
				if centered {
					encodeInput = make([][]float32, len(doc))
					for i, tok := range doc {
						c := make([]float32, tc.dims)
						for j := range c {
							c[j] = tok[j] - mean[j]
						}
						encodeInput[i] = c
					}
					for qi, q := range query {
						var c float32
						for j, m := range mean {
							c += q[j] * m
						}
						corr[qi] = c
					}
				}
				codes := make([][]uint64, len(encodeInput))
				for i, tok := range encodeInput {
					codes[i] = narrowStep(brq.Encode(tok))
				}
				want := referenceBRQMaxSim(t, brq, query, codes, corr)

				if math.Float32bits(got) != math.Float32bits(want) {
					t.Fatalf("packed %v (%#08x), BRQ reference %v (%#08x): the two quantizers must agree bit for bit",
						got, math.Float32bits(got), want, math.Float32bits(want))
				}
			})
		}
	}
}

// tokenMean returns the coordinate-wise mean of tokens (token vectors of
// dimensionality dims), the mu a caller would train for centered RQ1. All
// zeros when there are no tokens.
func tokenMean(tokens [][]float32, dims int) []float32 {
	mean := make([]float32, dims)
	if len(tokens) == 0 {
		return mean
	}
	sums := make([]float64, dims)
	for _, t := range tokens {
		for j, v := range t {
			sums[j] += float64(v)
		}
	}
	for j := range mean {
		mean[j] = float32(sums[j] / float64(len(tokens)))
	}
	return mean
}

// rq1Distance encodes doc under params and scores it against query with the
// 5-bit scorer, failing the test on any error.
//
// Arguments:
//   - doc: the document tokens.
//   - query: the query tokens.
//   - params: the parameters to encode and score under.
func rq1Distance(t *testing.T, doc, query [][]float32, params *RQ1Params) float32 {
	t.Helper()
	blob, err := EncodeRQ1(doc, params)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	parsed, err := Parse(blob)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	scorer, err := NewRQ1Scorer(query, params)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}
	got, err := scorer.Distance(parsed)
	if err != nil {
		t.Fatalf("Distance: %v", err)
	}
	return got
}

// TestRQ1ZeroQueryToken pins the query-side zero path against the same BRQ
// reference: a zero query token quantizes to step 0 and contributes exactly 0
// against every document token.
func TestRQ1ZeroQueryToken(t *testing.T) {
	const dims = 256
	rng := rand.New(rand.NewSource(11))
	doc := unitTokens(rng, 12, dims)
	query := unitTokens(rng, 4, dims)
	query[2] = make([]float32, dims)

	params, err := NewRQ1Params(dims, 99, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	got := rq1Distance(t, doc, query, params)

	brq, err := compressionhelpers.NewBinaryRotationalQuantizer(dims, 99, distancer.NewDotProductProvider())
	if err != nil {
		t.Fatalf("NewBinaryRotationalQuantizer: %v", err)
	}
	codes := make([][]uint64, len(doc))
	for i, tok := range doc {
		codes[i] = narrowStep(brq.Encode(tok))
	}
	want := referenceBRQMaxSim(t, brq, query, codes, make([]float32, len(query)))
	if math.Float32bits(got) != math.Float32bits(want) {
		t.Fatalf("packed %v, BRQ reference %v", got, want)
	}
}

// TestRQ1ZeroTokensUncentered pins the zero document token: it encodes to an
// all-zero code with Step exactly 0, so its estimate against any query token
// is exactly 0, the true value of <q, 0>. Real collections hold zero tokens,
// and a zero token can win the maximum.
func TestRQ1ZeroTokensUncentered(t *testing.T) {
	const dims = 128
	params, err := NewRQ1Params(dims, 42, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}

	doc := [][]float32{make([]float32, dims)}
	blob, err := EncodeRQ1(doc, params)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	parsed, err := Parse(blob)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got := parsed.Scalar(0); got != 0 {
		t.Fatalf("zero token's Step = %v, want exactly 0", got)
	}
	if !bytes.Equal(parsed.Code(0), make([]byte, len(parsed.Code(0)))) {
		t.Fatal("zero token's code has bits set; the rotation of zero is zero")
	}

	rng := rand.New(rand.NewSource(5))
	scorer, err := NewRQ1Scorer(unitTokens(rng, 32, dims), params)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}
	got, err := scorer.Distance(parsed)
	if err != nil {
		t.Fatalf("Distance: %v", err)
	}
	if got != 0 {
		t.Fatalf("distance to an all-zero document = %v, want exactly 0", got)
	}
}

// TestRQ1ZeroTokensCentered pins the zero document token under centering,
// where it becomes -mu: a real vector with a real code and a non-zero Step,
// whose estimate only the <q, mu> correction brings back near 0, with ordinary
// quantization error around it. Measured at this seed the distance is -0.240
// (32 query tokens, each with an independent 1-bit estimation error on
// <q, -mu>); the bound is about 3x that. What matters is that it is small and,
// unlike the uncentered case, not exact.
func TestRQ1ZeroTokensCentered(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(17))
	mean := make([]float32, dims)
	for j := range mean {
		mean[j] = float32(rng.NormFloat64()) * 0.05
	}
	params, err := NewRQ1Params(dims, 42, mean, 9, 1)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}

	doc := [][]float32{make([]float32, dims)}
	blob, err := EncodeRQ1(doc, params)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	parsed, err := Parse(blob)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if parsed.Scalar(0) == 0 {
		t.Fatal("centered zero token's Step is 0; it should encode -mu, a real vector")
	}

	query := unitTokens(rng, 32, dims)
	scorer, err := NewRQ1Scorer(query, params)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}
	got, err := scorer.Distance(parsed)
	if err != nil {
		t.Fatalf("Distance: %v", err)
	}
	if got == 0 {
		t.Fatal("centered distance to an all-zero document is exactly 0; expected quantization error around 0")
	}
	if math.Abs(float64(got)) > 0.75 {
		t.Fatalf("centered distance to an all-zero document = %v; its magnitude should be within quantization error of 0", got)
	}
}

// TestRQ1CenteringRankPreservation asserts the identity centering rests on:
// <q, t - mu> = <q, t> - <q, mu>, with the shift constant over document
// tokens, so the per-query-token argmax is unchanged and, after the
// correction, every document's score is unchanged. Computed in float64 so
// float32 rounding cannot affect the assertion.
func TestRQ1CenteringRankPreservation(t *testing.T) {
	const dims, nDoc, nQ, docs = 128, 60, 32, 25
	rng := rand.New(rand.NewSource(23))
	query := unitTokens(rng, nQ, dims)

	corpus := make([][][]float32, docs)
	var all [][]float32
	for i := range corpus {
		corpus[i] = unitTokens(rng, nDoc, dims)
		all = append(all, corpus[i]...)
	}
	mean := tokenMean(all, dims)

	dot64 := func(a, b []float32) float64 {
		var s float64
		for j := range a {
			s += float64(a[j]) * float64(b[j])
		}
		return s
	}

	plain := make([]float64, docs)
	shifted := make([]float64, docs)
	for i, doc := range corpus {
		for _, q := range query {
			qmu := dot64(q, mean)
			bestPlain, bestShifted := math.Inf(-1), math.Inf(-1)
			argPlain, argShifted := -1, -1
			for ti, tok := range doc {
				if s := dot64(q, tok); s > bestPlain {
					bestPlain, argPlain = s, ti
				}
				if s := dot64(q, tok) - qmu; s > bestShifted {
					bestShifted, argShifted = s, ti
				}
			}
			if argPlain != argShifted {
				t.Fatalf("doc %d: centering moved the argmax from token %d to %d", i, argPlain, argShifted)
			}
			plain[i] += bestPlain
			shifted[i] += bestShifted + qmu
		}
		if diff := math.Abs(plain[i] - shifted[i]); diff > 1e-9 {
			t.Fatalf("doc %d: corrected centered score %v differs from plain %v by %v", i, shifted[i], plain[i], diff)
		}
	}
}

// TestRQ1ZeroMeanMatchesUncentered pins that centering with mu = 0 is the
// identity: code and scalar sections byte-identical to the uncentered blob
// (headers differ only in the quantizer reference) and scores bit-identical.
func TestRQ1ZeroMeanMatchesUncentered(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(31))
	doc := unitTokens(rng, 40, dims)
	query := unitTokens(rng, 32, dims)

	uncentered, err := NewRQ1Params(dims, 42, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	zeroMean, err := NewRQ1Params(dims, 42, make([]float32, dims), 5, 1)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}

	blobU, err := EncodeRQ1(doc, uncentered)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	blobZ, err := EncodeRQ1(doc, zeroMean)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	parsedU, err := Parse(blobU)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	parsedZ, err := Parse(blobZ)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !bytes.Equal(parsedU.Codes(), parsedZ.Codes()) || !bytes.Equal(parsedU.Scalars(), parsedZ.Scalars()) {
		t.Fatal("centering with a zero mean changed the encoded sections")
	}

	scorerU, err := NewRQ1Scorer(query, uncentered)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}
	scorerZ, err := NewRQ1Scorer(query, zeroMean)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}
	gotU, err := scorerU.Distance(parsedU)
	if err != nil {
		t.Fatalf("Distance: %v", err)
	}
	gotZ, err := scorerZ.Distance(parsedZ)
	if err != nil {
		t.Fatalf("Distance: %v", err)
	}
	if math.Float32bits(gotU) != math.Float32bits(gotZ) {
		t.Fatalf("zero-mean centered %v, uncentered %v: must be bit-identical", gotZ, gotU)
	}
}

// TestRQ1EstimateAccuracy is a sanity check that the estimator estimates: on
// synthetic unit-norm tokens the compressed MaxSim must sit near exact float32
// MaxSim. Measured at these seeds: mean |err| 1.69, max 2.24, against a mean
// exact |MaxSim| of 6.19, the expected scale for 1-bit codes at d=128 (per-pair
// error about D^-1/2, plus selection bias from the max over 124 tokens). The
// bounds are roughly 1.5x that and deliberately no tighter: Gaussian input is
// RQ1's best case, and ranking on real data is measured on real collections,
// never asserted here.
func TestRQ1EstimateAccuracy(t *testing.T) {
	const dims, nDoc, nQ, docs = 128, 124, 32, 20
	rng := rand.New(rand.NewSource(41))
	query := unitTokens(rng, nQ, dims)
	params, err := NewRQ1Params(dims, 42, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	scorer, err := NewRQ1Scorer(query, params)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}

	var sumAbs, maxAbs float64
	for i := range docs {
		doc := unitTokens(rand.New(rand.NewSource(int64(1000+i))), nDoc, dims)
		blob, err := EncodeRQ1(doc, params)
		if err != nil {
			t.Fatalf("EncodeRQ1: %v", err)
		}
		parsed, err := Parse(blob)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		got, err := scorer.Distance(parsed)
		if err != nil {
			t.Fatalf("Distance: %v", err)
		}
		exact := referenceOrdered(query, doc)
		abs := math.Abs(float64(got - exact))
		sumAbs += abs
		if abs > maxAbs {
			maxAbs = abs
		}
	}
	if mean := sumAbs / docs; mean > 2.5 {
		t.Fatalf("mean |estimate - exact| = %v over %d documents; the estimator is not estimating", mean, docs)
	}
	if maxAbs > 3.5 {
		t.Fatalf("max |estimate - exact| = %v over %d documents; the estimator is not estimating", maxAbs, docs)
	}
}

// TestNewRQ1ParamsRejects checks that the constructor refuses a dimensionality
// that cannot be written to the header, a mean of another dimensionality, a
// centered parameter set without a quantizer id, and an uncentered one with a
// quantizer reference.
func TestNewRQ1ParamsRejects(t *testing.T) {
	tests := []struct {
		name    string
		dims    int
		mean    []float32
		id      uint16
		version uint16
	}{
		{name: "zero dimensions", dims: 0},
		{name: "negative dimensions", dims: -1},
		{name: "dimensions beyond uint16", dims: math.MaxUint16 + 1},
		{name: "mean shorter than dims", dims: 4, mean: []float32{1, 2}, id: 1},
		{name: "mean longer than dims", dims: 4, mean: []float32{1, 2, 3, 4, 5}, id: 1},
		{name: "centered without a quantizer id", dims: 4, mean: []float32{1, 2, 3, 4}, id: 0},
		{name: "uncentered with a quantizer id", dims: 4, id: 1},
		{name: "uncentered with a quantizer version", dims: 4, version: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRQ1Params(tt.dims, 42, tt.mean, tt.id, tt.version); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// TestEncodeRQ1RejectsBadShapes checks that the encoder refuses a document
// whose tokens do not all have the parameters' dimensionality.
func TestEncodeRQ1RejectsBadShapes(t *testing.T) {
	params, err := NewRQ1Params(4, 42, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	tests := []struct {
		name   string
		tokens [][]float32
	}{
		{"token shorter than dims", [][]float32{{1, 2}}},
		{"token longer than dims", [][]float32{{1, 2, 3, 4, 5}}},
		{"ragged tokens", [][]float32{{1, 2, 3, 4}, {1, 2, 3}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := EncodeRQ1(tt.tokens, params); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// TestEncodeRQ1RejectsStepOverflow pins that the encoder refuses a token whose
// Step does not fit binary16, naming the token. Such a Step would be stored as
// +Inf and score silently wrong (see rq1Codes). Underflow needs no case: a
// Step that rounds to zero estimates zero, as a zero token already does.
func TestEncodeRQ1RejectsStepOverflow(t *testing.T) {
	const dims = 128
	// Step scales with the token's magnitude (about 1.21x it for a constant
	// token at these dimensions), so 1e4 lands well inside binary16's largest
	// finite value, 65504, and 1e5 well past it
	const (
		inRange  = 1e4
		overflow = 1e5
	)
	constToken := func(v float32) []float32 {
		tok := make([]float32, dims)
		for i := range tok {
			tok[i] = v
		}
		return tok
	}

	uncentered, err := NewRQ1Params(dims, 42, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	// the Step that reaches the blob is the centered token's, so the guard
	// runs after the subtraction. A mean of -1e5 flips the verdict both ways:
	// it pushes the in-range token past binary16 and pulls a token that would
	// overflow on its own back inside.
	centered, err := NewRQ1Params(dims, 42, constToken(-overflow), 1, 1)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	tests := []struct {
		name   string
		params *RQ1Params
		tokens [][]float32
		// wantErr is the substring the error must carry; empty means the
		// tokens must encode, and their stored Steps must all be finite.
		wantErr string
	}{
		{name: "inside binary16", params: uncentered, tokens: [][]float32{constToken(inRange)}},
		{
			name:    "above binary16",
			params:  uncentered,
			tokens:  [][]float32{constToken(overflow)},
			wantErr: "token 0",
		},
		{
			name:    "second token above binary16",
			params:  uncentered,
			tokens:  [][]float32{constToken(inRange), constToken(overflow)},
			wantErr: "token 1",
		},
		{
			// accepted above, rejected here: the mean puts it past binary16
			name:    "centered past binary16",
			params:  centered,
			tokens:  [][]float32{constToken(inRange)},
			wantErr: "token 0",
		},
		{
			// and the converse: on its own this token overflows
			name:   "centered back inside binary16",
			params: centered,
			tokens: [][]float32{constToken(inRange - overflow)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blob, err := EncodeRQ1(tt.tokens, tt.params)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not name %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("EncodeRQ1: %v", err)
			}
			// the accepted case has to be a real one: a Step stored as +Inf
			// would make the test pass without exercising anything
			b, err := Parse(blob)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			for i := range tt.tokens {
				if s := b.Scalar(i); math.IsInf(float64(s), 0) {
					t.Fatalf("token %d stores Step %v, want a finite one", i, s)
				}
			}
		})
	}
}

// TestRQ1ScorerRejectsMismatchedBlobs covers every blob RQ1Scorer must refuse.
// The quantizer-reference cases are the ones the header reference exists for:
// an RQ1 blob from a different seed or mean has the same shape and would score
// silently wrong.
func TestRQ1ScorerRejectsMismatchedBlobs(t *testing.T) {
	const dims = 128
	rng := rand.New(rand.NewSource(3))
	query := unitTokens(rng, 4, dims)
	doc := unitTokens(rng, 4, dims)

	params, err := NewRQ1Params(dims, 42, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	scorer, err := NewRQ1Scorer(query, params)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}

	assertRejected := func(t *testing.T, blob []byte, what string) {
		t.Helper()
		parsed, err := Parse(blob)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if _, err := scorer.Distance(parsed); err == nil {
			t.Fatalf("Distance accepted %s", what)
		}
	}

	// a float32 blob
	f32, err := EncodeFloat32(doc, dims)
	if err != nil {
		t.Fatalf("EncodeFloat32: %v", err)
	}
	assertRejected(t, f32, "a float32 blob")

	// the right encoding at the wrong width
	narrow, err := NewRQ1Params(64, 42, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	wrongDims, err := EncodeRQ1(unitTokens(rng, 4, 64), narrow)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	assertRejected(t, wrongDims, "an RQ1 blob of the wrong dimensionality")

	// an RQ1 blob referencing trained parameters this scorer does not hold
	centered, err := NewRQ1Params(dims, 42, make([]float32, dims), 5, 1)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	otherParams, err := EncodeRQ1(doc, centered)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	assertRejected(t, otherParams, "an RQ1 blob referencing other quantizer parameters")

	// the same id at another version: the mean may have been retrained
	otherVersion, err := NewRQ1Params(dims, 42, make([]float32, dims), 5, 2)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	otherVersionBlob, err := EncodeRQ1(doc, otherVersion)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	centeredScorer, err := NewRQ1Scorer(query, centered)
	if err != nil {
		t.Fatalf("NewRQ1Scorer: %v", err)
	}
	parsed, err := Parse(otherVersionBlob)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := centeredScorer.Distance(parsed); err == nil {
		t.Fatal("a scorer accepted a blob referencing another version of its quantizer")
	}

	// and the reverse of the reference check: a centered scorer refusing an
	// uncentered blob
	uncenteredBlob, err := EncodeRQ1(doc, params)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	parsed, err = Parse(uncenteredBlob)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := centeredScorer.Distance(parsed); err == nil {
		t.Fatal("a centered scorer accepted an uncentered blob")
	}

	// shapes buildable only at the format level: a layout no RQ1 encoder
	// writes, and an RQ1 blob with no Step section, which every estimate needs
	perToken, err := codeLen(EncodingRQ1, dims)
	if err != nil {
		t.Fatalf("codeLen: %v", err)
	}
	unassignedLayout, err := build(Header{
		Encoding: EncodingRQ1, Layout: Layout(99), ScalarKind: ScalarFloat16,
		Dims: dims, Tokens: 1,
	}, []float32{1}, make([]byte, perToken))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	assertRejected(t, unassignedLayout, "an RQ1 blob in an unassigned layout")

	noScalars, err := build(Header{
		Encoding: EncodingRQ1, Layout: LayoutTokenMajor, ScalarKind: ScalarNone,
		Dims: dims, Tokens: 1,
	}, nil, make([]byte, perToken))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	assertRejected(t, noScalars, "an RQ1 blob with no scalar section")
}

// TestNewRQ1ScorerRejectsBadQueries checks that the scorer refuses a query
// whose tokens do not all have the parameters' dimensionality.
func TestNewRQ1ScorerRejectsBadQueries(t *testing.T) {
	params, err := NewRQ1Params(4, 42, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	tests := []struct {
		name  string
		query [][]float32
	}{
		{"query token shorter than dims", [][]float32{{1, 2}}},
		{"query token longer than dims", [][]float32{{1, 2, 3, 4, 5}}},
		{"ragged query", [][]float32{{1, 2, 3, 4}, {1, 2, 3}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRQ1Scorer(tt.query, params); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// TestGoldenRQ1 checks the committed v1 RQ1 blobs under testdata/ and requires
// every later version to keep producing and reading them byte for byte. Beyond
// the format, these fixtures pin the rotation: an RQ1 blob is scorable only
// under the same seed and the same rotation implementation, so a change to
// compression.FastRotation that alters its output invalidates every stored RQ1
// code. This test failing on an untouched packed package means that risk
// materialized; regenerating the fixtures would hide it.
func TestGoldenRQ1(t *testing.T) {
	const dims = 128
	const seed = 42
	goldenTokens := fixedTokens(3, dims) // token 1 is all zeros

	uncentered, err := NewRQ1Params(dims, seed, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	mean := make([]float32, dims)
	for j := range mean {
		mean[j] = float32(j%7-3) / 100
	}
	centered, err := NewRQ1Params(dims, seed, mean, 7, 1)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}

	blobU, err := EncodeRQ1(goldenTokens, uncentered)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	blobC, err := EncodeRQ1(goldenTokens, centered)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}

	tests := []struct {
		file   string
		blob   []byte
		header Header
	}{
		{
			file: "v1_rq1.blob",
			blob: blobU,
			header: Header{
				Version: 1, Encoding: EncodingRQ1, Layout: LayoutTokenMajor,
				ScalarKind: ScalarFloat16, Dims: dims, Tokens: 3,
			},
		},
		{
			file: "v1_rq1_centered.blob",
			blob: blobC,
			header: Header{
				Version: 1, Encoding: EncodingRQ1, Layout: LayoutTokenMajor,
				ScalarKind: ScalarFloat16, Dims: dims, Tokens: 3,
				QuantizerID: 7, QuantizerVersion: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			parsed := goldenBlob(t, tt.file, tt.blob, tt.header)

			// the zero token's Step tells the variants apart: exactly 0
			// uncentered, non-zero centered where the token encodes -mu
			if tt.header.QuantizerID == 0 {
				if got := parsed.Scalar(1); got != 0 {
					t.Fatalf("uncentered zero token's Step = %v, want exactly 0", got)
				}
			} else if parsed.Scalar(1) == 0 {
				t.Fatal("centered zero token's Step is 0, want the Step of -mu")
			}
		})
	}
}
