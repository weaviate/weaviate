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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// eps32 is the unit roundoff of binary32: the largest relative error a single
// rounded operation can introduce.
const eps32 = 1.0 / float64(int64(1)<<23)

// referenceProduction returns the MaxSim distance of query against doc the way
// the shipped rescoring loop computeScoreWithView (hnsw/search.go) computes
// it: one distancer per query token, the minimum distance over the document's
// tokens, summed over the query. The distances come from
// distancer.NewDotProductProvider(), the same assembly the vector layer uses.
//
// The reference is the shipped loop because agreeing with the answers users
// see today is the property that matters.
func referenceProduction(query, doc [][]float32) (float32, error) {
	provider := distancer.NewDotProductProvider()

	similarity := float32(0.0)
	for _, searchVec := range query {
		maxSim := float32(math.MaxFloat32)
		dist := provider.New(searchVec)
		for _, docVec := range doc {
			d, err := dist.Distance(docVec)
			if err != nil {
				return 0, err
			}
			if d < maxSim {
				maxSim = d
			}
		}
		similarity += maxSim
	}
	return similarity, nil
}

// referenceOrdered is referenceProduction over distancer's pure-Go dot
// product, which sums in index order.
//
// The assembly kernels keep several partial sums and reassociate, so parity
// against referenceProduction is only a bound, and a bound can hide small
// bugs. Float32Scorer sums in index order over the same terms, so against this
// reference it must agree bit for bit.
func referenceOrdered(query, doc [][]float32) float32 {
	similarity := float32(0.0)
	for _, searchVec := range query {
		maxSim := float32(math.MaxFloat32)
		for _, docVec := range doc {
			if d := distancer.DotProductFloatGo(searchVec, docVec); d < maxSim {
				maxSim = d
			}
		}
		similarity += maxSim
	}
	return similarity
}

// reassociationBound returns how far referenceProduction may sit from an
// in-order sum over query and doc (token vectors of dimensionality dims).
//
// A dot product of dims terms computed in any order, with or without fused
// multiply-add, rounds at most dims times on the path of any term, so it sits
// within dims*eps times the sum of the terms' magnitudes of the exact value.
// Two such computations can then differ by twice that, and MaxSim adds one dot
// product per query token. The sum of magnitudes is taken from the data (the
// largest over all pairs), which keeps the bound meaningful at any scale.
func reassociationBound(query, doc [][]float32, dims int) float32 {
	worst := 0.0
	for _, q := range query {
		for _, t := range doc {
			magnitudes := 0.0
			for j := range q {
				magnitudes += math.Abs(float64(q[j]) * float64(t[j]))
			}
			if magnitudes > worst {
				worst = magnitudes
			}
		}
	}
	return float32(float64(len(query)) * 2 * float64(dims) * eps32 * worst)
}

// unitTokens returns n random unit-norm tokens of dims coordinates drawn from
// rng, which is what a late-interaction encoder emits. Every eighth token is
// left at zero: real collections hold exact zero tokens, and a token scoring
// <q, 0> = 0 has to be able to win the maximum against negative alternatives.
func unitTokens(rng *rand.Rand, n, dims int) [][]float32 {
	tokens := make([][]float32, n)
	for i := range tokens {
		t := make([]float32, dims)
		if i%8 != 7 {
			norm := 0.0
			for j := range t {
				t[j] = float32(rng.NormFloat64())
				norm += float64(t[j]) * float64(t[j])
			}
			norm = math.Sqrt(norm)
			for j := range t {
				t[j] = float32(float64(t[j]) / norm)
			}
		}
		tokens[i] = t
	}
	return tokens
}

// assertParity encodes doc as float32 (token vectors of dimensionality dims), scores it
// against query with Float32Scorer, and holds the answer to both references:
// bit equality with referenceOrdered, and referenceProduction within
// reassociationBound.
func assertParity(t *testing.T, query, doc [][]float32, dims int) {
	t.Helper()

	blob, err := EncodeFloat32(doc, dims)
	if err != nil {
		t.Fatalf("EncodeFloat32: %v", err)
	}
	parsed, err := Parse(blob)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	scorer, err := NewFloat32Scorer(query, dims)
	if err != nil {
		t.Fatalf("NewFloat32Scorer: %v", err)
	}
	got, err := scorer.Distance(parsed)
	if err != nil {
		t.Fatalf("Distance: %v", err)
	}

	if want := referenceOrdered(query, doc); got != want {
		t.Errorf("packed %v (%#08x), in-order reference %v (%#08x): a passthrough encoding must not change the answer",
			got, math.Float32bits(got), want, math.Float32bits(want))
	}

	want, err := referenceProduction(query, doc)
	if err != nil {
		t.Fatalf("referenceProduction: %v", err)
	}
	// equality first, so the empty-document case (both sides overflow to
	// +Inf) is a match and not an Inf-minus-Inf NaN
	if got != want {
		if diff := float32(math.Abs(float64(got - want))); diff > reassociationBound(query, doc, dims) {
			t.Errorf("packed %v, production reference %v: differ by %v, beyond the %v reassociation can explain",
				got, want, diff, reassociationBound(query, doc, dims))
		}
	}
}

// TestFloat32ScorerParity checks that over a passthrough encoding the scorer
// reproduces exact float32 MaxSim, so that with RQ1 any disagreement is
// quantization error and nothing else.
func TestFloat32ScorerParity(t *testing.T) {
	// document token counts run from one token to the patch count of a page
	// image, with the typical text document sizes in between; the query is 32
	// tokens, the usual query length
	for _, dims := range []int{128, 256, 1024} {
		for _, n := range []int{1, 32, 124, 180, 1030} {
			t.Run(fmt.Sprintf("d%d_n%d", dims, n), func(t *testing.T) {
				rng := rand.New(rand.NewSource(int64(dims*10000 + n)))
				assertParity(t, unitTokens(rng, 32, dims), unitTokens(rng, n, dims), dims)
			})
		}
	}
}

// TestFloat32ScorerQueryShapes varies the query instead of the document. A
// query with no tokens has no maximum to take and must score zero.
func TestFloat32ScorerQueryShapes(t *testing.T) {
	const dims = 128
	for _, nq := range []int{0, 1, 4, 32} {
		t.Run(fmt.Sprintf("nq%d", nq), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(nq)))
			assertParity(t, unitTokens(rng, nq, dims), unitTokens(rng, 124, dims), dims)
		})
	}
}

// TestFloat32ScorerEdgeCases scores the degenerate documents against both
// references: no tokens, one token, identical tokens, and zero tokens next to
// non-zero ones.
func TestFloat32ScorerEdgeCases(t *testing.T) {
	identical := make([][]float32, 5)
	for i := range identical {
		identical[i] = []float32{1, 2, 3, 4, 5, 6, 7, 8}
	}
	allZero := make([][]float32, 4)
	for i := range allZero {
		allZero[i] = make([]float32, edgeCaseDims)
	}

	tests := []struct {
		name string
		doc  [][]float32
	}{
		{
			// No tokens to maximize over. The reference seeds the maximum with
			// the largest finite distance and never replaces it, so the score
			// saturates and the document sorts last. The scorer must do the
			// same and not return zero, which would sort it first.
			name: "empty document",
			doc:  [][]float32{},
		},
		{"nil document", nil},
		{"single token", [][]float32{{1, 2, 3, 4, 5, 6, 7, 8}}},
		{"all identical", identical},
		{
			// every token scores 0, so the maximum is 0 for every query token
			// whatever the query is
			name: "all zero",
			doc:  allZero,
		},
		{
			name: "mixed zero and non-zero",
			doc: [][]float32{
				{1, 2, 3, 4, 5, 6, 7, 8},
				{0, 0, 0, 0, 0, 0, 0, 0},
				{-1, -2, -3, -4, -5, -6, -7, -8},
				{0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
		{
			// A query token that scores negative against every non-zero
			// token, so the zero token wins the maximum. This happens in real
			// collections, which is why zero tokens are stored and not
			// stripped.
			name: "zero token wins the maximum",
			doc: [][]float32{
				{1, 1, 1, 1, 1, 1, 1, 1},
				{0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
	}

	rng := rand.New(rand.NewSource(7))
	query := unitTokens(rng, 32, edgeCaseDims)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertParity(t, query, tt.doc, edgeCaseDims)
		})
	}
}

// TestFloat32ScorerTailOnly covers documents shorter than a 16-token block.
// Float32Scorer has no block path, so these are ordinary shapes for it; they
// are here so the float32 answer is pinned at the shapes the block layout
// scorers treat as a tail only.
func TestFloat32ScorerTailOnly(t *testing.T) {
	const dims = 128
	for _, n := range []int{1, 2, 3, 5, 7, 15} {
		t.Run(fmt.Sprintf("n%d", n), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(n)))
			assertParity(t, unitTokens(rng, 32, dims), unitTokens(rng, n, dims), dims)
		})
	}
}

// TestNewFloat32ScorerRejectsBadQueries checks that the constructor refuses a
// dimension count that cannot be written to the header and a query whose
// tokens do not all have that many coordinates.
func TestNewFloat32ScorerRejectsBadQueries(t *testing.T) {
	tests := []struct {
		name  string
		query [][]float32
		dims  int
	}{
		{"zero dimensions", [][]float32{{}}, 0},
		{"negative dimensions", [][]float32{{1}}, -1},
		{"dimensions beyond uint16", [][]float32{}, math.MaxUint16 + 1},
		{"query token shorter than dims", [][]float32{{1, 2}}, 4},
		{"query token longer than dims", [][]float32{{1, 2, 3, 4, 5}}, 4},
		{"ragged query", [][]float32{{1, 2, 3, 4}, {1, 2, 3}}, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewFloat32Scorer(tt.query, tt.dims); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// TestFloat32ScorerRejectsMismatchedBlobs covers what the header is for: a
// blob that parses but is not one this scorer was prepared for must fail
// instead of being scored anyway.
func TestFloat32ScorerRejectsMismatchedBlobs(t *testing.T) {
	const dims = 4

	scorer, err := NewFloat32Scorer([][]float32{{1, 2, 3, 4}}, dims)
	if err != nil {
		t.Fatalf("NewFloat32Scorer: %v", err)
	}

	tests := []struct {
		name   string
		header Header
	}{
		{
			// Parse does not validate the layout (it does not move the
			// sections), so the scorer must reject a layout it cannot read
			name:   "unassigned layout",
			header: Header{Encoding: EncodingFloat32, Layout: Layout(99), Dims: dims, Tokens: 1},
		},
		{
			name:   "wrong dimensions",
			header: Header{Encoding: EncodingFloat32, Layout: LayoutTokenMajor, Dims: dims * 2, Tokens: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codes := make([]byte, int(tt.header.Tokens)*4*int(tt.header.Dims))
			blob, err := build(tt.header, nil, codes)
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			parsed, err := Parse(blob)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if _, err := scorer.Distance(parsed); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}

	// a blob written by another encoding (a real RQ1 blob, not a doctored
	// header)
	params, err := NewRQ1Params(dims, 1, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	rq1, err := EncodeRQ1([][]float32{{1, 2, 3, 4}}, params)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	parsed, err := Parse(rq1)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := scorer.Distance(parsed); err == nil {
		t.Fatal("Distance accepted an RQ1 blob")
	}

	// a float32 blob with a scalar section: buildable at the format level
	// (the golden fixture is one) but never written by the encoder. The
	// scorer must refuse it instead of ignoring data that, in every encoding
	// that has it, changes the score.
	withScalars, err := build(Header{
		Encoding: EncodingFloat32, Layout: LayoutTokenMajor, ScalarKind: ScalarFloat16,
		Dims: dims, Tokens: 1,
	}, []float32{1}, make([]byte, 4*dims))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	parsed, err = Parse(withScalars)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := scorer.Distance(parsed); err == nil {
		t.Fatal("Distance accepted a float32 blob with a scalar section")
	}
}
