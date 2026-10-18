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
	"testing"
)

// edgeCaseDims is the dimensionality of the hand-written edge case tokens.
const edgeCaseDims = 8

// assertTokensEqual fails the test unless got and want hold the same bit
// patterns, token by token. It compares bits, so signed zeros and NaNs are
// held to the same standard as everything else: float32 encoding is a
// passthrough and must be exact.
func assertTokensEqual(t *testing.T, got, want [][]float32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decoded %d tokens, want %d", len(got), len(want))
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("token %d has %d dimensions, want %d", i, len(got[i]), len(want[i]))
		}
		for j := range want[i] {
			if math.Float32bits(got[i][j]) != math.Float32bits(want[i][j]) {
				t.Fatalf("token %d coordinate %d = %v (%#08x), want %v (%#08x)",
					i, j, got[i][j], math.Float32bits(got[i][j]),
					want[i][j], math.Float32bits(want[i][j]))
			}
		}
	}
}

// TestEncodeDecodeFloat32 round-trips documents through EncodeFloat32, Parse
// and DecodeFloat32 at three dimensionalities and token counts from none to
// the patch count of a page image.
func TestEncodeDecodeFloat32(t *testing.T) {
	for _, dims := range []int{128, 256, 1024} {
		for _, n := range []int{0, 1, 32, 124, 180, 1030} {
			t.Run(fmt.Sprintf("d%d_n%d", dims, n), func(t *testing.T) {
				roundTripFloat32(t, fixedTokens(n, dims), dims)
			})
		}
	}
}

// TestEncodeDecodeFloat32EdgeCases round-trips the degenerate documents (no
// tokens, one token, identical tokens, zero tokens) and every corner of
// binary32.
func TestEncodeDecodeFloat32EdgeCases(t *testing.T) {
	identical := make([][]float32, 5)
	for i := range identical {
		identical[i] = []float32{1, 2, 3, 4, 5, 6, 7, 8}
	}
	allZero := make([][]float32, 4)
	for i := range allZero {
		allZero[i] = make([]float32, edgeCaseDims)
	}

	tests := []struct {
		name   string
		tokens [][]float32
	}{
		{"empty", [][]float32{}},
		{"nil", nil},
		{"single token", [][]float32{{1, 2, 3, 4, 5, 6, 7, 8}}},
		{"all identical", identical},
		{"all zero", allZero},
		{
			// real collections hold exact zero tokens, and a zero token can
			// win the maximum, so it is encoded like any other token
			name: "mixed zero and non-zero",
			tokens: [][]float32{
				{1, 2, 3, 4, 5, 6, 7, 8},
				{0, 0, 0, 0, 0, 0, 0, 0},
				{-1, -2, -3, -4, -5, -6, -7, -8},
				{0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
		{
			// A passthrough must return every corner of binary32 untouched,
			// including a NaN's payload bits.
			name: "signed zeros, extremes and NaN",
			tokens: [][]float32{{
				0, float32(math.Copysign(0, -1)),
				math.MaxFloat32, -math.MaxFloat32,
				math.SmallestNonzeroFloat32, -math.SmallestNonzeroFloat32,
				float32(math.Inf(1)), math.Float32frombits(0x7fc00123),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			roundTripFloat32(t, tt.tokens, edgeCaseDims)
		})
	}
}

// roundTripFloat32 encodes tokens (token vectors of dimensionality dims),
// parses the blob, decodes it and asserts the result is bit-identical to
// tokens.
func roundTripFloat32(t *testing.T, tokens [][]float32, dims int) {
	t.Helper()

	blob, err := EncodeFloat32(tokens, dims)
	if err != nil {
		t.Fatalf("EncodeFloat32: %v", err)
	}
	parsed, err := Parse(blob)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if int(parsed.Tokens) != len(tokens) || int(parsed.Dims) != dims {
		t.Fatalf("header says %d tokens of %d dimensions, want %d of %d",
			parsed.Tokens, parsed.Dims, len(tokens), dims)
	}
	decoded, err := DecodeFloat32(parsed)
	if err != nil {
		t.Fatalf("DecodeFloat32: %v", err)
	}
	if tokens == nil {
		tokens = [][]float32{}
	}
	assertTokensEqual(t, decoded, tokens)
}

// badShapes are the token sets every validateTokens caller must refuse: a
// dimensionality that cannot be written to the header and tokens that do
// not all have that dimensionality.
var badShapes = []struct {
	name   string
	tokens [][]float32
	dims   int
}{
	{"zero dimensions", [][]float32{{}}, 0},
	{"negative dimensions", [][]float32{{1}}, -1},
	{"dimensions beyond uint16", [][]float32{}, math.MaxUint16 + 1},
	{"token shorter than dims", [][]float32{{1, 2}}, 4},
	{"token longer than dims", [][]float32{{1, 2, 3, 4, 5}}, 4},
	{"ragged tokens", [][]float32{{1, 2, 3, 4}, {1, 2, 3}}, 4},
}

// TestEncodeFloat32RejectsBadShapes checks that the encoder refuses a
// dimensionality that cannot be written to the header and a document whose
// tokens do not all have that dimensionality.
func TestEncodeFloat32RejectsBadShapes(t *testing.T) {
	for _, tt := range badShapes {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := EncodeFloat32(tt.tokens, tt.dims); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// TestDecodeFloat32RejectsMismatchedBlobs checks that the decoder refuses a
// blob that parses but is not one EncodeFloat32 writes: an unassigned layout,
// another encoding, or a scalar section.
func TestDecodeFloat32RejectsMismatchedBlobs(t *testing.T) {
	// Parse does not validate the layout (it does not move the sections), so
	// the decoder must reject a layout it cannot read
	unassignedLayout, err := build(Header{
		Encoding:   EncodingFloat32,
		Layout:     Layout(99), // unassigned; no encoder writes it
		ScalarKind: ScalarNone,
		Dims:       4,
		Tokens:     1,
	}, nil, make([]byte, 16))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	parsed, err := Parse(unassignedLayout)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := DecodeFloat32(parsed); err == nil {
		t.Fatal("DecodeFloat32 accepted a blob with an unassigned layout")
	}

	// nor a blob written by another encoding (a real RQ1 blob, not a doctored
	// header)
	params, err := NewRQ1Params(4, 1, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewRQ1Params: %v", err)
	}
	rq1, err := EncodeRQ1([][]float32{{1, 2, 3, 4}}, params)
	if err != nil {
		t.Fatalf("EncodeRQ1: %v", err)
	}
	parsed, err = Parse(rq1)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := DecodeFloat32(parsed); err == nil {
		t.Fatal("DecodeFloat32 accepted an RQ1 blob")
	}

	// nor a float32 blob with a scalar section: buildable at the format level
	// (the golden fixture is one) but never written by the encoder, and
	// float32 gives the scalars no meaning
	withScalars, err := build(Header{
		Encoding: EncodingFloat32, Layout: LayoutTokenMajor, ScalarKind: ScalarFloat16,
		Dims: 4, Tokens: 1,
	}, []float32{1}, make([]byte, 16))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	parsed, err = Parse(withScalars)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, err := DecodeFloat32(parsed); err == nil {
		t.Fatal("DecodeFloat32 accepted a float32 blob with a scalar section")
	}
}
