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
	"encoding/binary"
	"fmt"
	"math"
)

// EncodeFloat32 packs tokens as raw float32 and returns the blob. It is the
// test control described at EncodingFloat32: a passthrough that lets the
// kernel tests separate format and kernel bugs from quantization error.
//
// Arguments:
//   - tokens: one slice per token, each exactly dims long.
//   - dims: dimensionality of each token vector. Passed explicitly because a
//     document with no tokens is legal and carries no dimensionality of its
//     own.
func EncodeFloat32(tokens [][]float32, dims int) ([]byte, error) {
	if err := validateTokens(tokens, dims); err != nil {
		return nil, err
	}

	codes := make([]byte, 0, len(tokens)*dims*4)
	for _, t := range tokens {
		for _, v := range t {
			codes = binary.LittleEndian.AppendUint32(codes, math.Float32bits(v))
		}
	}

	return build(Header{
		Encoding:   EncodingFloat32,
		Layout:     LayoutTokenMajor,
		ScalarKind: ScalarNone,
		Dims:       uint16(dims),
		Tokens:     uint32(len(tokens)),
	}, nil, codes)
}

// DecodeFloat32 restores the tokens of a float32 blob. The tokens share one
// backing array, one allocation per document.
func DecodeFloat32(b Blob) ([][]float32, error) {
	if err := checkFloat32Blob(b); err != nil {
		return nil, err
	}

	dims := int(b.Dims)
	tokens := make([][]float32, b.Tokens)
	flat := make([]float32, int(b.Tokens)*dims)
	for i := range tokens {
		t := flat[i*dims : (i+1)*dims]
		code := b.Code(i)
		for j := range t {
			t[j] = math.Float32frombits(binary.LittleEndian.Uint32(code[4*j:]))
		}
		tokens[i] = t
	}
	return tokens, nil
}

var _ Scorer = (*Float32Scorer)(nil)

// Float32Scorer scores float32 blobs against a fixed query. It is the kernel
// of the test control: the encoding is a passthrough, so its MaxSim must equal
// exact float32 MaxSim up to float rounding, and any disagreement is a bug in
// the header, the section offsets or the kernel.
//
// The kernel is the plain double loop, reading coordinates straight out of the
// blob.
type Float32Scorer struct {
	query [][]float32
	dims  int
}

// NewFloat32Scorer prepares query for scoring float32 blobs. There is nothing
// to precompute; the query is kept as is.
//
// Arguments:
//   - query: one slice per query token, each exactly dims long. It is
//     retained, not copied, and must not be modified while the scorer is in
//     use.
//   - dims: dimensionality of each token vector. Explicit because a query with
//     no tokens is legal, and without it a blob of the wrong width could not
//     be rejected.
func NewFloat32Scorer(query [][]float32, dims int) (*Float32Scorer, error) {
	if err := validateTokens(query, dims); err != nil {
		return nil, fmt.Errorf("packed: query: %w", err)
	}
	return &Float32Scorer{query: query, dims: dims}, nil
}

// checkFloat32Blob rejects a blob that is not one EncodeFloat32 writes:
// another encoding, another layout, or a scalar section. The encoder never
// writes scalars, and in every encoding that has them they change the score,
// so a float32 blob with scalars was written by something else and must not
// be scored. (The golden fixture v1_float32_scalars.blob is such a blob; it is
// parsed, never decoded.)
func checkFloat32Blob(b Blob) error {
	if b.Encoding != EncodingFloat32 {
		return fmt.Errorf("packed: blob uses encoding %d, not float32", uint8(b.Encoding))
	}
	if b.Layout != LayoutTokenMajor {
		return fmt.Errorf("packed: float32 blobs are token-major, got layout %d", uint8(b.Layout))
	}
	if b.ScalarKind != ScalarNone {
		return fmt.Errorf("packed: float32 blobs carry no scalar section, got scalar kind %d", uint8(b.ScalarKind))
	}
	return nil
}

// Distance implements Scorer: the sum over query tokens of the smallest
// negated dot product against any document token.
func (s *Float32Scorer) Distance(b Blob) (float32, error) {
	if err := checkFloat32Blob(b); err != nil {
		return 0, err
	}
	if int(b.Dims) != s.dims {
		return 0, fmt.Errorf("packed: blob has %d dimensions, query has %d", b.Dims, s.dims)
	}

	tokens := int(b.Tokens)
	var sum float32
	for _, q := range s.query {
		// Seeded with the largest finite distance, so a document with no
		// tokens sorts last. Past one query token the sum saturates to +Inf,
		// which keeps the ordering.
		best := float32(math.MaxFloat32)
		for i := range tokens {
			if d := -dotFloat32Code(q, b.Code(i)); d < best {
				best = d
			}
		}
		sum += best
	}
	return sum, nil
}

// dotFloat32Code returns the dot product of q with one token's code.
//
// Arguments:
//   - q: a query token.
//   - code: a token's code, len(q) little-endian float32.
//
// The sum runs in index order, one term at a time, because that is what
// distancer's pure-Go dot product does; the two then agree bit for bit and the
// parity test can demand exact equality. The shipped SIMD kernels keep several
// partial sums and reassociate, so parity against those is a bound.
func dotFloat32Code(q []float32, code []byte) float32 {
	var sum float32
	for j, qj := range q {
		sum += qj * math.Float32frombits(binary.LittleEndian.Uint32(code[4*j:]))
	}
	return sum
}
