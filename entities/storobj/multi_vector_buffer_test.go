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

package storobj

import (
	"math/rand"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
)

// randomTokens returns n tokens of dims random coordinates drawn from rng.
func randomTokens(rng *rand.Rand, n, dims int) [][]float32 {
	tokens := make([][]float32, n)
	for i := range tokens {
		tokens[i] = make([]float32, dims)
		for j := range tokens[i] {
			tokens[i][j] = rng.Float32()
		}
	}
	return tokens
}

// multiVectorTestObject marshals an object whose only multi-vector, named
// "mv", holds tokens.
func multiVectorTestObject(t *testing.T, tokens [][]float32) []byte {
	t.Helper()
	return multiVectorsTestObject(t, map[string][][]float32{"mv": tokens})
}

// multiVectorsTestObject marshals an object holding the given multi-vectors,
// keyed by target vector name.
func multiVectorsTestObject(t *testing.T, multiVectors map[string][][]float32) []byte {
	t.Helper()
	obj := New(1)
	obj.Object = models.Object{
		ID:    strfmt.UUID("73f2eb5f-5abf-447a-81ca-74b1dd168247"),
		Class: "MultiVectorBufferClass",
	}
	obj.MultiVectors = multiVectors
	data, err := obj.MarshalBinary()
	require.NoError(t, err)
	return data
}

// TestMultiVectorFromBinaryInto reads documents of different shapes through
// one buffer, in the order that exposes a stale backing array: a short
// document after a long one must not see the long one's tail, and a ragged
// document must decode token by token at each token's own width. The tokens
// that were marshalled are the oracle.
func TestMultiVectorFromBinaryInto(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	docs := []struct {
		name   string
		tokens [][]float32
	}{
		{"130 tokens", randomTokens(rng, 130, 128)},
		{"3 tokens", randomTokens(rng, 3, 128)},
		{"200 tokens", randomTokens(rng, 200, 128)},
		{"1 token", randomTokens(rng, 1, 128)},
		{"ragged, one token empty", [][]float32{{1, 2, 3}, {4, 5}, {}, {6, 7, 8, 9}}},
		{"no tokens", [][]float32{}},
		{"130 tokens again", randomTokens(rng, 130, 128)},
	}

	var buffer []float32
	for _, doc := range docs {
		t.Run(doc.name, func(t *testing.T) {
			data := multiVectorTestObject(t, doc.tokens)

			got, buf, err := MultiVectorFromBinaryInto(data, buffer, "mv")
			require.NoError(t, err)
			require.Equal(t, doc.tokens, got)

			retained, err := MultiVectorFromBinary(data, "mv")
			require.NoError(t, err)
			require.Equal(t, doc.tokens, retained)

			buffer = buf
		})
	}

	// the buffer grew to the largest document and was then reused, never
	// reallocated for a smaller one
	require.Equal(t, 200*128, cap(buffer))

	t.Run("tokens view the buffer", func(t *testing.T) {
		data := multiVectorTestObject(t, [][]float32{{1, 2}, {3, 4}})
		got, buf, err := MultiVectorFromBinaryInto(data, buffer, "mv")
		require.NoError(t, err)
		buf[2] = 42
		require.Equal(t, float32(42), got[1][0])
		// a token's capacity ends where the next token starts, so appending to
		// one cannot overwrite its neighbour
		require.Equal(t, 2, cap(got[0]))
	})

	t.Run("nil buffer allocates", func(t *testing.T) {
		data := multiVectorTestObject(t, [][]float32{{1, 2}, {3, 4}})
		got, buf, err := MultiVectorFromBinaryInto(data, nil, "mv")
		require.NoError(t, err)
		require.Equal(t, [][]float32{{1, 2}, {3, 4}}, got)
		require.Equal(t, 4, cap(buf))
	})

	t.Run("missing target vector", func(t *testing.T) {
		data := multiVectorTestObject(t, [][]float32{{1, 2}})
		_, _, err := MultiVectorFromBinaryInto(data, nil, "other")
		require.ErrorContains(t, err, "vector not found for target vector: other")
	})

	t.Run("two multi-vectors in one object", func(t *testing.T) {
		// one of the two starts at a non-zero offset inside the segment, so
		// this pins the name lookup and the seek to that offset
		a := randomTokens(rng, 5, 8)
		b := randomTokens(rng, 7, 8)
		data := multiVectorsTestObject(t, map[string][][]float32{"a": a, "b": b})
		gotA, buf, err := MultiVectorFromBinaryInto(data, buffer, "a")
		require.NoError(t, err)
		require.Equal(t, a, gotA)
		// reading b reuses the array under gotA, so gotA was checked first
		gotB, _, err := MultiVectorFromBinaryInto(data, buf, "b")
		require.NoError(t, err)
		require.Equal(t, b, gotB)
	})
}

// TestMultiVectorFromBinaryIntoAllocates pins what the buffer is for: once it
// has grown, a read allocates the same whether the document has 13 tokens or
// 130 (nothing per token), and less than MultiVectorFromBinary, which
// allocates a fresh backing array.
func TestMultiVectorFromBinaryIntoAllocates(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	long := multiVectorTestObject(t, randomTokens(rng, 130, 128))
	short := multiVectorTestObject(t, randomTokens(rng, 13, 128))

	_, buffer, err := MultiVectorFromBinaryInto(long, nil, "mv")
	require.NoError(t, err)

	buffered := func(data []byte) float64 {
		return testing.AllocsPerRun(100, func() {
			var err error
			_, buffer, err = MultiVectorFromBinaryInto(data, buffer, "mv")
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	longAllocs := buffered(long)
	shortAllocs := buffered(short)
	retained := testing.AllocsPerRun(100, func() {
		if _, err := MultiVectorFromBinary(long, "mv"); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("allocations per document: buffered %.0f (130 tokens) / %.0f (13 tokens), retained %.0f",
		longAllocs, shortAllocs, retained)

	require.Equal(t, shortAllocs, longAllocs, "the buffered read allocates per token")
	require.Less(t, longAllocs, retained)
}
