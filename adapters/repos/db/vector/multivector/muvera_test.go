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

package multivector

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestMuveraEncodeSparseMultiVectors(t *testing.T) {
	config := ent.MuveraConfig{
		KSim:         3,
		DProjections: 8,
		Repetitions:  5,
	}

	encoder := NewMuveraEncoder(config, nil)
	encoder.InitEncoder(64)

	t.Run("single token multi-vector", func(t *testing.T) {
		singleToken := [][]float32{
			make([]float32, 64),
		}
		for i := range singleToken[0] {
			singleToken[0][i] = float32(i) * 0.1
		}

		docEncoded := encoder.EncodeDoc(singleToken)
		require.NotNil(t, docEncoded)
		assertAllFinite(t, docEncoded, "EncodeDoc single token")

		queryEncoded := encoder.EncodeQuery(singleToken)
		require.NotNil(t, queryEncoded)
		assertAllFinite(t, queryEncoded, "EncodeQuery single token")
	})

	t.Run("two token multi-vector", func(t *testing.T) {
		twoTokens := [][]float32{
			make([]float32, 64),
			make([]float32, 64),
		}
		for i := range twoTokens[0] {
			twoTokens[0][i] = float32(i) * 0.1
			twoTokens[1][i] = float32(64-i) * 0.1
		}

		docEncoded := encoder.EncodeDoc(twoTokens)
		require.NotNil(t, docEncoded)
		assertAllFinite(t, docEncoded, "EncodeDoc two tokens")

		queryEncoded := encoder.EncodeQuery(twoTokens)
		require.NotNil(t, queryEncoded)
		assertAllFinite(t, queryEncoded, "EncodeQuery two tokens")
	})

	t.Run("many tokens filling few clusters", func(t *testing.T) {
		// With KSim=3, we have 8 clusters. Create vectors that will
		// likely hash to the same clusters (similar vectors)
		similarTokens := make([][]float32, 10)
		for i := range similarTokens {
			similarTokens[i] = make([]float32, 64)
			for j := range similarTokens[i] {
				similarTokens[i][j] = 1.0 + float32(i)*0.01
			}
		}

		docEncoded := encoder.EncodeDoc(similarTokens)
		require.NotNil(t, docEncoded)
		assertAllFinite(t, docEncoded, "EncodeDoc similar tokens")

		queryEncoded := encoder.EncodeQuery(similarTokens)
		require.NotNil(t, queryEncoded)
		assertAllFinite(t, queryEncoded, "EncodeQuery similar tokens")
	})

	t.Run("zero vectors", func(t *testing.T) {
		zeroTokens := [][]float32{
			make([]float32, 64),
			make([]float32, 64),
		}

		docEncoded := encoder.EncodeDoc(zeroTokens)
		require.NotNil(t, docEncoded)
		assertAllFinite(t, docEncoded, "EncodeDoc zero tokens")

		queryEncoded := encoder.EncodeQuery(zeroTokens)
		require.NotNil(t, queryEncoded)
		assertAllFinite(t, queryEncoded, "EncodeQuery zero tokens")
	})
}

func assertAllFinite(t *testing.T, vec []float32, context string) {
	t.Helper()
	for i, v := range vec {
		require.False(t, math.IsNaN(float64(v)), "%s: NaN at index %d", context, i)
		require.False(t, math.IsInf(float64(v), 0), "%s: Inf at index %d", context, i)
	}
}
