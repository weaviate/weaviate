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
	"math/rand/v2"
	"testing"

	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func randomTokens(rng *rand.Rand, numTokens, dims int) [][]float32 {
	tokens := make([][]float32, numTokens)
	for i := range tokens {
		tokens[i] = make([]float32, dims)
		for j := range tokens[i] {
			tokens[i][j] = float32(rng.NormFloat64())
		}
	}
	return tokens
}

func defaultTestEncoder(dims int) *MuveraEncoder {
	encoder := NewMuveraEncoder(ent.MuveraConfig{
		KSim:         ent.DefaultMultivectorKSim,
		DProjections: ent.DefaultMultivectorDProjections,
		Repetitions:  ent.DefaultMultivectorRepetitions,
	}, nil)
	encoder.InitEncoder(dims)
	return encoder
}

// ColBERT-style shapes: 128-d tokens, ~100 tokens per doc, 32 per query.
func BenchmarkMuveraEncodeDoc(b *testing.B) {
	rng := rand.New(rand.NewPCG(42, 43))
	encoder := defaultTestEncoder(128)
	doc := randomTokens(rng, 100, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encoder.EncodeDoc(doc)
	}
}

func BenchmarkMuveraEncodeQuery(b *testing.B) {
	rng := rand.New(rand.NewPCG(42, 43))
	encoder := defaultTestEncoder(128)
	query := randomTokens(rng, 32, 128)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = encoder.EncodeQuery(query)
	}
}
