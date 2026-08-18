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
	"testing"

	"github.com/stretchr/testify/assert"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// MuveraConfig.EncodedDimensions() computes the encoded length independently of the encoder,
// pin that the two cannot drift.
func TestEncodedVectorMatchesConfiguredDimensions(t *testing.T) {
	tests := []struct {
		name       string
		config     ent.MuveraConfig
		dimensions int
		tokens     int
	}{
		{
			name: "defaults",
			config: ent.MuveraConfig{
				KSim:         ent.DefaultMultivectorKSim,
				DProjections: ent.DefaultMultivectorDProjections,
				Repetitions:  ent.DefaultMultivectorRepetitions,
			},
			dimensions: 128,
			tokens:     7,
		},
		{
			name: "single repetition",
			config: ent.MuveraConfig{
				KSim:         3,
				DProjections: 8,
				Repetitions:  1,
			},
			dimensions: 64,
			tokens:     1,
		},
		{
			name: "ksim at the validated upper bound",
			config: ent.MuveraConfig{
				KSim:         10,
				DProjections: 4,
				Repetitions:  2,
			},
			dimensions: 32,
			tokens:     3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoder := NewMuveraEncoder(tt.config, nil)
			encoder.InitEncoder(tt.dimensions)

			tokens := make([][]float32, tt.tokens)
			for i := range tokens {
				tokens[i] = make([]float32, tt.dimensions)
				for j := range tokens[i] {
					tokens[i][j] = float32(i+1) / float32(j+1)
				}
			}

			expected := tt.config.EncodedDimensions()
			assert.Positive(t, expected)
			assert.Len(t, encoder.EncodeDoc(tokens), expected)
			assert.Len(t, encoder.EncodeQuery(tokens), expected)
		})
	}
}
