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

package hnsw

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMuveraConfigEncodedDimensions(t *testing.T) {
	tests := []struct {
		name     string
		config   MuveraConfig
		expected int
	}{
		{
			name: "defaults",
			config: MuveraConfig{
				KSim:         DefaultMultivectorKSim,
				DProjections: DefaultMultivectorDProjections,
				Repetitions:  DefaultMultivectorRepetitions,
			},
			expected: 10 * 16 * 16, // repetitions × 2^ksim × dprojections
		},
		{
			name: "custom values",
			config: MuveraConfig{
				KSim:         3,
				DProjections: 8,
				Repetitions:  5,
			},
			expected: 5 * 8 * 8,
		},
		{
			name: "ksim at the validated upper bound",
			config: MuveraConfig{
				KSim:         10,
				DProjections: DefaultMultivectorDProjections,
				Repetitions:  DefaultMultivectorRepetitions,
			},
			expected: 10 * 1024 * 16,
		},
		{
			name: "negative ksim does not panic",
			config: MuveraConfig{
				KSim:         -1,
				DProjections: 16,
				Repetitions:  10,
			},
			expected: 0,
		},
		{
			name: "zero repetitions",
			config: MuveraConfig{
				KSim:         DefaultMultivectorKSim,
				DProjections: DefaultMultivectorDProjections,
				Repetitions:  0,
			},
			expected: 0,
		},
		{
			name: "negative repetitions",
			config: MuveraConfig{
				KSim:         DefaultMultivectorKSim,
				DProjections: DefaultMultivectorDProjections,
				Repetitions:  -1,
			},
			expected: 0,
		},
		{
			name: "zero dprojections",
			config: MuveraConfig{
				KSim:         DefaultMultivectorKSim,
				DProjections: 0,
				Repetitions:  DefaultMultivectorRepetitions,
			},
			expected: 0,
		},
		{
			name: "negative dprojections",
			config: MuveraConfig{
				KSim:         DefaultMultivectorKSim,
				DProjections: -1,
				Repetitions:  DefaultMultivectorRepetitions,
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.config.EncodedDimensions())
		})
	}
}
