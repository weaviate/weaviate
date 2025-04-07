//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package multivector

import (
	"testing"
)

func TestSimHashTest(t *testing.T) {
	// Create a default config
	config := DefaultMuveraConfig()
	encoder := NewMuveraEncoder(config)

	// Test case 1: Similar vectors should produce similar hashes
	vec1 := make([]float32, encoder.config.Dimensions)
	vec2 := make([]float32, encoder.config.Dimensions)
	vec3 := make([]float32, encoder.config.Dimensions)
	for i := 0; i < encoder.config.Dimensions; i++ {
		vec1[i] = 1.0
		vec2[i] = 0.9  // Slightly different but similar vector
		vec3[i] = -1.0 // Opposite direction
	}
	zeroVec := make([]float32, encoder.config.Dimensions)

	for i := 0; i < encoder.config.Repetitions; i++ {
		hash1 := encoder.simHash(vec1, encoder.gaussians[i])
		hash2 := encoder.simHash(vec2, encoder.gaussians[i])

		// Calculate Hamming distance between hashes
		hammingDist := hammingDistance(hash1, hash2)
		if hammingDist > config.KSim/2 {
			t.Errorf("Similar vectors produced very different hashes. Hamming distance: %d", hammingDist)
		}

		// Test case 2: Orthogonal vectors should produce different hashes
		hash3 := encoder.simHash(vec3, encoder.gaussians[i])
		hammingDist = hammingDistance(hash1, hash3)
		if hammingDist < config.KSim/2 {
			t.Errorf("Orthogonal vectors produced similar hashes. Hamming distance: %d", hammingDist)
		}

		// Test case 3: Zero vector should produce consistent hash
		hashZero := encoder.simHash(zeroVec, encoder.gaussians[i])
		if hashZero != 0 {
			t.Errorf("Zero vector produced non-zero hash: %d", hashZero)
		}

		// Test case 4: Same vector should produce same hash
		hash1Rep1 := encoder.simHash(vec1, encoder.gaussians[i])
		hash1Rep2 := encoder.simHash(vec1, encoder.gaussians[i])
		hammingDist = hammingDistance(hash1Rep1, hash1Rep2)
		if hammingDist > 0 {
			t.Error("Same vector produced different hashes")
		}

		maxHash := uint64(1<<uint(config.KSim)) - 1
		if hash1 > maxHash {
			t.Errorf("Hash value %d exceeds maximum possible value %d", hash1, maxHash)
		}
	}
}
