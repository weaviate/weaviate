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

package compressionhelpers

import (
	"math"
	"math/rand/v2"
)

type Rotator interface {
	Rotate([]float32) []float32
}

type FastRotation struct {
	outputDim    int
	rounds       int
	permutations [][]int
	signs        [][]float32
	tmp          []float32
}

func randomSigns(dim int, rng *rand.Rand) []float32 {
	signs := make([]float32, dim)
	for i := range signs {
		if rng.Float64() < 0.5 {
			signs[i] = -1.0
		} else {
			signs[i] = 1.0
		}
	}
	return signs
}

// These rotations only produce good pseudorandom results for dimensions that
// are relatively large, for example greater than 32 or 64 as it relies on
// central limit theorem properties of summation.
func NewFastRotation(inputDim int, rounds int, seed uint64) *FastRotation {
	outputDim := 64
	for outputDim < inputDim {
		outputDim *= 2
	}
	rng := rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
	permutations := make([][]int, rounds)
	signs := make([][]float32, rounds)
	tmp := make([]float32, outputDim)
	for i := range rounds {
		permutations[i] = rng.Perm(outputDim)
		signs[i] = randomSigns(outputDim, rng)
	}
	return &FastRotation{
		outputDim:    outputDim,
		rounds:       rounds,
		permutations: permutations,
		signs:        signs,
		tmp:          tmp,
	}
}

func (fr *FastRotation) OutputDimension() int {
	return fr.outputDim
}

// TODO(tobias): a) Benchmark error/speed tradeoff if we switch to float64. b)
// Use tricks with overlapping transforms to preserve input dimensionality. This
// may requre applying different signs for each overlapping transform in order
// to avoid a partial inverse transform (we experienced problems with vectors of
// lengths just slightly greater than a power of two.
func (fr *FastRotation) Rotate(x []float32) []float32 {
	// Apply random signs, permute, transform, normalize.
	y := make([]float32, fr.outputDim)
	copy(y, x)
	for i := range fr.rounds {
		for j := range fr.outputDim {
			p := fr.permutations[i][j]
			fr.tmp[j] = fr.signs[i][j] * y[p]
		}
		FastWalshHadamardTransform(fr.tmp)
		normalize := float32(1.0 / math.Sqrt(float64(fr.outputDim)))
		for j := range fr.outputDim {
			y[j] = normalize * fr.tmp[j]
		}
	}
	return y
}

// What does the inverse transform look like?
func FastWalshHadamardTransform(x []float32) {
	if len(x) == 1 {
		return
	}
	m := len(x) / 2
	FastWalshHadamardTransform(x[:m])
	FastWalshHadamardTransform(x[m:])
	for i := range m {
		x[i], x[m+i] = x[i]+x[m+i], x[i]-x[m+i]
	}
}
