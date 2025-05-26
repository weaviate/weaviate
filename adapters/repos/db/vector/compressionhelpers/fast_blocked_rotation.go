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
	"math/rand/v2"
)

type FastBlock64Rotation struct {
	outputDim    int
	rounds       int
	permutations [][]int32
	signs        [][]int8
	tmp          []float64
}

func RandomSignsInt8(dim int, rng *rand.Rand) []int8 {
	signs := make([]int8, dim)
	for i := range signs {
		if rng.Float64() < 0.5 {
			signs[i] = -1
		} else {
			signs[i] = 1
		}
	}
	return signs
}

func RandomPermutationInt32(n int, rng *rand.Rand) []int32 {
	p := rng.Perm(n)
	p32 := make([]int32, n)
	for j := range p {
		p32[j] = int32(p[j])
	}
	return p32
}

func (r *FastBlock64Rotation) OutputDimension() int {
	return r.outputDim
}

// Greedily rotate using largest possible powers of two.
func NewFastBlock64Rotation(inputDim int, rounds int, seed uint64) *FastBlock64Rotation {
	outputDim := 64
	for outputDim < inputDim {
		outputDim += 64
	}
	rng := rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
	tmp := make([]float64, outputDim)
	permutations := make([][]int32, rounds)
	signs := make([][]int8, rounds)
	for i := range rounds {
		permutations[i] = RandomPermutationInt32(outputDim, rng)
		signs[i] = RandomSignsInt8(outputDim, rng)
	}
	return &FastBlock64Rotation{
		outputDim:    outputDim,
		rounds:       rounds,
		permutations: permutations,
		signs:        signs,
		tmp:          tmp,
	}
}

// Permute x in place according to p and apply signs according to s.
func PermuteAndApplySigns(x []float64, p []int32, s []int8) {
	// We set p[i] to -p[i]-1 to indicate that p[i] has been applied.
	for i := range p {
		from := int32(i)
		tmp := x[from]
		for !(p[from] < 0) {
			to := p[from]
			tmp, x[to] = x[to], float64(s[to])*tmp
			p[from] = -p[from] - 1
			from = to
		}
	}

	// Reset the permutation.
	for i := range p {
		p[i] = -p[i] - 1
	}
}

func (r *FastBlock64Rotation) Rotate(x []float32) []float32 {
	// Apply random signs, permute, transform, normalize.
	for i := range x {
		r.tmp[i] = float64(x[i])
	}
	for i := range r.rounds {
		PermuteAndApplySigns(r.tmp, r.permutations[i], r.signs[i])

		// Greedily apply the largest possible FWHT of length 2^2k >= 64 to the
		// remaining untransformed portion of the vector. We restrict ourselves
		// to lengths of the form 2^2k because the normalization factors
		// 1/SQRT(length) are negative powers of two which should keep floating
		// point errors to a minimum, but this effect might be negligible.
		pos := 0
		for pos < r.outputDim {
			length := 64
			normalize := 1.0 / 8.0
			for pos+4*length <= r.outputDim {
				length *= 4
				normalize *= 0.5
			}
			FastWalshHadamardTransform64(r.tmp[pos : pos+length])
			for j := range length {
				r.tmp[pos+j] *= normalize
			}
			pos += length
		}
	}
	y := make([]float32, r.outputDim)
	for i := range r.tmp {
		y[i] = float32(r.tmp[i])
		r.tmp[i] = 0 // Clear for next Rotation.
	}
	return y
}

func FastWalshHadamardTransform64(x []float64) {
	// Unrolling the recursion at d = 4 gives an almost 2x speedup compared to
	// no unrolling. Unrolling to d = 8 only gave a further ~1.1x speedup.
	// Consider an iterative implementation if we want to optimize further.
	if len(x) == 8 {
		// FWHT(x[0:2])
		x[0], x[1] = x[0]+x[1], x[0]-x[1]
		// FWHT(x[2:4])
		x[2], x[3] = x[2]+x[3], x[2]-x[3]

		// FWHT(x[0:4]), merging step
		x[0], x[2] = x[0]+x[2], x[0]-x[2]
		x[1], x[3] = x[1]+x[3], x[1]-x[3]

		// FWHT(x[4:6])
		x[4], x[5] = x[4]+x[5], x[4]-x[5]
		// FWHT(x[6:8])
		x[6], x[7] = x[6]+x[7], x[6]-x[7]

		// FWHT(x[4:8]), merging step
		x[4], x[6] = x[4]+x[6], x[4]-x[6]
		x[5], x[7] = x[5]+x[7], x[5]-x[7]

		// FWHT(x[0:8]), merging step
		x[0], x[4] = x[0]+x[4], x[0]-x[4]
		x[1], x[5] = x[1]+x[5], x[1]-x[5]
		x[2], x[6] = x[2]+x[6], x[2]-x[6]
		x[3], x[7] = x[3]+x[7], x[3]-x[7]
		return
	}
	m := len(x) / 2
	FastWalshHadamardTransform64(x[:m])
	FastWalshHadamardTransform64(x[m:])
	for i := range m {
		x[i], x[m+i] = x[i]+x[m+i], x[i]-x[m+i]
	}
}
