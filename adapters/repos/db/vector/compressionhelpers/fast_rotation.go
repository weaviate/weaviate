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

type FastRotation struct {
	outputDim int      // The dimension of the output returned by Rotate().
	rounds    int      // The number of rounds of random signs, swaps, and blocked transforms that the Rotate() function is going to apply.
	swaps     [][]Swap // Random swaps to apply each round prior to transforming.
	signs     [][]int8 // Random signs to apply each round prior to transforming.
}

func randomSignsInt8(dim int, rng *rand.Rand) []int8 {
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

type Swap struct {
	i, j uint16
}

// Returns a slice of n/2 random swaps such that every element in a slice of length n gets swapped exactly once.
// Consider performing the swaps in sorted order to make the access pattern more sequential.
func randomSwaps(n int, rng *rand.Rand) []Swap {
	swaps := make([]Swap, n/2)
	p := rng.Perm(n)
	for s := range swaps {
		swaps[s] = Swap{i: uint16(p[2*s]), j: uint16(p[2*s+1])}
	}
	return swaps
}

func NewFastRotation(inputDim int, rounds int, seed uint64) *FastRotation {
	outputDim := 64
	for outputDim < inputDim {
		outputDim += 64
	}
	rng := rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
	swaps := make([][]Swap, rounds)
	signs := make([][]int8, rounds)
	for i := range rounds {
		swaps[i] = randomSwaps(outputDim, rng)
		signs[i] = randomSignsInt8(outputDim, rng)
	}
	return &FastRotation{
		outputDim: outputDim,
		rounds:    rounds,
		swaps:     swaps,
		signs:     signs,
	}
}

func (r *FastRotation) OutputDimension() int {
	return r.outputDim
}

func (r *FastRotation) RotateInPlaceFloat64(x []float64) []float64 {
	for i := range r.rounds {
		// Apply random swaps and signs.
		for _, s := range r.swaps[i] {
			x[s.i], x[s.j] = float64(r.signs[i][s.i])*x[s.j], float64(r.signs[i][s.j])*x[s.i]
		}
		// Greedily apply the largest possible FWHT of length 2^k >= 64 to the
		// remaining untransformed portion of the vector.
		pos := 0
		for pos < r.outputDim {
			length := 64
			normalize := 0.125
			for pos+2*length <= r.outputDim {
				length *= 2
				normalize *= 1.0 / math.Sqrt2
			}
			FastWalshHadamardTransform(x[pos : pos+length])
			for j := range length {
				x[pos+j] *= normalize
			}
			pos += length
		}
	}
	return x
}

func (r *FastRotation) RotateFloat64(x []float64) []float64 {
	xCopy := make([]float64, r.outputDim)
	copy(xCopy, x)
	return r.RotateInPlaceFloat64(xCopy)
}

func (r *FastRotation) rotateFloat32UsingFloat64(x []float32) []float32 {
	xFloat64 := make([]float64, r.outputDim)
	for i := range x {
		xFloat64[i] = float64(x[i])
	}
	r.RotateInPlaceFloat64(xFloat64)
	res := make([]float32, r.outputDim)
	for i := range xFloat64 {
		res[i] = float32(xFloat64[i])
	}
	return res
}

func (r *FastRotation) Rotate(x []float32) []float32 {
	return r.rotateFloat32UsingFloat64(x)
}

func FastWalshHadamardTransform(x []float64) {
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
	FastWalshHadamardTransform(x[:m])
	FastWalshHadamardTransform(x[m:])
	for i := range m {
		x[i], x[m+i] = x[i]+x[m+i], x[i]-x[m+i]
	}
}
