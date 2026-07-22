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

package compression

import (
	"math/rand/v2"
	"slices"
	"sync"
)

const (
	DefaultFastRotationSeed = uint64(0x535ab5105169b1df)
)

// Swap represents a pair of indices to swap during fast rotation.
type Swap struct {
	I, J uint16
}

// FastRotation holds the configuration for fast random rotation.
type FastRotation struct {
	OutputDim uint32      // The dimension of the output returned by Rotate().
	Rounds    uint32      // The number of rounds of random signs, swaps, and blocked transforms that the Rotate() function is going to apply.
	Swaps     [][]Swap    // Random swaps to apply each round prior to transforming.
	Signs     [][]float32 // Random signs to apply each round prior to transforming. We store these as float32 values for performance reasons, to avoid casts.

	// derived holds acceleration tables computed by the constructors. It is
	// intentionally behind a pointer so that copying a FastRotation value
	// (e.g. inside RQData) stays cheap and vet-clean despite the contained
	// sync.Pool. A hand-constructed FastRotation literal leaves it nil, in
	// which case Rotate falls back to the original (bit-identical) swap loop.
	derived *fastRotationDerived
}

// fastRotationDerived caches the swap network of each round flattened into a
// signed permutation. Applying out[i] = signs[i] * in[perm[i]] touches memory
// with sequential writes and random reads, which pipelines far better than
// the in-place swap loop (random reads AND writes with loop-carried
// aliasing), at the cost of ping-ponging between two buffers.
type fastRotationDerived struct {
	perm [][]uint16 // perm[r][i]: source index of destination i in round r
	pool sync.Pool  // scratch *[]float32 of length OutputDim for the ping-pong
}

func buildFastRotationDerived(outputDim int, swaps [][]Swap) *fastRotationDerived {
	d := &fastRotationDerived{perm: make([][]uint16, len(swaps))}
	for r, roundSwaps := range swaps {
		p := make([]uint16, outputDim)
		for i := range p {
			p[i] = uint16(i)
		}
		for _, s := range roundSwaps {
			p[s.I] = s.J
			p[s.J] = s.I
		}
		d.perm[r] = p
	}
	d.pool.New = func() any {
		s := make([]float32, outputDim)
		return &s
	}
	return d
}

func randomSigns(dim int, rng *rand.Rand) []float32 {
	signs := make([]float32, dim)
	for i := range signs {
		if rng.Float64() < 0.5 {
			signs[i] = -1
		} else {
			signs[i] = 1
		}
	}
	return signs
}

// Returns a slice of n/2 random swaps such that every element in a slice of length n gets swapped exactly once.
// We order the swaps to make the access pattern more sequential.
func randomSwaps(n int, rng *rand.Rand) []Swap {
	swaps := make([]Swap, n/2)
	p := rng.Perm(n)
	for s := range swaps {
		a := uint16(p[2*s])
		b := uint16(p[2*s+1])
		if a < b {
			swaps[s] = Swap{I: a, J: b}
		} else {
			swaps[s] = Swap{I: b, J: a}
		}
	}
	slices.SortFunc(swaps, func(a, b Swap) int {
		if a.I < b.I {
			return -1
		}
		if a.I > b.I {
			return 1
		}
		return 0
	})
	return swaps
}

func NewFastRotation(inputDim int, rounds int, seed uint64) *FastRotation {
	outputDim := 64
	for outputDim < inputDim {
		outputDim += 64
	}
	rng := rand.New(rand.NewPCG(seed, 0x385ab5285169b1ac))
	swaps := make([][]Swap, rounds)
	signs := make([][]float32, rounds)
	for i := range rounds {
		swaps[i] = randomSwaps(outputDim, rng)
		signs[i] = randomSigns(outputDim, rng)
	}
	return &FastRotation{
		OutputDim: uint32(outputDim),
		Rounds:    uint32(rounds),
		Swaps:     swaps,
		Signs:     signs,
		derived:   buildFastRotationDerived(outputDim, swaps),
	}
}

func RestoreFastRotation(outputDim int, rounds int, swaps [][]Swap, signs [][]float32) *FastRotation {
	return &FastRotation{
		OutputDim: uint32(outputDim),
		Rounds:    uint32(rounds),
		Swaps:     swaps,
		Signs:     signs,
		derived:   buildFastRotationDerived(outputDim, swaps),
	}
}

func (r *FastRotation) Rotate(x []float32) []float32 {
	return r.RotateInto(x, make([]float32, r.OutputDim))
}

// RotateInto is Rotate with a caller-provided output buffer, for hot paths
// that pool their allocations. out must have at least OutputDim elements and
// is fully overwritten; the result is returned as out[:OutputDim]. out must
// not alias x.
func (r *FastRotation) RotateInto(x []float32, out []float32) []float32 {
	out = out[:r.OutputDim]
	if r.derived == nil {
		return r.rotateSwapLoop(x, out)
	}

	sp := r.derived.pool.Get().(*[]float32)
	scratch := *sp
	// The signed permutation reads one buffer and writes the other, so each
	// round flips between out and scratch. Pick the starting buffer such
	// that the final round always writes out.
	a, b := scratch, out
	if r.Rounds%2 == 0 {
		a, b = out, scratch
	}
	n := copy(a, x)
	clear(a[n:])
	for i := range r.Rounds {
		perm := r.derived.perm[i]
		signs := r.Signs[i][:len(perm)]
		dst := b[:len(perm)]
		for j, p := range perm {
			dst[j] = signs[j] * a[p]
		}
		fwhtBlocks(b)
		a, b = b, a
	}
	r.derived.pool.Put(sp)
	return a
}

// rotateSwapLoop is the fallback for hand-constructed FastRotation values
// without derived tables. It is bit-identical to the permutation path: every
// element takes part in exactly one swap per round, so the in-place swap
// loop is the same signed permutation.
func (r *FastRotation) rotateSwapLoop(x []float32, rx []float32) []float32 {
	n := copy(rx, x)
	clear(rx[n:])
	for i := range r.Rounds {
		// Apply random swaps and signs.
		for _, s := range r.Swaps[i] {
			rx[s.I], rx[s.J] = r.Signs[i][s.I]*rx[s.J], r.Signs[i][s.J]*rx[s.I]
		}
		fwhtBlocks(rx)
	}
	return rx
}

// fwhtBlocks transforms x in blocks (of length 256 if possible, otherwise
// length 64).
func fwhtBlocks(x []float32) {
	pos := 0
	for pos < len(x) {
		if len(x)-pos >= 256 {
			FastWalshHadamardTransform256(x[pos:(pos + 256)])
			pos += 256
			continue
		}
		FastWalshHadamardTransform64(x[pos:(pos + 64)])
		pos += 64
	}
}

func (r *FastRotation) UnRotate(rx []float32) []float32 {
	x := make([]float32, len(rx))
	copy(x, rx)

	return r.UnRotateInPlace(x)
}

func (r *FastRotation) UnRotateInPlace(x []float32) []float32 {
	// Apply rounds in REVERSE order
	for i := int(r.Rounds) - 1; i >= 0; i-- {
		// First: Inverse Walsh-Hadamard transforms (FWHT is self-inverse)
		fwhtBlocks(x)

		// Then: Inverse swaps with signs (apply in REVERSE order)
		for j := len(r.Swaps[i]) - 1; j >= 0; j-- {
			s := r.Swaps[i][j]
			// Inverse of: rx[s.I], rx[s.J] = Signs[s.I]*rx[s.J], Signs[s.J]*rx[s.I]
			// Is: rx[s.I], rx[s.J] = Signs[s.J]*rx[s.J], Signs[s.I]*rx[s.I]
			x[s.I], x[s.J] = r.Signs[i][s.J]*x[s.J], r.Signs[i][s.I]*x[s.I]
		}
	}

	return x[:r.OutputDim] // Return only the original dimensions
}

func fastWalshHadamardTransform16(x []float32, normalize float32) {
	x0 := normalize * x[0]
	x1 := normalize * x[1]
	x2 := normalize * x[2]
	x3 := normalize * x[3]
	x4 := normalize * x[4]
	x5 := normalize * x[5]
	x6 := normalize * x[6]
	x7 := normalize * x[7]
	x8 := normalize * x[8]
	x9 := normalize * x[9]
	x10 := normalize * x[10]
	x11 := normalize * x[11]
	x12 := normalize * x[12]
	x13 := normalize * x[13]
	x14 := normalize * x[14]
	x15 := normalize * x[15]

	x0, x1 = x0+x1, x0-x1
	x2, x3 = x2+x3, x2-x3

	x0, x2 = x0+x2, x0-x2
	x1, x3 = x1+x3, x1-x3

	x4, x5 = x4+x5, x4-x5
	x6, x7 = x6+x7, x6-x7

	x4, x6 = x4+x6, x4-x6
	x5, x7 = x5+x7, x5-x7

	x0, x4 = x0+x4, x0-x4
	x1, x5 = x1+x5, x1-x5
	x2, x6 = x2+x6, x2-x6
	x3, x7 = x3+x7, x3-x7

	x8, x9 = x8+x9, x8-x9
	x10, x11 = x10+x11, x10-x11

	x8, x10 = x8+x10, x8-x10
	x9, x11 = x9+x11, x9-x11

	x12, x13 = x12+x13, x12-x13
	x14, x15 = x14+x15, x14-x15

	x12, x14 = x12+x14, x12-x14
	x13, x15 = x13+x15, x13-x15

	x8, x12 = x8+x12, x8-x12
	x9, x13 = x9+x13, x9-x13
	x10, x14 = x10+x14, x10-x14
	x11, x15 = x11+x15, x11-x15

	x0, x8 = x0+x8, x0-x8
	x1, x9 = x1+x9, x1-x9
	x2, x10 = x2+x10, x2-x10
	x3, x11 = x3+x11, x3-x11
	x4, x12 = x4+x12, x4-x12
	x5, x13 = x5+x13, x5-x13
	x6, x14 = x6+x14, x6-x14
	x7, x15 = x7+x15, x7-x15

	x[0] = x0
	x[1] = x1
	x[2] = x2
	x[3] = x3
	x[4] = x4
	x[5] = x5
	x[6] = x6
	x[7] = x7
	x[8] = x8
	x[9] = x9
	x[10] = x10
	x[11] = x11
	x[12] = x12
	x[13] = x13
	x[14] = x14
	x[15] = x15
}

// fastWalshHadamardTransform64Go performs a fast Walsh-Hadamard transform on a 64-element vector.
// This explicit instantiation is about 10% faster than a generic recursion.
func fastWalshHadamardTransform64Go(x []float32) {
	const normalize = 0.125
	fastWalshHadamardTransform16(x[:16], normalize)
	fastWalshHadamardTransform16(x[16:32], normalize)
	for i := range 16 {
		x[i], x[16+i] = x[i]+x[16+i], x[i]-x[16+i]
	}

	fastWalshHadamardTransform16(x[32:48], normalize)
	fastWalshHadamardTransform16(x[48:], normalize)
	for i := 32; i < 48; i++ {
		x[i], x[16+i] = x[i]+x[16+i], x[i]-x[16+i]
	}

	for i := range 32 {
		x[i], x[32+i] = x[i]+x[32+i], x[i]-x[32+i]
	}
}

func block64FWHT256(x []float32) {
	const normalize = 0.0625
	fastWalshHadamardTransform16(x[0:16], normalize)
	fastWalshHadamardTransform16(x[16:32], normalize)
	for i := range 16 {
		x[i], x[16+i] = x[i]+x[16+i], x[i]-x[16+i]
	}

	fastWalshHadamardTransform16(x[32:48], normalize)
	fastWalshHadamardTransform16(x[48:64], normalize)
	for i := 32; i < 48; i++ {
		x[i], x[16+i] = x[i]+x[16+i], x[i]-x[16+i]
	}

	for i := range 32 {
		x[i], x[32+i] = x[i]+x[32+i], x[i]-x[32+i]
	}
}

func fastWalshHadamardTransform256Go(x []float32) {
	block64FWHT256(x[0:64])
	block64FWHT256(x[64:128])
	for i := range 64 {
		x[i], x[64+i] = x[i]+x[64+i], x[i]-x[64+i]
	}

	block64FWHT256(x[128:192])
	block64FWHT256(x[192:256])
	for i := 128; i < 192; i++ {
		x[i], x[64+i] = x[i]+x[64+i], x[i]-x[64+i]
	}

	for i := range 128 {
		x[i], x[128+i] = x[i]+x[128+i], x[i]-x[128+i]
	}
}
