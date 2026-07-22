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

package compressionhelpers

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// nibbleKernelSizes covers all remainder paths of the SIMD kernels: the
// 32-byte main loop, the 16-byte chunk, the scalar tail, and combinations.
var nibbleKernelSizes = []int{0, 1, 2, 7, 8, 15, 16, 17, 31, 32, 33, 47, 48, 63, 64, 65, 100, 127, 128, 512, 768, 1000}

func randomNibbleKernelInput(half int, rng *rand.Rand) (q, packed []byte) {
	q = make([]byte, 2*half)
	packed = make([]byte, half)
	for i := range q {
		q[i] = byte(rng.UintN(256))
	}
	for i := range packed {
		packed[i] = byte(rng.UintN(256))
	}
	return q, packed
}

// maxNibbleKernelInput saturates every value to verify the kernels have
// enough headroom in their intermediate accumulators (VPMADDUBSW pair sums,
// UADALP halfword products).
func maxNibbleKernelInput(half int) (q, packed []byte) {
	q = make([]byte, 2*half)
	packed = make([]byte, half)
	for i := range q {
		q[i] = 255
	}
	for i := range packed {
		packed[i] = 0xFF
	}
	return q, packed
}

// dotByteNibbleVariants returns the dispatched implementation plus all
// architecture-specific kernels runnable on this machine (defined in the
// arch-specific test files).
func TestDotByteNibbleImplMatchesGo(t *testing.T) {
	variants := dotByteNibbleVariantsUnderTest()
	variants["dispatched"] = dotByteNibbleImpl
	for name, impl := range variants {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, 2))
			for _, half := range nibbleKernelSizes {
				for trial := range 10 {
					q, packed := randomNibbleKernelInput(half, rng)
					want := dotByteNibbleGo(q, packed)
					got := impl(q, packed)
					require.Equal(t, want, got, "half=%d trial=%d", half, trial)
				}
				q, packed := maxNibbleKernelInput(half)
				require.Equal(t, dotByteNibbleGo(q, packed), impl(q, packed),
					"half=%d saturated input", half)
			}
		})
	}
}

func TestDotNibbleNibbleImplMatchesGo(t *testing.T) {
	variants := dotNibbleNibbleVariantsUnderTest()
	variants["dispatched"] = dotNibbleNibbleImpl
	for name, impl := range variants {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(3, 4))
			for _, n := range nibbleKernelSizes {
				for trial := range 10 {
					_, a := randomNibbleKernelInput(n, rng)
					_, b := randomNibbleKernelInput(n, rng)
					want := dotNibbleNibbleGo(a, b)
					got := impl(a, b)
					require.Equal(t, want, got, "n=%d trial=%d", n, trial)
				}
				_, a := maxNibbleKernelInput(n)
				_, b := maxNibbleKernelInput(n)
				require.Equal(t, dotNibbleNibbleGo(a, b), impl(a, b),
					"n=%d saturated input", n)
			}
		})
	}
}

func BenchmarkDotByteNibble(b *testing.B) {
	rng := rand.New(rand.NewPCG(5, 6))
	for _, half := range []int{512, 768} {
		q, packed := randomNibbleKernelInput(half, rng)
		b.Run(fmt.Sprintf("dispatched-d%d", 2*half), func(b *testing.B) {
			for b.Loop() {
				dotByteNibbleImpl(q, packed)
			}
		})
		b.Run(fmt.Sprintf("go-d%d", 2*half), func(b *testing.B) {
			for b.Loop() {
				dotByteNibbleGo(q, packed)
			}
		})
	}
}

func BenchmarkDotNibbleNibble(b *testing.B) {
	rng := rand.New(rand.NewPCG(7, 8))
	for _, half := range []int{512, 768} {
		_, x := randomNibbleKernelInput(half, rng)
		_, y := randomNibbleKernelInput(half, rng)
		b.Run(fmt.Sprintf("dispatched-d%d", 2*half), func(b *testing.B) {
			for b.Loop() {
				dotNibbleNibbleImpl(x, y)
			}
		})
		b.Run(fmt.Sprintf("go-d%d", 2*half), func(b *testing.B) {
			for b.Loop() {
				dotNibbleNibbleGo(x, y)
			}
		})
	}
}
