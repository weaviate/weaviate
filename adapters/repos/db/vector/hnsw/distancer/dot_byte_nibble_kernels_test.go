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

package distancer

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// byteKernelSizes covers all remainder paths of the SIMD byte/nibble kernels
// (256/128/64/32 byte main loops and step-down tails, 16- and 8-byte chunks,
// K-masked tails, scalar tails) and every small-size branch of the goat
// wrapper switches in asm (which special-case lengths up to 12; length 8 had
// a uint8 truncation bug in L2ByteARM64 once).
var byteKernelSizes = []int{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
	24, 25, 31, 32, 33, 40, 47, 48, 56, 63, 64, 65, 100, 127, 128,
	129, 192, 255, 256, 257, 320, 512, 768, 1000,
}

// dotByteNibbleRef is the pure Go reference for the dot product between an
// unpacked 8-bit code (one byte per dimension) and a packed 4-bit code in
// plane layout (byte j holds dimension j in the low nibble and dimension
// j+D/2 in the high nibble). It mirrors the dispatch fallback in
// compressionhelpers.
func dotByteNibbleRef(q, packed []byte) uint32 {
	var sum uint32
	half := len(packed)
	for i, b := range packed {
		sum += uint32(q[i])*uint32(b&0x0F) + uint32(q[half+i])*uint32(b>>4)
	}
	return sum
}

// dotNibbleNibbleRef is the pure Go reference for the dot product between
// two packed 4-bit codes in plane layout.
func dotNibbleNibbleRef(a, b []byte) uint32 {
	var sum uint32
	for i := range a {
		x, y := a[i], b[i]
		sum += uint32(x&0x0F)*uint32(y&0x0F) + uint32(x>>4)*uint32(y>>4)
	}
	return sum
}

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

// TestDotByteNibbleImplMatchesGo pins all architecture-specific byte-nibble
// kernels runnable on this machine (defined in the arch-specific variant
// files) to the pure Go reference, exact integer equality.
func TestDotByteNibbleImplMatchesGo(t *testing.T) {
	for name, impl := range dotByteNibbleVariantsUnderTest() {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, 2))
			for _, half := range byteKernelSizes {
				for trial := range 10 {
					q, packed := randomNibbleKernelInput(half, rng)
					want := dotByteNibbleRef(q, packed)
					got := impl(q, packed)
					require.Equal(t, want, got, "half=%d trial=%d", half, trial)
				}
				q, packed := maxNibbleKernelInput(half)
				require.Equal(t, dotByteNibbleRef(q, packed), impl(q, packed),
					"half=%d saturated input", half)
			}
		})
	}
}

func TestDotNibbleNibbleImplMatchesGo(t *testing.T) {
	for name, impl := range dotNibbleNibbleVariantsUnderTest() {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(3, 4))
			for _, n := range byteKernelSizes {
				for trial := range 10 {
					_, a := randomNibbleKernelInput(n, rng)
					_, b := randomNibbleKernelInput(n, rng)
					want := dotNibbleNibbleRef(a, b)
					got := impl(a, b)
					require.Equal(t, want, got, "n=%d trial=%d", n, trial)
				}
				_, a := maxNibbleKernelInput(n)
				_, b := maxNibbleKernelInput(n)
				require.Equal(t, dotNibbleNibbleRef(a, b), impl(a, b),
					"n=%d saturated input", n)
			}
		})
	}
}

func BenchmarkDotByteNibble(b *testing.B) {
	rng := rand.New(rand.NewPCG(5, 6))
	for _, half := range []int{512, 768} {
		q, packed := randomNibbleKernelInput(half, rng)
		for name, impl := range dotByteNibbleVariantsUnderTest() {
			b.Run(fmt.Sprintf("%s-d%d", name, 2*half), func(b *testing.B) {
				for b.Loop() {
					impl(q, packed)
				}
			})
		}
		b.Run(fmt.Sprintf("go-d%d", 2*half), func(b *testing.B) {
			for b.Loop() {
				dotByteNibbleRef(q, packed)
			}
		})
	}
}

func BenchmarkDotNibbleNibble(b *testing.B) {
	rng := rand.New(rand.NewPCG(7, 8))
	for _, half := range []int{512, 768} {
		_, x := randomNibbleKernelInput(half, rng)
		_, y := randomNibbleKernelInput(half, rng)
		for name, impl := range dotNibbleNibbleVariantsUnderTest() {
			b.Run(fmt.Sprintf("%s-d%d", name, 2*half), func(b *testing.B) {
				for b.Loop() {
					impl(x, y)
				}
			})
		}
		b.Run(fmt.Sprintf("go-d%d", 2*half), func(b *testing.B) {
			for b.Loop() {
				dotNibbleNibbleRef(x, y)
			}
		})
	}
}
