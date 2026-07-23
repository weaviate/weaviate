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

// Pure Go references, mirroring the default impls in distance.go (the vars
// are replaced at init time, so the references live here).
func dotByteRef(a, b []byte) uint32 {
	var sum uint32
	for i := range a {
		sum += uint32(a[i]) * uint32(b[i])
	}
	return sum
}

func l2ByteRef(a, b []byte) uint32 {
	var sum uint32
	for i := range a {
		diff := uint32(a[i]) - uint32(b[i])
		sum += diff * diff
	}
	return sum
}

func randomByteVector(n int, rng *rand.Rand) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(rng.UintN(256))
	}
	return v
}

// TestDotByteImplMatchesGo pins the dispatched u8 dot product and all
// architecture-specific kernels (including the goat-generated ones they may
// replace) to the pure Go reference, exact integer equality.
func TestDotByteImplMatchesGo(t *testing.T) {
	variants := dotByteVariantsUnderTest()
	variants["dispatched"] = dotByteImpl
	for name, impl := range variants {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(11, 12))
			for _, n := range nibbleKernelSizes {
				for trial := range 10 {
					a := randomByteVector(n, rng)
					b := randomByteVector(n, rng)
					require.Equal(t, dotByteRef(a, b), impl(a, b), "n=%d trial=%d", n, trial)
				}
				// Saturated input: max products stress accumulator headroom.
				a := make([]byte, n)
				b := make([]byte, n)
				for i := range a {
					a[i], b[i] = 255, 255
				}
				require.Equal(t, dotByteRef(a, b), impl(a, b), "n=%d saturated", n)
			}
		})
	}
}

func TestL2ByteImplMatchesGo(t *testing.T) {
	variants := l2ByteVariantsUnderTest()
	variants["dispatched"] = l2SquaredByteImpl
	for name, impl := range variants {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(13, 14))
			for _, n := range nibbleKernelSizes {
				for trial := range 10 {
					a := randomByteVector(n, rng)
					b := randomByteVector(n, rng)
					require.Equal(t, l2ByteRef(a, b), impl(a, b), "n=%d trial=%d", n, trial)
				}
				// Max differences in both directions.
				a := make([]byte, n)
				b := make([]byte, n)
				for i := range a {
					if i%2 == 0 {
						a[i], b[i] = 255, 0
					} else {
						a[i], b[i] = 0, 255
					}
				}
				require.Equal(t, l2ByteRef(a, b), impl(a, b), "n=%d max diffs", n)
			}
		})
	}
}

func BenchmarkDotByte(b *testing.B) {
	rng := rand.New(rand.NewPCG(15, 16))
	for _, n := range []int{1024, 1536} {
		x := randomByteVector(n, rng)
		y := randomByteVector(n, rng)
		for name, impl := range dotByteVariantsUnderTest() {
			b.Run(fmt.Sprintf("%s-d%d", name, n), func(b *testing.B) {
				for b.Loop() {
					impl(x, y)
				}
			})
		}
		b.Run(fmt.Sprintf("dispatched-d%d", n), func(b *testing.B) {
			for b.Loop() {
				dotByteImpl(x, y)
			}
		})
	}
}

func BenchmarkL2Byte(b *testing.B) {
	rng := rand.New(rand.NewPCG(17, 18))
	for _, n := range []int{1024, 1536} {
		x := randomByteVector(n, rng)
		y := randomByteVector(n, rng)
		for name, impl := range l2ByteVariantsUnderTest() {
			b.Run(fmt.Sprintf("%s-d%d", name, n), func(b *testing.B) {
				for b.Loop() {
					impl(x, y)
				}
			})
		}
		b.Run(fmt.Sprintf("dispatched-d%d", n), func(b *testing.B) {
			for b.Loop() {
				l2SquaredByteImpl(x, y)
			}
		})
	}
}
