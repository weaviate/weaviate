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
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// The per-kernel parity tests live next to the kernels in hnsw/distancer;
// this file only pins the CPU-feature dispatch: whatever implementation init
// selected on this machine must match the pure Go semantics exactly. The
// size sweep covers all SIMD block sizes, step-down tails, and scalar tails.
var dispatchSizes = []int{
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
	24, 25, 31, 32, 33, 40, 47, 48, 56, 63, 64, 65, 100, 127, 128,
	129, 192, 255, 256, 257, 320, 512, 768, 1000,
}

func randomBytes(n int, rng *rand.Rand) []byte {
	v := make([]byte, n)
	for i := range v {
		v[i] = byte(rng.UintN(256))
	}
	return v
}

func TestDispatchedByteDistancesMatchGo(t *testing.T) {
	rng := rand.New(rand.NewPCG(21, 22))

	dotByteRef := func(a, b []byte) uint32 {
		var sum uint32
		for i := range a {
			sum += uint32(a[i]) * uint32(b[i])
		}
		return sum
	}
	l2ByteRef := func(a, b []byte) uint32 {
		var sum uint32
		for i := range a {
			diff := uint32(a[i]) - uint32(b[i])
			sum += diff * diff
		}
		return sum
	}

	for _, n := range dispatchSizes {
		for trial := range 5 {
			a, b := randomBytes(n, rng), randomBytes(n, rng)
			require.Equal(t, dotByteRef(a, b), dotByteImpl(a, b), "dot n=%d trial=%d", n, trial)
			require.Equal(t, l2ByteRef(a, b), l2SquaredByteImpl(a, b), "l2 n=%d trial=%d", n, trial)

			q, packed := randomBytes(2*n, rng), randomBytes(n, rng)
			require.Equal(t, dotByteNibbleGo(q, packed), dotByteNibbleImpl(q, packed),
				"byte-nibble half=%d trial=%d", n, trial)
			x, y := randomBytes(n, rng), randomBytes(n, rng)
			require.Equal(t, dotNibbleNibbleGo(x, y), dotNibbleNibbleImpl(x, y),
				"nibble-nibble n=%d trial=%d", n, trial)
		}
	}
}
