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

import "math/bits"

// weightedHammingImpl returns S = Σ_p 2^p · popcount(q_p XOR x) over the
// first len(q0) words, q0 to q4 the five bit planes of the multi-bit query
// code of the 1-bit rotational quantizers and x the bits of a 1-bit code.
// Every plane and x must hold at least len(q0) words. The architecture files
// replace it with a SIMD kernel; every implementation returns the same S.
var weightedHammingImpl = weightedHammingGo

// weightedHamming is the integer the 1-bit RQ estimates are built on: the
// dot product of the query code and the 1-bit code x is 31·D - 2·S, D the
// dimensions. It reads x once for the five planes. Five Hamming distances,
// one per plane, give the same S, reading x five times.
func (c *RQMultiBitCode) weightedHamming(x []uint64) int {
	return weightedHammingImpl(c.bits0, c.bits1, c.bits2, c.bits3, c.bits4, x)
}

// weightedHammingGo is weightedHammingImpl in Go, one pass over the words.
func weightedHammingGo(q0, q1, q2, q3, q4, x []uint64) int {
	n := len(q0)
	x, q1, q2, q3, q4 = x[:n], q1[:n], q2[:n], q3[:n], q4[:n]
	var c0, c1, c2, c3, c4 int
	for i, w := range x {
		c0 += bits.OnesCount64(q0[i] ^ w)
		c1 += bits.OnesCount64(q1[i] ^ w)
		c2 += bits.OnesCount64(q2[i] ^ w)
		c3 += bits.OnesCount64(q3[i] ^ w)
		c4 += bits.OnesCount64(q4[i] ^ w)
	}
	return c0 + c1<<1 + c2<<2 + c3<<3 + c4<<4
}
