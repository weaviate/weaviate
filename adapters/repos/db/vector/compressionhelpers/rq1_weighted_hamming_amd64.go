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

//go:build amd64

package compressionhelpers

// weightedHammingChunkWords is what one pass of the AVX2 kernel reads of
// every input: 256 bits.
const weightedHammingChunkWords = 4

//go:noescape
func weightedHammingAsm(q0, q1, q2, q3, q4, x *uint64, chunks int) int

// weightedHammingAVX2 runs the AVX2 kernel over the 256-bit chunks and
// finishes the last words in Go.
func weightedHammingAVX2(q0, q1, q2, q3, q4, x []uint64) int {
	n := len(q0)
	x, q1, q2, q3, q4 = x[:n], q1[:n], q2[:n], q3[:n], q4[:n]
	s, done := 0, n/weightedHammingChunkWords*weightedHammingChunkWords
	if done > 0 {
		s = weightedHammingAsm(&q0[0], &q1[0], &q2[0], &q3[0], &q4[0], &x[0], done/weightedHammingChunkWords)
	}
	if done < n {
		s += weightedHammingGo(q0[done:], q1[done:], q2[done:], q3[done:], q4[done:], x[done:])
	}
	return s
}
