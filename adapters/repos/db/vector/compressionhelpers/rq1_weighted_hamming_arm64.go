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

//go:build arm64

package compressionhelpers

const (
	// weightedHammingChunkWords is what one pass of the NEON kernel reads of
	// every input: 128 bits.
	weightedHammingChunkWords = 2
	// weightedHammingMaxChunks is the most chunks one call of the NEON kernel
	// takes: it counts every plane in bytes, 8 per chunk, at most 255.
	weightedHammingMaxChunks = 31
)

//go:noescape
func weightedHammingAsm(q0, q1, q2, q3, q4, x *uint64, chunks int) int

// weightedHammingNEON runs the NEON kernel over the 128-bit chunks, at most
// weightedHammingMaxChunks per call, and finishes an odd last word in Go.
func weightedHammingNEON(q0, q1, q2, q3, q4, x []uint64) int {
	n := len(q0)
	x, q1, q2, q3, q4 = x[:n], q1[:n], q2[:n], q3[:n], q4[:n]
	s, done := 0, 0
	for n-done >= weightedHammingChunkWords {
		chunks := min((n-done)/weightedHammingChunkWords, weightedHammingMaxChunks)
		s += weightedHammingAsm(&q0[done], &q1[done], &q2[done], &q3[done], &q4[done], &x[done], chunks)
		done += chunks * weightedHammingChunkWords
	}
	if done < n {
		s += weightedHammingGo(q0[done:], q1[done:], q2[done:], q3[done:], q4[done:], x[done:])
	}
	return s
}
