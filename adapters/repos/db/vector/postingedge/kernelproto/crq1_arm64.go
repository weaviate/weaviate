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

package kernelproto

// fusedNEON returns S over chunks 128-bit chunks of the code x, with the
// query planes interleaved per chunk at q. chunks is at most maxFusedChunks.
//
//go:noescape
func fusedNEON(q, x *uint64, chunks int) int

// scanBlockNEON writes S of the 32 codes of a block into s: groups tables of
// 16 bytes at lut, and as many 16-byte rows of packed nibbles at nibbles.
//
//go:noescape
func scanBlockNEON(lut, nibbles *uint8, groups int, s *uint16)

func fused(q *Query, x []uint64) int {
	chunks := q.Words / chunkWords
	if chunks > maxFusedChunks {
		return FusedGo(q, x)
	}
	return fusedNEON(&q.interleaved[0], &x[0], chunks)
}

func scanBlock(lut, nibbles []uint8, s *[BlockCodes]uint16) {
	scanBlockNEON(&lut[0], &nibbles[0], len(nibbles)/16, &s[0])
}
