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

//go:build !noasm && amd64

package packed

import "golang.org/x/sys/cpu"

// init installs the widest x86 block kernel the machine supports: SSE, AVX2
// (two blocks per pass) or AVX-512 (four blocks per pass).
//
// PSHUFB is SSSE3 and the sign-extending widenings are SSE4.1, so both are
// checked, as compressionhelpers/distance_amd64.go does. The AVX2 kernel is
// installed inside the SSE check because it hands an odd trailing block to the
// SSE routine. AVX-512 needs the BW extension (byte and word instructions) as
// well as F (the foundation): the shuffle, the widening and the int16 adds are
// byte and word instructions the foundation subset lacks. Its routine scores
// its own remainder.
//
// The wider kernels read the stored 16-token blocks two or four at a time, one
// block per 128-bit lane, so no second layout is needed. There is no query
// tile on x86: width 4 does not fit the register file, and width 2 gained
// nothing.
func init() {
	if cpu.X86.HasSSSE3 && cpu.X86.HasSSE41 {
		rq1ScanBlocksImpl = rq1ScanBlocksSSE
		rq1ScanBlocksVariants = append(rq1ScanBlocksVariants,
			rq1ScanBlocksVariant{name: "sse", fn: rq1ScanBlocksSSE})
		if cpu.X86.HasAVX2 {
			rq1ScanBlocksImpl = rq1ScanBlocksAVX2
			rq1ScanBlocksVariants = append(rq1ScanBlocksVariants,
				rq1ScanBlocksVariant{name: "avx2", fn: rq1ScanBlocksAVX2})
		}
		if cpu.X86.HasAVX512F && cpu.X86.HasAVX512BW {
			rq1ScanBlocksImpl = rq1ScanBlocksAVX512
			rq1ScanBlocksVariants = append(rq1ScanBlocksVariants,
				rq1ScanBlocksVariant{name: "avx512", fn: rq1ScanBlocksAVX512})
		}
	}
}

//go:noescape
func rq1ScanBlocksAsmSSE(tbl *int8, codes *byte, steps, best *float32, groups, blocks int, scale float32)

// rq1ScanBlocksSSE is the SSE block kernel; arguments are rq1ScanBlocksImpl's.
// It bounds-checks what the assembly then reads unchecked, in the shape
// asm/dot_byte_nibble_arm64.go uses: one indexed read per slice, so a short
// slice panics here instead of being read past its end in the assembly.
func rq1ScanBlocksSSE(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32) {
	if blocks == 0 {
		return
	}
	_ = tbl[32*groups-1]
	_ = codes[blocks*blockTokens*groups-1]
	_ = steps[blocks*blockTokens-1]
	_ = best[blockTokens-1]
	rq1ScanBlocksAsmSSE(&tbl[0], &codes[0], &steps[0], &best[0], groups, blocks, scale)
}

//go:noescape
func rq1ScanBlocksAsmAVX2(tbl *int8, codes *byte, steps, best *float32, groups, pairs int, scale float32)

// rq1ScanBlocksAVX2 is the AVX2 block kernel; arguments are
// rq1ScanBlocksImpl's. It bounds-checks the whole scan like rq1ScanBlocksSSE,
// runs the AVX2 routine over the blocks in pairs, and hands an odd trailing
// block to the SSE routine. The SSE routine is already the parity-checked way
// to score one block, and both fold into the same sixteen lane maxima, so the
// order does not affect the result.
func rq1ScanBlocksAVX2(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32) {
	if blocks == 0 {
		return
	}
	_ = tbl[32*groups-1]
	_ = codes[blocks*blockTokens*groups-1]
	_ = steps[blocks*blockTokens-1]
	_ = best[blockTokens-1]
	if pairs := blocks / 2; pairs > 0 {
		rq1ScanBlocksAsmAVX2(&tbl[0], &codes[0], &steps[0], &best[0], groups, pairs, scale)
	}
	if blocks&1 != 0 {
		// token offset of the last block; its codes start groups bytes per
		// token later
		off := (blocks - 1) * blockTokens
		rq1ScanBlocksAsmSSE(&tbl[0], &codes[off*groups], &steps[off], &best[0], groups, 1, scale)
	}
}

//go:noescape
func rq1ScanBlocksAsmAVX512(tbl *int8, codes *byte, steps, best *float32, groups, blocks int, scale float32)

// rq1ScanBlocksAVX512 is the AVX-512 block kernel; arguments are
// rq1ScanBlocksImpl's. It bounds-checks like rq1ScanBlocksSSE. The routine
// takes the block count itself and scores the one to three blocks a four-block
// pass cannot reach, so nothing is handed on.
func rq1ScanBlocksAVX512(tbl []int8, codes []byte, steps, best []float32, groups, blocks int, scale float32) {
	if blocks == 0 {
		return
	}
	_ = tbl[32*groups-1]
	_ = codes[blocks*blockTokens*groups-1]
	_ = steps[blocks*blockTokens-1]
	_ = best[blockTokens-1]
	rq1ScanBlocksAsmAVX512(&tbl[0], &codes[0], &steps[0], &best[0], groups, blocks, scale)
}
