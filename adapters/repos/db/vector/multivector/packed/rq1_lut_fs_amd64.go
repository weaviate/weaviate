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
// (two blocks per pass) or AVX-512 (four blocks per pass). x86Kernels makes
// the choice from the reported feature flags, so the tests can call it with
// every combination of flags, whatever the test machine has.
//
// The wider kernels read the stored 16-token blocks two or four at a time, one
// block per 128-bit lane, so no second layout is needed. There is no query
// tile on x86: width 4 does not fit the register file, and width 2 gained
// nothing.
func init() {
	rq1ScanBlocksVariants = x86Kernels(
		cpu.X86.HasSSSE3 && cpu.X86.HasSSE41,
		cpu.X86.HasAVX2,
		cpu.X86.HasAVX512F && cpu.X86.HasAVX512BW)
	if n := len(rq1ScanBlocksVariants); n > 0 {
		rq1ScanBlocksImpl = rq1ScanBlocksVariants[n-1].fn
	}
}

// x86Kernels lists the block kernels the reported instruction sets allow,
// narrowest first; the last one is the one to install. Empty means the
// portable kernel.
//
// Arguments:
//   - sse: SSSE3 and SSE4.1. PSHUFB is SSSE3 and the sign-extending widenings
//     are SSE4.1, so both are needed, as compressionhelpers/distance_amd64.go
//     checks them.
//   - avx2: AVX2. The AVX2 kernel also needs sse, because it hands an odd
//     trailing block to the SSE routine.
//   - avx512: AVX-512 F and BW. The foundation subset lacks the shuffle, the
//     widening and the int16 adds, which are byte and word instructions. The
//     AVX-512 kernel also needs AVX2: its remainder loop scores the one to
//     three blocks a four-block pass cannot reach with VEX-encoded
//     instructions on 128-bit and 256-bit registers, which are AVX2.
func x86Kernels(sse, avx2, avx512 bool) []rq1ScanBlocksVariant {
	if !sse {
		return nil
	}
	variants := []rq1ScanBlocksVariant{{name: "sse", fn: rq1ScanBlocksSSE}}
	if avx2 {
		variants = append(variants, rq1ScanBlocksVariant{name: "avx2", fn: rq1ScanBlocksAVX2})
	}
	if avx2 && avx512 {
		variants = append(variants, rq1ScanBlocksVariant{name: "avx512", fn: rq1ScanBlocksAVX512})
	}
	return variants
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
