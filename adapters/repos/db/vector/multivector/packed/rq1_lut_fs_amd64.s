//go:build !noasm && amd64

#include "textflag.h"

// The x86 fast scans of the int8-table RQ1 estimator: the SSSE3/SSE4.1 kernel
// (rq1_lut_fs_arm64.s with PSHUFB in place of TBL), the AVX2 kernel (two
// blocks per pass) and the AVX-512 kernel (four blocks per pass plus its own
// remainder). The Go wrappers in rq1_lut_fs_amd64.go document the arguments.
//
// All three score one query token's tables against sixteen document tokens
// per iteration, held in the byte lanes. Per stored code byte: one 16-byte
// load is that byte position for the whole block, the two nibbles are unpacked
// in registers (PAND / PSRLW+PAND), and each nibble indexes a 16-entry table
// held in a register. PSHUFB zeroes a lane whose index has its high bit set,
// which cannot happen here, since a nibble is 0..15.
//
// The int8 lookups accumulate into int16 lanes, exact while nibbles*127 fits
// int16; the caller gates on that. There are two independent accumulator
// pairs, because with one pair every lookup of a block sits on one dependency
// chain and the kernel is latency-bound. The epilogue widens to int32,
// converts to float, multiplies by the query token's scale and then by the
// sixteen tokens' Steps (that order, because the reference computes
// (S*scale)*Step and parity is bit equality), and folds into four running lane
// maxima kept in registers across blocks.
//
// Every 128-bit memory operand goes through MOVOU. MULPS with a memory operand
// would fault on a 16-byte misalignment, and nothing here is 16-byte aligned:
// a Go []float32 is 8-byte aligned, and a blob from an mmap'd segment has no
// alignment guarantee at an interior offset.

DATA fsNibbleMask<>+0x00(SB)/8, $0x0f0f0f0f0f0f0f0f
DATA fsNibbleMask<>+0x08(SB)/8, $0x0f0f0f0f0f0f0f0f
GLOBL fsNibbleMask<>(SB), RODATA|NOPTR, $16

// func rq1ScanBlocksAsmSSE(tbl *int8, codes *byte, steps, best *float32, groups, blocks int, scale float32)
TEXT ·rq1ScanBlocksAsmSSE(SB), NOSPLIT, $0-52
    MOVQ  tbl+0(FP), SI
    MOVQ  codes+8(FP), DI
    MOVQ  steps+16(FP), DX
    MOVQ  best+24(FP), BX
    MOVQ  groups+32(FP), CX
    MOVQ  blocks+40(FP), R8
    TESTQ R8, R8
    JZ    fs_done

    MOVSS  scale+48(FP), X12
    SHUFPS $0, X12, X12                 // scale in every lane
    MOVOU  fsNibbleMask<>(SB), X15

    MOVOU (BX), X8                      // running lane maxima
    MOVOU 16(BX), X9
    MOVOU 32(BX), X10
    MOVOU 48(BX), X11

fs_block:
    PXOR X13, X13                       // int16 accumulators, lanes 0..7
    PXOR X14, X14                       // and lanes 8..15
    PXOR X3, X3                         // a second, independent pair for the
    PXOR X6, X6                         // high nibbles' lookups
    MOVQ SI, R9                         // table cursor, reset per block
    MOVQ CX, R10                        // group counter

fs_group:
    MOVOU (DI), X0                      // one code byte for sixteen tokens
    MOVOU (R9), X1                      // the two nibble groups' tables
    MOVOU 16(R9), X2
    ADDQ  $16, DI
    ADDQ  $32, R9

    MOVO  X0, X7
    PAND  X15, X7                       // low nibbles
    PSRLW $4, X0
    PAND  X15, X0                       // high nibbles
    PSHUFB X7, X1
    PSHUFB X0, X2

    PMOVSXBW X1, X4
    PADDW    X4, X13
    PSRLDQ   $8, X1
    PMOVSXBW X1, X4
    PADDW    X4, X14
    PMOVSXBW X2, X5
    PADDW    X5, X3
    PSRLDQ   $8, X2
    PMOVSXBW X2, X5
    PADDW    X5, X6

    DECQ R10
    JNZ  fs_group

    PADDW    X3, X13
    PADDW    X6, X14
    PMOVSXWD X13, X0                    // tokens 0..3
    MOVO     X13, X1
    PSRLDQ   $8, X1
    PMOVSXWD X1, X1                     // tokens 4..7
    PMOVSXWD X14, X2                    // tokens 8..11
    MOVO     X14, X3
    PSRLDQ   $8, X3
    PMOVSXWD X3, X3                     // tokens 12..15
    CVTPL2PS X0, X0
    CVTPL2PS X1, X1
    CVTPL2PS X2, X2
    CVTPL2PS X3, X3
    MULPS    X12, X0
    MULPS    X12, X1
    MULPS    X12, X2
    MULPS    X12, X3

    MOVOU (DX), X4                      // sixteen tokens' Steps
    MOVOU 16(DX), X5
    MOVOU 32(DX), X6
    MOVOU 48(DX), X7
    ADDQ  $64, DX
    MULPS X4, X0
    MULPS X5, X1
    MULPS X6, X2
    MULPS X7, X3

    MAXPS X0, X8
    MAXPS X1, X9
    MAXPS X2, X10
    MAXPS X3, X11

    DECQ R8
    JNZ  fs_block

    MOVOU X8, (BX)
    MOVOU X9, 16(BX)
    MOVOU X10, 32(BX)
    MOVOU X11, 48(BX)

fs_done:
    RET

// The AVX2 fast scan: two blocks per pass, one in each 128-bit half of a ymm.
//
// The codes come from two cursors. A block's bytes for group g are sixteen
// contiguous bytes at b*16*groups + g*16, and consecutive blocks are 16*groups
// apart, so no 32-byte load fetches both: VMOVDQU loads block b's sixteen into
// the low half and VINSERTI128 block b+1's into the high half, each cursor
// stepping 16 per group. VPSHUFB shuffles within each 128-bit lane, so one
// 16-entry table serves both halves once VBROADCASTI128 has put it in both.
//
// What this buys over SSE is the widening: VPMOVSXBW into a ymm widens sixteen
// bytes at once, where PMOVSXBW widens eight. Three-operand VEX also removes
// the MOVO copies, and VEX memory operands carry no alignment requirement, so
// the Steps multiply reads memory directly.
//
// Four independent accumulator chains, for the reason the SSE kernel keeps
// two pairs. They fall out of the shape: the widening splits each shuffle
// result into block b and block b+1, and there are two nibble groups. The
// epilogue is the SSE one per block, in the same order (widen to int32,
// convert, scale, then Step), folding into the sixteen lane maxima both blocks
// share. The two blocks' Steps are contiguous, at 0/32 and 64/96 from the
// Steps cursor, which then advances 128.
//
// VZEROUPPER before returning: the caller may run the SSE routine on an odd
// trailing block next, and dirty upper halves would put every SSE instruction
// after this through the transition penalty.

// func rq1ScanBlocksAsmAVX2(tbl *int8, codes *byte, steps, best *float32, groups, pairs int, scale float32)
TEXT ·rq1ScanBlocksAsmAVX2(SB), NOSPLIT, $0-52
    MOVQ  tbl+0(FP), SI
    MOVQ  codes+8(FP), DI
    MOVQ  steps+16(FP), DX
    MOVQ  best+24(FP), BX
    MOVQ  groups+32(FP), CX
    MOVQ  pairs+40(FP), R8
    TESTQ R8, R8
    JZ    fs2_done

    MOVQ CX, R12
    SHLQ $4, R12                        // block stride: sixteen bytes per group
    LEAQ (DI)(R12*1), R11               // cursor for the pair's second block

    VBROADCASTSS   scale+48(FP), Y12    // scale in every lane
    VBROADCASTI128 fsNibbleMask<>(SB), Y15

    VMOVUPS (BX), Y8                    // running lane maxima
    VMOVUPS 32(BX), Y9

fs2_pair:
    VPXOR Y13, Y13, Y13                 // int16 accumulators: block b, low nibbles
    VPXOR Y14, Y14, Y14                 // block b+1, low nibbles
    VPXOR Y3, Y3, Y3                    // block b, high nibbles
    VPXOR Y6, Y6, Y6                    // block b+1, high nibbles
    MOVQ  SI, R9                        // table cursor, reset per pair
    MOVQ  CX, R10                       // group counter

fs2_group:
    VMOVDQU        (DI), X0             // one code byte for block b's sixteen tokens
    VINSERTI128    $1, (R11), Y0, Y0    // and block b+1's, in the upper half
    VBROADCASTI128 (R9), Y1             // the two nibble groups' tables, both halves
    VBROADCASTI128 16(R9), Y2
    ADDQ $16, DI
    ADDQ $16, R11
    ADDQ $32, R9

    VPAND   Y15, Y0, Y7                 // low nibbles
    VPSRLW  $4, Y0, Y0
    VPAND   Y15, Y0, Y0                 // high nibbles
    VPSHUFB Y7, Y1, Y1
    VPSHUFB Y0, Y2, Y2

    VPMOVSXBW    X1, Y4                 // block b's low-nibble lookups, widened
    VPADDW       Y4, Y13, Y13
    VEXTRACTI128 $1, Y1, X4
    VPMOVSXBW    X4, Y4                 // block b+1's
    VPADDW       Y4, Y14, Y14
    VPMOVSXBW    X2, Y5                 // the same for the high nibbles
    VPADDW       Y5, Y3, Y3
    VEXTRACTI128 $1, Y2, X5
    VPMOVSXBW    X5, Y5
    VPADDW       Y5, Y6, Y6

    DECQ R10
    JNZ  fs2_group

    VPADDW Y3, Y13, Y13                 // block b's sixteen sums
    VPADDW Y6, Y14, Y14                 // block b+1's

    VPMOVSXWD    X13, Y0                // block b, tokens 0..7
    VEXTRACTI128 $1, Y13, X1
    VPMOVSXWD    X1, Y1                 // tokens 8..15
    VCVTDQ2PS    Y0, Y0
    VCVTDQ2PS    Y1, Y1
    VMULPS       Y12, Y0, Y0            // scale first
    VMULPS       Y12, Y1, Y1
    VMULPS       (DX), Y0, Y0           // then the tokens' Steps
    VMULPS       32(DX), Y1, Y1
    VMAXPS       Y0, Y8, Y8
    VMAXPS       Y1, Y9, Y9

    VPMOVSXWD    X14, Y0                // block b+1, the same
    VEXTRACTI128 $1, Y14, X1
    VPMOVSXWD    X1, Y1
    VCVTDQ2PS    Y0, Y0
    VCVTDQ2PS    Y1, Y1
    VMULPS       Y12, Y0, Y0
    VMULPS       Y12, Y1, Y1
    VMULPS       64(DX), Y0, Y0
    VMULPS       96(DX), Y1, Y1
    VMAXPS       Y0, Y8, Y8
    VMAXPS       Y1, Y9, Y9
    ADDQ         $128, DX

    MOVQ R11, DI                        // next pair starts past the block R11 walked
    ADDQ R12, R11
    DECQ R8
    JNZ  fs2_pair

    VMOVUPS Y8, (BX)
    VMOVUPS Y9, 32(BX)

fs2_done:
    VZEROUPPER
    RET

// The AVX-512 fast scan: four blocks per pass, one in each 128-bit lane of a
// zmm. The one to three blocks a four-block pass cannot reach are scored here
// too, one at a time. Three block counts in four leave a remainder, so handing
// it to a narrower kernel would be the common path, and every hand-off stores
// the lane maxima for the next kernel to reload.
//
// The shape is the AVX2 kernel's, widened. Codes: VMOVDQU into the low lane
// and three VINSERTI32X4; blocks b+1 and b+2 are reached by indexed addressing
// off b's cursor and only b+3 has a cursor of its own, because the
// general-purpose registers are the scarce ones (Go reserves R14 and R15),
// with thirty-two zmm to spare. Tables: VBROADCASTI32X4 into all four lanes,
// since VPSHUFB is per lane. Widening a 64-byte shuffle result gives blocks b
// and b+1 in one zmm and b+2 and b+3 in another; with the two nibble groups
// that is again four independent int16 chains. Sixteen float32 lanes are
// exactly one zmm, so a block's sixteen sums are one register and the running
// lane maxima another.
// The epilogue per block is widen, convert, scale, then Step, then fold into
// the maxima, in block order.
//
// The remainder loop's VEX-encoded xmm and ymm instructions write temporaries
// only. The maxima, the scale and the mask live in zmm registers written only
// by EVEX instructions, so a VEX write's zeroing of the upper bits touches
// nothing kept. VZEROUPPER before returning, as in the AVX2 kernel.

// func rq1ScanBlocksAsmAVX512(tbl *int8, codes *byte, steps, best *float32, groups, blocks int, scale float32)
TEXT ·rq1ScanBlocksAsmAVX512(SB), NOSPLIT, $0-52
    MOVQ  tbl+0(FP), SI
    MOVQ  codes+8(FP), DI
    MOVQ  steps+16(FP), DX
    MOVQ  best+24(FP), BX
    MOVQ  groups+32(FP), CX
    MOVQ  blocks+40(FP), R8
    TESTQ R8, R8
    JZ    fs4_done

    MOVQ CX, AX
    SHLQ $4, AX                         // block stride: sixteen bytes per group
    MOVQ R8, R11
    ANDQ $3, R11                        // rest = blocks & 3
    SHRQ $2, R8                         // quads = blocks >> 2

    VBROADCASTSS    scale+48(FP), Z12   // scale in every lane
    VBROADCASTI32X4 fsNibbleMask<>(SB), Z15
    VMOVUPS         (BX), Z8            // running lane maxima, one zmm

    TESTQ R8, R8
    JZ    fs4_rest

    LEAQ (DI)(AX*2), R13
    ADDQ AX, R13                        // cursor for block b+3

fs4_quad:
    VPXORD Z13, Z13, Z13                // int16 accumulators: blocks b, b+1, low nibbles
    VPXORD Z14, Z14, Z14                // blocks b+2, b+3, low nibbles
    VPXORD Z3, Z3, Z3                   // blocks b, b+1, high nibbles
    VPXORD Z6, Z6, Z6                   // blocks b+2, b+3, high nibbles
    MOVQ   SI, R9                       // table cursor, reset per quad
    MOVQ   CX, R10                      // group counter

fs4_group:
    VMOVDQU         (DI), X0            // one code byte for block b's sixteen tokens
    VINSERTI32X4    $1, (DI)(AX*1), Z0, Z0   // block b+1's
    VINSERTI32X4    $2, (DI)(AX*2), Z0, Z0   // block b+2's
    VINSERTI32X4    $3, (R13), Z0, Z0        // block b+3's
    VBROADCASTI32X4 (R9), Z1            // the two nibble groups' tables, all four lanes
    VBROADCASTI32X4 16(R9), Z2
    ADDQ $16, DI
    ADDQ $16, R13
    ADDQ $32, R9

    VPANDD  Z15, Z0, Z7                 // low nibbles
    VPSRLW  $4, Z0, Z0
    VPANDD  Z15, Z0, Z0                 // high nibbles
    VPSHUFB Z7, Z1, Z1
    VPSHUFB Z0, Z2, Z2

    VPMOVSXBW     Y1, Z4                // blocks b, b+1: low-nibble lookups, widened
    VPADDW        Z4, Z13, Z13
    VEXTRACTI64X4 $1, Z1, Y4
    VPMOVSXBW     Y4, Z4                // blocks b+2, b+3
    VPADDW        Z4, Z14, Z14
    VPMOVSXBW     Y2, Z5                // the same for the high nibbles
    VPADDW        Z5, Z3, Z3
    VEXTRACTI64X4 $1, Z2, Y5
    VPMOVSXBW     Y5, Z5
    VPADDW        Z5, Z6, Z6

    DECQ R10
    JNZ  fs4_group

    VPADDW Z3, Z13, Z13                 // blocks b, b+1: sixteen sums each
    VPADDW Z6, Z14, Z14                 // blocks b+2, b+3

    VPMOVSXWD     Y13, Z0               // block b, sixteen int32
    VEXTRACTI64X4 $1, Z13, Y1
    VPMOVSXWD     Y1, Z1                // block b+1
    VPMOVSXWD     Y14, Z2               // block b+2
    VEXTRACTI64X4 $1, Z14, Y3
    VPMOVSXWD     Y3, Z3                // block b+3
    VCVTDQ2PS     Z0, Z0
    VCVTDQ2PS     Z1, Z1
    VCVTDQ2PS     Z2, Z2
    VCVTDQ2PS     Z3, Z3
    VMULPS        Z12, Z0, Z0           // scale first
    VMULPS        Z12, Z1, Z1
    VMULPS        Z12, Z2, Z2
    VMULPS        Z12, Z3, Z3
    VMULPS        (DX), Z0, Z0          // then each block's sixteen Steps
    VMULPS        64(DX), Z1, Z1
    VMULPS        128(DX), Z2, Z2
    VMULPS        192(DX), Z3, Z3
    VMAXPS        Z0, Z8, Z8
    VMAXPS        Z1, Z8, Z8
    VMAXPS        Z2, Z8, Z8
    VMAXPS        Z3, Z8, Z8
    ADDQ          $256, DX

    LEAQ (DI)(AX*2), DI
    ADDQ AX, DI                         // past blocks b+1..b+3, which the lanes walked
    LEAQ (DI)(AX*2), R13
    ADDQ AX, R13
    DECQ R8
    JNZ  fs4_quad

fs4_rest:
    TESTQ R11, R11
    JZ    fs4_store

fs4_block:
    VPXOR Y13, Y13, Y13                 // int16 accumulators, one block: low nibbles
    VPXOR Y3, Y3, Y3                    // and high nibbles
    MOVQ  SI, R9                        // table cursor, reset per block
    MOVQ  CX, R10                       // group counter

fs4_block_group:
    VMOVDQU (DI), X0                    // one code byte for sixteen tokens
    VMOVDQU (R9), X1                    // the two nibble groups' tables
    VMOVDQU 16(R9), X2
    ADDQ    $16, DI
    ADDQ    $32, R9

    VPAND   X15, X0, X7                 // low nibbles
    VPSRLW  $4, X0, X0
    VPAND   X15, X0, X0                 // high nibbles
    VPSHUFB X7, X1, X1
    VPSHUFB X0, X2, X2

    VPMOVSXBW X1, Y4                    // sixteen lookups, widened
    VPADDW    Y4, Y13, Y13
    VPMOVSXBW X2, Y5
    VPADDW    Y5, Y3, Y3

    DECQ R10
    JNZ  fs4_block_group

    VPADDW    Y3, Y13, Y13              // the block's sixteen sums
    VPMOVSXWD Y13, Z0
    VCVTDQ2PS Z0, Z0
    VMULPS    Z12, Z0, Z0               // scale first
    VMULPS    (DX), Z0, Z0              // then the tokens' Steps
    VMAXPS    Z0, Z8, Z8
    ADDQ      $64, DX

    DECQ R11
    JNZ  fs4_block

fs4_store:
    VMOVUPS Z8, (BX)

fs4_done:
    VZEROUPPER
    RET
