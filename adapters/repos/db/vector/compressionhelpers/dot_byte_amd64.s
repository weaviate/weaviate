//go:build amd64

#include "textflag.h"

// Hand-written AVX2 kernels for uint8 dot product and squared L2 distance,
// used by the 8-bit rotational and scalar quantizers. The goat-generated
// kernels in distancer/asm widen bytes to 32/64-bit lanes (vpmovzxbd /
// vpmovzxbq, 8 elements per op); these widen to 16-bit (vpmovzxbw, 16
// elements) and multiply-accumulate with VPMADDWD. u8*u8 products (max
// 65025) and their pairwise sums (max 130050) fit int32 exactly, so no
// saturation is possible. Results are exact integer sums, identical to the
// pure Go fallbacks (including uint32 wraparound behavior).

// func dotByteWideAVX2Asm(a, b *byte, n int) uint32
TEXT ·dotByteWideAVX2Asm(SB), NOSPLIT, $0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), DX
    XORL BX, BX                // scalar accumulator
    VPXOR Y0, Y0, Y0
    VPXOR Y1, Y1, Y1
    MOVQ DX, CX
    SHRQ $5, CX
    JZ dbw_tail

dbw_loop32:
    VPMOVZXBW (SI), Y2         // a[0:16] as 16 uint16 words
    VPMOVZXBW 16(SI), Y3
    VPMOVZXBW (DI), Y4
    VPMOVZXBW 16(DI), Y5
    VPMADDWD Y4, Y2, Y2
    VPADDD Y2, Y0, Y0
    VPMADDWD Y5, Y3, Y3
    VPADDD Y3, Y1, Y1
    ADDQ $32, SI
    ADDQ $32, DI
    DECQ CX
    JNZ dbw_loop32

dbw_tail:
    MOVQ DX, CX
    ANDQ $31, CX
    JZ dbw_reduce

dbw_scalar:
    MOVBLZX (SI), R9
    MOVBLZX (DI), R10
    IMULL R10, R9
    ADDL R9, BX
    INCQ SI
    INCQ DI
    DECQ CX
    JNZ dbw_scalar

dbw_reduce:
    VPADDD Y1, Y0, Y0
    VEXTRACTI128 $1, Y0, X1
    VPADDD X1, X0, X0
    VPSHUFD $0x4E, X0, X1
    VPADDD X1, X0, X0
    VPSHUFD $0xB1, X0, X1
    VPADDD X1, X0, X0
    VMOVD X0, AX
    ADDL BX, AX
    VZEROUPPER
    MOVL AX, ret+24(FP)
    RET

// func l2ByteWideAVX2Asm(a, b *byte, n int) uint32
// Word differences of zero-extended bytes fit int16 (-255..255), and
// VPMADDWD of the difference with itself accumulates the squared terms.
TEXT ·l2ByteWideAVX2Asm(SB), NOSPLIT, $0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), DX
    XORL BX, BX
    VPXOR Y0, Y0, Y0
    VPXOR Y1, Y1, Y1
    MOVQ DX, CX
    SHRQ $5, CX
    JZ l2bw_tail

l2bw_loop32:
    VPMOVZXBW (SI), Y2
    VPMOVZXBW 16(SI), Y3
    VPMOVZXBW (DI), Y4
    VPMOVZXBW 16(DI), Y5
    VPSUBW Y4, Y2, Y2
    VPMADDWD Y2, Y2, Y2
    VPADDD Y2, Y0, Y0
    VPSUBW Y5, Y3, Y3
    VPMADDWD Y3, Y3, Y3
    VPADDD Y3, Y1, Y1
    ADDQ $32, SI
    ADDQ $32, DI
    DECQ CX
    JNZ l2bw_loop32

l2bw_tail:
    MOVQ DX, CX
    ANDQ $31, CX
    JZ l2bw_reduce

l2bw_scalar:
    MOVBLZX (SI), R9
    MOVBLZX (DI), R10
    SUBL R10, R9
    IMULL R9, R9
    ADDL R9, BX
    INCQ SI
    INCQ DI
    DECQ CX
    JNZ l2bw_scalar

l2bw_reduce:
    VPADDD Y1, Y0, Y0
    VEXTRACTI128 $1, Y0, X1
    VPADDD X1, X0, X0
    VPSHUFD $0x4E, X0, X1
    VPADDD X1, X0, X0
    VPSHUFD $0xB1, X0, X1
    VPADDD X1, X0, X0
    VMOVD X0, AX
    ADDL BX, AX
    VZEROUPPER
    MOVL AX, ret+24(FP)
    RET
