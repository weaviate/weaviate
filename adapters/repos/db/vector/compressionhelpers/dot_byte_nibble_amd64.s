//go:build amd64

#include "textflag.h"

// Hand-written AVX2 kernels for the packed 4-bit code dot products of the
// 4-bit rotational quantizer. The nibbles are unpacked in registers (VPAND /
// VPSRLW+VPAND) and fed into VPMADDUBSW + VPMADDWD accumulation, so the
// packed codes never round-trip through a scratch buffer.
//
// VPMADDUBSW multiplies unsigned by signed bytes with saturating pair sums.
// The query codes (<= 255) go in the unsigned operand and the nibbles
// (<= 15) in the signed operand, so pair sums are bounded by 2*255*15 = 7650
// < 32767 and never saturate. Results are exact integer sums, identical to
// the pure Go fallbacks (including uint32 wraparound behavior).

DATA nibbleByteMask<>+0x00(SB)/1, $0x0f
GLOBL nibbleByteMask<>(SB), RODATA|NOPTR, $1

// func dotByteNibbleAVX2Asm(q, packed *byte, half int) uint32
TEXT ·dotByteNibbleAVX2Asm(SB), NOSPLIT, $0-28
    MOVQ q+0(FP), SI
    MOVQ packed+8(FP), DI
    MOVQ half+16(FP), DX
    MOVQ SI, R8
    ADDQ DX, R8                // R8 = high-nibble query plane
    XORL BX, BX                // scalar accumulator
    VPBROADCASTB nibbleByteMask<>(SB), Y13
    VPCMPEQD Y14, Y14, Y14
    VPSRLW $15, Y14, Y14       // Y14 = 16 x uint16(1) for VPMADDWD
    VPXOR Y0, Y0, Y0           // lo-plane accumulator
    VPXOR Y1, Y1, Y1           // hi-plane accumulator
    MOVQ DX, CX
    SHRQ $5, CX
    JZ bn_tail

bn_loop32:
    VMOVDQU (DI), Y2
    VPAND Y13, Y2, Y3          // lo nibbles
    VPSRLW $4, Y2, Y4
    VPAND Y13, Y4, Y4          // hi nibbles
    VMOVDQU (SI), Y5
    VMOVDQU (R8), Y6
    VPMADDUBSW Y3, Y5, Y7      // q lo (unsigned) * lo nibbles (signed)
    VPMADDWD Y14, Y7, Y7
    VPADDD Y7, Y0, Y0
    VPMADDUBSW Y4, Y6, Y8      // q hi (unsigned) * hi nibbles (signed)
    VPMADDWD Y14, Y8, Y8
    VPADDD Y8, Y1, Y1
    ADDQ $32, DI
    ADDQ $32, SI
    ADDQ $32, R8
    DECQ CX
    JNZ bn_loop32

bn_tail:
    MOVQ DX, CX
    ANDQ $31, CX
    JZ bn_reduce

bn_scalar:
    MOVBLZX (DI), R9
    MOVBLZX (SI), R10
    MOVBLZX (R8), R11
    MOVL R9, R12
    ANDL $0x0F, R12
    IMULL R10, R12
    ADDL R12, BX
    SHRL $4, R9
    IMULL R11, R9
    ADDL R9, BX
    INCQ DI
    INCQ SI
    INCQ R8
    DECQ CX
    JNZ bn_scalar

bn_reduce:
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

// func dotNibbleNibbleAVX2Asm(a, b *byte, n int) uint32
TEXT ·dotNibbleNibbleAVX2Asm(SB), NOSPLIT, $0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), DX
    XORL BX, BX
    VPBROADCASTB nibbleByteMask<>(SB), Y13
    VPCMPEQD Y14, Y14, Y14
    VPSRLW $15, Y14, Y14
    VPXOR Y0, Y0, Y0
    VPXOR Y1, Y1, Y1
    MOVQ DX, CX
    SHRQ $5, CX
    JZ nn_tail

nn_loop32:
    VMOVDQU (SI), Y2
    VMOVDQU (DI), Y5
    VPAND Y13, Y2, Y3          // lo(a)
    VPSRLW $4, Y2, Y4
    VPAND Y13, Y4, Y4          // hi(a)
    VPAND Y13, Y5, Y6          // lo(b)
    VPSRLW $4, Y5, Y7
    VPAND Y13, Y7, Y7          // hi(b)
    VPMADDUBSW Y6, Y3, Y8      // lo(a) unsigned * lo(b) signed, both <= 15
    VPMADDWD Y14, Y8, Y8
    VPADDD Y8, Y0, Y0
    VPMADDUBSW Y7, Y4, Y9
    VPMADDWD Y14, Y9, Y9
    VPADDD Y9, Y1, Y1
    ADDQ $32, SI
    ADDQ $32, DI
    DECQ CX
    JNZ nn_loop32

nn_tail:
    MOVQ DX, CX
    ANDQ $31, CX
    JZ nn_reduce

nn_scalar:
    MOVBLZX (SI), R9
    MOVBLZX (DI), R10
    MOVL R9, R11
    ANDL $0x0F, R11
    MOVL R10, R12
    ANDL $0x0F, R12
    IMULL R11, R12
    ADDL R12, BX
    SHRL $4, R9
    SHRL $4, R10
    IMULL R9, R10
    ADDL R10, BX
    INCQ SI
    INCQ DI
    DECQ CX
    JNZ nn_scalar

nn_reduce:
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
