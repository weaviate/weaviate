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

// AVX-512 VNNI variants (gate: HasAVX512VNNI && HasAVX512BW). Nibbles are
// 0..15, which are non-negative as int8, so VPDPBUSD applies with NO
// correction term: the query bytes (or the other code's nibbles) ride in
// the unsigned operand and the nibbles in the signed one. The whole
// multiply+widen+accumulate is one instruction per 64 unpacked values -
// the same load/unpack/dot shape as the arm64 UDOT nibble kernels.
// Accumulation wraps mod 2^32 like the pure Go fallbacks (VPDPBUSD does
// not saturate), so results are bit-identical for every input.
//
// Tails have no scalar loop: the final 1..63 packed bytes use a BZHI-built
// K-mask with zeroing loads. Masked-off packed bytes unpack to nibble 0, so
// they contribute 0 regardless of the other operand (the query planes are
// masked with the same K register only to stay within the slice bounds).

// func dotByteNibbleVNNI512Asm(q, packed *byte, half int) uint32
TEXT ·dotByteNibbleVNNI512Asm(SB), NOSPLIT, $0-28
    MOVQ q+0(FP), SI
    MOVQ packed+8(FP), DI
    MOVQ half+16(FP), DX
    MOVQ SI, R8
    ADDQ DX, R8                // R8 = high-nibble query plane
    VPBROADCASTB nibbleByteMask<>(SB), Z13
    VPXORD Z0, Z0, Z0          // lo-plane accumulator (even block)
    VPXORD Z1, Z1, Z1          // hi-plane accumulator (even block)
    VPXORD Z20, Z20, Z20       // lo-plane accumulator (odd block)
    VPXORD Z21, Z21, Z21       // hi-plane accumulator (odd block)
    MOVQ DX, CX
    SHRQ $7, CX
    JZ bnv_tail64

bnv_loop128:
    VMOVDQU64 (DI), Z2         // packed[0:64]
    VPANDD Z13, Z2, Z3         // lo nibbles
    VPSRLW $4, Z2, Z4
    VPANDD Z13, Z4, Z4         // hi nibbles
    VMOVDQU64 (SI), Z5         // q lo plane
    VMOVDQU64 (R8), Z6         // q hi plane
    VPDPBUSD Z3, Z5, Z0        // q (unsigned) * lo nibbles (signed)
    VPDPBUSD Z4, Z6, Z1
    VMOVDQU64 64(DI), Z7       // packed[64:128]
    VPANDD Z13, Z7, Z8
    VPSRLW $4, Z7, Z9
    VPANDD Z13, Z9, Z9
    VMOVDQU64 64(SI), Z10
    VMOVDQU64 64(R8), Z11
    VPDPBUSD Z8, Z10, Z20
    VPDPBUSD Z9, Z11, Z21
    ADDQ $128, DI
    ADDQ $128, SI
    ADDQ $128, R8
    DECQ CX
    JNZ bnv_loop128

bnv_tail64:
    TESTQ $64, DX
    JZ bnv_tail_mask
    VMOVDQU64 (DI), Z2
    VPANDD Z13, Z2, Z3
    VPSRLW $4, Z2, Z4
    VPANDD Z13, Z4, Z4
    VMOVDQU64 (SI), Z5
    VMOVDQU64 (R8), Z6
    VPDPBUSD Z3, Z5, Z0
    VPDPBUSD Z4, Z6, Z1
    ADDQ $64, DI
    ADDQ $64, SI
    ADDQ $64, R8

bnv_tail_mask:
    MOVQ DX, CX
    ANDQ $63, CX
    JZ bnv_reduce
    MOVQ $-1, AX
    BZHIQ CX, AX, AX           // low CX bits set
    KMOVQ AX, K1
    VMOVDQU8.Z (DI), K1, Z2    // masked-off bytes unpack to nibble 0
    VPANDD Z13, Z2, Z3
    VPSRLW $4, Z2, Z4
    VPANDD Z13, Z4, Z4
    VMOVDQU8.Z (SI), K1, Z5
    VMOVDQU8.Z (R8), K1, Z6
    VPDPBUSD Z3, Z5, Z20
    VPDPBUSD Z4, Z6, Z21

bnv_reduce:
    VPADDD Z1, Z0, Z0
    VPADDD Z21, Z20, Z20
    VPADDD Z20, Z0, Z0
    VEXTRACTI64X4 $1, Z0, Y1
    VPADDD Y1, Y0, Y0
    VEXTRACTI128 $1, Y0, X1
    VPADDD X1, X0, X0
    VPSHUFD $0x4E, X0, X1
    VPADDD X1, X0, X0
    VPSHUFD $0xB1, X0, X1
    VPADDD X1, X0, X0
    VMOVD X0, AX
    VZEROUPPER
    MOVL AX, ret+24(FP)
    RET

// func dotNibbleNibbleVNNI512Asm(a, b *byte, n int) uint32
TEXT ·dotNibbleNibbleVNNI512Asm(SB), NOSPLIT, $0-28
    MOVQ a+0(FP), SI
    MOVQ b+8(FP), DI
    MOVQ n+16(FP), DX
    VPBROADCASTB nibbleByteMask<>(SB), Z13
    VPXORD Z0, Z0, Z0          // lo-plane accumulator (even block)
    VPXORD Z1, Z1, Z1          // hi-plane accumulator (even block)
    VPXORD Z20, Z20, Z20       // lo-plane accumulator (odd block)
    VPXORD Z21, Z21, Z21       // hi-plane accumulator (odd block)
    MOVQ DX, CX
    SHRQ $7, CX
    JZ nnv_tail64

nnv_loop128:
    VMOVDQU64 (SI), Z2         // a[0:64]
    VMOVDQU64 (DI), Z5         // b[0:64]
    VPANDD Z13, Z2, Z3         // lo(a)
    VPSRLW $4, Z2, Z4
    VPANDD Z13, Z4, Z4         // hi(a)
    VPANDD Z13, Z5, Z6         // lo(b)
    VPSRLW $4, Z5, Z7
    VPANDD Z13, Z7, Z7         // hi(b)
    VPDPBUSD Z6, Z3, Z0        // lo(a) unsigned * lo(b) signed, both <= 15
    VPDPBUSD Z7, Z4, Z1
    VMOVDQU64 64(SI), Z8       // a[64:128]
    VMOVDQU64 64(DI), Z11      // b[64:128]
    VPANDD Z13, Z8, Z9
    VPSRLW $4, Z8, Z10
    VPANDD Z13, Z10, Z10
    VPANDD Z13, Z11, Z12
    VPSRLW $4, Z11, Z14
    VPANDD Z13, Z14, Z14
    VPDPBUSD Z12, Z9, Z20
    VPDPBUSD Z14, Z10, Z21
    ADDQ $128, SI
    ADDQ $128, DI
    DECQ CX
    JNZ nnv_loop128

nnv_tail64:
    TESTQ $64, DX
    JZ nnv_tail_mask
    VMOVDQU64 (SI), Z2
    VMOVDQU64 (DI), Z5
    VPANDD Z13, Z2, Z3
    VPSRLW $4, Z2, Z4
    VPANDD Z13, Z4, Z4
    VPANDD Z13, Z5, Z6
    VPSRLW $4, Z5, Z7
    VPANDD Z13, Z7, Z7
    VPDPBUSD Z6, Z3, Z0
    VPDPBUSD Z7, Z4, Z1
    ADDQ $64, SI
    ADDQ $64, DI

nnv_tail_mask:
    MOVQ DX, CX
    ANDQ $63, CX
    JZ nnv_reduce
    MOVQ $-1, AX
    BZHIQ CX, AX, AX
    KMOVQ AX, K1
    VMOVDQU8.Z (SI), K1, Z2    // masked-off bytes unpack to nibble 0
    VMOVDQU8.Z (DI), K1, Z5
    VPANDD Z13, Z2, Z3
    VPSRLW $4, Z2, Z4
    VPANDD Z13, Z4, Z4
    VPANDD Z13, Z5, Z6
    VPSRLW $4, Z5, Z7
    VPANDD Z13, Z7, Z7
    VPDPBUSD Z6, Z3, Z20
    VPDPBUSD Z7, Z4, Z21

nnv_reduce:
    VPADDD Z1, Z0, Z0
    VPADDD Z21, Z20, Z20
    VPADDD Z20, Z0, Z0
    VEXTRACTI64X4 $1, Z0, Y1
    VPADDD Y1, Y0, Y0
    VEXTRACTI128 $1, Y0, X1
    VPADDD X1, X0, X0
    VPSHUFD $0x4E, X0, X1
    VPADDD X1, X0, X0
    VPSHUFD $0xB1, X0, X1
    VPADDD X1, X0, X0
    VMOVD X0, AX
    VZEROUPPER
    MOVL AX, ret+24(FP)
    RET
