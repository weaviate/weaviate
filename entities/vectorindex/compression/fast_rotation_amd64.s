//go:build amd64

#include "textflag.h"

// Hand-written AVX implementation of the fast Walsh-Hadamard transform used
// by the fast random rotation. A 64-point transform fits in eight YMM
// registers (64 float32 = 8 x 256-bit vectors): lane-level butterflies use
// VPERMILPS/VPERM2F128 shuffles (with VADDSUBPS producing the interleaved
// sum/difference pattern of the stride-1 stage directly), and the remaining
// stages are three-operand VADDPS/VSUBPS ping-ponging between Y0-Y7 and
// Y8-Y15. The butterfly ordering matches the pure Go implementation, so
// results are bit-identical (VADDSUBPS computes the odd-lane sums as b+a
// instead of a+b, which is bit-identical for IEEE addition).
//
// Requires AVX; the Go dispatcher falls back to the pure Go implementation
// on CPUs without it.

DATA fwhtScale64<>+0x00(SB)/4, $0x3E000000  // float32(0.125)
GLOBL fwhtScale64<>(SB), RODATA|NOPTR, $4
DATA fwhtScale256<>+0x00(SB)/4, $0x3D800000 // float32(0.0625)
GLOBL fwhtScale256<>(SB), RODATA|NOPTR, $4

// func fwht64AVX(x *float32)
// 64-point FWHT with 1/8 normalization, x must have 64 readable floats.
TEXT ·fwht64AVX(SB), NOSPLIT, $0-8
    MOVQ x+0(FP), DI
    VMOVUPS 0(DI), Y0
    VMOVUPS 32(DI), Y1
    VMOVUPS 64(DI), Y2
    VMOVUPS 96(DI), Y3
    VMOVUPS 128(DI), Y4
    VMOVUPS 160(DI), Y5
    VMOVUPS 192(DI), Y6
    VMOVUPS 224(DI), Y7
    VBROADCASTSS fwhtScale64<>(SB), Y8
    VMULPS Y8, Y0, Y0
    VMULPS Y8, Y1, Y1
    VMULPS Y8, Y2, Y2
    VMULPS Y8, Y3, Y3
    VMULPS Y8, Y4, Y4
    VMULPS Y8, Y5, Y5
    VMULPS Y8, Y6, Y6
    VMULPS Y8, Y7, Y7
    // stage 1: element stride 1.
    // [d0,s0,d2,s2,...] = VADDSUBPS(v, pair-swap(v)), then swap pairs.
    VPERMILPS $0xB1, Y0, Y8
    VADDSUBPS Y8, Y0, Y0
    VPERMILPS $0xB1, Y0, Y0
    VPERMILPS $0xB1, Y1, Y9
    VADDSUBPS Y9, Y1, Y1
    VPERMILPS $0xB1, Y1, Y1
    VPERMILPS $0xB1, Y2, Y10
    VADDSUBPS Y10, Y2, Y2
    VPERMILPS $0xB1, Y2, Y2
    VPERMILPS $0xB1, Y3, Y11
    VADDSUBPS Y11, Y3, Y3
    VPERMILPS $0xB1, Y3, Y3
    VPERMILPS $0xB1, Y4, Y12
    VADDSUBPS Y12, Y4, Y4
    VPERMILPS $0xB1, Y4, Y4
    VPERMILPS $0xB1, Y5, Y13
    VADDSUBPS Y13, Y5, Y5
    VPERMILPS $0xB1, Y5, Y5
    VPERMILPS $0xB1, Y6, Y14
    VADDSUBPS Y14, Y6, Y6
    VPERMILPS $0xB1, Y6, Y6
    VPERMILPS $0xB1, Y7, Y15
    VADDSUBPS Y15, Y7, Y7
    VPERMILPS $0xB1, Y7, Y7
    // stage 2: element stride 2. Per 128-bit lane: s/d against the
    // 64-bit-swapped vector, then [s0,s1,d0,d1] via VSHUFPS.
    VPERMILPS $0x4E, Y0, Y8
    VADDPS Y8, Y0, Y9
    VSUBPS Y8, Y0, Y0
    VSHUFPS $0x44, Y0, Y9, Y0
    VPERMILPS $0x4E, Y1, Y10
    VADDPS Y10, Y1, Y11
    VSUBPS Y10, Y1, Y1
    VSHUFPS $0x44, Y1, Y11, Y1
    VPERMILPS $0x4E, Y2, Y12
    VADDPS Y12, Y2, Y13
    VSUBPS Y12, Y2, Y2
    VSHUFPS $0x44, Y2, Y13, Y2
    VPERMILPS $0x4E, Y3, Y14
    VADDPS Y14, Y3, Y15
    VSUBPS Y14, Y3, Y3
    VSHUFPS $0x44, Y3, Y15, Y3
    VPERMILPS $0x4E, Y4, Y8
    VADDPS Y8, Y4, Y9
    VSUBPS Y8, Y4, Y4
    VSHUFPS $0x44, Y4, Y9, Y4
    VPERMILPS $0x4E, Y5, Y10
    VADDPS Y10, Y5, Y11
    VSUBPS Y10, Y5, Y5
    VSHUFPS $0x44, Y5, Y11, Y5
    VPERMILPS $0x4E, Y6, Y12
    VADDPS Y12, Y6, Y13
    VSUBPS Y12, Y6, Y6
    VSHUFPS $0x44, Y6, Y13, Y6
    VPERMILPS $0x4E, Y7, Y14
    VADDPS Y14, Y7, Y15
    VSUBPS Y14, Y7, Y7
    VSHUFPS $0x44, Y7, Y15, Y7
    // stage 3: element stride 4. s/d against the 128-bit-half-swapped
    // vector, then [s.low, d.low] via VPERM2F128.
    VPERM2F128 $0x01, Y0, Y0, Y8
    VADDPS Y8, Y0, Y9
    VSUBPS Y8, Y0, Y0
    VPERM2F128 $0x20, Y0, Y9, Y0
    VPERM2F128 $0x01, Y1, Y1, Y10
    VADDPS Y10, Y1, Y11
    VSUBPS Y10, Y1, Y1
    VPERM2F128 $0x20, Y1, Y11, Y1
    VPERM2F128 $0x01, Y2, Y2, Y12
    VADDPS Y12, Y2, Y13
    VSUBPS Y12, Y2, Y2
    VPERM2F128 $0x20, Y2, Y13, Y2
    VPERM2F128 $0x01, Y3, Y3, Y14
    VADDPS Y14, Y3, Y15
    VSUBPS Y14, Y3, Y3
    VPERM2F128 $0x20, Y3, Y15, Y3
    VPERM2F128 $0x01, Y4, Y4, Y8
    VADDPS Y8, Y4, Y9
    VSUBPS Y8, Y4, Y4
    VPERM2F128 $0x20, Y4, Y9, Y4
    VPERM2F128 $0x01, Y5, Y5, Y10
    VADDPS Y10, Y5, Y11
    VSUBPS Y10, Y5, Y5
    VPERM2F128 $0x20, Y5, Y11, Y5
    VPERM2F128 $0x01, Y6, Y6, Y12
    VADDPS Y12, Y6, Y13
    VSUBPS Y12, Y6, Y6
    VPERM2F128 $0x20, Y6, Y13, Y6
    VPERM2F128 $0x01, Y7, Y7, Y14
    VADDPS Y14, Y7, Y15
    VSUBPS Y14, Y7, Y7
    VPERM2F128 $0x20, Y7, Y15, Y7
    // stage 4: element stride 8 (register pairs, bank Y0-7 -> Y8-15)
    VADDPS Y1, Y0, Y8
    VSUBPS Y1, Y0, Y9
    VADDPS Y3, Y2, Y10
    VSUBPS Y3, Y2, Y11
    VADDPS Y5, Y4, Y12
    VSUBPS Y5, Y4, Y13
    VADDPS Y7, Y6, Y14
    VSUBPS Y7, Y6, Y15
    // stage 5: element stride 16 (bank Y8-15 -> Y0-7)
    VADDPS Y10, Y8, Y0
    VSUBPS Y10, Y8, Y2
    VADDPS Y11, Y9, Y1
    VSUBPS Y11, Y9, Y3
    VADDPS Y14, Y12, Y4
    VSUBPS Y14, Y12, Y6
    VADDPS Y15, Y13, Y5
    VSUBPS Y15, Y13, Y7
    // stage 6: element stride 32 (bank Y0-7 -> Y8-15)
    VADDPS Y4, Y0, Y8
    VSUBPS Y4, Y0, Y12
    VADDPS Y5, Y1, Y9
    VSUBPS Y5, Y1, Y13
    VADDPS Y6, Y2, Y10
    VSUBPS Y6, Y2, Y14
    VADDPS Y7, Y3, Y11
    VSUBPS Y7, Y3, Y15
    VMOVUPS Y8, 0(DI)
    VMOVUPS Y9, 32(DI)
    VMOVUPS Y10, 64(DI)
    VMOVUPS Y11, 96(DI)
    VMOVUPS Y12, 128(DI)
    VMOVUPS Y13, 160(DI)
    VMOVUPS Y14, 192(DI)
    VMOVUPS Y15, 224(DI)
    VZEROUPPER
    RET

// func fwht256AVX(x *float32)
// 256-point FWHT with 1/16 normalization: four in-register 64-point
// transforms followed by a fused radix-4 combine of the stride-64 and
// stride-128 butterfly stages.
TEXT ·fwht256AVX(SB), NOSPLIT, $0-8
    MOVQ x+0(FP), DI
    MOVQ $4, CX

fwht256_block:
    VMOVUPS 0(DI), Y0
    VMOVUPS 32(DI), Y1
    VMOVUPS 64(DI), Y2
    VMOVUPS 96(DI), Y3
    VMOVUPS 128(DI), Y4
    VMOVUPS 160(DI), Y5
    VMOVUPS 192(DI), Y6
    VMOVUPS 224(DI), Y7
    VBROADCASTSS fwhtScale256<>(SB), Y8
    VMULPS Y8, Y0, Y0
    VMULPS Y8, Y1, Y1
    VMULPS Y8, Y2, Y2
    VMULPS Y8, Y3, Y3
    VMULPS Y8, Y4, Y4
    VMULPS Y8, Y5, Y5
    VMULPS Y8, Y6, Y6
    VMULPS Y8, Y7, Y7
    // stage 1: element stride 1.
    // [d0,s0,d2,s2,...] = VADDSUBPS(v, pair-swap(v)), then swap pairs.
    VPERMILPS $0xB1, Y0, Y8
    VADDSUBPS Y8, Y0, Y0
    VPERMILPS $0xB1, Y0, Y0
    VPERMILPS $0xB1, Y1, Y9
    VADDSUBPS Y9, Y1, Y1
    VPERMILPS $0xB1, Y1, Y1
    VPERMILPS $0xB1, Y2, Y10
    VADDSUBPS Y10, Y2, Y2
    VPERMILPS $0xB1, Y2, Y2
    VPERMILPS $0xB1, Y3, Y11
    VADDSUBPS Y11, Y3, Y3
    VPERMILPS $0xB1, Y3, Y3
    VPERMILPS $0xB1, Y4, Y12
    VADDSUBPS Y12, Y4, Y4
    VPERMILPS $0xB1, Y4, Y4
    VPERMILPS $0xB1, Y5, Y13
    VADDSUBPS Y13, Y5, Y5
    VPERMILPS $0xB1, Y5, Y5
    VPERMILPS $0xB1, Y6, Y14
    VADDSUBPS Y14, Y6, Y6
    VPERMILPS $0xB1, Y6, Y6
    VPERMILPS $0xB1, Y7, Y15
    VADDSUBPS Y15, Y7, Y7
    VPERMILPS $0xB1, Y7, Y7
    // stage 2: element stride 2. Per 128-bit lane: s/d against the
    // 64-bit-swapped vector, then [s0,s1,d0,d1] via VSHUFPS.
    VPERMILPS $0x4E, Y0, Y8
    VADDPS Y8, Y0, Y9
    VSUBPS Y8, Y0, Y0
    VSHUFPS $0x44, Y0, Y9, Y0
    VPERMILPS $0x4E, Y1, Y10
    VADDPS Y10, Y1, Y11
    VSUBPS Y10, Y1, Y1
    VSHUFPS $0x44, Y1, Y11, Y1
    VPERMILPS $0x4E, Y2, Y12
    VADDPS Y12, Y2, Y13
    VSUBPS Y12, Y2, Y2
    VSHUFPS $0x44, Y2, Y13, Y2
    VPERMILPS $0x4E, Y3, Y14
    VADDPS Y14, Y3, Y15
    VSUBPS Y14, Y3, Y3
    VSHUFPS $0x44, Y3, Y15, Y3
    VPERMILPS $0x4E, Y4, Y8
    VADDPS Y8, Y4, Y9
    VSUBPS Y8, Y4, Y4
    VSHUFPS $0x44, Y4, Y9, Y4
    VPERMILPS $0x4E, Y5, Y10
    VADDPS Y10, Y5, Y11
    VSUBPS Y10, Y5, Y5
    VSHUFPS $0x44, Y5, Y11, Y5
    VPERMILPS $0x4E, Y6, Y12
    VADDPS Y12, Y6, Y13
    VSUBPS Y12, Y6, Y6
    VSHUFPS $0x44, Y6, Y13, Y6
    VPERMILPS $0x4E, Y7, Y14
    VADDPS Y14, Y7, Y15
    VSUBPS Y14, Y7, Y7
    VSHUFPS $0x44, Y7, Y15, Y7
    // stage 3: element stride 4. s/d against the 128-bit-half-swapped
    // vector, then [s.low, d.low] via VPERM2F128.
    VPERM2F128 $0x01, Y0, Y0, Y8
    VADDPS Y8, Y0, Y9
    VSUBPS Y8, Y0, Y0
    VPERM2F128 $0x20, Y0, Y9, Y0
    VPERM2F128 $0x01, Y1, Y1, Y10
    VADDPS Y10, Y1, Y11
    VSUBPS Y10, Y1, Y1
    VPERM2F128 $0x20, Y1, Y11, Y1
    VPERM2F128 $0x01, Y2, Y2, Y12
    VADDPS Y12, Y2, Y13
    VSUBPS Y12, Y2, Y2
    VPERM2F128 $0x20, Y2, Y13, Y2
    VPERM2F128 $0x01, Y3, Y3, Y14
    VADDPS Y14, Y3, Y15
    VSUBPS Y14, Y3, Y3
    VPERM2F128 $0x20, Y3, Y15, Y3
    VPERM2F128 $0x01, Y4, Y4, Y8
    VADDPS Y8, Y4, Y9
    VSUBPS Y8, Y4, Y4
    VPERM2F128 $0x20, Y4, Y9, Y4
    VPERM2F128 $0x01, Y5, Y5, Y10
    VADDPS Y10, Y5, Y11
    VSUBPS Y10, Y5, Y5
    VPERM2F128 $0x20, Y5, Y11, Y5
    VPERM2F128 $0x01, Y6, Y6, Y12
    VADDPS Y12, Y6, Y13
    VSUBPS Y12, Y6, Y6
    VPERM2F128 $0x20, Y6, Y13, Y6
    VPERM2F128 $0x01, Y7, Y7, Y14
    VADDPS Y14, Y7, Y15
    VSUBPS Y14, Y7, Y7
    VPERM2F128 $0x20, Y7, Y15, Y7
    // stage 4: element stride 8 (register pairs, bank Y0-7 -> Y8-15)
    VADDPS Y1, Y0, Y8
    VSUBPS Y1, Y0, Y9
    VADDPS Y3, Y2, Y10
    VSUBPS Y3, Y2, Y11
    VADDPS Y5, Y4, Y12
    VSUBPS Y5, Y4, Y13
    VADDPS Y7, Y6, Y14
    VSUBPS Y7, Y6, Y15
    // stage 5: element stride 16 (bank Y8-15 -> Y0-7)
    VADDPS Y10, Y8, Y0
    VSUBPS Y10, Y8, Y2
    VADDPS Y11, Y9, Y1
    VSUBPS Y11, Y9, Y3
    VADDPS Y14, Y12, Y4
    VSUBPS Y14, Y12, Y6
    VADDPS Y15, Y13, Y5
    VSUBPS Y15, Y13, Y7
    // stage 6: element stride 32 (bank Y0-7 -> Y8-15)
    VADDPS Y4, Y0, Y8
    VSUBPS Y4, Y0, Y12
    VADDPS Y5, Y1, Y9
    VSUBPS Y5, Y1, Y13
    VADDPS Y6, Y2, Y10
    VSUBPS Y6, Y2, Y14
    VADDPS Y7, Y3, Y11
    VSUBPS Y7, Y3, Y15
    VMOVUPS Y8, 0(DI)
    VMOVUPS Y9, 32(DI)
    VMOVUPS Y10, 64(DI)
    VMOVUPS Y11, 96(DI)
    VMOVUPS Y12, 128(DI)
    VMOVUPS Y13, 160(DI)
    VMOVUPS Y14, 192(DI)
    VMOVUPS Y15, 224(DI)
    ADDQ $256, DI
    DECQ CX
    JNZ fwht256_block

    // Combine stages: butterflies at element strides 64 and 128, fused as
    // a radix-4 pass over quadruples (x[i], x[i+64], x[i+128], x[i+192]).
    MOVQ x+0(FP), SI
    MOVQ $8, CX

fwht256_combine:
    VMOVUPS (SI), Y0
    VMOVUPS 256(SI), Y1
    VMOVUPS 512(SI), Y2
    VMOVUPS 768(SI), Y3
    VADDPS Y1, Y0, Y4          // t0 = a+b
    VSUBPS Y1, Y0, Y5          // t1 = a-b
    VADDPS Y3, Y2, Y6          // t2 = c+d
    VSUBPS Y3, Y2, Y7          // t3 = c-d
    VADDPS Y6, Y4, Y0          // a' = t0+t2
    VSUBPS Y6, Y4, Y2          // c' = t0-t2
    VADDPS Y7, Y5, Y1          // b' = t1+t3
    VSUBPS Y7, Y5, Y3          // d' = t1-t3
    VMOVUPS Y0, (SI)
    VMOVUPS Y1, 256(SI)
    VMOVUPS Y2, 512(SI)
    VMOVUPS Y3, 768(SI)
    ADDQ $32, SI
    DECQ CX
    JNZ fwht256_combine
    VZEROUPPER
    RET

