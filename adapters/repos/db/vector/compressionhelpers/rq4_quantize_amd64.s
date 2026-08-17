//go:build amd64

#include "textflag.h"

// Fused quantize+correlate AVX2 kernel for the 4-bit rotational quantizer's
// encode-time interval search. See rq4_quantize_arm64.s for the algorithm;
// this is the amd64 twin. Quantization semantics are bit-identical to the
// pure Go reference: VMULPS then VADDPS (two roundings, never FMA),
// VMAXPS/VMINPS clamp with the value in the first-source position so a NaN
// yields the bound (0), truncating VCVTTPS2DQ conversion.
//
// sumC/sumC2 accumulate as int32 lanes (exact). sumXC accumulates in 16
// float32 lanes (two YMM accumulators); the reduction order is fixed but
// differs from the arm64 kernel, so sumXC may differ in the last ulps across
// architectures (sumC/sumC2 and the codes are identical).
//
// n must be a multiple of 16; the Go wrapper handles the tail.

// func rq4QuantCorrAsm(xs *float32, ci *int32, n int, invStep, offset float32) (sumXC float32, sumC int32, sumC2 int32)
TEXT ·rq4QuantCorrAsm(SB), NOSPLIT, $0-44
    MOVQ xs+0(FP), SI
    MOVQ ci+8(FP), DI
    MOVQ n+16(FP), CX
    VBROADCASTSS invStep+24(FP), Y12
    VBROADCASTSS offset+28(FP), Y13
    VXORPS Y14, Y14, Y14               // 0.0 (clamp lower bound)
    MOVL $0x41700000, AX               // 15.0f (clamp upper bound)
    MOVL AX, X15
    VBROADCASTSS X15, Y15
    VXORPS Y0, Y0, Y0                  // sumXC accumulator lanes 0-7
    VXORPS Y1, Y1, Y1                  // sumXC accumulator lanes 8-15
    VPXOR Y2, Y2, Y2                   // sumC accumulator lanes 0-7
    VPXOR Y3, Y3, Y3                   // sumC accumulator lanes 8-15
    VPXOR Y4, Y4, Y4                   // sumC2 accumulator lanes 0-7
    VPXOR Y5, Y5, Y5                   // sumC2 accumulator lanes 8-15
    SHRQ $4, CX                        // CX = n/16 iterations
    JZ qc_reduce

qc_loop:
    VMOVUPS (SI), Y6
    VMOVUPS 32(SI), Y7
    ADDQ $64, SI
    VMULPS Y12, Y6, Y8                 // t = x*invStep (rounded)
    VMULPS Y12, Y7, Y9
    VADDPS Y13, Y8, Y8                 // t += offset (rounded)
    VADDPS Y13, Y9, Y9
    VMAXPS Y14, Y8, Y8                 // clamp low; NaN -> 0 (t is SRC1)
    VMAXPS Y14, Y9, Y9
    VMINPS Y15, Y8, Y8                 // clamp high
    VMINPS Y15, Y9, Y9
    VCVTTPS2DQ Y8, Y8                  // c = int32(t), truncating
    VCVTTPS2DQ Y9, Y9
    VMOVDQU Y8, (DI)
    VMOVDQU Y9, 32(DI)
    ADDQ $64, DI
    VPADDD Y8, Y2, Y2                  // sumC += c
    VPADDD Y9, Y3, Y3
    VPMULLD Y8, Y8, Y10                // c*c
    VPMULLD Y9, Y9, Y11
    VPADDD Y10, Y4, Y4                 // sumC2 += c*c
    VPADDD Y11, Y5, Y5
    VCVTDQ2PS Y8, Y8                   // cf = float32(c)
    VCVTDQ2PS Y9, Y9
    VMULPS Y8, Y6, Y8                  // x*cf (rounded)
    VMULPS Y9, Y7, Y9
    VADDPS Y8, Y0, Y0                  // sumXC += x*cf (rounded)
    VADDPS Y9, Y1, Y1
    DECQ CX
    JNZ qc_loop

qc_reduce:
    // sumXC: (lanes0-7 + lanes8-15), then horizontal.
    VADDPS Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS X1, X0, X0
    VHADDPS X0, X0, X0
    VHADDPS X0, X0, X0
    VMOVSS X0, sumXC+32(FP)
    // sumC
    VPADDD Y3, Y2, Y2
    VEXTRACTI128 $1, Y2, X3
    VPADDD X3, X2, X2
    VPSHUFD $0x4E, X2, X3
    VPADDD X3, X2, X2
    VPSHUFD $0xB1, X2, X3
    VPADDD X3, X2, X2
    VMOVD X2, AX
    MOVL AX, sumC+36(FP)
    // sumC2
    VPADDD Y5, Y4, Y4
    VEXTRACTI128 $1, Y4, X5
    VPADDD X5, X4, X4
    VPSHUFD $0x4E, X4, X5
    VPADDD X5, X4, X4
    VPSHUFD $0xB1, X4, X5
    VPADDD X5, X4, X4
    VMOVD X4, AX
    MOVL AX, sumC2+40(FP)
    VZEROUPPER
    RET

// Fused min/max/sum kernel: one sweep replaces the separate f32.Min, f32.Max
// and f32.Sum passes of the interval search. min/max are exact
// (order-independent); the sum accumulates in 16 float32 lanes. n must be a
// multiple of 16 and >= 16; the Go wrapper handles tails and short inputs.
//
// func rq4MinMaxSumAsm(xs *float32, n int) (minV, maxV, sum float32)
TEXT ·rq4MinMaxSumAsm(SB), NOSPLIT, $0-28
    MOVQ xs+0(FP), SI
    MOVQ n+8(FP), CX
    // Seed the min/max accumulators with the first block so no sentinel
    // values are needed.
    VMOVUPS (SI), Y2
    VMOVUPS 32(SI), Y3
    VMOVAPS Y2, Y4                     // min accumulator lanes 0-7
    VMOVAPS Y3, Y5                     // min accumulator lanes 8-15
    VMOVAPS Y2, Y6                     // max accumulator lanes 0-7
    VMOVAPS Y3, Y7                     // max accumulator lanes 8-15
    VXORPS Y0, Y0, Y0                  // sum accumulator lanes 0-7
    VXORPS Y1, Y1, Y1                  // sum accumulator lanes 8-15
    SHRQ $4, CX                        // CX = n/16 iterations

mms_loop:
    VMOVUPS (SI), Y2
    VMOVUPS 32(SI), Y3
    ADDQ $64, SI
    VMINPS Y2, Y4, Y4
    VMINPS Y3, Y5, Y5
    VMAXPS Y2, Y6, Y6
    VMAXPS Y3, Y7, Y7
    VADDPS Y2, Y0, Y0
    VADDPS Y3, Y1, Y1
    DECQ CX
    JNZ mms_loop

    // min
    VMINPS Y5, Y4, Y4
    VEXTRACTF128 $1, Y4, X5
    VMINPS X5, X4, X4
    VPERMILPS $0x4E, X4, X5
    VMINPS X5, X4, X4
    VPERMILPS $0xB1, X4, X5
    VMINPS X5, X4, X4
    VMOVSS X4, minV+16(FP)
    // max
    VMAXPS Y7, Y6, Y6
    VEXTRACTF128 $1, Y6, X7
    VMAXPS X7, X6, X6
    VPERMILPS $0x4E, X6, X7
    VMAXPS X7, X6, X6
    VPERMILPS $0xB1, X6, X7
    VMAXPS X7, X6, X6
    VMOVSS X6, maxV+20(FP)
    // sum
    VADDPS Y1, Y0, Y0
    VEXTRACTF128 $1, Y0, X1
    VADDPS X1, X0, X0
    VHADDPS X0, X0, X0
    VHADDPS X0, X0, X0
    VMOVSS X0, sum+24(FP)
    VZEROUPPER
    RET
