//go:build arm64

#include "textflag.h"

// Fused quantize+correlate NEON kernel for the 4-bit rotational quantizer's
// encode-time interval search. One pass replaces five separate sweeps
// (quantize, int->float, sum, sum-of-squares, dot): the codes and all three
// reductions are produced while the input is still in registers.
//
// Quantization semantics are bit-identical to
// tphakala/simd f32.Float32ToInt32ScaleClamp(ci, xs, invStep, offset, 0, 15):
// two separate float32 roundings (FMUL then FADD, never FMLA), FMAX/FMIN
// clamp, truncating FCVTZS conversion. NaN inputs quantize to code 0
// (FMAX/FMIN propagate the NaN and FCVTZS converts NaN to 0, matching the
// library and the pure Go reference rq4QuantCorrGo).
//
// sumC and sumC2 accumulate as int32 (exact; max 15*n and 225*n stay far
// below 2^31 for any supported dimension). sumXC accumulates as float32 in
// 16 lanes (4 vector accumulators), reduced pairwise:
// ((acc0+acc1)+(acc2+acc3)) then FADDP. n must be a multiple of 16; the Go
// wrapper handles the scalar tail.
//
// FMUL   Vd.4S, Vn.4S, Vm.4S: 0x6E20DC00 | Vm<<16 | Vn<<5 | Vd
// FADD   Vd.4S, Vn.4S, Vm.4S: 0x4E20D400 | Vm<<16 | Vn<<5 | Vd
// FMAX   Vd.4S, Vn.4S, Vm.4S: 0x4E20F400 | Vm<<16 | Vn<<5 | Vd
// FMIN   Vd.4S, Vn.4S, Vm.4S: 0x4EA0F400 | Vm<<16 | Vn<<5 | Vd
// FMLA   Vd.4S, Vn.4S, Vm.4S: 0x4E20CC00 | Vm<<16 | Vn<<5 | Vd
// FCVTZS Vd.4S, Vn.4S:        0x4EA1B800 | Vn<<5 | Vd
// SCVTF  Vd.4S, Vn.4S:        0x4E21D800 | Vn<<5 | Vd
// ADD    Vd.4S, Vn.4S, Vm.4S: 0x4EA08400 | Vm<<16 | Vn<<5 | Vd
// MLA    Vd.4S, Vn.4S, Vm.4S: 0x4EA09400 | Vm<<16 | Vn<<5 | Vd
// FADDP  Vd.4S, Vn.4S, Vm.4S: 0x6E20D400 | Vm<<16 | Vn<<5 | Vd
// FADDP  Sd, Vn.2S:           0x7E30D800 | Vn<<5 | Vd
// ADDV   Sd, Vn.4S:           0x4EB1B800 | Vn<<5 | Vd
// DUP    Vd.4S, Vn.S[0]:      0x4E040400 | Vn<<5 | Vd
// FMOV   Wd, Sn:              0x1E260000 | Sn<<5 | Wd

// func rq4QuantCorrAsm(xs *float32, ci *int32, n int, invStep, offset float32) (sumXC float32, sumC int32, sumC2 int32)
TEXT ·rq4QuantCorrAsm(SB), NOSPLIT, $0-44
    MOVD xs+0(FP), R0
    MOVD ci+8(FP), R1
    MOVD n+16(FP), R2
    FMOVS invStep+24(FP), F28
    FMOVS offset+28(FP), F29
    WORD $0x4E04079C           // DUP V28.4S, V28.S[0]
    WORD $0x4E0407BD           // DUP V29.4S, V29.S[0]
    VEOR V30.B16, V30.B16, V30.B16    // 0.0 (clamp lower bound)
    MOVD $0x41700000, R3               // 15.0f (clamp upper bound)
    FMOVS R3, F31
    WORD $0x4E0407FF           // DUP V31.4S, V31.S[0]
    VEOR V0.B16, V0.B16, V0.B16
    VEOR V1.B16, V1.B16, V1.B16
    VEOR V2.B16, V2.B16, V2.B16
    VEOR V3.B16, V3.B16, V3.B16
    VEOR V4.B16, V4.B16, V4.B16
    VEOR V5.B16, V5.B16, V5.B16
    VEOR V6.B16, V6.B16, V6.B16
    VEOR V7.B16, V7.B16, V7.B16
    LSR $4, R2, R4                     // R4 = n/16 iterations
    CBZ R4, qc_reduce

qc_loop:
    VLD1.P 64(R0), [V16.S4, V17.S4, V18.S4, V19.S4]
    WORD $0x6E3CDE14           // FMUL V20.4S, V16.4S, V28.4S
    WORD $0x6E3CDE35           // FMUL V21.4S, V17.4S, V28.4S
    WORD $0x6E3CDE56           // FMUL V22.4S, V18.4S, V28.4S
    WORD $0x6E3CDE77           // FMUL V23.4S, V19.4S, V28.4S
    WORD $0x4E3DD694           // FADD V20.4S, V20.4S, V29.4S
    WORD $0x4E3DD6B5           // FADD V21.4S, V21.4S, V29.4S
    WORD $0x4E3DD6D6           // FADD V22.4S, V22.4S, V29.4S
    WORD $0x4E3DD6F7           // FADD V23.4S, V23.4S, V29.4S
    WORD $0x4E3EF694           // FMAX V20.4S, V20.4S, V30.4S
    WORD $0x4E3EF6B5           // FMAX V21.4S, V21.4S, V30.4S
    WORD $0x4E3EF6D6           // FMAX V22.4S, V22.4S, V30.4S
    WORD $0x4E3EF6F7           // FMAX V23.4S, V23.4S, V30.4S
    WORD $0x4EBFF694           // FMIN V20.4S, V20.4S, V31.4S
    WORD $0x4EBFF6B5           // FMIN V21.4S, V21.4S, V31.4S
    WORD $0x4EBFF6D6           // FMIN V22.4S, V22.4S, V31.4S
    WORD $0x4EBFF6F7           // FMIN V23.4S, V23.4S, V31.4S
    WORD $0x4EA1BA94           // FCVTZS V20.4S, V20.4S
    WORD $0x4EA1BAB5           // FCVTZS V21.4S, V21.4S
    WORD $0x4EA1BAD6           // FCVTZS V22.4S, V22.4S
    WORD $0x4EA1BAF7           // FCVTZS V23.4S, V23.4S
    VST1.P [V20.S4, V21.S4, V22.S4, V23.S4], 64(R1)
    WORD $0x4EB48484           // ADD V4.4S, V4.4S, V20.4S
    WORD $0x4EB584A5           // ADD V5.4S, V5.4S, V21.4S
    WORD $0x4EB49686           // MLA V6.4S, V20.4S, V20.4S
    WORD $0x4EB596A7           // MLA V7.4S, V21.4S, V21.4S
    WORD $0x4EB68484           // ADD V4.4S, V4.4S, V22.4S
    WORD $0x4EB784A5           // ADD V5.4S, V5.4S, V23.4S
    WORD $0x4EB696C6           // MLA V6.4S, V22.4S, V22.4S
    WORD $0x4EB796E7           // MLA V7.4S, V23.4S, V23.4S
    WORD $0x4E21DA94           // SCVTF V20.4S, V20.4S
    WORD $0x4E21DAB5           // SCVTF V21.4S, V21.4S
    WORD $0x4E21DAD6           // SCVTF V22.4S, V22.4S
    WORD $0x4E21DAF7           // SCVTF V23.4S, V23.4S
    WORD $0x4E34CE00           // FMLA V0.4S, V16.4S, V20.4S
    WORD $0x4E35CE21           // FMLA V1.4S, V17.4S, V21.4S
    WORD $0x4E36CE42           // FMLA V2.4S, V18.4S, V22.4S
    WORD $0x4E37CE63           // FMLA V3.4S, V19.4S, V23.4S
    SUB $1, R4
    CBNZ R4, qc_loop

qc_reduce:
    WORD $0x4E21D400           // FADD V0.4S, V0.4S, V1.4S
    WORD $0x4E23D442           // FADD V2.4S, V2.4S, V3.4S
    WORD $0x4E22D400           // FADD V0.4S, V0.4S, V2.4S
    WORD $0x6E20D400           // FADDP V0.4S, V0.4S, V0.4S
    WORD $0x7E30D800           // FADDP S0, V0.2S
    FMOVS F0, sumXC+32(FP)
    WORD $0x4EA58484           // ADD V4.4S, V4.4S, V5.4S
    WORD $0x4EB1B884           // ADDV S4, V4.4S
    WORD $0x1E260083           // FMOV W3, S4
    MOVW R3, sumC+36(FP)
    WORD $0x4EA784C6           // ADD V6.4S, V6.4S, V7.4S
    WORD $0x4EB1B8C6           // ADDV S6, V6.4S
    WORD $0x1E2600C3           // FMOV W3, S6
    MOVW R3, sumC2+40(FP)
    RET

// Fused min/max/sum kernel: one sweep replaces the separate f32.Min,
// f32.Max and f32.Sum passes of the interval search. min/max are exact
// (order-independent); the sum accumulates in 16 lanes reduced pairwise
// like the correlation kernel. n must be a multiple of 16 and >= 16;
// the Go wrapper handles tails and short inputs.
//
// Additional opcode formulas:
//   FMINV Sd, Vn.4S:  0x6EB0F800 | Vn<<5 | Vd
//   FMAXV Sd, Vn.4S:  0x6E30F800 | Vn<<5 | Vd
//   FMIN  Sd, Sn, Sm: 0x1E205C00 | Sm<<16 | Sn<<5 | Sd
//   FMAX  Sd, Sn, Sm: 0x1E204C00 | Sm<<16 | Sn<<5 | Sd

// func rq4MinMaxSumAsm(xs *float32, n int) (minV, maxV, sum float32)
TEXT ·rq4MinMaxSumAsm(SB), NOSPLIT, $0-28
    MOVD xs+0(FP), R0
    MOVD n+8(FP), R2
    // Seed the min/max accumulators with the first block so no
    // sentinel values are needed.
    VLD1 (R0), [V16.S4, V17.S4, V18.S4, V19.S4]
    VMOV V16.B16, V20.B16
    VMOV V18.B16, V22.B16
    VMOV V17.B16, V21.B16
    VMOV V19.B16, V23.B16
    VEOR V24.B16, V24.B16, V24.B16
    VEOR V25.B16, V25.B16, V25.B16
    VEOR V26.B16, V26.B16, V26.B16
    VEOR V27.B16, V27.B16, V27.B16
    LSR $4, R2, R4                     // R4 = n/16 iterations

mms_loop:
    VLD1.P 64(R0), [V0.S4, V1.S4, V2.S4, V3.S4]
    WORD $0x4EA0F694           // FMIN V20.4S, V20.4S, V0.4S
    WORD $0x4EA1F6B5           // FMIN V21.4S, V21.4S, V1.4S
    WORD $0x4EA2F694           // FMIN V20.4S, V20.4S, V2.4S
    WORD $0x4EA3F6B5           // FMIN V21.4S, V21.4S, V3.4S
    WORD $0x4E20F6D6           // FMAX V22.4S, V22.4S, V0.4S
    WORD $0x4E21F6F7           // FMAX V23.4S, V23.4S, V1.4S
    WORD $0x4E22F6D6           // FMAX V22.4S, V22.4S, V2.4S
    WORD $0x4E23F6F7           // FMAX V23.4S, V23.4S, V3.4S
    WORD $0x4E20D718           // FADD V24.4S, V24.4S, V0.4S
    WORD $0x4E21D739           // FADD V25.4S, V25.4S, V1.4S
    WORD $0x4E22D75A           // FADD V26.4S, V26.4S, V2.4S
    WORD $0x4E23D77B           // FADD V27.4S, V27.4S, V3.4S
    SUB $1, R4
    CBNZ R4, mms_loop

    WORD $0x4EB5F694           // FMIN V20.4S, V20.4S, V21.4S
    WORD $0x6EB0FA80           // FMINV S0, V20.4S
    FMOVS F0, minV+16(FP)
    WORD $0x4E37F6D6           // FMAX V22.4S, V22.4S, V23.4S
    WORD $0x6E30FAC1           // FMAXV S1, V22.4S
    FMOVS F1, maxV+20(FP)
    WORD $0x4E39D718           // FADD V24.4S, V24.4S, V25.4S
    WORD $0x4E3BD75A           // FADD V26.4S, V26.4S, V27.4S
    WORD $0x4E3AD718           // FADD V24.4S, V24.4S, V26.4S
    WORD $0x6E38D718           // FADDP V24.4S, V24.4S, V24.4S
    WORD $0x7E30DB02           // FADDP S2, V24.2S
    FMOVS F2, sum+24(FP)
    RET
