//go:build arm64

#include "textflag.h"

// Hand-written NEON implementation of the fast Walsh-Hadamard transform
// used by the fast random rotation. A 64-point transform fits entirely in
// NEON registers (64 float32 = 16 x 128-bit vectors): lane-level butterflies
// (element strides 1 and 2) use REV64/EXT shuffles with TRN1/ZIP1 lane
// merges, and the remaining stages are plain cross-register FADD/FSUB
// ping-ponging between V0-V15 and V16-V31. The butterfly ordering matches
// the pure Go implementation exactly, so results are bit-identical.
//
// Vector float instructions are WORD-encoded because Go's arm64 assembler
// does not support NEON float mnemonics (same approach as tphakala/simd):
//   FADD  Vd.4S, Vn.4S, Vm.4S:      0x4E20D400 | Vm<<16 | Vn<<5 | Vd
//   FSUB  Vd.4S, Vn.4S, Vm.4S:      0x4EA0D400 | Vm<<16 | Vn<<5 | Vd
//   FMUL  Vd.4S, Vn.4S, Vm.4S:      0x6E20DC00 | Vm<<16 | Vn<<5 | Vd
//   REV64 Vd.4S, Vn.4S:             0x4EA00800 | Vn<<5 | Vd
//   TRN1  Vd.4S, Vn.4S, Vm.4S:      0x4E802800 | Vm<<16 | Vn<<5 | Vd
//   EXT   Vd.16B, Vn.16B, Vm.16B,#8: 0x6E000000 | Vm<<16 | 8<<11 | Vn<<5 | Vd
//   ZIP1  Vd.2D, Vn.2D, Vm.2D:      0x4EC03800 | Vm<<16 | Vn<<5 | Vd
//   DUP   Vd.4S, Vn.S[0]:           0x4E040400 | Vn<<5 | Vd

// func fwht64NEON(x *float32)
// 64-point FWHT with 1/8 normalization, x must have 64 readable floats.
TEXT ·fwht64NEON(SB), NOSPLIT, $0-8
    MOVD x+0(FP), R0
    MOVD R0, R1
    FMOVS $(0.125), F31
    WORD $0x4E0407FF           // DUP V31.4S, V31.S[0]
    VLD1.P 64(R1), [V0.S4, V1.S4, V2.S4, V3.S4]
    VLD1.P 64(R1), [V4.S4, V5.S4, V6.S4, V7.S4]
    VLD1.P 64(R1), [V8.S4, V9.S4, V10.S4, V11.S4]
    VLD1.P 64(R1), [V12.S4, V13.S4, V14.S4, V15.S4]
    WORD $0x6E3FDC00           // FMUL V0.4S, V0.4S, V31.4S
    WORD $0x6E3FDC21           // FMUL V1.4S, V1.4S, V31.4S
    WORD $0x6E3FDC42           // FMUL V2.4S, V2.4S, V31.4S
    WORD $0x6E3FDC63           // FMUL V3.4S, V3.4S, V31.4S
    WORD $0x6E3FDC84           // FMUL V4.4S, V4.4S, V31.4S
    WORD $0x6E3FDCA5           // FMUL V5.4S, V5.4S, V31.4S
    WORD $0x6E3FDCC6           // FMUL V6.4S, V6.4S, V31.4S
    WORD $0x6E3FDCE7           // FMUL V7.4S, V7.4S, V31.4S
    WORD $0x6E3FDD08           // FMUL V8.4S, V8.4S, V31.4S
    WORD $0x6E3FDD29           // FMUL V9.4S, V9.4S, V31.4S
    WORD $0x6E3FDD4A           // FMUL V10.4S, V10.4S, V31.4S
    WORD $0x6E3FDD6B           // FMUL V11.4S, V11.4S, V31.4S
    WORD $0x6E3FDD8C           // FMUL V12.4S, V12.4S, V31.4S
    WORD $0x6E3FDDAD           // FMUL V13.4S, V13.4S, V31.4S
    WORD $0x6E3FDDCE           // FMUL V14.4S, V14.4S, V31.4S
    WORD $0x6E3FDDEF           // FMUL V15.4S, V15.4S, V31.4S
    WORD $0x4EA00810           // REV64 V16.4S, V0.4S
    WORD $0x4E30D411           // FADD V17.4S, V0.4S, V16.4S
    WORD $0x4EB0D412           // FSUB V18.4S, V0.4S, V16.4S
    WORD $0x4E922A20           // TRN1 V0.4S, V17.4S, V18.4S
    WORD $0x6E004010           // EXT V16.16B, V0.16B, V0.16B, #8
    WORD $0x4E30D411           // FADD V17.4S, V0.4S, V16.4S
    WORD $0x4EB0D412           // FSUB V18.4S, V0.4S, V16.4S
    WORD $0x4ED23A20           // ZIP1 V0.2D, V17.2D, V18.2D
    WORD $0x4EA00833           // REV64 V19.4S, V1.4S
    WORD $0x4E33D434           // FADD V20.4S, V1.4S, V19.4S
    WORD $0x4EB3D435           // FSUB V21.4S, V1.4S, V19.4S
    WORD $0x4E952A81           // TRN1 V1.4S, V20.4S, V21.4S
    WORD $0x6E014033           // EXT V19.16B, V1.16B, V1.16B, #8
    WORD $0x4E33D434           // FADD V20.4S, V1.4S, V19.4S
    WORD $0x4EB3D435           // FSUB V21.4S, V1.4S, V19.4S
    WORD $0x4ED53A81           // ZIP1 V1.2D, V20.2D, V21.2D
    WORD $0x4EA00856           // REV64 V22.4S, V2.4S
    WORD $0x4E36D457           // FADD V23.4S, V2.4S, V22.4S
    WORD $0x4EB6D458           // FSUB V24.4S, V2.4S, V22.4S
    WORD $0x4E982AE2           // TRN1 V2.4S, V23.4S, V24.4S
    WORD $0x6E024056           // EXT V22.16B, V2.16B, V2.16B, #8
    WORD $0x4E36D457           // FADD V23.4S, V2.4S, V22.4S
    WORD $0x4EB6D458           // FSUB V24.4S, V2.4S, V22.4S
    WORD $0x4ED83AE2           // ZIP1 V2.2D, V23.2D, V24.2D
    WORD $0x4EA00879           // REV64 V25.4S, V3.4S
    WORD $0x4E39D47A           // FADD V26.4S, V3.4S, V25.4S
    WORD $0x4EB9D47B           // FSUB V27.4S, V3.4S, V25.4S
    WORD $0x4E9B2B43           // TRN1 V3.4S, V26.4S, V27.4S
    WORD $0x6E034079           // EXT V25.16B, V3.16B, V3.16B, #8
    WORD $0x4E39D47A           // FADD V26.4S, V3.4S, V25.4S
    WORD $0x4EB9D47B           // FSUB V27.4S, V3.4S, V25.4S
    WORD $0x4EDB3B43           // ZIP1 V3.2D, V26.2D, V27.2D
    WORD $0x4EA00890           // REV64 V16.4S, V4.4S
    WORD $0x4E30D491           // FADD V17.4S, V4.4S, V16.4S
    WORD $0x4EB0D492           // FSUB V18.4S, V4.4S, V16.4S
    WORD $0x4E922A24           // TRN1 V4.4S, V17.4S, V18.4S
    WORD $0x6E044090           // EXT V16.16B, V4.16B, V4.16B, #8
    WORD $0x4E30D491           // FADD V17.4S, V4.4S, V16.4S
    WORD $0x4EB0D492           // FSUB V18.4S, V4.4S, V16.4S
    WORD $0x4ED23A24           // ZIP1 V4.2D, V17.2D, V18.2D
    WORD $0x4EA008B3           // REV64 V19.4S, V5.4S
    WORD $0x4E33D4B4           // FADD V20.4S, V5.4S, V19.4S
    WORD $0x4EB3D4B5           // FSUB V21.4S, V5.4S, V19.4S
    WORD $0x4E952A85           // TRN1 V5.4S, V20.4S, V21.4S
    WORD $0x6E0540B3           // EXT V19.16B, V5.16B, V5.16B, #8
    WORD $0x4E33D4B4           // FADD V20.4S, V5.4S, V19.4S
    WORD $0x4EB3D4B5           // FSUB V21.4S, V5.4S, V19.4S
    WORD $0x4ED53A85           // ZIP1 V5.2D, V20.2D, V21.2D
    WORD $0x4EA008D6           // REV64 V22.4S, V6.4S
    WORD $0x4E36D4D7           // FADD V23.4S, V6.4S, V22.4S
    WORD $0x4EB6D4D8           // FSUB V24.4S, V6.4S, V22.4S
    WORD $0x4E982AE6           // TRN1 V6.4S, V23.4S, V24.4S
    WORD $0x6E0640D6           // EXT V22.16B, V6.16B, V6.16B, #8
    WORD $0x4E36D4D7           // FADD V23.4S, V6.4S, V22.4S
    WORD $0x4EB6D4D8           // FSUB V24.4S, V6.4S, V22.4S
    WORD $0x4ED83AE6           // ZIP1 V6.2D, V23.2D, V24.2D
    WORD $0x4EA008F9           // REV64 V25.4S, V7.4S
    WORD $0x4E39D4FA           // FADD V26.4S, V7.4S, V25.4S
    WORD $0x4EB9D4FB           // FSUB V27.4S, V7.4S, V25.4S
    WORD $0x4E9B2B47           // TRN1 V7.4S, V26.4S, V27.4S
    WORD $0x6E0740F9           // EXT V25.16B, V7.16B, V7.16B, #8
    WORD $0x4E39D4FA           // FADD V26.4S, V7.4S, V25.4S
    WORD $0x4EB9D4FB           // FSUB V27.4S, V7.4S, V25.4S
    WORD $0x4EDB3B47           // ZIP1 V7.2D, V26.2D, V27.2D
    WORD $0x4EA00910           // REV64 V16.4S, V8.4S
    WORD $0x4E30D511           // FADD V17.4S, V8.4S, V16.4S
    WORD $0x4EB0D512           // FSUB V18.4S, V8.4S, V16.4S
    WORD $0x4E922A28           // TRN1 V8.4S, V17.4S, V18.4S
    WORD $0x6E084110           // EXT V16.16B, V8.16B, V8.16B, #8
    WORD $0x4E30D511           // FADD V17.4S, V8.4S, V16.4S
    WORD $0x4EB0D512           // FSUB V18.4S, V8.4S, V16.4S
    WORD $0x4ED23A28           // ZIP1 V8.2D, V17.2D, V18.2D
    WORD $0x4EA00933           // REV64 V19.4S, V9.4S
    WORD $0x4E33D534           // FADD V20.4S, V9.4S, V19.4S
    WORD $0x4EB3D535           // FSUB V21.4S, V9.4S, V19.4S
    WORD $0x4E952A89           // TRN1 V9.4S, V20.4S, V21.4S
    WORD $0x6E094133           // EXT V19.16B, V9.16B, V9.16B, #8
    WORD $0x4E33D534           // FADD V20.4S, V9.4S, V19.4S
    WORD $0x4EB3D535           // FSUB V21.4S, V9.4S, V19.4S
    WORD $0x4ED53A89           // ZIP1 V9.2D, V20.2D, V21.2D
    WORD $0x4EA00956           // REV64 V22.4S, V10.4S
    WORD $0x4E36D557           // FADD V23.4S, V10.4S, V22.4S
    WORD $0x4EB6D558           // FSUB V24.4S, V10.4S, V22.4S
    WORD $0x4E982AEA           // TRN1 V10.4S, V23.4S, V24.4S
    WORD $0x6E0A4156           // EXT V22.16B, V10.16B, V10.16B, #8
    WORD $0x4E36D557           // FADD V23.4S, V10.4S, V22.4S
    WORD $0x4EB6D558           // FSUB V24.4S, V10.4S, V22.4S
    WORD $0x4ED83AEA           // ZIP1 V10.2D, V23.2D, V24.2D
    WORD $0x4EA00979           // REV64 V25.4S, V11.4S
    WORD $0x4E39D57A           // FADD V26.4S, V11.4S, V25.4S
    WORD $0x4EB9D57B           // FSUB V27.4S, V11.4S, V25.4S
    WORD $0x4E9B2B4B           // TRN1 V11.4S, V26.4S, V27.4S
    WORD $0x6E0B4179           // EXT V25.16B, V11.16B, V11.16B, #8
    WORD $0x4E39D57A           // FADD V26.4S, V11.4S, V25.4S
    WORD $0x4EB9D57B           // FSUB V27.4S, V11.4S, V25.4S
    WORD $0x4EDB3B4B           // ZIP1 V11.2D, V26.2D, V27.2D
    WORD $0x4EA00990           // REV64 V16.4S, V12.4S
    WORD $0x4E30D591           // FADD V17.4S, V12.4S, V16.4S
    WORD $0x4EB0D592           // FSUB V18.4S, V12.4S, V16.4S
    WORD $0x4E922A2C           // TRN1 V12.4S, V17.4S, V18.4S
    WORD $0x6E0C4190           // EXT V16.16B, V12.16B, V12.16B, #8
    WORD $0x4E30D591           // FADD V17.4S, V12.4S, V16.4S
    WORD $0x4EB0D592           // FSUB V18.4S, V12.4S, V16.4S
    WORD $0x4ED23A2C           // ZIP1 V12.2D, V17.2D, V18.2D
    WORD $0x4EA009B3           // REV64 V19.4S, V13.4S
    WORD $0x4E33D5B4           // FADD V20.4S, V13.4S, V19.4S
    WORD $0x4EB3D5B5           // FSUB V21.4S, V13.4S, V19.4S
    WORD $0x4E952A8D           // TRN1 V13.4S, V20.4S, V21.4S
    WORD $0x6E0D41B3           // EXT V19.16B, V13.16B, V13.16B, #8
    WORD $0x4E33D5B4           // FADD V20.4S, V13.4S, V19.4S
    WORD $0x4EB3D5B5           // FSUB V21.4S, V13.4S, V19.4S
    WORD $0x4ED53A8D           // ZIP1 V13.2D, V20.2D, V21.2D
    WORD $0x4EA009D6           // REV64 V22.4S, V14.4S
    WORD $0x4E36D5D7           // FADD V23.4S, V14.4S, V22.4S
    WORD $0x4EB6D5D8           // FSUB V24.4S, V14.4S, V22.4S
    WORD $0x4E982AEE           // TRN1 V14.4S, V23.4S, V24.4S
    WORD $0x6E0E41D6           // EXT V22.16B, V14.16B, V14.16B, #8
    WORD $0x4E36D5D7           // FADD V23.4S, V14.4S, V22.4S
    WORD $0x4EB6D5D8           // FSUB V24.4S, V14.4S, V22.4S
    WORD $0x4ED83AEE           // ZIP1 V14.2D, V23.2D, V24.2D
    WORD $0x4EA009F9           // REV64 V25.4S, V15.4S
    WORD $0x4E39D5FA           // FADD V26.4S, V15.4S, V25.4S
    WORD $0x4EB9D5FB           // FSUB V27.4S, V15.4S, V25.4S
    WORD $0x4E9B2B4F           // TRN1 V15.4S, V26.4S, V27.4S
    WORD $0x6E0F41F9           // EXT V25.16B, V15.16B, V15.16B, #8
    WORD $0x4E39D5FA           // FADD V26.4S, V15.4S, V25.4S
    WORD $0x4EB9D5FB           // FSUB V27.4S, V15.4S, V25.4S
    WORD $0x4EDB3B4F           // ZIP1 V15.2D, V26.2D, V27.2D
    // stage 3: element stride 4 (vector pairs stride 1)
    WORD $0x4E21D410           // FADD V16.4S, V0.4S, V1.4S
    WORD $0x4EA1D411           // FSUB V17.4S, V0.4S, V1.4S
    WORD $0x4E23D452           // FADD V18.4S, V2.4S, V3.4S
    WORD $0x4EA3D453           // FSUB V19.4S, V2.4S, V3.4S
    WORD $0x4E25D494           // FADD V20.4S, V4.4S, V5.4S
    WORD $0x4EA5D495           // FSUB V21.4S, V4.4S, V5.4S
    WORD $0x4E27D4D6           // FADD V22.4S, V6.4S, V7.4S
    WORD $0x4EA7D4D7           // FSUB V23.4S, V6.4S, V7.4S
    WORD $0x4E29D518           // FADD V24.4S, V8.4S, V9.4S
    WORD $0x4EA9D519           // FSUB V25.4S, V8.4S, V9.4S
    WORD $0x4E2BD55A           // FADD V26.4S, V10.4S, V11.4S
    WORD $0x4EABD55B           // FSUB V27.4S, V10.4S, V11.4S
    WORD $0x4E2DD59C           // FADD V28.4S, V12.4S, V13.4S
    WORD $0x4EADD59D           // FSUB V29.4S, V12.4S, V13.4S
    WORD $0x4E2FD5DE           // FADD V30.4S, V14.4S, V15.4S
    WORD $0x4EAFD5DF           // FSUB V31.4S, V14.4S, V15.4S
    // stage 4: element stride 8
    WORD $0x4E32D600           // FADD V0.4S, V16.4S, V18.4S
    WORD $0x4EB2D602           // FSUB V2.4S, V16.4S, V18.4S
    WORD $0x4E33D621           // FADD V1.4S, V17.4S, V19.4S
    WORD $0x4EB3D623           // FSUB V3.4S, V17.4S, V19.4S
    WORD $0x4E36D684           // FADD V4.4S, V20.4S, V22.4S
    WORD $0x4EB6D686           // FSUB V6.4S, V20.4S, V22.4S
    WORD $0x4E37D6A5           // FADD V5.4S, V21.4S, V23.4S
    WORD $0x4EB7D6A7           // FSUB V7.4S, V21.4S, V23.4S
    WORD $0x4E3AD708           // FADD V8.4S, V24.4S, V26.4S
    WORD $0x4EBAD70A           // FSUB V10.4S, V24.4S, V26.4S
    WORD $0x4E3BD729           // FADD V9.4S, V25.4S, V27.4S
    WORD $0x4EBBD72B           // FSUB V11.4S, V25.4S, V27.4S
    WORD $0x4E3ED78C           // FADD V12.4S, V28.4S, V30.4S
    WORD $0x4EBED78E           // FSUB V14.4S, V28.4S, V30.4S
    WORD $0x4E3FD7AD           // FADD V13.4S, V29.4S, V31.4S
    WORD $0x4EBFD7AF           // FSUB V15.4S, V29.4S, V31.4S
    // stage 5: element stride 16
    WORD $0x4E24D410           // FADD V16.4S, V0.4S, V4.4S
    WORD $0x4EA4D414           // FSUB V20.4S, V0.4S, V4.4S
    WORD $0x4E25D431           // FADD V17.4S, V1.4S, V5.4S
    WORD $0x4EA5D435           // FSUB V21.4S, V1.4S, V5.4S
    WORD $0x4E26D452           // FADD V18.4S, V2.4S, V6.4S
    WORD $0x4EA6D456           // FSUB V22.4S, V2.4S, V6.4S
    WORD $0x4E27D473           // FADD V19.4S, V3.4S, V7.4S
    WORD $0x4EA7D477           // FSUB V23.4S, V3.4S, V7.4S
    WORD $0x4E2CD518           // FADD V24.4S, V8.4S, V12.4S
    WORD $0x4EACD51C           // FSUB V28.4S, V8.4S, V12.4S
    WORD $0x4E2DD539           // FADD V25.4S, V9.4S, V13.4S
    WORD $0x4EADD53D           // FSUB V29.4S, V9.4S, V13.4S
    WORD $0x4E2ED55A           // FADD V26.4S, V10.4S, V14.4S
    WORD $0x4EAED55E           // FSUB V30.4S, V10.4S, V14.4S
    WORD $0x4E2FD57B           // FADD V27.4S, V11.4S, V15.4S
    WORD $0x4EAFD57F           // FSUB V31.4S, V11.4S, V15.4S
    // stage 6: element stride 32
    WORD $0x4E38D600           // FADD V0.4S, V16.4S, V24.4S
    WORD $0x4EB8D608           // FSUB V8.4S, V16.4S, V24.4S
    WORD $0x4E39D621           // FADD V1.4S, V17.4S, V25.4S
    WORD $0x4EB9D629           // FSUB V9.4S, V17.4S, V25.4S
    WORD $0x4E3AD642           // FADD V2.4S, V18.4S, V26.4S
    WORD $0x4EBAD64A           // FSUB V10.4S, V18.4S, V26.4S
    WORD $0x4E3BD663           // FADD V3.4S, V19.4S, V27.4S
    WORD $0x4EBBD66B           // FSUB V11.4S, V19.4S, V27.4S
    WORD $0x4E3CD684           // FADD V4.4S, V20.4S, V28.4S
    WORD $0x4EBCD68C           // FSUB V12.4S, V20.4S, V28.4S
    WORD $0x4E3DD6A5           // FADD V5.4S, V21.4S, V29.4S
    WORD $0x4EBDD6AD           // FSUB V13.4S, V21.4S, V29.4S
    WORD $0x4E3ED6C6           // FADD V6.4S, V22.4S, V30.4S
    WORD $0x4EBED6CE           // FSUB V14.4S, V22.4S, V30.4S
    WORD $0x4E3FD6E7           // FADD V7.4S, V23.4S, V31.4S
    WORD $0x4EBFD6EF           // FSUB V15.4S, V23.4S, V31.4S
    VST1.P [V0.S4, V1.S4, V2.S4, V3.S4], 64(R0)
    VST1.P [V4.S4, V5.S4, V6.S4, V7.S4], 64(R0)
    VST1.P [V8.S4, V9.S4, V10.S4, V11.S4], 64(R0)
    VST1.P [V12.S4, V13.S4, V14.S4, V15.S4], 64(R0)
    RET

// func fwht256NEON(x *float32)
// 256-point FWHT with 1/16 normalization: four in-register 64-point
// transforms followed by a fused radix-4 combine of the stride-64 and
// stride-128 butterfly stages.
TEXT ·fwht256NEON(SB), NOSPLIT, $0-8
    MOVD x+0(FP), R0
    MOVD $4, R2

fwht256_block:
    MOVD R0, R1
    FMOVS $(0.0625), F31
    WORD $0x4E0407FF           // DUP V31.4S, V31.S[0]
    VLD1.P 64(R1), [V0.S4, V1.S4, V2.S4, V3.S4]
    VLD1.P 64(R1), [V4.S4, V5.S4, V6.S4, V7.S4]
    VLD1.P 64(R1), [V8.S4, V9.S4, V10.S4, V11.S4]
    VLD1.P 64(R1), [V12.S4, V13.S4, V14.S4, V15.S4]
    WORD $0x6E3FDC00           // FMUL V0.4S, V0.4S, V31.4S
    WORD $0x6E3FDC21           // FMUL V1.4S, V1.4S, V31.4S
    WORD $0x6E3FDC42           // FMUL V2.4S, V2.4S, V31.4S
    WORD $0x6E3FDC63           // FMUL V3.4S, V3.4S, V31.4S
    WORD $0x6E3FDC84           // FMUL V4.4S, V4.4S, V31.4S
    WORD $0x6E3FDCA5           // FMUL V5.4S, V5.4S, V31.4S
    WORD $0x6E3FDCC6           // FMUL V6.4S, V6.4S, V31.4S
    WORD $0x6E3FDCE7           // FMUL V7.4S, V7.4S, V31.4S
    WORD $0x6E3FDD08           // FMUL V8.4S, V8.4S, V31.4S
    WORD $0x6E3FDD29           // FMUL V9.4S, V9.4S, V31.4S
    WORD $0x6E3FDD4A           // FMUL V10.4S, V10.4S, V31.4S
    WORD $0x6E3FDD6B           // FMUL V11.4S, V11.4S, V31.4S
    WORD $0x6E3FDD8C           // FMUL V12.4S, V12.4S, V31.4S
    WORD $0x6E3FDDAD           // FMUL V13.4S, V13.4S, V31.4S
    WORD $0x6E3FDDCE           // FMUL V14.4S, V14.4S, V31.4S
    WORD $0x6E3FDDEF           // FMUL V15.4S, V15.4S, V31.4S
    WORD $0x4EA00810           // REV64 V16.4S, V0.4S
    WORD $0x4E30D411           // FADD V17.4S, V0.4S, V16.4S
    WORD $0x4EB0D412           // FSUB V18.4S, V0.4S, V16.4S
    WORD $0x4E922A20           // TRN1 V0.4S, V17.4S, V18.4S
    WORD $0x6E004010           // EXT V16.16B, V0.16B, V0.16B, #8
    WORD $0x4E30D411           // FADD V17.4S, V0.4S, V16.4S
    WORD $0x4EB0D412           // FSUB V18.4S, V0.4S, V16.4S
    WORD $0x4ED23A20           // ZIP1 V0.2D, V17.2D, V18.2D
    WORD $0x4EA00833           // REV64 V19.4S, V1.4S
    WORD $0x4E33D434           // FADD V20.4S, V1.4S, V19.4S
    WORD $0x4EB3D435           // FSUB V21.4S, V1.4S, V19.4S
    WORD $0x4E952A81           // TRN1 V1.4S, V20.4S, V21.4S
    WORD $0x6E014033           // EXT V19.16B, V1.16B, V1.16B, #8
    WORD $0x4E33D434           // FADD V20.4S, V1.4S, V19.4S
    WORD $0x4EB3D435           // FSUB V21.4S, V1.4S, V19.4S
    WORD $0x4ED53A81           // ZIP1 V1.2D, V20.2D, V21.2D
    WORD $0x4EA00856           // REV64 V22.4S, V2.4S
    WORD $0x4E36D457           // FADD V23.4S, V2.4S, V22.4S
    WORD $0x4EB6D458           // FSUB V24.4S, V2.4S, V22.4S
    WORD $0x4E982AE2           // TRN1 V2.4S, V23.4S, V24.4S
    WORD $0x6E024056           // EXT V22.16B, V2.16B, V2.16B, #8
    WORD $0x4E36D457           // FADD V23.4S, V2.4S, V22.4S
    WORD $0x4EB6D458           // FSUB V24.4S, V2.4S, V22.4S
    WORD $0x4ED83AE2           // ZIP1 V2.2D, V23.2D, V24.2D
    WORD $0x4EA00879           // REV64 V25.4S, V3.4S
    WORD $0x4E39D47A           // FADD V26.4S, V3.4S, V25.4S
    WORD $0x4EB9D47B           // FSUB V27.4S, V3.4S, V25.4S
    WORD $0x4E9B2B43           // TRN1 V3.4S, V26.4S, V27.4S
    WORD $0x6E034079           // EXT V25.16B, V3.16B, V3.16B, #8
    WORD $0x4E39D47A           // FADD V26.4S, V3.4S, V25.4S
    WORD $0x4EB9D47B           // FSUB V27.4S, V3.4S, V25.4S
    WORD $0x4EDB3B43           // ZIP1 V3.2D, V26.2D, V27.2D
    WORD $0x4EA00890           // REV64 V16.4S, V4.4S
    WORD $0x4E30D491           // FADD V17.4S, V4.4S, V16.4S
    WORD $0x4EB0D492           // FSUB V18.4S, V4.4S, V16.4S
    WORD $0x4E922A24           // TRN1 V4.4S, V17.4S, V18.4S
    WORD $0x6E044090           // EXT V16.16B, V4.16B, V4.16B, #8
    WORD $0x4E30D491           // FADD V17.4S, V4.4S, V16.4S
    WORD $0x4EB0D492           // FSUB V18.4S, V4.4S, V16.4S
    WORD $0x4ED23A24           // ZIP1 V4.2D, V17.2D, V18.2D
    WORD $0x4EA008B3           // REV64 V19.4S, V5.4S
    WORD $0x4E33D4B4           // FADD V20.4S, V5.4S, V19.4S
    WORD $0x4EB3D4B5           // FSUB V21.4S, V5.4S, V19.4S
    WORD $0x4E952A85           // TRN1 V5.4S, V20.4S, V21.4S
    WORD $0x6E0540B3           // EXT V19.16B, V5.16B, V5.16B, #8
    WORD $0x4E33D4B4           // FADD V20.4S, V5.4S, V19.4S
    WORD $0x4EB3D4B5           // FSUB V21.4S, V5.4S, V19.4S
    WORD $0x4ED53A85           // ZIP1 V5.2D, V20.2D, V21.2D
    WORD $0x4EA008D6           // REV64 V22.4S, V6.4S
    WORD $0x4E36D4D7           // FADD V23.4S, V6.4S, V22.4S
    WORD $0x4EB6D4D8           // FSUB V24.4S, V6.4S, V22.4S
    WORD $0x4E982AE6           // TRN1 V6.4S, V23.4S, V24.4S
    WORD $0x6E0640D6           // EXT V22.16B, V6.16B, V6.16B, #8
    WORD $0x4E36D4D7           // FADD V23.4S, V6.4S, V22.4S
    WORD $0x4EB6D4D8           // FSUB V24.4S, V6.4S, V22.4S
    WORD $0x4ED83AE6           // ZIP1 V6.2D, V23.2D, V24.2D
    WORD $0x4EA008F9           // REV64 V25.4S, V7.4S
    WORD $0x4E39D4FA           // FADD V26.4S, V7.4S, V25.4S
    WORD $0x4EB9D4FB           // FSUB V27.4S, V7.4S, V25.4S
    WORD $0x4E9B2B47           // TRN1 V7.4S, V26.4S, V27.4S
    WORD $0x6E0740F9           // EXT V25.16B, V7.16B, V7.16B, #8
    WORD $0x4E39D4FA           // FADD V26.4S, V7.4S, V25.4S
    WORD $0x4EB9D4FB           // FSUB V27.4S, V7.4S, V25.4S
    WORD $0x4EDB3B47           // ZIP1 V7.2D, V26.2D, V27.2D
    WORD $0x4EA00910           // REV64 V16.4S, V8.4S
    WORD $0x4E30D511           // FADD V17.4S, V8.4S, V16.4S
    WORD $0x4EB0D512           // FSUB V18.4S, V8.4S, V16.4S
    WORD $0x4E922A28           // TRN1 V8.4S, V17.4S, V18.4S
    WORD $0x6E084110           // EXT V16.16B, V8.16B, V8.16B, #8
    WORD $0x4E30D511           // FADD V17.4S, V8.4S, V16.4S
    WORD $0x4EB0D512           // FSUB V18.4S, V8.4S, V16.4S
    WORD $0x4ED23A28           // ZIP1 V8.2D, V17.2D, V18.2D
    WORD $0x4EA00933           // REV64 V19.4S, V9.4S
    WORD $0x4E33D534           // FADD V20.4S, V9.4S, V19.4S
    WORD $0x4EB3D535           // FSUB V21.4S, V9.4S, V19.4S
    WORD $0x4E952A89           // TRN1 V9.4S, V20.4S, V21.4S
    WORD $0x6E094133           // EXT V19.16B, V9.16B, V9.16B, #8
    WORD $0x4E33D534           // FADD V20.4S, V9.4S, V19.4S
    WORD $0x4EB3D535           // FSUB V21.4S, V9.4S, V19.4S
    WORD $0x4ED53A89           // ZIP1 V9.2D, V20.2D, V21.2D
    WORD $0x4EA00956           // REV64 V22.4S, V10.4S
    WORD $0x4E36D557           // FADD V23.4S, V10.4S, V22.4S
    WORD $0x4EB6D558           // FSUB V24.4S, V10.4S, V22.4S
    WORD $0x4E982AEA           // TRN1 V10.4S, V23.4S, V24.4S
    WORD $0x6E0A4156           // EXT V22.16B, V10.16B, V10.16B, #8
    WORD $0x4E36D557           // FADD V23.4S, V10.4S, V22.4S
    WORD $0x4EB6D558           // FSUB V24.4S, V10.4S, V22.4S
    WORD $0x4ED83AEA           // ZIP1 V10.2D, V23.2D, V24.2D
    WORD $0x4EA00979           // REV64 V25.4S, V11.4S
    WORD $0x4E39D57A           // FADD V26.4S, V11.4S, V25.4S
    WORD $0x4EB9D57B           // FSUB V27.4S, V11.4S, V25.4S
    WORD $0x4E9B2B4B           // TRN1 V11.4S, V26.4S, V27.4S
    WORD $0x6E0B4179           // EXT V25.16B, V11.16B, V11.16B, #8
    WORD $0x4E39D57A           // FADD V26.4S, V11.4S, V25.4S
    WORD $0x4EB9D57B           // FSUB V27.4S, V11.4S, V25.4S
    WORD $0x4EDB3B4B           // ZIP1 V11.2D, V26.2D, V27.2D
    WORD $0x4EA00990           // REV64 V16.4S, V12.4S
    WORD $0x4E30D591           // FADD V17.4S, V12.4S, V16.4S
    WORD $0x4EB0D592           // FSUB V18.4S, V12.4S, V16.4S
    WORD $0x4E922A2C           // TRN1 V12.4S, V17.4S, V18.4S
    WORD $0x6E0C4190           // EXT V16.16B, V12.16B, V12.16B, #8
    WORD $0x4E30D591           // FADD V17.4S, V12.4S, V16.4S
    WORD $0x4EB0D592           // FSUB V18.4S, V12.4S, V16.4S
    WORD $0x4ED23A2C           // ZIP1 V12.2D, V17.2D, V18.2D
    WORD $0x4EA009B3           // REV64 V19.4S, V13.4S
    WORD $0x4E33D5B4           // FADD V20.4S, V13.4S, V19.4S
    WORD $0x4EB3D5B5           // FSUB V21.4S, V13.4S, V19.4S
    WORD $0x4E952A8D           // TRN1 V13.4S, V20.4S, V21.4S
    WORD $0x6E0D41B3           // EXT V19.16B, V13.16B, V13.16B, #8
    WORD $0x4E33D5B4           // FADD V20.4S, V13.4S, V19.4S
    WORD $0x4EB3D5B5           // FSUB V21.4S, V13.4S, V19.4S
    WORD $0x4ED53A8D           // ZIP1 V13.2D, V20.2D, V21.2D
    WORD $0x4EA009D6           // REV64 V22.4S, V14.4S
    WORD $0x4E36D5D7           // FADD V23.4S, V14.4S, V22.4S
    WORD $0x4EB6D5D8           // FSUB V24.4S, V14.4S, V22.4S
    WORD $0x4E982AEE           // TRN1 V14.4S, V23.4S, V24.4S
    WORD $0x6E0E41D6           // EXT V22.16B, V14.16B, V14.16B, #8
    WORD $0x4E36D5D7           // FADD V23.4S, V14.4S, V22.4S
    WORD $0x4EB6D5D8           // FSUB V24.4S, V14.4S, V22.4S
    WORD $0x4ED83AEE           // ZIP1 V14.2D, V23.2D, V24.2D
    WORD $0x4EA009F9           // REV64 V25.4S, V15.4S
    WORD $0x4E39D5FA           // FADD V26.4S, V15.4S, V25.4S
    WORD $0x4EB9D5FB           // FSUB V27.4S, V15.4S, V25.4S
    WORD $0x4E9B2B4F           // TRN1 V15.4S, V26.4S, V27.4S
    WORD $0x6E0F41F9           // EXT V25.16B, V15.16B, V15.16B, #8
    WORD $0x4E39D5FA           // FADD V26.4S, V15.4S, V25.4S
    WORD $0x4EB9D5FB           // FSUB V27.4S, V15.4S, V25.4S
    WORD $0x4EDB3B4F           // ZIP1 V15.2D, V26.2D, V27.2D
    // stage 3: element stride 4 (vector pairs stride 1)
    WORD $0x4E21D410           // FADD V16.4S, V0.4S, V1.4S
    WORD $0x4EA1D411           // FSUB V17.4S, V0.4S, V1.4S
    WORD $0x4E23D452           // FADD V18.4S, V2.4S, V3.4S
    WORD $0x4EA3D453           // FSUB V19.4S, V2.4S, V3.4S
    WORD $0x4E25D494           // FADD V20.4S, V4.4S, V5.4S
    WORD $0x4EA5D495           // FSUB V21.4S, V4.4S, V5.4S
    WORD $0x4E27D4D6           // FADD V22.4S, V6.4S, V7.4S
    WORD $0x4EA7D4D7           // FSUB V23.4S, V6.4S, V7.4S
    WORD $0x4E29D518           // FADD V24.4S, V8.4S, V9.4S
    WORD $0x4EA9D519           // FSUB V25.4S, V8.4S, V9.4S
    WORD $0x4E2BD55A           // FADD V26.4S, V10.4S, V11.4S
    WORD $0x4EABD55B           // FSUB V27.4S, V10.4S, V11.4S
    WORD $0x4E2DD59C           // FADD V28.4S, V12.4S, V13.4S
    WORD $0x4EADD59D           // FSUB V29.4S, V12.4S, V13.4S
    WORD $0x4E2FD5DE           // FADD V30.4S, V14.4S, V15.4S
    WORD $0x4EAFD5DF           // FSUB V31.4S, V14.4S, V15.4S
    // stage 4: element stride 8
    WORD $0x4E32D600           // FADD V0.4S, V16.4S, V18.4S
    WORD $0x4EB2D602           // FSUB V2.4S, V16.4S, V18.4S
    WORD $0x4E33D621           // FADD V1.4S, V17.4S, V19.4S
    WORD $0x4EB3D623           // FSUB V3.4S, V17.4S, V19.4S
    WORD $0x4E36D684           // FADD V4.4S, V20.4S, V22.4S
    WORD $0x4EB6D686           // FSUB V6.4S, V20.4S, V22.4S
    WORD $0x4E37D6A5           // FADD V5.4S, V21.4S, V23.4S
    WORD $0x4EB7D6A7           // FSUB V7.4S, V21.4S, V23.4S
    WORD $0x4E3AD708           // FADD V8.4S, V24.4S, V26.4S
    WORD $0x4EBAD70A           // FSUB V10.4S, V24.4S, V26.4S
    WORD $0x4E3BD729           // FADD V9.4S, V25.4S, V27.4S
    WORD $0x4EBBD72B           // FSUB V11.4S, V25.4S, V27.4S
    WORD $0x4E3ED78C           // FADD V12.4S, V28.4S, V30.4S
    WORD $0x4EBED78E           // FSUB V14.4S, V28.4S, V30.4S
    WORD $0x4E3FD7AD           // FADD V13.4S, V29.4S, V31.4S
    WORD $0x4EBFD7AF           // FSUB V15.4S, V29.4S, V31.4S
    // stage 5: element stride 16
    WORD $0x4E24D410           // FADD V16.4S, V0.4S, V4.4S
    WORD $0x4EA4D414           // FSUB V20.4S, V0.4S, V4.4S
    WORD $0x4E25D431           // FADD V17.4S, V1.4S, V5.4S
    WORD $0x4EA5D435           // FSUB V21.4S, V1.4S, V5.4S
    WORD $0x4E26D452           // FADD V18.4S, V2.4S, V6.4S
    WORD $0x4EA6D456           // FSUB V22.4S, V2.4S, V6.4S
    WORD $0x4E27D473           // FADD V19.4S, V3.4S, V7.4S
    WORD $0x4EA7D477           // FSUB V23.4S, V3.4S, V7.4S
    WORD $0x4E2CD518           // FADD V24.4S, V8.4S, V12.4S
    WORD $0x4EACD51C           // FSUB V28.4S, V8.4S, V12.4S
    WORD $0x4E2DD539           // FADD V25.4S, V9.4S, V13.4S
    WORD $0x4EADD53D           // FSUB V29.4S, V9.4S, V13.4S
    WORD $0x4E2ED55A           // FADD V26.4S, V10.4S, V14.4S
    WORD $0x4EAED55E           // FSUB V30.4S, V10.4S, V14.4S
    WORD $0x4E2FD57B           // FADD V27.4S, V11.4S, V15.4S
    WORD $0x4EAFD57F           // FSUB V31.4S, V11.4S, V15.4S
    // stage 6: element stride 32
    WORD $0x4E38D600           // FADD V0.4S, V16.4S, V24.4S
    WORD $0x4EB8D608           // FSUB V8.4S, V16.4S, V24.4S
    WORD $0x4E39D621           // FADD V1.4S, V17.4S, V25.4S
    WORD $0x4EB9D629           // FSUB V9.4S, V17.4S, V25.4S
    WORD $0x4E3AD642           // FADD V2.4S, V18.4S, V26.4S
    WORD $0x4EBAD64A           // FSUB V10.4S, V18.4S, V26.4S
    WORD $0x4E3BD663           // FADD V3.4S, V19.4S, V27.4S
    WORD $0x4EBBD66B           // FSUB V11.4S, V19.4S, V27.4S
    WORD $0x4E3CD684           // FADD V4.4S, V20.4S, V28.4S
    WORD $0x4EBCD68C           // FSUB V12.4S, V20.4S, V28.4S
    WORD $0x4E3DD6A5           // FADD V5.4S, V21.4S, V29.4S
    WORD $0x4EBDD6AD           // FSUB V13.4S, V21.4S, V29.4S
    WORD $0x4E3ED6C6           // FADD V6.4S, V22.4S, V30.4S
    WORD $0x4EBED6CE           // FSUB V14.4S, V22.4S, V30.4S
    WORD $0x4E3FD6E7           // FADD V7.4S, V23.4S, V31.4S
    WORD $0x4EBFD6EF           // FSUB V15.4S, V23.4S, V31.4S
    VST1.P [V0.S4, V1.S4, V2.S4, V3.S4], 64(R0)
    VST1.P [V4.S4, V5.S4, V6.S4, V7.S4], 64(R0)
    VST1.P [V8.S4, V9.S4, V10.S4, V11.S4], 64(R0)
    VST1.P [V12.S4, V13.S4, V14.S4, V15.S4], 64(R0)
    SUB $1, R2
    CBNZ R2, fwht256_block

    // Combine stages: butterflies at element strides 64 and 128, fused
    // as a radix-4 pass over quadruples (x[i], x[i+64], x[i+128], x[i+192]).
    MOVD x+0(FP), R0
    ADD $256, R0, R1
    ADD $512, R0, R3
    ADD $768, R0, R4
    MOVD $16, R2

fwht256_combine:
    VLD1 (R0), [V0.S4]
    VLD1 (R1), [V1.S4]
    VLD1 (R3), [V2.S4]
    VLD1 (R4), [V3.S4]
    WORD $0x4E21D404           // FADD V4.4S, V0.4S, V1.4S    // t0 = a+b
    WORD $0x4EA1D405           // FSUB V5.4S, V0.4S, V1.4S    // t1 = a-b
    WORD $0x4E23D446           // FADD V6.4S, V2.4S, V3.4S    // t2 = c+d
    WORD $0x4EA3D447           // FSUB V7.4S, V2.4S, V3.4S    // t3 = c-d
    WORD $0x4E26D480           // FADD V0.4S, V4.4S, V6.4S    // a' = t0+t2
    WORD $0x4EA6D482           // FSUB V2.4S, V4.4S, V6.4S    // c' = t0-t2
    WORD $0x4E27D4A1           // FADD V1.4S, V5.4S, V7.4S    // b' = t1+t3
    WORD $0x4EA7D4A3           // FSUB V3.4S, V5.4S, V7.4S    // d' = t1-t3
    VST1.P [V0.S4], 16(R0)
    VST1.P [V1.S4], 16(R1)
    VST1.P [V2.S4], 16(R3)
    VST1.P [V3.S4], 16(R4)
    SUB $1, R2
    CBNZ R2, fwht256_combine
    RET

