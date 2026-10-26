//go:build arm64

#include "textflag.h"

// Hand-written NEON kernels for the packed 4-bit code dot products of the
// 4-bit rotational quantizer. The nibbles are unpacked in registers (AND /
// USHR #4) and fed directly into unsigned integer dot accumulators, so the
// packed codes never round-trip through a scratch buffer.
//
// Two variants per operation: UDOT (ARMv8.2 DotProd, all Apple M-series and
// Graviton2+) and UMULL/UADALP for baseline ASIMD, selected at init time.
// Results are exact integer sums, identical to the pure Go fallbacks
// (including uint32 wraparound behavior).
//
// WORD-encoded opcodes (Go's assembler lacks these vector mnemonics):
//   UDOT   Vd.4S, Vn.16B, Vm.16B: 0x6E809400 | Vm<<16 | Vn<<5 | Vd
//   UMULL  Vd.8H, Vn.8B,  Vm.8B:  0x2E20C000 | Vm<<16 | Vn<<5 | Vd
//   UMULL2 Vd.8H, Vn.16B, Vm.16B: 0x6E20C000 | Vm<<16 | Vn<<5 | Vd
//   UADALP Vd.4S, Vn.8H:          0x6E606800 | Vn<<5 | Vd
//   AND    Vd.16B, Vn.16B, Vm.16B: 0x4E201C00 | Vm<<16 | Vn<<5 | Vd
//   USHR   Vd.16B, Vn.16B, #4:    0x6F0C0400 | Vn<<5 | Vd
//   MOVI   Vd.16B, #0x0F:         0x4F00E400 | 0x0F<<5 | Vd
//   ADD    Vd.4S, Vn.4S, Vm.4S:   0x4EA08400 | Vm<<16 | Vn<<5 | Vd
//   ADDV   Sd, Vn.4S:             0x4EB1B800 | Vn<<5 | Vd
//   FMOV   Wd, Sn:                0x1E260000 | Sn<<5 | Wd

// func dotByteNibbleUDOTAsm(q, packed *byte, half int) uint32
TEXT ·dotByteNibbleUDOTAsm(SB), NOSPLIT, $0-28
    MOVD q+0(FP), R0
    MOVD packed+8(FP), R1
    MOVD half+16(FP), R2
    ADD R2, R0, R3            // R3 = high-nibble query plane
    WORD $0x4F00E5FC           // MOVI V28.16B, $0x0F
    VEOR V0.B16, V0.B16, V0.B16
    VEOR V1.B16, V1.B16, V1.B16
    VEOR V2.B16, V2.B16, V2.B16
    VEOR V3.B16, V3.B16, V3.B16
    MOVD $0, R7
    LSR $5, R2, R4
    CBZ R4, bnu_tail16

bnu_loop32:
    VLD1.P 32(R1), [V4.B16, V5.B16]
    WORD $0x4E3C1C86           // AND V6.16B, V4.16B, V28.16B   // lo nibbles
    WORD $0x4E3C1CA7           // AND V7.16B, V5.16B, V28.16B
    WORD $0x6F0C0490           // USHR V16.16B, V4.16B, #4      // hi nibbles
    WORD $0x6F0C04B1           // USHR V17.16B, V5.16B, #4
    VLD1.P 32(R0), [V18.B16, V19.B16]
    VLD1.P 32(R3), [V20.B16, V21.B16]
    WORD $0x6E869640           // UDOT V0.4S, V18.16B, V6.16B
    WORD $0x6E879661           // UDOT V1.4S, V19.16B, V7.16B
    WORD $0x6E909682           // UDOT V2.4S, V20.16B, V16.16B
    WORD $0x6E9196A3           // UDOT V3.4S, V21.16B, V17.16B
    SUB $1, R4
    CBNZ R4, bnu_loop32

bnu_tail16:
    TBZ $4, R2, bnu_scalar_check
    VLD1.P 16(R1), [V4.B16]
    WORD $0x4E3C1C86           // AND V6.16B, V4.16B, V28.16B
    WORD $0x6F0C0490           // USHR V16.16B, V4.16B, #4
    VLD1.P 16(R0), [V18.B16]
    VLD1.P 16(R3), [V20.B16]
    WORD $0x6E869640           // UDOT V0.4S, V18.16B, V6.16B
    WORD $0x6E909682           // UDOT V2.4S, V20.16B, V16.16B

bnu_scalar_check:
    AND $15, R2, R5
    CBZ R5, bnu_reduce

bnu_scalar:
    MOVBU.P 1(R1), R9
    MOVBU.P 1(R0), R8
    MOVBU.P 1(R3), R10
    AND $0x0F, R9, R11
    LSR $4, R9, R12
    MUL R8, R11, R11
    ADD R11, R7
    MUL R10, R12, R12
    ADD R12, R7
    SUB $1, R5
    CBNZ R5, bnu_scalar

bnu_reduce:
    WORD $0x4EA18400           // ADD V0.4S, V0.4S, V1.4S
    WORD $0x4EA38442           // ADD V2.4S, V2.4S, V3.4S
    WORD $0x4EA28400           // ADD V0.4S, V0.4S, V2.4S
    WORD $0x4EB1B800           // ADDV S0, V0.4S
    WORD $0x1E260006           // FMOV W6, S0
    ADD R6, R7
    MOVW R7, ret+24(FP)
    RET

// func dotByteNibbleUADALPAsm(q, packed *byte, half int) uint32
TEXT ·dotByteNibbleUADALPAsm(SB), NOSPLIT, $0-28
    MOVD q+0(FP), R0
    MOVD packed+8(FP), R1
    MOVD half+16(FP), R2
    ADD R2, R0, R3            // R3 = high-nibble query plane
    WORD $0x4F00E5FC           // MOVI V28.16B, $0x0F
    VEOR V0.B16, V0.B16, V0.B16
    VEOR V1.B16, V1.B16, V1.B16
    VEOR V2.B16, V2.B16, V2.B16
    VEOR V3.B16, V3.B16, V3.B16
    MOVD $0, R7
    LSR $5, R2, R4
    CBZ R4, bnu_tail16

bnu_loop32:
    VLD1.P 32(R1), [V4.B16, V5.B16]
    WORD $0x4E3C1C86           // AND V6.16B, V4.16B, V28.16B   // lo nibbles
    WORD $0x4E3C1CA7           // AND V7.16B, V5.16B, V28.16B
    WORD $0x6F0C0490           // USHR V16.16B, V4.16B, #4      // hi nibbles
    WORD $0x6F0C04B1           // USHR V17.16B, V5.16B, #4
    VLD1.P 32(R0), [V18.B16, V19.B16]
    VLD1.P 32(R3), [V20.B16, V21.B16]
    WORD $0x2E26C25D           // UMULL V29.8H, V18.8B, V6.8B
    WORD $0x6E26C25E           // UMULL2 V30.8H, V18.16B, V6.16B
    WORD $0x6E606BA0           // UADALP V0.4S, V29.8H
    WORD $0x6E606BC0           // UADALP V0.4S, V30.8H
    WORD $0x2E27C27D           // UMULL V29.8H, V19.8B, V7.8B
    WORD $0x6E27C27E           // UMULL2 V30.8H, V19.16B, V7.16B
    WORD $0x6E606BA1           // UADALP V1.4S, V29.8H
    WORD $0x6E606BC1           // UADALP V1.4S, V30.8H
    WORD $0x2E30C29D           // UMULL V29.8H, V20.8B, V16.8B
    WORD $0x6E30C29E           // UMULL2 V30.8H, V20.16B, V16.16B
    WORD $0x6E606BA2           // UADALP V2.4S, V29.8H
    WORD $0x6E606BC2           // UADALP V2.4S, V30.8H
    WORD $0x2E31C2BD           // UMULL V29.8H, V21.8B, V17.8B
    WORD $0x6E31C2BE           // UMULL2 V30.8H, V21.16B, V17.16B
    WORD $0x6E606BA3           // UADALP V3.4S, V29.8H
    WORD $0x6E606BC3           // UADALP V3.4S, V30.8H
    SUB $1, R4
    CBNZ R4, bnu_loop32

bnu_tail16:
    TBZ $4, R2, bnu_scalar_check
    VLD1.P 16(R1), [V4.B16]
    WORD $0x4E3C1C86           // AND V6.16B, V4.16B, V28.16B
    WORD $0x6F0C0490           // USHR V16.16B, V4.16B, #4
    VLD1.P 16(R0), [V18.B16]
    VLD1.P 16(R3), [V20.B16]
    WORD $0x2E26C25D           // UMULL V29.8H, V18.8B, V6.8B
    WORD $0x6E26C25E           // UMULL2 V30.8H, V18.16B, V6.16B
    WORD $0x6E606BA0           // UADALP V0.4S, V29.8H
    WORD $0x6E606BC0           // UADALP V0.4S, V30.8H
    WORD $0x2E30C29D           // UMULL V29.8H, V20.8B, V16.8B
    WORD $0x6E30C29E           // UMULL2 V30.8H, V20.16B, V16.16B
    WORD $0x6E606BA2           // UADALP V2.4S, V29.8H
    WORD $0x6E606BC2           // UADALP V2.4S, V30.8H

bnu_scalar_check:
    AND $15, R2, R5
    CBZ R5, bnu_reduce

bnu_scalar:
    MOVBU.P 1(R1), R9
    MOVBU.P 1(R0), R8
    MOVBU.P 1(R3), R10
    AND $0x0F, R9, R11
    LSR $4, R9, R12
    MUL R8, R11, R11
    ADD R11, R7
    MUL R10, R12, R12
    ADD R12, R7
    SUB $1, R5
    CBNZ R5, bnu_scalar

bnu_reduce:
    WORD $0x4EA18400           // ADD V0.4S, V0.4S, V1.4S
    WORD $0x4EA38442           // ADD V2.4S, V2.4S, V3.4S
    WORD $0x4EA28400           // ADD V0.4S, V0.4S, V2.4S
    WORD $0x4EB1B800           // ADDV S0, V0.4S
    WORD $0x1E260006           // FMOV W6, S0
    ADD R6, R7
    MOVW R7, ret+24(FP)
    RET

// func dotNibbleNibbleUDOTAsm(a, b *byte, n int) uint32
TEXT ·dotNibbleNibbleUDOTAsm(SB), NOSPLIT, $0-28
    MOVD a+0(FP), R0
    MOVD b+8(FP), R1
    MOVD n+16(FP), R2
    WORD $0x4F00E5FC           // MOVI V28.16B, $0x0F
    VEOR V0.B16, V0.B16, V0.B16
    VEOR V1.B16, V1.B16, V1.B16
    VEOR V2.B16, V2.B16, V2.B16
    VEOR V3.B16, V3.B16, V3.B16
    MOVD $0, R7
    LSR $5, R2, R4
    CBZ R4, nnu_tail16

nnu_loop32:
    VLD1.P 32(R0), [V4.B16, V5.B16]
    VLD1.P 32(R1), [V18.B16, V19.B16]
    WORD $0x4E3C1C86           // AND V6.16B, V4.16B, V28.16B   // lo(a)
    WORD $0x4E3C1CA7           // AND V7.16B, V5.16B, V28.16B
    WORD $0x6F0C0490           // USHR V16.16B, V4.16B, #4      // hi(a)
    WORD $0x6F0C04B1           // USHR V17.16B, V5.16B, #4
    WORD $0x4E3C1E56           // AND V22.16B, V18.16B, V28.16B // lo(b)
    WORD $0x4E3C1E77           // AND V23.16B, V19.16B, V28.16B
    WORD $0x6F0C0658           // USHR V24.16B, V18.16B, #4     // hi(b)
    WORD $0x6F0C0679           // USHR V25.16B, V19.16B, #4
    WORD $0x6E9694C0           // UDOT V0.4S, V6.16B, V22.16B
    WORD $0x6E9794E1           // UDOT V1.4S, V7.16B, V23.16B
    WORD $0x6E989602           // UDOT V2.4S, V16.16B, V24.16B
    WORD $0x6E999623           // UDOT V3.4S, V17.16B, V25.16B
    SUB $1, R4
    CBNZ R4, nnu_loop32

nnu_tail16:
    TBZ $4, R2, nnu_scalar_check
    VLD1.P 16(R0), [V4.B16]
    VLD1.P 16(R1), [V18.B16]
    WORD $0x4E3C1C86           // AND V6.16B, V4.16B, V28.16B
    WORD $0x6F0C0490           // USHR V16.16B, V4.16B, #4
    WORD $0x4E3C1E56           // AND V22.16B, V18.16B, V28.16B
    WORD $0x6F0C0658           // USHR V24.16B, V18.16B, #4
    WORD $0x6E9694C0           // UDOT V0.4S, V6.16B, V22.16B
    WORD $0x6E989602           // UDOT V2.4S, V16.16B, V24.16B

nnu_scalar_check:
    AND $15, R2, R5
    CBZ R5, nnu_reduce

nnu_scalar:
    MOVBU.P 1(R0), R8
    MOVBU.P 1(R1), R9
    AND $0x0F, R8, R10
    AND $0x0F, R9, R11
    MUL R10, R11, R11
    ADD R11, R7
    LSR $4, R8, R10
    LSR $4, R9, R11
    MUL R10, R11, R11
    ADD R11, R7
    SUB $1, R5
    CBNZ R5, nnu_scalar

nnu_reduce:
    WORD $0x4EA18400           // ADD V0.4S, V0.4S, V1.4S
    WORD $0x4EA38442           // ADD V2.4S, V2.4S, V3.4S
    WORD $0x4EA28400           // ADD V0.4S, V0.4S, V2.4S
    WORD $0x4EB1B800           // ADDV S0, V0.4S
    WORD $0x1E260006           // FMOV W6, S0
    ADD R6, R7
    MOVW R7, ret+24(FP)
    RET

// func dotNibbleNibbleUADALPAsm(a, b *byte, n int) uint32
TEXT ·dotNibbleNibbleUADALPAsm(SB), NOSPLIT, $0-28
    MOVD a+0(FP), R0
    MOVD b+8(FP), R1
    MOVD n+16(FP), R2
    WORD $0x4F00E5FC           // MOVI V28.16B, $0x0F
    VEOR V0.B16, V0.B16, V0.B16
    VEOR V1.B16, V1.B16, V1.B16
    VEOR V2.B16, V2.B16, V2.B16
    VEOR V3.B16, V3.B16, V3.B16
    MOVD $0, R7
    LSR $5, R2, R4
    CBZ R4, nnu_tail16

nnu_loop32:
    VLD1.P 32(R0), [V4.B16, V5.B16]
    VLD1.P 32(R1), [V18.B16, V19.B16]
    WORD $0x4E3C1C86           // AND V6.16B, V4.16B, V28.16B   // lo(a)
    WORD $0x4E3C1CA7           // AND V7.16B, V5.16B, V28.16B
    WORD $0x6F0C0490           // USHR V16.16B, V4.16B, #4      // hi(a)
    WORD $0x6F0C04B1           // USHR V17.16B, V5.16B, #4
    WORD $0x4E3C1E56           // AND V22.16B, V18.16B, V28.16B // lo(b)
    WORD $0x4E3C1E77           // AND V23.16B, V19.16B, V28.16B
    WORD $0x6F0C0658           // USHR V24.16B, V18.16B, #4     // hi(b)
    WORD $0x6F0C0679           // USHR V25.16B, V19.16B, #4
    WORD $0x2E36C0DD           // UMULL V29.8H, V6.8B, V22.8B
    WORD $0x6E36C0DE           // UMULL2 V30.8H, V6.16B, V22.16B
    WORD $0x6E606BA0           // UADALP V0.4S, V29.8H
    WORD $0x6E606BC0           // UADALP V0.4S, V30.8H
    WORD $0x2E37C0FD           // UMULL V29.8H, V7.8B, V23.8B
    WORD $0x6E37C0FE           // UMULL2 V30.8H, V7.16B, V23.16B
    WORD $0x6E606BA1           // UADALP V1.4S, V29.8H
    WORD $0x6E606BC1           // UADALP V1.4S, V30.8H
    WORD $0x2E38C21D           // UMULL V29.8H, V16.8B, V24.8B
    WORD $0x6E38C21E           // UMULL2 V30.8H, V16.16B, V24.16B
    WORD $0x6E606BA2           // UADALP V2.4S, V29.8H
    WORD $0x6E606BC2           // UADALP V2.4S, V30.8H
    WORD $0x2E39C23D           // UMULL V29.8H, V17.8B, V25.8B
    WORD $0x6E39C23E           // UMULL2 V30.8H, V17.16B, V25.16B
    WORD $0x6E606BA3           // UADALP V3.4S, V29.8H
    WORD $0x6E606BC3           // UADALP V3.4S, V30.8H
    SUB $1, R4
    CBNZ R4, nnu_loop32

nnu_tail16:
    TBZ $4, R2, nnu_scalar_check
    VLD1.P 16(R0), [V4.B16]
    VLD1.P 16(R1), [V18.B16]
    WORD $0x4E3C1C86           // AND V6.16B, V4.16B, V28.16B
    WORD $0x6F0C0490           // USHR V16.16B, V4.16B, #4
    WORD $0x4E3C1E56           // AND V22.16B, V18.16B, V28.16B
    WORD $0x6F0C0658           // USHR V24.16B, V18.16B, #4
    WORD $0x2E36C0DD           // UMULL V29.8H, V6.8B, V22.8B
    WORD $0x6E36C0DE           // UMULL2 V30.8H, V6.16B, V22.16B
    WORD $0x6E606BA0           // UADALP V0.4S, V29.8H
    WORD $0x6E606BC0           // UADALP V0.4S, V30.8H
    WORD $0x2E38C21D           // UMULL V29.8H, V16.8B, V24.8B
    WORD $0x6E38C21E           // UMULL2 V30.8H, V16.16B, V24.16B
    WORD $0x6E606BA2           // UADALP V2.4S, V29.8H
    WORD $0x6E606BC2           // UADALP V2.4S, V30.8H

nnu_scalar_check:
    AND $15, R2, R5
    CBZ R5, nnu_reduce

nnu_scalar:
    MOVBU.P 1(R0), R8
    MOVBU.P 1(R1), R9
    AND $0x0F, R8, R10
    AND $0x0F, R9, R11
    MUL R10, R11, R11
    ADD R11, R7
    LSR $4, R8, R10
    LSR $4, R9, R11
    MUL R10, R11, R11
    ADD R11, R7
    SUB $1, R5
    CBNZ R5, nnu_scalar

nnu_reduce:
    WORD $0x4EA18400           // ADD V0.4S, V0.4S, V1.4S
    WORD $0x4EA38442           // ADD V2.4S, V2.4S, V3.4S
    WORD $0x4EA28400           // ADD V0.4S, V0.4S, V2.4S
    WORD $0x4EB1B800           // ADDV S0, V0.4S
    WORD $0x1E260006           // FMOV W6, S0
    ADD R6, R7
    MOVW R7, ret+24(FP)
    RET

