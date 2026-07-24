//go:build arm64

#include "textflag.h"

// Hand-written UDOT kernels for uint8 dot product and squared L2 distance,
// used by the 8-bit rotational and scalar quantizers. They require the
// ARMv8.2 DotProd extension (all Apple M-series, Graviton2+); init falls
// back to the goat-generated UMULL/UADALP kernels in distancer/asm without
// it. UDOT accumulates 16 byte-products per instruction where the UMULL
// idiom needs four, roughly doubling throughput on cache-resident codes.
// Results are exact integer sums, identical to the pure Go fallbacks
// (including uint32 wraparound behavior).
//
// WORD-encoded opcodes (Go's assembler lacks these vector mnemonics):
//   UDOT Vd.4S, Vn.16B, Vm.16B: 0x6E809400 | Vm<<16 | Vn<<5 | Vd
//   UABD Vd.16B, Vn.16B, Vm.16B: 0x6E207400 | Vm<<16 | Vn<<5 | Vd
//   ADD  Vd.4S, Vn.4S, Vm.4S:   0x4EA08400 | Vm<<16 | Vn<<5 | Vd
//   ADDV Sd, Vn.4S:             0x4EB1B800 | Vn<<5 | Vd
//   FMOV Wd, Sn:                0x1E260000 | Sn<<5 | Wd

// func dotByteUDOTAsm(a, b *byte, n int) uint32
TEXT ·dotByteUDOTAsm(SB), NOSPLIT, $0-28
    MOVD a+0(FP), R0
    MOVD b+8(FP), R1
    MOVD n+16(FP), R2
    VEOR V0.B16, V0.B16, V0.B16
    VEOR V1.B16, V1.B16, V1.B16
    VEOR V2.B16, V2.B16, V2.B16
    VEOR V3.B16, V3.B16, V3.B16
    MOVD $0, R7
    LSR $6, R2, R4
    CBZ R4, db_tail32

db_loop64:
    VLD1.P 32(R0), [V4.B16, V5.B16]
    VLD1.P 32(R0), [V6.B16, V7.B16]
    VLD1.P 32(R1), [V16.B16, V17.B16]
    VLD1.P 32(R1), [V18.B16, V19.B16]
    WORD $0x6E909480           // UDOT V0.4S, V4.16B, V16.16B
    WORD $0x6E9194A1           // UDOT V1.4S, V5.16B, V17.16B
    WORD $0x6E9294C2           // UDOT V2.4S, V6.16B, V18.16B
    WORD $0x6E9394E3           // UDOT V3.4S, V7.16B, V19.16B
    SUB $1, R4
    CBNZ R4, db_loop64

db_tail32:
    TBZ $5, R2, db_tail16
    VLD1.P 32(R0), [V4.B16, V5.B16]
    VLD1.P 32(R1), [V16.B16, V17.B16]
    WORD $0x6E909480           // UDOT V0.4S, V4.16B, V16.16B
    WORD $0x6E9194A1           // UDOT V1.4S, V5.16B, V17.16B

db_tail16:
    TBZ $4, R2, db_scalar_check
    VLD1.P 16(R0), [V4.B16]
    VLD1.P 16(R1), [V16.B16]
    WORD $0x6E909480           // UDOT V0.4S, V4.16B, V16.16B

db_scalar_check:
    AND $15, R2, R5
    CBZ R5, db_reduce

db_scalar:
    MOVBU.P 1(R0), R8
    MOVBU.P 1(R1), R9
    MUL R8, R9, R9
    ADD R9, R7
    SUB $1, R5
    CBNZ R5, db_scalar

db_reduce:
    WORD $0x4EA18400           // ADD V0.4S, V0.4S, V1.4S
    WORD $0x4EA38442           // ADD V2.4S, V2.4S, V3.4S
    WORD $0x4EA28400           // ADD V0.4S, V0.4S, V2.4S
    WORD $0x4EB1B800           // ADDV S0, V0.4S
    WORD $0x1E260006           // FMOV W6, S0
    ADD R6, R7
    MOVW R7, ret+24(FP)
    RET

// func l2ByteUDOTAsm(a, b *byte, n int) uint32
TEXT ·l2ByteUDOTAsm(SB), NOSPLIT, $0-28
    MOVD a+0(FP), R0
    MOVD b+8(FP), R1
    MOVD n+16(FP), R2
    VEOR V0.B16, V0.B16, V0.B16
    VEOR V1.B16, V1.B16, V1.B16
    VEOR V2.B16, V2.B16, V2.B16
    VEOR V3.B16, V3.B16, V3.B16
    MOVD $0, R7
    LSR $6, R2, R4
    CBZ R4, l2b_tail32

l2b_loop64:
    VLD1.P 32(R0), [V4.B16, V5.B16]
    VLD1.P 32(R0), [V6.B16, V7.B16]
    VLD1.P 32(R1), [V16.B16, V17.B16]
    VLD1.P 32(R1), [V18.B16, V19.B16]
    WORD $0x6E30749D           // UABD V29.16B, V4.16B, V16.16B
    WORD $0x6E9D97A0           // UDOT V0.4S, V29.16B, V29.16B
    WORD $0x6E3174BD           // UABD V29.16B, V5.16B, V17.16B
    WORD $0x6E9D97A1           // UDOT V1.4S, V29.16B, V29.16B
    WORD $0x6E3274DD           // UABD V29.16B, V6.16B, V18.16B
    WORD $0x6E9D97A2           // UDOT V2.4S, V29.16B, V29.16B
    WORD $0x6E3374FD           // UABD V29.16B, V7.16B, V19.16B
    WORD $0x6E9D97A3           // UDOT V3.4S, V29.16B, V29.16B
    SUB $1, R4
    CBNZ R4, l2b_loop64

l2b_tail32:
    TBZ $5, R2, l2b_tail16
    VLD1.P 32(R0), [V4.B16, V5.B16]
    VLD1.P 32(R1), [V16.B16, V17.B16]
    WORD $0x6E30749D           // UABD V29.16B, V4.16B, V16.16B
    WORD $0x6E9D97A0           // UDOT V0.4S, V29.16B, V29.16B
    WORD $0x6E3174BD           // UABD V29.16B, V5.16B, V17.16B
    WORD $0x6E9D97A1           // UDOT V1.4S, V29.16B, V29.16B

l2b_tail16:
    TBZ $4, R2, l2b_scalar_check
    VLD1.P 16(R0), [V4.B16]
    VLD1.P 16(R1), [V16.B16]
    WORD $0x6E30749D           // UABD V29.16B, V4.16B, V16.16B
    WORD $0x6E9D97A0           // UDOT V0.4S, V29.16B, V29.16B

l2b_scalar_check:
    AND $15, R2, R5
    CBZ R5, l2b_reduce

l2b_scalar:
    MOVBU.P 1(R0), R8
    MOVBU.P 1(R1), R9
    SUB R9, R8, R10
    MUL R10, R10, R10
    ADD R10, R7
    SUB $1, R5
    CBNZ R5, l2b_scalar

l2b_reduce:
    WORD $0x4EA18400           // ADD V0.4S, V0.4S, V1.4S
    WORD $0x4EA38442           // ADD V2.4S, V2.4S, V3.4S
    WORD $0x4EA28400           // ADD V0.4S, V0.4S, V2.4S
    WORD $0x4EB1B800           // ADDV S0, V0.4S
    WORD $0x1E260006           // FMOV W6, S0
    ADD R6, R7
    MOVW R7, ret+24(FP)
    RET

