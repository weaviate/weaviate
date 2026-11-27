//go:build !noasm && arm64

#include "textflag.h"

// NEON fast scan of the int8-table RQ1 estimator. The Go wrappers in
// rq1_lut_fs_arm64.go document the arguments.
//
// One query token's tables against sixteen document tokens per iteration, held
// in the byte lanes. Per stored code byte: one 16-byte load is that byte
// position for the whole block, the two nibbles are unpacked in registers
// (AND / USHR #4), and each nibble indexes a 16-entry table held in a vector
// register, so a lookup is one TBL. Out-of-range indices cannot occur: a
// nibble is 0..15, exactly TBL's single-register range.
//
// The int8 lookups accumulate into int16 lanes (SADDW / SADDW2), exact while
// nibbles*127 fits int16; the caller gates on that and falls back to the
// portable kernel in rq1_lut_fs.go. There are two independent accumulator
// pairs: with one pair every lookup in a block sits on one dependency chain of
// 32 SADDWs and the kernel is latency-bound. Adding the two pairs at the end is
// exact (integer addition). The epilogue widens to int32, converts to float,
// multiplies by the query token's scale and then by the sixteen tokens' Steps
// (that order, because the reference computes (S*scale)*Step and parity is bit
// equality), and folds into four running lane maxima kept in registers across
// blocks.
//
// WORD-encoded opcodes (Go's assembler lacks these vector mnemonics), the same
// treatment asm/dot_byte_nibble_arm64.s gives UDOT and UADALP:
//   SADDW  Vd.8H, Vn.8H, Vm.8B:  0x0E201000 | Vm<<16 | Vn<<5 | Vd
//   SADDW2 Vd.8H, Vn.8H, Vm.16B: 0x4E201000 | Vm<<16 | Vn<<5 | Vd
//   SSHLL  Vd.4S, Vn.4H, #0:     0x0F10A400 | Vn<<5 | Vd
//   SSHLL2 Vd.4S, Vn.8H, #0:     0x4F10A400 | Vn<<5 | Vd
//   SCVTF  Vd.4S, Vn.4S:         0x4E21D800 | Vn<<5 | Vd
//   FMUL   Vd.4S, Vn.4S, Vm.4S:  0x6E20DC00 | Vm<<16 | Vn<<5 | Vd
//   FMAX   Vd.4S, Vn.4S, Vm.4S:  0x4E20F400 | Vm<<16 | Vn<<5 | Vd

// func rq1ScanBlocksAsmNEON(tbl *int8, codes *byte, steps, best *float32, groups, blocks int, scale float32)
TEXT ·rq1ScanBlocksAsmNEON(SB), NOSPLIT, $0-52
    MOVD tbl+0(FP), R0
    MOVD codes+8(FP), R1
    MOVD steps+16(FP), R2
    MOVD best+24(FP), R3
    MOVD groups+32(FP), R4
    MOVD blocks+40(FP), R5
    CBZ  R5, fs_done

    MOVW  scale+48(FP), R9
    VDUP  R9, V31.S4                    // scale in every lane
    VMOVI $0x0F, V28.B16                // low-nibble mask
    VLD1  (R3), [V16.S4, V17.S4, V18.S4, V19.S4]  // running lane maxima

fs_block:
    VEOR V24.B16, V24.B16, V24.B16      // int16 accumulators, lanes 0..7
    VEOR V25.B16, V25.B16, V25.B16      // and lanes 8..15
    VEOR V26.B16, V26.B16, V26.B16      // a second, independent pair for the
    VEOR V27.B16, V27.B16, V27.B16      // high nibbles' lookups
    MOVD R0, R6                         // table cursor, reset per block
    MOVD R4, R7                         // group counter

fs_group:
    VLD1.P 16(R1), [V0.B16]             // one code byte for sixteen tokens
    VLD1.P 32(R6), [V1.B16, V2.B16]     // the two nibble groups' tables
    VAND   V28.B16, V0.B16, V3.B16
    VUSHR  $4, V0.B16, V4.B16
    VTBL   V3.B16, [V1.B16], V5.B16
    VTBL   V4.B16, [V2.B16], V6.B16
    WORD $0x0E251318                    // SADDW  V24.8H, V24.8H, V5.8B
    WORD $0x4E251339                    // SADDW2 V25.8H, V25.8H, V5.16B
    WORD $0x0E26135A                    // SADDW  V26.8H, V26.8H, V6.8B
    WORD $0x4E26137B                    // SADDW2 V27.8H, V27.8H, V6.16B
    SUB  $1, R7
    CBNZ R7, fs_group

    WORD $0x4E7A8718                    // ADD V24.8H, V24.8H, V26.8H
    WORD $0x4E7B8739                    // ADD V25.8H, V25.8H, V27.8H
    WORD $0x0F10A708                    // SSHLL  V8.4S,  V24.4H, #0   tokens 0..3
    WORD $0x4F10A709                    // SSHLL2 V9.4S,  V24.8H, #0   tokens 4..7
    WORD $0x0F10A72A                    // SSHLL  V10.4S, V25.4H, #0   tokens 8..11
    WORD $0x4F10A72B                    // SSHLL2 V11.4S, V25.8H, #0   tokens 12..15
    WORD $0x4E21D908                    // SCVTF V8.4S, V8.4S
    WORD $0x4E21D929                    // SCVTF V9.4S, V9.4S
    WORD $0x4E21D94A                    // SCVTF V10.4S, V10.4S
    WORD $0x4E21D96B                    // SCVTF V11.4S, V11.4S
    WORD $0x6E3FDD08                    // FMUL V8.4S, V8.4S, V31.4S
    WORD $0x6E3FDD29                    // FMUL V9.4S, V9.4S, V31.4S
    WORD $0x6E3FDD4A                    // FMUL V10.4S, V10.4S, V31.4S
    WORD $0x6E3FDD6B                    // FMUL V11.4S, V11.4S, V31.4S
    VLD1.P 64(R2), [V12.S4, V13.S4, V14.S4, V15.S4]   // sixteen tokens' Steps
    WORD $0x6E2CDD08                    // FMUL V8.4S, V8.4S, V12.4S
    WORD $0x6E2DDD29                    // FMUL V9.4S, V9.4S, V13.4S
    WORD $0x6E2EDD4A                    // FMUL V10.4S, V10.4S, V14.4S
    WORD $0x6E2FDD6B                    // FMUL V11.4S, V11.4S, V15.4S
    WORD $0x4E28F610                    // FMAX V16.4S, V16.4S, V8.4S
    WORD $0x4E29F631                    // FMAX V17.4S, V17.4S, V9.4S
    WORD $0x4E2AF652                    // FMAX V18.4S, V18.4S, V10.4S
    WORD $0x4E2BF673                    // FMAX V19.4S, V19.4S, V11.4S

    SUB  $1, R5
    CBNZ R5, fs_block

    VST1 [V16.S4, V17.S4, V18.S4, V19.S4], (R3)

fs_done:
    RET

// The query-tiled NEON fast scan: four query tokens at a time over the same
// stored bytes.
//
// The kernel above walks a document's code section once per query token. Only
// three of its ten instructions per byte group (the 16-byte load, the AND, the
// USHR) are shared work, so a tile of Q query tokens costs 3+7Q per group where
// Q passes cost 10Q. Four is where that stops paying: 7.75 against a limit
// of 7.
//
// Each query token wants four running lane maxima, so the register file sets
// the trade between tile width and accumulator chains per query token. Four
// tokens with two chains each is the only pairing that fits: sixteen lane
// maxima in V16..V31 and eight accumulators in V8..V15 leave V0..V7, which is
// why the mask is rebuilt per block and each query token's scale is broadcast
// in the epilogue and never held.
//
// Everything else is the one-token kernel's, instruction for instruction, and
// each query token's arithmetic is unchanged, so parity with it is bit
// equality.


// func rq1ScanTileAsmNEON4(tbl *int8, tblStride int, codes *byte, steps, best, scales *float32, groups, blocks int)
TEXT ·rq1ScanTileAsmNEON4(SB), NOSPLIT, $0-64
    MOVD tbl+0(FP), R0
    MOVD tblStride+8(FP), R12
    MOVD codes+16(FP), R1
    MOVD steps+24(FP), R2
    MOVD best+32(FP), R3
    MOVD scales+40(FP), R11
    MOVD groups+48(FP), R4
    MOVD blocks+56(FP), R5
    CBZ  R5, ts4_done

    ADD  $64, R3, R13               // one lane-maximum vector per query
    ADD  $128, R3, R14              // token of the tile, four in all
    ADD  $192, R3, R15
    VLD1 (R3), [V16.S4, V17.S4, V18.S4, V19.S4]
    VLD1 (R13), [V20.S4, V21.S4, V22.S4, V23.S4]
    VLD1 (R14), [V24.S4, V25.S4, V26.S4, V27.S4]
    VLD1 (R15), [V28.S4, V29.S4, V30.S4, V31.S4]

ts4_block:
    VEOR V8.B16, V8.B16, V8.B16     // q0 accumulators, lanes 0..7
    VEOR V9.B16, V9.B16, V9.B16     // and lanes 8..15
    VEOR V10.B16, V10.B16, V10.B16  // q1 accumulators, lanes 0..7
    VEOR V11.B16, V11.B16, V11.B16  // and lanes 8..15
    VEOR V12.B16, V12.B16, V12.B16  // q2 accumulators, lanes 0..7
    VEOR V13.B16, V13.B16, V13.B16  // and lanes 8..15
    VEOR V14.B16, V14.B16, V14.B16  // q3 accumulators, lanes 0..7
    VEOR V15.B16, V15.B16, V15.B16  // and lanes 8..15
    VMOVI $0x0F, V7.B16             // low-nibble mask; the epilogue needs V7,
                                    // so it is rebuilt once per block
    MOVD R0, R6                     // table cursors, reset per block
    ADD  R12, R0, R8
    ADD  R12, R8, R9
    ADD  R12, R9, R10
    MOVD R4, R7                     // group counter

ts4_group:
    VLD1.P 16(R1), [V0.B16]         // one code byte for sixteen tokens
    VAND   V7.B16, V0.B16, V1.B16   // unpacked once for the whole tile
    VUSHR  $4, V0.B16, V2.B16
    VLD1.P 32(R6), [V3.B16, V4.B16] // q0's two nibble groups' tables
    VTBL   V1.B16, [V3.B16], V5.B16
    VTBL   V2.B16, [V4.B16], V6.B16
    WORD $0x0E251108                // SADDW  V8.8H, V8.8H, V5.8B
    WORD $0x4E251129                // SADDW2 V9.8H, V9.8H, V5.16B
    WORD $0x0E261108                // SADDW  V8.8H, V8.8H, V6.8B
    WORD $0x4E261129                // SADDW2 V9.8H, V9.8H, V6.16B
    VLD1.P 32(R8), [V3.B16, V4.B16] // q1's two nibble groups' tables
    VTBL   V1.B16, [V3.B16], V5.B16
    VTBL   V2.B16, [V4.B16], V6.B16
    WORD $0x0E25114A                // SADDW  V10.8H, V10.8H, V5.8B
    WORD $0x4E25116B                // SADDW2 V11.8H, V11.8H, V5.16B
    WORD $0x0E26114A                // SADDW  V10.8H, V10.8H, V6.8B
    WORD $0x4E26116B                // SADDW2 V11.8H, V11.8H, V6.16B
    VLD1.P 32(R9), [V3.B16, V4.B16] // q2's two nibble groups' tables
    VTBL   V1.B16, [V3.B16], V5.B16
    VTBL   V2.B16, [V4.B16], V6.B16
    WORD $0x0E25118C                // SADDW  V12.8H, V12.8H, V5.8B
    WORD $0x4E2511AD                // SADDW2 V13.8H, V13.8H, V5.16B
    WORD $0x0E26118C                // SADDW  V12.8H, V12.8H, V6.8B
    WORD $0x4E2611AD                // SADDW2 V13.8H, V13.8H, V6.16B
    VLD1.P 32(R10), [V3.B16, V4.B16]// q3's two nibble groups' tables
    VTBL   V1.B16, [V3.B16], V5.B16
    VTBL   V2.B16, [V4.B16], V6.B16
    WORD $0x0E2511CE                // SADDW  V14.8H, V14.8H, V5.8B
    WORD $0x4E2511EF                // SADDW2 V15.8H, V15.8H, V5.16B
    WORD $0x0E2611CE                // SADDW  V14.8H, V14.8H, V6.8B
    WORD $0x4E2611EF                // SADDW2 V15.8H, V15.8H, V6.16B
    SUB  $1, R7
    CBNZ R7, ts4_group

    WORD $0x0F10A500                // SSHLL  V0.4S, V8.4H, #0   tokens 0..3
    WORD $0x4F10A501                // SSHLL2 V1.4S, V8.8H, #0   tokens 4..7
    WORD $0x0F10A522                // SSHLL  V2.4S, V9.4H, #0   tokens 8..11
    WORD $0x4F10A523                // SSHLL2 V3.4S, V9.8H, #0   tokens 12..15
    WORD $0x4E21D800                // SCVTF V0.4S, V0.4S
    WORD $0x4E21D821                // SCVTF V1.4S, V1.4S
    WORD $0x4E21D842                // SCVTF V2.4S, V2.4S
    WORD $0x4E21D863                // SCVTF V3.4S, V3.4S
    MOVW  (R11), R19                // q0's scale
    VDUP  R19, V4.S4
    WORD $0x6E24DC00                // FMUL V0.4S, V0.4S, V4.4S
    WORD $0x6E24DC21                // FMUL V1.4S, V1.4S, V4.4S
    WORD $0x6E24DC42                // FMUL V2.4S, V2.4S, V4.4S
    WORD $0x6E24DC63                // FMUL V3.4S, V3.4S, V4.4S
    VLD1 (R2), [V4.S4, V5.S4, V6.S4, V7.S4]// sixteen tokens' Steps
    WORD $0x6E24DC00                // FMUL V0.4S, V0.4S, V4.4S
    WORD $0x6E25DC21                // FMUL V1.4S, V1.4S, V5.4S
    WORD $0x6E26DC42                // FMUL V2.4S, V2.4S, V6.4S
    WORD $0x6E27DC63                // FMUL V3.4S, V3.4S, V7.4S
    WORD $0x4E20F610                // FMAX V16.4S, V16.4S, V0.4S
    WORD $0x4E21F631                // FMAX V17.4S, V17.4S, V1.4S
    WORD $0x4E22F652                // FMAX V18.4S, V18.4S, V2.4S
    WORD $0x4E23F673                // FMAX V19.4S, V19.4S, V3.4S

    WORD $0x0F10A540                // SSHLL  V0.4S, V10.4H, #0   tokens 0..3
    WORD $0x4F10A541                // SSHLL2 V1.4S, V10.8H, #0   tokens 4..7
    WORD $0x0F10A562                // SSHLL  V2.4S, V11.4H, #0   tokens 8..11
    WORD $0x4F10A563                // SSHLL2 V3.4S, V11.8H, #0   tokens 12..15
    WORD $0x4E21D800                // SCVTF V0.4S, V0.4S
    WORD $0x4E21D821                // SCVTF V1.4S, V1.4S
    WORD $0x4E21D842                // SCVTF V2.4S, V2.4S
    WORD $0x4E21D863                // SCVTF V3.4S, V3.4S
    MOVW  4(R11), R19               // q1's scale
    VDUP  R19, V4.S4
    WORD $0x6E24DC00                // FMUL V0.4S, V0.4S, V4.4S
    WORD $0x6E24DC21                // FMUL V1.4S, V1.4S, V4.4S
    WORD $0x6E24DC42                // FMUL V2.4S, V2.4S, V4.4S
    WORD $0x6E24DC63                // FMUL V3.4S, V3.4S, V4.4S
    VLD1 (R2), [V4.S4, V5.S4, V6.S4, V7.S4]// sixteen tokens' Steps
    WORD $0x6E24DC00                // FMUL V0.4S, V0.4S, V4.4S
    WORD $0x6E25DC21                // FMUL V1.4S, V1.4S, V5.4S
    WORD $0x6E26DC42                // FMUL V2.4S, V2.4S, V6.4S
    WORD $0x6E27DC63                // FMUL V3.4S, V3.4S, V7.4S
    WORD $0x4E20F694                // FMAX V20.4S, V20.4S, V0.4S
    WORD $0x4E21F6B5                // FMAX V21.4S, V21.4S, V1.4S
    WORD $0x4E22F6D6                // FMAX V22.4S, V22.4S, V2.4S
    WORD $0x4E23F6F7                // FMAX V23.4S, V23.4S, V3.4S

    WORD $0x0F10A580                // SSHLL  V0.4S, V12.4H, #0   tokens 0..3
    WORD $0x4F10A581                // SSHLL2 V1.4S, V12.8H, #0   tokens 4..7
    WORD $0x0F10A5A2                // SSHLL  V2.4S, V13.4H, #0   tokens 8..11
    WORD $0x4F10A5A3                // SSHLL2 V3.4S, V13.8H, #0   tokens 12..15
    WORD $0x4E21D800                // SCVTF V0.4S, V0.4S
    WORD $0x4E21D821                // SCVTF V1.4S, V1.4S
    WORD $0x4E21D842                // SCVTF V2.4S, V2.4S
    WORD $0x4E21D863                // SCVTF V3.4S, V3.4S
    MOVW  8(R11), R19               // q2's scale
    VDUP  R19, V4.S4
    WORD $0x6E24DC00                // FMUL V0.4S, V0.4S, V4.4S
    WORD $0x6E24DC21                // FMUL V1.4S, V1.4S, V4.4S
    WORD $0x6E24DC42                // FMUL V2.4S, V2.4S, V4.4S
    WORD $0x6E24DC63                // FMUL V3.4S, V3.4S, V4.4S
    VLD1 (R2), [V4.S4, V5.S4, V6.S4, V7.S4]// sixteen tokens' Steps
    WORD $0x6E24DC00                // FMUL V0.4S, V0.4S, V4.4S
    WORD $0x6E25DC21                // FMUL V1.4S, V1.4S, V5.4S
    WORD $0x6E26DC42                // FMUL V2.4S, V2.4S, V6.4S
    WORD $0x6E27DC63                // FMUL V3.4S, V3.4S, V7.4S
    WORD $0x4E20F718                // FMAX V24.4S, V24.4S, V0.4S
    WORD $0x4E21F739                // FMAX V25.4S, V25.4S, V1.4S
    WORD $0x4E22F75A                // FMAX V26.4S, V26.4S, V2.4S
    WORD $0x4E23F77B                // FMAX V27.4S, V27.4S, V3.4S

    WORD $0x0F10A5C0                // SSHLL  V0.4S, V14.4H, #0   tokens 0..3
    WORD $0x4F10A5C1                // SSHLL2 V1.4S, V14.8H, #0   tokens 4..7
    WORD $0x0F10A5E2                // SSHLL  V2.4S, V15.4H, #0   tokens 8..11
    WORD $0x4F10A5E3                // SSHLL2 V3.4S, V15.8H, #0   tokens 12..15
    WORD $0x4E21D800                // SCVTF V0.4S, V0.4S
    WORD $0x4E21D821                // SCVTF V1.4S, V1.4S
    WORD $0x4E21D842                // SCVTF V2.4S, V2.4S
    WORD $0x4E21D863                // SCVTF V3.4S, V3.4S
    MOVW  12(R11), R19              // q3's scale
    VDUP  R19, V4.S4
    WORD $0x6E24DC00                // FMUL V0.4S, V0.4S, V4.4S
    WORD $0x6E24DC21                // FMUL V1.4S, V1.4S, V4.4S
    WORD $0x6E24DC42                // FMUL V2.4S, V2.4S, V4.4S
    WORD $0x6E24DC63                // FMUL V3.4S, V3.4S, V4.4S
    VLD1 (R2), [V4.S4, V5.S4, V6.S4, V7.S4]// sixteen tokens' Steps
    WORD $0x6E24DC00                // FMUL V0.4S, V0.4S, V4.4S
    WORD $0x6E25DC21                // FMUL V1.4S, V1.4S, V5.4S
    WORD $0x6E26DC42                // FMUL V2.4S, V2.4S, V6.4S
    WORD $0x6E27DC63                // FMUL V3.4S, V3.4S, V7.4S
    WORD $0x4E20F79C                // FMAX V28.4S, V28.4S, V0.4S
    WORD $0x4E21F7BD                // FMAX V29.4S, V29.4S, V1.4S
    WORD $0x4E22F7DE                // FMAX V30.4S, V30.4S, V2.4S
    WORD $0x4E23F7FF                // FMAX V31.4S, V31.4S, V3.4S

    ADD  $64, R2                    // one Steps block, shared by the tile
    SUB  $1, R5
    CBNZ R5, ts4_block

    VST1 [V16.S4, V17.S4, V18.S4, V19.S4], (R3)
    VST1 [V20.S4, V21.S4, V22.S4, V23.S4], (R13)
    VST1 [V24.S4, V25.S4, V26.S4, V27.S4], (R14)
    VST1 [V28.S4, V29.S4, V30.S4, V31.S4], (R15)

ts4_done:
    RET
