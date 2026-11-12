//go:build arm64

#include "textflag.h"

// func fusedNEON(q, x *uint64, chunks int) int
//
// Per 128-bit chunk of the code: one load of the code, one of the five query
// planes over it, and per plane an EOR, a CNT and a byte add into the
// plane's own counter. At the end each counter is summed across its lanes and
// the five are weighted 1, 2, 4, 8 and 16.
TEXT ·fusedNEON(SB), NOSPLIT, $0-32
	MOVD q+0(FP), R0
	MOVD x+8(FP), R1
	MOVD chunks+16(FP), R2
	VEOR V20.B16, V20.B16, V20.B16
	VEOR V21.B16, V21.B16, V21.B16
	VEOR V22.B16, V22.B16, V22.B16
	VEOR V23.B16, V23.B16, V23.B16
	VEOR V24.B16, V24.B16, V24.B16

loop:
	VLD1.P 16(R1), [V0.B16]
	VLD1.P 64(R0), [V1.B16, V2.B16, V3.B16, V4.B16]
	VLD1.P 16(R0), [V5.B16]
	VEOR   V0.B16, V1.B16, V1.B16
	VEOR   V0.B16, V2.B16, V2.B16
	VEOR   V0.B16, V3.B16, V3.B16
	VEOR   V0.B16, V4.B16, V4.B16
	VEOR   V0.B16, V5.B16, V5.B16
	VCNT   V1.B16, V1.B16
	VCNT   V2.B16, V2.B16
	VCNT   V3.B16, V3.B16
	VCNT   V4.B16, V4.B16
	VCNT   V5.B16, V5.B16
	VADD   V1.B16, V20.B16, V20.B16
	VADD   V2.B16, V21.B16, V21.B16
	VADD   V3.B16, V22.B16, V22.B16
	VADD   V4.B16, V23.B16, V23.B16
	VADD   V5.B16, V24.B16, V24.B16
	SUBS   $1, R2, R2
	BNE    loop

	VUADDLV V20.B16, V20
	VUADDLV V21.B16, V21
	VUADDLV V22.B16, V22
	VUADDLV V23.B16, V23
	VUADDLV V24.B16, V24
	VMOV    V20.H[0], R3
	VMOV    V21.H[0], R4
	VMOV    V22.H[0], R5
	VMOV    V23.H[0], R6
	VMOV    V24.H[0], R7
	ADD     R4<<1, R3, R3
	ADD     R5<<2, R3, R3
	ADD     R6<<3, R3, R3
	ADD     R7<<4, R3, R3
	MOVD    R3, ret+24(FP)
	RET

// func scanBlockNEON(lut, nibbles *uint8, groups int, s *uint16)
//
// Per group of 4 dimensions: one load of the packed nibbles of the 32 codes,
// one of the group's table, an AND and a shift to split the nibbles of codes
// 0 to 15 from those of codes 16 to 31, a TBL for each half, and four
// widening adds into 16-bit sums, 8 codes each.
TEXT ·scanBlockNEON(SB), NOSPLIT, $0-32
	MOVD lut+0(FP), R0
	MOVD nibbles+8(FP), R1
	MOVD groups+16(FP), R2
	MOVD s+24(FP), R3
	VMOVI $15, V31.B16
	VEOR  V16.B16, V16.B16, V16.B16
	VEOR  V17.B16, V17.B16, V17.B16
	VEOR  V18.B16, V18.B16, V18.B16
	VEOR  V19.B16, V19.B16, V19.B16

scan:
	VLD1.P  16(R1), [V0.B16]
	VLD1.P  16(R0), [V1.B16]
	VAND    V31.B16, V0.B16, V2.B16
	VUSHR   $4, V0.B16, V3.B16
	VTBL    V2.B16, [V1.B16], V4.B16
	VTBL    V3.B16, [V1.B16], V5.B16
	VUADDW  V4.B8, V16.H8, V16.H8
	VUADDW2 V4.B16, V17.H8, V17.H8
	VUADDW  V5.B8, V18.H8, V18.H8
	VUADDW2 V5.B16, V19.H8, V19.H8
	SUBS    $1, R2, R2
	BNE     scan

	VST1 [V16.H8, V17.H8, V18.H8, V19.H8], (R3)
	RET
