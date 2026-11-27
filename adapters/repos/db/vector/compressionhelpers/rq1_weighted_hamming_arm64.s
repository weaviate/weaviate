//go:build arm64

#include "textflag.h"

// func weightedHammingAsm(q0, q1, q2, q3, q4, x *uint64, chunks int) int
//
// S = Σ_p 2^p · popcount(q_p XOR x) over chunks 128-bit chunks, the five
// planes of a multi-bit RQ query code against the bits of a 1-bit code, in
// one pass over x. Per chunk: a load of x and of every plane, and per plane
// an EOR, a CNT and a byte add into the plane's own counter (V20 to V24). A
// counter byte grows by at most 8 per chunk, so chunks must be 1 to 31; the
// Go wrapper splits longer codes. At the end every counter is summed across
// its lanes and the five are weighted 1, 2, 4, 8 and 16.
TEXT ·weightedHammingAsm(SB), NOSPLIT, $0-64
	MOVD q0+0(FP), R0
	MOVD q1+8(FP), R1
	MOVD q2+16(FP), R2
	MOVD q3+24(FP), R3
	MOVD q4+32(FP), R4
	MOVD x+40(FP), R5
	MOVD chunks+48(FP), R6
	VEOR V20.B16, V20.B16, V20.B16
	VEOR V21.B16, V21.B16, V21.B16
	VEOR V22.B16, V22.B16, V22.B16
	VEOR V23.B16, V23.B16, V23.B16
	VEOR V24.B16, V24.B16, V24.B16

loop:
	VLD1.P 16(R5), [V0.B16]
	VLD1.P 16(R0), [V1.B16]
	VLD1.P 16(R1), [V2.B16]
	VLD1.P 16(R2), [V3.B16]
	VLD1.P 16(R3), [V4.B16]
	VLD1.P 16(R4), [V5.B16]
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
	SUBS   $1, R6, R6
	BNE    loop

	VUADDLV V20.B16, V20
	VUADDLV V21.B16, V21
	VUADDLV V22.B16, V22
	VUADDLV V23.B16, V23
	VUADDLV V24.B16, V24
	VMOV    V20.H[0], R7
	VMOV    V21.H[0], R8
	VMOV    V22.H[0], R9
	VMOV    V23.H[0], R10
	VMOV    V24.H[0], R11
	ADD     R8<<1, R7, R7
	ADD     R9<<2, R7, R7
	ADD     R10<<3, R7, R7
	ADD     R11<<4, R7, R7
	MOVD    R7, ret+56(FP)
	RET
