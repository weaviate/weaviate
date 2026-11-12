//go:build amd64

#include "textflag.h"

// func weightedHammingAsm(q0, q1, q2, q3, q4, x *uint64, chunks int) int
//
// S = Σ_p 2^p · popcount(q_p XOR x) over chunks 256-bit chunks, the five
// planes of a multi-bit RQ query code against the bits of a 1-bit code, in
// one pass over x. See rq1_weighted_hamming_arm64.s for the NEON twin.
//
// AVX2 has no vector popcount: the bits of every byte are counted with a
// 16-entry table looked up with VPSHUFB, once for the low nibble and once for
// the high one (the method of Muła, Kurz and Lemire). Per chunk the five
// byte counts, at most 8 each, are folded highest plane first, doubling the
// sum before adding the next plane, into one byte of at most 8·31 = 248, which
// VPSADBW sums into four 64-bit lanes. The 64-bit sums cannot overflow, so
// any chunks >= 1 goes in one call.

DATA popcntNibble<>+0(SB)/8, $0x0302020102010100
DATA popcntNibble<>+8(SB)/8, $0x0403030203020201
DATA popcntNibble<>+16(SB)/8, $0x0302020102010100
DATA popcntNibble<>+24(SB)/8, $0x0403030203020201
GLOBL popcntNibble<>(SB), RODATA|NOPTR, $32

DATA lowNibbles<>+0(SB)/8, $0x0f0f0f0f0f0f0f0f
DATA lowNibbles<>+8(SB)/8, $0x0f0f0f0f0f0f0f0f
DATA lowNibbles<>+16(SB)/8, $0x0f0f0f0f0f0f0f0f
DATA lowNibbles<>+24(SB)/8, $0x0f0f0f0f0f0f0f0f
GLOBL lowNibbles<>(SB), RODATA|NOPTR, $32

// POPCNT_BYTES replaces every byte of in with the number of its set bits,
// with lo and hi as scratch.
#define POPCNT_BYTES(in, lo, hi) \
	VPAND   Y14, in, lo  \
	VPSRLW  $4, in, hi   \
	VPAND   Y14, hi, hi  \
	VPSHUFB lo, Y15, lo  \
	VPSHUFB hi, Y15, hi  \
	VPADDB  lo, hi, in

TEXT ·weightedHammingAsm(SB), NOSPLIT, $0-64
	MOVQ q0+0(FP), AX
	MOVQ q1+8(FP), BX
	MOVQ q2+16(FP), CX
	MOVQ q3+24(FP), DX
	MOVQ q4+32(FP), SI
	MOVQ x+40(FP), DI
	MOVQ chunks+48(FP), R8
	VMOVDQU popcntNibble<>(SB), Y15
	VMOVDQU lowNibbles<>(SB), Y14
	VPXOR   Y13, Y13, Y13
	VPXOR   Y12, Y12, Y12

loop:
	VMOVDQU (DI), Y0

	// plane 4, the highest, starts the fold
	VPXOR (SI), Y0, Y1
	POPCNT_BYTES(Y1, Y2, Y3)

	VPXOR  (DX), Y0, Y4
	POPCNT_BYTES(Y4, Y2, Y3)
	VPADDB Y1, Y1, Y1
	VPADDB Y4, Y1, Y1

	VPXOR  (CX), Y0, Y4
	POPCNT_BYTES(Y4, Y2, Y3)
	VPADDB Y1, Y1, Y1
	VPADDB Y4, Y1, Y1

	VPXOR  (BX), Y0, Y4
	POPCNT_BYTES(Y4, Y2, Y3)
	VPADDB Y1, Y1, Y1
	VPADDB Y4, Y1, Y1

	VPXOR  (AX), Y0, Y4
	POPCNT_BYTES(Y4, Y2, Y3)
	VPADDB Y1, Y1, Y1
	VPADDB Y4, Y1, Y1

	VPSADBW Y13, Y1, Y1
	VPADDQ  Y1, Y12, Y12

	ADDQ $32, AX
	ADDQ $32, BX
	ADDQ $32, CX
	ADDQ $32, DX
	ADDQ $32, SI
	ADDQ $32, DI
	DECQ R8
	JNZ  loop

	VEXTRACTI128 $1, Y12, X1
	VPADDQ       X1, X12, X12
	VPSHUFD      $0x4e, X12, X1
	VPADDQ       X1, X12, X12
	VMOVQ        X12, R9
	MOVQ         R9, ret+56(FP)
	VZEROUPPER
	RET
