//go:build arm64

#include "textflag.h"

// func Prefetch(addr uintptr)
// PRFM PLDL1KEEP: prefetch for load into L1, temporal.
TEXT ·Prefetch(SB), NOSPLIT, $0-8
	MOVD addr+0(FP), R0
	PRFM (R0), PLDL1KEEP
	RET
