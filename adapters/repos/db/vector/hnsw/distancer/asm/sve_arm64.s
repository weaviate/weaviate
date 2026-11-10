//go:build arm64

#include "textflag.h"

// func SVEVectorLengthBytes() int
TEXT ·SVEVectorLengthBytes(SB), NOSPLIT, $0-8
	WORD $0x04bf5020 // rdvl x0, #1
	MOVD R0, ret+0(FP)
	RET
