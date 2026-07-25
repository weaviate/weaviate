//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build arm64

#include "textflag.h"

// func PrefetchN(addr uintptr, n int)
// Issues PRFM PLDL1KEEP hints for ceil(n/64) cache lines starting at addr,
// looping inside assembly so the caller pays one CALL for the whole range
// instead of one per line. n must be > 0.
TEXT ·PrefetchN(SB), NOSPLIT, $0-16
	MOVD addr+0(FP), R0
	MOVD n+8(FP), R1

loop:
	PRFM (R0), PLDL1KEEP
	ADD  $64, R0
	SUBS $64, R1
	BGT  loop
	RET
