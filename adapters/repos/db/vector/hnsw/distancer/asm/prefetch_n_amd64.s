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

#include "textflag.h"

// func PrefetchN(addr uintptr, n int)
// Issues PREFETCHT0 hints for ceil(n/64) cache lines starting at addr,
// looping inside assembly so the caller pays one CALL for the whole range
// instead of one per line. n must be > 0.
// (A 128B stride relying on the L2 spatial prefetcher to complete line
// pairs was tried and is a wash: the pair completion lands in L2 only, so
// the distance kernel trades prefetch-issue time for L1 misses.)
TEXT ·PrefetchN(SB), NOSPLIT, $0-16
	MOVQ addr+0(FP), AX
	MOVQ n+8(FP), CX

loop:
	PREFETCHT0 (AX)
	ADDQ       $64, AX
	SUBQ       $64, CX
	JGT        loop
	RET
