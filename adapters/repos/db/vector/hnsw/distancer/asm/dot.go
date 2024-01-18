//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build ignore
// +build ignore

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	. "github.com/mmcloughlin/avo/reg"
)

var unroll = 4

// inspired by the avo example which is itself inspired by the PeachPy example.
// Adjusted unrolling that fits our use cases better.
func main() {
	TEXT("Dot", NOSPLIT, "func(x, y []float32) float32")
	x := Mem{Base: Load(Param("x").Base(), GP64())}
	y := Mem{Base: Load(Param("y").Base(), GP64())}
	n := Load(Param("x").Len(), GP64())

	acc := make([]VecVirtual, unroll)
	for i := 0; i < unroll; i++ {
		acc[i] = YMM()
	}

	for i := 0; i < unroll; i++ {
		VXORPS(acc[i], acc[i], acc[i])
	}

	blockitems := 8 * unroll
	blocksize := 4 * blockitems
	Label("blockloop")
	CMPQ(n, U32(blockitems))
	JL(LabelRef("tail"))

	// Load x.
	xs := make([]VecVirtual, unroll)
	for i := 0; i < unroll; i++ {
		xs[i] = YMM()
	}

	for i := 0; i < unroll; i++ {
		VMOVUPS(x.Offset(32*i), xs[i])
	}

	// The actual FMA.
	for i := 0; i < unroll; i++ {
		VFMADD231PS(y.Offset(32*i), xs[i], acc[i])
	}

	ADDQ(U32(blocksize), x.Base)
	ADDQ(U32(blocksize), y.Base)
	SUBQ(U32(blockitems), n)
	JMP(LabelRef("blockloop"))

	// Process any trailing entries.
	Label("tail")
	tail := XMM()
	VXORPS(tail, tail, tail)

	Label("tailloop")
	CMPQ(n, U32(0))
	JE(LabelRef("reduce"))

	xt := XMM()
	VMOVSS(x, xt)
	VFMADD231SS(y, xt, tail)

	ADDQ(U32(4), x.Base)
	ADDQ(U32(4), y.Base)
	DECQ(n)
	JMP(LabelRef("tailloop"))

	// Reduce the lanes to one.
	Label("reduce")
	if unroll != 4 {
		// we have hard-coded the reduction for this specific unrolling as it
		// allows us to do 0+1 and 2+3 and only then have a multiplication which
		// touches both.
		panic("addition is hard-coded")
	}

	// Manual reduction
	VADDPS(acc[0], acc[1], acc[0])
	VADDPS(acc[2], acc[3], acc[2])
	VADDPS(acc[0], acc[2], acc[0])

	result := acc[0].AsX()
	top := XMM()
	VEXTRACTF128(U8(1), acc[0], top)
	VADDPS(result, top, result)
	VADDPS(result, tail, result)
	VHADDPS(result, result, result)
	VHADDPS(result, result, result)
	Store(result, ReturnIndex(0))

	RET()

	Generate()
}
