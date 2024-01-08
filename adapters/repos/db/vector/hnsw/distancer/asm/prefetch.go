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
	// . "github.com/mmcloughlin/avo/reg"
)

func main() {
	TEXT("Prefetch", NOSPLIT, "func(addr uintptr)")
	addr := Mem{Base: Load(Param("addr"), GP64())}
	_ = addr

	PREFETCHT0(addr)

	RET()

	Generate()
}
