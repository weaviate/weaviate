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
