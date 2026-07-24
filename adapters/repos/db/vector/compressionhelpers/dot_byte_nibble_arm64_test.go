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

package compressionhelpers

import (
	"golang.org/x/sys/cpu"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

// Both NEON variants are testable on any ASIMD machine; UDOT additionally
// needs the DotProd extension.
func dotByteNibbleVariantsUnderTest() map[string]func(q, packed []byte) uint32 {
	variants := map[string]func(q, packed []byte) uint32{
		"uadalp": dotByteNibbleUADALP,
	}
	if cpu.ARM64.HasASIMDDP {
		variants["udot"] = dotByteNibbleUDOT
	}
	return variants
}

func dotNibbleNibbleVariantsUnderTest() map[string]func(a, b []byte) uint32 {
	variants := map[string]func(a, b []byte) uint32{
		"uadalp": dotNibbleNibbleUADALP,
	}
	if cpu.ARM64.HasASIMDDP {
		variants["udot"] = dotNibbleNibbleUDOT
	}
	return variants
}

func dotByteVariantsUnderTest() map[string]func(a, b []byte) uint32 {
	variants := map[string]func(a, b []byte) uint32{}
	if cpu.ARM64.HasASIMD {
		variants["goat"] = asm.DotByteARM64
	}
	if cpu.ARM64.HasASIMDDP {
		variants["udot"] = dotByteUDOT
	}
	return variants
}

func l2ByteVariantsUnderTest() map[string]func(a, b []byte) uint32 {
	variants := map[string]func(a, b []byte) uint32{}
	if cpu.ARM64.HasASIMD {
		variants["goat"] = asm.L2ByteARM64
	}
	if cpu.ARM64.HasASIMDDP {
		variants["udot"] = l2ByteUDOT
	}
	return variants
}
