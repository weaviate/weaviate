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

//nolint:govet
//go:build ignore

package main

func main() {
	TEXT("Prefetch", NOSPLIT, "func(addr uintptr)")
	addr := Mem{Base: Load(Param("addr"), GP64())}
	_ = addr

	// L2 prefetch: T1 avoids L1-fill-buffer (MSHR) pressure when many
	// lines are hinted per code; see prefetch_n_amd64.s for the rationale.
	PREFETCHT1(addr)

	RET()

	Generate()
}
