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

//go:build !amd64

package simd

// No hand-written float32 Dot/L2 assembly remains outside amd64: the arm64
// NEON and SVE kernels measured slower than archsimd (see docs/simd.md).

func asmDotKernels() []asmKernel { return nil }

func asmL2Kernels() []asmKernel { return nil }
