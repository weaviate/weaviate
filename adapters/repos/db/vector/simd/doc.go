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

// Package simd provides vectorized float32 and int8 slice kernels for the
// vector index and quantizer code paths: elementwise arithmetic, reductions,
// and the quantize/dequantize conversions used by the rotational quantizers.
//
// Every exported function has a portable Go implementation. When the toolchain
// is built with GOEXPERIMENT=simd, the package additionally compiles kernels
// written against the standard library's simd/archsimd intrinsics and selects
// them at init based on the CPU: AVX2+FMA and AVX-512 tiers on amd64, NEON on
// arm64. Without the experiment (or on other architectures) the Go kernels are
// used and the API is unchanged, so GOEXPERIMENT=simd is a pure performance
// switch, never a correctness one.
//
// # Length and aliasing contract
//
// Every function processes n = min(len(...)) elements over all of its slice
// arguments and ignores the rest, so callers never need to pre-slice. The
// destination of an elementwise operation may be exactly one of its inputs
// (dst == a or dst == b, in-place), because every lane is loaded before any
// lane of the same block is stored. A destination that overlaps an input at a
// shifted offset is not supported.
//
// # Rounding contract
//
// Elementwise operations, Int32ToFloat32Scale and Float32ToInt32ScaleClamp
// are bit-identical across all kernels: each element is one IEEE operation
// (or, for the clamp, the documented fixed sequence). Reductions (Sum,
// SumOfSquares, DotProduct) accumulate in lanes and may fuse multiply-add, so
// their results differ from a sequential scalar loop by the usual
// reassociation error; callers must not depend on the exact bits. Min, Max and
// MaxAbs are exact for inputs without NaN; NaN handling is unspecified.
package simd
