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

// Package asm holds the SIMD distance kernels behind the distancer package.
//
// # Float32 kernels
//
// The float32 Dot and L2 kernels (dot_*_amd64.s, l2_*_amd64.s, *_neon_arm64.s)
// are hand-written through two small generators in this directory
// (gen_float_amd64.py, gen_float_arm64.py). They share one design:
//
//   - a main loop of W-lane blocks over C independent FMA chains (eight on
//     NEON and AVX-512, four on AVX2; 32 elements per trip on NEON and AVX2,
//     128 on AVX-512) so the loop runs at load throughput, not FMA latency;
//   - after the loop, the remainder steps down through half-width stages,
//     each into chains of its own, so no tail is a serial dependency;
//   - one overlapped masked block for the final partial vector: the last W
//     elements of both operands are reloaded and the already-counted lanes are
//     zeroed through a mask table (zeroing both operands keeps a non-finite
//     value in a counted lane from producing NaN), so there is no scalar loop;
//   - for lengths below one trip, a staged short path that uses only as many
//     chains as it has blocks, so the fold is only as deep as the work (HNSW
//     calls a kernel with one length over and over, so these branches are
//     perfectly predicted);
//   - a shuffle-and-add horizontal reduce (no VHADDPS), and VZEROUPPER before
//     every 256/512-bit return so the caller's SSE code pays no transition
//     penalty (the old avo kernels lacked it and ran at ~130 ns on Intel).
//
// Contract: the kernels read len(x) elements from both operands, so y must be
// at least as long as x; the distancer checks equal lengths before calling.
// The AVX2 and NEON kernels need len(x) >= 4, the AVX-512 kernel len(x) >= 16
// (its wrapper routes shorter vectors to the AVX2 kernel). Results are float32
// sums in a lane-wise association order, so they differ from a sequential
// loop by the usual reassociation error and must not be compared bit for bit.
//
// Vectors of up to SmallFloatMax elements skip the kernels: the wrappers
// evaluate them in Go (n = 1 and 2, the geo-coordinate case, inline).
//
// Feature gates live in the distancer package: NEON is the arm64 baseline;
// AVX2 kernels require FMA as well; the AVX-512 kernels are selected only when
// AMX-BF16 is also present, a proxy for Sapphire Rapids or newer, where 512-bit
// code carries no frequency penalty. On Zen4 the AVX-512 kernel measured equal
// to the AVX2 one (512-bit ops are double-pumped), so the gate costs nothing
// there. SVE kernels were removed after measuring slower than NEON on
// Neoverse V2.
//
// # Quantized kernels
//
// The byte, packed-nibble, VNNI, UDOT and Hamming kernels are generated from
// the C sources in ../c with goat (see the go:generate directives); they rely
// on integer dot-product instructions and keep the generated form. The
// generic dotN/l2N helpers in dot_inline.go and l2_inline.go serve their
// small-vector cases.
package asm
