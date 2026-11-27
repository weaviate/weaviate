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

// Package kernelproto holds prototypes of the kernels that estimate the
// distance from a query to centered 1-bit RQ codes (CRQ1), to be measured
// against the kernel the indexes use today. Nothing outside its tests calls
// it: it is the measurement behind "Coste por distancia" in
// postingedge/bench-report.md, and a starting point if one of the kernels is
// taken into compressionhelpers.
//
// A CRQ1 query is a 5-bit value per dimension, held as five bit planes, and a
// data code is one sign bit per dimension after a header word. The integer at
// the core of the estimate is
//
//	S = Σ_p 2^p · popcount(q_p XOR x)
//
// from which the dot product of the two codes is 31·D - 2·S, D the
// dimensions. compressionhelpers.CenteredBinaryRQDistancer computes it with
// five Hamming distances, one per plane, each a call of its own that reads
// the whole code again. The prototypes compute it:
//
//   - Fused: in one pass over the code for the five planes, with NEON on
//     arm64.
//   - ScanBlock (FastScan): for 32 codes at once, laid out by groups of 4
//     dimensions, with a 16-entry table per group that a TBL looks up for 16
//     codes in one instruction. Per dimension, S adds 31 - v when the code
//     bit is set and v when it is not, v the query value, so a group adds one
//     of 16 sums that only depend on the query.
//
// The kernels give the same S as the five Hamming distances, bit for bit; the
// tests hold them to it.
package kernelproto
