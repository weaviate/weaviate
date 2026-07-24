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

package compressionhelpers

import (
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasAVX2 {
		l2SquaredByteImpl = asm.L2ByteAVX256
		dotByteImpl = asm.DotByteAVX256
		hammingBitwiseImpl = asm.HammingBitwiseAVX256
		dotByteNibbleImpl = dotByteNibbleAVX2
		dotNibbleNibbleImpl = dotNibbleNibbleAVX2
		rq4QuantCorrImpl = rq4QuantCorrAVX2
		rq4MinMaxSumImpl = rq4MinMaxSumAVX2
	}
	// One VPDPBUSD replaces the widen+multiply+accumulate sequence of the
	// AVX2 byte kernels. 256-bit AVX-VNNI (Alder Lake+ client CPUs) first,
	// then the 512-bit form (Ice Lake+/Zen 4+ servers) on top.
	if cpu.X86.HasAVXVNNI {
		l2SquaredByteImpl = asm.L2ByteAVXVNNI
		dotByteImpl = asm.DotByteAVXVNNI
	}
	if cpu.X86.HasAVX512VNNI && cpu.X86.HasAVX512BW {
		l2SquaredByteImpl = asm.L2ByteVNNI512
		dotByteImpl = asm.DotByteVNNI512
		// Nibbles are non-negative as int8, so the packed 4-bit kernels use
		// VPDPBUSD without any unsigned correction: load, unpack, dot.
		dotByteNibbleImpl = dotByteNibbleVNNI512
		dotNibbleNibbleImpl = dotNibbleNibbleVNNI512
	}
}
