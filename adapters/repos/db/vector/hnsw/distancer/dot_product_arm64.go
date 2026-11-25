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

package distancer

import (
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"golang.org/x/sys/cpu"
)

func init() {
	// The SVE dot kernel pays off only with 256-bit vectors (Neoverse V1,
	// Graviton3). With 128-bit SVE (Neoverse V2: Axion, Graviton4) it is 2.3x
	// slower than the NEON kernel at 1536 dims, so the vector length decides.
	// L2 (l2_arm64.go) keeps SVE whenever present: there the SVE kernel is
	// faster than the NEON one on both cores.
	if cpu.ARM64.HasASIMD {
		dotProductImplementation = asm.Dot_Neon
		if cpu.ARM64.HasSVE && asm.SVEVectorLengthBytes() >= 32 {
			dotProductImplementation = asm.Dot_SVE
		}
	}
}
