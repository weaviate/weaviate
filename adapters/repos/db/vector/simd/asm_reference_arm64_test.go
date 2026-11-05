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

package simd

import "github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"

func asmDotKernels() []asmDotKernel {
	return []asmDotKernel{{"asm-neon", asm.Dot_Neon}}
}
