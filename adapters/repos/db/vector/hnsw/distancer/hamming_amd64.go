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
	"golang.org/x/sys/cpu"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

func init() {
	if cpu.X86.HasAMXBF16 && cpu.X86.HasAVX512 {
		hammingImpl = asm.HammingAVX512
	} else if cpu.X86.HasAVX2 {
		hammingImpl = asm.HammingAVX256
	}
	if cpu.X86.HasAVX2 {
		hammingBitwiseImpl = asm.HammingBitwiseAVX256
	}
}
