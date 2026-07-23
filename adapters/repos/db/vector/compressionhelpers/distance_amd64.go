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
		l2SquaredByteImpl = l2ByteWideAVX2
		dotByteImpl = dotByteWideAVX2
		hammingBitwiseImpl = asm.HammingBitwiseAVX256
		dotByteNibbleImpl = dotByteNibbleAVX2
		dotNibbleNibbleImpl = dotNibbleNibbleAVX2
	}
}
