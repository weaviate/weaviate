package compressionhelpers

import (
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
	"golang.org/x/sys/cpu"
)

func init() {
	if cpu.X86.HasAVX2 {
		l2SquaredByteImpl = asm.L2ByteAVX256
		dotByteImpl = asm.DotByteAVX256
		dotFloatByteImpl = asm.DotFloatByteAVX256
	}
}
