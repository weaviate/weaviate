package distancer

import (
	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"
)

func init() {
	if cpu.X86.HasAVX2 {
		dotProductImplementation = asm.Dot
	} 
}
