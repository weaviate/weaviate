package hnsw

import "github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer/asm"

func init() {
	prefetchFunc = asm.Prefetch
}
