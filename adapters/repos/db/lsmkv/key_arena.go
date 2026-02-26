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

package lsmkv

const keyArenaChunkSize = 4 * 1024 * 1024 // 4 MB

// keyArena is a chunk-based arena allocator for key copies during compaction.
// It hands out sub-slices from a large backing buffer, avoiding per-key heap
// allocations. When the current chunk is exhausted a new one is allocated;
// old chunks remain alive as long as existing sub-slice references (e.g. in
// KeyRedux.Key) keep them reachable.
type keyArena struct {
	buf []byte
	off int
}

// CopyKey allocates len(key) bytes from the arena, copies key into them,
// and returns the arena sub-slice.
func (a *keyArena) CopyKey(key []byte) []byte {
	dst := a.Alloc(len(key))
	copy(dst, key)
	return dst
}

// Alloc returns a []byte of the requested size from the arena.
func (a *keyArena) Alloc(size int) []byte {
	if a.off+size > len(a.buf) {
		newSize := keyArenaChunkSize
		if size > newSize {
			newSize = size
		}
		a.buf = make([]byte, newSize)
		a.off = 0
	}
	buf := a.buf[a.off : a.off+size]
	a.off += size
	return buf
}
