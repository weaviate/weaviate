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

package cache

import (
	"context"
	"sync"
	"testing"
	"unsafe"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// TestPrefetchRangeReachesTheEndOfTheVector pins what the prefetch asks for.
// The hints themselves cannot be observed, so the test follows the loop of
// asm.PrefetchN: one hint per 64 bytes of the size, counting from the address.
// A vector that starts late in a cache line ends one line further than its
// size says, and the hints have to reach that line too.
func TestPrefetchRangeReachesTheEndOfTheVector(t *testing.T) {
	const line = 64
	tests := []struct {
		name     string
		bytes    int
		maxBytes int
	}{
		{name: "a 256d rq1 code", bytes: 40, maxBytes: compressedPrefetchMaxBytes},
		{name: "a 768d rq1 code", bytes: 104, maxBytes: compressedPrefetchMaxBytes},
		{name: "a 1536d rq1 code", bytes: 200, maxBytes: compressedPrefetchMaxBytes},
		{name: "a 1536d 4-bit rq code", bytes: 784, maxBytes: compressedPrefetchMaxBytes},
		{name: "longer than the cap", bytes: compressedPrefetchMaxBytes + 64, maxBytes: compressedPrefetchMaxBytes},
		{name: "the head of a float vector", bytes: 6144, maxBytes: floatPrefetchMaxBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// words, so that the same memory serves the byte and the uint64 view
			backing := make([]uint64, (tt.bytes+2*line)/8+1)
			buf := unsafe.Slice((*byte)(unsafe.Pointer(&backing[0])), len(backing)*8)
			aligned := int((line - uintptr(unsafe.Pointer(&buf[0]))%line) % line)

			for offset := 0; offset < line; offset += 8 {
				vec := buf[aligned+offset : aligned+offset+tt.bytes]
				addr, size := prefetchRange(vec, tt.maxBytes)
				assertHintsCover(t, addr, size, uintptr(unsafe.Pointer(&vec[0])), min(tt.bytes, tt.maxBytes), offset)

				if tt.bytes%8 != 0 {
					continue
				}
				words := backing[(aligned+offset)/8 : (aligned+offset+tt.bytes)/8]
				addr, size = prefetchRange(words, tt.maxBytes)
				assertHintsCover(t, addr, size, uintptr(unsafe.Pointer(&words[0])), min(tt.bytes, tt.maxBytes), offset)
			}
		})
	}
}

// assertHintsCover checks that the hints for (addr, size) cover the n bytes
// at start and nothing after them.
func assertHintsCover(t *testing.T, addr uintptr, size int, start uintptr, n, offset int) {
	t.Helper()
	const line = 64
	hinted := map[uintptr]bool{}
	for left := size; left > 0; left -= line {
		hinted[addr/line] = true
		addr += line
	}
	first, last := start/line, (start+uintptr(n)-1)/line
	for l := first; l <= last; l++ {
		assert.True(t, hinted[l], "offset %d: line %d of the vector is not hinted", offset, l-first)
	}
	assert.Len(t, hinted, int(last-first)+1, "offset %d: hints beyond the vector", offset)
}

// TestPrefetch exercises the data-prefetch path: empty slots, short vectors
// (below one cache line), vectors longer than the cache's prefetch cap, and
// concurrent use against Preload under the race detector. Prefetch is a
// hint, so there is nothing to assert beyond memory safety.
func TestPrefetch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	vecForID := func(context.Context, uint64) ([]byte, error) { return nil, nil }
	c := NewShardedByteLockCache(vecForID, 1000, 1, logger, 0, memwatch.NewDummyMonitor())
	defer c.Drop()

	c.Preload(0, []byte{1})                                   // shorter than a cache line
	c.Preload(1, make([]byte, 784))                           // a d1536 4-bit code, fully covered
	c.Preload(2, nil)                                         // explicit nil
	c.Preload(4, make([]byte, compressedPrefetchMaxBytes+64)) // longer than the cap
	c.Preload(5, make([]byte, 63))                            // one line, sub-line tail
	c.Preload(6, make([]byte, 64))                            // exactly one line
	c.Preload(7, make([]byte, 65))                            // one line + 1 byte
	c.Preload(8, make([]byte, 1552))                          // a d1536 8-bit RQ code
	for _, id := range []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8} {  // 3 was never loaded
		c.Prefetch(id)
	}

	// Out-of-range ids must be a safe no-op for Prefetch and return nil from
	// PrefetchGet, on both cache flavors.
	outOfRange := uint64(InitialSize + 10)
	c.Prefetch(outOfRange)
	if got := c.PrefetchGet(outOfRange); got != nil {
		t.Errorf("PrefetchGet(%d) = %v, want nil", outOfRange, got)
	}
	mc := NewShardedMultiByteLockCache(vecForID, 1000, logger, 0, memwatch.NewDummyMonitor())
	defer mc.Drop()
	mc.Prefetch(outOfRange)
	if got := mc.PrefetchGet(outOfRange); got != nil {
		t.Errorf("multi PrefetchGet(%d) = %v, want nil", outOfRange, got)
	}

	// PrefetchGet returns the cached slice without loading; empty slots are nil.
	if got := c.PrefetchGet(1); len(got) != 784 {
		t.Errorf("PrefetchGet(1) len = %d, want 784", len(got))
	}
	if got := c.PrefetchGet(3); got != nil { // never loaded
		t.Errorf("PrefetchGet(3) = %v, want nil", got)
	}

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := range 500 {
				id := uint64((seed*500 + i) % 900)
				if i%3 == 0 {
					c.Preload(id, make([]byte, 64))
				} else {
					c.Prefetch(id)
				}
			}
		}(w)
	}
	wg.Wait()
}
