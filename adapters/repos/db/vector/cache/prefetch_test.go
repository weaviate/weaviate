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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

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
