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
// (below one cache line), vectors longer than prefetchMaxBytes, and
// concurrent use against Preload under the race detector. Prefetch is a
// hint, so there is nothing to assert beyond memory safety.
func TestPrefetch(t *testing.T) {
	logger, _ := test.NewNullLogger()
	vecForID := func(context.Context, uint64) ([]byte, error) { return nil, nil }
	c := NewShardedByteLockCache(vecForID, 1000, 1, logger, 0, memwatch.NewDummyMonitor())
	defer c.Drop()

	c.Preload(0, []byte{1})                   // shorter than a cache line
	c.Preload(1, make([]byte, 784))           // longer than prefetchMaxBytes
	c.Preload(2, nil)                         // explicit nil
	for _, id := range []uint64{0, 1, 2, 3} { // 3 was never loaded
		c.Prefetch(id)
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
