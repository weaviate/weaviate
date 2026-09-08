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

package replica

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPrefilterShardRootsAllocsScaleWithInput: per-call allocations must scale with len(roots), not prefilterMaxShardsPerRPC.
func TestPrefilterShardRootsAllocsScaleWithInput(t *testing.T) {
	f := &Finder{}
	ctx := context.Background()
	f.PrefilterShardRoots(ctx, nil)

	const iters = 100
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range iters {
		f.PrefilterShardRoots(ctx, nil)
	}
	runtime.ReadMemStats(&after)

	perCall := (after.TotalAlloc - before.TotalAlloc) / iters
	assert.Less(t, perCall, uint64(8*1024))
}
