//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestLazyLoadShard_ObjectCountAsync_Race(t *testing.T) {
	ctx := context.Background()
	className := "TestClass"

	// Create shard and add objects to create .cna files
	shd, idx := testShard(t, ctx, className)
	defer os.RemoveAll(shd.Index().Config.RootPath)

	// Add objects and flush to create .cna files
	for i := 0; i < 100; i++ {
		errs := shd.PutObjectBatch(ctx, []*storobj.Object{testObject(className)})
		require.NoError(t, errs[0])
		if i%5 == 0 {
			shd.Store().Bucket(helpers.ObjectsBucketLSM).FlushMemtable()
		}
	}

	require.NoError(t, idx.Shutdown(ctx))

	// Create LazyLoadShard that reads from disk
	class := idx.getSchema.ReadOnlyClass(className)
	lazyShard := NewLazyLoadShard(ctx, monitoring.GetMetrics(), shd.Name(), idx, class,
		idx.centralJobQueue, idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter,
		idx.shardReindexer, false, idx.bitmapBufPool)
	require.False(t, lazyShard.loaded.Load())

	// Start multiple concurrent ObjectCountAsync calls, then load shard to trigger race condition
	const numConcurrentCalls = 10
	var wg sync.WaitGroup
	results := make([]struct {
		count int64
		err   error
	}, numConcurrentCalls)

	wg.Add(numConcurrentCalls)
	for i := 0; i < numConcurrentCalls; i++ {
		go func(idx int) {
			defer wg.Done()
			// Small random delay to increase race condition probability
			time.Sleep(time.Duration(idx) * time.Millisecond)
			results[idx].count, results[idx].err = lazyShard.ObjectCountAsync(ctx)
		}(i)
	}

	// Load shard while ObjectCountAsync is running (triggers compaction)
	time.Sleep(5 * time.Millisecond)
	require.NoError(t, lazyShard.Load(ctx))
	wg.Wait()

	// Verify all calls complete gracefully (success or error, but no panic)
	for i, result := range results {
		if result.err != nil {
			assert.Contains(t, result.err.Error(), "error while getting object count", "goroutine %d failed", i)
		} else {
			// This is not made equal because it's fine to report eventually consistent
			// data here as this used for usage and billing purposes
			assert.Greater(t, result.count, int64(0), "goroutine %d should return positive count", i)
		}
	}
}
