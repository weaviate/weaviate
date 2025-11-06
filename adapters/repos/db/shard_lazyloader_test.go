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
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/indexcheckpoint"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func TestLazyLoadShard_Load_SingleFlight(t *testing.T) {
	ctx := context.Background()
	logger := logrus.New()

	class := &models.Class{
		Class:               "TestClass",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}
	tmpDir := t.TempDir()
	promMetrics := monitoring.GetMetrics()
	jobQueueCh := make(chan job, 100)
	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})

	shardState := singleShardState()
	shardName := shardState.AllPhysicalShards()[0]
	mockSchemaGetter := &fakeSchemaGetter{
		shardState: shardState,
		schema:     schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}},
	}

	index, err := NewIndex(ctx, IndexConfig{
		RootPath:          tmpDir,
		ClassName:         schema.ClassName(class.Class),
		ReplicationFactor: 1,
		ShardLoadLimiter:  NewShardLoadLimiter(monitoring.NoopRegisterer, 10),
	}, shardState, inverted.ConfigFromModel(invertedConfig()),
		enthnsw.NewDefaultUserConfig(), nil, nil, mockSchemaGetter, nil, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, memwatch.NewDummyMonitor(), NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	indexCheckpoints, err := indexcheckpoint.New(tmpDir, logger)
	require.NoError(t, err)

	lazyShard := NewLazyLoadShard(
		ctx,
		promMetrics,
		shardName,
		index,
		class,
		jobQueueCh,
		indexCheckpoints,
		memwatch.NewDummyMonitor(),
		NewShardLoadLimiter(monitoring.NoopRegisterer, 10),
		NewShardReindexerV3Noop(),
		false,
		roaringset.NewBitmapBufPoolNoop(),
	)

	// Run a burst of concurrent Load() calls
	const goroutines = 32
	var wg sync.WaitGroup

	errs := make([]error, goroutines)
	var mu sync.Mutex
	var shardPointers []*Shard

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := lazyShard.Load(ctx); err != nil {
				errs[idx] = err
				return
			}
			mu.Lock()
			shardPointers = append(shardPointers, lazyShard.shard)
			mu.Unlock()
		}(i)
	}

	time.Sleep(10 * time.Millisecond)
	wg.Wait()

	// All calls succeeded
	for i, e := range errs {
		assert.NoError(t, e, "goroutine %d should have succeeded", i)
	}
	// Shard is loaded
	assert.True(t, lazyShard.isLoaded())
	// Everyone observed the same shard instance
	require.NotEmpty(t, shardPointers)
	first := shardPointers[0]
	require.NotNil(t, first)
	for i, s := range shardPointers {
		assert.Equal(t, first, s, "goroutine %d should see the same shard instance", i)
	}

	// Fast path: subsequent Load() is a no-op
	err = lazyShard.Load(ctx)
	assert.NoError(t, err)
	assert.True(t, lazyShard.isLoaded())
}
