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

package aggregator

import (
	"context"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/schema"
)

// approximateCardinality must pin buckets via AcquireBucketForRead: swap-style
// teardowns (reindex ReplaceBuckets) deregister the bucket, drain, shut it
// down, and delete its directory, and a cold lazy segment loaded by an
// unpinned in-flight reader then panics on the missing files. Smoke-guards the
// reader against teardown/reload cycles under -race; the deterministic drain
// semantics are pinned in lsmkv's store_acquire_drain_test.go.
func TestApproximateCardinality_ConcurrentBucketTeardown(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroup("compactionObjects", logger, 1),
		cyclemanager.NewCallbackGroup("compactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	const prop = "title"
	bucketName := helpers.BucketFromPropNameLSM(prop)
	load := func() {
		require.NoError(t, store.CreateOrLoadBucket(ctx, bucketName,
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			lsmkv.WithUseBloomFilter(true)))
	}
	load()

	b := store.Bucket(bucketName)
	const distinct = 2000
	for i := 0; i < distinct; i++ {
		key := []byte(fmt.Sprintf("value-%06d", i))
		require.NoError(t, b.RoaringSetAddList(key, []uint64{uint64(i)}))
	}
	require.NoError(t, b.FlushAndSwitch())

	agg := &Aggregator{store: store}

	// the reader reports no estimate for a bucket it cannot find, so without a
	// baseline a name it never resolves would churn through the loop unexercised
	est, err := agg.approximateCardinality(schema.PropertyName(prop))
	require.NoError(t, err)
	require.NotNil(t, est)
	require.InDelta(t, distinct, float64(*est), distinct*0.05)

	stop := make(chan struct{})
	done := make(chan struct{})
	var estimated int
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// A nil estimate mid-teardown is fine; racing or crashing is not.
			est, err := agg.approximateCardinality(schema.PropertyName(prop))
			if err == nil && est != nil {
				estimated++
			}
		}
	}()

	for i := 0; i < 30; i++ {
		require.NoError(t, store.ShutdownBucket(ctx, bucketName))
		load()
	}

	close(stop)
	<-done
	require.Positive(t, estimated, "reader never reached a loaded bucket")
}
