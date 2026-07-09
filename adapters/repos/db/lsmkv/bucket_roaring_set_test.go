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

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestRoaringSetWritePathRefCount ensures that all write paths of the
// RoaringSet type correctly use and release refcounts on the active memtable
// and therefore do not block a flushlock for the entire duration of the wrige.
func TestRoaringSetWritePathRefCount(t *testing.T) {
	b := Bucket{
		strategy: StrategyRoaringSet,
		disk:     &SegmentGroup{segments: []Segment{}},
		active:   newTestMemtableRoaringSet(nil),
	}

	expectedRefs := 0
	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}

	// add one
	err := b.RoaringSetAddOne([]byte("key1"), 1)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add list
	err = b.RoaringSetAddList([]byte("key1"), []uint64{2, 3, 4})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add bitmap
	err = b.RoaringSetAddBitmap([]byte("key1"), bitmapFromSlice([]uint64{5, 6, 7}))
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// remove one
	err = b.RoaringSetRemoveOne([]byte("key1"), 2)
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// sanity check, final state:
	v, releaseBufPol, err := b.RoaringSetGet(context.Background(), []byte("key1"))
	defer releaseBufPol()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3, 4, 5, 6, 7}, v.ToArray())
}

// TestBucket_RoaringSetGet_RespectsConcurrencyBudget proves the per-query
// budget threaded into RoaringSetGet actually caps sroar's internal merge
// fan-out. With a budget of 1 the *Conc merge ops run single-threaded, so
// hammering the bucket from K callers cannot inflate the live goroutine count
// beyond K (plus slack), no matter how many disk segments or containers are
// involved. A regression that stopped honoring the budget would let each Get
// fan out ~SROAR_MERGE workers per merge and blow the ceiling.
func TestBucket_RoaringSetGet_RespectsConcurrencyBudget(t *testing.T) {
	if concurrency.SROAR_MERGE < 2 {
		t.Skipf("SROAR_MERGE=%d < 2: no merge fan-out possible, nothing to bound",
			concurrency.SROAR_MERGE)
	}

	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	tmpDir := t.TempDir()

	b, err := NewBucketCreator().NewBucket(ctx, tmpDir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet),
		WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })

	// never auto-flush; we flush explicitly to control the disk segment count
	b.SetMemtableThreshold(1e9)

	// one value per sroar container (stride 2^16) => ~200 containers, enough
	// that the *Conc ops want min(200/24, SROAR_MERGE) ~ 8 > 1 merge workers
	const numContainers = 200
	values := make([]uint64, numContainers)
	for i := range values {
		values[i] = uint64(i) << 16
	}

	key := []byte("key")

	// >= 8 disk segments so each Get runs several segment merges
	const numSegments = 8
	for s := 0; s < numSegments; s++ {
		require.NoError(t, b.RoaringSetAddList(key, values))
		require.NoError(t, b.FlushAndSwitch())
	}

	budget1 := concurrency.CtxWithBudget(ctx, 1)

	// correctness: a budget of 1 returns exactly the same set as an
	// unconstrained query (merges are deterministic regardless of concurrency)
	got1, release1, err := b.RoaringSetGet(budget1, key)
	require.NoError(t, err)
	arr1 := got1.ToArray()
	release1()

	gotDefault, releaseDefault, err := b.RoaringSetGet(ctx, key)
	require.NoError(t, err)
	arrDefault := gotDefault.ToArray()
	releaseDefault()

	require.Equal(t, values, arr1)
	require.Equal(t, arrDefault, arr1)

	// bounding leg
	const (
		numWorkers = 16
		runFor     = 200 * time.Millisecond
		// slack absorbs the sampler goroutine and transient runtime/GC workers
		slack = 8
	)

	stop := make(chan struct{})
	samplerDone := make(chan struct{})
	var maxSeen int // written only by the sampler, read after samplerDone closes

	go func() {
		defer close(samplerDone)
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if n := runtime.NumGoroutine(); n > maxSeen {
					maxSeen = n
				}
			}
		}
	}()

	// let the sampler settle, then baseline (includes the sampler, excludes the
	// workers we are about to launch)
	time.Sleep(5 * time.Millisecond)
	base := runtime.NumGoroutine()

	firstErr := make(chan error, 1)
	var wg sync.WaitGroup
	deadline := time.Now().Add(runFor)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				bm, release, err := b.RoaringSetGet(budget1, key)
				if err != nil {
					select {
					case firstErr <- err:
					default:
					}
					return
				}
				_ = bm
				release()
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-samplerDone

	select {
	case err := <-firstErr:
		require.NoError(t, err)
	default:
	}

	ceiling := base + numWorkers + slack
	assert.LessOrEqualf(t, maxSeen, ceiling,
		"live goroutines peaked at %d, above base(%d)+workers(%d)+slack(%d)=%d; "+
			"a budget of 1 must not fan out sroar merge workers",
		maxSeen, base, numWorkers, slack, ceiling)
}
