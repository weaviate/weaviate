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

//go:build integrationTest

package lsmkv

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

var logger, _ = test.NewNullLogger()

// newTestBucketWithFlushCycle creates a bucket with a started flush cycle
// and registers teardown via t.Cleanup immediately after each resource is
// created. This guarantees the cyclemanager goroutine and bucket are
// shut down even if a later require fails - which is critical inside a
// synctest bubble, where a leaked goroutine would mask the original
// failure as a synctest leak/deadlock.
func newTestBucketWithFlushCycle(t *testing.T, opts ...BucketOption) *Bucket {
	t.Helper()

	dirName := t.TempDir()

	flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	flushCycle := cyclemanager.NewManager("flush", cyclemanager.MemtableFlushCycleTicker(false), flushCallbacks.CycleCallback, logger)
	flushCycle.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		require.Nil(t, flushCycle.StopAndWait(ctx))
	})

	bucket, err := NewBucketCreator().NewBucket(testCtx(), dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), flushCallbacks,
		opts...,
	)
	require.Nil(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		require.Nil(t, bucket.Shutdown(ctx))
	})

	return bucket
}

// This test ensures that the WAL threshold is being adhered to, and that a
// flush to segment followed by a switch to a new WAL is being performed
// once the threshold is reached
func TestWriteAheadLogThreshold_Replace(t *testing.T) {
	amount := 100
	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	walThreshold := uint64(4096)
	tolerance := 4.

	// generate only a small amount of sequential values. this allows
	// us to keep the memtable small (the net additions will be close
	// to zero), and focus on testing the WAL threshold
	for i := range keys {
		n, err := json.Marshal(i)
		require.Nil(t, err)

		keys[i], values[i] = n, n
	}

	synctest.Test(t, func(t *testing.T) {
		bucket := newTestBucketWithFlushCycle(t,
			WithStrategy(StrategyReplace),
			WithMemtableThreshold(1024*1024*1024),
			WithWalThreshold(walThreshold),
			WithMinWalThreshold(0), // small enough to not affect this test
		)

		bucket.flushLock.RLock()
		initialWalFile := bucket.active.commitlogWalPath()
		bucket.flushLock.RUnlock()

		// Write pacing is 1.6ms per put; the flush ticker fires every 100ms.
		// Each pass through the cycle callback checks WAL size against the
		// threshold and switches the commitlog when exceeded. We observe the
		// WAL size after each write and keep the last observation before the
		// WAL path changes - that is the size at (or just before) the switch.
		const maxIterations = 5000 // bounded by virtual time; ample to cross the threshold
		var sizeBeforeSwitch int64
		switched := false
		for i := 0; i < maxIterations; i++ {
			require.Nil(t, bucket.Put(keys[i%amount], values[i%amount]))
			time.Sleep(1600 * time.Microsecond)

			bucket.flushLock.RLock()
			currentWalFile := bucket.active.commitlogWalPath()
			currentWalSize := bucket.active.commitlogSize()
			bucket.flushLock.RUnlock()

			if currentWalFile != initialWalFile {
				switched = true
				break
			}
			sizeBeforeSwitch = currentWalSize
		}

		require.Truef(t, switched,
			"WAL was never switched; last observed size was (%d)", sizeBeforeSwitch)
		require.Truef(t, isSizeWithinTolerance(t, uint64(sizeBeforeSwitch), walThreshold, tolerance),
			"WAL size (%d) was allowed to increase beyond threshold (%d) with tolerance of (%f)%%",
			sizeBeforeSwitch, walThreshold, tolerance*100)
	})
}

// This test ensures that the Memtable threshold is being adhered to, and
// that a flush to segment followed by a switch to a new WAL is being
// performed once the threshold is reached
func TestMemtableThreshold_Replace(t *testing.T) {
	amount := 10000
	sizePerValue := 8

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	memtableThreshold := uint64(4096)
	tolerance := 4.

	for i := range keys {
		n, err := json.Marshal(i)
		require.Nil(t, err)

		keys[i] = n
		values[i] = make([]byte, sizePerValue)
		rand.Read(values[i])
	}

	synctest.Test(t, func(t *testing.T) {
		bucket := newTestBucketWithFlushCycle(t,
			WithStrategy(StrategyReplace),
			WithMemtableThreshold(memtableThreshold),
			WithMinWalThreshold(0),
		)

		bucket.flushLock.RLock()
		initialPath := bucket.active.Path()
		bucket.flushLock.RUnlock()

		// Write pacing is 0.8ms per put; the flush ticker fires every 100ms.
		// Each pass through the cycle callback checks memtable size against the
		// threshold and flushes when exceeded. We observe the memtable size after
		// each write and keep the last observation before the active path
		// changes - that is the size at (or just before) the flush moment.
		var sizeBeforeFlush uint64
		flushed := false
		for i := 0; i < amount; i++ {
			require.Nil(t, bucket.Put(keys[i], values[i]))
			time.Sleep(800 * time.Microsecond)

			bucket.flushLock.RLock()
			currentPath := bucket.active.Path()
			currentSize := bucket.active.Size()
			bucket.flushLock.RUnlock()

			if currentPath != initialPath {
				flushed = true
				break
			}
			sizeBeforeFlush = currentSize
		}

		require.Truef(t, flushed,
			"Memtable was never flushed; last observed size was (%d)", sizeBeforeFlush)
		require.Truef(t, isSizeWithinTolerance(t, sizeBeforeFlush, memtableThreshold, tolerance),
			"Memtable size (%d) was allowed to increase beyond threshold (%d) with tolerance of (%f)%%",
			sizeBeforeFlush, memtableThreshold, tolerance*100)
	})
}

func isSizeWithinTolerance(t *testing.T, detectedSize uint64, threshold uint64, tolerance float64) bool {
	return detectedSize > 0 && float64(detectedSize) <= float64(threshold)*(tolerance+1)
}

func assertSegmentCount(t *testing.T, bucket *Bucket, expectedFn func(t *testing.T, count int)) {
	t.Helper()

	segments, release := mustSegmentView(t, bucket.disk)
	defer release()

	expectedFn(t, len(segments))
}

func TestMemtableFlushesIfDirty(t *testing.T) {
	t.Run("an empty memtable is not flushed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			bucket := newTestBucketWithFlushCycle(t,
				WithStrategy(StrategyReplace),
				WithMemtableThreshold(1e12), // large enough to not affect this test
				WithWalThreshold(1e12),      // large enough to not affect this test
				WithMinWalThreshold(0),      // small enough to not affect this test
				WithDirtyThreshold(10*time.Millisecond),
			)

			assertSegmentCount(t, bucket, func(t *testing.T, count int) {
				assert.Equal(t, 0, count)
			})

			// wait (virtual) until past dirty threshold and let flush cycle fire
			time.Sleep(200 * time.Millisecond)
			synctest.Wait()

			assertSegmentCount(t, bucket, func(t *testing.T, count int) {
				assert.Equal(t, 0, count)
			})
		})
	})

	t.Run("a dirty memtable is flushed once dirty period has passed with single write", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			bucket := newTestBucketWithFlushCycle(t,
				WithStrategy(StrategyReplace),
				WithMemtableThreshold(1e12), // large enough to not affect this test
				WithWalThreshold(1e12),      // large enough to not affect this test
				WithMinWalThreshold(0),      // small enough to not affect this test
				WithDirtyThreshold(50*time.Millisecond),
			)

			require.Nil(t, bucket.Put([]byte("some-key"), []byte("some-value")))

			assertSegmentCount(t, bucket, func(t *testing.T, count int) {
				assert.Equal(t, 0, count)
			})

			// wait (virtual) until past dirty threshold and let flush cycle fire
			time.Sleep(200 * time.Millisecond)
			synctest.Wait()

			assertSegmentCount(t, bucket, func(t *testing.T, count int) {
				assert.Equal(t, 1, count)
			})
		})
	})

	t.Run("a dirty memtable is flushed once dirty period has passed with ongoing writes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			bucket := newTestBucketWithFlushCycle(t,
				WithStrategy(StrategyReplace),
				WithMemtableThreshold(1e12), // large enough to not affect this test
				WithWalThreshold(1e12),      // large enough to not affect this test
				WithMinWalThreshold(0),      // small enough to not affect this test
				WithDirtyThreshold(50*time.Millisecond),
			)

			require.Nil(t, bucket.Put([]byte("some-key"), []byte("some-value")))

			assertSegmentCount(t, bucket, func(t *testing.T, count int) {
				assert.Equal(t, 0, count)
			})

			// keep importing crossing the dirty threshold (300ms of virtual time)
			rounds := 12
			data := make([]byte, rounds*4)
			_, err := rand.Read(data)
			require.Nil(t, err)

			for i := 0; i < rounds; i++ {
				key := data[(i * 4) : (i+1)*4]
				require.Nil(t, bucket.Put(key, []byte("value")))
				time.Sleep(25 * time.Millisecond)
			}
			synctest.Wait()

			assertSegmentCount(t, bucket, func(t *testing.T, count int) {
				assert.GreaterOrEqual(t, count, 2)
			})
		})
	})
}
