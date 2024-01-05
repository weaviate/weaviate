//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// This test ensures that the WAL threshold is being adhered to, and that a
// flush to segment followed by a switch to a new WAL is being performed
// once the threshold is reached
func TestWriteAheadLogThreshold_Replace(t *testing.T) {
	dirName := t.TempDir()

	amount := 100
	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	walThreshold := uint64(4096)
	tolerance := 4.

	flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	flushCycle := cyclemanager.NewManager(cyclemanager.MemtableFlushCycleTicker(), flushCallbacks.CycleCallback)
	flushCycle.Start()

	bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), flushCallbacks,
		WithStrategy(StrategyReplace),
		WithMemtableThreshold(1024*1024*1024),
		WithWalThreshold(walThreshold))
	require.Nil(t, err)

	// generate only a small amount of sequential values. this allows
	// us to keep the memtable small (the net additions will be close
	// to zero), and focus on testing the WAL threshold
	t.Run("generate sequential data", func(t *testing.T) {
		for i := range keys {
			n, err := json.Marshal(i)
			require.Nil(t, err)

			keys[i], values[i] = n, n
		}
	})

	t.Run("check switchover during insertion", func(t *testing.T) {
		// Importing data for over 10s with 1.6ms break between each object
		// should result in ~100kB of commitlog data in total.
		// With couple of flush attempts happening during this 10s period,
		// and with threshold set to 4kB, first .wal size should be much smaller than 100kb
		// when commitlog switched to new .wal file.
		ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 10*time.Second)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for {
				for i := range keys {
					if i%100 == 0 && ctxTimeout.Err() != nil {
						wg.Done()
						return
					}
					assert.Nil(t, bucket.Put(keys[i], values[i]))
					time.Sleep(1600 * time.Microsecond)
				}
			}
		}()

		var firstWalFile string
		var firstWalSize int64
	out:
		for {
			time.Sleep(time.Millisecond)
			if ctxTimeout.Err() != nil {
				t.Fatalf("Import finished without flushing in the meantime. Size of first WAL file was (%d)", firstWalSize)
			}

			bucket.flushLock.RLock()
			walFile := bucket.active.commitlog.path
			walSize := bucket.active.commitlog.Size()
			bucket.flushLock.RUnlock()

			if firstWalFile == "" {
				firstWalFile = walFile
			}

			if firstWalFile == walFile {
				firstWalSize = walSize
			} else {
				// new path found; flush must have occurred - stop import and exit loop
				cancelTimeout()
				break out
			}
		}

		wg.Wait()
		if !isSizeWithinTolerance(t, uint64(firstWalSize), walThreshold, tolerance) {
			t.Fatalf("WAL size (%d) was allowed to increase beyond threshold (%d) with tolerance of (%f)%%",
				firstWalSize, walThreshold, tolerance*100)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
	require.Nil(t, flushCycle.StopAndWait(ctx))
}

// This test ensures that the Memtable threshold is being adhered to, and
// that a flush to segment followed by a switch to a new WAL is being
// performed once the threshold is reached
func TestMemtableThreshold_Replace(t *testing.T) {
	dirName := t.TempDir()

	amount := 10000
	sizePerValue := 8

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	memtableThreshold := uint64(4096)
	tolerance := 4.

	flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
	flushCycle := cyclemanager.NewManager(cyclemanager.MemtableFlushCycleTicker(), flushCallbacks.CycleCallback)
	flushCycle.Start()

	bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), flushCallbacks,
		WithStrategy(StrategyReplace),
		WithMemtableThreshold(memtableThreshold))
	require.Nil(t, err)

	t.Run("generate random data", func(t *testing.T) {
		for i := range keys {
			n, err := json.Marshal(i)
			require.Nil(t, err)

			keys[i] = n
			values[i] = make([]byte, sizePerValue)
			rand.Read(values[i])
		}
	})

	t.Run("check switchover during insertion", func(t *testing.T) {
		// Importing data for over 10s with 0.8ms break between each object
		// should result in ~100kB of memtable data.
		// With couple of flush attempts happening during this 10s period,
		// and with threshold set to 4kB, first memtable size should be much smaller than 100kb
		// when memtable flushed and replaced with new one
		ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 10*time.Second)

		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			for {
				for i := range keys {
					if i%100 == 0 && ctxTimeout.Err() != nil {
						wg.Done()
						return
					}
					assert.Nil(t, bucket.Put(keys[i], values[i]))
					time.Sleep(800 * time.Microsecond)
				}
			}
		}()

		var firstMemtablePath string
		var firstMemtableSize uint64
	out:
		for {
			time.Sleep(time.Millisecond)
			if ctxTimeout.Err() != nil {
				t.Fatalf("Import finished without flushing in the meantime. Size of first memtable was (%d)", firstMemtableSize)
			}

			bucket.flushLock.RLock()
			activePath := bucket.active.path
			activeSize := bucket.active.Size()
			bucket.flushLock.RUnlock()

			if firstMemtablePath == "" {
				firstMemtablePath = activePath
			}

			if firstMemtablePath == activePath {
				firstMemtableSize = activeSize
			} else {
				// new path found; flush must have occurred - stop import and exit loop
				cancelTimeout()
				break out
			}
		}

		wg.Wait()
		if !isSizeWithinTolerance(t, uint64(firstMemtableSize), memtableThreshold, tolerance) {
			t.Fatalf("Memtable size (%d) was allowed to increase beyond threshold (%d) with tolerance of (%f)%%",
				firstMemtableSize, memtableThreshold, tolerance*100)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
	require.Nil(t, flushCycle.StopAndWait(ctx))
}

func isSizeWithinTolerance(t *testing.T, detectedSize uint64, threshold uint64, tolerance float64) bool {
	return detectedSize > 0 && float64(detectedSize) <= float64(threshold)*(tolerance+1)
}

func TestMemtableFlushesIfIdle(t *testing.T) {
	t.Run("an empty memtable is not flushed", func(t *testing.T) {
		dirName := t.TempDir()

		flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
		flushCycle := cyclemanager.NewManager(cyclemanager.MemtableFlushCycleTicker(), flushCallbacks.CycleCallback)
		flushCycle.Start()

		bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), flushCallbacks,
			WithStrategy(StrategyReplace),
			WithMemtableThreshold(1e12), // large enough to not affect this test
			WithWalThreshold(1e12),      // large enough to not affect this test
			WithIdleThreshold(10*time.Millisecond),
		)
		require.Nil(t, err)

		t.Run("assert no segments exist initially", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 0, len(bucket.disk.segments))
		})

		t.Run("wait until idle threshold has passed", func(t *testing.T) {
			// First flush attempt should occur after ~100ms after creating bucket.
			// Buffer of 200ms guarantees, flush will be called during sleep period.
			time.Sleep(200 * time.Millisecond)
		})

		t.Run("assert no segments exist even after passing the idle threshold", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 0, len(bucket.disk.segments))
		})

		t.Run("shutdown bucket", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			require.Nil(t, bucket.Shutdown(ctx))
			require.Nil(t, flushCycle.StopAndWait(ctx))
		})
	})

	t.Run("a dirty memtable is flushed once the idle period is over", func(t *testing.T) {
		dirName := t.TempDir()

		flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
		flushCycle := cyclemanager.NewManager(cyclemanager.MemtableFlushCycleTicker(), flushCallbacks.CycleCallback)
		flushCycle.Start()

		bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), flushCallbacks,
			WithStrategy(StrategyReplace),
			WithMemtableThreshold(1e12), // large enough to not affect this test
			WithWalThreshold(1e12),      // large enough to not affect this test
			WithIdleThreshold(50*time.Millisecond),
		)
		require.Nil(t, err)

		t.Run("import something to make it dirty", func(t *testing.T) {
			require.Nil(t, bucket.Put([]byte("some-key"), []byte("some-value")))
		})

		t.Run("assert no segments exist initially", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 0, len(bucket.disk.segments))
		})

		t.Run("wait until idle threshold has passed", func(t *testing.T) {
			// First flush attempt should occur after ~100ms after creating bucket.
			// Buffer of 200ms guarantees, flush will be called during sleep period.
			time.Sleep(200 * time.Millisecond)
		})

		t.Run("assert that a flush has occurred (and one segment exists)", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 1, len(bucket.disk.segments))
		})

		t.Run("shutdown bucket", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			require.Nil(t, bucket.Shutdown(ctx))
			require.Nil(t, flushCycle.StopAndWait(ctx))
		})
	})

	t.Run("a dirty memtable is not flushed as long as the next write occurs before the idle threshold", func(t *testing.T) {
		dirName := t.TempDir()

		flushCallbacks := cyclemanager.NewCallbackGroup("flush", nullLogger(), 1)
		flushCycle := cyclemanager.NewManager(cyclemanager.MemtableFlushCycleTicker(), flushCallbacks.CycleCallback)
		flushCycle.Start()

		bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), flushCallbacks,
			WithStrategy(StrategyReplace),
			WithMemtableThreshold(1e12), // large enough to not affect this test
			WithWalThreshold(1e12),      // large enough to not affect this test
			WithIdleThreshold(50*time.Millisecond),
		)
		require.Nil(t, err)

		t.Run("import something to make it dirty", func(t *testing.T) {
			require.Nil(t, bucket.Put([]byte("some-key"), []byte("some-value")))
		})

		t.Run("assert no segments exist initially", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 0, len(bucket.disk.segments))
		})

		t.Run("keep importing without ever crossing the idle threshold", func(t *testing.T) {
			rounds := 20
			data := make([]byte, rounds*4)
			_, err := rand.Read(data)
			require.Nil(t, err)

			for i := 0; i < rounds; i++ {
				key := data[(i * 4) : (i+1)*4]
				bucket.Put(key, []byte("value"))
				time.Sleep(25 * time.Millisecond)
			}
		})

		t.Run("assert that no flushing has occurred", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 0, len(bucket.disk.segments))
		})

		t.Run("wait until idle threshold has passed", func(t *testing.T) {
			// At that point 2 flush attempt have already occurred.
			// 3rd attempt should occur after ~930ms after creating bucket.
			// Buffer of 500ms guarantees, flush will be called during sleep period.
			time.Sleep(500 * time.Millisecond)
		})

		t.Run("assert that a flush has occurred (and one segment exists)", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 1, len(bucket.disk.segments))
		})

		t.Run("shutdown bucket", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			require.Nil(t, bucket.Shutdown(ctx))
			require.Nil(t, flushCycle.StopAndWait(ctx))
		})
	})
}
