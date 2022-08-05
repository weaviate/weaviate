//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test ensures that the WAL threshold is being adhered to, and that a
// flush to segment followed by a switch to a new WAL is being performed
// once the threshold is reached
func TestWriteAheadLogThreshold_Replace(t *testing.T) {
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		if err := os.RemoveAll(dirName); err != nil {
			fmt.Println(err)
		}
	}()

	amount := 100
	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	walThreshold := uint64(4096)
	tolerance := 0.5

	bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
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
		var (
			done        = make(chan bool)
			origWalFile string
		)
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					for i := range keys {
						err := bucket.Put(keys[i], values[i])
						assert.Nil(t, err)
						time.Sleep(time.Millisecond)
					}
				}
			}
		}()

	out:
		for {
			entries, err := os.ReadDir(dirName)
			require.Nil(t, err)

			for _, entry := range entries {
				info, err := entry.Info()
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				require.Nil(t, err)

				entrySize := info.Size()
				entryName := info.Name()

				if filepath.Ext(entryName) == ".wal" {
					if !isSizeWithinTolerance(t, uint64(entrySize), walThreshold, tolerance) {
						t.Fatalf("WAL size (%d) was allowed to increase beyond threshold (%d) with tolerance of (%f)%%",
							entrySize, walThreshold, tolerance*100)
					}

					// Set the name of the first WAL file created
					if origWalFile == "" {
						origWalFile = entryName
					} else if entryName != origWalFile {
						// If a new WAL is detected, the switch over was successful
						done <- true
						break out
					}
				}
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}

// This test ensures that the Memtable threshold is being adhered to, and
// that a flush to segment followed by a switch to a new WAL is being
// performed once the threshold is reached
func TestMemtableThreshold_Replace(t *testing.T) {
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(dirName, 0o777)
	defer func() {
		if err := os.RemoveAll(dirName); err != nil {
			fmt.Println(err)
		}
	}()

	amount := 10000
	sizePerValue := 8

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	memtableThreshold := uint64(4096)
	tolerance := float64(0.5)

	bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
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
		done := make(chan bool)

		go func() {
			for {
				select {
				case <-done:
					return
				default:
					for i := range keys {
						err := bucket.Put(keys[i], values[i])
						assert.Nil(t, err)
						time.Sleep(time.Microsecond)
					}
				}
			}
		}()

		// give the bucket time to fill up beyond threshold
		time.Sleep(time.Millisecond)

		if !isSizeWithinTolerance(t, bucket.active.Size(), memtableThreshold, tolerance) {
			t.Fatalf("memtable size (%d) was allowed to increase beyond threshold (%d) with tolerance of (%f)%%",
				bucket.active.Size(), memtableThreshold, tolerance)
		} else {
			done <- true
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	require.Nil(t, bucket.Shutdown(ctx))
}

func isSizeWithinTolerance(t *testing.T, detectedSize uint64, threshold uint64, tolerance float64) bool {
	return float64(detectedSize) <= float64(threshold)*(tolerance+1)
}

func TestMemtableFlushesIfIdle(t *testing.T) {
	t.Run("an empty memtable is not flushed", func(t *testing.T) {
		dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
		os.MkdirAll(dirName, 0o777)
		defer func() {
			if err := os.RemoveAll(dirName); err != nil {
				fmt.Println(err)
			}
		}()

		bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
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
			time.Sleep(50 * time.Millisecond)
		})

		t.Run("assert no segments exist even after passing the idle threshold", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 0, len(bucket.disk.segments))
		})

		t.Run("shutdown bucket", func(t *testing.T) {
			ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
			require.Nil(t, bucket.Shutdown(ctx))
		})
	})

	t.Run("a dirty memtable is flushed once the idle period is over", func(t *testing.T) {
		dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
		os.MkdirAll(dirName, 0o777)
		defer func() {
			if err := os.RemoveAll(dirName); err != nil {
				fmt.Println(err)
			}
		}()

		bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
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
			time.Sleep(500 * time.Millisecond)
		})

		t.Run("assert that a flush has occurred (and one segment exists)", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 1, len(bucket.disk.segments))
		})

		t.Run("shutdown bucket", func(t *testing.T) {
			ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
			require.Nil(t, bucket.Shutdown(ctx))
		})
	})

	t.Run("a dirty memtable is not flushed as long as the next write occurs before the idle threshold", func(t *testing.T) {
		dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
		os.MkdirAll(dirName, 0o777)
		defer func() {
			if err := os.RemoveAll(dirName); err != nil {
				fmt.Println(err)
			}
		}()

		bucket, err := NewBucket(testCtx(), dirName, "", nullLogger(), nil,
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
			time.Sleep(500 * time.Millisecond)
		})

		t.Run("assert that a flush has occurred (and one segment exists)", func(t *testing.T) {
			bucket.disk.maintenanceLock.RLock()
			defer bucket.disk.maintenanceLock.RUnlock()

			assert.Equal(t, 1, len(bucket.disk.segments))
		})

		t.Run("shutdown bucket", func(t *testing.T) {
			ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
			require.Nil(t, bucket.Shutdown(ctx))
		})
	})
}
