//go:build integrationTest
// +build integrationTest

package lsmkv

import (
	"context"
	"encoding/json"
	"fmt"
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
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	amount := 100
	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	walThreshold := uint64(4096)
	tolerance := 0.5

	bucket, err := NewBucket(testCtx(), dirName, nullLogger(),
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
				if filepath.Ext(entry.Name()) == ".wal" {
					info, err := entry.Info()
					require.Nil(t, err)

					if !isSizeWithinTolerance(t, uint64(info.Size()), walThreshold, tolerance) {
						t.Fatalf("WAL size (%d) was allowed to increase beyond threshold (%d) with tolerance of (%f)%%",
							info.Size(), walThreshold, tolerance*100)
					}

					// Set the name of the first WAL file created
					if origWalFile == "" {
						origWalFile = entry.Name()
					} else if entry.Name() != origWalFile {
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
		err := os.RemoveAll(dirName)
		fmt.Println(err)
	}()

	amount := 10000
	sizePerValue := 8

	keys := make([][]byte, amount)
	values := make([][]byte, amount)

	memtableThreshold := uint64(4096)
	tolerance := float64(0.5)

	bucket, err := NewBucket(testCtx(), dirName, nullLogger(),
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
