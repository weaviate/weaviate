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

package lsmkv

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// BenchmarkBatchGetBySecondary compares three secondary-lookup strategies on a
// real on-disk bucket that uses pread (no mmap) so that actual disk I/O is
// exercised.  Run with:
//
//	go test -bench=BenchmarkBatchGetBySecondary -benchmem \
//	    -benchtime=5s ./adapters/repos/db/lsmkv/
//
// The three sub-benchmarks per batch size are:
//
//	Sequential  – one GetBySecondaryWithBuffer call per ID, single goroutine
//	Parallel    – goroutine pool (current ObjectsByDocID default)
//	Batch       – single BatchGetBySecondary call (io_uring on Linux)
func BenchmarkBatchGetBySecondary(b *testing.B) {
	const objectCount = 5000
	const valueSize = 512 // bytes per object value

	dir := b.TempDir()
	defer os.RemoveAll(dir)

	// Create a Replace-strategy bucket with one secondary index.
	// WithPread(true) disables mmap so segment reads go through pread64 /
	// io_uring rather than a plain memory copy.
	bucket, err := NewBucketCreator().NewBucket(
		testCtxB(), dir, "", nullLoggerB(), nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
		WithSecondaryIndices(1),
		WithPread(true),
	)
	require.NoError(b, err)
	defer bucket.Shutdown(testCtxB())

	// Populate the bucket.
	//   Primary key:   16-byte little-endian uint64 (mimics UUID slot)
	//   Value:         fixed-size blob (mimics serialised object)
	//   Secondary key: 8-byte little-endian doc ID (matches ObjectsByDocID usage)
	value := make([]byte, valueSize)
	secKeys := make([][]byte, objectCount)

	for i := 0; i < objectCount; i++ {
		var pk [16]byte
		binary.LittleEndian.PutUint64(pk[:], uint64(i))

		var sk [8]byte
		binary.LittleEndian.PutUint64(sk[:], uint64(i))
		secKeys[i] = sk[:]

		err := bucket.Put(pk[:], value, WithSecondaryKey(0, sk[:]))
		require.NoError(b, err)
	}

	// Flush the active memtable to a segment file so reads actually hit disk.
	require.NoError(b, bucket.FlushAndSwitch())

	batchSizes := []int{10, 100, 500, 1000, 5000}

	for _, n := range batchSizes {
		keys := secKeys[:n]

		b.Run(fmt.Sprintf("Sequential/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var buf []byte
				for _, k := range keys {
					var err error
					_, buf, err = bucket.GetBySecondaryWithBuffer(testCtxB(), 0, k, buf)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})

		b.Run(fmt.Sprintf("Parallel/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := benchParallelGetBySecondary(bucket, 0, keys)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("Batch_iouring/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := bucket.BatchGetBySecondary(0, keys)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// benchParallelGetBySecondary mirrors the goroutine-chunking strategy of
// objectsByDocIDParallel so we can compare it apples-to-apples against
// BatchGetBySecondary inside the lsmkv package without pulling in the
// entities/storobj dependency.
func benchParallelGetBySecondary(b *Bucket, pos int, keys [][]byte) ([][]byte, error) {
	parallel := 2 * runtime.GOMAXPROCS(0)
	chunkSize := int(math.Ceil(float64(len(keys)) / float64(parallel)))
	if chunkSize < 1 {
		chunkSize = 1
	}

	results := make([][]byte, len(keys))
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup

	for chunk := 0; chunk < parallel; chunk++ {
		start := chunk * chunkSize
		end := start + chunkSize
		if end > len(keys) {
			end = len(keys)
		}
		if start >= len(keys) {
			break
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			var buf []byte
			for j := start; j < end; j++ {
				v, newBuf, err := b.GetBySecondaryWithBuffer(context.Background(), pos, keys[j], buf)
				buf = newBuf
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				results[j] = v
			}
		}(start, end)
	}

	wg.Wait()
	return results, firstErr
}
