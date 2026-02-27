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

//go:build linux

package lsmkv

// BenchmarkBatchGetBySecondaryPreadCold measures cold-read latency for the
// three secondary-lookup strategies by evicting the OS page cache between
// every benchmark iteration using posix_fadvise(POSIX_FADV_DONTNEED).
//
// IMPORTANT: the segment files must live on a real disk-backed filesystem.
// /tmp is usually tmpfs on Linux (no page cache), so this benchmark writes
// to the package source directory which is on btrfs/ext4/xfs.
// If that directory is also in-memory (e.g. tmpfs mount), the eviction will
// silently have no effect and results will match the warm-cache benchmark.
//
// Run with:
//
//	go test -bench=BenchmarkBatchGetBySecondaryPreadCold -benchmem \
//	    -benchtime=10x ./adapters/repos/db/lsmkv/
//
// Use -benchtime=Nx (iteration count) rather than a duration; each iteration
// performs a full cold read, which can take hundreds of milliseconds.

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"golang.org/x/sys/unix"
)

// evictSegmentPages advises the kernel to release all page-cache pages for
// every *.db segment file inside dir. Must be called on a non-tmpfs path.
func evictSegmentPages(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".db") {
			continue
		}
		f, err := os.Open(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		// POSIX_FADV_DONTNEED advises the kernel that the caller does not
		// expect to access the given file range in the near future, prompting
		// it to release those pages.
		_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
		f.Close()
	}
}

func BenchmarkBatchGetBySecondaryPreadCold(b *testing.B) {
	const objectCount = 2000
	const valueSize = 512

	// Write to the package source directory (btrfs/ext4/xfs) so that
	// fadvise actually evicts pages.  b.TempDir() resolves to /tmp (tmpfs)
	// on most Linux systems and is intentionally avoided here.
	dir, err := os.MkdirTemp(".", "bench_cold_pread_")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(dir) })

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

	value := make([]byte, valueSize)
	secKeys := make([][]byte, objectCount)

	for i := 0; i < objectCount; i++ {
		var pk [16]byte
		binary.LittleEndian.PutUint64(pk[:], uint64(i))
		var sk [8]byte
		binary.LittleEndian.PutUint64(sk[:], uint64(i))
		secKeys[i] = make([]byte, 8)
		copy(secKeys[i], sk[:])
		require.NoError(b, bucket.Put(pk[:], value, WithSecondaryKey(0, sk[:])))
	}

	// Flush to disk so reads go through pread / io_uring.
	require.NoError(b, bucket.FlushAndSwitch())

	batchSizes := []int{10, 100, 500, 1000, 2000}

	for _, n := range batchSizes {
		keys := secKeys[:n]

		b.Run(fmt.Sprintf("Sequential/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// Evict pages — excluded from timer.
				b.StopTimer()
				evictSegmentPages(dir)
				b.StartTimer()

				var buf []byte
				for _, k := range keys {
					_, buf, err = bucket.GetBySecondaryWithBuffer(testCtxB(), 0, k, buf)
					if err != nil {
						b.Fatal(err)
					}
				}
			}
		})

		b.Run(fmt.Sprintf("Parallel/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				evictSegmentPages(dir)
				b.StartTimer()

				_, err := benchParallelGetBySecondary(bucket, 0, keys)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("Batch_iouring/n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				evictSegmentPages(dir)
				b.StartTimer()

				_, err := bucket.BatchGetBySecondary(context.Background(), 0, keys)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
