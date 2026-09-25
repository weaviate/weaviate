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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// walReplayBenchMemoryLimit sits below the live heap an unchunked replay reaches
// and above what a chunked one holds. A live heap over the limit makes the
// collector run back to back, which is the regime the chunking is for.
const (
	walReplayBenchEntries     = 600_000
	walReplayBenchPayload     = 400
	walReplayBenchChunkSize   = 8 * 1024 * 1024
	walReplayBenchMemoryLimit = 192 * 1024 * 1024
)

// BenchmarkWALReplay reports the peak live heap next to the wall time. Run it with
// -benchtime=1x, since each iteration copies the whole WAL back.
func BenchmarkWALReplay(b *testing.B) {
	// a map bucket is where a replay is pointer-dense, every entry becoming a
	// retained MapPair on a tree node
	tc := chunkedWALCases()[2]
	require.Equal(b, StrategyMapCollection, tc.strategy)

	source := b.TempDir()
	buildChunkTestWAL(b, source, tc, walReplayBenchEntries, walReplayBenchPayload)

	for _, limit := range []int64{math.MaxInt64, walReplayBenchMemoryLimit} {
		for _, threshold := range []int{0, walReplayBenchChunkSize} {
			b.Run(walReplayBenchName(limit, threshold), func(b *testing.B) {
				previousLimit := debug.SetMemoryLimit(limit)
				defer debug.SetMemoryLimit(previousLimit)

				for range b.N {
					b.StopTimer()
					dir := b.TempDir()
					copyWAL(b, source, dir)
					runtime.GC()
					b.StartTimer()

					var bucket *Bucket
					peak := peakHeapDuring(func() {
						bucket = openChunkTestBucket(b, dir, tc.strategy, threshold)
					})

					b.StopTimer()
					b.ReportMetric(float64(peak)/(1024*1024), "MB-peak-heap")
					require.NoError(b, bucket.Shutdown(context.Background()))
					b.StartTimer()
				}
			})
		}
	}
}

func walReplayBenchName(limit int64, threshold int) string {
	chunking := "unchunked"
	if threshold > 0 {
		chunking = "chunked"
	}
	if limit == math.MaxInt64 {
		return fmt.Sprintf("%s/nomemlimit", chunking)
	}

	return fmt.Sprintf("%s/memlimit=%dMB", chunking, limit/(1024*1024))
}

func copyWAL(b testing.TB, from, to string) {
	b.Helper()

	name := filesWithExt(b, from, ".wal")[0]
	contents, err := os.ReadFile(filepath.Join(from, name))
	require.NoError(b, err)
	require.NoError(b, os.WriteFile(filepath.Join(to, name), contents, 0o666))
}

// peakHeapDuring samples the live heap while fn runs. Each sample stops the world,
// so the interval stays far above one sample's cost.
func peakHeapDuring(fn func()) uint64 {
	done := make(chan struct{})
	peak := make(chan uint64, 1)

	enterrors.GoWrapper(func() {
		var highest uint64
		var stats runtime.MemStats

		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				peak <- highest
				return
			case <-ticker.C:
				runtime.ReadMemStats(&stats)
				if stats.HeapAlloc > highest {
					highest = stats.HeapAlloc
				}
			}
		}
	}, nullLogger())

	fn()
	close(done)

	return <-peak
}
