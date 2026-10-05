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
	"io"
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

// walReplayBenchMemoryLimit is set below the peak heap an unchunked replay of this
// fixture reaches, so the collector runs back to back — the regime chunking is for.
// The nomemlimit arms run the same replays without it.
const (
	walReplayBenchEntries     = 600_000
	walReplayBenchPayload     = 400
	walReplayBenchMemoryLimit = 192 * 1024 * 1024
)

// walReplayBenchChunkSizes sweeps the chunk threshold so defaultWALReplayMaxMemtableSize
// can be re-decided rather than taken on trust: the sweep spans it from well below to
// above. 0 is the baseline arm, configured so no cut can fire.
var walReplayBenchChunkSizes = []int{
	0,
	2 * 1024 * 1024,
	8 * 1024 * 1024,
	32 * 1024 * 1024,
	128 * 1024 * 1024,
	256 * 1024 * 1024,
	512 * 1024 * 1024,
}

// BenchmarkWALReplay reports the peak live heap next to the wall time. Run it with
// -benchtime=1x, since each iteration copies the whole WAL back.
//
// The replace arm carries WithCalcCountNetAdditions, which the objects bucket sets:
// committing a run then recounts each chunk against the chunks already added, a term
// that grows with the square of the chunk count and is absent from every other arm.
func BenchmarkWALReplay(b *testing.B) {
	for _, arm := range []struct {
		strategy string
		extra    []BucketOption
	}{
		// a map bucket is where a replay is pointer-dense, every entry becoming a
		// retained MapPair on a tree node
		{strategy: StrategyMapCollection},
		{strategy: StrategyReplace, extra: []BucketOption{WithCalcCountNetAdditions(true)}},
	} {
		b.Run(arm.strategy, func(b *testing.B) {
			benchmarkWALReplayArm(b, arm.strategy, arm.extra)
		})
	}
}

func benchmarkWALReplayArm(b *testing.B, strategy string, extra []BucketOption) {
	tc := chunkedWALCaseFor(b, strategy)

	// built straight onto the disk rather than through chunkTestWAL, whose cache would
	// retain the whole fixture for the process and land it inside every heap sample
	source := b.TempDir()
	buildChunkTestWALRange(b, source, tc, 0, walReplayBenchEntries, walReplayBenchPayload)

	for _, limit := range []int64{math.MaxInt64, walReplayBenchMemoryLimit} {
		for _, threshold := range walReplayBenchChunkSizes {
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
						bucket = openChunkTestBucket(b, dir, tc.strategy, threshold,
							append(walReplayBenchBaseline(threshold), extra...)...)
					})

					b.StopTimer()
					wantMemtableBound := uint64(threshold)
					if threshold == 0 {
						wantMemtableBound = walReplayBenchUnchunkedSize
					}
					require.Equal(b, wantMemtableBound, bucket.walReplayMaxMemtableSize(),
						"the swept threshold has to be the memtable bound this arm cuts on")
					require.Equal(b, uint64(1<<40), bucket.walThreshold,
						"or the WAL trigger gets there first and the sweep moves nothing")
					b.ReportMetric(float64(peak)/(1024*1024), "MB-peak-heap")
					require.NoError(b, bucket.Shutdown(context.Background()))
					b.StartTimer()
				}
			})
		}
	}
}

// walReplayBenchBaseline puts the WAL trigger out of reach on every arm, so the
// swept threshold is the only bound that can cut. The 0 arm raises the memtable
// trigger too, or it keeps the production defaults and still chunks, leaving the
// sweep no unchunked replay to measure against.
func walReplayBenchBaseline(threshold int) []BucketOption {
	if threshold > 0 {
		return []BucketOption{WithWalThreshold(1 << 40)}
	}

	return []BucketOption{
		WithDynamicMemtableSizing(walReplayBenchUnchunkedSize, walReplayBenchUnchunkedSize, 1, 3600),
		WithWalThreshold(1 << 40),
	}
}

// walReplayBenchUnchunkedSize is the memtable bound of the 0 arm, above any fixture
// this benchmark builds.
const walReplayBenchUnchunkedSize = 2048 * 1024 * 1024

func walReplayBenchName(limit int64, threshold int) string {
	chunking := "unchunked"
	if threshold > 0 {
		chunking = fmt.Sprintf("chunk=%dMB", threshold/(1024*1024))
	}
	if limit == math.MaxInt64 {
		return fmt.Sprintf("%s/nomemlimit", chunking)
	}

	return fmt.Sprintf("%s/memlimit=%dMB", chunking, limit/(1024*1024))
}

// copyAllWALs copies every write-ahead-log in a directory, which is what a
// control arm for a multi-WAL recovery needs.
func copyAllWALs(t testing.TB, from, to string) {
	t.Helper()

	for _, name := range filesWithExt(t, from, ".wal") {
		copyNamedWAL(t, from, to, name)
	}
}

func copyWAL(b testing.TB, from, to string) {
	b.Helper()

	copyNamedWAL(b, from, to, filesWithExt(b, from, ".wal")[0])
}

// copyNamedWAL streams rather than reading the file in, so setting up an
// iteration never allocates a fixture-sized buffer.
func copyNamedWAL(b testing.TB, from, to, name string) {
	b.Helper()

	src, err := os.Open(filepath.Join(from, name))
	require.NoError(b, err)
	defer src.Close()

	dst, err := os.Create(filepath.Join(to, name))
	require.NoError(b, err)
	defer dst.Close()

	_, err = io.Copy(dst, src)
	require.NoError(b, err)
}

// peakHeapDuring returns the peak HeapAlloc while fn runs, an upper bound on
// live heap since HeapAlloc counts unswept garbage. Sampling stops the world.
func peakHeapDuring(fn func()) (peaked uint64) {
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

	// deferred so a require failure inside fn still stops the sampler, which
	// would otherwise keep stopping the world for every later benchmark arm
	defer func() {
		close(done)
		peaked = <-peak
	}()

	fn()

	return
}
