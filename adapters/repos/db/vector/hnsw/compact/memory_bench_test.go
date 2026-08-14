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

package compact

import (
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// These benchmarks pin the load-bearing invariants of the streaming snapshot
// writer (issue #263): peak heap during snapshot creation must be independent
// of the live-set size and of maxNodeID, bounded instead by the block size.
//
// The assertions are scaling invariants (ratios), not machine-tuned absolute
// thresholds, so they hold in CI regardless of hardware. They use only the
// public AddNode/Flush API, which is exactly the per-node path WriteFromMerger
// drives, so they compile and run on both the pre-fix (buffering) and post-fix
// (streaming) writer — the pre/post asymmetry is the proof:
//
//   - On the pre-fix writer the body is materialized in an ID-indexed slice,
//     so peak heap scales ~linearly with N (AC2) and with maxNodeID (AC3); the
//     ratio assertions FAIL.
//   - On the streaming writer peak heap is flat (~one block); the assertions
//     PASS.

const (
	benchConnsPerNode = 32
	mib               = 1024 * 1024
)

// measurePeakHeapDelta runs fn while sampling the live Go heap and returns the
// peak HeapAlloc observed during the run minus a post-GC baseline taken just
// before. A background sampler is used because peak (not cumulative or final)
// heap is what the OOM risk depends on.
func measurePeakHeapDelta(fn func()) uint64 {
	runtime.GC()
	runtime.GC()

	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	var peak atomic.Uint64
	peak.Store(base.HeapAlloc)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			default:
			}
			runtime.ReadMemStats(&m)
			for {
				cur := peak.Load()
				if m.HeapAlloc <= cur {
					break
				}
				if peak.CompareAndSwap(cur, m.HeapAlloc) {
					break
				}
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()

	fn()

	close(stop)
	<-done

	p := peak.Load()
	if p < base.HeapAlloc {
		return 0
	}
	return p - base.HeapAlloc
}

// writeDenseSnapshot writes liveNodes contiguous nodes, each with a fresh
// connection slice, to a writer with the given block size, discarding output.
// A fresh slice per node mirrors a real graph: the pre-fix writer retains every
// one of them, the streaming writer encodes and releases each immediately.
func writeDenseSnapshot(tb testing.TB, liveNodes int, blockSize int64) {
	tb.Helper()
	sw := NewSnapshotWriterWithBlockSize(io.Discard, blockSize)
	sw.SetEntrypoint(0, 0)
	for i := 0; i < liveNodes; i++ {
		sw.AddNode(uint64(i), 0, [][]uint64{genConns(uint64(i), benchConnsPerNode)}, false)
	}
	if err := sw.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}
}

// writeSparseSnapshot writes liveNodes nodes spread evenly across [0, idSpace)
// so maxNodeID ~= idSpace. The pre-fix writer's ID-indexed slice pays for the
// whole space; the streaming writer pays only for one block.
func writeSparseSnapshot(tb testing.TB, liveNodes int, idSpace uint64, blockSize int64) {
	tb.Helper()
	stride := idSpace / uint64(liveNodes)
	if stride == 0 {
		stride = 1
	}
	sw := NewSnapshotWriterWithBlockSize(io.Discard, blockSize)
	sw.SetEntrypoint(0, 0)
	for i := 0; i < liveNodes; i++ {
		id := uint64(i) * stride
		sw.AddNode(id, 0, [][]uint64{genConns(id, benchConnsPerNode)}, false)
	}
	if err := sw.Flush(); err != nil {
		tb.Fatalf("flush: %v", err)
	}
}

func genConns(seed uint64, n int) []uint64 {
	c := make([]uint64, n)
	for j := 0; j < n; j++ {
		c[j] = seed + uint64(j) + 1
	}
	return c
}

// TestSnapshotWriterMemory_PeakIndependentOfLiveSet is AC2: an 8x larger dense
// graph must not grow peak heap by more than a block-sized constant (flat, not
// ~8x linear).
func TestSnapshotWriterMemory_PeakIndependentOfLiveSet(t *testing.T) {
	if testing.Short() {
		t.Skip("memory invariant test; skipped in -short")
	}
	const (
		n         = 50_000
		k         = 8
		blockSize = 1 * mib
	)

	peakN := measurePeakHeapDelta(func() { writeDenseSnapshot(t, n, blockSize) })
	peakKN := measurePeakHeapDelta(func() { writeDenseSnapshot(t, k*n, blockSize) })

	t.Logf("AC2 dense: peak(N=%d)=%.1f MiB, peak(%dxN=%d)=%.1f MiB",
		n, float64(peakN)/mib, k, k*n, float64(peakKN)/mib)

	// Flat-delta invariant: growing the live set 8x must not grow peak heap by
	// more than a block-sized constant. Streaming gives a few MiB; the old
	// buffering writer grew ~+200 MiB. A ratio test flakes here because the
	// absolute peaks are single-digit MiB where GC timing dominates, so assert
	// the delta instead — far below the linear growth, well above GC noise.
	const ceiling = 64 * mib
	if delta := float64(peakKN) - float64(peakN); delta > ceiling {
		t.Fatalf("AC2 violated: peak heap scales with live-set size: peak(%dxN)−peak(N)=%.1f MiB exceeds %d MiB",
			k, delta/mib, ceiling/mib)
	}
}

// TestSnapshotWriterMemory_PeakIndependentOfMaxNodeID is AC3: peak heap for
// 100k live nodes must not grow with the ID space (#262's scenario).
func TestSnapshotWriterMemory_PeakIndependentOfMaxNodeID(t *testing.T) {
	if testing.Short() || raceDetectorEnabled {
		// -race turns this 1e8-slot peak-heap test into minutes for no signal.
		t.Skip("heavy 1e8 memory test; skipped in -short and under -race")
	}
	const (
		liveNodes = 100_000
		blockSize = 1 * mib
		small     = uint64(10_000_000)  // 1e7
		large     = uint64(100_000_000) // 1e8
	)

	peakSmall := measurePeakHeapDelta(func() { writeSparseSnapshot(t, liveNodes, small, blockSize) })
	peakLarge := measurePeakHeapDelta(func() { writeSparseSnapshot(t, liveNodes, large, blockSize) })

	t.Logf("AC3 sparse: peak(maxID~1e7)=%.1f MiB, peak(maxID~1e8)=%.1f MiB",
		float64(peakSmall)/mib, float64(peakLarge)/mib)

	// A 10x larger ID space with the same live set must not grow peak heap.
	// Flat (streaming) gives ~1x; the ID-indexed slice gives ~10x.
	if float64(peakLarge) > 2.0*float64(peakSmall) {
		t.Fatalf("AC3 violated: peak heap scales with maxNodeID: peak(1e8)=%.1f MiB > 2 x peak(1e7)=%.1f MiB",
			float64(peakLarge)/mib, float64(peakSmall)/mib)
	}
}

// TestSnapshotWriterMemory_AbsoluteCeiling is AC4: snapshotting a >=1M node
// graph keeps peak additional heap below a fixed ceiling on the order of
// blockSize x small constant, independent of N. Uses the production 4MB block.
func TestSnapshotWriterMemory_AbsoluteCeiling(t *testing.T) {
	if testing.Short() || raceDetectorEnabled {
		// -race turns this 1.5M-node peak-heap test into minutes for no signal.
		t.Skip("heavy memory ceiling test; skipped in -short and under -race")
	}
	const (
		liveNodes = 1_500_000
		ceiling   = 64 * mib
	)

	peak := measurePeakHeapDelta(func() { writeDenseSnapshot(t, liveNodes, defaultBlockSize) })
	t.Logf("AC4 ceiling: peak(N=%d, 4MiB blocks)=%.1f MiB (ceiling %d MiB)",
		liveNodes, float64(peak)/mib, ceiling/mib)

	if peak > ceiling {
		t.Fatalf("AC4 violated: peak additional heap %.1f MiB exceeds %d MiB ceiling at N=%d",
			float64(peak)/mib, ceiling/mib, liveNodes)
	}
}

// BenchmarkSnapshotWritePeak reports peak heap (MiB) as a custom metric for a
// benchstat A/B between the buffering and streaming writers (AC10). Run with
// -benchtime=1x.
func BenchmarkSnapshotWritePeak(b *testing.B) {
	cases := []struct {
		name      string
		run       func(testing.TB)
		blockSize int64
	}{
		{"dense_N", func(tb testing.TB) { writeDenseSnapshot(tb, 50_000, 1*mib) }, 1 * mib},
		{"dense_8N", func(tb testing.TB) { writeDenseSnapshot(tb, 400_000, 1*mib) }, 1 * mib},
		{"sparse_1e7", func(tb testing.TB) { writeSparseSnapshot(tb, 100_000, 10_000_000, 1*mib) }, 1 * mib},
		{"sparse_1e8", func(tb testing.TB) { writeSparseSnapshot(tb, 100_000, 100_000_000, 1*mib) }, 1 * mib},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			var peak uint64
			for i := 0; i < b.N; i++ {
				peak = measurePeakHeapDelta(func() { c.run(b) })
			}
			b.ReportMetric(float64(peak)/mib, "peakMiB")
		})
	}
}

// BenchmarkSnapshotWriteThroughput times the full per-node write + flush for a
// representative dense graph (AC9). benchstat A/B vs the buffering writer must
// show no material wall-time regression.
func BenchmarkSnapshotWriteThroughput(b *testing.B) {
	const n = 200_000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeDenseSnapshot(b, n, defaultBlockSize)
	}
}
