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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// quietLoaderLogger keeps debug/info chatter out of memory and throughput
// measurements (the loader logs per WAL file at debug level).
func quietLoaderLogger() logrus.FieldLogger {
	l := logrus.New()
	l.SetLevel(logrus.ErrorLevel)
	return l
}

// Reproduction for issue #264: Loader.Load peak heap must not scale with the
// trailing-WAL count once a snapshot is loaded. The load applies WALs on top of
// the snapshot's already-allocated, slot-indexed Graph.Nodes; growing that
// slice during replay reallocates the whole O(maxNodeID) backing array, so a
// single WAL node past the snapshot's max can transiently double peak heap.

// writeSparseSnapshotForLoad writes a snapshot file (default block size so the
// Loader's default reader can read it) with liveNodes nodes spread across
// [0, idSpace), so the on-disk node count (and the reader's Graph.Nodes slice)
// is ~idSpace.
func writeSparseSnapshotForLoad(tb testing.TB, dir string, ts int64, liveNodes int, idSpace uint64) {
	tb.Helper()
	f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%d.snapshot", ts)))
	require.NoError(tb, err)
	defer f.Close()

	sw := NewSnapshotWriter(f) // default 4MB blocks, matches the loader's reader
	sw.SetEntrypoint(0, 0)
	stride := idSpace / uint64(liveNodes)
	if stride == 0 {
		stride = 1
	}
	for i := 0; i < liveNodes; i++ {
		id := uint64(i) * stride
		sw.AddNode(id, 0, [][]uint64{{(id + 1) % idSpace, (id + 2) % idSpace}}, false)
	}
	require.NoError(tb, sw.Flush())
}

// writeTrailingWALsForLoad writes count raw WAL files (timestamps after the
// snapshot) that mimic a real post-snapshot tail: a few new nodes appended just
// past the snapshot's max ID, plus link updates to existing nodes.
func writeTrailingWALsForLoad(tb testing.TB, dir string, snapshotTS int64, count, newPerWAL int, snapshotMaxID uint64) {
	tb.Helper()
	nextID := snapshotMaxID + 1
	for w := 0; w < count; w++ {
		ts := snapshotTS + int64(w) + 1
		f, err := os.Create(filepath.Join(dir, fmt.Sprintf("%d", ts)))
		require.NoError(tb, err)
		ww := NewWALWriter(f)
		for n := 0; n < newPerWAL; n++ {
			require.NoError(tb, ww.WriteAddNode(nextID, 0))
			require.NoError(tb, ww.WriteAddLinkAtLevel(nextID, 0, nextID-1))
			// touch an existing node too (in-place update, no growth)
			require.NoError(tb, ww.WriteAddLinkAtLevel(uint64(n)%snapshotMaxID, 0, nextID))
			nextID++
		}
		require.NoError(tb, f.Close())
	}
}

func loadPeak(tb testing.TB, dir string) uint64 {
	tb.Helper()
	return measurePeakHeapDelta(func() {
		loader := NewLoader(LoaderConfig{Dir: dir, Logger: quietLoaderLogger()})
		res, err := loader.Load()
		require.NoError(tb, err)
		require.NotNil(tb, res)
	})
}

// TestLoaderMemory_PeakIndependentOfWALCount is AC2: loading a snapshot plus 50
// trailing WALs must not peak materially higher than the snapshot alone. Fails
// on the pre-fix Loader (each grow reallocates the whole slice ~+1x); passes on
// the fix (slice pre-sized once, replay in place).
func TestLoaderMemory_PeakIndependentOfWALCount(t *testing.T) {
	if testing.Short() {
		t.Skip("memory invariant test; skipped in -short")
	}
	const (
		liveNodes = 20_000
		idSpace   = uint64(10_000_000) // 1e7
		newPerWAL = 10
	)
	snapshotMaxID := uint64(liveNodes-1) * (idSpace / uint64(liveNodes))

	dir0 := t.TempDir()
	writeSparseSnapshotForLoad(t, dir0, 1000, liveNodes, idSpace)
	peak0 := loadPeak(t, dir0)

	dir50 := t.TempDir()
	writeSparseSnapshotForLoad(t, dir50, 1000, liveNodes, idSpace)
	writeTrailingWALsForLoad(t, dir50, 1000, 50, newPerWAL, snapshotMaxID)
	peak50 := loadPeak(t, dir50)

	t.Logf("AC2 load: peak(snap+0)=%.1f MiB, peak(snap+50)=%.1f MiB, delta=%.1f MiB",
		float64(peak0)/mib, float64(peak50)/mib, float64(peak50-peak0)/mib)

	// Replaying 50 WALs must not allocate a second full graph. Flat (in-place)
	// gives ~1x; the pre-fix per-grow realloc gives ~1.8x at this scale.
	if float64(peak50) > 1.5*float64(peak0) {
		t.Fatalf("AC2 violated: WAL replay peak scales with WAL count: peak(snap+50)=%.1f MiB > 1.5 x peak(snap+0)=%.1f MiB",
			float64(peak50)/mib, float64(peak0)/mib)
	}
}

// TestLoaderMemory_PeakIndependentOfMaxNodeID is AC3: with the live set fixed,
// the snapshot+WALs delta over snapshot-only must not scale with the ID space.
// Pre-fix the replay realloc is ~one full O(maxNodeID) slice (+~800 MiB at 1e8);
// the fix keeps the delta to the new nodes.
func TestLoaderMemory_PeakIndependentOfMaxNodeID(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy 1e8 memory test; skipped in -short")
	}
	const (
		liveNodes = 100_000
		idSpace   = uint64(100_000_000) // 1e8
		newPerWAL = 10
		ceiling   = 64 * mib
	)
	snapshotMaxID := uint64(liveNodes-1) * (idSpace / uint64(liveNodes))

	dir0 := t.TempDir()
	writeSparseSnapshotForLoad(t, dir0, 1000, liveNodes, idSpace)
	peak0 := loadPeak(t, dir0)

	dir50 := t.TempDir()
	writeSparseSnapshotForLoad(t, dir50, 1000, liveNodes, idSpace)
	writeTrailingWALsForLoad(t, dir50, 1000, 50, newPerWAL, snapshotMaxID)
	peak50 := loadPeak(t, dir50)

	delta := float64(peak50) - float64(peak0)
	t.Logf("AC3 load(1e8): peak(snap+0)=%.0f MiB, peak(snap+50)=%.0f MiB, delta=%.1f MiB",
		float64(peak0)/mib, float64(peak50)/mib, delta/mib)

	if delta > float64(ceiling) {
		t.Fatalf("AC3 violated: snap+50 over snap+0 delta %.1f MiB exceeds %d MiB at 1e8 ID space",
			delta/mib, ceiling/mib)
	}
}

// BenchmarkLoaderLoadSnapPlusWALs measures Loader.Load wall-time for a snapshot
// plus 50 trailing WALs (AC9). benchstat A/B vs the pre-fix Loader must show no
// material regression from the WAL pre-scan.
func BenchmarkLoaderLoadSnapPlusWALs(b *testing.B) {
	const (
		liveNodes = 20_000
		idSpace   = uint64(10_000_000)
		newPerWAL = 10
	)
	snapshotMaxID := uint64(liveNodes-1) * (idSpace / uint64(liveNodes))

	dir := b.TempDir()
	writeSparseSnapshotForLoad(b, dir, 1000, liveNodes, idSpace)
	writeTrailingWALsForLoad(b, dir, 1000, 50, newPerWAL, snapshotMaxID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loader := NewLoader(LoaderConfig{Dir: dir, Logger: quietLoaderLogger()})
		res, err := loader.Load()
		if err != nil || res == nil {
			b.Fatalf("load: %v", err)
		}
	}
}
