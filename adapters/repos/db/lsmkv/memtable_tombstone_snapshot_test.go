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
	"path"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func buildInvertedMemtableWithTombstones(tb testing.TB, n int) *Memtable {
	tb.Helper()
	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()
	cl, err := newCommitLogger(dir, StrategyInverted, 0)
	require.NoError(tb, err)
	m, err := newMemtable(cl, nil, logger, nil, memtableConfig{
		path:     path.Join(dir, "will-never-flush"),
		strategy: StrategyInverted,
	})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = m.commitlog.close() })
	for i := 0; i < n; i++ {
		require.NoError(tb, m.SetTombstone(uint64(i)))
	}
	return m
}

// The snapshot must reflect every tombstone visible to the reader (no stale reads),
// and successive reads with no intervening write must share one immutable bitmap.
func TestReadOnlyTombstonesSnapshotFreshnessAndSharing(t *testing.T) {
	m := buildInvertedMemtableWithTombstones(t, 1000)

	a, err := m.ReadOnlyTombstones()
	require.NoError(t, err)
	require.True(t, a.Contains(500))
	require.False(t, a.Contains(5000))

	// no write between calls -> same shared pointer, no clone
	b, err := m.ReadOnlyTombstones()
	require.NoError(t, err)
	require.Same(t, a, b)

	// a new tombstone must be visible on the next read (freshness preserved)
	require.NoError(t, m.SetTombstone(5000))
	c, err := m.ReadOnlyTombstones()
	require.NoError(t, err)
	require.True(t, c.Contains(5000))
	require.NotSame(t, a, c)

	// the previously returned snapshot must be untouched (immutable share)
	require.False(t, a.Contains(5000))
}

// Because ReadOnlyTombstones now hands every reader the same shared bitmap, the
// invariant guarding correctness is that no one mutates it in place. Run under
// -race: concurrent SetTombstone writers clear the snapshot (driving readers
// through the exclusive-lock rebuild) while readers assert the snapshot they
// hold never changes underneath them.
func TestReadOnlyTombstonesConcurrentReadWrite(t *testing.T) {
	const (
		base           = 2000
		writes         = 8000
		readers        = 4
		readsPerReader = 2000
		firstWritten   = uint64(1_000_000)
	)
	m := buildInvertedMemtableWithTombstones(t, base)

	var wg sync.WaitGroup

	// require.* calls FailNow, which is illegal off the test goroutine, so the
	// workers report via t.Errorf (goroutine-safe) and return.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < writes; i++ {
			if err := m.SetTombstone(firstWritten + uint64(i)); err != nil {
				t.Errorf("SetTombstone: %v", err)
				return
			}
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < readsPerReader; i++ {
				snap, err := m.ReadOnlyTombstones()
				if err != nil {
					t.Errorf("ReadOnlyTombstones: %v", err)
					return
				}
				// base tombstones predate every writer, so they are always present
				if !snap.Contains(base / 2) {
					t.Errorf("snapshot missing base tombstone %d", base/2)
					return
				}
				// a concurrent SetTombstone must not grow the snapshot this reader
				// already holds; it rebuilds a fresh clone instead
				card := snap.GetCardinality()
				for _, id := range []uint64{0, base / 2, base - 1, firstWritten} {
					_ = snap.Contains(id)
				}
				if got := snap.GetCardinality(); got != card {
					t.Errorf("shared snapshot mutated under reader: %d -> %d", card, got)
					return
				}
			}
		}()
	}

	wg.Wait()

	// Freshness end-to-end: once the writer is joined, the next read reflects
	// every tombstone it wrote.
	final, err := m.ReadOnlyTombstones()
	require.NoError(t, err)
	for i := 0; i < writes; i++ {
		require.True(t, final.Contains(firstWritten+uint64(i)))
	}
}

func BenchmarkReadOnlyTombstonesSnapshot(b *testing.B) {
	m := buildInvertedMemtableWithTombstones(b, 500_000)

	// Read-heavy: many queries between writes -> cached snapshot, ~0 alloc/op.
	b.Run("cached_reads", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := m.ReadOnlyTombstones(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// What the old ReadOnlyTombstones paid on EVERY call.
	b.Run("raw_clone_old", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = m.tombstones.Clone()
		}
	})

	// Pathological: a delete before every read -> snapshot rebuilds each call
	// (this is the continuous-write case where caching cannot help).
	b.Run("read_after_delete", func(b *testing.B) {
		b.ReportAllocs()
		id := uint64(1_000_000)
		for i := 0; i < b.N; i++ {
			if err := m.SetTombstone(id); err != nil {
				b.Fatal(err)
			}
			id++
			if _, err := m.ReadOnlyTombstones(); err != nil {
				b.Fatal(err)
			}
		}
	})

	// b.Error (not b.Fatal) inside RunParallel: FailNow is illegal off the
	// benchmark goroutine.

	// Concurrent read-only path: every reader hits the RLock snapshot fast path,
	// so throughput should scale with -cpu at ~0 alloc/op.
	b.Run("cached_reads_parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := m.ReadOnlyTombstones(); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})

	// The contended write-heavy case the single-threaded read_after_delete cannot
	// show: a delete before every read clears the snapshot, so concurrent
	// readers serialize on the exclusive Lock to rebuild the snapshot.
	b.Run("read_after_delete_parallel", func(b *testing.B) {
		b.ReportAllocs()
		id := uint64(2_000_000)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := m.SetTombstone(atomic.AddUint64(&id, 1)); err != nil {
					b.Error(err)
					return
				}
				if _, err := m.ReadOnlyTombstones(); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
