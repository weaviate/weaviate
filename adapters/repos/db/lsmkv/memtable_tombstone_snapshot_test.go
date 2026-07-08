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
}
