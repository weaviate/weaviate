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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/diskio"
)

// TestBucketShutdownStoresMemtableNetCount pins that the sidecars a bucket
// leaves on shutdown add up to its object count, whether the shutdown flushes
// the memtable into a segment or keeps it as a WAL, and with a memtable left
// by a failed flush. An unloaded shard is counted from those sidecars alone.
func TestBucketShutdownStoresMemtableNetCount(t *testing.T) {
	tests := []struct {
		name          string
		reuseWAL      bool
		writeMetadata bool
		failedFlush   bool
	}{
		{name: "flushed segment", reuseWAL: false},
		{name: "flushed segment next to metadata files", reuseWAL: false, writeMetadata: true},
		{name: "reused WAL", reuseWAL: true},
		{name: "reused WAL next to metadata files", reuseWAL: true, writeMetadata: true},
		{name: "flushed segment after a failed flush", reuseWAL: false, failedFlush: true},
		{name: "reused WAL after a failed flush", reuseWAL: true, failedFlush: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			open := func() *Bucket {
				return openCountingBucket(t, dir, tt.reuseWAL, WithWriteMetadata(tt.writeMetadata))
			}

			b := open()
			for i := range 10 {
				require.NoError(t, b.Put(countTestKey(i), []byte("v1")))
			}
			require.NoError(t, b.FlushAndSwitch())

			want := 12
			if tt.failedFlush {
				// 2 updates, 1 delete of a key on disk and 2 new keys, left
				// flushing by a flush that fails
				for _, i := range []int{3, 4, 30, 31} {
					require.NoError(t, b.Put(countTestKey(i), []byte("v0")))
				}
				require.NoError(t, b.Delete(countTestKey(9)))
				active := b.active.(*Memtable)
				active.commitlog = failingCloseCommitLog{active.commitlog, true}
				require.Error(t, b.FlushAndSwitch())
				require.NotNil(t, b.flushing)
				// key 30 is new in the flushing memtable, so it is no new key in
				// the active one
				require.NoError(t, b.Put(countTestKey(30), []byte("v2")))
				want = 12 + 1 + 1 - 1 // flushing: 30, 31, -9; active: 30 again
			}

			// 5 updates and 5 new keys, 3 deletes of keys on disk, 1 of a key
			// that never existed
			for i := 5; i < 15; i++ {
				require.NoError(t, b.Put(countTestKey(i), []byte("v2")))
			}
			for i := range 3 {
				require.NoError(t, b.Delete(countTestKey(i)))
			}
			require.NoError(t, b.Delete(countTestKey(100)))
			if tt.failedFlush {
				// key 9 was deleted in the flushing memtable, so it is new again
				want = 12 + 1 + 1 - 1 + 1
			}

			count, err := b.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, want, count)
			require.NoError(t, b.Shutdown(ctx))
			require.Equal(t, want, sidecarCount(t, dir))

			// the load replays a reused WAL into a memtable that takes more
			// writes, so the WAL's sidecar has to go
			b = open()
			count, err = b.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, want, count)
			if !tt.reuseWAL {
				require.Equal(t, want, sidecarCount(t, dir))
			}
			require.Empty(t, globSidecars(t, dir, WALCountNetAdditionsFileSuffix))

			require.NoError(t, b.Put(countTestKey(20), []byte("v1")))
			require.NoError(t, b.Shutdown(ctx))
			require.Equal(t, want+1, sidecarCount(t, dir))
		})
	}
}

// TestBucketShutdownWALCountOutlivesItsWAL pins that the count a shutdown
// keeps beside a WAL counts only while that WAL is unchanged. A version
// without this count file can flush the WAL or write to it, and leaves the
// file behind.
func TestBucketShutdownWALCountOutlivesItsWAL(t *testing.T) {
	tests := []struct {
		name string
		// what a version unaware of the WAL's count file does to the bucket
		olderVersion func(t *testing.T, b *Bucket)
		// whether the left count file still describes the WAL
		wantCounted bool
	}{
		{
			name:         "flushes the WAL into a segment",
			olderVersion: func(t *testing.T, b *Bucket) { require.NoError(t, b.FlushAndSwitch()) },
		},
		{
			name: "writes more and keeps the WAL",
			olderVersion: func(t *testing.T, b *Bucket) {
				require.NoError(t, b.Put(countTestKey(50), []byte("v1")))
				require.NoError(t, b.Delete(countTestKey(10)))
			},
		},
		{
			name:         "opens the bucket without writes",
			olderVersion: func(t *testing.T, b *Bucket) {},
			wantCounted:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()

			b := openCountingBucket(t, dir, true)
			for i := range 10 {
				require.NoError(t, b.Put(countTestKey(i), []byte("v1")))
			}
			require.NoError(t, b.FlushAndSwitch())
			for i := 10; i < 13; i++ {
				require.NoError(t, b.Put(countTestKey(i), []byte("v1")))
			}
			require.NoError(t, b.Shutdown(ctx))
			walCounts := globSidecars(t, dir, WALCountNetAdditionsFileSuffix)
			require.Len(t, walCounts, 1)

			// the older version neither reads, writes nor removes the file
			hidden := filepath.Join(t.TempDir(), "hidden")
			require.NoError(t, os.Rename(walCounts[0], hidden))
			b = openCountingBucket(t, dir, true)
			tt.olderVersion(t, b)
			require.NoError(t, b.Shutdown(ctx))
			for _, p := range globSidecars(t, dir, WALCountNetAdditionsFileSuffix) {
				require.NoError(t, os.Remove(p))
			}
			withoutLeftover := sidecarCount(t, dir)
			require.NoError(t, os.Rename(hidden, walCounts[0]))
			got := sidecarCount(t, dir)

			b = openCountingBucket(t, dir, true)
			want, err := b.Count(ctx)
			require.NoError(t, err)
			require.NoError(t, b.Shutdown(ctx))

			if tt.wantCounted {
				require.Equal(t, want, got)
			} else {
				require.Equal(t, withoutLeftover, got)
				require.LessOrEqual(t, got, want)
			}
		})
	}
}

// TestBucketShutdownSkipsUnreliableNetCount pins that a count the shutdown
// cannot vouch for is not stored, and that the shutdown and a later load go
// through: the load computes the segment's count itself.
func TestBucketShutdownSkipsUnreliableNetCount(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, hook := test.NewNullLogger()
	noopCB := cyclemanager.NewCallbackGroupNoop()
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil, noopCB, noopCB,
		WithStrategy(StrategyReplace),
		WithCalcCountNetAdditions(true),
		WithMinWalThreshold(0),
	)
	require.NoError(t, err)

	for i := range 10 {
		require.NoError(t, b.Put(countTestKey(i), []byte("v1")))
	}
	require.NoError(t, b.FlushAndSwitch())
	for i := 5; i < 15; i++ {
		require.NoError(t, b.Put(countTestKey(i), []byte("v2")))
	}

	b.disk.maintenanceLock.Lock()
	for i, s := range b.disk.segments {
		b.disk.segments[i] = unreadableSegment{s}
	}
	b.disk.maintenanceLock.Unlock()

	require.NoError(t, b.Shutdown(ctx))
	require.Equal(t, 10, sidecarCount(t, dir), "no count for the segment the shutdown flushed")
	requireWarning(t, hook, "count memtable net additions for shutdown")

	b = openCountingBucket(t, dir, false)
	defer b.Shutdown(ctx)
	require.Equal(t, 15, sidecarCount(t, dir))
	count, err := b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 15, count)
}

// unreadableSegment fails every existence check, the way a segment that
// cannot be read does.
type unreadableSegment struct{ Segment }

func (s unreadableSegment) exists(key []byte) error {
	return errors.New("input/output error")
}

func TestShutdownNetCountDescribes(t *testing.T) {
	tests := []struct {
		name  string
		after func(t *testing.T, b *Bucket)
		want  bool
	}{
		{name: "no write after the count", after: func(t *testing.T, b *Bucket) {}, want: true},
		{name: "a write after the count", after: func(t *testing.T, b *Bucket) {
			require.NoError(t, b.Put(countTestKey(99), []byte("v1")))
		}, want: false},
		{name: "a flush after the count", after: func(t *testing.T, b *Bucket) {
			require.NoError(t, b.FlushAndSwitch())
		}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			b := openCountingBucket(t, t.TempDir(), true)
			defer b.Shutdown(ctx)
			require.NoError(t, b.Put(countTestKey(1), []byte("v1")))

			c, err := b.takeShutdownNetCount(ctx)
			require.NoError(t, err)
			tt.after(t, b)

			b.flushLock.RLock()
			defer b.flushLock.RUnlock()
			require.Equal(t, tt.want, c.describes(b.active, b.flushing))
		})
	}
}

// TestBucketShutdownStoreNetCountFails pins that a count file that cannot be
// written costs only the count: the shutdown and the next load go through,
// and the bucket keeps its data.
func TestBucketShutdownStoreNetCountFails(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, hook := test.NewNullLogger()
	noopCB := cyclemanager.NewCallbackGroupNoop()
	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil, noopCB, noopCB,
		WithStrategy(StrategyReplace),
		WithCalcCountNetAdditions(true),
		WithMinWalThreshold(1<<30),
	)
	require.NoError(t, err)

	for i := range 5 {
		require.NoError(t, b.Put(countTestKey(i), []byte("v1")))
	}
	// a directory with an entry cannot be renamed over
	walCount := walCountNetPathFor(b.active.commitlogWalPath())
	require.NoError(t, os.MkdirAll(filepath.Join(walCount, "entry"), 0o700))
	require.NoError(t, b.Shutdown(ctx))
	requireWarning(t, hook, "store memtable net additions at shutdown")

	b = openCountingBucket(t, dir, true)
	defer b.Shutdown(ctx)
	count, err := b.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 5, count)
}

func openCountingBucket(t *testing.T, dir string, reuseWAL bool, opts ...BucketOption) *Bucket {
	t.Helper()
	walThreshold := int64(0)
	if reuseWAL {
		walThreshold = 1 << 30
	}
	noopCB := cyclemanager.NewCallbackGroupNoop()
	b, err := NewBucketCreator().NewBucket(context.Background(), dir, "", testLogger(), nil, noopCB, noopCB,
		append([]BucketOption{
			WithStrategy(StrategyReplace),
			WithCalcCountNetAdditions(true),
			WithMinWalThreshold(walThreshold),
		}, opts...)...,
	)
	require.NoError(t, err)
	return b
}

func requireWarning(t *testing.T, hook *test.Hook, msg string) {
	t.Helper()
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel && strings.Contains(e.Message, msg) {
			return
		}
	}
	require.Failf(t, "warning not logged", "no warning containing %q", msg)
}

func TestBucketShutdownStoresNoNetCount(t *testing.T) {
	tests := []struct {
		name      string
		calcCount bool
		writes    int
	}{
		{name: "bucket that does not count", calcCount: false, writes: 5},
		{name: "empty memtable", calcCount: true, writes: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			noopCB := cyclemanager.NewCallbackGroupNoop()
			b, err := NewBucketCreator().NewBucket(ctx, dir, "", testLogger(), nil, noopCB, noopCB,
				WithStrategy(StrategyReplace),
				WithCalcCountNetAdditions(tt.calcCount),
				WithMinWalThreshold(0),
			)
			require.NoError(t, err)

			for i := range tt.writes {
				require.NoError(t, b.Put(countTestKey(i), []byte("v1")))
			}
			require.NoError(t, b.Shutdown(ctx))

			require.Empty(t, globSidecars(t, dir, CountNetAdditionsFileSuffix))
		})
	}
}

func countTestKey(i int) []byte {
	return fmt.Appendf(nil, "key-%03d", i)
}

func globSidecars(t *testing.T, dir, suffix string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*"+suffix))
	require.NoError(t, err)
	return paths
}

// sidecarCount counts a bucket directory the way an unloaded shard is counted.
func sidecarCount(t *testing.T, dir string) int {
	t.Helper()
	files, _, err := diskio.GetFileWithSizes(dir)
	require.NoError(t, err)
	total, err := CountFromSidecars(dir, files)
	require.NoError(t, err)
	return int(total)
}
