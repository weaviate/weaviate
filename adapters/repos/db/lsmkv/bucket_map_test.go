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
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestMapWritePathRefCount(t *testing.T) {
	b := Bucket{
		strategy: StrategyMapCollection,
		disk:     &SegmentGroup{segments: []Segment{}},
		active:   newTestMemtableMap(nil),
		logger:   nullLogger(),
	}

	expectedRefs := 0
	assertWriterRefs := func() {
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountIncs)
		require.Equal(t, expectedRefs, b.active.(*testMemtable).totalWriteCountDecs)
	}
	assertWriterRefs()

	// add one
	err := b.MapSet([]byte("key1"), MapPair{Key: []byte("k1"), Value: []byte("v1")})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// add many
	err = b.MapSetMulti([]byte("key1"), []MapPair{
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	})
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// delete one
	err = b.MapDeleteKey([]byte("key1"), []byte("k2"))
	require.NoError(t, err)
	expectedRefs++
	assertWriterRefs()

	// sanity check, final state:
	v, err := b.MapList(context.Background(), []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []MapPair{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}, v)
}

// A map pair whose key and value are each within the uint16 limit, but whose
// encoded size is above it, must survive every stage of the bucket lifecycle.
func TestMapPairEncodedSizeAboveUint16_BucketJourney(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rowKey := []byte("row")

	largePair := func(id byte) MapPair {
		return MapPair{
			Key:   bytes.Repeat([]byte{id}, 40000),
			Value: bytes.Repeat([]byte{id + 1}, 40000),
		}
	}
	small := MapPair{Key: []byte("small"), Value: []byte("value")}

	newBucket := func(t *testing.T, dir string) *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyMapCollection), WithForceCompaction(true))
		require.NoError(t, err)
		return b
	}
	assertPairs := func(t *testing.T, b *Bucket, expected ...MapPair) {
		t.Helper()
		got, err := b.MapList(ctx, rowKey)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	}

	b := newBucket(t, dir)
	require.NoError(t, b.MapSet(rowKey, largePair(0x10)))
	require.NoError(t, b.MapSet(rowKey, small))
	t.Run("memtable", func(t *testing.T) {
		assertPairs(t, b, largePair(0x10), small)
	})

	require.NoError(t, b.FlushAndSwitch())
	t.Run("flushed segment", func(t *testing.T) {
		assertPairs(t, b, largePair(0x10), small)
	})

	require.NoError(t, b.MapSetMulti(rowKey, []MapPair{largePair(0x20)}))
	require.NoError(t, b.MapDeleteKey(rowKey, largePair(0x10).Key))
	t.Run("segment and memtable", func(t *testing.T) {
		assertPairs(t, b, largePair(0x20), small)
	})

	t.Run("wal replay", func(t *testing.T) {
		// a copy taken before the memtable is flushed only has the WAL for
		// the latest writes
		require.NoError(t, b.active.(*Memtable).commitlog.flushBuffers())
		recoveredDir := t.TempDir()
		require.NoError(t, os.CopyFS(recoveredDir, os.DirFS(dir)))

		recovered := newBucket(t, recoveredDir)
		assertPairs(t, recovered, largePair(0x20), small)
		require.NoError(t, recovered.Shutdown(ctx))
	})

	t.Run("compaction", func(t *testing.T) {
		require.NoError(t, b.FlushAndSwitch())
		compacted, err := b.disk.compactOnce(ctx)
		require.NoError(t, err)
		require.True(t, compacted)
		assertPairs(t, b, largePair(0x20), small)
	})

	t.Run("cursor", func(t *testing.T) {
		c, err := b.MapCursor()
		require.NoError(t, err)
		defer c.Close()
		k, pairs := c.First(ctx)
		require.Equal(t, rowKey, k)
		require.Equal(t, []MapPair{largePair(0x20), small}, pairs)
	})

	require.NoError(t, b.Shutdown(ctx))
}

// The memtable encodes every pair into a buffer it reuses for the next write.
// Each WAL record must still carry its own pair.
func TestMapSetWALReplayAfterBufferReuse(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rowKey := []byte("row")

	newBucket := func(t *testing.T, dir string) *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dir, "", nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyMapCollection))
		require.NoError(t, err)
		return b
	}
	pair := func(id byte, keyLen, valueLen int) MapPair {
		return MapPair{
			Key:   bytes.Repeat([]byte{id}, keyLen),
			Value: bytes.Repeat([]byte{id + 0x80}, valueLen),
		}
	}

	b := newBucket(t, dir)
	// sizes grow, shrink and grow again, with a tombstone in between
	require.NoError(t, b.MapSet(rowKey, pair(1, 8, 8)))
	require.NoError(t, b.MapSet(rowKey, pair(2, 300, 5000)))
	require.NoError(t, b.MapSetMulti(rowKey, []MapPair{pair(3, 1, 0), pair(4, 64, 64)}))
	require.NoError(t, b.MapDeleteKey(rowKey, pair(2, 300, 5000).Key))
	require.NoError(t, b.MapSet(rowKey, pair(5, 2, 9000)))
	require.NoError(t, b.MapSet([]byte("other row"), pair(6, 3, 3)))

	require.NoError(t, b.active.(*Memtable).commitlog.flushBuffers())
	recoveredDir := t.TempDir()
	require.NoError(t, os.CopyFS(recoveredDir, os.DirFS(dir)))
	require.NoError(t, b.Shutdown(ctx))

	recovered := newBucket(t, recoveredDir)
	defer recovered.Shutdown(ctx)

	got, err := recovered.MapList(ctx, rowKey)
	require.NoError(t, err)
	require.Equal(t, []MapPair{pair(1, 8, 8), pair(3, 1, 0), pair(4, 64, 64), pair(5, 2, 9000)}, got)

	got, err = recovered.MapList(ctx, []byte("other row"))
	require.NoError(t, err)
	require.Equal(t, []MapPair{pair(6, 3, 3)}, got)
}

func TestMapSetDoesNotAllocatePerWrite(t *testing.T) {
	tests := []struct {
		strategy string
		write    func(b *Bucket, rowKey, mapKey, value []byte) error
	}{
		{
			strategy: StrategyMapCollection,
			write: func(b *Bucket, rowKey, mapKey, value []byte) error {
				return b.MapSet(rowKey, MapPair{Key: mapKey, Value: value})
			},
		},
		{
			strategy: StrategyInverted,
			write: func(b *Bucket, rowKey, _, _ []byte) error {
				return b.InvertedSet(rowKey, 7, 3, 7)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.strategy, func(t *testing.T) {
			ctx := context.Background()
			b, err := NewBucketCreator().NewBucket(ctx, t.TempDir(), "", nullLogger(), nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(tt.strategy))
			require.NoError(t, err)
			defer b.Shutdown(ctx)

			const warmup, runs = 1000, 100
			rowKey := []byte("row")
			value := make([]byte, 8)
			// the memtable keeps the key slices, so every write needs its own
			keys := make([][]byte, warmup+runs+1)
			for i := range keys {
				keys[i] = binary.BigEndian.AppendUint64(nil, uint64(i))
			}

			next := 0
			var setErr error
			set := func() {
				if err := tt.write(b, rowKey, keys[next], value); err != nil {
					setErr = err
				}
				next++
			}
			for range warmup {
				set()
			}

			allocs := testing.AllocsPerRun(runs, set)
			require.NoError(t, setErr)
			require.Zero(t, allocs, "writing to an existing row must not allocate")
		})
	}
}
