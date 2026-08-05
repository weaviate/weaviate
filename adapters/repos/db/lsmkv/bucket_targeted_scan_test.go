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

//go:build integrationTest

package lsmkv

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// targetedTestValue builds a value identifiable from its peek: 8-byte BE id, then a
// deterministic filler of variable length so peeks, tails, and whole reads differ.
func targetedTestValue(id uint64, fillerLen int) []byte {
	v := make([]byte, 8+fillerLen)
	binary.BigEndian.PutUint64(v, id)
	for i := 0; i < fillerLen; i++ {
		v[8+i] = byte((id + uint64(i)) % 251)
	}
	return v
}

func targetedPut(t *testing.T, b *Bucket, id uint64, fillerLen int) {
	t.Helper()
	require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), targetedTestValue(id, fillerLen)))
}

// a zero-length value is a live entry, not a deletion
func targetedPutEmpty(t *testing.T, b *Bucket, id uint64) {
	t.Helper()
	require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), []byte{}))
}

// collectMergedCursor is the reference visibility ScanTargetedReplace must match:
// value -> occurrence count over Bucket.Cursor's merged view.
func collectMergedCursor(t *testing.T, b *Bucket) map[string]int {
	t.Helper()
	expected := map[string]int{}
	c := b.Cursor()
	defer c.Close()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		expected[string(v)]++
	}
	return expected
}

// scanCollect runs a full scan, reading each entry's whole value, in the same
// value -> count shape as collectMergedCursor.
func scanCollect(t *testing.T, b *Bucket, peekSize, parallel int) map[string]int {
	t.Helper()
	var mu sync.Mutex
	got := map[string]int{}
	require.NoError(t, b.ScanTargetedReplace(context.Background(), peekSize, parallel,
		func(e *TargetedScanEntry) error {
			raw, err := e.ReadRange(0, 0)
			if err != nil {
				return err
			}
			mu.Lock()
			got[string(raw)]++
			mu.Unlock()
			return nil
		}, nullLogger()))
	return got
}

// TestScanTargetedReplace verifies merged-cursor visibility plus the entry
// mechanics: only the newest version of each key is served (updates across
// segments and memtables supersede, deletes hide bucket-wide), and Peek /
// ReadRange expose exact value bytes — in both mmap and pread modes.
func TestScanTargetedReplace(t *testing.T) {
	ctx := context.Background()

	modes := []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true), WithMinMMapSize(0)}},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			b := newReusableTestBucket(t, ctx, mode.opts...)
			defer b.Shutdown(ctx)

			put := func(id uint64, fillerLen int) { targetedPut(t, b, id, fillerLen) }
			putEmpty := func(id uint64) { targetedPutEmpty(t, b, id) }
			// segment 1: ids 0..39, plus empties at 100..102
			for i := uint64(0); i < 40; i++ {
				put(i, int(i)*7%300)
			}
			for i := uint64(100); i < 103; i++ {
				putEmpty(i)
			}
			require.NoError(t, b.FlushAndSwitch())
			// segment 2: updates 5..14, delete 20, new ids 40..59; an empty is
			// superseded by a value and a value by an empty, across segments
			for i := uint64(5); i < 15; i++ {
				put(i, 500+int(i))
			}
			require.NoError(t, b.Delete([]byte("key-020")))
			for i := uint64(40); i < 60; i++ {
				put(i, 30)
			}
			put(100, 64)
			putEmpty(30)
			require.NoError(t, b.FlushAndSwitch())
			// memtable: update a segment-2 winner, delete another, new ids 60..69,
			// one empty overriding a segment value and one fresh empty
			put(7, 900)
			require.NoError(t, b.Delete([]byte("key-010")))
			for i := uint64(60); i < 70; i++ {
				put(i, 500)
			}
			putEmpty(45)
			putEmpty(110)

			expected := collectMergedCursor(t, b)

			// parallel=1 keeps each segment as a single task, 4 and 16 split each
			// segment's index into multiple node-aligned byte ranges
			for _, parallel := range []int{1, 4, 16} {
				const peekSize = 16
				var mu sync.Mutex
				got := map[string]int{}
				err := b.ScanTargetedReplace(ctx, peekSize, parallel, func(e *TargetedScanEntry) error {
					// assert, not require: the callback runs on worker goroutines and
					// FailNow must not run off the test goroutine
					wantPeek := e.ValueSize
					if wantPeek > peekSize {
						wantPeek = peekSize
					}
					assert.Equal(t, wantPeek, uint64(len(e.Peek)))

					raw, err := e.ReadRange(0, 0)
					if !assert.NoError(t, err) {
						return err
					}
					assert.Equal(t, e.ValueSize, uint64(len(raw)))
					// a second ReadRange invalidates raw (shared scratch buffer): copy first
					whole := make([]byte, len(raw))
					copy(whole, raw)
					assert.Equal(t, whole[:len(e.Peek)], e.Peek)

					if e.ValueSize >= 11 {
						part, err := e.ReadRange(3, 11)
						assert.NoError(t, err)
						assert.Equal(t, whole[3:11], part)
					}
					_, err = e.ReadRange(0, e.ValueSize+1)
					assert.Error(t, err)

					mu.Lock()
					got[string(whole)]++
					mu.Unlock()
					return nil
				}, nullLogger())
				require.NoError(t, err)

				require.Equal(t, expected, got, "parallel=%d", parallel)
			}
		})
	}
}

func TestScanTargetedReplaceEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("empty bucket", func(t *testing.T) {
		b := newReusableTestBucket(t, ctx)
		defer b.Shutdown(ctx)
		err := b.ScanTargetedReplace(ctx, 16, 4, func(e *TargetedScanEntry) error {
			t.Error("callback must not run on an empty bucket")
			return nil
		}, nullLogger())
		require.NoError(t, err)
	})

	t.Run("context cancelled", func(t *testing.T) {
		b := newReusableTestBucket(t, ctx)
		defer b.Shutdown(ctx)
		for i := uint64(0); i < 3000; i++ {
			require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%05d", i)), targetedTestValue(i, 20)))
		}
		require.NoError(t, b.FlushAndSwitch())

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		err := b.ScanTargetedReplace(cancelled, 16, 4, func(e *TargetedScanEntry) error {
			return nil
		}, nullLogger())
		require.ErrorIs(t, err, context.Canceled)
	})
}

// TestScanTargetedReplaceFlushingMemtable pins the two-memtable visibility path:
// with a flushing memtable parked, active entries and tombstones must hide
// flushing rows, and both memtables must hide segment rows.
func TestScanTargetedReplaceFlushingMemtable(t *testing.T) {
	ctx := context.Background()
	b := newReusableTestBucket(t, ctx)
	defer b.Shutdown(ctx)

	put := func(id uint64, fillerLen int) { targetedPut(t, b, id, fillerLen) }
	putEmpty := func(id uint64) { targetedPutEmpty(t, b, id) }

	// segment: ids 0..19
	for i := uint64(0); i < 20; i++ {
		put(i, 40)
	}
	require.NoError(t, b.FlushAndSwitch())

	// this generation is parked below as the flushing memtable: updates 0..4, a
	// tombstone and an empty over segment rows, new ids 100..104
	for i := uint64(0); i < 5; i++ {
		put(i, 200)
	}
	require.NoError(t, b.Delete([]byte("key-010")))
	putEmpty(15)
	for i := uint64(100); i < 105; i++ {
		put(i, 60)
	}
	switched, err := b.atomicallySwitchMemtable(b.createNewActiveMemtable)
	require.NoError(t, err)
	require.True(t, switched)
	require.NotNil(t, b.flushing)

	// active: supersedes flushing rows (update, tombstone, empty-over-value) and
	// segment rows, plus fresh ids 200..204
	put(0, 700)
	require.NoError(t, b.Delete([]byte("key-100")))
	putEmpty(101)
	put(12, 300)
	for i := uint64(200); i < 205; i++ {
		put(i, 80)
	}

	require.Equal(t, collectMergedCursor(t, b), scanCollect(t, b, 16, 4))

	// finish the parked flush the same way FlushAndSwitch would, so Shutdown
	// sees a normal bucket
	segPath, err := b.flushing.flush()
	require.NoError(t, err)
	seg, err := b.disk.initAndPrecomputeNewSegment(segPath)
	require.NoError(t, err)
	require.NoError(t, b.atomicallyAddDiskSegmentAndRemoveFlushing(seg))
}

// TestScanTargetedReplaceLazySegments reopens a populated bucket with lazy
// segment loading, so the scan runs through lazySegment's forwarders (fresh
// buckets only ever hold eager segments).
func TestScanTargetedReplaceLazySegments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	newB := func(opts ...BucketOption) *Bucket {
		o := append([]BucketOption{WithStrategy(StrategyReplace)}, opts...)
		b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), o...)
		require.NoError(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	b := newB()
	put := func(id uint64, fillerLen int) { targetedPut(t, b, id, fillerLen) }
	for i := uint64(0); i < 30; i++ {
		put(i, 50)
	}
	require.NoError(t, b.FlushAndSwitch())
	for i := uint64(5); i < 10; i++ {
		put(i, 300)
	}
	require.NoError(t, b.Delete([]byte("key-020")))
	for i := uint64(40); i < 50; i++ {
		put(i, 25)
	}
	require.NoError(t, b.FlushAndSwitch())
	require.NoError(t, b.Shutdown(ctx))

	b = newB(WithLazySegmentLoading(true))
	defer b.Shutdown(ctx)

	segments, release := b.disk.getConsistentViewOfSegments()
	require.NotEmpty(t, segments)
	_, isLazy := segments[0].(*lazySegment)
	release()
	require.True(t, isLazy, "reopen with lazy loading must yield lazy segments")

	require.Equal(t, collectMergedCursor(t, b), scanCollect(t, b, 16, 4))
}

// BenchmarkScanTargetedReplace scans one flushed ~200k-row segment, peek 16,
// parallel 4.
//
// Run with:
//
//	go test -tags integrationTest -run x -bench BenchmarkScanTargetedReplace ./adapters/repos/db/lsmkv/
func BenchmarkScanTargetedReplace(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(b, err)
	defer bucket.Shutdown(ctx)
	bucket.SetMemtableThreshold(1e9)

	const rows = 200_000
	for i := uint64(0); i < rows; i++ {
		require.NoError(b, bucket.Put([]byte(fmt.Sprintf("key-%08d", i)), targetedTestValue(i, 56)))
	}
	require.NoError(b, bucket.FlushAndSwitch())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var count atomic.Int64
		err := bucket.ScanTargetedReplace(ctx, 16, 4, func(e *TargetedScanEntry) error {
			count.Add(1)
			return nil
		}, nullLogger())
		if err != nil {
			b.Fatal(err)
		}
		if count.Load() != rows {
			b.Fatalf("scanned %d rows, want %d", count.Load(), rows)
		}
	}
}

// TestScanTargetedReplaceChecksumTail pins that the checksum trailer is stripped
// from the index blob on the writer's terms, not the reader's: a segment written
// with checksums must scan when reopened with validation off (the default), where
// the trailer would otherwise sit in the primary index as a partial node.
func TestScanTargetedReplaceChecksumTail(t *testing.T) {
	ctx := context.Background()

	// no secondary index, so nothing bounds the primary index before EOF
	for _, writeChecksums := range []bool{true, false} {
		t.Run(fmt.Sprintf("written_with_checksums=%v", writeChecksums), func(t *testing.T) {
			dir := t.TempDir()
			newB := func(opts ...BucketOption) *Bucket {
				o := append([]BucketOption{WithStrategy(StrategyReplace)}, opts...)
				b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), o...)
				require.NoError(t, err)
				b.SetMemtableThreshold(1e9)
				return b
			}

			b := newB(WithSegmentsChecksumValidationEnabled(writeChecksums))
			for i := uint64(0); i < 20; i++ {
				targetedPut(t, b, i, 40)
			}
			require.NoError(t, b.FlushAndSwitch())
			expected := collectMergedCursor(t, b)
			require.NoError(t, b.Shutdown(ctx))

			// reopen with validation off, as a differently-configured node would
			b = newB()
			defer b.Shutdown(ctx)
			require.Equal(t, expected, scanCollect(t, b, 16, 4))
		})
	}
}

// TestScanTargetedReplaceCorruptValueLength corrupts a node's stored value
// length on disk: the scan must report it rather than size a read from it.
func TestScanTargetedReplaceCorruptValueLength(t *testing.T) {
	ctx := context.Background()

	modes := []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true), WithMinMMapSize(0)}},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			dir := t.TempDir()
			newB := func() *Bucket {
				o := append([]BucketOption{WithStrategy(StrategyReplace)}, mode.opts...)
				b, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), o...)
				require.NoError(t, err)
				b.SetMemtableThreshold(1e9)
				return b
			}

			b := newB()
			key := []byte("key-000")
			require.NoError(t, b.Put(key, targetedTestValue(0, 100)))
			require.NoError(t, b.FlushAndSwitch())

			segments, release := b.disk.getConsistentViewOfSegments()
			require.Len(t, segments, 1)
			seg := segments[0].(*segment)
			node, err := seg.index.Get(key)
			require.NoError(t, err)
			path := seg.getPath()
			release()
			require.NoError(t, b.Shutdown(ctx))

			// the 8-byte little-endian value length follows the node's tombstone byte
			f, err := os.OpenFile(path, os.O_WRONLY, 0)
			require.NoError(t, err)
			_, err = f.WriteAt(bytes.Repeat([]byte{0xFF}, 8), int64(node.Start)+1)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			b = newB()
			defer b.Shutdown(ctx)

			var served atomic.Int64
			err = b.ScanTargetedReplace(ctx, 16, 1, func(e *TargetedScanEntry) error {
				served.Add(1)
				return nil
			}, nullLogger())
			require.ErrorContains(t, err, "value length")
			require.Zero(t, served.Load(), "the corrupt row must not be served")
		})
	}
}

func TestEstimatedEntrySize(t *testing.T) {
	ctx := context.Background()

	t.Run("with net-additions tracking", func(t *testing.T) {
		b := newReusableTestBucket(t, ctx, WithCalcCountNetAdditions(true))
		defer b.Shutdown(ctx)

		require.Zero(t, b.EstimatedEntrySize())

		const entries = 100
		for i := uint64(0); i < entries; i++ {
			require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), targetedTestValue(i, 100)))
		}
		require.NoError(t, b.FlushAndSwitch())

		segments, release := b.disk.getConsistentViewOfSegments()
		var size int64
		for _, seg := range segments {
			size += int64(seg.payloadSize())
			// the estimate must exclude the index tree
			require.Less(t, int64(seg.payloadSize()), seg.Size())
		}
		release()
		require.Equal(t, size/entries, b.EstimatedEntrySize())
	})

	t.Run("without net-additions tracking", func(t *testing.T) {
		b := newReusableTestBucket(t, ctx)
		defer b.Shutdown(ctx)

		for i := uint64(0); i < 10; i++ {
			require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", i)), targetedTestValue(i, 100)))
		}
		require.NoError(t, b.FlushAndSwitch())
		require.Zero(t, b.EstimatedEntrySize())
	})
}
