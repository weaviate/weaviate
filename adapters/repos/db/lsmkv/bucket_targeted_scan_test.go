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
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

// collectMergedCursor is the reference visibility ScanTargetedReplace must match.
// Keyed by key, not value: zero-length values are indistinguishable otherwise, so
// a duplicated or dropped empty row would not show up.
func collectMergedCursor(t *testing.T, b *Bucket) map[string]string {
	t.Helper()
	expected := map[string]string{}
	c := b.Cursor()
	defer c.Close()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		_, seen := expected[string(k)]
		require.False(t, seen, "cursor yielded key %q twice", k)
		expected[string(k)] = string(v)
	}
	return expected
}

func scanCollect(t *testing.T, b *Bucket, peekSize, parallel int) map[string]string {
	t.Helper()
	var mu sync.Mutex
	got := map[string]string{}
	require.NoError(t, b.ScanTargetedReplace(context.Background(), peekSize, parallel,
		func(e *TargetedScanEntry) error {
			raw, err := e.ReadRange(0, 0)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			if _, seen := got[string(e.Key)]; seen {
				return fmt.Errorf("key %q served twice", e.Key)
			}
			got[string(e.Key)] = string(raw)
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
			for i := uint64(0); i < 40; i++ {
				put(i, int(i)*7%300)
			}
			for i := uint64(100); i < 103; i++ {
				putEmpty(i)
			}
			require.NoError(t, b.FlushAndSwitch())
			// second segment: an empty is superseded by a value and a value by an
			// empty, across segments
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
			// left in the memtable: updates and deletes over segment winners
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
				got := map[string]string{}
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
					defer mu.Unlock()
					_, seen := got[string(e.Key)]
					assert.False(t, seen, "key %q served twice", e.Key)
					got[string(e.Key)] = string(whole)
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

	for i := uint64(0); i < 20; i++ {
		put(i, 40)
	}
	require.NoError(t, b.FlushAndSwitch())

	// this generation is parked below as the flushing memtable
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

// benchScanFixture spreads rows over the given number of flushed segments. Keys
// are disjoint per segment, so every row survives the newest-wins probe and the
// scan pays the full O(rows x segments) probe term.
func benchScanFixture(b *testing.B, segments, valueLen int, opts ...BucketOption) (*Bucket, int) {
	b.Helper()
	ctx := context.Background()
	dir := b.TempDir()
	o := append([]BucketOption{WithStrategy(StrategyReplace)}, opts...)
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, nullLogger(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), o...)
	require.NoError(b, err)
	bucket.SetMemtableThreshold(1e9)

	const rows = 200_000
	perSegment := rows / segments
	for s := 0; s < segments; s++ {
		for i := 0; i < perSegment; i++ {
			id := uint64(s*perSegment + i)
			require.NoError(b, bucket.Put([]byte(fmt.Sprintf("key-%08d", id)),
				targetedTestValue(id, valueLen)))
		}
		require.NoError(b, bucket.FlushAndSwitch())
	}
	return bucket, perSegment * segments
}

// BenchmarkScanTargetedReplace compares the targeted scan against the merged
// cursor it exists to avoid. Both arms read every row; the scan additionally
// extracts a 16-byte prefix, which is the whole point — the cursor materializes
// each full value to hand it back.
//
// Values are 4 KiB, the shape the scan is for: a caller wanting a few bytes out
// of a large object. Segment counts bracket a freshly compacted bucket and a
// churned one.
func BenchmarkScanTargetedReplace(b *testing.B) {
	ctx := context.Background()
	const valueLen = 4096

	// pread is what ships (PERSISTENCE_LSM_ACCESS_STRATEGY); mmap serves reads as
	// zero-copy subslices, so it hides the per-row reader stack the pread arm pays
	modes := []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true), WithMinMMapSize(0)}},
	}

	for _, mode := range modes {
		for _, segments := range []int{1, 16} {
			b.Run(fmt.Sprintf("%s/segments=%d/targetedScan", mode.name, segments), func(b *testing.B) {
				bucket, rows := benchScanFixture(b, segments, valueLen, mode.opts...)
				defer bucket.Shutdown(ctx)

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
					if count.Load() != int64(rows) {
						b.Fatalf("scanned %d rows, want %d", count.Load(), rows)
					}
				}
			})

			b.Run(fmt.Sprintf("%s/segments=%d/mergedCursor", mode.name, segments), func(b *testing.B) {
				bucket, rows := benchScanFixture(b, segments, valueLen, mode.opts...)
				defer bucket.Shutdown(ctx)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count := 0
					c := bucket.Cursor()
					for k, v := c.First(); k != nil; k, v = c.Next() {
						_ = v[:16]
						count++
					}
					c.Close()
					if count != rows {
						b.Fatalf("cursor saw %d rows, want %d", count, rows)
					}
				}
			})
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

		// asserted against the fixture's own geometry rather than a recomputation
		// of the implementation: 108-byte values plus a 7-byte key and the node's
		// per-row overhead, and well under the row size once the index is counted
		const valueLen = 8 + 100
		got := b.EstimatedEntrySize()
		require.Greater(t, got, int64(valueLen))
		require.Less(t, got, int64(valueLen*2))

		segments, release := b.disk.getConsistentViewOfSegments()
		for _, seg := range segments {
			// the estimate must exclude the index tree
			require.Less(t, int64(seg.payloadSize()), seg.Size())
		}
		release()
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

// TestScanTargetedReplaceSlicesAreCapped: every slice the scan hands out is a
// window into a segment or a reused buffer, so spare capacity would let a caller
// reslice into a neighbouring row. A caller decoding a length field out of a
// corrupt row does exactly that.
func TestScanTargetedReplaceSlicesAreCapped(t *testing.T) {
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

			for i := uint64(0); i < 30; i++ {
				targetedPut(t, b, i, 200)
			}
			require.NoError(t, b.FlushAndSwitch())
			// memtable rows too: they hand out the memtable's own value slices
			for i := uint64(30); i < 40; i++ {
				targetedPut(t, b, i, 200)
			}

			var served atomic.Int64
			require.NoError(t, b.ScanTargetedReplace(ctx, 16, 4, func(e *TargetedScanEntry) error {
				assert.Equal(t, len(e.Peek), cap(e.Peek), "Peek carries spare capacity")

				part, err := e.ReadRange(0, 32)
				if assert.NoError(t, err) {
					assert.Equal(t, len(part), cap(part), "ReadRange carries spare capacity")
				}
				whole, err := e.ReadRange(0, 0)
				if assert.NoError(t, err) {
					assert.Equal(t, len(whole), cap(whole), "ReadRange carries spare capacity")
				}
				served.Add(1)
				return nil
			}, nullLogger()))
			require.Equal(t, int64(40), served.Load())
		})
	}
}

// TestScanTargetedReplaceSecondaryIndex is the objects-bucket shape: a secondary
// index bounds the primary one before EOF, so the index blob the scan walks is
// delimited by the first secondary offset rather than by the file end.
func TestScanTargetedReplaceSecondaryIndex(t *testing.T) {
	ctx := context.Background()

	for _, mode := range []struct {
		name string
		opts []BucketOption
	}{
		{"mmap", nil},
		{"pread", []BucketOption{WithPread(true), WithMinMMapSize(0)}},
		{"mmap/checksums", []BucketOption{WithSegmentsChecksumValidationEnabled(true)}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			opts := append([]BucketOption{WithSecondaryIndices(1)}, mode.opts...)
			b := newReusableTestBucket(t, ctx, opts...)
			defer b.Shutdown(ctx)

			put := func(id uint64, fillerLen int) {
				require.NoError(t, b.Put(
					[]byte(fmt.Sprintf("key-%03d", id)), targetedTestValue(id, fillerLen),
					WithSecondaryKey(0, []byte(fmt.Sprintf("sec-%03d", id)))))
			}

			for i := uint64(0); i < 40; i++ {
				put(i, int(i)*7%300)
			}
			require.NoError(t, b.FlushAndSwitch())
			// second segment supersedes some rows and deletes one
			for i := uint64(5); i < 15; i++ {
				put(i, 500+int(i))
			}
			require.NoError(t, b.Delete([]byte("key-020"),
				WithSecondaryKey(0, []byte("sec-020"))))
			require.NoError(t, b.FlushAndSwitch())
			// and a memtable generation on top
			put(7, 900)

			expected := collectMergedCursor(t, b)
			require.NotEmpty(t, expected)
			for _, parallel := range []int{1, 4, 16} {
				require.Equal(t, expected, scanCollect(t, b, 16, parallel),
					"parallel=%d", parallel)
			}
		})
	}
}

// TestScanTargetedReplaceMemtableOnly: a bucket that has never flushed has no
// segment to read the oldest memtable's keys back, so they are not collected.
func TestScanTargetedReplaceMemtableOnly(t *testing.T) {
	ctx := context.Background()
	b := newReusableTestBucket(t, ctx)
	defer b.Shutdown(ctx)

	for i := uint64(0); i < 50; i++ {
		targetedPut(t, b, i, 120)
	}
	targetedPutEmpty(t, b, 60)
	require.NoError(t, b.Delete([]byte("key-010")))

	require.Equal(t, collectMergedCursor(t, b), scanCollect(t, b, 16, 4))
}

// TestScanTargetedReplaceFullyShadowed: every row of the older segment is hidden
// by newer tombstones, so the scan serves nothing from it while still walking it.
func TestScanTargetedReplaceFullyShadowed(t *testing.T) {
	ctx := context.Background()
	b := newReusableTestBucket(t, ctx)
	defer b.Shutdown(ctx)

	const rows = 40
	for i := uint64(0); i < rows; i++ {
		targetedPut(t, b, i, 200)
	}
	require.NoError(t, b.FlushAndSwitch())
	for i := uint64(0); i < rows; i++ {
		require.NoError(t, b.Delete([]byte(fmt.Sprintf("key-%03d", i))))
	}
	require.NoError(t, b.FlushAndSwitch())

	expected := collectMergedCursor(t, b)
	require.Empty(t, expected, "fixture must leave nothing live")
	require.Equal(t, expected, scanCollect(t, b, 16, 4))
}

func TestScanTargetedReplaceArgumentValidation(t *testing.T) {
	ctx := context.Background()
	b := newReusableTestBucket(t, ctx)
	defer b.Shutdown(ctx)
	for i := uint64(0); i < 20; i++ {
		targetedPut(t, b, i, 100)
	}
	require.NoError(t, b.FlushAndSwitch())
	expected := collectMergedCursor(t, b)

	t.Run("peek size must be positive", func(t *testing.T) {
		for _, peek := range []int{0, -1} {
			err := b.ScanTargetedReplace(ctx, peek, 4, func(*TargetedScanEntry) error {
				t.Error("callback must not run")
				return nil
			}, nullLogger())
			require.ErrorContains(t, err, "peek size must be positive")
		}
	})

	// parallel is clamped rather than rejected: every value >= 1 yields the same
	// rows, and the upper bound keeps an oversized value from becoming a task per
	// index node
	t.Run("parallel is clamped", func(t *testing.T) {
		for _, parallel := range []int{-1, 0, 1, 100000} {
			require.Equal(t, expected, scanCollect(t, b, 16, parallel),
				"parallel=%d", parallel)
		}
	})
}

// scanAbortFixture: enough rows across enough segments that a scan is still
// running by the time a callback aborts it.
func scanAbortFixture(t *testing.T, ctx context.Context) *Bucket {
	t.Helper()
	b := newReusableTestBucket(t, ctx)
	t.Cleanup(func() { b.Shutdown(ctx) })
	for seg := 0; seg < 4; seg++ {
		for i := 0; i < 4000; i++ {
			id := uint64(seg*4000 + i)
			require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%08d", id)),
				targetedTestValue(id, 40)))
		}
		require.NoError(t, b.FlushAndSwitch())
	}
	return b
}

// TestScanTargetedReplaceCancelledMidScan reaches the periodic context checks,
// which the already-cancelled case cannot: that one returns before the snapshot.
func TestScanTargetedReplaceCancelledMidScan(t *testing.T) {
	ctx := context.Background()
	b := scanAbortFixture(t, ctx)

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var served atomic.Int64
	err := b.ScanTargetedReplace(scanCtx, 16, 4, func(e *TargetedScanEntry) error {
		if served.Add(1) == 2000 {
			cancel()
		}
		return nil
	}, nullLogger())

	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, served.Load(), int64(16000), "scan should stop short of every row")
}

func TestScanTargetedReplaceCallbackError(t *testing.T) {
	ctx := context.Background()
	b := scanAbortFixture(t, ctx)
	sentinel := errors.New("caller gave up")

	for _, parallel := range []int{1, 8} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			var served atomic.Int64
			err := b.ScanTargetedReplace(ctx, 16, parallel, func(e *TargetedScanEntry) error {
				if served.Add(1) == 100 {
					return sentinel
				}
				return nil
			}, nullLogger())

			require.ErrorIs(t, err, sentinel)
			// the remaining workers stop rather than draining the bucket
			require.Less(t, served.Load(), int64(16000))
		})
	}
}

// TestScanTargetedReplaceCallbackPanic pins the containment claim: a panicking
// worker becomes an error and cancels its siblings, so the scan returns instead
// of leaving the caller blocked while segment references are held.
func TestScanTargetedReplaceCallbackPanic(t *testing.T) {
	// containment is what is under test, and DISABLE_RECOVERY_ON_PANIC turns the
	// error group's recover into a no-op — set in CI and in some local setups,
	// where the panic would take the test binary down instead of being asserted on
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	ctx := context.Background()
	b := scanAbortFixture(t, ctx)

	for _, parallel := range []int{1, 8} {
		t.Run(fmt.Sprintf("parallel=%d", parallel), func(t *testing.T) {
			var served atomic.Int64
			done := make(chan error, 1)
			enterrors.GoWrapper(func() {
				done <- b.ScanTargetedReplace(ctx, 16, parallel, func(e *TargetedScanEntry) error {
					if served.Add(1) == 100 {
						panic("boom")
					}
					return nil
				}, nullLogger())
			}, nullLogger())

			select {
			case err := <-done:
				require.Error(t, err, "a panicking callback must surface as an error")
			case <-time.After(30 * time.Second):
				t.Fatal("scan did not return after a callback panic")
			}
		})
	}
}

// BenchmarkScanTargetedReplaceParallelism sweeps the worker count in pread, where
// concurrent reads share one file descriptor and the answer turns out to depend
// on the platform. Measured over 200k rows of 4 KiB across 8 segments, warm:
//
//	workers        1      4     16     64    256
//	linux/amd64  309ms   91ms   40ms   35ms   35ms
//	darwin/arm64 204ms   86ms  164ms  163ms  169ms
//
// Linux scales to roughly the core count and then flattens; Darwin peaks around
// four workers and loses half its throughput beyond that. Size this for the
// deployment target — a number tuned on a laptop is wrong on a server, and the
// reverse.
func BenchmarkScanTargetedReplaceParallelism(b *testing.B) {
	ctx := context.Background()
	bucket, rows := benchScanFixture(b, 8, 4096, WithPread(true), WithMinMMapSize(0))
	defer bucket.Shutdown(ctx)

	for _, parallel := range []int{1, 4, 16, 64, 256} {
		b.Run(fmt.Sprintf("parallel=%d", parallel), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var count atomic.Int64
				err := bucket.ScanTargetedReplace(ctx, 16, parallel,
					func(e *TargetedScanEntry) error {
						count.Add(1)
						return nil
					}, nullLogger())
				if err != nil {
					b.Fatal(err)
				}
				if count.Load() != int64(rows) {
					b.Fatalf("scanned %d rows, want %d", count.Load(), rows)
				}
			}
		})
	}
}

// TestScanTargetedReplaceConcurrentWrites exercises the snapshot claim on
// targetedScanSnapshot: memtable cursors flatten shallow node copies under the
// memtable's read lock, so a same-key update reassigns the original node's
// fields rather than the cursor's. Run with -race; the assertion is that the
// detector stays quiet and the scan still completes.
func TestScanTargetedReplaceConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	b := newReusableTestBucket(t, ctx)
	defer b.Shutdown(ctx)

	const rows = 3000
	for i := uint64(0); i < rows; i++ {
		targetedPut(t, b, i, 150)
	}
	require.NoError(t, b.FlushAndSwitch())
	// a memtable generation, which is the half that hands out live value slices
	for i := uint64(0); i < 500; i++ {
		targetedPut(t, b, i, 90)
	}

	writerCtx, stopWriter := context.WithCancel(ctx)
	var writerDone sync.WaitGroup
	writerDone.Add(1)
	enterrors.GoWrapper(func() {
		defer writerDone.Done()
		for n := 0; writerCtx.Err() == nil; n++ {
			targetedPut(t, b, uint64(n%500), 90+n%40)
		}
	}, nullLogger())

	var served atomic.Int64
	err := b.ScanTargetedReplace(ctx, 16, 4, func(e *TargetedScanEntry) error {
		raw, err := e.ReadRange(0, 0)
		if err != nil {
			return err
		}
		// touch the bytes so the detector sees the read
		if len(raw) > 0 {
			_ = raw[len(raw)-1]
		}
		served.Add(1)
		return nil
	}, nullLogger())

	stopWriter()
	writerDone.Wait()

	require.NoError(t, err)
	require.Positive(t, served.Load())
}
