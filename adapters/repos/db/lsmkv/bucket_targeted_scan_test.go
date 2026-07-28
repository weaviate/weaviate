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
	"context"
	"encoding/binary"
	"fmt"
	"sync"
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

			put := func(id uint64, fillerLen int) {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), targetedTestValue(id, fillerLen)))
			}
			// a zero-length value is a live entry, not a deletion
			putEmpty := func(id uint64) {
				require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), []byte{}))
			}
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

			expected := map[string]int{}
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				expected[string(v)]++
				_ = k
			}
			c.Close()

			// parallel=1 splits nothing, 4 exercises the single-seed split, 16 the
			// multi-seed path (sorted/deduped quantile bounds, both-bounded ranges)
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

	put := func(id uint64, fillerLen int) {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), targetedTestValue(id, fillerLen)))
	}
	putEmpty := func(id uint64) {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), []byte{}))
	}

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

	expected := map[string]int{}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		expected[string(v)]++
		_ = k
	}
	c.Close()

	var mu sync.Mutex
	got := map[string]int{}
	require.NoError(t, b.ScanTargetedReplace(ctx, 16, 4, func(e *TargetedScanEntry) error {
		raw, err := e.ReadRange(0, 0)
		if err != nil {
			return err
		}
		mu.Lock()
		got[string(raw)]++
		mu.Unlock()
		return nil
	}, nullLogger()))
	require.Equal(t, expected, got)

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
	put := func(id uint64, fillerLen int) {
		require.NoError(t, b.Put([]byte(fmt.Sprintf("key-%03d", id)), targetedTestValue(id, fillerLen)))
	}
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

	expected := map[string]int{}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		expected[string(v)]++
		_ = k
	}
	c.Close()

	var mu sync.Mutex
	got := map[string]int{}
	require.NoError(t, b.ScanTargetedReplace(ctx, 16, 4, func(e *TargetedScanEntry) error {
		raw, err := e.ReadRange(0, 0)
		if err != nil {
			return err
		}
		mu.Lock()
		got[string(raw)]++
		mu.Unlock()
		return nil
	}, nullLogger()))
	require.Equal(t, expected, got)
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
			size += seg.Size()
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
