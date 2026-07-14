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
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func newPinTestBucket(t *testing.T, dir string, opts ...BucketOption) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()

	b, err := NewBucketCreator().NewBucket(context.Background(), dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		append([]BucketOption{
			WithStrategy(StrategyReplace),
			// pinning requires an allocChecker (conservative default is not to pin)
			WithAllocChecker(memwatch.NewDummyMonitor()),
		}, opts...)...)
	require.NoError(t, err)
	return b
}

// failingAllocChecker simulates memory pressure: every allocation is denied.
type failingAllocChecker struct{}

func (failingAllocChecker) CheckAlloc(int64) error { return fmt.Errorf("not enough memory") }
func (failingAllocChecker) CheckMappingAndReserve(int64, int) error {
	return fmt.Errorf("not enough mappings")
}
func (failingAllocChecker) Refresh(bool) {}

// pinTestSegments returns the bucket's on-disk segments as concrete *segment
// values; only safe once the bucket is quiesced.
func pinTestSegments(t *testing.T, b *Bucket) []*segment {
	t.Helper()
	out := make([]*segment, 0, len(b.disk.segments))
	for _, s := range b.disk.segments {
		seg, ok := s.(*segment)
		require.True(t, ok, "expected eagerly loaded *segment, got %T", s)
		out = append(out, seg)
	}
	return out
}

func pinTestKey(i int) []byte {
	return []byte(fmt.Sprintf("primary-key-%05d", i))
}

func pinTestSecondaryKey(i int) []byte {
	// mirrors the docID secondary key shape: 8 bytes little-endian
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(i))
	return buf
}

func pinTestValue(i int) []byte {
	return []byte(fmt.Sprintf("value-%05d-%s", i, string(make([]byte, i%64))))
}

// TestSegmentIndexPin_ByteIdenticalResults asserts Get/GetBySecondary return
// identical results with pinning enabled vs disabled.
func TestSegmentIndexPin_ByteIdenticalResults(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "objects")
	const numKeys = 500

	opts := []BucketOption{WithSecondaryIndices(1)}

	type result struct {
		val []byte
		err error
	}
	expectGet := make([]result, numKeys+1)          // +1: one never-written key
	expectSecondary := make([]result, numKeys+1)    // +1: one never-written secondary
	deleted := func(i int) bool { return i%7 == 0 } // every 7th key gets deleted
	updated := func(i int) bool { return i%5 == 0 } // every 5th key gets updated in segment 2

	b := newPinTestBucket(t, dir, opts...)

	for i := 0; i < numKeys; i++ {
		require.NoError(t, b.Put(pinTestKey(i), pinTestValue(i), WithSecondaryKey(0, pinTestSecondaryKey(i))))
	}
	require.NoError(t, b.FlushMemtable())

	for i := 0; i < numKeys; i++ {
		if updated(i) {
			require.NoError(t, b.Put(pinTestKey(i), append(pinTestValue(i), []byte("-v2")...),
				WithSecondaryKey(0, pinTestSecondaryKey(i))))
		}
		if deleted(i) {
			require.NoError(t, b.Delete(pinTestKey(i), WithSecondaryKey(0, pinTestSecondaryKey(i))))
		}
	}
	require.NoError(t, b.FlushMemtable())

	for i := 0; i <= numKeys; i++ {
		val, err := b.Get(pinTestKey(i))
		expectGet[i] = result{val, err}
		val, err = b.GetBySecondary(ctx, 0, pinTestSecondaryKey(i))
		expectSecondary[i] = result{val, err}
	}
	require.NoError(t, b.Shutdown(ctx))

	verify := func(t *testing.T, b *Bucket) {
		for i := 0; i <= numKeys; i++ {
			val, err := b.Get(pinTestKey(i))
			assert.Equal(t, expectGet[i].err, err, "Get error for key %d", i)
			assert.Equal(t, expectGet[i].val, val, "Get value for key %d", i)

			val, err = b.GetBySecondary(ctx, 0, pinTestSecondaryKey(i))
			assert.Equal(t, expectSecondary[i].err, err, "GetBySecondary error for key %d", i)
			assert.Equal(t, expectSecondary[i].val, val, "GetBySecondary value for key %d", i)
		}
	}

	t.Run("pin enabled", func(t *testing.T) {
		b := newPinTestBucket(t, dir, append(opts, WithSegmentIndexPin(1<<30, 8<<30, SegmentIndexPinScopeObjects))...)
		defer func() { require.NoError(t, b.Shutdown(ctx)) }()

		segments := pinTestSegments(t, b)
		require.Len(t, segments, 2)
		for _, seg := range segments {
			assert.Positive(t, seg.pinnedIndexBytes, "segment %s should be pinned", seg.path)
			assert.Equal(t, seg.size-int64(seg.segmentStartPos), seg.pinnedIndexBytes)
		}
		verify(t, b)
	})

	t.Run("pin disabled (control)", func(t *testing.T) {
		b := newPinTestBucket(t, dir, opts...)
		defer func() { require.NoError(t, b.Shutdown(ctx)) }()

		for _, seg := range pinTestSegments(t, b) {
			assert.Zero(t, seg.pinnedIndexBytes)
		}
		verify(t, b)
	})
}

// TestSegmentIndexPin_ScopeFiltering asserts scope "objects" (and empty
// default) pin only the objects bucket, while "all" pins any bucket.
func TestSegmentIndexPin_ScopeFiltering(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name          string
		scope         string
		objectsPinned bool
		otherPinned   bool
	}{
		{name: "scope objects", scope: SegmentIndexPinScopeObjects, objectsPinned: true, otherPinned: false},
		{name: "scope empty defaults to objects", scope: "", objectsPinned: true, otherPinned: false},
		{name: "scope all", scope: SegmentIndexPinScopeAll, objectsPinned: true, otherPinned: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for _, bucketCase := range []struct {
				bucketName string
				wantPinned bool
			}{
				{"objects", tc.objectsPinned},
				{"property_title_searchable", tc.otherPinned},
			} {
				b := newPinTestBucket(t, filepath.Join(root, bucketCase.bucketName),
					WithSegmentIndexPin(1<<30, 8<<30, tc.scope))
				require.NoError(t, b.Put([]byte("key"), []byte("value")))
				require.NoError(t, b.FlushMemtable())

				segments := pinTestSegments(t, b)
				require.Len(t, segments, 1)
				if bucketCase.wantPinned {
					assert.Positive(t, segments[0].pinnedIndexBytes,
						"bucket %q should be pinned under scope %q", bucketCase.bucketName, tc.scope)
				} else {
					assert.Zero(t, segments[0].pinnedIndexBytes,
						"bucket %q should NOT be pinned under scope %q", bucketCase.bucketName, tc.scope)
				}
				require.NoError(t, b.Shutdown(ctx))
			}
		})
	}
}

// TestSegmentIndexPin_ThresholdBoundary asserts the threshold is inclusive of
// the index-region size, and that 0 disables pinning.
func TestSegmentIndexPin_ThresholdBoundary(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "objects")

	b := newPinTestBucket(t, dir)
	for i := 0; i < 100; i++ {
		require.NoError(t, b.Put(pinTestKey(i), pinTestValue(i)))
	}
	require.NoError(t, b.FlushMemtable())
	segments := pinTestSegments(t, b)
	require.Len(t, segments, 1)
	regionSize := segments[0].size - int64(segments[0].segmentStartPos)
	require.Positive(t, regionSize)
	require.NoError(t, b.Shutdown(ctx))

	cases := []struct {
		name       string
		threshold  int64
		wantPinned bool
	}{
		{name: "threshold equals region size", threshold: regionSize, wantPinned: true},
		{name: "threshold one below region size", threshold: regionSize - 1, wantPinned: false},
		{name: "threshold zero disables", threshold: 0, wantPinned: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := newPinTestBucket(t, dir, WithSegmentIndexPin(tc.threshold, 8<<30, SegmentIndexPinScopeObjects))
			defer func() { require.NoError(t, b.Shutdown(ctx)) }()

			segments := pinTestSegments(t, b)
			require.Len(t, segments, 1)
			if tc.wantPinned {
				assert.Equal(t, regionSize, segments[0].pinnedIndexBytes)
			} else {
				assert.Zero(t, segments[0].pinnedIndexBytes)
			}
		})
	}
}

// TestSegmentIndexPin_CompactionLifecycle asserts the pinned gauges track
// segment compaction and shutdown back to baseline.
func TestSegmentIndexPin_CompactionLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "objects")
	logger, _ := test.NewNullLogger()

	metrics, err := NewMetrics(monitoring.GetMetrics(), "pin-test-class", "pin-test-shard")
	require.NoError(t, err)

	pinnedTotal := func() float64 {
		return testutil.ToFloat64(metrics.segmentIndexPinnedTotal.WithLabelValues(StrategyReplace, "objects"))
	}
	pinnedBytes := func() float64 {
		return testutil.ToFloat64(metrics.segmentIndexPinnedBytes.WithLabelValues(StrategyReplace, "objects"))
	}
	baseTotal, baseBytes := pinnedTotal(), pinnedBytes()

	b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, metrics,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1),
		WithAllocChecker(memwatch.NewDummyMonitor()),
		WithSegmentIndexPin(1<<30, 8<<30, SegmentIndexPinScopeObjects))
	require.NoError(t, err)

	for i := 0; i < 200; i++ {
		require.NoError(t, b.Put(pinTestKey(i), pinTestValue(i), WithSecondaryKey(0, pinTestSecondaryKey(i))))
	}
	require.NoError(t, b.FlushMemtable())
	for i := 200; i < 400; i++ {
		require.NoError(t, b.Put(pinTestKey(i), pinTestValue(i), WithSecondaryKey(0, pinTestSecondaryKey(i))))
	}
	require.NoError(t, b.FlushMemtable())

	segments := pinTestSegments(t, b)
	require.Len(t, segments, 2)
	var wantBytes int64
	for _, seg := range segments {
		require.Positive(t, seg.pinnedIndexBytes)
		wantBytes += seg.pinnedIndexBytes
	}
	assert.Equal(t, baseTotal+2, pinnedTotal())
	assert.Equal(t, baseBytes+float64(wantBytes), pinnedBytes())

	// refcount is already zero here, so one dropSegmentsAwaiting pass suffices
	compacted, err := b.disk.compactOnce(ctx)
	require.NoError(t, err)
	require.True(t, compacted)
	_, err = b.disk.dropSegmentsAwaiting()
	require.NoError(t, err)

	segments = pinTestSegments(t, b)
	require.Len(t, segments, 1)
	require.Positive(t, segments[0].pinnedIndexBytes)
	assert.Equal(t, baseTotal+1, pinnedTotal())
	assert.Equal(t, baseBytes+float64(segments[0].pinnedIndexBytes), pinnedBytes())

	// results still byte-identical after the swap
	for i := 0; i < 400; i++ {
		val, err := b.Get(pinTestKey(i))
		require.NoError(t, err)
		require.Equal(t, pinTestValue(i), val)
		val, err = b.GetBySecondary(ctx, 0, pinTestSecondaryKey(i))
		require.NoError(t, err)
		require.Equal(t, pinTestValue(i), val)
	}

	require.NoError(t, b.Shutdown(ctx))
	assert.Equal(t, baseTotal, pinnedTotal())
	assert.Equal(t, baseBytes, pinnedBytes())
}

// TestSegmentIndexPin_SafetyFallbacks: pinning falls back to mmap (never
// fails the segment) under memory pressure, no allocChecker, or budget exhaustion.
func TestSegmentIndexPin_SafetyFallbacks(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	newBucketRaw := func(t *testing.T, dir string, opts ...BucketOption) *Bucket {
		t.Helper()
		b, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			append([]BucketOption{WithStrategy(StrategyReplace)}, opts...)...)
		require.NoError(t, err)
		return b
	}

	seed := func(t *testing.T, b *Bucket) {
		t.Helper()
		for i := 0; i < 100; i++ {
			require.NoError(t, b.Put(pinTestKey(i), pinTestValue(i)))
		}
		require.NoError(t, b.FlushMemtable())
	}

	verifyReads := func(t *testing.T, b *Bucket) {
		t.Helper()
		for i := 0; i < 100; i++ {
			val, err := b.Get(pinTestKey(i))
			require.NoError(t, err)
			require.Equal(t, pinTestValue(i), val)
		}
	}

	t.Run("memory pressure skips pin", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "objects")
		b := newBucketRaw(t, dir,
			WithAllocChecker(failingAllocChecker{}),
			WithSegmentIndexPin(1<<30, 8<<30, SegmentIndexPinScopeObjects))
		defer func() { require.NoError(t, b.Shutdown(ctx)) }()

		seed(t, b)
		for _, seg := range pinTestSegments(t, b) {
			assert.Zero(t, seg.pinnedIndexBytes)
		}
		verifyReads(t, b)
	})

	t.Run("nil allocChecker skips pin", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "objects")
		b := newBucketRaw(t, dir,
			WithSegmentIndexPin(1<<30, 8<<30, SegmentIndexPinScopeObjects))
		defer func() { require.NoError(t, b.Shutdown(ctx)) }()

		seed(t, b)
		for _, seg := range pinTestSegments(t, b) {
			assert.Zero(t, seg.pinnedIndexBytes)
		}
		verifyReads(t, b)
	})

	t.Run("node-wide budget clamps pinning", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "objects")

		// measure one segment's index-region size first
		b := newBucketRaw(t, dir)
		seed(t, b)
		segments := pinTestSegments(t, b)
		require.Len(t, segments, 1)
		regionSize := segments[0].size - int64(segments[0].segmentStartPos)
		require.NoError(t, b.Shutdown(ctx))

		// budget fits exactly one segment; write a second segment: only one
		// of the two can be pinned
		b = newBucketRaw(t, dir,
			WithAllocChecker(memwatch.NewDummyMonitor()),
			WithSegmentIndexPin(1<<30, regionSize, SegmentIndexPinScopeObjects))
		for i := 100; i < 200; i++ {
			require.NoError(t, b.Put(pinTestKey(i), pinTestValue(i)))
		}
		require.NoError(t, b.FlushMemtable())

		segments = pinTestSegments(t, b)
		require.Len(t, segments, 2)
		var pinned int
		for _, seg := range segments {
			if seg.pinnedIndexBytes > 0 {
				pinned++
			}
		}
		assert.Equal(t, 1, pinned, "budget of one region must pin exactly one segment")
		for i := 0; i < 200; i++ {
			val, err := b.Get(pinTestKey(i))
			require.NoError(t, err)
			require.Equal(t, pinTestValue(i), val)
		}
		require.NoError(t, b.Shutdown(ctx))

		// after shutdown the budget is released: a fresh open can pin again
		b = newBucketRaw(t, dir,
			WithAllocChecker(memwatch.NewDummyMonitor()),
			WithSegmentIndexPin(1<<30, regionSize, SegmentIndexPinScopeObjects))
		defer func() { require.NoError(t, b.Shutdown(ctx)) }()
		pinned = 0
		for _, seg := range pinTestSegments(t, b) {
			if seg.pinnedIndexBytes > 0 {
				pinned++
			}
		}
		assert.Equal(t, 1, pinned)
	})
}

// BenchmarkSegmentIndexPin_GetBySecondary compares pinned vs mmap'd reads;
// a warm page cache masks the real benefit (no page faults under memory pressure).
func BenchmarkSegmentIndexPin_GetBySecondary(b *testing.B) {
	ctx := context.Background()
	const numKeys = 20_000

	dir := filepath.Join(b.TempDir(), "objects")
	logger, _ := test.NewNullLogger()

	setup, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSecondaryIndices(1))
	require.NoError(b, err)
	for i := 0; i < numKeys; i++ {
		require.NoError(b, setup.Put(pinTestKey(i), pinTestValue(i), WithSecondaryKey(0, pinTestSecondaryKey(i))))
	}
	require.NoError(b, setup.FlushMemtable())
	require.NoError(b, setup.Shutdown(ctx))

	for _, bc := range []struct {
		name      string
		threshold int64
	}{
		{name: "pin=off", threshold: 0},
		{name: "pin=on", threshold: 1 << 30},
	} {
		b.Run(bc.name, func(b *testing.B) {
			bucket, err := NewBucketCreator().NewBucket(ctx, dir, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(StrategyReplace), WithSecondaryIndices(1),
				WithAllocChecker(memwatch.NewDummyMonitor()),
				WithSegmentIndexPin(bc.threshold, 8<<30, SegmentIndexPinScopeObjects))
			require.NoError(b, err)
			defer bucket.Shutdown(ctx)

			rnd := rand.New(rand.NewSource(42))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := bucket.GetBySecondary(ctx, 0, pinTestSecondaryKey(rnd.Intn(numKeys)))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
