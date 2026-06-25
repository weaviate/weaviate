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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func prefixTransformerBuilder(ops []ActiveOp) func([]byte) ([]byte, error) {
	return func(v []byte) ([]byte, error) { return append([]byte("X:"), v...), nil }
}

// denyAllocChecker always reports memory pressure, forcing the cleanup OOM guard.
type denyAllocChecker struct{}

func (denyAllocChecker) CheckAlloc(int64) error                  { return errors.New("no memory") }
func (denyAllocChecker) CheckMappingAndReserve(int64, int) error { return nil }
func (denyAllocChecker) Refresh(bool)                            {}

// newReplaceBucketWithEditOps wires a real cleaner (cleanupInterval > 0) plus an
// edit-ops facility, with a long interval so the time/size heuristic would never
// fire — proving the edit-ops path bypasses it. The facility is owned by the
// segment group and closed on bucket shutdown.
//
// A non-nil builder goes through the production WithTransformerBuilder path. A
// nil builder can't (WithTransformerBuilder opens nothing for a nil func), so the
// helper attaches an edit-ops store with a nil builder directly — exercising the
// nil-transformer guard, which in production is reached via orphan pending rows.
func newReplaceBucketWithEditOps(t *testing.T, builder TransformerBuilder) (*Bucket, *SegmentEditOps) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	opts := []BucketOption{WithStrategy(StrategyReplace), WithSegmentsCleanupInterval(time.Hour)}
	if builder != nil {
		opts = append(opts, WithTransformerBuilder(builder))
	}
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	bucket.SetMemtableThreshold(1e9)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	if builder != nil {
		return bucket, bucket.disk.editOps
	}

	// Assigned onto the segment group so bucket.Shutdown closes it (no extra
	// Cleanup, which would double-close).
	editOps := newSegmentEditOps(bucket.disk.dir, nil)
	bucket.disk.editOps = editOps
	return bucket, editOps
}

func segIDsOf(bucket *Bucket) []string {
	var ids []string
	for _, s := range bucket.disk.segments {
		ids = append(ids, segmentID(s.getPath()))
	}
	return ids
}

// TestSegmentCleanerEditOps_PicksPendingRegardlessOfInterval cleans every pending
// segment through the transformer — including the last segment, which the
// time/size heuristic skips — even though the cleanup interval has not elapsed.
func TestSegmentCleanerEditOps_PicksPendingRegardlessOfInterval(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformerBuilder)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())

	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	require.True(t, cleaned)

	v1, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v1"), v1)
	v2, err := bucket.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v2"), v2)

	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.Empty(t, pending)
}

// TestSegmentCleanerEditOps_MultiOpSameSegment pins the grouping contract: when
// several ops have the same segment pending, it is rewritten exactly once (the
// transformer is applied a single time, not twice) and every op's row for it is
// marked done.
func TestSegmentCleanerEditOps_MultiOpSameSegment(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformerBuilder)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	segs := segIDsOf(bucket)
	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.RegisterOp("op2", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 2}))
	require.NoError(t, editOps.SnapshotSegments("op1", segs))
	require.NoError(t, editOps.SnapshotSegments("op2", segs))

	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	require.True(t, cleaned)

	// Rewritten exactly once: a single transformer application (X:v1, not X:X:v1).
	v1, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v1"), v1)

	// Both ops' rows for the shared segment are marked done.
	p1, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.Empty(t, p1)
	p2, err := editOps.Pending("op2")
	require.NoError(t, err)
	require.Empty(t, p2)
}

// TestSegmentCleanerEditOps_NilTransformerDoesNotMarkDone pins the guard against
// silently completing a drop without stripping: with a nil transformer (no
// builder) but pending rows, cleanup must not rewrite-and-mark-done.
func TestSegmentCleanerEditOps_NilTransformerDoesNotMarkDone(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, nil) // nil builder -> nil transformer

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDsOf(bucket)))

	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	require.False(t, cleaned)

	// The row must stay pending: a nil transformer strips nothing, so marking it
	// done would falsely complete the drop.
	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.NotEmpty(t, pending)
}

// TestSegmentCleanerEditOps_MemoryPressurePausesWithoutBumping pins that an OOM
// guard hit pauses the pass (nothing cleaned) without counting as a failed
// attempt — so a transient low-memory window can't quarantine a healthy segment.
func TestSegmentCleanerEditOps_MemoryPressurePausesWithoutBumping(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformerBuilder)
	bucket.disk.allocChecker = denyAllocChecker{}

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	segID := segIDsOf(bucket)[0]
	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", []string{segID}))

	// More passes than the quarantine budget: if pauses bumped attempts the
	// segment would be quarantined, so this proves a pause is not a failure.
	for range maxCleanupAttempts + 2 {
		cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
		require.NoError(t, err)
		require.False(t, cleaned)
	}

	all, err := editOps.AllPending()
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, 0, all[0].Attempts)

	q, err := editOps.Quarantined()
	require.NoError(t, err)
	require.Empty(t, q)
}

// TestSegmentCleanerEditOps_ENOENTRemovesStaleRow drops a pending row whose
// segment is no longer on disk (merged away by a concurrent compaction).
func TestSegmentCleanerEditOps_ENOENTRemovesStaleRow(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformerBuilder)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", []string{"9999999999999999999"}))

	cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	require.True(t, cleaned)

	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.Empty(t, pending)
}

// TestSegmentCleanerEditOps_QuarantineAfterMaxAttempts quarantines a segment
// whose rewrite keeps failing, so it stops being retried (C6).
func TestSegmentCleanerEditOps_QuarantineAfterMaxAttempts(t *testing.T) {
	failing := func(ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) { return nil, errors.New("boom") }
	}
	bucket, editOps := newReplaceBucketWithEditOps(t, failing)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())

	segID := segIDsOf(bucket)[0]
	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", []string{segID}))

	for range maxCleanupAttempts {
		cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
		require.NoError(t, err)
		require.False(t, cleaned)
	}

	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.Empty(t, pending)

	quarantined, err := editOps.Quarantined()
	require.NoError(t, err)
	require.Len(t, quarantined, 1)
	require.Equal(t, segID, quarantined[0].SegmentID)
}
