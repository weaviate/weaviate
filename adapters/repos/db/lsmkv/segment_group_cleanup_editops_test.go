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

func prefixTransformerBuilder(ops []ActiveOp) valueTransformer {
	return func(v []byte) ([]byte, error) { return append([]byte("X:"), v...), nil }
}

// denyAllocChecker always reports memory pressure, forcing the cleanup OOM guard.
type denyAllocChecker struct{}

func (denyAllocChecker) CheckAlloc(int64) error                  { return errors.New("no memory") }
func (denyAllocChecker) CheckMappingAndReserve(int64, int) error { return nil }
func (denyAllocChecker) Refresh(bool)                            {}

// newReplaceBucketWithEditOps wires a real cleaner (cleanupInterval > 0) plus an
// edit-ops facility, with a long interval so the time/size heuristic would never
// fire — proving the edit-ops path bypasses it.
func newReplaceBucketWithEditOps(t *testing.T, builder transformerBuilder) (*Bucket, *SegmentEditOps) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSegmentsCleanupInterval(time.Hour))
	require.NoError(t, err)
	bucket.SetMemtableThreshold(1e9)

	editOps, err := openSegmentEditOps(bucket.disk.dir, builder)
	require.NoError(t, err)
	bucket.disk.editOps = editOps

	t.Cleanup(func() {
		require.NoError(t, editOps.Close())
		require.NoError(t, bucket.Shutdown(ctx))
	})
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

	// Cleanup processes one segment per pass; drain.
	drainEditOpsCleanup(t, bucket)

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

// drainEditOpsCleanup runs the cleanup cycle until the edit-ops pending set is
// empty (one segment is rewritten per pass). It stops on empty rather than on a
// no-work return so it doesn't fall through to the heuristic cleanup path (which
// would re-apply the non-idempotent test transformer).
func drainEditOpsCleanup(t *testing.T, bucket *Bucket) {
	t.Helper()
	for range 20 {
		pending, err := bucket.disk.editOps.AllPending()
		require.NoError(t, err)
		if len(pending) == 0 {
			return
		}
		_, err = bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
		require.NoError(t, err)
	}
	t.Fatal("edit-ops cleanup did not drain within 20 passes")
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

// TestSegmentCleanerEditOps_AbortMidRewriteDoesNotBump pins that a shouldAbort
// firing mid-rewrite (e.g. shutdown, or a frequently-firing abort under load)
// pauses the pass WITHOUT counting as a failed attempt — an interrupted rewrite
// must not push a healthy segment toward quarantine. Mirrors the OOM-pause test.
func TestSegmentCleanerEditOps_AbortMidRewriteDoesNotBump(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformerBuilder)

	// >=100 keys so writeKeys reaches its i%100==0 shouldAbort check mid-rewrite
	// (a smaller segment would finish before the check ever fires).
	for i := range 150 {
		require.NoError(t, bucket.Put([]byte{byte(i)}, []byte("v")))
	}
	require.NoError(t, bucket.FlushAndSwitch())

	segID := segIDsOf(bucket)[0]
	require.NoError(t, editOps.RegisterOp("op1", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", []string{segID}))

	// More passes than the quarantine budget: if aborts bumped attempts the
	// segment would be quarantined, so this proves an abort is not a failure.
	for range maxCleanupAttempts + 2 {
		// false on the first call (cleanupOnceEditOps' entry guard) so the pass
		// proceeds into the rewrite, then true — firing inside writeKeys.
		calls := 0
		shouldAbort := func() bool { calls++; return calls > 1 }
		cleaned, err := bucket.disk.segmentCleaner.cleanupOnce(shouldAbort)
		require.NoError(t, err)
		require.False(t, cleaned)
	}

	all, err := editOps.AllPending()
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, 0, all[0].Attempts, "an aborted rewrite must not count as a failed attempt")

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
	failing := func(ops []ActiveOp) valueTransformer {
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
