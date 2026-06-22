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
