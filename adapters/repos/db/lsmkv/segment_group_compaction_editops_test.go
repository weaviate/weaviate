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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestSegmentEditOps_RecordCompaction pins the completion bookkeeping by op-set
// membership: an op the pass's transformer was built from (in builtOps) has its
// merged inputs marked done and nothing re-queued (the merge already stripped
// it); an op absent from builtOps — registered after the transformer was built —
// has its inputs marked done but the merged output re-queued (the merge ran with
// a transformer that never saw it).
func TestSegmentEditOps_RecordCompaction(t *testing.T) {
	editOps, err := openSegmentEditOps(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })

	built := OpDescriptor{Type: "remove_target_vectors", CreatedAt: 100}
	require.NoError(t, editOps.RegisterOp("built", built))
	require.NoError(t, editOps.RegisterOp("late", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 300}))
	require.NoError(t, editOps.SnapshotSegments("built", []string{"100", "200"}))
	require.NoError(t, editOps.SnapshotSegments("late", []string{"100", "200"}))

	// The transformer was built from "built" only; "late" landed afterwards.
	builtOps := []ActiveOp{{ID: "built", Descriptor: built}}

	require.NoError(t, editOps.RecordCompaction("100", "200", builtOps))

	// built op (in the transformer set): inputs done, merged NOT re-queued.
	pBuilt, err := editOps.Pending("built")
	require.NoError(t, err)
	assert.Empty(t, pBuilt)

	// late op (absent from the transformer set): inputs done, merged re-queued.
	pLate, err := editOps.Pending("late")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"100_200"}, pLate)
}

// TestSegmentEditOps_RecordCompaction_NoReQueueWhenInputNotPending checks an op
// absent from builtOps is only re-queued when one of the merged inputs was
// actually pending for it.
func TestSegmentEditOps_RecordCompaction_NoReQueueWhenInputNotPending(t *testing.T) {
	editOps, err := openSegmentEditOps(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })

	require.NoError(t, editOps.RegisterOp("late", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 300}))
	// Pending for some unrelated segment, not the ones being merged.
	require.NoError(t, editOps.SnapshotSegments("late", []string{"999"}))

	require.NoError(t, editOps.RecordCompaction("100", "200", nil))

	pLate, err := editOps.Pending("late")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"999"}, pLate) // unchanged; merged not added
}

// TestSegmentEditOps_RecordCompaction_ReQueuesLateOpWithEarlyCreatedAt pins the
// fix for the clock-mismatch race: an op that registered after the transformer
// was built (so it is absent from builtOps) must be re-queued even when its
// caller-supplied CreatedAt predates the merge. The merged output was never
// stripped for it, so dropping the re-queue would silently retain its target.
// The earlier, timestamp-based gate (CreatedAt > startedAt) would wrongly skip
// this op; membership-based bookkeeping does not.
func TestSegmentEditOps_RecordCompaction_ReQueuesLateOpWithEarlyCreatedAt(t *testing.T) {
	editOps, err := openSegmentEditOps(t.TempDir(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })

	// CreatedAt=1 is far in the past (e.g. clock skew, or a logical timestamp
	// stamped at op creation while RegisterOp committed only later), yet the op
	// is absent from builtOps because the transformer was already built.
	require.NoError(t, editOps.RegisterOp("late", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("late", []string{"100", "200"}))

	require.NoError(t, editOps.RecordCompaction("100", "200", nil /* not in build set */))

	pLate, err := editOps.Pending("late")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"100_200"}, pLate)
}

// TestSegmentGroup_CompactionAppliesEditOpsTransformer exercises the full
// replace-compaction path with edit ops wired: the per-pass transformer is
// built from the active ops and applied to merged values, and on completion the
// merged inputs are marked done.
func TestSegmentGroup_CompactionAppliesEditOpsTransformer(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithForceCompaction(true))
	require.NoError(t, err)
	bucket.SetMemtableThreshold(1e9)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())

	editOps, err := openSegmentEditOps(bucket.disk.dir, func(ops []ActiveOp) func([]byte) ([]byte, error) {
		require.NotEmpty(t, ops)
		return func(v []byte) ([]byte, error) { return append([]byte("X:"), v...), nil }
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })
	bucket.disk.editOps = editOps

	var segIDs []string
	for _, s := range bucket.disk.segments {
		segIDs = append(segIDs, segmentID(s.getPath()))
	}
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: "remove_target_vectors", CreatedAt: time.Now().UnixNano()}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDs))

	compacted, err := bucket.disk.compactOnce(ctx)
	require.NoError(t, err)
	require.True(t, compacted)

	v1, err := bucket.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v1"), v1)
	v2, err := bucket.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v2"), v2)

	// Inputs were marked done; the op predated the pass, so nothing re-queued.
	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.Empty(t, pending)
}
