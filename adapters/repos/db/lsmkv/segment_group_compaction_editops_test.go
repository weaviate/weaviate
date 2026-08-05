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
	editOps := newSegmentEditOps(t.TempDir(), "")
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

	// late op (absent from the transformer set): inputs done, merged re-queued
	// under the RIGHT input's ID — the name the renamed output carries on disk.
	pLate, err := editOps.Pending("late")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"200"}, pLate)
}

// TestSegmentEditOps_RecordCompaction_NoReQueueWhenInputNotPending checks an op
// absent from builtOps is only re-queued when one of the merged inputs was
// actually pending for it.
func TestSegmentEditOps_RecordCompaction_NoReQueueWhenInputNotPending(t *testing.T) {
	editOps := newSegmentEditOps(t.TempDir(), "")
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })

	require.NoError(t, editOps.RegisterOp("late", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 300}))
	// Pending for some unrelated segment, not the ones being merged.
	require.NoError(t, editOps.SnapshotSegments("late", []string{"999"}))

	require.NoError(t, editOps.RecordCompaction("100", "200", nil))

	pLate, err := editOps.Pending("late")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"999"}, pLate) // unchanged; merged not added
}

// TestSegmentEditOps_RecordCompaction_ClearsQuarantineOfMergedInputs pins the
// quarantine bookkeeping of a merge: the inputs no longer exist, so their
// quarantine rows must go in the same transaction — a stale row would fail
// every later round until a re-arm dropped it. For an op IN the transformer
// set the merge itself stripped the output, so nothing is re-queued; for an
// op ABSENT from it a quarantined input counts like a pending one — the
// merge rewrote that data into a new file the verdict knows nothing about,
// so the output is re-queued as ordinary pending with a fresh retry budget.
func TestSegmentEditOps_RecordCompaction_ClearsQuarantineOfMergedInputs(t *testing.T) {
	editOps := newSegmentEditOps(t.TempDir(), "")
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })

	built := OpDescriptor{Type: "remove_target_vectors", CreatedAt: 100}
	require.NoError(t, editOps.RegisterOp("built", built))
	require.NoError(t, editOps.RegisterOp("late", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 300}))
	require.NoError(t, editOps.SnapshotSegments("built", []string{"100", "200"}))
	require.NoError(t, editOps.SnapshotSegments("late", []string{"100", "200"}))
	require.NoError(t, editOps.Quarantine("built", "100"))
	require.NoError(t, editOps.Quarantine("late", "100"))
	require.NoError(t, editOps.MarkSegmentDone("late", "200")) // only the quarantined input remains

	require.NoError(t, editOps.RecordCompaction("100", "200", []ActiveOp{{ID: "built", Descriptor: built}}))

	q, err := editOps.Quarantined()
	require.NoError(t, err)
	assert.Empty(t, q, "no quarantine row may outlive its merged-away segment")

	pBuilt, err := editOps.Pending("built")
	require.NoError(t, err)
	assert.Empty(t, pBuilt, "the merge stripped the output for the built op")

	pLate, err := editOps.Pending("late")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"200"}, pLate,
		"a quarantined input of a late op still forces the re-queue — clearing its row without one would under-strip")
}

// TestSegmentEditOps_RecordCompaction_ReQueuesLateOpWithEarlyCreatedAt pins the
// fix for the clock-mismatch race: an op that registered after the transformer
// was built (so it is absent from builtOps) must be re-queued even when its
// caller-supplied CreatedAt predates the merge. The merged output was never
// stripped for it, so dropping the re-queue would silently retain its target.
// The earlier, timestamp-based gate (CreatedAt > startedAt) would wrongly skip
// this op; membership-based bookkeeping does not.
func TestSegmentEditOps_RecordCompaction_ReQueuesLateOpWithEarlyCreatedAt(t *testing.T) {
	editOps := newSegmentEditOps(t.TempDir(), "")
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })

	// CreatedAt=1 is far in the past (e.g. clock skew, or a logical timestamp
	// stamped at op creation while RegisterOp committed only later), yet the op
	// is absent from builtOps because the transformer was already built.
	require.NoError(t, editOps.RegisterOp("late", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("late", []string{"100", "200"}))

	require.NoError(t, editOps.RecordCompaction("100", "200", nil /* not in build set */))

	pLate, err := editOps.Pending("late")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"200"}, pLate)
}

// TestSegmentGroup_CompactionReQueueNamesLiveSegment pins the re-queue against
// the real on-disk naming: the merged output is renamed to the RIGHT input's ID
// (stripTmpExtension), so the pending row RecordCompaction re-queues for a
// late op must name that ID. A row under any other name never matches a live
// segment again — the drain stalls on it until a restart, whose load-time
// prune then drops it and the drop completes without stripping the merged
// output's data.
func TestSegmentGroup_CompactionReQueueNamesLiveSegment(t *testing.T) {
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

	// No factory for the op type: BuildCurrentTransformer excludes the op from
	// builtOps, the same state as an op armed after the pass built its
	// transformer — the merge does not strip for it, so RecordCompaction must
	// keep its data pending.
	editOps := newSegmentEditOpsWithLookup(bucket.disk.dir, "TestClass",
		staticResolver(map[OpType]OpTransformerFactory{}))
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })
	bucket.disk.editOps = editOps

	var segIDs []string
	for _, s := range bucket.disk.segments {
		segIDs = append(segIDs, segmentID(s.getPath()))
	}
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: time.Now().UnixNano()}))
	require.NoError(t, editOps.SnapshotSegments("op1", segIDs))

	compacted, err := bucket.disk.compactOnce(ctx)
	require.NoError(t, err)
	require.True(t, compacted)

	live := map[string]struct{}{}
	for _, s := range bucket.disk.segments {
		live[segmentID(s.getPath())] = struct{}{}
	}

	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.NotEmpty(t, pending, "the merged output was not stripped for op1; it must stay pending")
	for _, segID := range pending {
		require.Contains(t, live, segID,
			"re-queued pending row must name a live segment (the merged output keeps the right input's ID)")
	}
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

	editOps := newSegmentEditOpsWithLookup(bucket.disk.dir, "TestClass", staticResolver(map[OpType]OpTransformerFactory{
		OpTypeRemoveTargetVectors: func(className string, ops []ActiveOp) func([]byte) ([]byte, error) {
			require.NotEmpty(t, ops)
			return func(v []byte) ([]byte, error) { return append([]byte("X:"), v...), nil }
		},
	}))
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
