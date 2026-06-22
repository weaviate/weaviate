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
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestSegmentGroup_RecordCompactionInEditOps pins the completion bookkeeping
// with controlled timestamps: an op that predates the pass has its merged inputs
// marked done and nothing re-queued (the compactor already stripped for it);
// an op registered after the pass began also has the inputs marked done but the
// merged output re-queued (the transformer the compactor ran with predated it).
func TestSegmentGroup_RecordCompactionInEditOps(t *testing.T) {
	editOps, err := OpenSegmentEditOps(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })

	require.NoError(t, editOps.RegisterOp("old", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 100}))
	require.NoError(t, editOps.RegisterOp("new", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 300}))
	require.NoError(t, editOps.SnapshotSegments("old", []string{"100", "200"}))
	require.NoError(t, editOps.SnapshotSegments("new", []string{"100", "200"}))

	sg := &SegmentGroup{editOps: editOps}
	startedAt := int64(200) // between the two ops' CreatedAt

	require.NoError(t, sg.recordCompactionInEditOps(
		filepath.Join("dir", "segment-100.db"),
		filepath.Join("dir", "segment-200.db"),
		startedAt))

	// old op (registered before the pass): inputs done, merged NOT re-queued.
	pOld, err := editOps.Pending("old")
	require.NoError(t, err)
	assert.Empty(t, pOld)

	// new op (registered mid-pass): inputs done, merged re-queued for re-clean.
	pNew, err := editOps.Pending("new")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"100_200"}, pNew)
}

// TestSegmentGroup_RecordCompactionInEditOps_NoReQueueWhenInputNotPending checks
// the mid-pass op is only re-queued when one of the merged inputs was actually
// pending for it.
func TestSegmentGroup_RecordCompactionInEditOps_NoReQueueWhenInputNotPending(t *testing.T) {
	editOps, err := OpenSegmentEditOps(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })

	require.NoError(t, editOps.RegisterOp("new", OpDescriptor{Type: "remove_target_vectors", CreatedAt: 300}))
	// Pending for some unrelated segment, not the ones being merged.
	require.NoError(t, editOps.SnapshotSegments("new", []string{"999"}))

	sg := &SegmentGroup{editOps: editOps}
	require.NoError(t, sg.recordCompactionInEditOps(
		filepath.Join("dir", "segment-100.db"),
		filepath.Join("dir", "segment-200.db"),
		int64(200)))

	pNew, err := editOps.Pending("new")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"999"}, pNew) // unchanged; merged not added
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

	editOps, err := OpenSegmentEditOps(bucket.disk.dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, editOps.Close()) })
	bucket.disk.editOps = editOps
	bucket.disk.transformerBuilder = func(ops []ActiveOp) valueTransformer {
		require.NotEmpty(t, ops)
		return func(v []byte) ([]byte, error) { return append([]byte("X:"), v...), nil }
	}

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
