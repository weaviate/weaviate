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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucket_WithTransformerBuilder_OpensAndClosesEditOps proves the option wires
// the edit-ops sidecar at bucket init, that the bolt file is materialized lazily
// on the first registered op (not at init), and that shutdown closes the handle
// (the lsmkv-owned C9 lifecycle).
func TestBucket_WithTransformerBuilder_OpensAndClosesEditOps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	builder := func(ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) { return v, nil }
	}

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithTransformerBuilder(builder))
	require.NoError(t, err)
	require.NotNil(t, bucket.disk.editOps, "WithTransformerBuilder should wire the edit-ops sidecar")

	editOpsDir := bucket.disk.dir
	// Lazy: no bolt file until an op is registered.
	require.NoFileExists(t, filepath.Join(editOpsDir, segmentEditOpsFileName))
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.FileExists(t, filepath.Join(editOpsDir, segmentEditOpsFileName))

	require.NoError(t, bucket.Shutdown(ctx))

	// Re-opening the (now-existing) file takes an exclusive bolt lock, so this
	// succeeds only if shutdown closed the first handle.
	reopened := newSegmentEditOps(editOpsDir, nil)
	_, err = reopened.LoadOps()
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

// TestWithTransformerBuilder_RejectsNonReplace pins the fail-fast replace-only
// guard: wiring the option on a non-replace bucket errors at setup.
func TestWithTransformerBuilder_RejectsNonReplace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	builder := func(ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) { return v, nil }
	}

	_, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategySetCollection), WithTransformerBuilder(builder))
	require.Error(t, err)
	require.Contains(t, err.Error(), "replace")
}

// TestBucket_NoTransformerBuilder_NoEditOps confirms a bucket without the option
// keeps no edit-ops sidecar (no per-bucket bolt file when the feature is unused).
func TestBucket_NoTransformerBuilder_NoEditOps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

	require.Nil(t, bucket.disk.editOps)
}

// TestBucket_RegisterEditOp_SnapshotsAndResumes pins the op-registration entry
// point: it flushes + snapshots the current segments as pending, and a repeat
// call (task resume) is an idempotent no-op rather than re-queueing them.
func TestBucket_RegisterEditOp_SnapshotsAndResumes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	builder := func(ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) { return v, nil }
	}
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithTransformerBuilder(builder))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })
	bucket.SetMemtableThreshold(1e9)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))

	// RegisterEditOp flushes the active memtable then snapshots every segment.
	desc := OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}
	require.NoError(t, bucket.RegisterEditOp("op1", desc))

	pending, err := bucket.EditOpPending("op1")
	require.NoError(t, err)
	require.NotEmpty(t, pending, "current segments must be snapshotted as pending")
	first := append([]string(nil), pending...)

	// Resume: a second call must not re-snapshot / change the pending set.
	require.NoError(t, bucket.RegisterEditOp("op1", desc))
	pending2, err := bucket.EditOpPending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, first, pending2)
}

// TestSegmentGroup_RecoverEditOps_ReQueuesUnknownSegment pins the startup
// crash-window recovery: a live op that is missing a segment (the merged output
// from a crash between switchOnDisk and RecordCompaction) gets it re-queued, and
// a stale pending row for a now-absent segment is pruned.
func TestSegmentGroup_RecoverEditOps_ReQueuesUnknownSegment(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	builder := func(ops []ActiveOp) func([]byte) ([]byte, error) {
		return func(v []byte) ([]byte, error) { return v, nil }
	}
	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithTransformerBuilder(builder))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })
	bucket.SetMemtableThreshold(1e9)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())

	segs := segIDsOf(bucket)
	require.Len(t, segs, 2)

	editOps := bucket.disk.editOps
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	// Crash-window state: op knows only the first segment; the second is a merged
	// output it was never told about. Plus a stale row for an absent segment.
	require.NoError(t, editOps.SnapshotSegments("op1", []string{segs[0], "9999999999999999999"}))

	require.NoError(t, bucket.disk.recoverEditOps())

	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, segs, pending,
		"recovery re-queues every current segment and prunes the stale row")
}
