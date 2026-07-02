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
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestBucket_EditOps_WiredByClassNameOnReplace proves a non-empty className on a
// replace bucket enables the edit-ops sidecar, that the bolt file is materialized
// lazily on the first registered op (not at init), and that shutdown closes the
// handle (the lsmkv-owned lifecycle).
func TestBucket_EditOps_WiredByClassNameOnReplace(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithClassName("MyClass"))
	require.NoError(t, err)
	require.NotNil(t, bucket.disk.editOps, "className on a replace bucket should wire the edit-ops sidecar")

	editOpsDir := bucket.disk.dir
	// Lazy: no bolt file until an op is registered.
	require.NoFileExists(t, filepath.Join(editOpsDir, segmentEditOpsFileName))
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.FileExists(t, filepath.Join(editOpsDir, segmentEditOpsFileName))

	require.NoError(t, bucket.Shutdown(ctx))

	// Re-opening the (now-existing) file takes an exclusive bolt lock, so this
	// succeeds only if shutdown closed the first handle.
	reopened := newSegmentEditOps(editOpsDir, "")
	_, err = reopened.LoadOps()
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

// TestBucket_EditOps_NotWiredWithoutClassName confirms a replace bucket without a
// className keeps no edit-ops sidecar (only the objects bucket sets className).
func TestBucket_EditOps_NotWiredWithoutClassName(t *testing.T) {
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

// TestBucket_EditOps_NotWiredOnNonReplace confirms the sidecar is gated to the
// replace strategy: even with a className, a non-replace bucket gets no edit-ops
// facility (edit ops only apply to the objects store).
func TestBucket_EditOps_NotWiredOnNonReplace(t *testing.T) {
	for _, strategy := range []string{
		StrategySetCollection,
		StrategyMapCollection,
		StrategyRoaringSet,
		StrategyInverted,
	} {
		t.Run(strategy, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			logger, _ := test.NewNullLogger()

			bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				WithStrategy(strategy), WithClassName("MyClass"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })

			require.Nil(t, bucket.disk.editOps)
		})
	}
}

// TestBucket_RegisterEditOp_SnapshotsAndResumes pins the op-registration entry
// point: it flushes + snapshots the current segments as pending, and a repeat
// call (task resume) is an idempotent no-op rather than re-queueing them.
func TestBucket_RegisterEditOp_SnapshotsAndResumes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithClassName("MyClass"))
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
	require.Len(t, pending, 2, "both segments (flushed + memtable-flushed) must be snapshotted")
	first := append([]string(nil), pending...)

	// Resume: a second call must not re-snapshot / change the pending set.
	require.NoError(t, bucket.RegisterEditOp("op1", desc))
	pending2, err := bucket.EditOpPending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, first, pending2)
}

// TestBucket_RegisterEditOp_CompletesInterruptedSnapshot pins B1: if a prior
// register persisted the op descriptor but not its pending rows (a two-step
// register interrupted between the writes), a resume must still take the snapshot.
// The guard keys off the snapshot, not the descriptor — otherwise EditOpPending
// would read empty, the drain would report "done", and the drop would complete
// without ever stripping the segments present at registration.
func TestBucket_RegisterEditOp_CompletesInterruptedSnapshot(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithClassName("MyClass"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(ctx)) })
	bucket.SetMemtableThreshold(1e9)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))

	// Simulate the interrupted state: descriptor written, no snapshot taken.
	desc := OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1", desc))

	require.NoError(t, bucket.RegisterEditOp("op1", desc))

	pending, err := bucket.EditOpPending("op1")
	require.NoError(t, err)
	require.Len(t, pending, 2, "resume with a descriptor but no snapshot must still snapshot the segments")
}

// TestBucket_RecoverEditOps_OnReopen_ReSnapshotsCurrentSegments proves the startup
// recovery runs through the real newSegmentGroup wiring (not just a direct call):
// after reopening the bucket, a live op whose only snapshot row is stale gets the
// current on-disk segments re-queued and the stale row pruned.
func TestBucket_RecoverEditOps_OnReopen_ReSnapshotsCurrentSegments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	open := func() *Bucket {
		b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyReplace), WithClassName("MyClass"))
		require.NoError(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	bucket := open()
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.Len(t, segs, 2)

	// Crash-window state: op registered, but only a now-absent segment snapshotted.
	editOps := bucket.disk.editOps
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", []string{"9999999999999999999"}))
	require.NoError(t, bucket.Shutdown(ctx))

	reopened := open()
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })

	pending, err := reopened.EditOpPending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, segs, pending,
		"reopen recovery must re-snapshot current segments and drop the stale row")
}

// TestBucket_RecoverEditOps_SweepsOrphanedOpOnReopen pins the multi-node data-loss
// fix: on shard load an op whose task is gone (not in the live-op set) is swept, not
// re-armed. Without it a completed op left on a shard that was unloaded at finalize
// time would re-arm on reactivation and strip a re-created same-name vector.
func TestBucket_RecoverEditOps_SweepsOrphanedOpOnReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	open := func(live EditOpLivenessProvider) *Bucket {
		opts := []BucketOption{WithStrategy(StrategyReplace), WithClassName("MyClass")}
		if live != nil {
			opts = append(opts, WithEditOpLivenessProvider(live))
		}
		b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	bucket := open(nil)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.NoError(t, bucket.disk.editOps.SnapshotSegments("op1", segIDsOf(bucket)))
	require.NoError(t, bucket.Shutdown(ctx))

	// Reopen with a liveness provider reporting NO live ops → op1 is orphaned.
	reopened := open(func(context.Context) (map[string]struct{}, error) {
		return map[string]struct{}{}, nil
	})
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })

	pending, err := reopened.EditOpPending("op1")
	require.NoError(t, err)
	require.Empty(t, pending, "orphaned op must be swept, not re-armed")
	ops, err := reopened.disk.editOps.LoadOps()
	require.NoError(t, err)
	require.Empty(t, ops, "orphaned op descriptor must be removed")
}

// TestBucket_RecoverEditOps_KeepsLiveOpOnReopen is the counterpart: an op whose task
// is still live is re-armed (re-snapshotted), not swept.
func TestBucket_RecoverEditOps_KeepsLiveOpOnReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	open := func(live EditOpLivenessProvider) *Bucket {
		opts := []BucketOption{WithStrategy(StrategyReplace), WithClassName("MyClass")}
		if live != nil {
			opts = append(opts, WithEditOpLivenessProvider(live))
		}
		b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	bucket := open(nil)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.NoError(t, bucket.disk.editOps.SnapshotSegments("op1", segs))
	require.NoError(t, bucket.Shutdown(ctx))

	reopened := open(func(context.Context) (map[string]struct{}, error) {
		return map[string]struct{}{"op1": {}}, nil
	})
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })

	pending, err := reopened.EditOpPending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, segs, pending, "a live op must be re-armed, not swept")
}

// TestBucket_RecoverEditOps_ProviderErrorSkipsSweep pins the data-safety fallback:
// when the liveness lookup errors, recovery must NOT sweep (an "empty set on
// error" refactor would silently drop live ops) — the op is re-armed instead.
func TestBucket_RecoverEditOps_ProviderErrorSkipsSweep(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	open := func(live EditOpLivenessProvider) *Bucket {
		opts := []BucketOption{WithStrategy(StrategyReplace), WithClassName("MyClass")}
		if live != nil {
			opts = append(opts, WithEditOpLivenessProvider(live))
		}
		b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
		require.NoError(t, err)
		b.SetMemtableThreshold(1e9)
		return b
	}

	bucket := open(nil)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.NoError(t, bucket.disk.editOps.SnapshotSegments("op1", segs))
	require.NoError(t, bucket.Shutdown(ctx))

	reopened := open(func(context.Context) (map[string]struct{}, error) {
		return nil, errors.New("dtm not reachable")
	})
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })

	pending, err := reopened.EditOpPending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, segs, pending, "a lookup error must skip the sweep and re-arm the op")
}

// TestSegmentGroup_RecoverEditOps_ReQueuesUnknownSegment pins the startup
// crash-window recovery: a live op that is missing a segment (the merged output
// from a crash between switchOnDisk and RecordCompaction) gets it re-queued, and
// a stale pending row for a now-absent segment is pruned.
func TestSegmentGroup_RecoverEditOps_ReQueuesUnknownSegment(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithClassName("MyClass"))
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

	require.NoError(t, bucket.disk.recoverEditOps(ctx))

	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, segs, pending,
		"recovery re-queues every current segment and prunes the stale row")
}
