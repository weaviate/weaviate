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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/editops"
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

// TestBucket_RegisterEditOp_CompletesInterruptedSnapshot pins: if a prior
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

// reopenableEditOpsBucket opens (or reopens) a replace bucket on dir with the
// class-name-wired edit-ops sidecar — the shared open step of every
// close/reopen edit-ops test. extraOpts append to the base options.
func reopenableEditOpsBucket(t *testing.T, ctx context.Context, dir string, extraOpts ...BucketOption) *Bucket {
	t.Helper()
	logger, _ := test.NewNullLogger()
	opts := append([]BucketOption{WithStrategy(StrategyReplace), WithClassName("MyClass")}, extraOpts...)
	b, err := NewBucketCreator().NewBucket(ctx, dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), opts...)
	require.NoError(t, err)
	b.SetMemtableThreshold(1e9)
	return b
}

// reopenWithLiveness installs the package-level liveness provider (reset on
// cleanup, like the transformers registry) and opens the bucket — the phase
// step of the sweep-behavior tests.
func reopenWithLiveness(t *testing.T, ctx context.Context, dir string, live editops.LivenessProvider) *Bucket {
	t.Helper()
	editops.SetLivenessProvider(live)
	t.Cleanup(func() { editops.SetLivenessProvider(nil) })
	return reopenableEditOpsBucket(t, ctx, dir)
}

// TestBucket_RecoverEditOps_OnReopen_ResumesFromRecordedPending proves the
// startup recovery runs through the real newSegmentGroup wiring (not just a
// direct call): after reopening the bucket, a live op's recorded pending set
// is kept as-is (the stripped segment is not re-pended) and a stale row for
// an absent segment is pruned.
func TestBucket_RecoverEditOps_OnReopen_ResumesFromRecordedPending(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	bucket := reopenableEditOpsBucket(t, ctx, dir)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.Len(t, segs, 2)

	// Interrupted-strip state: one segment already stripped (row removed),
	// one still pending, plus a stale row for an absent segment.
	editOps := bucket.disk.editOps
	require.NoError(t, editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("op1", []string{segs[1], "9999999999999999999"}))
	require.NoError(t, bucket.Shutdown(ctx))

	reopened := reopenableEditOpsBucket(t, ctx, dir)
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })

	pending, err := reopened.EditOpPending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{segs[1]}, pending,
		"recovery keeps the recorded pending set (the resume point): the stripped "+
			"segment must NOT be re-pended and the stale row is pruned")
}

// TestBucket_RecoverEditOps_SweepsOrphanedOpOnReopen pins the multi-node data-loss
// fix: on shard load an op whose task is gone (not in the live-op set) is swept, not
// re-armed. Without it a completed op left on a shard that was unloaded at finalize
// time would re-arm on reactivation and strip a re-created same-name vector.
func TestBucket_RecoverEditOps_SweepsOrphanedOpOnReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	bucket := reopenWithLiveness(t, ctx, dir, nil)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.NoError(t, bucket.disk.editOps.SnapshotSegments("op1", segIDsOf(bucket)))
	require.NoError(t, bucket.Shutdown(ctx))

	// Reopen with a liveness provider reporting NO live ops → op1 is orphaned.
	reopened := reopenWithLiveness(t, ctx, dir, func(context.Context) (map[string]struct{}, error) {
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

	bucket := reopenWithLiveness(t, ctx, dir, nil)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.NoError(t, bucket.disk.editOps.SnapshotSegments("op1", segs))
	require.NoError(t, bucket.Shutdown(ctx))

	reopened := reopenWithLiveness(t, ctx, dir, func(context.Context) (map[string]struct{}, error) {
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

	bucket := reopenWithLiveness(t, ctx, dir, nil)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.NoError(t, bucket.disk.editOps.SnapshotSegments("op1", segs))
	require.NoError(t, bucket.Shutdown(ctx))

	reopened := reopenWithLiveness(t, ctx, dir, func(context.Context) (map[string]struct{}, error) {
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
func TestSegmentGroup_RecoverEditOps_LeavesUnknownSegmentsAlone(t *testing.T) {
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
	// The op knows only the first segment; the second is outside its pending
	// set (either already stripped, or created after the arm — clean by the
	// write-path guards). Plus a stale row for an absent segment.
	require.NoError(t, editOps.SnapshotSegments("op1", []string{segs[0], "9999999999999999999"}))

	require.NoError(t, bucket.disk.recoverEditOps(ctx))

	pending, err := editOps.Pending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{segs[0]}, pending,
		"recovery must not re-pend segments outside the recorded set (they are "+
			"stripped-or-post-arm); only the stale row is pruned")
}

// TestBucket_ListFiles_ExcludesEditOpsSidecar: the sidecar is live-writable bolt
// state (a mid-write stream is a torn copy) and derived (restore reconciliation
// re-creates it), so backups must not include it.
func TestBucket_ListFiles_ExcludesEditOpsSidecar(t *testing.T) {
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
	require.NoError(t, bucket.disk.editOps.RegisterOp("op1",
		OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}))
	require.FileExists(t, filepath.Join(bucket.disk.dir, segmentEditOpsFileName))

	files, err := bucket.ListFiles(ctx, "base")
	require.NoError(t, err)
	for _, f := range files {
		require.NotContains(t, f, segmentEditOpsFileName, "backup file list must exclude the edit-ops sidecar")
	}
	require.NotEmpty(t, files, "segments must still be listed")
}

// TestBucket_EditOpQuarantined_ScopedByOp pins the per-op scoping: op B must not
// see op A's quarantine verdicts (a re-triggered drop gets a fresh op ID and
// must not fail on stale rows from a failed predecessor).
func TestBucket_EditOpQuarantined_ScopedByOp(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.Len(t, segs, 1)

	require.NoError(t, editOps.RegisterOp("opA", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, editOps.SnapshotSegments("opA", segs))
	require.NoError(t, editOps.Quarantine("opA", segs[0]))

	got, err := bucket.EditOpQuarantined("opA")
	require.NoError(t, err)
	require.Equal(t, []string{segs[0]}, got)

	got, err = bucket.EditOpQuarantined("opB")
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestBucket_EditOpQuarantined_RequiresEditOps(t *testing.T) {
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	bucket, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bucket.Shutdown(context.Background())) })

	_, err = bucket.EditOpQuarantined("op1")
	require.ErrorContains(t, err, "edit ops not enabled")
}

// TestBucket_EditOps_FullyDrainedOpStaysDrainedAcrossReopen pins the terminal
// resume state: an op whose every segment was stripped keeps its EMPTY
// pending sub-bucket across a reopen — the sub-bucket's existence IS the
// "already snapshotted" signal, so the next round's re-arm must skip the
// snapshot and its poll reads empty pending as instant completion. Recovery
// dropping the empty sub-bucket (or a row-count HasPendingSnapshot) would
// re-snapshot and re-strip the whole shard instead.
func TestBucket_EditOps_FullyDrainedOpStaysDrainedAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	bucket := reopenableEditOpsBucket(t, ctx, dir)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.Len(t, segs, 1)

	desc := OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}
	require.NoError(t, bucket.RegisterEditOp("op1", desc))
	require.NoError(t, bucket.disk.editOps.MarkSegmentDone("op1", segs[0]))
	require.NoError(t, bucket.Shutdown(ctx))

	reopened := reopenableEditOpsBucket(t, ctx, dir)
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })
	require.NoError(t, reopened.RegisterEditOp("op1", desc))

	pending, err := reopened.EditOpPending("op1")
	require.NoError(t, err)
	require.Empty(t, pending,
		"drained means drained: the empty pending sub-bucket must survive recovery and veto a re-snapshot")
}

// TestBucket_EditOps_MixedVersionOpsCoexist pins the rolling-upgrade shape: a
// uuid-keyed op armed by a pre-upgrade round and the epoch-keyed op of the
// same drop coexist on one bucket. Each keeps its own rows — draining and
// disarming the epoch op leaves the uuid op's resume state untouched (the
// post-marker orphan sweep collects it later), so a mixed-version window
// costs duplicate idempotent strip work, never lost state.
func TestBucket_EditOps_MixedVersionOpsCoexist(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.Len(t, segs, 1)

	const (
		uuidOp  = "0f0c6053-8be1-4c31-8f38-cba5ee9b2e28" // pre-upgrade: one id per round
		epochOp = "epoch-e1"                             // post-upgrade: the drop epoch
	)
	desc := OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}
	require.NoError(t, bucket.RegisterEditOp(uuidOp, desc))
	require.NoError(t, bucket.RegisterEditOp(epochOp, desc))

	require.NoError(t, editOps.MarkSegmentDone(epochOp, segs[0]))
	deleted, _, _, err := bucket.DeleteEditOpIfDrained(epochOp)
	require.NoError(t, err)
	require.True(t, deleted)

	pending, err := bucket.EditOpPending(uuidOp)
	require.NoError(t, err)
	require.Equal(t, segs, pending, "the pre-upgrade op keeps its own pending rows")
	ops, err := editOps.LoadOps()
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.Equal(t, uuidOp, ops[0].ID)
}

// TestBucket_RegisterEditOp_RearmRequeuesQuarantine pins the bucket-level
// wire-through of the fresh-budget rule: re-arming an already-snapshotted op
// (the next round after a FAILED one) returns its quarantined segments to
// pending instead of short-circuiting past them. Without it the drop is
// permanently wedged: the op survives FAILED rounds as the resume point, the
// pending snapshot skips the re-snapshot, and every subsequent round fails
// instantly on the standing quarantine verdict.
func TestBucket_RegisterEditOp_RearmRequeuesQuarantine(t *testing.T) {
	bucket, editOps := newReplaceBucketWithEditOps(t, prefixTransformer)

	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	segs := segIDsOf(bucket)
	require.Len(t, segs, 1)

	desc := OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}
	require.NoError(t, bucket.RegisterEditOp("op1", desc))
	require.NoError(t, editOps.Quarantine("op1", segs[0]))

	// Round N failed on the quarantine; round N+1 re-arms the same op.
	require.NoError(t, bucket.RegisterEditOp("op1", desc))

	q, err := bucket.EditOpQuarantined("op1")
	require.NoError(t, err)
	require.Empty(t, q, "the new round must start with a clean quarantine slate")
	pending, err := bucket.EditOpPending("op1")
	require.NoError(t, err)
	require.Equal(t, segs, pending, "the quarantined segment is pending again, awaiting its fresh retries")
}

// TestBucket_EditOps_ResumeSkipsStrippedSegments_RealCleaner proves the resume
// property with the production machinery end to end: the real cleanup pass
// strips one of two segments, the bucket is closed and reopened (load-time
// recovery runs), the idempotent re-arm finds the recorded pending set, and
// the real cleaner drains only the remainder. The transformer is deliberately
// non-idempotent (prefixes "X:"), so a restart that re-stripped the
// already-done segment would show up as a double prefix — the assertion is on
// work SKIPPED, not just on eventual convergence.
func TestBucket_EditOps_ResumeSkipsStrippedSegments_RealCleaner(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	transformers := map[OpType]OpTransformerFactory{OpTypeRemoveTargetVectors: prefixTransformer}

	open := func() *Bucket {
		b := reopenableEditOpsBucket(t, ctx, dir, WithSegmentsCleanupInterval(time.Hour))
		// Swap in the fake-transformer resolver over the SAME bolt state — the
		// production resolver would try to decode the plain test values as
		// storobj. Load-time recovery already ran above, on the real instance.
		require.NoError(t, b.disk.editOps.Close())
		b.disk.editOps = newSegmentEditOpsWithLookup(b.disk.dir, "MyClass", staticResolver(transformers))
		return b
	}

	bucket := open()
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.NoError(t, bucket.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, bucket.FlushAndSwitch())
	require.Len(t, segIDsOf(bucket), 2)

	desc := OpDescriptor{Type: OpTypeRemoveTargetVectors, Targets: []string{"foo"}, CreatedAt: 1}
	require.NoError(t, bucket.RegisterEditOp("op1", desc))

	// One real cleanup pass rewrites exactly one segment, then the process
	// "dies" (deactivation → clean shutdown).
	_, err := bucket.disk.segmentCleaner.cleanupOnce(func() bool { return false })
	require.NoError(t, err)
	pending, err := bucket.EditOpPending("op1")
	require.NoError(t, err)
	require.Len(t, pending, 1, "one segment stripped, one still owed")
	remaining := pending[0]
	require.NoError(t, bucket.Shutdown(ctx))

	reopened := open()
	t.Cleanup(func() { require.NoError(t, reopened.Shutdown(ctx)) })
	require.NoError(t, reopened.RegisterEditOp("op1", desc))
	pending, err = reopened.EditOpPending("op1")
	require.NoError(t, err)
	require.Equal(t, []string{remaining}, pending,
		"the resumed op must owe exactly what the interruption left unstripped")
	drainEditOpsCleanup(t, reopened)

	v1, err := reopened.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v1"), v1, "each value must be stripped exactly once across the restart")
	v2, err := reopened.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("X:v2"), v2, "each value must be stripped exactly once across the restart")

	pending, err = reopened.EditOpPending("op1")
	require.NoError(t, err)
	require.Empty(t, pending)
}
