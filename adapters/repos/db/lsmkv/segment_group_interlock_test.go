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
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

func newInterlockTestBucket(t *testing.T, dir string) (*Bucket, error) {
	t.Helper()
	logger, _ := test.NewNullLogger()
	return NewBucketCreator().NewBucket(context.Background(), dir, dir, logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSegmentsCleanupInterval(time.Hour),
		WithClassName("TestClass"))
}

// TestRegisterEditOp_RefusedOnClosingBucket pins the arm-time interlock: once
// a bucket's shutdown began, registering a NEW edit op must hard-fail — the
// flush + segment snapshot would race the dismantling and could record
// "nothing to clean" for a bucket full of data.
func TestRegisterEditOp_RefusedOnClosingBucket(t *testing.T) {
	bucket, _ := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))

	bucket.shuttingDown.Store(true)
	err := bucket.RegisterEditOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "shutting down")
}

// TestWALRecovery_FlushesLastWALUnderLiveOps pins the recovery contract:
// with ops in the sidecar, EVERY recovered WAL — including the one that
// would otherwise seed the live memtable — is flushed into segments, and
// those segments are PENDED for every surviving op. The re-pend is
// load-bearing: a WAL on disk under a live op is not necessarily post-arm —
// an older binary's b.flushing clobber (since fixed) could orphan a failed
// flush's memtable, leaving pre-arm bytes in no segment and no pending row;
// its WALs survive an upgrade. Recovery would flush them into a segment that
// reads as clean, and the dropped vector would survive finalize. For a
// genuinely post-arm WAL the re-pend costs one idempotent re-clean of one
// small segment. The op here is registered through a side-channel handle to
// keep the WAL alive across the close — the same "WAL bytes the arm's flush
// never saw" shape.
func TestWALRecovery_FlushesLastWALUnderLiveOps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	b1, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	// Small enough for Shutdown's flushWAL path, which KEEPS the WAL on disk —
	// the exact shape an interrupted close leaves behind.
	require.NoError(t, b1.Put([]byte("k1"), []byte("v1")))

	ext := newSegmentEditOps(dir, "TestClass")
	require.NoError(t, ext.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, ext.Close())
	require.NoError(t, b1.Shutdown(ctx))

	b2, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, b2.Shutdown(ctx)) }()

	// The flush must have happened: no data may sit in the live memtable
	// outside compaction's reach while an op is armed.
	segs := segIDsOf(b2)
	require.NotEmpty(t, segs, "recovered WAL bytes must land in a segment")

	require.NotNil(t, b2.disk.editOps)
	pending, err := b2.disk.editOps.Pending("op1")
	require.NoError(t, err)
	require.ElementsMatch(t, segs, pending,
		"WAL-recovered segments may hold pre-arm bytes no snapshot covered; they must be pended for the surviving op")

	v, err := b2.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v, "recovered data must still be readable after the flush")
}

// TestNewBucket_FailsWhenSidecarLocked pins the reload fence: a sidecar
// still flocked by a previous instance means that instance has not finished
// closing, and loading blind would cost a completed drop its healing
// re-pend. The load must fail retryably instead.
func TestNewBucket_FailsWhenSidecarLocked(t *testing.T) {
	dir := t.TempDir()

	ext := newSegmentEditOps(dir, "TestClass")
	require.NoError(t, ext.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	defer ext.Close() // holds the bolt flock for the duration of the test

	_, err := newInterlockTestBucket(t, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "still locked by a previous instance")

	// The failed load must not itself leak a handle: once the previous
	// instance releases the lock, a retry succeeds (a leak would wedge every
	// reload until process restart).
	require.NoError(t, ext.Close())
	b, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err, "retry after the lock is released must succeed")
	require.NoError(t, b.Shutdown(context.Background()))
}

// TestSegmentEditOps_SidecarRemovedWithLastOp pins that deleting the last op
// deletes the sidecar file: a one-time drop must not leave a permanent
// fd + mmap on every tenant shard (openIfExists would reopen it on every
// shard load and cleanup pass, forever). A later op re-creates it.
func TestSegmentEditOps_SidecarRemovedWithLastOp(t *testing.T) {
	dir := t.TempDir()
	ops := newSegmentEditOps(dir, "TestClass")
	sidecar := filepath.Join(dir, segmentEditOpsFileName)

	require.NoError(t, ops.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, ops.RegisterOp("op2", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 2}))
	_, err := os.Stat(sidecar)
	require.NoError(t, err, "sidecar exists while ops exist")

	require.NoError(t, ops.DeleteOp("op1"))
	_, err = os.Stat(sidecar)
	require.NoError(t, err, "sidecar stays while another op remains")

	require.NoError(t, ops.DeleteOp("op2"))
	_, err = os.Stat(sidecar)
	require.ErrorIs(t, err, os.ErrNotExist, "last op deleted => sidecar file removed")

	pending, err := ops.Pending("op1")
	require.NoError(t, err)
	require.Empty(t, pending, "reads on a removed sidecar are clean no-ops")

	require.NoError(t, ops.RegisterOp("op3", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 3}))
	_, err = os.Stat(sidecar)
	require.NoError(t, err, "a later op re-creates the sidecar")
	require.NoError(t, ops.Close())
}

// TestRecover_SweepOfLastOpRemovesSidecar pins that the load-time orphan
// sweep (which deletes ops via Reconcile's own transaction, not DeleteOp)
// still ends with the empty sidecar file removed — otherwise the fd/mmap
// leak DeleteOp's cleanup targets survives via the more common path.
func TestRecover_SweepOfLastOpRemovesSidecar(t *testing.T) {
	dir := t.TempDir()
	ops := newSegmentEditOps(dir, "TestClass")
	sidecar := filepath.Join(dir, segmentEditOpsFileName)

	require.NoError(t, ops.RegisterOp("orphan", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	_, err := os.Stat(sidecar)
	require.NoError(t, err)

	// No live task for the op: the sweep removes it during Recover.
	noneLive := func() map[string]struct{} { return map[string]struct{}{} }
	require.NoError(t, ops.Recover(nil, noneLive, noneLive))

	_, err = os.Stat(sidecar)
	require.ErrorIs(t, err, os.ErrNotExist, "sweeping the last op must also remove the sidecar file")
}

// TestRegisterEditOp_RefusedOnReadOnlyBucket pins the read-only guard:
// RegisterEditOp bypasses FlushAndSwitch's public entry (lock-ordering), so
// it must re-check readOnlyErr itself — arming flushes the memtable, the
// exact write StatusReadOnly (disk pressure, backup) exists to block.
func TestRegisterEditOp_RefusedOnReadOnlyBucket(t *testing.T) {
	bucket, _ := newReplaceBucketWithEditOps(t, prefixTransformer)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))

	bucket.UpdateStatus(storagestate.StatusReadOnly)
	err := bucket.RegisterEditOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1})
	require.Error(t, err, "arming on a read-only bucket must be refused")

	bucket.UpdateStatus(storagestate.StatusReady)
	require.NoError(t, bucket.RegisterEditOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
}

// TestBucketReinit_RefusedWhileLeakedOpenThenHealsAfterClose pins the
// reactivation contract behind a deep shard-teardown failure: a bucket the
// failed teardown left open keeps its GlobalBucketRegistry entry, so a
// tenant re-init over the same directory is refused BEFORE any file is
// touched (a later check would let the doomed re-open hang on leaked flocks
// or mutate the live instance's WAL — see the claim in NewBucket), and
// succeeds once the leaked handle is finally shut down (in practice:
// process restart).
func TestBucketReinit_RefusedWhileLeakedOpenThenHealsAfterClose(t *testing.T) {
	dir := t.TempDir()

	leaked, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	require.NoError(t, leaked.Put([]byte("k1"), []byte("v1")))

	_, err = newInterlockTestBucket(t, dir)
	require.ErrorIs(t, err, ErrBucketAlreadyRegistered,
		"re-init over a leaked open bucket must be refused before touching any file")

	require.NoError(t, leaked.Shutdown(context.Background()))

	reinit, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err, "once the leak clears, the tenant must be re-initializable")
	v, err := reinit.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)
	require.NoError(t, reinit.Shutdown(context.Background()))
}

// TestBucketShutdown_KeepsRegistryClaimOnFailure pins the release side of the
// claim-before-touch contract: a FAILED teardown may leave open handles, so
// it must keep the registry claim (re-open refused up front), and only a
// completed teardown frees the slot.
func TestBucketShutdown_KeepsRegistryClaimOnFailure(t *testing.T) {
	dir := t.TempDir()
	bucket, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, bucket.Shutdown(cancelled), "teardown with a dead ctx must fail")

	_, err = newInterlockTestBucket(t, dir)
	require.ErrorIs(t, err, ErrBucketAlreadyRegistered,
		"a failed teardown must keep the claim; freeing it lets a re-open race the leaked handles")

	require.NoError(t, bucket.Shutdown(context.Background()))
	reopened, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err, "a completed teardown frees the slot")
	require.NoError(t, reopened.Shutdown(context.Background()))
}

// TestNewBucket_LateInitFailureTearsDownDiskBeforeReleasingClaim pins the
// failed-late-init contract: a failure AFTER newSegmentGroup succeeded must
// tear down the constructed segment group (mmapped segments, flocked bolt
// sidecars) before releasing the registry claim. Releasing over live handles
// would let a retry double-open the directory — a leaked sidecar flock makes
// the retry below time out, a leaked claim makes it refuse.
func TestNewBucket_LateInitFailureTearsDownDiskBeforeReleasingClaim(t *testing.T) {
	dir := t.TempDir()
	newBucketPostDiskInitHook = func(*Bucket) error { return errors.New("post-disk-init fault") }
	defer func() { newBucketPostDiskInitHook = nil }()

	_, err := newInterlockTestBucket(t, dir)
	require.ErrorContains(t, err, "post-disk-init fault")

	newBucketPostDiskInitHook = nil
	bucket, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err, "retry must find no leaked claim and no leaked flocks")
	require.NoError(t, bucket.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, bucket.Shutdown(context.Background()))
}

// TestNewBucket_LockedSidecarRetriesDoNotLeakSegmentMmaps pins the failure
// path with EXISTING segments: when the load fails on the still-locked
// sidecar, the segments already mmapped for this attempt must be closed —
// the shard lifecycle retries this load, and a per-retry leak of every
// segment mapping wedges the node long before the old instance releases the
// lock. Observed through the segment-total gauge, which the close path
// decrements symmetrically with the load path's increment.
func TestNewBucket_LockedSidecarRetriesDoNotLeakSegmentMmaps(t *testing.T) {
	dir := t.TempDir()
	seeded, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	require.NoError(t, seeded.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, seeded.FlushAndSwitch())
	require.NoError(t, seeded.Shutdown(context.Background()))

	ext := newSegmentEditOps(dir, "TestClass")
	require.NoError(t, ext.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	defer ext.Close() // holds the bolt flock for the duration of the test

	metrics, err := NewMetrics(monitoring.GetMetrics(), "TestClass", "shard")
	require.NoError(t, err)
	gauge, err := metrics.segmentTotalByStrategy.GetMetricWithLabelValues(StrategyReplace)
	require.NoError(t, err)
	before := testutil.ToFloat64(gauge)

	logger, _ := test.NewNullLogger()
	_, err = NewBucketCreator().NewBucket(context.Background(), dir, dir, logger, metrics,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSegmentsCleanupInterval(time.Hour),
		WithClassName("TestClass"))
	require.Error(t, err)

	require.Equal(t, before, testutil.ToFloat64(gauge),
		"a failed load must close (and un-count) every segment it opened")
}

// TestNewBucket_MidLoopSegmentFailureClosesEarlierSegments pins the mid-loop
// variant: when the SECOND segment fails to open, the first — already
// mmapped — must be closed; the loading loop returns before the group is
// published anywhere.
func TestNewBucket_MidLoopSegmentFailureClosesEarlierSegments(t *testing.T) {
	dir := t.TempDir()
	seeded, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	require.NoError(t, seeded.Put([]byte("k1"), []byte("v1")))
	require.NoError(t, seeded.FlushAndSwitch())
	require.NoError(t, seeded.Put([]byte("k2"), []byte("v2")))
	require.NoError(t, seeded.FlushAndSwitch())
	require.NoError(t, seeded.Shutdown(context.Background()))

	segs, err := filepath.Glob(filepath.Join(dir, "segment-*.db"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(segs), 2)
	sort.Strings(segs)
	require.NoError(t, os.Truncate(segs[len(segs)-1], 3)) // corrupt the last-loaded one

	metrics, err := NewMetrics(monitoring.GetMetrics(), "TestClass", "shard")
	require.NoError(t, err)
	gauge, err := metrics.segmentTotalByStrategy.GetMetricWithLabelValues(StrategyReplace)
	require.NoError(t, err)
	before := testutil.ToFloat64(gauge)

	logger, _ := test.NewNullLogger()
	_, err = NewBucketCreator().NewBucket(context.Background(), dir, dir, logger, metrics,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace), WithSegmentsCleanupInterval(time.Hour),
		WithClassName("TestClass"))
	require.Error(t, err)

	require.Equal(t, before, testutil.ToFloat64(gauge),
		"segments opened before the failing one must be closed (and un-counted)")
}
