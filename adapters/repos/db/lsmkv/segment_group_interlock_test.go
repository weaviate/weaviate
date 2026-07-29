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
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storagestate"
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

// TestWALRecovery_FlushesLastWALUnderLiveOps pins the resurrection fix: data
// recovered from the last WAL used to become the live memtable — pre-strip
// bytes OUTSIDE the pending-segment bookkeeping, resurrecting a completed
// drop's data with nothing left to re-clean it. With ops in the sidecar,
// recovery must flush every WAL into segments so the sidecar re-snapshot
// covers them.
func TestWALRecovery_FlushesLastWALUnderLiveOps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	b1, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	// Small enough for Shutdown's flushWAL path, which KEEPS the WAL on disk —
	// the exact shape an interrupted close leaves behind.
	require.NoError(t, b1.Put([]byte("k1"), []byte("v1")))

	// Register the op through a separate sidecar handle: going through the
	// bucket would FlushAndSwitch and drain the very WAL this test needs to
	// survive the close.
	ext := newSegmentEditOps(dir, "TestClass")
	require.NoError(t, ext.RegisterOp("op1", OpDescriptor{Type: OpTypeRemoveTargetVectors, CreatedAt: 1}))
	require.NoError(t, ext.Close())
	require.NoError(t, b1.Shutdown(ctx))

	b2, err := newInterlockTestBucket(t, dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, b2.Shutdown(ctx)) }()

	require.NotNil(t, b2.disk.editOps)
	pending, err := b2.disk.editOps.Pending("op1")
	require.NoError(t, err)
	require.NotEmpty(t, pending,
		"WAL-recovered data must land in segments re-pended under the surviving op, not in the live memtable")

	v, err := b2.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v, "recovered data must still be readable after the flush")
}

// TestNewBucket_FailsWhenSidecarLocked pins the reload fence: a sidecar still
// flocked by a previous instance means that instance has not finished closing
// — loading blind here (the old soft-skip logged "cleanup stalled") is how a
// completed drop lost its healing re-pend. The load must fail retryably.
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
// tenant re-init over the same directory must be refused BEFORE it touches
// any file, and must succeed once the leaked handle is finally shut down
// (in practice: process restart). Writing this pin found two bugs: the
// registry was checked LAST, so a doomed re-open first hung forever on the
// cleanup.db flock and, once that got a timeout, deleted the live
// instance's active WAL during recovery — the leaked bucket's buffered
// writes then flushed into the unlinked inode: silent data loss.
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
