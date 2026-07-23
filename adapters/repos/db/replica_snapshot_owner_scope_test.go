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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

// These tests drive the real Index+Shard+LSM harness (newSharedHaltTestShard) so
// index.ReleaseBackup / index.resumeMaintenanceCycles run the REAL index-wide sweep
// against a REAL counter. Each pins the owner-scoping invariant: a foreign
// operation's release (or a watchdog fire) must not lift a halt a different
// in-flight operation still holds. The observable is the guard site itself —
// ListBackupFiles returns "not paused for transfer" only when the shard's total
// halt count is zero — so a survivor's ListBackupFiles succeeding proves its halt
// survived.

const ownerScopeObj1 = strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b")

// requireHalted asserts the shard is still paused for transfer by exercising the
// exact guard the reported flake tripped.
func requireHalted(t *testing.T, ctx context.Context, shard *Shard, msg string) {
	t.Helper()
	_, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.NoError(t, err, msg)
}

func requireTotal(t *testing.T, shard *Shard, want int, msg string) {
	t.Helper()
	shard.haltForTransferMux.Lock()
	defer shard.haltForTransferMux.Unlock()
	require.Equal(t, want, shard.haltTotalLocked(), msg)
}

// The reported flake: a hardlink backup holds a shard; a DIFFERENT backup's
// release runs the index-wide sweep and used to drive the holder 1->0, so its
// next ListBackupFiles saw "not paused for transfer".
func TestForeignReleaseKeepsHardlinkHolderHalted(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	ownerA := backup.NewOp("A").HaltOwner()
	require.NoError(t, shard.HaltForTransfer(ctx, ownerA, false, 0))
	defer func() { _ = shard.resumeMaintenanceCycles(ctx, ownerA) }()

	// Foreign backup B releases while A still holds the shard.
	require.NoError(t, index.ReleaseBackup(ctx, backup.NewOp("B")))

	files, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.NoError(t, err, "foreign release must not unhalt A's shard")
	require.NotEmpty(t, files)
	_, err = shard.GetFileMetadata(ctx, files[0])
	require.NoError(t, err, "GetFileMetadata must still see A's halt")
	rc, err := shard.GetFile(ctx, files[0])
	require.NoError(t, err, "GetFile must still see A's halt")
	require.NoError(t, rc.Close())
	requireTotal(t, shard, 1, "only A's halt should remain")
}

// A same-ID cancel->retry: the prior backup's in-flight release must not resume
// the retry's fresh halt. The fence makes the retry's Op distinct from the
// releasing prior backup's, even though the ID is identical.
func TestSameIDReleaseKeepsRetryHalted(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	opX1, opX2 := backup.NewOp("X"), backup.NewOp("X")

	// B1 is admitted and releases, freeing the admission gate.
	require.NoError(t, index.initBackup(opX1))
	require.NoError(t, index.ReleaseBackup(ctx, opX1))

	// B2 (same ID, new fence) is admitted and halts the shard.
	require.NoError(t, index.initBackup(opX2))
	require.NoError(t, shard.HaltForTransfer(ctx, opX2.HaltOwner(), false, 0))
	defer func() { _ = shard.resumeMaintenanceCycles(ctx, opX2.HaltOwner()) }()

	// B1's in-flight sweep (fence of opX1) lands on the shard B2 now holds.
	require.NoError(t, index.resumeMaintenanceCycles(ctx, opX1.HaltOwner()))

	requireTotal(t, shard, 1, "B1's sweep must not resume B2's halt")
	requireHalted(t, ctx, shard, "B2's fresh halt must survive B1's release")
}

// A redundant/late prior-instance release must be inert against a same-ID
// successor on BOTH axes: it must neither resume the successor's halt nor clear
// its admission gate.
func TestStaleGenerationReleaseInertAgainstSuccessor(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	opX1, opX2 := backup.NewOp("X"), backup.NewOp("X")

	require.NoError(t, index.initBackup(opX1))
	require.NoError(t, index.ReleaseBackup(ctx, opX1))

	require.NoError(t, index.initBackup(opX2))
	require.NoError(t, shard.HaltForTransfer(ctx, opX2.HaltOwner(), false, 0))
	defer func() { _ = shard.resumeMaintenanceCycles(ctx, opX2.HaltOwner()) }()

	// The redundant B1 (opX1) release lands after B2 is admitted.
	require.NoError(t, index.ReleaseBackup(ctx, opX1))

	requireTotal(t, shard, 1, "stale B1 release must not resume B2's halt")
	requireHalted(t, ctx, shard, "B2's halt must survive the stale B1 release")

	cur := index.lastBackup.Load()
	require.NotNil(t, cur, "stale B1 release must not clear B2's admission gate")
	require.Equal(t, opX2, cur.Op, "the live gate must still hold B2's Op")
}

// Non-hardlink silent leak: two independent holders (a backup and a fallback
// replica op) must both survive a foreign backup release — no holder is silently
// absorbed. The bug here surfaces as state, not an error.
func TestForeignReleaseKeepsAllOwnersOnNonHardlink(t *testing.T) {
	t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	ownerA := backup.NewOp("A").HaltOwner()
	require.NoError(t, shard.HaltForTransfer(ctx, ownerA, false, 0))
	defer func() { _ = shard.resumeMaintenanceCycles(ctx, ownerA) }()

	const opB = "00000000-0000-0000-0000-0000000000b0"
	_, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", opB)
	require.NoError(t, err)
	defer func() { _ = index.IncomingReleaseReplicaSnapshot(ctx, opB) }()

	opC := backup.NewOp("C")
	require.NoError(t, index.initBackup(opC))
	require.NoError(t, index.ReleaseBackup(ctx, opC))

	requireTotal(t, shard, 2, "both holders must survive a foreign release")
	shard.haltForTransferMux.Lock()
	backupHalts := shard.haltForTransferOwners[ownerA]
	replicaHalts := shard.haltForTransferOwners[replicaHaltOwner(opB)]
	shard.haltForTransferMux.Unlock()
	require.Equal(t, 1, backupHalts, "backup A's own halt must survive")
	require.Equal(t, 1, replicaHalts, "replica op B's own halt must survive")
}

// Offload halts a shard and never resumes on the success path (the shard is
// frozen then dropped); a concurrent backup's release must not decrement it and
// resume compaction mid cloud upload.
func TestForeignReleaseKeepsOffloadHalt(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	offloadOwner := offloadHaltOwner("shard1")
	require.NoError(t, shard.HaltForTransfer(ctx, offloadOwner, true, 0))
	defer func() { _ = shard.resumeMaintenanceCycles(ctx, offloadOwner) }()

	opB := backup.NewOp("B")
	require.NoError(t, index.initBackup(opB))
	require.NoError(t, index.ReleaseBackup(ctx, opB))

	requireTotal(t, shard, 1, "offload halt must survive a foreign backup release")
	requireHalted(t, ctx, shard, "compaction must stay paused for the live offload")
}

// The offload abort path (cloud Upload fails, tenant reverts to HOT without a
// drop) has no teardown to release its halt, so it must resume its own halt or a
// live tenant stays paused for compaction forever.
func TestOffloadAbortResumesOwnHalt(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	m := &Migrator{
		cloud:   &fakeOffloadCloud{uploadErr: context.DeadlineExceeded},
		cluster: fakeTenantProcessor{},
		nodeId:  "node1",
		logger:  logrus.New(),
	}

	ec := errorcompounder.New()
	m.freeze(ctx, index, "TestClass", []string{"shard1"}, ec)

	requireTotal(t, shard, 0, "offload abort must lift its own halt")
	// A resumed shard must reject file listing again.
	_, err := shard.ListBackupFiles(ctx, &backup.ShardDescriptor{})
	require.ErrorContains(t, err, "not paused for transfer",
		"compaction must be resumed after the aborted offload")
}

// The abort's resume must lift the offload halt even when the operation ctx is
// already cancellation-driven — freeze resumes on context.Background(), matching
// the sibling abort paths. The cloud Upload cancels the op ctx before returning
// its error, so the abort branch runs with a canceled ctx.
func TestOffloadAbortResumesOwnHaltUnderCanceledCtx(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	m := &Migrator{
		cloud:   &fakeOffloadCloud{uploadErr: context.Canceled, onUpload: cancel},
		cluster: fakeTenantProcessor{},
		nodeId:  "node1",
		logger:  logrus.New(),
	}

	ec := errorcompounder.New()
	m.freeze(ctx, index, "TestClass", []string{"shard1"}, ec)

	requireTotal(t, shard, 0, "the offload halt must be lifted even under a canceled op ctx")
	_, err := shard.ListBackupFiles(context.Background(), &backup.ShardDescriptor{})
	require.ErrorContains(t, err, "not paused for transfer",
		"compaction must be resumed after an aborted offload under a canceled ctx")
}

// Owner keys are namespaced per subsystem, so a backup whose user-supplied ID
// equals a shard name cannot collide with that shard's offload key: the backup's
// halt+self-resume+release must net zero on its own key and leave offload intact.
func TestOwnerKeyNamespacesDoNotCollide(t *testing.T) {
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	offloadOwner := offloadHaltOwner("shard1")
	require.NoError(t, shard.HaltForTransfer(ctx, offloadOwner, true, 0))
	defer func() { _ = shard.resumeMaintenanceCycles(ctx, offloadOwner) }()

	// A hardlink backup whose ID equals the shard name halts, self-resumes, and
	// releases against the same shard.
	opShard := backup.NewOp("shard1")
	require.NoError(t, index.initBackup(opShard))
	_, err := shard.CreateBackupSnapshot(ctx, opShard.HaltOwner(), &backup.ShardDescriptor{}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, index.ReleaseBackup(ctx, opShard))

	requireTotal(t, shard, 1, "offload:shard1 and backup:shard1:<fence> must be distinct keys")
	requireHalted(t, ctx, shard, "the offload must survive a same-name backup's full lifecycle")
}

// One monitor per shard is shared by every armed halt-for-duration transfer. A
// fire must force-resume EVERY armed owner (not just the last to arm) while
// leaving a non-armed backup co-holder intact, and the two-armer/one-completes
// sequence must not orphan the still-stalled armer.
func TestForcedResumeReleasesAllArmedTransfers(t *testing.T) {
	t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
	index, shard := newSharedHaltTestShard(t)
	index.Config.TransferInactivityTimeout = time.Hour
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	const opT1 = "00000000-0000-0000-0000-0000000000t1"
	const opT2 = "00000000-0000-0000-0000-0000000000t2"
	_, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", opT1)
	require.NoError(t, err)
	_, err = index.IncomingCreateReplicaSnapshot(ctx, "shard1", opT2)
	require.NoError(t, err)

	backupOwner := backup.NewOp("B").HaltOwner()
	require.NoError(t, shard.HaltForTransfer(ctx, backupOwner, false, 0))
	defer func() { _ = shard.resumeMaintenanceCycles(ctx, backupOwner) }()

	// T2 completes normally while T1 is still stalled; the monitor must survive.
	require.NoError(t, index.IncomingReleaseReplicaSnapshot(ctx, opT2))
	shard.haltForTransferMux.Lock()
	armedCount := len(shard.haltForTransferInactivityOwners)
	_, t1Armed := shard.haltForTransferInactivityOwners[replicaHaltOwner(opT1)]
	shard.haltForTransferMux.Unlock()
	require.Equal(t, 1, armedCount, "the monitor must stay armed for the still-stalled T1")
	require.True(t, t1Armed, "T1 must still be the armed owner")

	forceInactivityFire(t, shard)

	shard.haltForTransferMux.Lock()
	_, t1Present := shard.haltForTransferOwners[replicaHaltOwner(opT1)]
	_, backupPresent := shard.haltForTransferOwners[backupOwner]
	monitorDown := shard.haltForTransferCtxCancel == nil
	shard.haltForTransferMux.Unlock()

	require.False(t, t1Present, "the stalled T1 must be force-resumed")
	require.True(t, backupPresent, "the non-armed backup co-holder must survive the fire")
	require.True(t, monitorDown, "the monitor must be torn down once no armed owner remains")
	requireHalted(t, ctx, shard, "the backup's halt must survive the watchdog fire")
}

// The single-armer case: a fire lifts the armed transfer but must spare a
// non-armed backup co-holder (vs. the old whole-shard zero).
func TestForcedResumeSpareBackupCoHolder(t *testing.T) {
	t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
	index, shard := newSharedHaltTestShard(t)
	index.Config.TransferInactivityTimeout = time.Hour
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	const opT = "00000000-0000-0000-0000-0000000000ff"
	_, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", opT)
	require.NoError(t, err)

	backupOwner := backup.NewOp("B").HaltOwner()
	require.NoError(t, shard.HaltForTransfer(ctx, backupOwner, false, 0))
	defer func() { _ = shard.resumeMaintenanceCycles(ctx, backupOwner) }()

	forceInactivityFire(t, shard)

	shard.haltForTransferMux.Lock()
	_, tPresent := shard.haltForTransferOwners[replicaHaltOwner(opT)]
	_, backupPresent := shard.haltForTransferOwners[backupOwner]
	shard.haltForTransferMux.Unlock()

	require.False(t, tPresent, "the armed transfer must be force-resumed")
	require.True(t, backupPresent, "the backup co-holder must survive")
	requireHalted(t, ctx, shard, "the backup's halt must survive the fire")
}

// A replica-snapshot holder must survive a foreign backup release too — the
// silent torn-snapshot variant of the reported flake for a different subsystem.
func TestForeignReleaseKeepsReplicaHolderHalted(t *testing.T) {
	t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
	index, shard := newSharedHaltTestShard(t)
	ctx := context.Background()
	putSharedHaltObject(t, index, ownerScopeObj1, 0)

	const opR = "00000000-0000-0000-0000-0000000000f0"
	_, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", opR)
	require.NoError(t, err)
	defer func() { _ = index.IncomingReleaseReplicaSnapshot(ctx, opR) }()

	opB := backup.NewOp("B")
	require.NoError(t, index.initBackup(opB))
	require.NoError(t, index.ReleaseBackup(ctx, opB))

	requireTotal(t, shard, 1, "the replica holder must survive a foreign backup release")
	requireHalted(t, ctx, shard, "compaction must stay paused for the live replica transfer")
}

// forceInactivityFire drives a deterministic watchdog fire by expiring the
// deadline under the mux and calling handleInactivityFire directly, mirroring
// shard_autoresume_maintenance_test.go.
func forceInactivityFire(t *testing.T, shard *Shard) {
	t.Helper()
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()

	shard.haltForTransferMux.Lock()
	shard.haltForTransferInactivityDeadline = time.Now().Add(-time.Hour)
	shard.haltForTransferMux.Unlock()

	require.False(t, shard.handleInactivityFire(context.Background(), timer),
		"a genuine fire stops watching")
}

type fakeOffloadCloud struct {
	uploadErr error
	// onUpload fires at the start of Upload — used to cancel the operation ctx
	// mid-offload so the abort's resume path is exercised under cancellation.
	onUpload func()
}

func (f *fakeOffloadCloud) VerifyBucket(context.Context) error { return nil }

func (f *fakeOffloadCloud) Upload(context.Context, string, string, string) error {
	if f.onUpload != nil {
		f.onUpload()
	}
	return f.uploadErr
}

func (f *fakeOffloadCloud) Download(context.Context, string, string, string) error { return nil }

func (f *fakeOffloadCloud) Delete(context.Context, string, string, string) error { return nil }

type fakeTenantProcessor struct{}

func (fakeTenantProcessor) UpdateTenantsProcess(context.Context, string, *command.TenantProcessRequest) (uint64, error) {
	return 0, nil
}
