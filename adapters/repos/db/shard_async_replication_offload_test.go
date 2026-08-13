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

//go:build integrationTest

package db

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// uuidPostDump is written after the snapshot dump; a correct rebuild differs from the dump-time root.
const uuidPostDump = strfmt.UUID("77777777-7777-7777-7777-777777777777")

// haltForOffload puts s into the post-offloading-halt state: maintenance paused,
// async replication off, and NO hashtree on disk. The halt deliberately does not
// persist one — the shard survives the halt and keeps taking writes, so a snapshot
// would be accepted as authoritative on the next load while already stale.
func haltForOffload(t *testing.T, ctx context.Context, s *Shard) {
	t.Helper()
	require.NoError(t, s.HaltForTransfer(ctx, offloadHaltOwner(s.name), true, 0))
	require.Empty(t, htFilesInDir(t, s.pathHashTree()),
		"a halt the shard survives must not persist a hashtree")
	s.asyncReplicationRWMux.RLock()
	require.Nil(t, s.hashtree, "offloading halt must nil the hashtree")
	s.asyncReplicationRWMux.RUnlock()
	requireTotal(t, s, 1, "offloading halt must pause maintenance")
}

// haltForOffloadWithStaleSnapshot halts s for offload and plants a stale .ht (as a pre-fix binary could); recovery must discard it, not trust it.
func haltForOffloadWithStaleSnapshot(t *testing.T, ctx context.Context, s *Shard) {
	t.Helper()
	s.asyncReplicationRWMux.RLock()
	var payload bytes.Buffer
	_, serErr := s.hashtree.Serialize(&payload)
	s.asyncReplicationRWMux.RUnlock()
	require.NoError(t, serErr)

	haltForOffload(t, ctx, s)

	require.NoError(t, os.MkdirAll(s.pathHashTree(), os.ModePerm))
	stale := filepath.Join(s.pathHashTree(), "hashtree-0000000000000001.ht")
	require.NoError(t, os.WriteFile(stale, payload.Bytes(), 0o600))
}

// TestResumeAfterAbortedOffload_RebuildsFromScratch: a post-dump write must appear in the rebuilt tree.
func TestResumeAfterAbortedOffload_RebuildsFromScratch(t *testing.T) {
	ctx := context.Background()
	const class = "ResumeAfterAbortedOffloadRebuild"

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	setShardReplicas(t, idx, "node1", "node2")

	cfg := minAsyncReplicationConfig()

	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	require.NoError(t, s.store.FlushMemtables(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	rootAtDump := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()
	require.NotEqual(t, hashtree.Digest{}, rootAtDump, "sanity: seeded hashtree must have a non-zero root")

	haltForOffloadWithStaleSnapshot(t, ctx, s)

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidPostDump, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))

	require.NoError(t, idx.resumeAfterAbortedOffload(ctx, s.name))

	requireTotal(t, s, 0, "maintenance must be resumed")
	require.Empty(t, htFilesInDir(t, s.pathHashTree()), "stale .ht must be discarded")
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	rootAfterResume := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()
	require.NotEqual(t, rootAtDump, rootAfterResume,
		"rebuilt hashtree must reflect the post-dump write; equal roots mean the stale snapshot was trusted and divergence is invisible to repair")
}

// TestResumeAfterAbortedOffload_AsyncDisabledRemovesStaleHashtree: recovery drops the snapshot, leaves async off.
func TestResumeAfterAbortedOffload_AsyncDisabledRemovesStaleHashtree(t *testing.T) {
	ctx := context.Background()
	const class = "ResumeAfterAbortedOffloadAsyncDisabled"

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	setShardReplicas(t, idx, "node1") // single replica → not enabled

	cfg := minAsyncReplicationConfig()

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	haltForOffloadWithStaleSnapshot(t, ctx, s)

	require.NoError(t, idx.resumeAfterAbortedOffload(ctx, s.name))

	requireTotal(t, s, 0, "maintenance must be resumed")
	require.Empty(t, htFilesInDir(t, s.pathHashTree()), "stale .ht must be discarded even when async is disabled")
	s.asyncReplicationRWMux.RLock()
	require.Nil(t, s.hashtree, "async replication must stay off when not enabled for the shard")
	s.asyncReplicationRWMux.RUnlock()
}

// TestResumeAfterAbortedOffload_NotHalted: the HaltForTransfer-failed shape; recovery still drops the snapshot and rebuilds.
func TestResumeAfterAbortedOffload_NotHalted(t *testing.T) {
	ctx := context.Background()
	const class = "ResumeAfterAbortedOffloadNotHalted"

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	setShardReplicas(t, idx, "node1", "node2")

	cfg := minAsyncReplicationConfig()

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	// snapshot + disable without halting
	stopAsyncAndDump(t, s)
	require.Len(t, htFilesInDir(t, s.pathHashTree()), 1, "pre-condition: a snapshot exists")
	requireTotal(t, s, 0, "pre-condition: maintenance not halted")

	require.NoError(t, idx.resumeAfterAbortedOffload(ctx, s.name))
	requireTotal(t, s, 0, "maintenance must be resumed")
	require.Empty(t, htFilesInDir(t, s.pathHashTree()), "stale .ht must be discarded")
	awaitHashtreeInitialized(t, s)

	// second recovery is idempotent
	require.NoError(t, idx.resumeAfterAbortedOffload(ctx, s.name))
	require.Empty(t, htFilesInDir(t, s.pathHashTree()))
	awaitHashtreeInitialized(t, s)
}

// TestResumeAfterAbortedOffload_ConcurrentReconcileNoStaleTree: reconcile racing recovery, -race.
func TestResumeAfterAbortedOffload_ConcurrentReconcileNoStaleTree(t *testing.T) {
	ctx := context.Background()
	const class = "ResumeAfterAbortedOffloadConcurrentReconcile"

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	setShardReplicas(t, idx, "node1", "node2")

	cfg := minAsyncReplicationConfig()

	for _, id := range []strfmt.UUID{uuidLow, uuidMid, uuidHigh} {
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, tsFarPast)))
	}
	require.NoError(t, s.store.FlushMemtables(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, cfg))
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	rootAtDump := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()

	haltForOffloadWithStaleSnapshot(t, ctx, s)

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidPostDump, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = idx.reconcileAsyncReplication(ctx)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, idx.resumeAfterAbortedOffload(ctx, s.name))
	}()
	wg.Wait()

	require.Empty(t, htFilesInDir(t, s.pathHashTree()), "no stale .ht may survive the race")
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	rootAfterResume := s.hashtree.Root()
	s.asyncReplicationRWMux.RUnlock()
	require.NotEqual(t, rootAtDump, rootAfterResume,
		"final hashtree must reflect the post-dump write even under a concurrent reconcile")
}

// seedHealTestShard returns a loaded shard of class with async replication running
// and one flushed object, the state a live HOT tenant is in when a freeze starts.
func seedHealTestShard(t *testing.T, ctx context.Context, class string) (ShardLike, *Index, *Shard) {
	t.Helper()

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	setShardReplicas(t, idx, "node1", "node2")

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))
	require.NoError(t, s.enableAsyncReplication(ctx, minAsyncReplicationConfig()))
	awaitHashtreeInitialized(t, s)

	return sl, idx, s
}

// updateTenantToHot drives the production entry point the abort takes on a node
// whose own upload succeeded: the FSM applies the tenant's return to HOT through
// Migrator.UpdateTenants.
func updateTenantToHot(t *testing.T, ctx context.Context, idx *Index, class, shardName string) {
	t.Helper()

	m := newDropTestMigrator(idx, class, nil)
	require.NoError(t, m.UpdateTenants(ctx, &models.Class{Class: class},
		[]*schemaUC.UpdateTenantPayload{{Name: shardName, Status: models.TenantActivityStatusHOT}}, true))
}

// TestAbortedFreezeHealsOffloadHaltOnHotTransition: a freeze this node uploaded
// successfully keeps its offload halt until FROZEN drops the shard. When another
// node aborts the round the tenant returns to HOT instead, and no offload halt may
// survive that transition — nothing else would ever lift it, leaving compaction,
// vector maintenance and async replication off on a live, write-serving tenant.
func TestAbortedFreezeHealsOffloadHaltOnHotTransition(t *testing.T) {
	coResidentBackupOwner := backup.NewOp("b").HaltOwner()

	tests := []struct {
		name   string
		class  string
		setup  func(t *testing.T, ctx context.Context, s *Shard)
		assert func(t *testing.T, ctx context.Context, s *Shard)
	}{
		{
			name:  "leaked offload halt",
			class: "AbortedFreezeHealLeaked",
			setup: func(t *testing.T, ctx context.Context, s *Shard) {
				haltForOffload(t, ctx, s)
			},
			assert: func(t *testing.T, ctx context.Context, s *Shard) {
				requireTotal(t, s, 0, "the offload halt must not survive the return to HOT")
				require.Empty(t, htFilesInDir(t, s.pathHashTree()), "no stale .ht may be trusted after the heal")
				awaitHashtreeInitialized(t, s)
			},
		},
		{
			name:  "two aborted rounds",
			class: "AbortedFreezeHealTwoRounds",
			setup: func(t *testing.T, ctx context.Context, s *Shard) {
				owner := offloadHaltOwner(s.name)
				require.NoError(t, s.HaltForTransfer(ctx, owner, true, 0))
				require.NoError(t, s.HaltForTransfer(ctx, owner, true, 0))
				requireTotal(t, s, 2, "pre-condition: two freeze rounds leaked one halt each")
			},
			assert: func(t *testing.T, ctx context.Context, s *Shard) {
				requireTotal(t, s, 0, "the heal drops the owner outright, so a second leaked round clears too")
				awaitHashtreeInitialized(t, s)
			},
		},
		{
			name:  "co-resident backup halt",
			class: "AbortedFreezeHealCoResidentBackup",
			setup: func(t *testing.T, ctx context.Context, s *Shard) {
				haltForOffload(t, ctx, s)
				require.NoError(t, s.HaltForTransfer(ctx, coResidentBackupOwner, false, 0))
			},
			assert: func(t *testing.T, ctx context.Context, s *Shard) {
				requireTotal(t, s, 1, "a backup still holding this shard must keep its halt")
				s.haltForTransferMux.Lock()
				defer s.haltForTransferMux.Unlock()
				_, backupHeld := s.haltForTransferOwners[coResidentBackupOwner]
				require.True(t, backupHeld, "the heal is scoped to the offload owner")
			},
		},
		{
			name:  "no halt",
			class: "AbortedFreezeHealNoHalt",
			setup: func(t *testing.T, ctx context.Context, s *Shard) {},
			assert: func(t *testing.T, ctx context.Context, s *Shard) {
				requireTotal(t, s, 0, "a routine activation must not invent a halt")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			sl, idx, s := seedHealTestShard(t, ctx, tt.class)

			tt.setup(t, ctx, s)
			updateTenantToHot(t, ctx, idx, tt.class, sl.Name())
			tt.assert(t, ctx, s)
		})
	}
}

// TestHotTransitionWithoutOffloadHaltDoesNotRescan pins the wasHeld gate.
//
// This test is not red against the unhealed code — there the HOT branch is a no-op.
// It is red against a heal that recovers unconditionally: rebuildAsyncReplicationFromScratch
// replaces the hashtree by a full object scan, so every routine tenant activation
// would pay one.
func TestHotTransitionWithoutOffloadHaltDoesNotRescan(t *testing.T) {
	ctx := context.Background()
	const class = "HotTransitionNoRescan"

	sl, idx, s := seedHealTestShard(t, ctx, class)

	s.asyncReplicationRWMux.RLock()
	before := s.hashtree
	s.asyncReplicationRWMux.RUnlock()
	require.NotNil(t, before, "pre-condition: async replication is running")

	updateTenantToHot(t, ctx, idx, class, sl.Name())

	s.asyncReplicationRWMux.RLock()
	after := s.hashtree
	s.asyncReplicationRWMux.RUnlock()
	require.Same(t, before, after, "an unhalted HOT transition must not rebuild the hashtree")
}

// blockingDeactivateCtrl parks the vector-maintenance pause leg of HaltForTransfer.
// That leg runs while haltForTransferMux is held, so it reproduces the shape of a
// slow seal (compaction drain, vector and geo PrepareForBackup, memtable flush),
// which is bounded only by HaltForTransferTimeout — one hour by default.
type blockingDeactivateCtrl struct {
	cyclemanager.CycleCallbackCtrl
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func (c *blockingDeactivateCtrl) Deactivate(ctx context.Context) error {
	c.enteredOnce.Do(func() { close(c.entered) })
	<-c.release
	return c.CycleCallbackCtrl.Deactivate(ctx)
}

// unblock is idempotent so a failing test can free the parked seal from a cleanup
// and still tear the shard down, instead of hanging until the suite timeout.
func (c *blockingDeactivateCtrl) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

// TestHotTransitionDoesNotBlockOnInFlightSeal: the offload-halt heal runs on the RAFT
// apply goroutine, so it must decide "nothing to heal" without taking
// haltForTransferMux. A replica snapshot or backup takes no backupLock, so a seal of
// shard N overlaps a routine activation of tenant N freely; a heal that parked on the
// mux would stall every RAFT apply on the node for the seal's whole duration.
func TestHotTransitionDoesNotBlockOnInFlightSeal(t *testing.T) {
	ctx := context.Background()
	const class = "HotTransitionInFlightSeal"

	_, idx, s := seedHealTestShard(t, ctx, class)

	sealOwner := replicaHaltOwner("op-in-flight")
	ctrl := &blockingDeactivateCtrl{
		CycleCallbackCtrl: s.cycleCallbacks.vectorCombinedCallbacksCtrl,
		entered:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	s.cycleCallbacks.vectorCombinedCallbacksCtrl = ctrl
	t.Cleanup(ctrl.unblock)

	sealed := make(chan error, 1)
	go func() { sealed <- s.HaltForTransfer(ctx, sealOwner, false, 0) }()
	<-ctrl.entered // the seal now holds haltForTransferMux

	healed := make(chan error, 1)
	go func() { healed <- idx.resumeOffloadHaltAfterAbortedFreeze(ctx, s.name) }()

	select {
	case err := <-healed:
		require.NoError(t, err, "a shard holding no offload halt heals to a clean no-op")
	case <-time.After(10 * time.Second):
		t.Fatal("the offload heal parked on haltForTransferMux held by an in-flight seal; on the RAFT apply goroutine this stalls every apply on the node")
	}

	ctrl.unblock()
	require.NoError(t, <-sealed)
	requireTotal(t, s, 1, "the seal's own halt must survive the heal")
	require.NoError(t, s.resumeMaintenanceCycles(ctx, sealOwner))
}

// TestOffloadHaltProbeTracksOwnerMap pins the invariant the lock-free heal rests on:
// publishHaltTotalLocked derives the offload count from the owner keys, so only an
// offload owner's halt makes the probe positive and every mutation republishes.
func TestOffloadHaltProbeTracksOwnerMap(t *testing.T) {
	ctx := context.Background()
	const class = "OffloadHaltProbeTracksOwners"

	_, _, s := seedHealTestShard(t, ctx, class)

	backupOwner := backup.NewOp("probe").HaltOwner()
	require.NoError(t, s.HaltForTransfer(ctx, backupOwner, false, 0))
	require.False(t, s.haltedForOffload(), "a backup halt is not an offload halt")

	require.NoError(t, s.HaltForTransfer(ctx, offloadHaltOwner(s.name), true, 0))
	require.True(t, s.haltedForOffload(), "an offload halt must be visible to the probe")

	require.NoError(t, s.resumeMaintenanceCycles(ctx, offloadHaltOwner(s.name)))
	require.False(t, s.haltedForOffload(), "dropping the offload owner must republish the probe")
	require.True(t, s.haltedForTransfer(), "the co-resident backup halt still holds the shard")

	require.NoError(t, s.resumeMaintenanceCycles(ctx, backupOwner))
	require.False(t, s.haltedForTransfer())
}

var errResumeLegFailed = errors.New("simulated failing resume leg")

// failingActivateCtrl fails the vector-callback leg of the physical resume and
// delegates every other call to the control it replaces. It stands in for the
// Unregister trick the sibling halt tests use: this fixture registers the shard's
// cycle callbacks in noop index groups, whose controls cannot fail.
type failingActivateCtrl struct {
	cyclemanager.CycleCallbackCtrl
}

func (failingActivateCtrl) Activate() error { return errResumeLegFailed }

// failOffloadResumeLeg leaves s in the state a freeze abort finds when its
// maintenance resume is about to fail: the offload halt is placed and one leg of
// the physical resume is broken. haltForOffload asserts the pre-condition the
// rebuild assertions rest on: no hashtree in memory and none on disk.
func failOffloadResumeLeg(t *testing.T, ctx context.Context, s *Shard) {
	t.Helper()
	haltForOffload(t, ctx, s)
	s.cycleCallbacks.vectorCombinedCallbacksCtrl = failingActivateCtrl{s.cycleCallbacks.vectorCombinedCallbacksCtrl}
}

// TestOffloadHealRebuildsDespiteFailedResume: the resume clears the offload halt
// before the fallible part that restarts the cycles, so a heal that returns on that
// error never rebuilds async replication — and no retry can, because every later
// attempt finds no halt. The shard would serve writes with replica repair blind to
// them until restart.
func TestOffloadHealRebuildsDespiteFailedResume(t *testing.T) {
	tests := []struct {
		name  string
		class string
		heal  func(*Index, context.Context, string) error
	}{
		{
			name:  "heal after an aborted freeze",
			class: "OffloadHealFailedResumeFreeze",
			heal:  (*Index).resumeOffloadHaltAfterAbortedFreeze,
		},
		{
			name:  "heal after an aborted offload",
			class: "OffloadHealFailedResumeOffload",
			heal:  (*Index).resumeAfterAbortedOffload,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			_, idx, s := seedHealTestShard(t, ctx, tt.class)

			failOffloadResumeLeg(t, ctx, s)

			require.ErrorIs(t, tt.heal(idx, ctx, s.name), errResumeLegFailed,
				"the failed resume leg must still be reported")

			s.haltForTransferMux.Lock()
			_, held := s.haltForTransferOwners[offloadHaltOwner(s.name)]
			s.haltForTransferMux.Unlock()
			require.False(t, held,
				"the failed resume already dropped the halt owner, so nothing can trigger a later rebuild")

			awaitHashtreeInitialized(t, s)
		})
	}
}

// seedHealTestShardOnIndexConfig is seedHealTestShard with the shard enabled on the
// index's own async-replication config rather than the minimal test one. The heights
// then match what every later apply re-derives, so a snapshot planted on this shard
// is one a later enable would actually load and trust.
func seedHealTestShardOnIndexConfig(t *testing.T, ctx context.Context, class string) (ShardLike, *Index, *Shard) {
	t.Helper()

	sl, idx := testShard(t, ctx, class, asyncSchedulerOption(t, ctx))
	s := concreteShard(t, sl)
	t.Cleanup(func() { _ = sl.Shutdown(ctx) })
	setShardReplicas(t, idx, "node1", "node2")

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidLow, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))

	enabled, config := idx.asyncReplicationStateForShard(s.name)
	require.True(t, enabled, "pre-condition: async replication is enabled for this RF>1 shard")
	require.NoError(t, s.enableAsyncReplication(ctx, config))
	awaitHashtreeInitialized(t, s)

	return sl, idx, s
}

// blockHashtreeScrub makes removePersistedHashtree fail on the planted stale
// snapshot: the file cannot be deleted (the seam errors) and cannot be demoted (a
// directory occupies the demotion target). It returns the stale file's path, which
// survives every scrub until unblockHashtreeScrub runs.
func blockHashtreeScrub(t *testing.T, s *Shard) string {
	t.Helper()

	files := htFilesInDir(t, s.pathHashTree())
	require.Len(t, files, 1, "pre-condition: exactly one planted stale snapshot")
	stale := filepath.Join(s.pathHashTree(), files[0].Name())
	require.NoError(t, os.Mkdir(stale+".tmp", 0o755))

	prev := removeHashtreeFile
	removeHashtreeFile = func(name string) error {
		if name == stale {
			return os.ErrPermission
		}
		return os.Remove(name)
	}
	t.Cleanup(func() { removeHashtreeFile = prev })
	return stale
}

// unblockHashtreeScrub reverses blockHashtreeScrub, leaving the stale snapshot in
// place for the retry to scrub.
func unblockHashtreeScrub(t *testing.T, stale string) {
	t.Helper()
	removeHashtreeFile = os.Remove
	require.NoError(t, os.Remove(stale+".tmp"))
}

// TestOffloadHealRebuildFailureLeavesRetryHandle: rebuildAsyncReplicationFromScratch
// nils the tree and deregisters the shard before its fallible legs, and the healers
// consume the offload halt owner before calling it. A rebuild that then fails would
// leave an RF>1 tenant serving writes with no hashtree, no scheduler entry, and a
// stale snapshot a later enable would trust as authoritative — with nothing left to
// trigger a repair. The failure must leave a retry handle instead.
func TestOffloadHealRebuildFailureLeavesRetryHandle(t *testing.T) {
	// consumeByHaltCycle drives the retry through a later transfer halt and resume,
	// the trigger reapplyAsyncReplicationAfterResume owns.
	consumeByHaltCycle := func(t *testing.T, ctx context.Context, idx *Index, s *Shard) {
		t.Helper()
		retryOwner := backup.NewOp("retry").HaltOwner()
		require.NoError(t, s.HaltForTransfer(ctx, retryOwner, false, 0))
		require.NoError(t, s.resumeMaintenanceCycles(ctx, retryOwner))
	}

	// consumeByReconcile drives it through an enable apply instead, the trigger a
	// replica add or remove produces on a shard that was never re-halted.
	consumeByReconcile := func(t *testing.T, ctx context.Context, idx *Index, s *Shard) {
		t.Helper()
		require.NoError(t, idx.ReconcileAsyncReplicationForShard(ctx, s.name))
	}

	tests := []struct {
		name    string
		class   string
		heal    func(*Index, context.Context, string) error
		consume func(*testing.T, context.Context, *Index, *Shard)
	}{
		{
			name:    "heal after an aborted freeze, retried by a halt cycle",
			class:   "OffloadHealRetryHandleFreeze",
			heal:    (*Index).resumeOffloadHaltAfterAbortedFreeze,
			consume: consumeByHaltCycle,
		},
		{
			name:    "heal after an aborted offload, retried by a halt cycle",
			class:   "OffloadHealRetryHandleOffload",
			heal:    (*Index).resumeAfterAbortedOffload,
			consume: consumeByHaltCycle,
		},
		{
			name:    "heal after an aborted freeze, retried by a reconcile",
			class:   "OffloadHealRetryHandleReconcile",
			heal:    (*Index).resumeOffloadHaltAfterAbortedFreeze,
			consume: consumeByReconcile,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			sl, idx, s := seedHealTestShardOnIndexConfig(t, ctx, tt.class)

			s.asyncReplicationRWMux.RLock()
			rootAtHalt := s.hashtree.Root()
			s.asyncReplicationRWMux.RUnlock()

			haltForOffloadWithStaleSnapshot(t, ctx, s)
			stale := blockHashtreeScrub(t, s)

			// A write the stale snapshot cannot know about: trusting it hides this
			// object from replica repair for as long as the tree lives.
			require.NoError(t, sl.PutObject(ctx, testObjWithTime(tt.class, uuidPostDump, tsFarPast)))
			require.NoError(t, s.store.FlushMemtables(ctx))

			require.Error(t, tt.heal(idx, ctx, s.name), "a rebuild that cannot scrub the snapshot must report it")

			s.haltForTransferMux.Lock()
			_, held := s.haltForTransferOwners[offloadHaltOwner(s.name)]
			s.haltForTransferMux.Unlock()
			require.False(t, held, "the heal already consumed the halt owner, so no later heal can repair this shard")
			s.asyncReplicationRWMux.RLock()
			require.Nil(t, s.hashtree, "the failed rebuild left the shard not repairing")
			s.asyncReplicationRWMux.RUnlock()
			require.True(t, s.asyncReplicationRebuildIsPending(), "the failed rebuild must leave a retry handle")
			require.FileExists(t, stale, "pre-condition: the stale snapshot survived the failed scrub")

			unblockHashtreeScrub(t, stale)
			tt.consume(t, ctx, idx, s)

			awaitHashtreeInitialized(t, s)
			s.asyncReplicationRWMux.RLock()
			rebuilt := s.hashtree.Root()
			s.asyncReplicationRWMux.RUnlock()
			require.NotEqual(t, rootAtHalt, rebuilt,
				"the retry must rebuild by full scan; the pre-halt root means the stale snapshot was trusted and the post-halt write is invisible to repair")
			require.Empty(t, htFilesInDir(t, s.pathHashTree()), "the retry must scrub the stale snapshot")

			sched := idx.asyncReplicationScheduler
			require.Eventually(t, func() bool {
				sched.mu.Lock()
				defer sched.mu.Unlock()
				_, ok := sched.entries[s]
				return ok
			}, 10*time.Second, 10*time.Millisecond, "the retry must re-register the shard with the scheduler")
			require.False(t, s.asyncReplicationRebuildIsPending(), "a successful retry clears the handle")
		})
	}
}

// TestAbortedFreezeHealIsOneShot: the heal is keyed on holding the offload halt, and
// the first call consumes it. A second call must therefore be inert — no second full
// object scan, no error — even though the first call's resume leg failed.
func TestAbortedFreezeHealIsOneShot(t *testing.T) {
	ctx := context.Background()
	const class = "AbortedFreezeHealOneShot"

	_, idx, s := seedHealTestShard(t, ctx, class)

	failOffloadResumeLeg(t, ctx, s)
	require.ErrorIs(t, idx.resumeOffloadHaltAfterAbortedFreeze(ctx, s.name), errResumeLegFailed)
	awaitHashtreeInitialized(t, s)

	s.asyncReplicationRWMux.RLock()
	rebuilt := s.hashtree
	s.asyncReplicationRWMux.RUnlock()
	require.NotNil(t, rebuilt, "pre-condition: the first heal rebuilt the hashtree")

	require.NoError(t, idx.resumeOffloadHaltAfterAbortedFreeze(ctx, s.name),
		"a heal that finds no offload halt is a clean no-op")

	s.asyncReplicationRWMux.RLock()
	after := s.hashtree
	s.asyncReplicationRWMux.RUnlock()
	require.Same(t, rebuilt, after, "the second heal must not pay another full scan")
}
