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
