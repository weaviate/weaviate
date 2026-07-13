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
	"context"
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// uuidPostDump is written after the snapshot dump; a correct rebuild differs from the dump-time root.
const uuidPostDump = strfmt.UUID("77777777-7777-7777-7777-777777777777")

// haltAndDumpForOffload puts s into the post-offloading-halt state (maintenance paused, async off, stale .ht).
func haltAndDumpForOffload(t *testing.T, ctx context.Context, s *Shard) {
	t.Helper()
	require.NoError(t, s.HaltForTransfer(ctx, true, 0))
	require.Len(t, htFilesInDir(t, s.pathHashTree()), 1, "offloading halt must dump a .ht")
	s.asyncReplicationRWMux.RLock()
	require.Nil(t, s.hashtree, "offloading halt must nil the hashtree")
	s.asyncReplicationRWMux.RUnlock()
	require.Equal(t, 1, s.haltForTransferCount, "offloading halt must pause maintenance")
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

	haltAndDumpForOffload(t, ctx, s)

	require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, uuidPostDump, tsFarPast)))
	require.NoError(t, s.store.FlushMemtables(ctx))

	require.NoError(t, idx.resumeAfterAbortedOffload(ctx, s.name))

	require.Equal(t, 0, s.haltForTransferCount, "maintenance must be resumed")
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

	haltAndDumpForOffload(t, ctx, s)

	require.NoError(t, idx.resumeAfterAbortedOffload(ctx, s.name))

	require.Equal(t, 0, s.haltForTransferCount, "maintenance must be resumed")
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
	s.mayStopAsyncReplication()
	require.Len(t, htFilesInDir(t, s.pathHashTree()), 1, "pre-condition: a snapshot exists")
	require.Equal(t, 0, s.haltForTransferCount, "pre-condition: maintenance not halted")

	require.NoError(t, idx.resumeAfterAbortedOffload(ctx, s.name))
	require.Equal(t, 0, s.haltForTransferCount)
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

	haltAndDumpForOffload(t, ctx, s)

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
