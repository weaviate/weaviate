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
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Like newAddPropertyLazyFixture but without the Shutdown cleanup: a reproduced
// deadlock would make a deferred Shutdown hang the test binary forever.
func newReplConfigDeadlockFixture(t *testing.T, className string) (*DB, *Index) {
	t.Helper()
	ctx := testCtx()
	repo, migrator, schemaGetter := newLazyLoadRepo(t, singleShardState())

	class := &models.Class{
		Class:               className,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		ReplicationConfig:   &models.ReplicationConfig{Factor: 1},
	}
	require.NoError(t, migrator.AddClass(ctx, class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

	index := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, index)
	return repo, index
}

func soleColdShard(t *testing.T, index *Index) *LazyLoadShard {
	t.Helper()
	var lazy *LazyLoadShard
	index.shards.Range(func(name string, s ShardLike) error {
		ls, ok := s.(*LazyLoadShard)
		require.True(t, ok, "shard %q should be a LazyLoadShard", name)
		lazy = ls
		return nil
	})
	require.NotNil(t, lazy)
	require.False(t, lazy.isLoaded())
	return lazy
}

// deadlockStacks filters the dump to the cycle's goroutines; full dump if none match.
func deadlockStacks(full string) string {
	var kept []string
	for _, g := range strings.Split(full, "\n\n") {
		if strings.Contains(g, "updateReplicationConfig") ||
			strings.Contains(g, "LazyLoadShard") ||
			strings.Contains(g, "initNonVector") {
			kept = append(kept, g)
		}
	}
	if len(kept) == 0 {
		return full
	}
	return strings.Join(kept, "\n\n")
}

// Both operations succeed when they don't overlap — pins any deadlock-test
// failure on the interleaving rather than the fixture.
func TestUpdateReplicationConfig_SequentialWithLazyShard(t *testing.T) {
	ctx := context.Background()
	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigSequential")
	lazy := soleColdShard(t, index)

	require.NoError(t, lazy.Load(ctx))
	require.NoError(t, index.updateReplicationConfig(ctx, &models.ReplicationConfig{
		Factor: 1,
	}))
	require.NoError(t, repo.Shutdown(context.Background()))
}

// gateAllocChecker parks Load at CheckMappingAndReserve — inside Load's
// critical section (shard mutex held, before its config read) — giving the
// test a deterministic sync point.
type gateAllocChecker struct {
	entered chan struct{} // closed when Load reaches the gate
	release chan struct{} // closed by the test to let Load continue
}

func (g gateAllocChecker) CheckAlloc(int64) error { return nil }

func (g gateAllocChecker) CheckMappingAndReserve(int64, int) error {
	close(g.entered)
	<-g.release
	return nil
}

func (g gateAllocChecker) Refresh(bool) {}

// Pins the ABBA deadlock that wedged the RAFT FSM in prod (the UpdateClass
// apply never returns, so raft.Shutdown hangs on runFSM):
//
//	updateReplicationConfig: holds replicationConfigLock (W) -> wants LazyLoadShard.mutex (isLoaded)
//	LazyLoadShard.Load:      holds LazyLoadShard.mutex      -> wants replicationConfigLock (R via initNonVector)
//
// The interleaving is forced deterministically: Load is parked at the gate
// with the shard mutex held; a test-held read lock queues the updater's write
// (observable — a pending writer fails TryRLock); releasing both lets Load's
// config read collide with the fan-out. Every sync point fails the test loudly
// if it is not reached, so the test cannot pass without exercising the cycle.
func TestUpdateReplicationConfig_DeadlocksAgainstLazyShardLoad(t *testing.T) {
	const (
		syncTimeout     = 10 * time.Second
		deadlockTimeout = 15 * time.Second
	)
	ctx := context.Background()

	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigDeadlock")
	lazy := soleColdShard(t, index)

	gate := gateAllocChecker{entered: make(chan struct{}), release: make(chan struct{})}
	lazy.memMonitor = gate

	loadDone := make(chan error, 1)
	go func() { loadDone <- lazy.Load(ctx) }()

	select {
	case <-gate.entered:
	case <-time.After(syncTimeout):
		t.Fatal("Load never reached the gate inside its critical section — the fixture no longer exercises the interleaving")
	}

	// Queue the updater behind a test-held read lock so its write-lock request
	// is observably pending before Load is released.
	index.replicationConfigLock.RLock()

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- index.updateReplicationConfig(ctx, &models.ReplicationConfig{
			Factor: 1,
		})
	}()

	writerQueued := false
	for deadline := time.Now().Add(syncTimeout); time.Now().Before(deadline); {
		if !index.replicationConfigLock.TryRLock() {
			writerQueued = true // a pending writer blocks new readers
			break
		}
		index.replicationConfigLock.RUnlock()
		runtime.Gosched()
	}
	if !writerQueued {
		index.replicationConfigLock.RUnlock()
		t.Fatal("updateReplicationConfig never queued for the config write lock")
	}
	index.replicationConfigLock.RUnlock() // writer acquires the lock now
	close(gate.release)                   // Load proceeds into its config read

	loadOK, updateOK := false, false
	timeout := time.After(deadlockTimeout)
	for !loadOK || !updateOK {
		select {
		case err := <-loadDone:
			require.NoError(t, err)
			loadOK = true
		case err := <-updateDone:
			require.NoError(t, err)
			updateOK = true
		case <-timeout:
			buf := make([]byte, 1<<22)
			stacks := string(buf[:runtime.Stack(buf, true)])
			t.Fatalf("deadlock: updateReplicationConfig and LazyLoadShard.Load wedged for %s (load done: %v, update done: %v).\n\ninvolved goroutines:\n\n%s",
				deadlockTimeout, loadOK, updateOK, deadlockStacks(stacks))
		}
	}
	require.NoError(t, repo.Shutdown(context.Background()))
}
