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

const (
	deadlockSyncTimeout = 10 * time.Second
	deadlockTimeout     = 15 * time.Second
)

// No Shutdown cleanup: a reproduced deadlock would hang it forever.
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

func deadlockStacks(full string) string {
	var kept []string
	for _, g := range strings.Split(full, "\n\n") {
		if strings.Contains(g, "updateReplicationConfig") ||
			strings.Contains(g, "reconcileAsyncReplication") ||
			strings.Contains(g, "LazyLoadShard") ||
			strings.Contains(g, "initNonVector") ||
			strings.Contains(g, "ForEachLoadedShardConcurrently") {
			kept = append(kept, g)
		}
	}
	if len(kept) == 0 {
		return full
	}
	return strings.Join(kept, "\n\n")
}

// gateAllocChecker parks Load inside its critical section, before its config reads.
type gateAllocChecker struct {
	entered chan struct{}
	release chan struct{}
}

func (g gateAllocChecker) CheckAlloc(int64) error { return nil }

func (g gateAllocChecker) CheckMappingAndReserve(int64, int) error {
	close(g.entered)
	<-g.release
	return nil
}

func (g gateAllocChecker) Refresh(bool) {}

func startGatedLoad(t *testing.T, lazy *LazyLoadShard) (gateAllocChecker, chan error) {
	t.Helper()
	gate := gateAllocChecker{entered: make(chan struct{}), release: make(chan struct{})}
	lazy.memMonitor = gate

	loadDone := make(chan error, 1)
	go func() { loadDone <- lazy.Load(context.Background()) }()

	select {
	case <-gate.entered:
	case <-time.After(deadlockSyncTimeout):
		t.Fatal("Load never reached the gate inside its critical section — the fixture no longer exercises the interleaving")
	}
	return gate, loadDone
}

func requireBothComplete(t *testing.T, what string, loadDone, opDone chan error) {
	t.Helper()
	loadOK, opOK := false, false
	timeout := time.After(deadlockTimeout)
	for !loadOK || !opOK {
		select {
		case err := <-loadDone:
			require.NoError(t, err)
			loadOK = true
		case err := <-opDone:
			require.NoError(t, err)
			opOK = true
		case <-timeout:
			buf := make([]byte, 1<<22)
			stacks := string(buf[:runtime.Stack(buf, true)])
			t.Fatalf("deadlock: %s and LazyLoadShard.Load wedged for %s (load done: %v, op done: %v).\n\ninvolved goroutines:\n\n%s",
				what, deadlockTimeout, loadOK, opOK, deadlockStacks(stacks))
		}
	}
}

// Pins deadlock-test failures on the interleaving: both ops succeed when they don't overlap.
func TestUpdateReplicationConfig_SequentialWithLazyShard(t *testing.T) {
	ctx := context.Background()
	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigSequential")
	lazy := soleColdShard(t, index)

	require.NoError(t, lazy.Load(ctx))
	require.NoError(t, index.updateReplicationConfig(ctx, &models.ReplicationConfig{Factor: 1}))
	require.NoError(t, repo.Shutdown(context.Background()))
}

// Pins the ABBA cycle: the config write lock wants the shard mutex; a parked Load holds it wanting the config RLock.
func TestUpdateReplicationConfig_DeadlocksAgainstLazyShardLoad(t *testing.T) {
	ctx := context.Background()
	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigDeadlock")
	lazy := soleColdShard(t, index)

	gate, loadDone := startGatedLoad(t, lazy)

	// makes the updater's queued write observable below
	index.replicationConfigLock.RLock()

	updateDone := make(chan error, 1)
	go func() {
		updateDone <- index.updateReplicationConfig(ctx, &models.ReplicationConfig{Factor: 1})
	}()

	writerQueued := false
	for deadline := time.Now().Add(deadlockSyncTimeout); time.Now().Before(deadline); {
		if !index.replicationConfigLock.TryRLock() {
			writerQueued = true
			break
		}
		index.replicationConfigLock.RUnlock()
		runtime.Gosched()
	}
	if !writerQueued {
		index.replicationConfigLock.RUnlock()
		t.Fatal("updateReplicationConfig never queued for the config write lock")
	}
	index.replicationConfigLock.RUnlock()
	close(gate.release)

	requireBothComplete(t, "updateReplicationConfig", loadDone, updateDone)
	require.NoError(t, repo.Shutdown(context.Background()))
}

// Same cycle via the runtime-config hook path.
func TestReconcileAsyncReplication_DeadlocksAgainstLazyShardLoad(t *testing.T) {
	ctx := context.Background()
	repo, index := newReplConfigDeadlockFixture(t, "ReplConfigReconcileDeadlock")
	lazy := soleColdShard(t, index)

	gate, loadDone := startGatedLoad(t, lazy)

	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- index.reconcileAsyncReplication(ctx) }()

	fanOutBlocked := false
	buf := make([]byte, 1<<22)
	for deadline := time.Now().Add(deadlockSyncTimeout); time.Now().Before(deadline); {
		stacks := string(buf[:runtime.Stack(buf, true)])
		for _, g := range strings.Split(stacks, "\n\n") {
			if strings.Contains(g, "ForEachLoadedShardConcurrently") && strings.Contains(g, "isLoaded") {
				fanOutBlocked = true
				break
			}
		}
		if fanOutBlocked {
			break
		}
		runtime.Gosched()
	}
	if !fanOutBlocked {
		t.Fatal("reconcile fan-out never blocked on the mid-load shard — the fixture no longer exercises the interleaving")
	}
	close(gate.release)

	requireBothComplete(t, "reconcileAsyncReplication", loadDone, reconcileDone)
	require.NoError(t, repo.Shutdown(context.Background()))
}
