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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	entreplication "github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/usecases/cluster"
	runtimecfg "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/dynsemaphore"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// TestIndexNilShardGuard verifies that Index.HashTreeLevel and
// Index.CompareDigests return a non-nil error when the requested shard is
// absent from the index's shards map.
//
// Before the fix both methods silently returned (nil, nil). The hashbeater
// interpreted an empty/nil levelDigests slice as "no differences found" and
// marked the target as fully compared, preventing any further propagation
// attempts. The fix changes the nil-shard guard from:
//
//	return nil, nil
//
// to:
//
//	return nil, fmt.Errorf("shard %q is not yet initialized on this node", shardName)
//
// so that the hashbeater treats the failure as a transient error and retries.
func TestIndexNilShardGuard(t *testing.T) {
	ctx := context.Background()
	const class = "NilShardGuardTest"

	_, idx := testShard(t, ctx, class)

	// nonexistent is guaranteed to be absent: testShard stores exactly one
	// shard under a random name; "nonexistent-shard" will never collide.
	const nonexistent = "nonexistent-shard"

	disc := hashtree.NewBitset(hashtree.NodesCount(1))
	disc.Set(0) // mark root as "to compare"

	t.Run("HashTreeLevelReturnsError", func(t *testing.T) {
		_, err := idx.HashTreeLevel(ctx, nonexistent, 0, disc)
		require.Error(t, err, "HashTreeLevel must return an error for a nil shard, not (nil, nil)")
		assert.Contains(t, err.Error(), "not yet initialized")
	})

	t.Run("CompareDigestsReturnsError", func(t *testing.T) {
		_, err := idx.CompareDigests(ctx, nonexistent, nil)
		require.Error(t, err, "CompareDigests must return an error for a nil shard, not (nil, nil)")
		assert.Contains(t, err.Error(), "not yet initialized")
	})
}

// nilThenEmptyClient simulates a remote node whose shard transitions from
// "not yet initialized" to "initialized but empty".
//
// While !initialized it returns an error from HashTreeLevel (reproducing
// the nil-shard state after a node restart during RAFT catch-up). After
// setInitialized() is called it returns all-zero digests from an empty
// HashTree, causing the source to detect a diff and propagate objects.
//
// Once objects are received via OverwriteObjects they are considered
// present on the target; CompareDigests will no longer report them as
// missing so the hashbeater stops re-propagating them after the first
// successful round.
type nilThenEmptyClient struct {
	FakeReplicationClient

	initialized atomic.Bool
	emptyHT     *hashtree.HashTree
	emptyBuf    []hashtree.Digest

	mu          sync.Mutex
	propagated  []string        // IDs received via OverwriteObjects
	receivedIDs map[string]bool // tracks which IDs have been propagated to this target
}

func newNilThenEmptyClient(t *testing.T, height int) *nilThenEmptyClient {
	t.Helper()
	ht, err := hashtree.NewHashTree(height)
	require.NoError(t, err)
	return &nilThenEmptyClient{
		emptyHT:     ht,
		emptyBuf:    make([]hashtree.Digest, hashtree.LeavesCount(height)),
		receivedIDs: make(map[string]bool),
	}
}

func (c *nilThenEmptyClient) setInitialized() { c.initialized.Store(true) }

func (c *nilThenEmptyClient) HashTreeLevel(
	_ context.Context, _, _, shard string,
	level int, discriminant *hashtree.Bitset,
) ([]hashtree.Digest, error) {
	if !c.initialized.Load() {
		return nil, fmt.Errorf("shard %q is not yet initialized on this node", shard)
	}
	// Return all-zero digests from the empty HashTree. This guarantees that
	// hashtree.LevelDiff sees a non-zero XOR against the source (which has
	// objects), causing the hashbeater to proceed to range scanning.
	count, err := c.emptyHT.Level(level, discriminant, c.emptyBuf)
	if err != nil {
		return nil, err
	}
	result := make([]hashtree.Digest, count)
	copy(result, c.emptyBuf[:count])
	return result, nil
}

func (c *nilThenEmptyClient) CompareDigests(
	_ context.Context, _, _, _ string,
	sourceDigests []routerTypes.RepairResponse,
) ([]routerTypes.RepairResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Report only objects that have not yet been received via OverwriteObjects
	// as missing (UpdateTime == 0). Once propagated they are considered
	// present on this target, so subsequent hashbeats find no diff and stop
	// re-propagating the same objects.
	var missing []routerTypes.RepairResponse
	for _, d := range sourceDigests {
		if !c.receivedIDs[d.ID] {
			missing = append(missing, routerTypes.RepairResponse{ID: d.ID, UpdateTime: 0})
		}
	}
	return missing, nil
}

func (c *nilThenEmptyClient) OverwriteObjects(
	_ context.Context, _, _, _ string,
	objs []*objects.VObject,
) ([]routerTypes.RepairResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, obj := range objs {
		if obj != nil && obj.LatestObject != nil {
			id := string(obj.LatestObject.ID)
			c.propagated = append(c.propagated, id)
			c.receivedIDs[id] = true
		}
	}
	return nil, nil
}

func (c *nilThenEmptyClient) propagatedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.propagated)
}

// withTwoNodeReplication returns an indexOpt that replaces the index's
// replicator with a two-node setup ("node1" local, "node2" target) backed by
// the supplied client. It also initialises asyncReplicationWorkersLimiter so
// that the hashbeater can acquire worker permits without panicking.
func withTwoNodeReplication(t *testing.T, client replica.Client) func(*Index) {
	t.Helper()
	return func(idx *Index) {
		logger, _ := test.NewNullLogger()

		const (
			localNode  = "node1"
			remoteNode = "node2"
			localAddr  = "http://node1:8080"
			remoteAddr = "http://node2:8080"
		)

		localReplica := routerTypes.Replica{NodeName: localNode, ShardName: "shard1", HostAddr: localAddr}
		remoteReplica := routerTypes.Replica{NodeName: remoteNode, ShardName: "shard1", HostAddr: remoteAddr}

		readReplicaSet := routerTypes.ReadReplicaSet{Replicas: []routerTypes.Replica{localReplica, remoteReplica}}
		readPlan := routerTypes.ReadRoutingPlan{
			LocalHostname: localAddr,
			ReplicaSet:    readReplicaSet,
		}

		mockRouter := routerTypes.NewMockRouter(t)
		mockRouter.EXPECT().
			GetWriteReplicasLocation(mock.Anything, mock.Anything, mock.Anything).
			Return(routerTypes.WriteReplicaSet{Replicas: []routerTypes.Replica{localReplica, remoteReplica}}, nil).
			Maybe()
		mockRouter.EXPECT().
			GetReadReplicasLocation(mock.Anything, mock.Anything, mock.Anything).
			Return(readReplicaSet, nil).
			Maybe()
		mockRouter.EXPECT().
			AllHostnames().
			Return([]string{localAddr, remoteAddr}).
			Maybe()
		// CollectShardDifferences uses BuildRoutingPlanOptions + BuildReadRoutingPlan.
		mockRouter.EXPECT().
			BuildRoutingPlanOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(routerTypes.RoutingPlanBuildOptions{}).
			Maybe()
		mockRouter.EXPECT().
			BuildReadRoutingPlan(mock.Anything).
			Return(readPlan, nil).
			Maybe()

		nodeResolver := cluster.NewMockNodeResolver(t)
		nodeResolver.EXPECT().NodeHostname(localNode).Return(localAddr, true).Maybe()
		nodeResolver.EXPECT().NodeHostname(remoteNode).Return(remoteAddr, true).Maybe()

		rep, err := replica.NewReplicator(
			idx.Config.ClassName.String(),
			mockRouter,
			nodeResolver,
			localNode,
			func() string { return models.ReplicationConfigDeletionStrategyNoAutomatedResolution },
			client,
			monitoring.GetMetrics(),
			logger,
		)
		require.NoError(t, err)
		idx.replicator = rep
		idx.router = mockRouter

		// asyncReplicationWorkersLimiter must be non-nil before the hashbeater
		// goroutine calls asyncReplicationWorkerAcquire.
		idx.asyncReplicationWorkersLimiter = dynsemaphore.NewDynamicWeighted(func() int64 { return 1 })

		// globalreplicationConfig must be non-nil: asyncReplicationGloballyDisabled
		// calls globalreplicationConfig.AsyncReplicationDisabled.Get() which panics
		// on a nil pointer if globalreplicationConfig is unset.
		idx.globalreplicationConfig = &entreplication.GlobalConfig{
			AsyncReplicationDisabled: runtimecfg.NewDynamicValue(false),
		}
	}
}

// TestAsyncRepairObjectInsertionNilShardRetry is the integration equivalent of
// the acceptance test TestAsyncRepairObjectInsertionScenario. It exercises the
// full async-replication path with a target whose shard starts nil (simulating
// the post-restart RAFT catch-up window) and verifies that:
//
//  1. The hashbeater retries when HashTreeLevel returns an error instead of
//     treating (nil, nil) as "no differences found".
//  2. All objects are propagated once the target shard becomes available.
func TestAsyncRepairObjectInsertionNilShardRetry(t *testing.T) {
	const (
		class  = "AsyncRepairNilShardRetry"
		n      = 10
		height = 1
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := newNilThenEmptyClient(t, height)
	sl, _ := testShard(t, ctx, class, withTwoNodeReplication(t, client))
	s, ok := sl.(*Shard)
	require.True(t, ok)

	// Insert N objects with deterministic UUIDs so the test is reproducible.
	for i := range n {
		id := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012x", i+1))
		require.NoError(t, sl.PutObject(ctx, testObjWithTime(class, id, int64(i+1))))
	}

	// DigestObjectsInRange (called inside objectsToPropagateWithinRange) uses a
	// disk-only cursor. Flush memtables so that all N objects are visible on disk.
	require.NoError(t, s.store.FlushMemtables(ctx))

	// Enable async replication with fast tick rates so the test completes
	// in seconds rather than the production 30 s interval.
	// aliveNodesCheckingFrequency is intentionally long so that nt.C does not
	// fire during the test (it would call setLastComparedNodes and interfere).
	cfg := AsyncReplicationConfig{
		hashtreeHeight:              height,
		frequency:                   2 * time.Second,
		frequencyWhilePropagating:   500 * time.Millisecond,
		aliveNodesCheckingFrequency: 10 * time.Minute,
		diffPerNodeTimeout:          10 * time.Second,
		prePropagationTimeout:       10 * time.Second,
		propagationTimeout:          10 * time.Second,
		propagationLimit:            1_000,
		propagationConcurrency:      1,
		propagationBatchSize:        100,
		diffBatchSize:               100,
		loggingFrequency:            1 * time.Second,
		initShieldCPUEveryN:         1,
	}
	require.NoError(t, s.SetAsyncReplicationState(ctx, cfg, true))
	t.Cleanup(func() {
		_ = s.SetAsyncReplicationState(context.Background(), cfg, false)
	})

	// AsyncReplicationEnabled() returns true only when ReplicationFactor > 1
	// AND AsyncReplicationEnabled is true. Without both flags, handleHashbeatWakeup
	// skips every beat. Set them now — after SetAsyncReplicationState has
	// initialised the hashtree with the correct config — so that initShard did
	// not call initAsyncReplication with a zero-value AsyncReplicationConfig.
	func() {
		s.index.replicationConfigLock.Lock()
		defer s.index.replicationConfigLock.Unlock()
		s.index.Config.ReplicationFactor = 2
		s.index.Config.AsyncReplicationEnabled = true
	}()

	// Wait for the hashtree init scan to complete so that the source's root
	// digest reflects all N on-disk objects before the first hashbeat.
	awaitHashtreeInitialized(t, s)

	// Give the first hashbeat time to fire and fail (target shard is nil →
	// HashTreeLevel returns error → backoff ≥ 1 s). 1.5 s is enough.
	time.Sleep(1500 * time.Millisecond)
	assert.Equal(t, 0, client.propagatedCount(),
		"no objects should be propagated while the target shard is nil")

	// Switch the target to "empty shard" phase. The next hashbeat (fired by
	// ft.C after cfg.frequency = 2 s) will now see a diff and propagate all
	// N objects via OverwriteObjects.
	client.setInitialized()

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.Equal(ct, n, client.propagatedCount(),
			"all %d objects must be propagated after target shard becomes available", n)
	}, 30*time.Second, 200*time.Millisecond,
		"objects were not asynchronously propagated after target shard became available")
}
