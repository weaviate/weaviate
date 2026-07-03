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

package rest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeReconcileEnqueuer struct {
	active   map[string]bool // "collection/target" -> in-flight
	enqueued []string        // "collection/target"
}

func (f *fakeReconcileEnqueuer) HasActiveDrop(ctx context.Context, collection, targetVector string) (bool, error) {
	return f.active[collection+"/"+targetVector], nil
}

func (f *fakeReconcileEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string) error {
	for _, t := range targets {
		f.enqueued = append(f.enqueued, collection+"/"+t)
	}
	return nil
}

func dropped() models.VectorConfig {
	return models.VectorConfig{VectorIndexType: modelsext.VectorIndexTypeNone}
}
func nonDropped() models.VectorConfig { return models.VectorConfig{VectorIndexType: "hnsw"} }

func TestReconcile_EnqueuesMissingTasks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	classes := []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped(), "keep": nonDropped()}},
		{Class: "B", VectorConfig: map[string]models.VectorConfig{"v2": dropped()}},
	}
	enq := &fakeReconcileEnqueuer{active: map[string]bool{}}

	reconcileDroppedVectorIndexes(context.Background(), classes, enq, logger)

	require.ElementsMatch(t, []string{"A/v1", "B/v2"}, enq.enqueued,
		"every dropped marker without a live task is enqueued; non-dropped vectors are skipped")
}

func TestReconcile_SkipsClassesWithLiveTasks(t *testing.T) {
	logger, _ := test.NewNullLogger()
	classes := []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped(), "v2": dropped()}},
	}
	enq := &fakeReconcileEnqueuer{active: map[string]bool{"A/v1": true}} // v1 already in flight

	reconcileDroppedVectorIndexes(context.Background(), classes, enq, logger)

	require.Equal(t, []string{"A/v2"}, enq.enqueued, "only the marker without a live task is enqueued")
}

type fakeClusterDropClient struct {
	tasks        map[string][]*distributedtask.Task
	gotNamespace string
	gotTaskID    string
	gotPayload   any
	gotSpecs     []distributedtask.UnitSpec
}

func (f *fakeClusterDropClient) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return f.tasks, nil
}

func (f *fakeClusterDropClient) AddDistributedTaskWithGroups(ctx context.Context, namespace, taskID string,
	taskPayload any, unitSpecs []distributedtask.UnitSpec,
) error {
	f.gotNamespace, f.gotTaskID, f.gotPayload, f.gotSpecs = namespace, taskID, taskPayload, unitSpecs
	return nil
}

// TestHasActiveDrop_MatchesActiveTaskByCollectionAndTarget exercises the real
// HasActiveDrop against the cluster task list: it matches an active task by
// collection (case-insensitive) and target, and ignores terminal tasks.
func TestHasActiveDrop_MatchesActiveTaskByCollectionAndTarget(t *testing.T) {
	active := &distributedtask.Task{
		Namespace:      db.DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t1", Version: 1},
		Payload:        mustDropPayload(t, "C", "v1"),
		Status:         distributedtask.TaskStatusStarted,
	}
	cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
		db.DropVectorIndexNamespace: {active},
	}}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster}

	got, err := enq.HasActiveDrop(context.Background(), "c", "v1") // collection case-insensitive
	require.NoError(t, err)
	require.True(t, got)

	got, err = enq.HasActiveDrop(context.Background(), "C", "v2") // different target
	require.NoError(t, err)
	require.False(t, got)

	active.Status = distributedtask.TaskStatusFinished // terminal → ignored
	got, err = enq.HasActiveDrop(context.Background(), "C", "v1")
	require.NoError(t, err)
	require.False(t, got)
}

// TestLiveOpIDs_ReturnsActiveOpIDs pins the sweep input: LiveOpIDs returns op IDs
// of active drop tasks and excludes terminal ones (whose ops should be swept).
func TestLiveOpIDs_ReturnsActiveOpIDs(t *testing.T) {
	active := &distributedtask.Task{
		Namespace: db.DropVectorIndexNamespace,
		Payload:   mustPayloadWithOp(t, "C", "opActive", "v1"),
		Status:    distributedtask.TaskStatusStarted,
	}
	done := &distributedtask.Task{
		Namespace: db.DropVectorIndexNamespace,
		Payload:   mustPayloadWithOp(t, "C", "opDone", "v2"),
		Status:    distributedtask.TaskStatusFinished,
	}
	cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
		db.DropVectorIndexNamespace: {active, done},
	}}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster}

	live, err := enq.LiveOpIDs(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"opActive": {}}, live)
}

func mustPayloadWithOp(t *testing.T, collection, opID string, targets ...string) []byte {
	t.Helper()
	b, err := json.Marshal(db.DropVectorIndexTaskPayload{Collection: collection, Targets: targets, OpID: opID})
	require.NoError(t, err)
	return b
}

func mustDropPayload(t *testing.T, collection string, targets ...string) []byte {
	t.Helper()
	b, err := json.Marshal(db.DropVectorIndexTaskPayload{Collection: collection, Targets: targets, OpID: "op"})
	require.NoError(t, err)
	return b
}

// fakeShardingState returns a leader-consistent-shaped sharding state built from
// shard -> (status, nodes).
type fakeShardingState struct {
	state *sharding.State
	err   error
}

func (f *fakeShardingState) QueryShardingState(class string) (*sharding.State, uint64, error) {
	return f.state, 0, f.err
}

func shardingState(partitioning bool, shards map[string]sharding.Physical) *sharding.State {
	return &sharding.State{PartitioningEnabled: partitioning, Physical: shards}
}

func TestActiveShardOwnership_FiltersByTenantStatus(t *testing.T) {
	t.Run("multi-tenant returns only HOT/ACTIVE tenants", func(t *testing.T) {
		state := shardingState(true, map[string]sharding.Physical{
			"hot":    {Status: models.TenantActivityStatusHOT, BelongsToNodes: []string{"n1"}},
			"active": {Status: models.TenantActivityStatusACTIVE, BelongsToNodes: []string{"n2"}},
			"cold":   {Status: models.TenantActivityStatusCOLD, BelongsToNodes: []string{"n1"}},
			"frozen": {Status: "FROZEN", BelongsToNodes: []string{"n2"}},
		})
		require.Equal(t, map[string][]string{"n1": {"hot"}, "n2": {"active"}}, activeShardOwnership(state),
			"COLD/FROZEN tenants must be excluded")
	})

	t.Run("non-multi-tenant returns all shards regardless of status", func(t *testing.T) {
		state := shardingState(false, map[string]sharding.Physical{
			"s1": {BelongsToNodes: []string{"n1"}},
			"s2": {BelongsToNodes: []string{"n1", "n2"}},
		})
		require.Equal(t, map[string][]string{"n1": {"s1", "s2"}, "n2": {"s2"}}, activeShardOwnership(state))
	})
}

// TestEnqueueDropVectorIndex_AllColdMultiTenant_NoOp: an MT collection whose
// tenants are all inactive yields an empty active-ownership map; the enqueuer
// must treat that as a no-op success (the drop marker is already applied), not
// an error.
func TestEnqueueDropVectorIndex_AllColdMultiTenant_NoOp(t *testing.T) {
	cluster := &fakeClusterDropClient{}
	state := &fakeShardingState{state: shardingState(true, map[string]sharding.Physical{
		"cold": {Status: models.TenantActivityStatusCOLD, BelongsToNodes: []string{"n1"}},
	})}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, shardingState: state}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	require.Empty(t, cluster.gotTaskID, "no task should be enqueued when there are no active shards")
}

// TestEnqueueDropVectorIndex_NoShardsNonMultiTenant_Errors confirms the empty
// no-op is scoped to MT: a non-MT collection with no shards is a real error.
func TestEnqueueDropVectorIndex_NoShardsNonMultiTenant_Errors(t *testing.T) {
	cluster := &fakeClusterDropClient{}
	state := &fakeShardingState{state: shardingState(false, map[string]sharding.Physical{})}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, shardingState: state}

	require.Error(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
}

// TestEnqueueDropVectorIndex_PayloadSurvivesClusterMarshal pins the encoding
// contract: the enqueuer must hand AddDistributedTaskWithGroups the payload
// struct, not pre-marshaled bytes (which the cluster layer would double-encode
// into a JSON string, breaking CheckConflict and the provider — the bug the
// drop endpoint hit in e2e).
func TestEnqueueDropVectorIndex_PayloadSurvivesClusterMarshal(t *testing.T) {
	cluster := &fakeClusterDropClient{}
	state := &fakeShardingState{state: shardingState(false, map[string]sharding.Physical{
		"shard1": {BelongsToNodes: []string{"node1"}},
	})}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, shardingState: state}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))

	require.Equal(t, db.DropVectorIndexNamespace, cluster.gotNamespace)
	require.NotEmpty(t, cluster.gotTaskID)
	require.NotEmpty(t, cluster.gotSpecs)

	// The cluster layer json.Marshals taskPayload; it must round-trip.
	raw, err := json.Marshal(cluster.gotPayload)
	require.NoError(t, err)
	var p db.DropVectorIndexTaskPayload
	require.NoError(t, json.Unmarshal(raw, &p))
	require.Equal(t, "C", p.Collection)
	require.Equal(t, []string{"v1"}, p.Targets)
	require.NotEmpty(t, p.OpID)
	require.Equal(t, "node1", p.UnitToNode["shard1__node1"])
	require.Equal(t, "shard1", p.UnitToShard["shard1__node1"])
}
