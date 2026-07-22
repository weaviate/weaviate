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
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	entschema "github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type fakeReconcileEnqueuer struct {
	active   map[string]bool // "collection/target" -> in-flight
	enqueued []string        // "collection/target"
}

func (f *fakeReconcileEnqueuer) HasActiveDrop(ctx context.Context, collection, targetVector string) (bool, error) {
	return f.active[collection+"/"+targetVector], nil
}

func (f *fakeReconcileEnqueuer) EnqueueDropVectorIndex(ctx context.Context, collection string, targets []string, freshEpoch bool) error {
	if freshEpoch {
		return fmt.Errorf("reconciliation must continue the current epoch, not mint a fresh one")
	}
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

// probeRecordingEnqueuer flags when the DTM probe has run, so the order test can
// assert the schema is read only afterwards.
type probeRecordingEnqueuer struct {
	*fakeReconcileEnqueuer
	probed *bool
}

func (p *probeRecordingEnqueuer) HasActiveDrop(ctx context.Context, collection, targetVector string) (bool, error) {
	*p.probed = true
	return p.fakeReconcileEnqueuer.HasActiveDrop(ctx, collection, targetVector)
}

// orderLister records whether the schema was read before or after the probe.
type orderLister struct {
	probed  *bool
	orderOK *bool
	classes []*models.Class
}

func (l orderLister) GetSchemaSkipAuth() entschema.Schema {
	if *l.probed {
		*l.orderOK = true
	}
	return entschema.Schema{Objects: &models.Schema{Classes: l.classes}}
}

// TestReconciliationAtStartup_ReadsSchemaAfterProbe pins the restore race fix:
// at startup the local schema is restored by the same background open the probe
// waits for, so reading it before the probe would see an empty/stale snapshot
// and silently skip markers.
func TestReconciliationAtStartup_ReadsSchemaAfterProbe(t *testing.T) {
	logger, _ := test.NewNullLogger()
	probed, orderOK := false, false
	enq := &probeRecordingEnqueuer{
		fakeReconcileEnqueuer: &fakeReconcileEnqueuer{active: map[string]bool{}},
		probed:                &probed,
	}
	lister := orderLister{probed: &probed, orderOK: &orderOK, classes: []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped()}},
	}}

	// One round: the loop exits via ctx after the first pass.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	runDropVectorIndexReconciliation(ctx, lister, enq, logger, time.Hour)

	require.True(t, orderOK, "schema must be read AFTER the DTM readiness probe")
	require.Equal(t, []string{"A/v1"}, enq.enqueued)
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
// HasActiveDrop against the cluster task list: collections match
// case-insensitively, targets match exactly (case-sensitive identifiers), and
// terminal tasks are ignored.
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

	got, err = enq.HasActiveDrop(context.Background(), "C", "V1") // case-differing target = different vector
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
// shard -> (status, nodes), and a class whose VectorConfig is vectorCfg (defaults
// to the targets-still-dropped happy path for "v1").
type fakeShardingState struct {
	state     *sharding.State
	vectorCfg map[string]models.VectorConfig
	err       error
}

func (f *fakeShardingState) QueryShardingState(class string) (*sharding.State, uint64, error) {
	return f.state, 0, f.err
}

func (f *fakeShardingState) QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error) {
	if f.err != nil {
		return nil, f.err
	}
	cfg := f.vectorCfg
	if cfg == nil {
		cfg = map[string]models.VectorConfig{"v1": dropped()}
	}
	out := map[string]versioned.Class{}
	for _, name := range classes {
		out[name] = versioned.Class{Class: &models.Class{Class: name, VectorConfig: cfg}}
	}
	return out, nil
}

// TestEnqueueDropVectorIndex_TargetNoLongerDropped_NoOp pins the enqueue-time
// guard: a target that the leader-consistent class shows live (class re-created,
// or a stale reconciliation snapshot) must not get a cleanup task.
func TestEnqueueDropVectorIndex_TargetNoLongerDropped_NoOp(t *testing.T) {
	cluster := &fakeClusterDropClient{}
	state := &fakeShardingState{
		state:     shardingState(false, map[string]sharding.Physical{"s1": {BelongsToNodes: []string{"n1"}}}),
		vectorCfg: map[string]models.VectorConfig{"v1": nonDropped()},
	}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))
	require.Empty(t, cluster.gotTaskID, "no task may be enqueued for a live vector")
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
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))
	require.Empty(t, cluster.gotTaskID, "no task should be enqueued when there are no active shards")
}

// TestEnqueueDropVectorIndex_NoShardsNonMultiTenant_Errors confirms the empty
// no-op is scoped to MT: a non-MT collection with no shards is a real error.
func TestEnqueueDropVectorIndex_NoShardsNonMultiTenant_Errors(t *testing.T) {
	cluster := &fakeClusterDropClient{}
	state := &fakeShardingState{state: shardingState(false, map[string]sharding.Physical{})}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	require.Error(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))
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
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))

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

type fakeMarkerFinalizer struct {
	collections []string
	finalized   [][]string
}

func (f *fakeMarkerFinalizer) RemoveDroppedVectorConfig(ctx context.Context, collection string, targets []string) error {
	f.collections = append(f.collections, collection)
	f.finalized = append(f.finalized, targets)
	return nil
}

// completedEpochTask builds a FINISHED task of the given epoch with units over
// unitShards and an inherited cleaned set, startedAt seconds after a fixed base.
func completedEpochTask(t *testing.T, id, collection, epoch string, unitShards, cleaned []string, startedOffsetSec int, targets ...string) *distributedtask.Task {
	t.Helper()
	unitToShard := map[string]string{}
	for i, shard := range unitShards {
		unitToShard[fmt.Sprintf("%s__u%d", shard, i)] = shard
	}
	b, err := json.Marshal(db.DropVectorIndexTaskPayload{
		Collection: collection, Targets: targets, OpID: "op-" + id,
		UnitToShard: unitToShard, DropEpochID: epoch, CleanedShards: cleaned,
	})
	require.NoError(t, err)
	return &distributedtask.Task{
		Namespace:      db.DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        b,
		Status:         distributedtask.TaskStatusFinished,
		StartedAt:      time.Unix(1700000000, 0).Add(time.Duration(startedOffsetSec) * time.Second),
	}
}

func decodeEnqueuedPayload(t *testing.T, cluster *fakeClusterDropClient) db.DropVectorIndexTaskPayload {
	t.Helper()
	raw, err := json.Marshal(cluster.gotPayload)
	require.NoError(t, err)
	var p db.DropVectorIndexTaskPayload
	require.NoError(t, json.Unmarshal(raw, &p))
	return p
}

// TestEnqueueDropVectorIndex_CoverageInheritance pins the cleaned-shard chain:
// a continued-epoch enqueue inherits coverage from completed same-epoch tasks,
// skips cleaned shards when building units, finalizes directly when coverage is
// complete, and never inherits across epochs or on a fresh drop.
func TestEnqueueDropVectorIndex_CoverageInheritance(t *testing.T) {
	mtState := func(shards map[string]sharding.Physical) *fakeShardingState {
		return &fakeShardingState{state: shardingState(true, shards)}
	}
	hot := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusHOT, BelongsToNodes: nodes}
	}
	cold := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusCOLD, BelongsToNodes: nodes}
	}

	t.Run("continued epoch inherits coverage and skips cleaned shards", func(t *testing.T) {
		cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
			db.DropVectorIndexNamespace: {
				completedEpochTask(t, "t1", "C", "E1", []string{"s1"}, nil, 0, "v1"),
			},
		}}
		state := mtState(map[string]sharding.Physical{
			"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1"),
		})
		enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

		require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))
		p := decodeEnqueuedPayload(t, cluster)
		require.Equal(t, "E1", p.DropEpochID, "the newest task's epoch continues")
		require.Equal(t, []string{"s1"}, p.CleanedShards)
		require.Equal(t, map[string]string{"s2__n1": "s2"}, p.UnitToShard,
			"cleaned s1 gets no unit; cold s3 is excluded as before")
	})

	t.Run("fresh drop mints a new epoch and ignores prior coverage", func(t *testing.T) {
		cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
			db.DropVectorIndexNamespace: {
				completedEpochTask(t, "t1", "C", "E1", []string{"s1", "s2"}, nil, 0, "v1"),
			},
		}}
		state := mtState(map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")})
		enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

		require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, true))
		p := decodeEnqueuedPayload(t, cluster)
		require.NotEqual(t, "E1", p.DropEpochID, "a re-created then re-dropped name must not continue the old epoch")
		require.Empty(t, p.CleanedShards)
		require.Len(t, p.UnitToShard, 2, "no coverage inherited: every active shard gets a unit")
	})

	t.Run("coverage complete with nothing to enqueue finalizes directly", func(t *testing.T) {
		cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
			db.DropVectorIndexNamespace: {
				completedEpochTask(t, "t1", "C", "E1", []string{"s1"}, []string{"s2"}, 0, "v1"),
			},
		}}
		state := mtState(map[string]sharding.Physical{"s1": hot("n1"), "s2": cold("n1")})
		finalizer := &fakeMarkerFinalizer{}
		enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state, finalizer: finalizer}

		require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))
		require.Empty(t, cluster.gotTaskID, "no unit-less task may be enqueued")
		require.Equal(t, []string{"C"}, finalizer.collections)
		require.Equal(t, [][]string{{"v1"}}, finalizer.finalized)
	})

	t.Run("all active shards cleaned but a shard uncovered defers without a task", func(t *testing.T) {
		cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
			db.DropVectorIndexNamespace: {
				completedEpochTask(t, "t1", "C", "E1", []string{"s1"}, nil, 0, "v1"),
			},
		}}
		state := mtState(map[string]sharding.Physical{"s1": hot("n1"), "s2": cold("n1")})
		finalizer := &fakeMarkerFinalizer{}
		enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state, finalizer: finalizer}

		require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))
		require.Empty(t, cluster.gotTaskID)
		require.Empty(t, finalizer.collections, "cold s2 was never cleaned; the marker must stay")
	})

	t.Run("coverage does not cross epochs", func(t *testing.T) {
		cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
			db.DropVectorIndexNamespace: {
				completedEpochTask(t, "t1", "C", "E1", []string{"s1", "s2"}, nil, 0, "v1"),
				completedEpochTask(t, "t2", "C", "E2", []string{"s2"}, nil, 10, "v1"), // newest → current epoch
			},
		}}
		state := mtState(map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")})
		enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

		require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))
		p := decodeEnqueuedPayload(t, cluster)
		require.Equal(t, "E2", p.DropEpochID)
		require.Equal(t, []string{"s2"}, p.CleanedShards, "E1's coverage must not leak into E2")
		require.Equal(t, map[string]string{"s1__n1": "s1"}, p.UnitToShard)
	})

	t.Run("chain-less records (older nodes or aged out) start a fresh epoch", func(t *testing.T) {
		cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
			db.DropVectorIndexNamespace: {
				completedEpochTask(t, "t1", "C", "", []string{"s1"}, nil, 0, "v1"), // no epoch
			},
		}}
		state := mtState(map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")})
		enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

		require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}, false))
		p := decodeEnqueuedPayload(t, cluster)
		require.NotEmpty(t, p.DropEpochID)
		require.Empty(t, p.CleanedShards)
		require.Len(t, p.UnitToShard, 2)
	})
}

func TestDropVectorReconcileIntervalFromEnv(t *testing.T) {
	logger, _ := test.NewNullLogger()

	t.Run("default when unset", func(t *testing.T) {
		t.Setenv("DROP_VECTOR_INDEX_RECONCILE_INTERVAL_SECONDS", "")
		require.Equal(t, dropVectorReconcileInterval, dropVectorReconcileIntervalFromEnv(logger))
	})

	t.Run("override", func(t *testing.T) {
		t.Setenv("DROP_VECTOR_INDEX_RECONCILE_INTERVAL_SECONDS", "5")
		require.Equal(t, 5*time.Second, dropVectorReconcileIntervalFromEnv(logger))
	})

	t.Run("invalid values fall back to the default", func(t *testing.T) {
		for _, raw := range []string{"garbage", "0", "-30"} {
			t.Setenv("DROP_VECTOR_INDEX_RECONCILE_INTERVAL_SECONDS", raw)
			require.Equal(t, dropVectorReconcileInterval, dropVectorReconcileIntervalFromEnv(logger), raw)
		}
	})
}
