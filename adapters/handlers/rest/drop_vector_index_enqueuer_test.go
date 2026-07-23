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
	"strings"
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
	listed   int             // ListDistributedTasks calls (one per round)
}

func (f *fakeReconcileEnqueuer) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	f.listed++
	var tasks []*distributedtask.Task
	i := 0
	for key, inFlight := range f.active {
		if !inFlight {
			continue
		}
		collection, target, _ := strings.Cut(key, "/")
		b, err := json.Marshal(db.DropVectorIndexTaskPayload{
			Collection: collection, Targets: []string{target}, OpID: fmt.Sprintf("op-%d", i),
		})
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, &distributedtask.Task{
			Namespace:      db.DropVectorIndexNamespace,
			TaskDescriptor: distributedtask.TaskDescriptor{ID: fmt.Sprintf("t-%d", i), Version: uint64(i + 1)},
			Payload:        b,
			Status:         distributedtask.TaskStatusStarted,
		})
		i++
	}
	return map[string][]*distributedtask.Task{db.DropVectorIndexNamespace: tasks}, nil
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

// listErrEnqueuer fails the round's task-list fetch; nothing may be enqueued
// on unknown in-flight state.
type listErrEnqueuer struct {
	*fakeReconcileEnqueuer
}

func (f *listErrEnqueuer) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	return nil, fmt.Errorf("no leader")
}

func TestReconcile_ListError_SkipsRound(t *testing.T) {
	logger, _ := test.NewNullLogger()
	classes := []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped()}},
	}
	enq := &listErrEnqueuer{fakeReconcileEnqueuer: &fakeReconcileEnqueuer{active: map[string]bool{}}}

	reconcileDroppedVectorIndexes(context.Background(), classes, enq, logger)

	require.Empty(t, enq.enqueued, "an unreadable task list must skip the round, not enqueue blind")
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

func (p *probeRecordingEnqueuer) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	*p.probed = true
	return p.fakeReconcileEnqueuer.ListDistributedTasks(ctx)
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
	listErr      error
	gotNamespace string
	gotTaskID    string
	gotPayload   any
	gotSpecs     []distributedtask.UnitSpec
}

func (f *fakeClusterDropClient) ListDistributedTasks(ctx context.Context) (map[string][]*distributedtask.Task, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
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

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
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

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	require.Empty(t, cluster.gotTaskID, "no task should be enqueued when there are no active shards")
}

// TestEnqueueDropVectorIndex_NoShardsNonMultiTenant_Errors confirms the empty
// no-op is scoped to MT: a non-MT collection with no shards is a real error.
func TestEnqueueDropVectorIndex_NoShardsNonMultiTenant_Errors(t *testing.T) {
	cluster := &fakeClusterDropClient{}
	state := &fakeShardingState{state: shardingState(false, map[string]sharding.Physical{})}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

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
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

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

// epochTask builds a drop task record for target "v1" with the given epoch,
// raft version, status, own units, and inherited cleaned set.
func epochTask(t *testing.T, collection, id, epoch string, version uint64,
	status distributedtask.TaskStatus, unitShards, cleaned []string,
) *distributedtask.Task {
	t.Helper()
	unitToShard := map[string]string{}
	for i, shard := range unitShards {
		unitToShard[fmt.Sprintf("%s__u%d", shard, i)] = shard
	}
	b, err := json.Marshal(db.DropVectorIndexTaskPayload{
		Collection: collection, Targets: []string{"v1"}, OpID: "op-" + id,
		UnitToShard: unitToShard, DropEpochID: epoch, CleanedShards: cleaned,
	})
	require.NoError(t, err)
	return &distributedtask.Task{
		Namespace:      db.DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: version},
		Payload:        b,
		Status:         status,
	}
}

func corruptRecordTask(id string) *distributedtask.Task {
	return &distributedtask.Task{
		Namespace:      db.DropVectorIndexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 99},
		Payload:        []byte("not json"),
		Status:         distributedtask.TaskStatusFinished,
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

func TestSameTargetSet(t *testing.T) {
	tests := []struct {
		a, b []string
		want bool
	}{
		{[]string{"v1"}, []string{"v1"}, true},
		{[]string{"v1", "v2"}, []string{"v2", "v1"}, true},
		{[]string{"v1"}, []string{"v2"}, false},
		{[]string{"v1"}, []string{"v1", "v2"}, false},
		{[]string{"v1", "v2"}, []string{"v1", "v1"}, false}, // multiplicities matter
		{[]string{"v1"}, []string{"V1"}, false},             // case-sensitive identifiers
		{nil, nil, true},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, sameTargetSet(tt.a, tt.b), "%v vs %v", tt.a, tt.b)
	}
}

// TestEnqueueDropVectorIndex_TaskListError_Surfaces: coverage inheritance
// cannot be computed without the task list; enqueueing blind could re-clean or
// mint a wrong epoch, so the error surfaces for the caller to retry.
func TestEnqueueDropVectorIndex_TaskListError_Surfaces(t *testing.T) {
	cluster := &fakeClusterDropClient{listErr: fmt.Errorf("no leader")}
	state := &fakeShardingState{
		state: shardingState(false, map[string]sharding.Physical{
			"s1": {BelongsToNodes: []string{"n1"}},
		}),
		// A token-bearing marker: inheritance needs the task list, so the
		// list failure must surface (a pre-token marker skips the list).
		vectorCfg: map[string]models.VectorConfig{"v1": droppedWithEpoch("E1")},
	}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	err := enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"})
	require.Error(t, err)
	require.Empty(t, cluster.gotTaskID)
}

// droppedWithEpoch is a marker entry carrying the drop's generation token.
func droppedWithEpoch(epoch string) models.VectorConfig {
	return models.VectorConfig{
		VectorIndexType:   modelsext.VectorIndexTypeNone,
		VectorIndexConfig: map[string]any{modelsext.DropEpochIDKey: epoch},
	}
}

// TestEnqueueDropVectorIndex_GrownShardSetAfterFinalize pins the re-drop repro
// that broke the record-inference fence: the previous drop's FINISHED record
// covers everything that existed at its finalize, a shard was created since,
// and the marker belongs to a NEW drop. The marker's generation token makes
// the stale record unmistakable — the new task must carry the marker's epoch,
// inherit nothing, and clean every shard.
func TestEnqueueDropVectorIndex_GrownShardSetAfterFinalize(t *testing.T) {
	hot := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusHOT, BelongsToNodes: nodes}
	}
	cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
		db.DropVectorIndexNamespace: {
			epochTask(t, "C", "t1", "E1", 1, distributedtask.TaskStatusFinished, []string{"s1", "s2"}, nil),
		},
	}}
	state := &fakeShardingState{
		state: shardingState(true, map[string]sharding.Physical{
			"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1"), // s3 created after the finalize
		}),
		vectorCfg: map[string]models.VectorConfig{"v1": droppedWithEpoch("E2")},
	}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	p := decodeEnqueuedPayload(t, cluster)
	require.Equal(t, "E2", p.DropEpochID, "the task carries the marker's epoch, not the record's")
	require.Empty(t, p.CleanedShards, "stale coverage must not be inherited")
	require.Equal(t, map[string]string{"s1__n1": "s1", "s2__n1": "s2", "s3__n1": "s3"}, p.UnitToShard,
		"every shard must be cleaned by the new drop")
}

// TestEnqueueDropVectorIndex_CoverageInheritance pins the chain rules: the
// marker's generation token is the epoch; coverage is inherited only from
// completed task records carrying that same token. Cleaned shards get no
// unit; when every active shard is cleaned and the remainder is inactive,
// nothing is enqueued. A pre-token marker inherits nothing.
func TestEnqueueDropVectorIndex_CoverageInheritance(t *testing.T) {
	hot := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusHOT, BelongsToNodes: nodes}
	}
	cold := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusCOLD, BelongsToNodes: nodes}
	}
	fin := distributedtask.TaskStatusFinished

	tests := []struct {
		name        string
		markerEpoch string // "" = pre-token marker
		tasks       []*distributedtask.Task
		shards      map[string]sharding.Physical
		nonMT       bool
		wantEpoch   string // "" = fresh (must differ from every recorded epoch)
		wantCleaned []string
		wantUnits   map[string]string
		wantNoTask  bool
	}{
		{
			name:        "matching records: inherit coverage, skip cleaned shards",
			markerEpoch: "E1",
			tasks:       []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil)},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			name:        "a previous drop's records are not inherited (re-created + re-dropped name)",
			markerEpoch: "E2",
			tasks:       []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1", "s2"}, nil)},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch:   "E2",
			wantUnits:   map[string]string{"s1__n1": "s1", "s2__n1": "s2"},
		},
		{
			name:        "SWAPPING earlier task's coverage is inherited",
			markerEpoch: "E1",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, distributedtask.TaskStatusSwapping, []string{"s1"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			name:        "active same-epoch task contributes nothing",
			markerEpoch: "E1",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil),
				epochTask(t, "C", "t2", "E1", 2, distributedtask.TaskStatusStarted, []string{"s2"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2", "s3__n1": "s3"},
		},
		{
			name:        "FAILED task contributes nothing",
			markerEpoch: "E1",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil),
				epochTask(t, "C", "t2", "E1", 2, distributedtask.TaskStatusFailed, []string{"s2"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2", "s3__n1": "s3"},
		},
		{
			name:        "corrupt records are skipped, not fatal",
			markerEpoch: "E1",
			tasks: []*distributedtask.Task{
				corruptRecordTask("bad"),
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			name:        "inherited coverage is pruned to current shards",
			markerEpoch: "E1",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, []string{"sDeleted"}),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			name:        "a node whose every shard is cleaned gets no units",
			markerEpoch: "E1",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1", "s2"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n2"), "s4": cold("n2")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1", "s2"},
			wantUnits:   map[string]string{"s3__n2": "s3"},
		},
		{
			name:        "pre-token marker inherits nothing and mints a unique epoch",
			markerEpoch: "",
			tasks:       []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil)},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch:   "",
			wantUnits:   map[string]string{"s1__n1": "s1", "s2__n1": "s2"},
		},
		{
			name:        "foreign collection records are ignored",
			markerEpoch: "E1",
			tasks:       []*distributedtask.Task{epochTask(t, "Other", "t1", "E1", 1, fin, []string{"s1", "s2"}, nil)},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch:   "E1",
			wantUnits:   map[string]string{"s1__n1": "s1", "s2__n1": "s2"},
		},
		{
			// Non-MT shards carry no tenant status; the chain rules apply the same.
			name:        "non-MT collection: matching records inherit and skip cleaned",
			markerEpoch: "E1",
			tasks:       []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil)},
			shards:      map[string]sharding.Physical{"s1": {BelongsToNodes: []string{"n1"}}, "s2": {BelongsToNodes: []string{"n1"}}},
			nonMT:       true,
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			name:        "all active shards cleaned, remainder cold: defer without a task",
			markerEpoch: "E1",
			tasks:       []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil)},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": cold("n1")},
			wantNoTask:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
				db.DropVectorIndexNamespace: tt.tasks,
			}}
			marker := dropped()
			if tt.markerEpoch != "" {
				marker = droppedWithEpoch(tt.markerEpoch)
			}
			state := &fakeShardingState{
				state:     shardingState(!tt.nonMT, tt.shards),
				vectorCfg: map[string]models.VectorConfig{"v1": marker},
			}
			enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

			require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
			if tt.wantNoTask {
				require.Empty(t, cluster.gotTaskID, "nothing may be enqueued")
				return
			}
			p := decodeEnqueuedPayload(t, cluster)
			if tt.wantEpoch == "" {
				require.NotEmpty(t, p.DropEpochID)
				for _, task := range tt.tasks {
					prev, err := db.DecodeDropVectorIndexTaskPayload(task.Payload)
					if err != nil {
						continue
					}
					require.NotEqual(t, prev.DropEpochID, p.DropEpochID,
						"a fresh epoch must not continue any recorded epoch")
				}
			} else {
				require.Equal(t, tt.wantEpoch, p.DropEpochID)
			}
			require.Equal(t, tt.wantCleaned, p.CleanedShards)
			require.Equal(t, tt.wantUnits, p.UnitToShard)
		})
	}
}
