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
	"errors"
	"fmt"
	"slices"
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

func (f *fakeReconcileEnqueuer) EnqueueDropVectorIndexWithTasks(ctx context.Context, collection string,
	targets []string, _ map[string][]*distributedtask.Task,
) error {
	return f.EnqueueDropVectorIndex(ctx, collection, targets)
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
	require.Equal(t, 1, enq.listed,
		"the round fetches the task list once, not once per marker")
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
	runDropVectorIndexReconciliation(ctx, lister, enq, logger, time.Hour,
		func() bool { return true }, nil)

	require.True(t, orderOK, "schema must be read AFTER the DTM readiness probe")
	require.Equal(t, []string{"A/v1"}, enq.enqueued)
}

// TestReconciliation_FollowerSkipsRound pins the leader gate: every node runs
// the loop, but only the leader may submit — a follower's round would append a
// losing full-unit-map payload to the RAFT log before CheckConflict rejects it.
func TestReconciliation_FollowerSkipsRound(t *testing.T) {
	logger, _ := test.NewNullLogger()
	probed := false
	enq := &probeRecordingEnqueuer{
		fakeReconcileEnqueuer: &fakeReconcileEnqueuer{active: map[string]bool{}},
		probed:                &probed,
	}
	orderOK := false
	lister := orderLister{probed: &probed, orderOK: &orderOK, classes: []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped()}},
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	runDropVectorIndexReconciliation(ctx, lister, enq, logger, time.Hour,
		func() bool { return false }, nil)

	require.Empty(t, enq.enqueued, "a non-leader must not enqueue")
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

// TestLiveOpIDs_SpansRoundsWhileMarkerPending pins the liveness contract the
// strip-resume feature leans on: active rounds' ops are live, and a TERMINAL
// round's op stays live while its target's marker still stands (its pending
// set is the next round's resume point). Once the marker is gone — finalize,
// or a re-created live name — the op is sweepable. A failed leader read
// keeps ops alive (fail open: liveness feeds a destructive sweep).
func TestLiveOpIDs_SpansRoundsWhileMarkerPending(t *testing.T) {
	task := func(collection, op string, status distributedtask.TaskStatus, targets ...string) *distributedtask.Task {
		return &distributedtask.Task{
			Namespace: db.DropVectorIndexNamespace,
			Payload:   mustPayloadWithOp(t, collection, op, targets...),
			Status:    status,
		}
	}
	failed := distributedtask.TaskStatusFailed
	tasks := map[string][]*distributedtask.Task{db.DropVectorIndexNamespace: {
		task("C", "opActive", distributedtask.TaskStatusStarted, "v1"),
		task("C", "opFailedPending", failed, "v1"),                                            // marker for v1 stands → resume point
		task("C", "opDoneFinalized", distributedtask.TaskStatusFinished, "v2"),                // v2 absent from the class → finalized
		task("C", "opCancelledLive", distributedtask.TaskStatusCancelled, "v3"),               // v3 re-created live → fenced + sweepable
		task("C", "opSubsetPending", failed, "v3", "v1"),                                      // one target live, one still marked → keep
		task("Gone", "opClassGone", failed, "v1"),                                             // whole class deleted → nothing to resume
		{Namespace: db.DropVectorIndexNamespace, Payload: []byte("not json"), Status: failed}, // undecodable → skipped, not fatal
	}}
	cluster := &fakeClusterDropClient{tasks: tasks}
	state := &fakeShardingState{
		vectorCfg: map[string]models.VectorConfig{
			"v1": dropped(),
			"v3": {VectorIndexType: "hnsw"},
		},
		missingClasses: []string{"Gone"},
	}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	live, err := enq.LiveOpIDs(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{
		"opActive":        {},
		"opFailedPending": {},
		"opSubsetPending": {},
	}, live)
	require.Equal(t, 2, state.classReads,
		"one leader read per collection per call — terminal records share it, they don't fan out")

	t.Run("leader read failure keeps terminal ops alive", func(t *testing.T) {
		state := &fakeShardingState{err: errors.New("no leader")}
		enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}
		live, err := enq.LiveOpIDs(context.Background())
		require.NoError(t, err)
		require.Contains(t, live, "opFailedPending")
		require.Contains(t, live, "opDoneFinalized",
			"an unverifiable marker must not authorize a sweep")
		require.Equal(t, 2, state.classReads,
			"a failed read is memoized too: a partitioned leader costs one RPC per collection, not one per record")
	})

	t.Run("no tasks at all: empty non-nil set means sweep everything", func(t *testing.T) {
		enq := &dropVectorIndexEnqueuer{
			clusterService: &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{}},
			schemaState:    &fakeShardingState{},
		}
		live, err := enq.LiveOpIDs(context.Background())
		require.NoError(t, err)
		require.NotNil(t, live, "nil means 'liveness unknown, sweep disabled' — a clean empty answer must not be conflated with it")
		require.Empty(t, live)
	})
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
	// missingClasses lists collection names QueryReadOnlyClasses reports as
	// absent (deleted class). Without it the fake fabricates a class for ANY
	// name, and the class-gone sweep arm of LiveOpIDs is untestable.
	missingClasses []string
	classReads     int
	err            error
}

func (f *fakeShardingState) QueryShardingState(class string) (*sharding.State, uint64, error) {
	return f.state, 0, f.err
}

func (f *fakeShardingState) QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error) {
	f.classReads++
	if f.err != nil {
		return nil, f.err
	}
	cfg := f.vectorCfg
	if cfg == nil {
		cfg = map[string]models.VectorConfig{"v1": dropped()}
	}
	out := map[string]versioned.Class{}
	for _, name := range classes {
		if slices.Contains(f.missingClasses, name) {
			continue
		}
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

// TestEnqueueDropVectorIndex_ZeroTenants_FinalizesDirectly pins the escape
// for MT collections with NO tenants at all (never created, or all deleted
// after the marker landed): no cleanup task can ever exist to drive the
// finalize, so the enqueuer removes the dropped entries directly — the FSM
// removal gate allows the empty-shard-set case for exactly this. Tenants
// that merely exist-but-inactive must NOT trigger it.
func TestEnqueueDropVectorIndex_ZeroTenants_FinalizesDirectly(t *testing.T) {
	coldTenant := map[string]sharding.Physical{
		"cold": {Status: models.TenantActivityStatusCOLD, BelongsToNodes: []string{"n1"}},
	}
	tests := []struct {
		name          string
		shards        map[string]sharding.Physical
		finalizer     *fakeEnqueuerFinalizer // nil = not wired
		wantErr       string
		wantFinalized bool
	}{
		{
			name:          "no tenants: direct finalize, no task",
			shards:        map[string]sharding.Physical{},
			finalizer:     &fakeEnqueuerFinalizer{},
			wantFinalized: true,
		},
		{
			// A cold tenant's data must not be stranded by a premature finalize.
			name:      "inactive tenants exist: marker stays, no finalize",
			shards:    coldTenant,
			finalizer: &fakeEnqueuerFinalizer{},
		},
		{
			// A wiring regression must surface as an error, not a 200 over
			// a marker nothing can ever remove.
			name:    "no finalizer wired: error, not silent success",
			shards:  map[string]sharding.Physical{},
			wantErr: "no finalizer is wired",
		},
		{
			name:      "finalize failure surfaces",
			shards:    map[string]sharding.Physical{},
			finalizer: &fakeEnqueuerFinalizer{err: errors.New("gate refused")},
			wantErr:   "gate refused",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &fakeClusterDropClient{}
			state := &fakeShardingState{state: shardingState(true, tt.shards)}
			enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}
			if tt.finalizer != nil {
				enq.finalizer = tt.finalizer
			}

			err := enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Empty(t, cluster.gotTaskID, "no task may be enqueued on the zero/inactive-tenant paths")
			if tt.finalizer == nil {
				return
			}
			if tt.wantFinalized {
				require.Equal(t, [][]string{{"v1"}}, tt.finalizer.calls, "the marker must be finalized directly")
			} else {
				require.Empty(t, tt.finalizer.calls)
			}
		})
	}
}

type fakeEnqueuerFinalizer struct {
	calls [][]string
	err   error
}

func (f *fakeEnqueuerFinalizer) RemoveDroppedVectorConfig(_ context.Context, _ string, targets []string) error {
	f.calls = append(f.calls, targets)
	return f.err
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
	require.Equal(t, p.DropEpochID, p.OpID,
		"op identity is the drop epoch, not a per-round value — resume depends on it")
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

// failedEpochTask is epochTask with TaskStatusFailed and explicit per-unit
// terminal statuses: completedShards' units are COMPLETED, otherShards' are
// FAILED — the shape a mid-round tenant deactivation leaves behind.
func failedEpochTask(t *testing.T, collection, id, epoch string, version uint64,
	completedShards, otherShards []string,
) *distributedtask.Task {
	t.Helper()
	all := append(append([]string{}, completedShards...), otherShards...)
	task := epochTask(t, collection, id, epoch, version, distributedtask.TaskStatusFailed, all, nil)
	task.Units = map[string]*distributedtask.Unit{}
	for i, shard := range all {
		status := distributedtask.UnitStatusFailed
		if i < len(completedShards) {
			status = distributedtask.UnitStatusCompleted
		}
		task.Units[fmt.Sprintf("%s__u%d", shard, i)] = &distributedtask.Unit{Status: status}
	}
	return task
}

// failedEpochTaskInheriting is failedEpochTask carrying an inherited cleaned
// set — the shape a later round leaves behind when its own finalize is vetoed
// after its units completed.
func failedEpochTaskInheriting(t *testing.T, collection, id, epoch string, version uint64,
	completedShards, otherShards, cleaned []string,
) *distributedtask.Task {
	t.Helper()
	task := failedEpochTask(t, collection, id, epoch, version, completedShards, otherShards)
	var p db.DropVectorIndexTaskPayload
	require.NoError(t, json.Unmarshal(task.Payload, &p))
	p.CleanedShards = cleaned
	b, err := json.Marshal(p)
	require.NoError(t, err)
	task.Payload = b
	return task
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
		require.Equal(t, tt.want, db.SameTargetSet(tt.a, tt.b), "%v vs %v", tt.a, tt.b)
	}
}

// TestEnqueueDropVectorIndex_TaskListError_Surfaces: coverage inheritance
// cannot be computed without the task list; enqueueing blind could re-clean or
// mint a wrong epoch, so the error surfaces for the caller to retry.
func TestEnqueueDropVectorIndex_TaskListError_Surfaces(t *testing.T) {
	cluster := &fakeClusterDropClient{listErr: fmt.Errorf("no leader")}
	state := &fakeShardingState{state: shardingState(false, map[string]sharding.Physical{
		"s1": {BelongsToNodes: []string{"n1"}},
	})}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	err := enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"})
	require.Error(t, err)
	require.Empty(t, cluster.gotTaskID)
}

// NOTE on the grown-shard-set re-drop hazard: given a STALE record (a
// finalized previous drop of a re-created name) plus a shard created since,
// this enqueuer's fence alone would mis-inherit — the state is locally
// indistinguishable from an in-progress chain. Soundness comes from upstream:
// the schema FSM purges the previous drop's records in the same raft apply
// that introduces a new marker (see
// TestSchemaManager_UpdateClass_MarkerIntroductionPurgesRecords and the
// re-drop e2e), so stale records cannot exist next to a marker they don't
// belong to.

// TestEnqueueDropVectorIndex_CoverageInheritance pins the chain rules: inherit
// cleaned-shard coverage only from completed same-epoch tasks of an INCOMPLETE
// chain — a complete chain next to a marker is a closed epoch's residue
// (re-created then re-dropped name, or a missed finalize) and must start a
// fresh epoch with a full re-clean. Cleaned shards get no unit; when every
// active shard is cleaned and the remainder is inactive, nothing is enqueued.
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
		tasks       []*distributedtask.Task
		shards      map[string]sharding.Physical
		nonMT       bool
		wantEpoch   string // "" = fresh (must differ from every recorded epoch)
		wantCleaned []string
		wantUnits   map[string]string
		wantNoTask  bool
	}{
		{
			name:        "incomplete chain: inherit coverage, skip cleaned shards",
			tasks:       []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil)},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			// The P0 re-create pin: a chain that covers every current shard can
			// only be a finalized (closed) epoch's residue — trusting it would
			// finalize a re-dropped name with nothing stripped.
			name:      "complete chain is closed-epoch residue: fresh epoch, full re-clean",
			tasks:     []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1", "s2"}, nil)},
			shards:    map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch: "",
			wantUnits: map[string]string{"s1__n1": "s1", "s2__n1": "s2"},
		},
		{
			name: "active same-epoch task contributes nothing",
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
			// One deactivation fails a round; its FINISHED work must survive.
			name: "a FAILED round's COMPLETED units are inherited",
			tasks: []*distributedtask.Task{
				failedEpochTask(t, "C", "tf", "E1", 1, []string{"s1"}, []string{"s2"}),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2", "s3__n1": "s3"},
		},
		{
			// A quarantine on an inherited-coverage shard (s1) is unreachable
			// within its epoch: no later round arms s1, so nothing refreshes its
			// retry budget, and s1's rows veto the finalize of the round that
			// completes s2. That veto fails the round (bounded callback retries)
			// rather than looping — and two FAILED rounds whose COMPLETED units
			// together span every shard read as closed-epoch residue. So the epoch
			// closes and s1 IS re-armed, under a fresh epoch whose new op takes a
			// new snapshot with a new budget. Note t2 claims s1 as CleanedShards
			// and still does not vouch it: a terminal round vouches only the work
			// it finished, which is exactly what lets the chain read as complete
			// here rather than inheriting forward.
			name: "two FAILED rounds spanning every shard close the epoch and re-arm the cleaned shard",
			tasks: []*distributedtask.Task{
				failedEpochTask(t, "C", "t1", "E1", 1, []string{"s1"}, []string{"s2"}),
				failedEpochTaskInheriting(t, "C", "t2", "E1", 2, []string{"s2"}, nil, []string{"s1"}),
			},
			shards:    map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch: "",
			wantUnits: map[string]string{"s1__n1": "s1", "s2__n1": "s2"},
		},
		{
			name: "a FAILED round's completed units do not cross epochs",
			tasks: []*distributedtask.Task{
				failedEpochTask(t, "C", "tf", "E1", 1, []string{"s1"}, nil),
				epochTask(t, "C", "t2", "E2", 2, fin, []string{"s2"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1")},
			wantEpoch:   "E2",
			wantCleaned: []string{"s2"},
			wantUnits:   map[string]string{"s1__n1": "s1", "s3__n1": "s3"},
		},
		{
			name: "a FAILED round with no completed units contributes nothing",
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
			name: "coverage does not cross epochs",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1", "s2"}, nil),
				epochTask(t, "C", "t2", "E2", 2, fin, []string{"s2"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch:   "E2",
			wantCleaned: []string{"s2"},
			wantUnits:   map[string]string{"s1__n1": "s1"},
		},
		{
			name: "current epoch picked by raft version, not record order",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t2", "E2", 7, fin, []string{"s2"}, nil),
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1", "s2"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch:   "E2",
			wantCleaned: []string{"s2"},
			wantUnits:   map[string]string{"s1__n1": "s1"},
		},
		{
			name:      "chain-less records (older nodes or aged out) start a fresh epoch",
			tasks:     []*distributedtask.Task{epochTask(t, "C", "t1", "", 1, fin, []string{"s1"}, nil)},
			shards:    map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch: "",
			wantUnits: map[string]string{"s1__n1": "s1", "s2__n1": "s2"},
		},
		{
			name:      "foreign collection records are ignored",
			tasks:     []*distributedtask.Task{epochTask(t, "Other", "t1", "E1", 1, fin, []string{"s1", "s2"}, nil)},
			shards:    map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
			wantEpoch: "",
			wantUnits: map[string]string{"s1__n1": "s1", "s2__n1": "s2"},
		},
		{
			// Rule-9 pin for TaskStatus.IsCompleted's SWAPPING half at this level.
			name: "SWAPPING earlier task's coverage is inherited",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, distributedtask.TaskStatusSwapping, []string{"s1"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			name: "corrupt records are skipped, not fatal",
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
			name: "inherited coverage is pruned to current shards",
			tasks: []*distributedtask.Task{
				// sDeleted's tenant is gone; it must not ride along in payloads forever.
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, []string{"sDeleted"}),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			name: "a node whose every shard is cleaned gets no units",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1", "s2"}, nil),
			},
			shards:      map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n2"), "s4": cold("n2")},
			wantEpoch:   "E1",
			wantCleaned: []string{"s1", "s2"},
			wantUnits:   map[string]string{"s3__n2": "s3"},
		},
		{
			// Non-MT shards carry no tenant status; the chain rules apply the same.
			name:        "non-MT collection: incomplete chain inherits and skips cleaned",
			tasks:       []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil)},
			shards:      map[string]sharding.Physical{"s1": {BelongsToNodes: []string{"n1"}}, "s2": {BelongsToNodes: []string{"n1"}}},
			nonMT:       true,
			wantEpoch:   "E1",
			wantCleaned: []string{"s1"},
			wantUnits:   map[string]string{"s2__n1": "s2"},
		},
		{
			name:       "all active shards cleaned, remainder cold: defer without a task",
			tasks:      []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil)},
			shards:     map[string]sharding.Physical{"s1": hot("n1"), "s2": cold("n1")},
			wantNoTask: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
				db.DropVectorIndexNamespace: tt.tasks,
			}}
			state := &fakeShardingState{state: shardingState(!tt.nonMT, tt.shards)}
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
					require.NoError(t, err)
					require.NotEqual(t, prev.DropEpochID, p.DropEpochID,
						"a fresh epoch must not continue any recorded epoch")
				}
			} else {
				require.Equal(t, tt.wantEpoch, p.DropEpochID)
			}
			// Every recorded sibling here carries OpID "op-<id>" != epoch —
			// the pre-upgrade per-round shape — so this also pins the rolling
			// upgrade: inheriting an epoch from an old record yields the EPOCH
			// as the new op id, never the old record's OpID. uuid-recorded
			// progress is not resumed (a fresh snapshot re-covers it); the
			// stale uuid op is swept once the marker falls.
			require.Equal(t, p.DropEpochID, p.OpID,
				"every round of one drop must run under the epoch as its op ID, or a re-arm cannot resume the recorded pending set")
			require.Equal(t, tt.wantCleaned, p.CleanedShards)
			require.Equal(t, tt.wantUnits, p.UnitToShard)
		})
	}
}

// deferringEpochTask is epochTask that also recorded shards the round did not
// cover — what a round leaves behind when a tenant was inactive at enqueue.
func deferringEpochTask(t *testing.T, collection, id, epoch string, version uint64,
	status distributedtask.TaskStatus, unitShards, deferred []string,
) *distributedtask.Task {
	t.Helper()
	task := epochTask(t, collection, id, epoch, version, status, unitShards, nil)
	var p db.DropVectorIndexTaskPayload
	require.NoError(t, json.Unmarshal(task.Payload, &p))
	p.DeferredShards = deferred
	b, err := json.Marshal(p)
	require.NoError(t, err)
	task.Payload = b
	return task
}

// TestEnqueueDropVectorIndex_DeletedDeferredTenant_FinalizesWithoutReclean
// pins the tenant-deletion path: round one cleaned the hot tenants and
// recorded owing the cold one; deleting that tenant leaves the recorded
// coverage spanning every shard that still exists, so the drop finalizes on it
// instead of minting a fresh epoch and re-stripping shards already clean.
func TestEnqueueDropVectorIndex_DeletedDeferredTenant_FinalizesWithoutReclean(t *testing.T) {
	hot := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusHOT, BelongsToNodes: nodes}
	}
	cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
		db.DropVectorIndexNamespace: {
			deferringEpochTask(t, "C", "t1", "E1", 1,
				distributedtask.TaskStatusFinished, []string{"s1", "s2"}, []string{"s3"}),
		},
	}}
	// s3 deleted since round one.
	state := &fakeShardingState{
		state:     shardingState(true, map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")}),
		vectorCfg: map[string]models.VectorConfig{"v1": dropped()},
	}
	finalizer := &fakeEnqueuerFinalizer{}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state, finalizer: finalizer}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	require.Empty(t, cluster.gotTaskID,
		"no cleanup round may be enqueued: every remaining shard is already clean")
	require.Equal(t, [][]string{{"v1"}}, finalizer.calls,
		"the marker must be removed on the recorded coverage")
}

// TestEnqueueDropVectorIndex_CompleteChainOwedNothing_ReclansInstead is the
// safety twin: the same complete-coverage shape, but the chain owed nothing.
// That is indistinguishable from a finalized drop's residue beside a
// re-created name's marker, so it must re-clean rather than finalize.
func TestEnqueueDropVectorIndex_CompleteChainOwedNothing_ReclansInstead(t *testing.T) {
	hot := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusHOT, BelongsToNodes: nodes}
	}
	cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
		db.DropVectorIndexNamespace: {
			epochTask(t, "C", "t1", "E1", 1,
				distributedtask.TaskStatusFinished, []string{"s1", "s2"}, nil),
		},
	}}
	state := &fakeShardingState{
		state:     shardingState(true, map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")}),
		vectorCfg: map[string]models.VectorConfig{"v1": dropped()},
	}
	finalizer := &fakeEnqueuerFinalizer{}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state, finalizer: finalizer}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	require.Empty(t, finalizer.calls, "a chain that owed nothing must never finalize a standing marker")
	p := decodeEnqueuedPayload(t, cluster)
	require.NotEqual(t, "E1", p.DropEpochID, "closed-epoch residue starts a fresh epoch")
	require.Equal(t, map[string]string{"s1__n1": "s1", "s2__n1": "s2"}, p.UnitToShard)
}

// TestEnqueueDropVectorIndex_RecordsDeferredShards pins that a round writes
// down what it did not cover; without it a later round cannot tell a deleted
// tenant's vanished work from a chain that owed nothing.
func TestEnqueueDropVectorIndex_RecordsDeferredShards(t *testing.T) {
	hot := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusHOT, BelongsToNodes: nodes}
	}
	cold := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusCOLD, BelongsToNodes: nodes}
	}
	cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{}}
	state := &fakeShardingState{
		state: shardingState(true, map[string]sharding.Physical{
			"s1": hot("n1"), "s2": hot("n1"), "s3": cold("n1"), "s4": cold("n1"),
		}),
		vectorCfg: map[string]models.VectorConfig{"v1": dropped()},
	}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	p := decodeEnqueuedPayload(t, cluster)
	require.Equal(t, []string{"s3", "s4"}, p.DeferredShards,
		"the cold tenants are the work this round still owes")
}

// TestEnqueueDropVectorIndex_BatchesLargeCollections pins the round cap: a
// collection above maxShardsPerDropRound gets units only for the first
// (sorted) batch; the rest chains through follow-up rounds via CleanedShards
// inheritance, and the LAST batch's task still covers everyone for finalize.
func TestEnqueueDropVectorIndex_BatchesLargeCollections(t *testing.T) {
	prev := maxShardsPerDropRound
	maxShardsPerDropRound = 2
	defer func() { maxShardsPerDropRound = prev }()

	hot := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusHOT, BelongsToNodes: nodes}
	}
	cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{}}
	state := &fakeShardingState{
		state: shardingState(true, map[string]sharding.Physical{
			"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1"), "s4": hot("n1"),
		}),
		vectorCfg: map[string]models.VectorConfig{"v1": dropped()},
	}
	enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}

	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	p := decodeEnqueuedPayload(t, cluster)
	require.Equal(t, map[string]string{"s1__n1": "s1", "s2__n1": "s2"}, p.UnitToShard,
		"only the first sorted batch is armed this round")

	// Round 2: round 1 completed; inheritance covers its batch, the next
	// sorted batch is armed.
	cluster.tasks = map[string][]*distributedtask.Task{
		db.DropVectorIndexNamespace: {
			epochTask(t, "C", "t1", p.DropEpochID, 1, distributedtask.TaskStatusFinished, []string{"s1", "s2"}, nil),
		},
	}
	require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
	p2 := decodeEnqueuedPayload(t, cluster)
	require.Equal(t, p.DropEpochID, p2.DropEpochID, "batches share the drop's epoch")
	require.Equal(t, []string{"s1", "s2"}, p2.CleanedShards)
	require.Equal(t, map[string]string{"s3__n1": "s3", "s4__n1": "s4"}, p2.UnitToShard,
		"the last batch's units plus inherited cleaned shards cover everyone")
}

// TestReconciliation_NudgeWakesBeforeInterval pins the batch-chain latency
// fix: a nudge (round ended with work remaining) triggers the next round
// immediately instead of idling a full reconcile interval.
func TestReconciliation_NudgeWakesBeforeInterval(t *testing.T) {
	logger, _ := test.NewNullLogger()
	probed := false
	enq := &probeRecordingEnqueuer{
		fakeReconcileEnqueuer: &fakeReconcileEnqueuer{active: map[string]bool{}},
		probed:                &probed,
	}
	orderOK := false
	lister := orderLister{probed: &probed, orderOK: &orderOK, classes: []*models.Class{
		{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped()}},
	}}

	prevDelay := dropVectorNudgeDelay
	dropVectorNudgeDelay = time.Millisecond
	defer func() { dropVectorNudgeDelay = prevDelay }()

	nudge := make(chan struct{}, 1)
	nudge <- struct{}{}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	// Interval far beyond the ctx timeout: a second round can only come from
	// the nudge.
	runDropVectorIndexReconciliation(ctx, lister, enq, logger, time.Hour,
		func() bool { return true }, nudge)

	require.GreaterOrEqual(t, len(enq.enqueued), 2, "the nudge must trigger a follow-up round within the interval")
}

// TestEnqueuerGuardConsistency pins the two implementations of the
// inheritance rule against each other: whatever coverage claim the enqueuer
// composes, the AddTask-apply guard (CheckConflict's TOCTOU re-proof over the
// same records) must accept. The rule lives in epochAndInheritedCoverage AND
// in the guard; nothing else ties them together, and silent drift would
// reject every follow-up round (livelock) or accept unprovable claims.
func TestEnqueuerGuardConsistency(t *testing.T) {
	hot := func(nodes ...string) sharding.Physical {
		return sharding.Physical{Status: models.TenantActivityStatusHOT, BelongsToNodes: nodes}
	}
	fin := distributedtask.TaskStatusFinished

	scenarios := []struct {
		name   string
		tasks  []*distributedtask.Task
		shards map[string]sharding.Physical
	}{
		{
			name:   "completed record chain",
			tasks:  []*distributedtask.Task{epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil)},
			shards: map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1")},
		},
		{
			name: "chain with inherited cleaned set",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil),
				epochTask(t, "C", "t2", "E1", 2, fin, []string{"s2"}, []string{"s1"}),
			},
			shards: map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1")},
		},
		{
			name: "failed round with completed units",
			tasks: []*distributedtask.Task{
				failedEpochTask(t, "C", "tf", "E1", 1, []string{"s1"}, []string{"s2"}),
			},
			shards: map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1")},
		},
		{
			name: "mixed completed records and failed-round units",
			tasks: []*distributedtask.Task{
				epochTask(t, "C", "t1", "E1", 1, fin, []string{"s1"}, nil),
				failedEpochTask(t, "C", "tf", "E1", 2, []string{"s2"}, []string{"s3"}),
			},
			shards: map[string]sharding.Physical{"s1": hot("n1"), "s2": hot("n1"), "s3": hot("n1"), "s4": hot("n1")},
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{
				db.DropVectorIndexNamespace: sc.tasks,
			}}
			state := &fakeShardingState{
				state:     shardingState(true, sc.shards),
				vectorCfg: map[string]models.VectorConfig{"v1": dropped()},
			}
			enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state}
			require.NoError(t, enq.EnqueueDropVectorIndex(context.Background(), "C", []string{"v1"}))
			payload := decodeEnqueuedPayload(t, cluster)

			// Re-prove the enqueuer's claim exactly as the raft apply would.
			logger, _ := test.NewNullLogger()
			provider := db.NewDropVectorIndexProvider(nil, nil, nil, logger, "n1", context.Background(), nil)
			enc, err := json.Marshal(payload)
			require.NoError(t, err)
			require.NoError(t, provider.CheckConflict(enc, sc.tasks),
				"the guard must accept every claim the enqueuer composes")
		})
	}
}

// TestCapShardOwnership_KeepsAllReplicasOfKeptShards pins the RF>1 rule: the
// cap counts DISTINCT shards, and a kept shard keeps its unit on EVERY
// replica node — units are per (shard, replica) and a shard's coverage only
// counts when all of them complete, so capping away one replica would strand
// the whole shard's round.
func TestCapShardOwnership_KeepsAllReplicasOfKeptShards(t *testing.T) {
	ownership := map[string][]string{
		"node1": {"s1", "s2", "s3"},
		"node2": {"s1", "s2", "s3"},
		"node3": {"s2", "s3"},
	}

	kept, deferred := capShardOwnership(ownership, 2)
	require.Equal(t, 1, deferred, "one distinct shard defers to a later round")

	keptShards := map[string]int{}
	for _, shards := range kept {
		for _, s := range shards {
			keptShards[s]++
		}
	}
	require.Len(t, keptShards, 2, "the cap bounds DISTINCT shards, not units")
	for shard, replicas := range keptShards {
		switch shard {
		case "s1":
			require.Equal(t, 2, replicas, "s1 lives on 2 nodes; both units must stay")
		case "s2":
			require.Equal(t, 3, replicas, "s2 lives on 3 nodes; all three units must stay")
		default:
			t.Fatalf("unexpected shard kept: %s (deterministic sorted cap must keep s1, s2)", shard)
		}
	}
}
