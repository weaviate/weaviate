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

package schema_test

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// loadRecordingDB records what a shard loading inside db.AddClass reads: its tasks and the class's Live.
type loadRecordingDB struct {
	*fakes.MockSchemaExecutor
	tasks *distributedtask.Manager
	live  func(string) map[string]map[string]uint64
	shard string

	loaded     []distributedtask.ShardTaskState
	loadedLive map[string]map[string]uint64
}

func (d *loadRecordingDB) RestoreClassDir(string) error { return nil }

func (d *loadRecordingDB) AddClass(req command.AddClassRequest) error {
	d.loaded = d.tasks.ShardTaskStates(db.ReindexNamespace, req.Class.Class, d.shard, "node1")
	d.loadedLive = d.live(req.Class.Class)
	return nil
}

func newRestoreHarness(t *testing.T, shard string) (*clusterSchema.SchemaManager, *distributedtask.Manager, *loadRecordingDB) {
	t.Helper()
	parser := fakes.NewMockParser()
	parser.On("ParseClass", mock.Anything).Return(nil)
	recorder := &loadRecordingDB{MockSchemaExecutor: fakes.NewMockSchemaExecutor(), shard: shard}
	sm := clusterSchema.NewSchemaManager("node1", recorder, parser, prometheus.NewPedanticRegistry(), logrus.New())
	reader := sm.NewSchemaReader()
	tasks := distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:            logrus.New(),
		ConflictDetectors: map[string]distributedtask.ConflictDetector{db.ReindexNamespace: db.ReindexDetector{}},
		TaskRules:         map[string]distributedtask.TaskRules{db.ReindexNamespace: db.ReindexTaskRules{}},
		ClassLive:         reader.ClassLive,
		ReadSharding: func(collection string, read func(*sharding.State)) error {
			return reader.Read(collection, false, func(_ *models.Class, state *sharding.State) error {
				read(state)
				return nil
			})
		},
	})
	sm.SetDistributedTaskManager(tasks)
	recorder.tasks, recorder.live = tasks, reader.ClassLive
	return sm, tasks, recorder
}

func capturedReindexTask(t *testing.T, id string, version uint64, status distributedtask.TaskStatus,
	payload db.ReindexTaskPayload, switched ...string,
) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace: db.ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: version},
		Payload: raw, Status: status, NeedsPreparationBarrier: true, SwapDecidedGroups: map[string]time.Time{},
		Summary: &distributedtask.UnitSummary{Counts: map[distributedtask.UnitStatus]int{distributedtask.UnitStatusFailed: 1}},
	}
	for _, g := range switched {
		task.SwapDecidedGroups[g] = time.UnixMilli(1_700_000_000_000)
	}
	return task
}

func restoreClass(t *testing.T, sm *clusterSchema.SchemaManager, class *models.Class, state *sharding.State,
	live map[string]map[string]uint64, tasks ...*distributedtask.Task,
) {
	t.Helper()
	raw, err := json.Marshal(tasks)
	require.NoError(t, err)
	sub, err := json.Marshal(command.AddClassRequest{Class: class, State: state, DistributedTasks: raw, Live: live})
	require.NoError(t, err)
	require.NoError(t, sm.RestoreClass(&command.ApplyRequest{
		Type: command.ApplyRequest_TYPE_RESTORE_CLASS, Class: class.Class, Version: 20, SubCommand: sub,
	}, "node1", false, false))
}

func tenantsState(partitioned bool, shards ...string) *sharding.State {
	state := &sharding.State{PartitioningEnabled: partitioned, Physical: map[string]sharding.Physical{}}
	for _, s := range shards {
		state.Physical[s] = sharding.Physical{Name: s, BelongsToNodes: []string{"node1"}, Status: models.TenantActivityStatusHOT}
	}
	return state
}

func TestRestoreClassInstallsReindexTasksBeforeTheIndex(t *testing.T) {
	retokenize := func(collection string) db.ReindexTaskPayload {
		return db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeChangeTokenization, Collection: collection, Properties: []string{"title"},
			TargetTokenization: models.PropertyTokenizationField, ViewServed: true, Generation: 2,
		}
	}
	live := map[string]map[string]uint64{"title": {command.ReindexSearchable: 1}}
	for _, tc := range []struct {
		name         string
		class        string
		state        *sharding.State
		shard        string
		tasks        []*distributedtask.Task
		wantSwitched []string
	}{
		{
			name: "a switched tenant the restore lacks is dropped", class: "Movies",
			state: tenantsState(true, "t1"), shard: "t1", wantSwitched: []string{"t1"},
			tasks: []*distributedtask.Task{capturedReindexTask(t, "run", 7, distributedtask.TaskStatusFailed, retokenize("Movies"), "t1", "t2")},
		},
		{
			name: "a single-tenant run keeps the group of all shards", class: "Movies",
			state: tenantsState(false, "S1"), shard: "S1", wantSwitched: []string{""},
			tasks: []*distributedtask.Task{capturedReindexTask(t, "run", 7, distributedtask.TaskStatusFailed, retokenize("Movies"), "")},
		},
		{
			name: "a run captured under a namespace serves the stripped class, another class's run is skipped", class: "Movies",
			state: tenantsState(true, "t1"), shard: "t1", wantSwitched: []string{"t1"},
			tasks: []*distributedtask.Task{
				capturedReindexTask(t, "run", 7, distributedtask.TaskStatusFailed, retokenize("ns:Movies"), "t1"),
				capturedReindexTask(t, "other", 8, distributedtask.TaskStatusFailed, retokenize("Books"), "t1"),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm, tasks, loaded := newRestoreHarness(t, tc.shard)
			class := &models.Class{Class: tc.class, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: tc.state.PartitioningEnabled}}

			restoreClass(t, sm, class, tc.state, live, tc.tasks...)

			require.Len(t, loaded.loaded, 1, "the shard loading in the restore entry reads the restored task")
			state := loaded.loaded[0]
			require.True(t, state.Switched)
			header := state.Header.(db.ReindexTaskHeader)
			require.Equal(t, uint64(2), header.Generation)
			require.Equal(t, tc.class, header.Collection())
			require.Equal(t, live, loaded.loadedLive)

			restored := tasks.LocalEndedTasks(db.ReindexNamespace, tc.class)
			require.Len(t, restored, 1)
			require.Equal(t, "run", restored[0].ID)
			require.Equal(t, tc.wantSwitched, slices.Sorted(maps.Keys(restored[0].SwapDecidedGroups)))
		})
	}
}

// A restored run keeps its captured version, so a later run's generation must not follow the log index.
func TestRestoredReindexRunSetsTheNextGeneration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index uint64
	}{
		{name: "same cluster, new run at a higher log index", index: 100},
		{name: "fresh cluster, new run at a lower log index", index: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sm, tasks, _ := newRestoreHarness(t, "t1")
			scoped := db.ReindexTaskPayload{
				MigrationType: db.ReindexTypeRebuildSearchable, Collection: "Movies", Properties: []string{"title"},
				Tenants: []string{"t1"}, ViewServed: true, Generation: 2,
			}
			restoreClass(t, sm, &models.Class{Class: "Movies", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}},
				tenantsState(true, "t1"), nil, capturedReindexTask(t, "scoped", 50, distributedtask.TaskStatusFinished, scoped, "t1"))

			full := scoped
			full.Tenants, full.Generation = nil, 0
			payload, err := json.Marshal(full)
			require.NoError(t, err)
			sub, err := json.Marshal(&command.AddDistributedTaskRequest{
				Namespace: db.ReindexNamespace, Id: "full", Payload: payload, NeedsPreparationBarrier: true,
			})
			require.NoError(t, err)
			require.NoError(t, tasks.AddTask(&command.ApplyRequest{SubCommand: sub}, tc.index))

			gens := map[string]uint64{}
			list, err := tasks.ListDistributedTasks(t.Context())
			require.NoError(t, err)
			for _, task := range list[db.ReindexNamespace] {
				var p db.ReindexTaskPayload
				require.NoError(t, json.Unmarshal(task.Payload, &p))
				gens[task.ID] = p.Generation
			}
			require.Equal(t, map[string]uint64{"scoped": 2, "full": 3}, gens)
		})
	}
}
