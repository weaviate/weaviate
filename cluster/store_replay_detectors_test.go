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

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// refuseWhileTaskExists refuses a second task, and a class delete, while its
// namespace holds a task.
type refuseWhileTaskExists struct{}

func (refuseWhileTaskExists) CheckConflict(_ []byte, existing []*distributedtask.Task) error {
	if len(existing) > 0 {
		return fmt.Errorf("conflicts with task %s", existing[0].ID)
	}
	return nil
}

func (refuseWhileTaskExists) CheckClassMutation(class string, existing []*distributedtask.Task) error {
	if len(existing) > 0 {
		return fmt.Errorf("task in flight on %s", class)
	}
	return nil
}

func (refuseWhileTaskExists) CheckPropertyUpdate(string, string, []*distributedtask.Task) error {
	return nil
}

func (refuseWhileTaskExists) CheckTenantMutation(string, []string, []*distributedtask.Task) error {
	return nil
}

// newRestartableStore starts a single-node store and returns it with a restart
// that replays the whole log into a fresh FSM.
func newRestartableStore(t *testing.T, tweak func(*Config)) (*Raft, func(*Raft) *Raft) {
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort(), tweak)
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("AddClass", Anything).Return(nil).Maybe()
	m.indexer.On("DeleteClass", Anything).Return(nil).Maybe()
	m.indexer.On("AddTenants", Anything, Anything).Return(nil).Maybe()
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return().Maybe()
	m.indexer.On("Close", Anything).Return(nil)
	m.parser.On("ParseClass", Anything).Return(nil)
	m.replicationFSM.On("DeleteReplicationsByCollection", Anything).Return(nil).Maybe()

	start := func() *Raft {
		srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
		require.NoError(t, srv.Open(ctx, m.indexer))
		require.NoError(t, srv.store.Notify(m.cfg.NodeID, addr))
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		require.NoError(t, srv.WaitUntilDBRestored(wctx, time.Second, make(chan struct{})))
		require.True(t, tryNTimesWithWait(20, 200*time.Millisecond, srv.store.IsLeader))
		return srv
	}
	restart := func(srv *Raft) *Raft {
		lastIndex := srv.store.raft.Load().LastIndex()
		require.NoError(t, srv.Close(ctx))
		s := NewFSM(m.cfg, nil, prometheus.NewPedanticRegistry())
		s.schemaManager.SetReplicationFSM(m.replicationFSM)
		m.store = &s
		srv = start()
		require.True(t, tryNTimesWithWait(50, 100*time.Millisecond, func() bool {
			return srv.store.lastAppliedIndex.Load() >= lastIndex
		}), "the restarted node must replay the whole log")
		return srv
	}
	return start(), restart
}

// TestRestartReplaysRefusedEntriesWithTheSameVerdict restarts a node over a log
// holding entries its detectors refused live. Replay must refuse them again,
// or the restarted node's state diverges from its peers'.
func TestRestartReplaysRefusedEntriesWithTheSameVerdict(t *testing.T) {
	ctx := context.Background()
	const ns = "test-ns"
	srv, restart := newRestartableStore(t, func(cfg *Config) {
		cfg.DistributedTaskConflictDetectors = map[string]distributedtask.ConflictDetector{ns: refuseWhileTaskExists{}}
		cfg.DistributedTaskSchemaMutationDetectors = map[string]distributedtask.SchemaMutationDetector{ns: refuseWhileTaskExists{}}
	})
	_, err := srv.AddClass(ctx, &models.Class{Class: "C"}, &sharding.State{
		Physical: map[string]sharding.Physical{"S1": {Name: "S1", BelongsToNodes: []string{"Node-1"}}},
	})
	require.NoError(t, err)
	require.NoError(t, srv.AddDistributedTask(ctx, ns, "T1", map[string]string{}, []string{"u1"}))
	require.ErrorContains(t, srv.AddDistributedTask(ctx, ns, "T2", map[string]string{}, []string{"u1"}), "conflicts with task T1")
	_, err = srv.DeleteClass(ctx, "C")
	require.ErrorContains(t, err, "task in flight on C")
	srv = restart(srv)

	require.Equal(t, "C", srv.SchemaReader().ClassEqual("C"), "the replayed DELETE_CLASS must stay refused")
	tasks, err := srv.store.distributedTasksManager.ListDistributedTasks(ctx)
	require.NoError(t, err)
	require.Len(t, tasks[ns], 1, "the replayed conflicting task must stay refused")
	require.NoError(t, srv.Close(ctx))
}

// Pins replay adding the same missing-group units, read from the schema at that entry, not later tenants.
func TestRestartReplaysAddMissingGroupsWithTheSameUnits(t *testing.T) {
	ctx := context.Background()
	const ns = "ns"
	srv, restart := newRestartableStore(t, func(cfg *Config) {
		cfg.DistributedTaskConflictDetectors = map[string]distributedtask.ConflictDetector{ns: &addEveryMissingGroup{}}
		cfg.DistributedTaskCollectionExtractors = map[string]distributedtask.CollectionExtractor{
			ns: func(payload []byte) (string, bool) {
				var p struct{ Collection string }
				return p.Collection, json.Unmarshal(payload, &p) == nil
			},
		}
	})
	_, err := srv.AddClass(ctx, &models.Class{
		Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}, &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{
		"a": {Name: "a", BelongsToNodes: []string{"Node-1"}},
		"b": {Name: "b", BelongsToNodes: []string{"Node-1"}},
	}})
	require.NoError(t, err)
	require.NoError(t, srv.AddDistributedTaskWithGroupsBarrier(ctx, ns, "T", map[string]string{"Collection": "C"},
		[]distributedtask.UnitSpec{{ID: "a-unit", GroupID: "a"}}, nil))
	_, err = srv.AddTenants(ctx, "C", &command.AddTenantsRequest{
		ClusterNodes: []string{"Node-1"},
		Tenants:      []*command.Tenant{{Name: "d", Status: models.TenantActivityStatusHOT}},
	})
	require.NoError(t, err)

	units := func() []string {
		tasks := srv.LocalDistributedTasks()[ns]
		require.Len(t, tasks, 1)
		return slices.Sorted(maps.Keys(tasks[0].Units))
	}
	require.Equal(t, []string{"a-unit", "b-unit"}, units())

	srv = restart(srv)
	require.Equal(t, []string{"a-unit", "b-unit"}, units(), "replay must add the units the live apply added")
	require.NoError(t, srv.Close(ctx))
}

// Pins that this build's PREP ack names its group, the single-tenant group "" included.
func TestPreparationAckDecidesItsGroup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		groups []string
		want   distributedtask.TaskStatus
	}{
		{name: "one tenant of two", groups: []string{"g", "h"}, want: distributedtask.TaskStatusStarted},
		{name: "a single-tenant run", groups: []string{""}, want: distributedtask.TaskStatusSwapping},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newBarrierTestStore(t)
			ctx := context.Background()
			const ns, id = "ns", "T"
			var specs []distributedtask.UnitSpec
			for i, g := range tc.groups {
				specs = append(specs, distributedtask.UnitSpec{ID: string(rune('a' + i)), GroupID: g})
			}
			require.NoError(t, srv.addGroupedTask(ctx, &command.AddDistributedTaskRequest{
				Namespace: ns, Id: id, NeedsPreparationBarrier: true,
			}, struct{}{}, specs))
			task := srv.LocalDistributedTasks()[ns][0]
			require.NoError(t, srv.RecordDistributedTaskUnitCompletion(ctx, ns, id, task.Version, "Node-1", "a"))
			require.NoError(t, srv.RecordDistributedTaskPreparationCompleteAck(ctx, ns, id, task.Version, tc.groups[0], "Node-1", true, ""))

			task = srv.LocalDistributedTasks()[ns][0]
			require.Contains(t, task.SwapDecidedGroups, tc.groups[0])
			require.Equal(t, tc.want, task.Status)
		})
	}
}
