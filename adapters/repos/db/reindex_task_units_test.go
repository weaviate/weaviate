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
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

var unitTestNodes = []string{"node-2", "node-0", "node-1"}

// shardingInOrder fills the shard map in a seed-chosen order, as replicas that applied the same tenant adds hold it.
// A tenant missing from status is HOT.
func shardingInOrder(seed int64, tenants int, status map[string]string) *sharding.State {
	names := make([]string, tenants)
	for i := range names {
		names[i] = fmt.Sprintf("tenant-%05d", i)
	}
	rand.New(rand.NewSource(seed)).Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	state := &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{}}
	for _, name := range names {
		st, ok := status[name]
		if !ok {
			st = models.TenantActivityStatusHOT
		}
		state.Physical[name] = sharding.Physical{Name: name, BelongsToNodes: unitTestNodes, Status: st}
	}
	return state
}

func unitsManager(state *sharding.State) *distributedtask.Manager {
	logger, _ := test.NewNullLogger()
	return distributedtask.NewManager(distributedtask.ManagerParameters{
		Logger:            logger,
		ConflictDetectors: map[string]distributedtask.ConflictDetector{ReindexNamespace: ReindexDetector{}},
		TaskRules:         map[string]distributedtask.TaskRules{ReindexNamespace: ReindexTaskRules{}},
		ClassLive:         func(string) map[string]map[string]uint64 { return nil },
		ReadSharding: func(_ string, read func(*sharding.State)) error {
			read(state)
			return nil
		},
	})
}

func addWithoutUnits(t *testing.T, p ReindexTaskPayload) *api.ApplyRequest {
	t.Helper()
	payload, err := json.Marshal(p)
	require.NoError(t, err)
	sub, err := json.Marshal(&api.AddDistributedTaskRequest{
		Namespace: ReindexNamespace, Id: "Books:title:ab3f", Payload: payload, SubmittedAtUnixMillis: 1,
	})
	require.NoError(t, err)
	return &api.ApplyRequest{Type: api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD, SubCommand: sub}
}

func titleRun(mt ReindexMigrationType, tenants ...string) ReindexTaskPayload {
	return ReindexTaskPayload{
		MigrationType: mt, Collection: "Books", Properties: []string{"title"}, TargetTokenization: "field", Tenants: tenants,
	}
}

// Every replica builds the same units in the same order, whatever order its shard map was filled in.
func TestReindexUnits_SameOnEveryNode(t *testing.T) {
	const tenants = 2000
	for _, tc := range []struct {
		name         string
		payload      ReindexTaskPayload
		units        int
		singleTenant bool // one group "" covers every shard; a multi-tenant run has a group per tenant
		wantFirst    []string
	}{
		{
			name: "full semantic run", payload: titleRun(ReindexTypeChangeTokenization), units: 3 * tenants,
			wantFirst: []string{"tenant-00000__node-2", "tenant-00000__node-0", "tenant-00000__node-1", "tenant-00001__node-2"},
		},
		{
			name: "full non-semantic run", payload: titleRun(ReindexTypeRepairFilterable), units: 3 * tenants,
			wantFirst: []string{"tenant-00000__node-2", "tenant-00000__node-0", "tenant-00000__node-1", "tenant-00001__node-2"},
		},
		{
			name: "single-tenant enable-filterable run", payload: titleRun(ReindexTypeEnableFilterable), units: 3 * tenants,
			singleTenant: true,
			wantFirst:    []string{"tenant-00000__node-2", "tenant-00000__node-0", "tenant-00000__node-1", "tenant-00001__node-2"},
		},
		{
			name:      "scoped run, tenants named out of order and twice",
			payload:   titleRun(ReindexTypeRepairFilterable, "tenant-01999", "tenant-00007", "tenant-01000", "tenant-00007", "tenant-00500"),
			units:     3 * 4,
			wantFirst: []string{"tenant-00007__node-2", "tenant-00007__node-0", "tenant-00007__node-1", "tenant-00500__node-2"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var firstSpecs []distributedtask.UnitSpec
			var firstSnap []byte
			for seed := int64(1); seed <= 5; seed++ {
				state := shardingInOrder(seed, tenants, nil)
				state.PartitioningEnabled = !tc.singleTenant
				specs, err := ReindexTaskRules{}.Units(ReindexTaskHeader{tc.payload}, state)
				require.NoError(t, err)
				require.Len(t, specs, tc.units)
				m := unitsManager(state)
				require.NoError(t, m.AddTask(addWithoutUnits(t, tc.payload), 42))
				snap, err := m.Snapshot()
				require.NoError(t, err)

				if seed == 1 {
					firstSpecs, firstSnap = specs, snap
					for i, id := range tc.wantFirst {
						require.Equal(t, id, specs[i].ID)
						if tc.singleTenant {
							require.Empty(t, specs[i].GroupID)
						} else {
							require.Equal(t, specs[i].Shard, specs[i].GroupID)
						}
					}
					continue
				}
				require.Equal(t, firstSpecs, specs, "seed %d built other units", seed)
				require.Equal(t, string(firstSnap), string(snap), "seed %d stored another task", seed)
			}
		})
	}
}

// A run is refused on every replica, whatever its shard map order, and no replica stores it.
func TestReindexUnits_RefusedOnEveryNode(t *testing.T) {
	status := map[string]string{
		"tenant-00003": models.TenantActivityStatusCOLD,
		"tenant-00004": models.TenantActivityStatusFROZEN,
		"tenant-00005": models.TenantActivityStatusFREEZING,
		"tenant-00006": models.TenantActivityStatusOFFLOADED,
	}
	onlyStatus := func(st string) map[string]string { return map[string]string{"tenant-00001": st} }
	for _, tc := range []struct {
		name    string
		tenants int
		status  map[string]string
		payload ReindexTaskPayload
		want    error
	}{
		{"scoped run names a missing tenant", 10, status, titleRun(ReindexTypeRepairFilterable, "tenant-00001", "nope"), distributedtask.ErrTaskTenantsMissing},
		{"scoped run names a missing and a COLD tenant", 10, status, titleRun(ReindexTypeRepairFilterable, "tenant-00003", "nope"), distributedtask.ErrTaskTenantsMissing},
		{"scoped run names a COLD tenant", 10, status, titleRun(ReindexTypeRepairFilterable, "tenant-00003"), distributedtask.ErrTaskTenantsNotHot},
		{"scoped run names a FROZEN tenant", 10, status, titleRun(ReindexTypeRepairFilterable, "tenant-00004"), distributedtask.ErrTaskTenantsNotHot},
		{"scoped run names a FREEZING tenant", 10, status, titleRun(ReindexTypeRepairFilterable, "tenant-00005"), distributedtask.ErrTaskTenantsNotHot},
		{"scoped run names an OFFLOADED tenant", 10, status, titleRun(ReindexTypeRepairFilterable, "tenant-00006"), distributedtask.ErrTaskTenantsNotHot},
		{"full run over a COLD tenant", 10, onlyStatus(models.TenantActivityStatusCOLD), titleRun(ReindexTypeChangeTokenization), distributedtask.ErrTaskTenantsNotHot},
		{"full run over a FROZEN tenant", 10, onlyStatus(models.TenantActivityStatusFROZEN), titleRun(ReindexTypeEnableFilterable), distributedtask.ErrTaskTenantsNotHot},
		{"full run over an OFFLOADED tenant", 10, onlyStatus(models.TenantActivityStatusOFFLOADED), titleRun(ReindexTypeRepairFilterable), distributedtask.ErrTaskTenantsNotHot},
		{"collection with no tenants", 0, nil, titleRun(ReindexTypeChangeTokenization), distributedtask.ErrTaskHasNoUnits},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for seed := int64(1); seed <= 3; seed++ {
				m := unitsManager(shardingInOrder(seed, tc.tenants, tc.status))
				err := m.AddTask(addWithoutUnits(t, tc.payload), 42)
				require.ErrorIs(t, err, tc.want)
				require.ErrorIs(t, err, distributedtask.ErrPermanentRejection)
				require.False(t, m.HasActiveTaskForCollection("Books"))
			}
		})
	}
}

func TestReindexUnits_TenantStatus(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tenants   int
		status    map[string]string
		wantUnits int
		wantCount int
	}{
		{
			name: "HOT, ACTIVE and unset are admitted", tenants: 3, wantUnits: 9,
			status: map[string]string{"tenant-00000": models.TenantActivityStatusHOT, "tenant-00001": models.TenantActivityStatusACTIVE, "tenant-00002": ""},
		},
		{
			name: "the refusal counts every tenant that is not HOT, and only those", tenants: 4, wantCount: 3,
			status: map[string]string{"tenant-00000": models.TenantActivityStatusCOLD, "tenant-00001": models.TenantActivityStatusCOLD, "tenant-00002": models.TenantActivityStatusFROZEN},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			specs, err := ReindexTaskRules{}.Units(ReindexTaskHeader{titleRun(ReindexTypeEnableFilterable)}, shardingInOrder(1, tc.tenants, tc.status))
			if tc.wantCount == 0 {
				require.NoError(t, err)
				require.Len(t, specs, tc.wantUnits)
				return
			}
			count, ok := distributedtask.TenantsNotHotCount(err)
			require.True(t, ok)
			require.Equal(t, tc.wantCount, count)
		})
	}
}
