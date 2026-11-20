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
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type addEveryMissingGroup struct {
	calls           int
	paidBy          []uint64
	collectionTasks []string
	refuse          error
}

func (*addEveryMissingGroup) CheckConflict([]byte, []*distributedtask.Task) error { return nil }

func (*addEveryMissingGroup) DecodeHeader(payload []byte) (distributedtask.TaskHeader, error) {
	var p struct{ Collection string }
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, err
	}
	return collectionHeader(p.Collection), nil
}

func (c *addEveryMissingGroup) AddMissingGroups(task *distributedtask.Task, paidBy []uint64,
	collectionTasks []*distributedtask.Task, state *sharding.State,
) ([]*distributedtask.Unit, error) {
	c.calls++
	c.paidBy, c.collectionTasks = paidBy, nil
	for _, t := range collectionTasks {
		c.collectionTasks = append(c.collectionTasks, t.ID)
	}
	var added []*distributedtask.Unit
	for name := range state.Physical {
		if !task.HasGroup(name) && name != "c" {
			added = append(added, &distributedtask.Unit{ID: name + "-unit", GroupID: name})
		}
	}
	return added, c.refuse
}

func TestTaskAddAddsMissingGroupsFromTheSchemaAtItsIndex(t *testing.T) {
	const ns = "ns"
	adder := &addEveryMissingGroup{}
	srv, _ := newBarrierTestStore(t, func(cfg *Config) {
		cfg.DistributedTaskConflictDetectors = map[string]distributedtask.ConflictDetector{ns: adder}
	})
	ctx := context.Background()
	// The submitter read "a" and "c" and skipped "c" as already migrated. "b" was created after that read.
	_, err := srv.AddClass(ctx, &models.Class{
		Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	}, &sharding.State{PartitioningEnabled: true, Physical: map[string]sharding.Physical{
		"a": {Name: "a", BelongsToNodes: []string{"Node-1"}},
		"b": {Name: "b", BelongsToNodes: []string{"Node-1"}},
		"c": {Name: "c", BelongsToNodes: []string{"Node-1"}},
	}})
	require.NoError(t, err)
	onC, onD := map[string]string{"Collection": "C"}, map[string]string{"Collection": "D"}
	specs := []distributedtask.UnitSpec{{ID: "a-unit", GroupID: "a"}}
	task := func(id string) *distributedtask.Task {
		for _, task := range srv.LocalDistributedTasks()[ns] {
			if task.ID == id {
				return task
			}
		}
		return nil
	}

	require.NoError(t, srv.AddDistributedTaskWithGroups(ctx, ns, "T0", onC, specs))
	require.NoError(t, srv.AddDistributedTaskWithGroups(ctx, ns, "TD", onD, specs))
	require.Zero(t, adder.calls, "an entry without the marker, as a replayed old one, adds nothing")
	require.Len(t, task("T0").Units, 1)

	require.NoError(t, srv.AddDistributedTaskWithGroupsBarrier(ctx, ns, "T1", onC, specs, []uint64{7}))
	require.ElementsMatch(t, []string{"a-unit", "b-unit"}, slices.Collect(maps.Keys(task("T1").Units)))
	require.ElementsMatch(t, []string{"a", "b"}, task("T1").Groups())
	require.Equal(t, []uint64{7}, adder.paidBy)
	require.Equal(t, []string{"T0"}, adder.collectionTasks, "only the collection's own tasks")

	adder.refuse = distributedtask.NewTenantsNotHotError("C")
	err = srv.AddDistributedTaskWithGroupsBarrier(ctx, ns, "T3", onC, specs, nil)
	require.ErrorIs(t, err, distributedtask.ErrTaskTenantsNotHot)
	require.Nil(t, task("T3"), "a refused task leaves no state")

	adder.refuse = nil
	err = srv.AddDistributedTaskWithGroupsBarrier(ctx, ns, "T4", map[string]string{"Collection": "Gone"}, specs, nil)
	require.ErrorIs(t, err, distributedtask.ErrTaskReplicasChanged, "a collection deleted while the task was submitted")
	require.Nil(t, task("T4"))

	err = srv.AddDistributedTaskWithGroupsBarrier(ctx, "no-adder", "T5", onC, specs, nil)
	require.ErrorIs(t, err, distributedtask.ErrMissingGroupsUnsupported, "a namespace that cannot add missing groups must not drop the ask silently")
}
