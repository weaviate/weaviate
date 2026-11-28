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
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
)

func TestHeaderOfKeepsTheTaskListedUntilCleanup(t *testing.T) {
	p := NewReindexProvider(nil, logrus.New(), "node-0", nil, nil)
	raw, err := json.Marshal(ReindexTaskPayload{Collection: "Docs", MigrationType: ReindexTypeChangeTokenization, Generation: 1})
	require.NoError(t, err)
	task := loadTasks(t, ReindexNamespace, &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
		Status:         distributedtask.TaskStatusFailed, Payload: raw,
	})[0]

	_, err = p.headerOf(task)
	require.NoError(t, err)
	require.Equal(t, []distributedtask.TaskDescriptor{task.TaskDescriptor}, p.GetLocalTasks())

	require.NoError(t, p.CleanupTask(task.TaskDescriptor))
	require.Empty(t, p.GetLocalTasks())
}

// Each unit is bound to one node; another node's unit names a shard this node does not hold.
func TestStartTaskWorksOnlyThisNodesUnits(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{indices: map[string]*Index{indexID(entschema.ClassName("C")): {}}}
	p := NewReindexProvider(db, logger, "node1", func() int { return 2 }, context.Background())
	rec := newFakeRecorder()
	p.SetCompletionRecorder(rec)
	raw, err := json.Marshal(ReindexTaskPayload{
		Collection: "C", MigrationType: ReindexTypeRepairFilterable, Properties: []string{"title"}, Generation: 1,
	})
	require.NoError(t, err)

	handle, err := p.StartTask(loadTasks(t, ReindexNamespace, &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
		Status:         distributedtask.TaskStatusStarted, Payload: raw,
		Units: map[string]*distributedtask.Unit{
			"s1__node1": {ID: "s1__node1", Shard: "s1", NodeID: "node1"},
			"s2__node2": {ID: "s2__node2", Shard: "s2", NodeID: "node2"},
		},
	})[0])
	require.NoError(t, err)
	<-handle.Done()

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, []string{"s1__node1"}, slices.Collect(maps.Keys(rec.failed)), "this node fails its own unit, whose shard it lacks")
}
