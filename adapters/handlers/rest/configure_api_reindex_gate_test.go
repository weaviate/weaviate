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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// reindexGateTask builds a DTM task carrying a reindex payload.
func reindexGateTask(t *testing.T, id string, status distributedtask.TaskStatus,
	collection string, unitToShard map[string]string,
) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(db.ReindexTaskPayload{
		Collection:  collection,
		UnitToShard: unitToShard,
	})
	require.NoError(t, err)
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Namespace:      db.ReindexNamespace,
		Status:         status,
		Payload:        raw,
	}
}

// The gate's whole selectivity lives in this compare: it is what stops a
// migration on one shard from refusing backups of every other shard, and what
// stops a migration being missed because a sibling shard is idle. The db-side
// test can only reach it through an injected lookup, so it is pinned here,
// against the closure production actually installs.
func TestShardReindexActivityBuilderScopesByCollectionAndShard(t *testing.T) {
	logger, _ := test.NewNullLogger()
	tasks := map[string][]*distributedtask.Task{
		db.ReindexNamespace: {
			reindexGateTask(t, "t1", distributedtask.TaskStatusStarted, "MyClass",
				map[string]string{"u1": "shard1"}),
			reindexGateTask(t, "t2", distributedtask.TaskStatusStarted, "OtherClass",
				map[string]string{"u1": "shard9"}),
			reindexGateTask(t, "t3", distributedtask.TaskStatusFinished, "MyClass",
				map[string]string{"u1": "shard7"}),
		},
	}

	lookup := newShardReindexActivityBuilder(context.Background(),
		func(context.Context) (map[string][]*distributedtask.Task, error) {
			return tasks, nil
		}, logger)()

	tests := []struct {
		name       string
		collection string
		shard      string
		wantLive   bool
	}{
		{
			name:       "the tuple a live task names",
			collection: "MyClass", shard: "shard1", wantLive: true,
		},
		{
			name:       "right collection, other shard",
			collection: "MyClass", shard: "shard2",
			wantLive: false,
		},
		{
			name:       "other collection, same shard name",
			collection: "MyClass", shard: "shard9",
			wantLive: false,
		},
		{
			name:       "the other collection's own tuple",
			collection: "OtherClass", shard: "shard9", wantLive: true,
		},
		{
			name:       "a terminal task holds nothing",
			collection: "MyClass", shard: "shard7",
			wantLive: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantLive, lookup(tt.collection, tt.shard))
		})
	}
}

// A DTM the builder cannot reach must not read as "no migration anywhere":
// answering free from a question that was never asked admits a backup over a
// live migration.
func TestShardReindexActivityBuilderRefusesWhenDTMIsUnreachable(t *testing.T) {
	logger, hook := test.NewNullLogger()

	lookup := newShardReindexActivityBuilder(context.Background(),
		func(context.Context) (map[string][]*distributedtask.Task, error) {
			return nil, errors.New("raft: not leader")
		}, logger)()

	assert.True(t, lookup("MyClass", "shard1"),
		"an unreachable DTM must refuse every backup, not clear them all")
	require.NotEmpty(t, hook.AllEntries(),
		"the operator has to be told why every backup is being refused")
}

// One undecodable payload must not take the rest of the snapshot with it: the
// tasks around it still hold their shards.
func TestShardReindexActivityBuilderSkipsUndecodablePayloads(t *testing.T) {
	logger, _ := test.NewNullLogger()
	broken := reindexGateTask(t, "t1", distributedtask.TaskStatusStarted, "MyClass",
		map[string]string{"u1": "shard1"})
	broken.Payload = []byte("{not json")

	lookup := newShardReindexActivityBuilder(context.Background(),
		func(context.Context) (map[string][]*distributedtask.Task, error) {
			return map[string][]*distributedtask.Task{db.ReindexNamespace: {
				broken,
				reindexGateTask(t, "t2", distributedtask.TaskStatusStarted, "MyClass",
					map[string]string{"u1": "shard2"}),
			}}, nil
		}, logger)()

	assert.False(t, lookup("MyClass", "shard1"))
	assert.True(t, lookup("MyClass", "shard2"),
		"a task the snapshot could read must still hold its shard")
}
