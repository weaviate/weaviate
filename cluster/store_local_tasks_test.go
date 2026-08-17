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
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

// LocalDistributedTasks answers from this node's applied log rather than from
// a query routed to the leader, so a caller that compares a task's status
// against schema state reads both operands at the same log position. The
// leader-routed ListDistributedTasks cannot promise that: it can report a task
// committed at an index this node's schema has not applied yet.
func TestStore_LocalDistributedTasks_AnswersFromTheAppliedLog(t *testing.T) {
	ms, addClassLog := setupApplyTest(t)
	ms.parser.On("ParseClass", mock.Anything).Return(nil)
	ms.indexer.On("AddClass", mock.Anything).Return(nil)
	ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	applyOrFail(t, &ms, addClassLog, "add-class")

	applyOrFail(t, &ms, &raft.Log{
		Index: 2,
		Type:  raft.LogCommand,
		Data: cmdAsBytes("", api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD,
			&api.AddDistributedTaskRequest{
				Namespace:             "test-namespace",
				Id:                    "task-1",
				Payload:               []byte(`{}`),
				SubmittedAtUnixMillis: 1,
				UnitIds:               []string{"u-1"},
			}, nil),
	}, "add-task")

	tasks, err := ms.store.LocalDistributedTasks(context.Background())
	require.NoError(t, err)
	require.Len(t, tasks["test-namespace"], 1,
		"a task applied to this node's FSM must be readable without a leader round-trip")
	require.Equal(t, "task-1", tasks["test-namespace"][0].ID)

	// The schema entry that preceded the task in the log is visible to the
	// same reader. This is the property the index-status endpoint relies on
	// to compare a task's status against a schema flag.
	require.NotNil(t, ms.store.SchemaReader().ReadOnlyClass("TestClass"))
}
