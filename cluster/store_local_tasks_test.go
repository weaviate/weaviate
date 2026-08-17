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
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/proto/api"
)

// A task applied to this node's FSM is readable from this node, with no
// leader round-trip in between: Store delegates straight to the Manager's
// in-memory map.
//
// This harness cannot say more. setupApplyTest builds a bare FSM with no raft
// node, so the leader-routed path resolves to the same function and could not
// be caught disagreeing. The receipt for "both operands come from one node in
// one order" is TestIndexStatusOperands_ComeFromOneNodeInOneOrder, in the
// handler package.
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

	tasks := ms.store.LocalDistributedTasks()
	require.Len(t, tasks["test-namespace"], 1,
		"a task applied to this node's FSM must be readable without a leader round-trip")
	require.Equal(t, "task-1", tasks["test-namespace"][0].ID)
}
