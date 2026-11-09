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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// Pins that a decided swap retried after OnTaskCompleted decodes the payload
// once, and that the decoded copy is freed once the task leaves the list.
func TestLoadPayloadIsKeptUntilTheTaskLeavesTheList(t *testing.T) {
	p := NewReindexProvider(nil, nil, nil, logrus.New(), "node-0", nil, nil)
	raw, err := json.Marshal(ReindexTaskPayload{Collection: "Docs", MigrationType: ReindexTypeChangeTokenization})
	require.NoError(t, err)
	task := &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
		Status:         distributedtask.TaskStatusFailed, Payload: raw,
	}

	first, err := p.loadPayload(task)
	require.NoError(t, err)
	again, err := p.loadPayload(task)
	require.NoError(t, err)
	require.Same(t, first, again, "a retry reuses the decoded payload")
	require.Equal(t, []distributedtask.TaskDescriptor{task.TaskDescriptor}, p.GetLocalTasks())

	require.NoError(t, p.CleanupTask(task.TaskDescriptor))
	require.Empty(t, p.GetLocalTasks())
	fresh, err := p.loadPayload(task)
	require.NoError(t, err)
	require.NotSame(t, first, fresh, "a cleaned-up task decodes again")
}
