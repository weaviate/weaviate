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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// TestShouldRetainCompletedTaskDecodesEachRunOnce makes a payload unreadable after the first tick.
// A run decoded earlier keeps its verdict, and one that left the namespace is decoded again.
func TestShouldRetainCompletedTaskDecodesEachRunOnce(t *testing.T) {
	failedRun := func(version uint64, prop string) *distributedtask.Task {
		payload, err := json.Marshal(ReindexTaskPayload{
			MigrationType: ReindexTypeEnableFilterable, Collection: "C", Properties: []string{prop},
		})
		require.NoError(t, err)
		return &distributedtask.Task{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: prop, Version: version},
			Status:         distributedtask.TaskStatusFailed, Payload: payload,
		}
	}
	tests := []struct {
		name       string
		leaveFirst bool
		wantRetain bool
	}{
		{name: "a decoded run is not decoded again", wantRetain: true},
		{name: "a run that left the namespace is decoded again", leaveFirst: true, wantRetain: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			older, newer := failedRun(1, "foo"), failedRun(2, "bar")
			tasks := map[distributedtask.TaskDescriptor]*distributedtask.Task{
				older.TaskDescriptor: older, newer.TaskDescriptor: newer,
			}
			p := &ReindexProvider{}
			for _, task := range tasks {
				require.True(t, p.ShouldRetainCompletedTask(task, tasks))
			}
			if tt.leaveFirst {
				delete(tasks, older.TaskDescriptor)
				require.True(t, p.ShouldRetainCompletedTask(newer, tasks))
				tasks[older.TaskDescriptor] = older
			}
			older.Payload = []byte("unreadable")
			require.Equal(t, tt.wantRetain, p.ShouldRetainCompletedTask(older, tasks))
		})
	}
}
