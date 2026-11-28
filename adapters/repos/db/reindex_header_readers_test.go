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
	"fmt"
	"runtime"
	"strings"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// Blanking the payload bytes after the header is built catches a reader that still decodes them.
func TestReindexHeaderReaders(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	raw := payloadWithUnitMaps(t)

	for _, tc := range []struct {
		name   string
		status distributedtask.TaskStatus
		reads  func(task *distributedtask.Task) bool
	}{
		{"ReindexRuns", distributedtask.TaskStatusStarted, func(task *distributedtask.Task) bool {
			return len(ReindexRuns([]*distributedtask.Task{task})) == 1
		}},
		{"ShouldRetainCompletedTask", distributedtask.TaskStatusFailed, func(task *distributedtask.Task) bool {
			return (&ReindexProvider{}).ShouldRetainCompletedTask(task,
				map[distributedtask.TaskDescriptor]*distributedtask.Task{task.TaskDescriptor: task})
		}},
		{"StartTask", distributedtask.TaskStatusStarted, func(task *distributedtask.Task) bool {
			_, err := NewReindexProvider(&DB{}, logger, "n1", nil, context.Background()).StartTask(task)
			return err != nil && strings.Contains(err.Error(), `collection "C" not found`)
		}},
		{"NewShardReindexActivityLookup", distributedtask.TaskStatusStarted, func(task *distributedtask.Task) bool {
			hasUnit := func(distributedtask.TaskDescriptor, string) (bool, bool) { return true, true }
			return NewShardReindexActivityLookup([]*distributedtask.Task{task}, hasUnit, logger)("C", "s1")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := loadTasks(t, ReindexNamespace, &distributedtask.Task{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
				Status:         tc.status, Payload: raw,
				Units: map[string]*distributedtask.Unit{"s1__n1": {ID: "s1__n1", Shard: "s1", NodeID: "n1"}},
			})[0]

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			require.True(t, tc.reads(task))
			runtime.ReadMemStats(&after)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "payload of %d bytes", len(raw))

			task.Payload = []byte("{")
			require.True(t, tc.reads(task), "a reader that decodes the payload bytes loses the task")
		})
	}
}

// The leader reads a submitted task's collection before the append, and decodes only that key.
func TestCollectionOfTaskDecodesOnlyTheCollection(t *testing.T) {
	tenants := make([]string, 300_000)
	for i := range tenants {
		tenants[i] = fmt.Sprintf("tenant-%06d", i)
	}
	reindex, err := json.Marshal(ReindexTaskPayload{
		Collection: "C", MigrationType: ReindexTypeChangeTokenization, Properties: []string{"name"}, Tenants: tenants,
	})
	require.NoError(t, err)
	m := distributedtask.NewManager(distributedtask.ManagerParameters{ConflictDetectors: map[string]distributedtask.ConflictDetector{
		ReindexNamespace: ReindexDetector{},
	}})

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got := m.CollectionOfTask(ReindexNamespace, reindex)
	runtime.ReadMemStats(&after)
	require.Equal(t, "C", got)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10), "payload of %d bytes", len(reindex))
}
