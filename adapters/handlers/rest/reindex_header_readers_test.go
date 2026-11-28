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
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	clusterSchema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// Blanking the payload bytes after the header is built catches a reader that still decodes them.
func TestReindexHeaderReaders(t *testing.T) {
	const units = 300_000
	unitToNode, unitToShard := make(map[string]string, units), make(map[string]string, units)
	for i := range units {
		tenant, node := fmt.Sprintf("tenant-%06d", i/3), fmt.Sprintf("node-%d", i%3)
		unitToNode[tenant+"__"+node], unitToShard[tenant+"__"+node] = node, tenant
	}
	raw, err := json.Marshal(map[string]any{
		"collection": "C", "migrationType": db.ReindexTypeChangeTokenization, "properties": []string{"name"},
		"unitToNode": unitToNode, "unitToShard": unitToShard,
	})
	require.NoError(t, err)

	logger := logrus.New()
	classes := schemaUC.NewMockSchemaReader(t)
	classes.EXPECT().ClassInfo("C").Return(clusterSchema.ClassInfo{Exists: true}).Maybe()
	classes.EXPECT().ReadOnlyClass("C").Return(&models.Class{Class: "C"}).Maybe()

	for _, tc := range []struct {
		name  string
		reads func(tasks []*distributedtask.Task) bool
	}{
		{"firstActiveReindexTask", func(tasks []*distributedtask.Task) bool {
			_, _, found := firstActiveReindexTask(tasks, decodeSkip, func(db.ReindexTaskPayload) bool { return true })
			return found
		}},
		{"findCancelTarget", func(tasks []*distributedtask.Task) bool {
			target, _ := findCancelTarget(tasks, "C", "name", "searchable", logger)
			return target != nil
		}},
		{"countInFlightTasksForCollection", func(tasks []*distributedtask.Task) bool {
			return countInFlightTasksForCollection("C", tasks) == 1
		}},
		{"checkReindexConflict", func(tasks []*distributedtask.Task) bool {
			reason, err := checkReindexConflict("C", db.ReindexTypeChangeTokenization, []string{"name"}, tasks)
			return err == nil && reason != ""
		}},
		{"readClassAndTasks", func(tasks []*distributedtask.Task) bool {
			_, runs := readClassAndTasks("C", staticTasks{db.ReindexNamespace: tasks}, classes)
			return len(runs) == 1
		}},
		{"checkReindexConflictForPropertyMutation", func(tasks []*distributedtask.Task) bool {
			reason := gateWithTasks(tasks...).checkReindexConflictForPropertyMutation(context.Background(), "C", "name", "", true)
			return reason != "" && !strings.Contains(reason, "unparseable")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := withHeaders(&distributedtask.Task{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
				Status:         distributedtask.TaskStatusStarted, Payload: raw,
			})

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			require.True(t, tc.reads(tasks))
			runtime.ReadMemStats(&after)
			require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "payload of %d bytes", len(raw))

			tasks[0].Payload = []byte("{")
			require.True(t, tc.reads(tasks), "a reader that decodes the payload bytes loses the task")
		})
	}
}
