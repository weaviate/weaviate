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
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestRESTTaskListPassesLogUnreadablePayloadsAsOneLine(t *testing.T) {
	var tasks []*distributedtask.Task
	for i := range 3 {
		tasks = append(tasks, &distributedtask.Task{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: fmt.Sprintf("task-%d", i), Version: 1},
			Payload:        []byte("not json"), Status: distributedtask.TaskStatusStarted,
		})
	}
	for _, tc := range []struct {
		name string
		pass func(logrus.FieldLogger)
	}{
		{"cancel target", func(l logrus.FieldLogger) { findCancelTarget(tasks, "C", "p", "filterable", l) }},
		{"property task descriptors", func(l logrus.FieldLogger) { reindexTaskDescriptorsForProperty(tasks, "C", "p", l) }},
		{"drop-vector live op IDs", func(l logrus.FieldLogger) {
			cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{db.DropVectorIndexNamespace: tasks}}
			enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: &fakeShardingState{}, logger: l}
			_, err := enq.LiveOpIDs(context.Background())
			require.NoError(t, err)
		}},
		{"drop-vector reconcile round", func(l logrus.FieldLogger) {
			classes := []*models.Class{
				{Class: "A", VectorConfig: map[string]models.VectorConfig{"v1": dropped()}},
				{Class: "B", VectorConfig: map[string]models.VectorConfig{"v1": dropped()}},
			}
			cluster := &fakeClusterDropClient{tasks: map[string][]*distributedtask.Task{db.DropVectorIndexNamespace: tasks}}
			state := &fakeShardingState{state: shardingState(false, map[string]sharding.Physical{"s1": {BelongsToNodes: []string{"n1"}}})}
			enq := &dropVectorIndexEnqueuer{clusterService: cluster, schemaState: state, logger: l}
			reconcileDroppedVectorIndexes(context.Background(), classes, enq, l)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			tc.pass(logger)
			require.Len(t, hook.AllEntries(), 1)
			msg := hook.LastEntry().Message
			require.True(t, strings.HasPrefix(msg, "3 "), msg)
			require.Contains(t, msg, "task-0, task-1, task-2")
		})
	}
}
