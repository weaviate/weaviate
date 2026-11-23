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
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

func TestTaskListPassesLogUnreadablePayloadsAsOneLine(t *testing.T) {
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
		{"backup reindex gate", func(l logrus.FieldLogger) { NewShardReindexActivityLookup(tasks, l) }},
		{"drop-vector overlap check", func(l logrus.FieldLogger) { FirstActiveOverlappingDrop(tasks, "", "C", []string{"v"}, l) }},
		{"drop-vector has-active-drop", func(l logrus.FieldLogger) { ActiveDropCovers(tasks, "C", "v", l) }},
		{"drop-vector coverage inheritance", func(l logrus.FieldLogger) { newestDropEpoch(tasks, "C", []string{"v"}, l) }},
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

func TestDropVectorApplyChecksDoNotRelogRestoredUnreadablePayloads(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(t *testing.T, mgr *distributedtask.Manager)
	}{
		{"add tasks", func(t *testing.T, mgr *distributedtask.Manager) {
			for i := range 3 {
				payload, err := json.Marshal(DropVectorIndexTaskPayload{Collection: "C", Targets: []string{fmt.Sprintf("v%d", i)}, OpID: fmt.Sprintf("op-%d", i)})
				require.NoError(t, err)
				sub, err := json.Marshal(&api.AddDistributedTaskRequest{
					Namespace: DropVectorIndexNamespace, Id: fmt.Sprintf("new-%d", i), Payload: payload,
					UnitSpecs: []*api.UnitSpec{{Id: "u", GroupId: "s1"}},
				})
				require.NoError(t, err)
				require.NoError(t, mgr.AddTask(&api.ApplyRequest{SubCommand: sub}, uint64(10+i)))
			}
		}},
		{"remove vectors", func(t *testing.T, mgr *distributedtask.Manager) {
			for range 3 {
				require.NoError(t, mgr.CheckVectorConfigRemoval("C", []string{"v"}, nil))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := &distributedtask.Task{
				Namespace: DropVectorIndexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: "bad", Version: 1},
				Payload: []byte("not json"), Status: distributedtask.TaskStatusStarted,
			}
			snap, err := json.Marshal(map[string]map[string][]*distributedtask.Task{"tasks": {DropVectorIndexNamespace: {bad}}})
			require.NoError(t, err)
			logger, hook := logrustest.NewNullLogger()
			detector := NewDropVectorIndexDetector(logger)
			mgr := distributedtask.NewManager(distributedtask.ManagerParameters{
				Logger:                  logger,
				ConflictDetectors:       map[string]distributedtask.ConflictDetector{DropVectorIndexNamespace: detector},
				SchemaMutationDetectors: map[string]distributedtask.SchemaMutationDetector{DropVectorIndexNamespace: detector},
			})
			require.NoError(t, mgr.Restore(snap))
			require.Len(t, hook.AllEntries(), 1, "the restore reports the unreadable task once")

			tc.apply(t, mgr)
			require.Len(t, hook.AllEntries(), 1, "the apply checks add no line for it")
		})
	}
}
