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
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
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
