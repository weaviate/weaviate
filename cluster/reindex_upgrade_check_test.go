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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
)

type upgradeRun struct {
	version  uint64
	status   distributedtask.TaskStatus
	mt       db.ReindexMigrationType
	gen      uint64
	switched []string
	tenants  []string
	class    string
}

func (r upgradeRun) task(t *testing.T) *distributedtask.Task {
	class := r.class
	if class == "" {
		class = "Movies"
	}
	p := db.ReindexTaskPayload{
		MigrationType: r.mt, Collection: class, Properties: []string{"title"}, Tenants: r.tenants,
		ViewServed: r.gen > 0, Generation: r.gen,
	}
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	task := &distributedtask.Task{
		Namespace: db.ReindexNamespace, Status: r.status, SwapDecidedGroups: map[string]time.Time{},
		TaskDescriptor: distributedtask.TaskDescriptor{ID: string(r.mt), Version: r.version},
	}
	for _, g := range r.switched {
		task.SwapDecidedGroups[g] = time.Unix(1, 0)
	}
	task.SetHeader(raw, db.ReindexTaskHeader{ReindexTaskPayload: p})
	return task
}

func TestUnfinishedLegacyRun(t *testing.T) {
	const (
		tok     = db.ReindexTypeChangeTokenization
		rebuild = db.ReindexTypeRebuildSearchable
	)
	var (
		failed    = distributedtask.TaskStatusFailed
		cancelled = distributedtask.TaskStatusCancelled
		finished  = distributedtask.TaskStatusFinished
		started   = distributedtask.TaskStatusStarted
	)
	partial := upgradeRun{version: 5, status: cancelled, mt: tok, switched: []string{"t1"}}
	for _, tc := range []struct {
		name        string
		runs        []upgradeRun // the first is checked
		refuses     bool
		offerCancel bool
	}{
		{name: "cancelled with switched tenants", runs: []upgradeRun{partial}, refuses: true},
		{name: "finished", runs: []upgradeRun{{version: 5, status: finished, mt: tok}}},
		{name: "a generation run failed with switched tenants", runs: []upgradeRun{{version: 5, status: failed, mt: tok, gen: 2, switched: []string{"t1"}}}},
		{name: "started", runs: []upgradeRun{{version: 5, status: started, mt: tok}}, refuses: true, offerCancel: true},
		{name: "started with switched tenants", runs: []upgradeRun{{version: 5, status: started, mt: tok, switched: []string{"t1"}}}, refuses: true},
		{name: "failed without switched tenants", runs: []upgradeRun{{version: 5, status: failed, mt: tok}}},
		{name: "a newer finished run of the same type covers it", runs: []upgradeRun{partial, {version: 9, status: finished, mt: tok}}},
		{
			name: "a newer finished rebuild covers a partial rebuild",
			runs: []upgradeRun{{version: 5, status: cancelled, mt: rebuild, switched: []string{"t1"}}, {version: 9, status: finished, mt: rebuild}},
		},
		{name: "a newer run building fewer index types", runs: []upgradeRun{partial, {version: 9, status: finished, mt: rebuild}}, refuses: true},
		{name: "an older finished run", runs: []upgradeRun{partial, {version: 3, status: finished, mt: tok}}, refuses: true},
		{name: "a newer failed run", runs: []upgradeRun{partial, {version: 9, status: failed, mt: tok}}, refuses: true},
		{name: "a newer finished run on other tenants", runs: []upgradeRun{partial, {version: 9, status: finished, mt: tok, tenants: []string{"t2"}}}, refuses: true},
		{name: "a newer finished run on its tenants", runs: []upgradeRun{partial, {version: 9, status: finished, mt: tok, tenants: []string{"t1", "t2"}}}},
		{name: "a newer finished run of another collection", runs: []upgradeRun{partial, {version: 9, status: finished, mt: tok, class: "Books"}}, refuses: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := make([]*distributedtask.Task, len(tc.runs))
			for i, r := range tc.runs {
				tasks[i] = r.task(t)
			}
			msg := unfinishedLegacyRun(tasks[0], tasks)
			require.Equal(t, tc.refuses, msg != "", msg)
			if tc.refuses {
				require.Contains(t, msg, `"Movies"`)
				require.Contains(t, msg, "title")
				require.Contains(t, msg, tasks[0].ID)
				require.Equal(t, tc.offerCancel, strings.Contains(msg, "cancel it there"), msg)
			}
		})
	}
}

// The check runs on the process's first DB load only, against the tasks the
// log holds at that point.
func TestReloadDBFromSchemaRefusesUnfinishedLegacyRuns(t *testing.T) {
	partial := upgradeRun{version: 5, status: distributedtask.TaskStatusCancelled, mt: db.ReindexTypeChangeTokenization, switched: []string{"t1"}}
	resubmit := upgradeRun{version: 9, status: distributedtask.TaskStatusFinished, mt: db.ReindexTypeChangeTokenization}
	for _, tc := range []struct {
		name     string
		runs     []upgradeRun
		loaded   bool // a runtime snapshot install
		refusals int
	}{
		{name: "first load", runs: []upgradeRun{partial}, refusals: 1},
		{name: "snapshot install after the first load", runs: []upgradeRun{partial}, loaded: true},
		{name: "first load after the resubmit landed", runs: []upgradeRun{partial, resubmit}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ms := NewMockStore(t, t.Name(), 9092, func(c *Config) {
				c.DistributedTaskConflictDetectors = map[string]distributedtask.ConflictDetector{db.ReindexNamespace: db.ReindexDetector{}}
			})
			hook := logrustest.NewLocal(ms.logger)
			exits := 0
			ms.logger.ExitFunc = func(int) { exits++ }
			st := ms.Store(func(m *MockStore) { m.indexer.On("TriggerSchemaUpdateCallbacks").Return() })
			tasks := make([]*distributedtask.Task, len(tc.runs))
			for i, r := range tc.runs {
				tasks[i] = r.task(t)
			}
			raw, err := json.Marshal(map[string]any{"tasks": map[string][]*distributedtask.Task{db.ReindexNamespace: tasks}})
			require.NoError(t, err)
			require.NoError(t, st.distributedTasksManager.Restore(raw))
			st.dbLoaded.Store(tc.loaded)

			st.reloadDBFromSchema()

			require.Equal(t, tc.refusals, exits)
			var fatal []string
			for _, e := range hook.AllEntries() {
				if e.Level == logrus.FatalLevel {
					fatal = append(fatal, e.Message)
				}
			}
			require.Len(t, fatal, tc.refusals)
		})
	}
}
