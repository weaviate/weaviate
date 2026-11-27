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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

func TestReindexTaskRulesAdmit(t *testing.T) {
	const word, field = "word", "field"
	ended := func(id string, version uint64, status distributedtask.TaskStatus, p ReindexTaskPayload, switched ...string) *distributedtask.Task {
		p.ViewServed = true
		raw, err := json.Marshal(p)
		require.NoError(t, err)
		task := &distributedtask.Task{
			Namespace: ReindexNamespace, TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: version},
			Status: status, Payload: raw, SwapDecidedGroups: map[string]time.Time{},
		}
		for _, g := range switched {
			task.SwapDecidedGroups[g] = time.UnixMilli(1)
		}
		return task
	}
	run := func(collection, target string, gen uint64, props ...string) ReindexTaskPayload {
		return ReindexTaskPayload{
			Collection: collection, MigrationType: ReindexTypeChangeTokenization, Properties: props,
			TargetTokenization: target, Generation: gen,
		}
	}
	titleTo := func(target string, gen uint64) ReindexTaskPayload { return run("C", target, gen, "title") }

	for _, tc := range []struct {
		name     string
		live     map[string]map[string]uint64
		existing []*distributedtask.Task
		legacy   bool // the new run's payload has no ViewServed, as a v1.39.x entry does
		submit   ReindexTaskPayload

		wantErr      error
		wantGen      uint64
		wantSwitched []string
		wantGone     []string
	}{
		{
			name:     "an ended task's generation beats a lower live one",
			live:     map[string]map[string]uint64{"title": {"filterable": 2}},
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFinished, titleTo(field, 3))},
			submit:   titleTo(word, 0),
			wantGen:  4,
		},
		{
			name:     "a live generation beats a lower task one",
			live:     map[string]map[string]uint64{"title": {"filterable": 1, "searchable": 5}},
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFinished, titleTo(field, 3))},
			submit:   titleTo(word, 0),
			wantGen:  6,
		},
		{
			name:         "a cancelled same-target run is taken over",
			existing:     []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusCancelled, titleTo(word, 2), "t1")},
			submit:       titleTo(word, 0),
			wantGen:      2,
			wantSwitched: []string{"t1"},
			wantGone:     []string{"old"},
		},
		{
			name:     "a finished scoped same-target run is not taken over",
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFinished, titleTo(word, 2), "t1")},
			submit:   titleTo(word, 0),
			wantGen:  3,
		},
		{
			name: "of two failed same-target runs the higher generation is taken over",
			existing: []*distributedtask.Task{
				ended("low", 1, distributedtask.TaskStatusFailed, titleTo(word, 2), "t1"),
				ended("high", 2, distributedtask.TaskStatusFailed, titleTo(word, 3), "t2"),
			},
			submit:       titleTo(word, 0),
			wantGen:      3,
			wantSwitched: []string{"t2"},
			wantGone:     []string{"high"},
		},
		{
			name: "a run on a superset of the properties is neither counted nor taken over",
			existing: []*distributedtask.Task{
				ended("wide", 1, distributedtask.TaskStatusFailed, run("C", word, 4, "title", "body"), "t1"),
			},
			submit:  titleTo(word, 0),
			wantGen: 1,
		},
		{
			name:     "another target over a failed run with switched tenants is refused",
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFailed, titleTo(field, 2), "t1")},
			submit:   titleTo(word, 0),
			wantErr:  distributedtask.ErrTaskPartlyMigrated,
		},
		{
			name: "a same-named property in another collection never matches",
			existing: []*distributedtask.Task{
				ended("other", 1, distributedtask.TaskStatusCancelled, run("A", word, 2, "title"), "t1"),
			},
			submit:  run("B", word, 0, "title"),
			wantGen: 1,
		},
		{
			name:     "a payload without ViewServed is admitted unchanged",
			live:     map[string]map[string]uint64{"title": {"filterable": 2}},
			existing: []*distributedtask.Task{ended("old", 1, distributedtask.TaskStatusFailed, titleTo(field, 2), "t1")},
			legacy:   true,
			submit:   titleTo(word, 0),
			wantGen:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot, err := json.Marshal(map[string]map[string][]*distributedtask.Task{"tasks": {ReindexNamespace: tc.existing}})
			require.NoError(t, err)
			logger, _ := test.NewNullLogger()
			m := distributedtask.NewManager(distributedtask.ManagerParameters{
				Logger:            logger,
				ConflictDetectors: map[string]distributedtask.ConflictDetector{ReindexNamespace: ReindexDetector{}},
				TaskRules:         map[string]distributedtask.TaskRules{ReindexNamespace: ReindexTaskRules{}},
				ClassLive:         func(string) map[string]map[string]uint64 { return tc.live },
			})
			require.NoError(t, m.Restore(snapshot))
			before := taskIDs(m)

			submit := tc.submit
			submit.ViewServed = !tc.legacy
			payload, err := json.Marshal(submit)
			require.NoError(t, err)
			raw, err := json.Marshal(&api.AddDistributedTaskRequest{
				Namespace: ReindexNamespace, Id: "new", Payload: payload, SubmittedAtUnixMillis: 1,
				UnitSpecs: []*api.UnitSpec{
					{Id: "u1", GroupId: "t1", Shard: "t1", Node: "n1"},
					{Id: "u2", GroupId: "t2", Shard: "t2", Node: "n1"},
				},
			})
			require.NoError(t, err)
			err = m.AddTask(&api.ApplyRequest{SubCommand: raw}, 10)

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, before, taskIDs(m))
				return
			}
			require.NoError(t, err)
			tasks := m.LocalDistributedTasks()[ReindexNamespace]
			added := findTask(tasks, "new")
			require.NotNil(t, added)
			h, ok := ReindexHeaderOf(added)
			require.True(t, ok)
			require.Equal(t, tc.wantGen, h.Generation)
			if tc.legacy {
				require.JSONEq(t, string(payload), string(added.Payload))
			}
			var stored ReindexTaskPayload
			require.NoError(t, json.Unmarshal(added.Payload, &stored))
			require.Equal(t, tc.wantGen, stored.Generation, "the payload carries the generation the header does")

			require.Len(t, added.SwapDecidedGroups, len(tc.wantSwitched))
			for _, g := range tc.wantSwitched {
				require.Contains(t, added.SwapDecidedGroups, g)
			}
			for _, u := range added.Units {
				require.NotContains(t, tc.wantSwitched, u.GroupID, "a switched tenant gets no unit")
			}
			require.Len(t, added.Units, 2-len(tc.wantSwitched))

			want := append(before, "new")
			for _, gone := range tc.wantGone {
				want = removeString(want, gone)
			}
			require.ElementsMatch(t, want, taskIDs(m))
		})
	}
}

func taskIDs(m *distributedtask.Manager) []string {
	var ids []string
	for _, task := range m.LocalDistributedTasks()[ReindexNamespace] {
		ids = append(ids, task.ID)
	}
	return ids
}

func findTask(tasks []*distributedtask.Task, id string) *distributedtask.Task {
	for _, task := range tasks {
		if task.ID == id {
			return task
		}
	}
	return nil
}

func removeString(s []string, v string) []string {
	out := s[:0:0]
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}
