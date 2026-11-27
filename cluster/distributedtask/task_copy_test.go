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

package distributedtask

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"runtime"
	"slices"
	"testing"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

const copyNamespace = "ns"

type copyPayload struct {
	Collection string   `json:"collection"`
	Targets    []string `json:"targets"`
}

func newCopyTestManager() *Manager {
	logger, _ := logrustest.NewNullLogger()
	decode := decodeWith(func(raw []byte) (string, bool) {
		var p copyPayload
		return p.Collection, json.Unmarshal(raw, &p) == nil && p.Collection != ""
	})
	m := NewManager(ManagerParameters{Logger: logger, ConflictDetectors: map[string]ConflictDetector{copyNamespace: decode}})
	m.RegisterTargetVectorExtractor(copyNamespace, func(raw []byte) (string, []string, bool) {
		var p copyPayload
		return p.Collection, p.Targets, json.Unmarshal(raw, &p) == nil
	})
	return m
}

// copiedTask is what the scheduler can observe of one task.
type copiedTask struct {
	JSON       string
	Header     TaskHeader
	Groups     map[string][]string
	GroupCount int
}

func observe(t *testing.T, tasks map[string]map[TaskDescriptor]*Task) map[string]copiedTask {
	t.Helper()
	out := map[string]copiedTask{}
	for namespace, byDesc := range tasks {
		for desc, task := range byDesc {
			raw, err := json.Marshal(task)
			require.NoError(t, err)
			out[fmt.Sprintf("%s/%s/%d", namespace, desc.ID, desc.Version)] = copiedTask{
				JSON: string(raw), Header: task.header, Groups: task.groups, GroupCount: task.groupCount,
			}
		}
	}
	return out
}

func requireCopyMatches(t *testing.T, m *Manager, c *TaskCopy) {
	t.Helper()
	want := map[string]map[TaskDescriptor]*Task{}
	for namespace, list := range m.LocalDistributedTasks() {
		want[namespace] = map[TaskDescriptor]*Task{}
		for _, task := range list {
			want[namespace][task.TaskDescriptor] = task
		}
	}
	require.Equal(t, observe(t, want), observe(t, c.Tasks()))
}

// copyRun drives random applies against one manager.
type copyRun struct {
	t        *testing.T
	m        *Manager
	r        *rand.Rand
	seq      uint64
	snapshot []byte
}

var (
	copyTaskIDs = []string{"a", "b", "c"}
	copyGroups  = []string{"g0", "g1"}
	copyNodes   = []string{"n1", "n2"}
)

func (run *copyRun) pick(list []string) string { return list[run.r.IntN(len(list))] }

func (run *copyRun) task() (string, uint64) {
	id := run.pick(copyTaskIDs)
	if task := run.m.tasks[copyNamespace][id]; task != nil {
		return id, task.Version
	}
	return id, 0
}

// unit returns a unit ID of the form group/node and the node that owns it.
func (run *copyRun) unit() (string, string) {
	node := run.pick(copyNodes)
	if run.r.IntN(10) == 0 {
		return run.pick(copyGroups) + "/" + node, run.pick(copyNodes)
	}
	return run.pick(copyGroups) + "/" + node, node
}

func (run *copyRun) step() {
	t, m, now := run.t, run.m, int64(run.r.IntN(1_000_000))
	id, version := run.task()
	unitID, node := run.unit()
	group := run.pick(copyGroups)
	collection := run.pick([]string{"C1", "C2"})
	switch run.r.IntN(20) {
	case 0:
		run.seq++
		payload, err := json.Marshal(copyPayload{Collection: collection, Targets: []string{run.pick([]string{"v1", "v2"})}})
		require.NoError(t, err)
		req := &cmd.AddDistributedTaskRequest{
			Namespace: copyNamespace, Id: id, Payload: payload, SubmittedAtUnixMillis: now,
			NeedsPreparationBarrier: run.r.IntN(2) == 0,
		}
		for _, g := range copyGroups {
			for _, n := range copyNodes {
				if run.r.IntN(5) == 0 {
					req.UnitIds = append(req.UnitIds, g+"/"+n)
				} else {
					req.UnitSpecs = append(req.UnitSpecs, &cmd.UnitSpec{Id: g + "/" + n, GroupId: g})
				}
			}
		}
		if len(req.UnitSpecs) > 0 {
			req.UnitIds = nil
		}
		_ = m.AddTask(toCmd(t, req), run.seq)
	case 1, 2, 3, 4:
		_ = m.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace: copyNamespace, Id: id, Version: version, UnitId: unitID, NodeId: node,
			FinishedAtUnixMillis: now,
		}))
	case 5:
		_ = m.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace: copyNamespace, Id: id, Version: version, UnitId: unitID, NodeId: node,
			Error: "boom", FinishedAtUnixMillis: now,
		}))
	case 6:
		_ = m.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
			Namespace: copyNamespace, Id: id, Version: version, UnitId: unitID, NodeId: node,
			Progress: run.r.Float32(), UpdatedAtUnixMillis: now,
		}))
	case 7, 8, 9, 10:
		req := &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
			Namespace: copyNamespace, Id: id, Version: version, NodeId: node,
			Success: run.r.IntN(8) != 0, Error: "prep", AckedAtUnixMillis: now,
		}
		if run.r.IntN(4) != 0 {
			req.GroupId = &group
		}
		_ = m.RecordPreparationCompleteAck(toCmd(t, req))
	case 11, 12:
		_ = m.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
			Namespace: copyNamespace, Id: id, Version: version, GroupId: group, NodeId: node,
			Success: run.r.IntN(8) != 0, Error: "swap", AckedAtUnixMillis: now,
		}))
	case 13:
		_ = m.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
			Namespace: copyNamespace, Id: id, Version: version, FinalizedAtUnixMillis: now,
		}))
	case 14:
		_ = m.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
			Namespace: copyNamespace, Id: id, Version: version, Error: "late", FailedAtUnixMillis: now,
		}))
	case 15:
		_ = m.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
			Namespace: copyNamespace, Id: id, Version: version, CancelledAtUnixMillis: now,
		}))
	case 16:
		_ = m.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{Namespace: copyNamespace, Id: id, Version: version}))
	case 17:
		m.ForgetGroupsForCollection(collection, []string{group})
	case 18:
		if run.r.IntN(2) == 0 {
			m.DeleteTasksForCollection(collection)
		} else {
			_, _ = m.PurgeTasksForCollectionTargetVectors(collection, []string{run.pick([]string{"v1", "v2"})})
		}
	case 19:
		if run.snapshot == nil || run.r.IntN(2) == 0 {
			snapshot, err := m.Snapshot()
			require.NoError(t, err)
			run.snapshot = snapshot
		} else {
			require.NoError(t, m.Restore(run.snapshot))
		}
	}
}

// After any sequence of applies, an updated copy shows what the FSM holds.
func TestTaskCopyFollowsEveryApply(t *testing.T) {
	for seed := range uint64(1000) {
		m := newCopyTestManager()
		run := &copyRun{t: t, m: m, r: rand.New(rand.NewPCG(seed, 1))}
		var c TaskCopy
		for range 300 {
			run.step()
			if run.r.IntN(4) == 0 {
				require.NoError(t, m.UpdateTaskCopy(&c))
				requireCopyMatches(t, m, &c)
			}
		}
		require.NoError(t, m.UpdateTaskCopy(&c))
		requireCopyMatches(t, m, &c)
	}
}

func TestUpdateTaskCopyCostFollowsChanges(t *testing.T) {
	m := newCopyTestManager()
	ids := make([]string, 300_000)
	for i := range ids {
		ids[i] = fmt.Sprintf("u%d", i)
	}
	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{Namespace: copyNamespace, Id: "big", UnitIds: ids}), 1))
	var c TaskCopy
	require.NoError(t, m.UpdateTaskCopy(&c))

	for _, id := range ids[:10] {
		require.NoError(t, m.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
			Namespace: copyNamespace, Id: "big", Version: 1, UnitId: id, NodeId: "n1",
		})))
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	require.NoError(t, m.UpdateTaskCopy(&c))
	runtime.ReadMemStats(&after)
	require.Less(t, after.Mallocs-before.Mallocs, uint64(1000))
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
	requireCopyMatches(t, m, &c)
}

func TestTaskCopyDropsRestoredAndCleanedTasks(t *testing.T) {
	add := func(t *testing.T, m *Manager, id string) {
		require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{Namespace: copyNamespace, Id: id, UnitIds: []string{"u"}}), 1))
	}
	for _, tt := range []struct {
		name   string
		change func(t *testing.T, m *Manager, c *TaskCopy)
		want   []string
	}{
		{
			name: "restore replaces every task",
			change: func(t *testing.T, m *Manager, _ *TaskCopy) {
				other := newCopyTestManager()
				add(t, other, "b")
				snapshot, err := other.Snapshot()
				require.NoError(t, err)
				require.NoError(t, m.Restore(snapshot))
			},
			want: []string{"b"},
		},
		{
			name: "clean-up removes the task",
			change: func(t *testing.T, m *Manager, c *TaskCopy) {
				require.NoError(t, m.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{Namespace: copyNamespace, Id: "a", Version: 1})))
				require.NoError(t, m.UpdateTaskCopy(c))
				require.NoError(t, m.CleanUpTask(toCmd(t, &cmd.CleanUpDistributedTaskRequest{Namespace: copyNamespace, Id: "a", Version: 1})))
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := newCopyTestManager()
			add(t, m, "a")
			var c TaskCopy
			require.NoError(t, m.UpdateTaskCopy(&c))

			tt.change(t, m, &c)
			require.NoError(t, m.UpdateTaskCopy(&c))
			var got []string
			for desc := range c.Tasks()[copyNamespace] {
				got = append(got, desc.ID)
			}
			slices.Sort(got)
			require.Equal(t, tt.want, got)
		})
	}
}
