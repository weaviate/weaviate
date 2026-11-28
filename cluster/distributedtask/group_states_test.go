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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// endRecordingRules compacts every ended task and takes t1 from the task named "older".
type endRecordingRules struct {
	ended []string
	born  bool
}

func (r *endRecordingRules) Admit(*Task, []*Task, map[string]map[string]uint64) (*Task, error) {
	return nil, nil
}

func (r *endRecordingRules) Ended(task *Task, existing []*Task) EndRuling {
	r.ended = append(r.ended, task.ID)
	ruling := EndRuling{Compact: true}
	for _, other := range existing {
		if other.ID == "older" && other != task {
			ruling.Forget = map[*Task][]string{other: {"t1"}}
		}
	}
	return ruling
}

func (r *endRecordingRules) BornSwitched(*Task) bool { return r.born }

func (r *endRecordingRules) IndexOff(string, string, string, []*Task) (uint64, EndRuling) {
	return 0, EndRuling{}
}

func (r *endRecordingRules) Units(TaskHeader, *sharding.State, map[string]time.Time) ([]UnitSpec, error) {
	return nil, nil
}

func newRulesTestManager(t *testing.T, rules *endRecordingRules, seed ...*Task) *Manager {
	t.Helper()
	m := NewManager(ManagerParameters{
		ConflictDetectors: map[string]ConflictDetector{"reindex": compactTestCodec{}},
		TaskRules:         map[string]TaskRules{"reindex": rules},
		ClassLive:         func(string) map[string]map[string]uint64 { return nil },
	})
	snapshot, err := json.Marshal(map[string]map[string][]*Task{"tasks": {"reindex": seed}})
	require.NoError(t, err)
	require.NoError(t, m.Restore(snapshot))
	rules.ended = nil
	return m
}

func seededTask(id string, status TaskStatus, barrier bool, switched ...string) *Task {
	task := &Task{
		Namespace: "reindex", TaskDescriptor: TaskDescriptor{ID: id, Version: 1}, Status: status,
		Payload: []byte(`{"collection":"C"}`), NeedsPreparationBarrier: barrier, StartedAt: compactAt,
		Units:             map[string]*Unit{"s1": {ID: "s1", GroupID: switched[0], NodeID: "n1", Status: UnitStatusFailed}},
		SwapDecidedGroups: map[string]time.Time{},
	}
	for _, g := range switched {
		task.SwapDecidedGroups[g] = compactAt
	}
	return task
}

func taskByID(m *Manager, id string) *Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tasks["reindex"][id]
}

func TestManager_EveryTerminalApplyRunsTheEndRules(t *testing.T) {
	succeed := func(t *testing.T, m *Manager) { finishCompactTestUnit(t, m, "reindex", "new", "n1", "u1", "") }
	for _, tc := range []struct {
		name    string
		barrier bool
		end     func(t *testing.T, m *Manager)
	}{
		{name: "RecordUnitCompletion fails", end: func(t *testing.T, m *Manager) {
			finishCompactTestUnit(t, m, "reindex", "new", "n1", "u1", "boom")
		}},
		{name: "MarkTaskFailed", end: func(t *testing.T, m *Manager) {
			succeed(t, m)
			require.NoError(t, m.MarkTaskFailed(toCmd(t, &cmd.MarkTaskFailedRequest{
				Namespace: "reindex", Id: "new", Version: 1, FailedAtUnixMillis: compactAt.UnixMilli(),
			})))
		}},
		{name: "CancelTask", end: func(t *testing.T, m *Manager) {
			require.NoError(t, m.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
				Namespace: "reindex", Id: "new", Version: 1, CancelledAtUnixMillis: compactAt.UnixMilli(),
			})))
		}},
		{name: "MarkTaskFinalized", end: func(t *testing.T, m *Manager) {
			succeed(t, m)
			require.NoError(t, m.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
				Namespace: "reindex", Id: "new", Version: 1, FinalizedAtUnixMillis: compactAt.UnixMilli(),
			})))
		}},
		{name: "RecordPreparationCompleteAck fails", barrier: true, end: func(t *testing.T, m *Manager) {
			succeed(t, m)
			ackCompactTestGroup(t, m, "new", "t2", "n1", false)
		}},
		{name: "RecordPostCompletionAck fails", end: func(t *testing.T, m *Manager) {
			succeed(t, m)
			require.NoError(t, m.RecordPostCompletionAck(toCmd(t, &cmd.RecordDistributedTaskPostCompletionAckRequest{
				Namespace: "reindex", Id: "new", Version: 1, NodeId: "n1", Success: false, Error: "boom",
				AckedAtUnixMillis: compactAt.UnixMilli(),
			})))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rules := &endRecordingRules{}
			m := newRulesTestManager(t, rules, seededTask("older", TaskStatusFailed, true, "t1", "t3"))
			addCompactTestTask(t, m, "reindex", "new", `{"collection":"C"}`, tc.barrier, spec("u1", "t2", "n1"))

			tc.end(t, m)

			require.Equal(t, []string{"new"}, rules.ended)
			ended := taskByID(m, "new")
			require.True(t, ended.Status.IsTerminal())
			require.Nil(t, ended.Units)
			require.NotNil(t, ended.Summary)
			require.Equal(t, map[string]time.Time{"t3": compactAt}, taskByID(m, "older").SwapDecidedGroups)
		})
	}
}

func TestManager_StateListenerHearsADecisionOnceOutsideTheLock(t *testing.T) {
	rules := &endRecordingRules{}
	m := newRulesTestManager(t, rules)
	var heard [][]string
	m.SetStateListener(func(_ string, _ TaskHeader, groups []string) {
		m.LocalDistributedTasks() // deadlocks if fired under the Manager's lock
		heard = append(heard, groups)
	})
	addCompactTestTask(t, m, "reindex", "T", `{"collection":"C"}`, true, spec("u1", "t1", "n1"), spec("u2", "t2", "n1"))
	finishCompactTestUnit(t, m, "reindex", "T", "n1", "u1", "")
	finishCompactTestUnit(t, m, "reindex", "T", "n1", "u2", "")
	heard = nil

	ackCompactTestGroup(t, m, "T", "t1", "n1", true)

	require.Equal(t, [][]string{{"t1"}}, heard)
}

func TestManager_RestoreCompactsAnEndedTaskOnce(t *testing.T) {
	rules := &endRecordingRules{}
	// t2 is switched without a unit, as a born or taken-over tenant is.
	m := newRulesTestManager(t, rules, seededTask("T", TaskStatusFailed, true, "t1", "t2"))
	rules.ended = []string{"T"} // newRulesTestManager clears what its Restore recorded
	addCompactTestTask(t, m, "reindex", "other", `{"collection":"C"}`, false, spec("u1", "t9", "n1"))
	finishCompactTestUnit(t, m, "reindex", "other", "n1", "u1", "")

	require.Equal(t, []string{"T"}, rules.ended)
	restored := taskByID(m, "T")
	require.Nil(t, restored.Units)
	require.Equal(t, 1, restored.Summary.Counts[UnitStatusFailed])
	require.Equal(t, 2, restored.Summary.Groups, "the summary counts the switched tenant without a unit")
	require.Contains(t, restored.SwapDecidedGroups, "t1")
}

func TestManager_BornTenantsDoNotStallTheBarrier(t *testing.T) {
	for _, born := range []bool{true, false} {
		rules := &endRecordingRules{born: born}
		m := newRulesTestManager(t, rules)
		addCompactTestTask(t, m, "reindex", "T", `{"collection":"C"}`, true, spec("u1", "t1", "n1"), spec("u2", "t2", "n1"))
		finishCompactTestUnit(t, m, "reindex", "T", "n1", "u1", "")
		finishCompactTestUnit(t, m, "reindex", "T", "n1", "u2", "")

		m.MarkTenantsBornSwitched("C", []string{"t3"})
		ackCompactTestGroup(t, m, "T", "t1", "n1", true)
		require.Equal(t, TaskStatusPreparing, taskByID(m, "T").Status, "t2 is undecided")
		ackCompactTestGroup(t, m, "T", "t2", "n1", true)

		task := taskByID(m, "T")
		require.Equal(t, born, task.GroupSwapDecided("t3"))
		require.Equal(t, TaskStatusSwapping, task.Status)
	}
}

func TestManager_ShardTaskStates(t *testing.T) {
	m := newRulesTestManager(t, &endRecordingRules{}, seededTask("compacted", TaskStatusFailed, true, "t1"))
	addCompactTestTask(t, m, "reindex", "mt", `{"collection":"C"}`, true,
		&cmd.UnitSpec{Id: "a", GroupId: "t1", Shard: "t1", Node: "n1"},
		&cmd.UnitSpec{Id: "b", GroupId: "t2", Shard: "t2", Node: "n2"})
	require.NoError(t, m.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
		Namespace: "reindex", Id: "mt", Version: 1, NodeId: "n1", UnitId: "a", Progress: 0.5,
		UpdatedAtUnixMillis: compactAt.UnixMilli(),
	})))
	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace: "reindex", Id: "single", Payload: []byte(`{"collection":"C"}`), SubmittedAtUnixMillis: compactAt.UnixMilli(),
		UnitSpecs: []*cmd.UnitSpec{{Id: "s1", GroupId: "", Shard: "s1", Node: "n1"}},
	}), 2))
	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace: "reindex", Id: "elsewhere", Payload: []byte(`{"collection":"D"}`), SubmittedAtUnixMillis: compactAt.UnixMilli(),
		UnitSpecs: []*cmd.UnitSpec{{Id: "x", GroupId: "t1", Shard: "t1", Node: "n1"}},
	}), 3))

	for _, tc := range []struct {
		shard, node string
		want        map[string]ShardTaskState
	}{
		{shard: "t1", node: "n1", want: map[string]ShardTaskState{
			"compacted": {Switched: true}, "mt": {HasUnit: true, LocalUnitStarted: true}, "single": {},
		}},
		{shard: "t2", node: "n1", want: map[string]ShardTaskState{
			"compacted": {}, "mt": {HasUnit: true}, "single": {},
		}},
		{shard: "s1", node: "n1", want: map[string]ShardTaskState{
			"compacted": {}, "mt": {}, "single": {HasUnit: true},
		}},
	} {
		t.Run(tc.shard+"@"+tc.node, func(t *testing.T) {
			got := map[string]ShardTaskState{}
			for _, st := range m.ShardTaskStates("reindex", "c", tc.shard, tc.node) {
				got[st.ID] = ShardTaskState{Switched: st.Switched, HasUnit: st.HasUnit, LocalUnitStarted: st.LocalUnitStarted}
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestManager_ADeletedTenantLeavesACompactedTask(t *testing.T) {
	m := newRulesTestManager(t, &endRecordingRules{}, seededTask("T", TaskStatusFailed, true, "t1", "t2"))
	require.Nil(t, taskByID(m, "T").Units, "the seed is compacted")

	m.ForgetGroupsForCollection("C", []string{"t1"})

	require.Equal(t, map[string]time.Time{"t2": compactAt}, taskByID(m, "T").SwapDecidedGroups)
}
