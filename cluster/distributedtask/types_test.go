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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTaskStatus_IsTerminal pins the terminal-status classification used
// by providers' recovery-replay short-circuit (see e.g.
// [ReindexProvider.OnGroupCompleted] in
// adapters/repos/db/reindex_provider.go and https://github.com/weaviate/0-weaviate-issues/issues/217).
// Adding a new TaskStatus value should require an explicit decision
// here; the default-case below ensures unknown values are non-terminal.
func TestTaskStatus_IsTerminal(t *testing.T) {
	tests := []struct {
		status   TaskStatus
		terminal bool
	}{
		{TaskStatusStarted, false},
		{TaskStatusPreparing, false},
		{TaskStatusSwapping, false},
		{TaskStatusFinished, true},
		{TaskStatusFailed, true},
		{TaskStatusCancelled, true},
		// Unknown / future statuses default to non-terminal so providers
		// don't accidentally skip work they're meant to do.
		{unknownFutureStatus, false},
		{TaskStatus(""), false},
	}
	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			assert.Equal(t, tc.terminal, tc.status.IsTerminal(),
				"%q.IsTerminal() should be %v", tc.status, tc.terminal)
		})
	}
}

// TestTask_Clone_WritesToCloneDoNotReachOriginal pins that adding or replacing
// an entry in any of a clone's maps leaves the original untouched. The scheduler
// does this each tick, and it must not change the FSM-stored Task. PreparedGroups'
// inner maps are shared, so writers replace an inner map instead of mutating it.
func TestTask_Clone_WritesToCloneDoNotReachOriginal(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	orig := withUnits(&Task{
		PostCompletionAcks: map[string]PostCompletionAck{
			"node-1": {Success: true, AckedAt: now},
		},
		PreparedGroups: map[string]map[string]PostCompletionAck{
			"g1": {"node-1": {Success: true, AckedAt: now}},
		},
		SwapDecidedGroups:   map[string]time.Time{"g0": now},
		SwapsOwed:           map[string][]string{"g0": {"node-1"}},
		NodePreparationAcks: map[string]PostCompletionAck{"node-1": {Success: true, AckedAt: now}},
	}, map[string]*Unit{
		"u-1": {ID: "u-1", NodeID: "node-1", Status: UnitStatusCompleted},
	})

	clone := orig.Clone()

	// Write to every map on the clone.
	clone.Units["u-1"].Status = UnitStatusFailed
	clone.PostCompletionAcks["node-1"] = PostCompletionAck{Success: false, Error: "swap"}
	clone.PostCompletionAcks["node-2"] = PostCompletionAck{Success: true}
	clone.PreparedGroups["g1"] = map[string]PostCompletionAck{"node-1": {Success: false, Error: "prep"}}
	clone.PreparedGroups["g2"] = map[string]PostCompletionAck{"node-2": {Success: true}}
	clone.SwapDecidedGroups["g1"] = now
	clone.SwapsOwed["g1"] = []string{"node-2"}
	clone.NodePreparationAcks["node-2"] = PostCompletionAck{Success: true}

	// Original must be untouched on EVERY map.
	assert.Equal(t, UnitStatusCompleted, orig.Units["u-1"].Status, "Units value-level deep copy")
	assert.True(t, orig.PostCompletionAcks["node-1"].Success, "PostCompletionAcks must not alias clone")
	assert.NotContains(t, orig.PostCompletionAcks, "node-2", "PostCompletionAcks must not alias clone")
	assert.True(t, orig.PreparedGroups["g1"]["node-1"].Success, "PreparedGroups must not alias clone")
	assert.NotContains(t, orig.PreparedGroups, "g2", "PreparedGroups must not alias clone")
	assert.NotContains(t, orig.SwapDecidedGroups, "g1", "SwapDecidedGroups must not alias clone")
	assert.NotContains(t, orig.SwapsOwed, "g1", "SwapsOwed must not alias clone")
	assert.NotContains(t, orig.NodePreparationAcks, "node-2", "NodePreparationAcks must not alias clone")
}

// Pins that every copy of a task answers per-group questions, and that a
// decode of the leader's list builds no index for a run this node holds.
func TestTask_GroupIndexSurvivesEveryCopy(t *testing.T) {
	newTask := func() *Task {
		return withUnits(&Task{
			Namespace: "ns", TaskDescriptor: TaskDescriptor{ID: "T", Version: 3},
			SwapDecidedGroups: map[string]time.Time{"g1": {}},
		}, map[string]*Unit{
			"a": {ID: "a", GroupID: "g1"}, "b": {ID: "b", GroupID: "g1"}, "c": {ID: "c", GroupID: "g2"},
		})
	}
	orig := newTask()
	holding := NewManager(ManagerParameters{})
	holding.tasks["ns"] = map[string]*Task{"T": orig}
	raw, err := holding.ListDistributedTasksPayload(t.Context())
	require.NoError(t, err)
	decode := func(m *Manager) *Task {
		tasks, err := m.DecodeTaskList(raw)
		require.NoError(t, err)
		return tasks["ns"][0]
	}
	shrunk := newTask()
	shrunk.forgetGroups([]string{"g2"})
	differentRun := NewManager(ManagerParameters{})
	differentRun.tasks["ns"] = map[string]*Task{"T": shrunk}
	restored := NewManager(ManagerParameters{})
	snap, err := holding.Snapshot()
	require.NoError(t, err)
	require.NoError(t, restored.Restore(snap))

	sameIndex := func(a, b *Task) bool {
		return reflect.ValueOf(a.groups).UnsafePointer() == reflect.ValueOf(b.groups).UnsafePointer()
	}
	for _, tc := range []struct {
		name   string
		task   *Task
		shares bool
	}{
		{name: "original", task: orig, shares: true},
		{name: "clone", task: orig.Clone(), shares: true},
		{name: "decoded where the run is held", task: decode(holding), shares: true},
		{name: "decoded where the run is unknown", task: decode(NewManager(ManagerParameters{}))},
		{name: "decoded where the run has fewer units", task: decode(differentRun)},
		{name: "restored", task: restored.tasks["ns"]["T"]},
	} {
		task, name := tc.task, tc.name
		assert.Equal(t, tc.shares, sameIndex(orig, task), name)
		assert.ElementsMatch(t, []string{"g1", "g2"}, task.Groups(), name)
		assert.False(t, task.AllGroupsSwapDecided(), name)
	}
}

// Pins that a tenant delete costs the tenant, not the run: it removes the
// tenant's units in place and leaves the index every copy shares untouched.
func TestTask_ForgetGroupsKeepsTheSharedIndex(t *testing.T) {
	task := withUnits(&Task{SwapDecidedGroups: map[string]time.Time{"g2": {}, "g3": {}}}, map[string]*Unit{
		"a": {ID: "a", GroupID: "g1", NodeID: "n1"},
		"b": {ID: "b", GroupID: "g2", NodeID: "n1"},
		"c": {ID: "c", GroupID: "g2", NodeID: "n2"},
		"d": {ID: "d", GroupID: "g3", NodeID: "n1"},
	})
	clone := task.Clone()
	index := reflect.ValueOf(task.groups).UnsafePointer()

	task.forgetGroups([]string{"g2", "unknown"})
	task.forgetGroups([]string{"g2"})

	require.Equal(t, index, reflect.ValueOf(task.groups).UnsafePointer(), "the index is not rebuilt")
	require.ElementsMatch(t, []string{"g1", "g3"}, task.Groups())
	require.Empty(t, task.LocalGroupUnitIDs("g2", "n1"))
	require.Empty(t, task.groupNodes("g2"))
	require.False(t, task.AllGroupsSwapDecided(), "g1 is still undecided")
	task.SwapDecidedGroups["g1"] = time.Time{}
	require.True(t, task.AllGroupsSwapDecided())

	require.ElementsMatch(t, []string{"g1", "g2", "g3"}, clone.Groups(), "a copy taken before keeps the tenant")
}

func withUnits(task *Task, units map[string]*Unit) *Task {
	task.Units = units
	task.indexGroups()
	return task
}

func unitPerGroup(groups []string) map[string]*Unit {
	units := map[string]*Unit{}
	for i, g := range groups {
		id := fmt.Sprintf("u-%d", i)
		units[id] = &Unit{ID: id, GroupID: g, NodeID: "node-a"}
	}
	return units
}
