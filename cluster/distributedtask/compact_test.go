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
	"maps"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

type compactTestHeader struct {
	Name       string   `json:"collection"`
	Tenants    []string `json:"tenants,omitempty"`
	Generation uint64   `json:"generation,omitempty"`
}

func (h compactTestHeader) Collection() string             { return h.Name }
func (h compactTestHeader) VisiblePayload() map[string]any { return nil }
func (h compactTestHeader) SwitchedByView() bool           { return h.Generation > 0 }

type compactTestCodec struct{}

func (compactTestCodec) CheckConflict(TaskHeader, []*Task) error { return nil }

func (compactTestCodec) DecodeHeader(payload []byte) (TaskHeader, error) {
	var h compactTestHeader
	err := json.Unmarshal(payload, &h)
	return h, err
}

func (compactTestCodec) DecodeCollection(payload []byte) (string, error) {
	var h compactTestHeader
	err := json.Unmarshal(payload, &h)
	return h.Name, err
}

func (compactTestCodec) RenameCollection(payload []byte, collection string) ([]byte, error) {
	var h compactTestHeader
	if err := json.Unmarshal(payload, &h); err != nil {
		return nil, err
	}
	h.Name = collection
	return json.Marshal(h)
}

var compactAt = time.UnixMilli(1_700_000_000_000).UTC()

func newCompactTestManager() *Manager {
	return NewManager(ManagerParameters{ConflictDetectors: map[string]ConflictDetector{
		"reindex": compactTestCodec{}, "other": compactTestCodec{},
	}})
}

func addCompactTestTask(t *testing.T, m *Manager, ns, id, payload string, barrier bool, specs ...*cmd.UnitSpec) {
	t.Helper()
	require.NoError(t, m.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace: ns, Id: id, Payload: []byte(payload), SubmittedAtUnixMillis: compactAt.UnixMilli(),
		UnitSpecs: specs, NeedsPreparationBarrier: barrier,
	}), 1))
}

func finishCompactTestUnit(t *testing.T, m *Manager, ns, id, node, unit, errMsg string) {
	t.Helper()
	require.NoError(t, m.RecordUnitCompletion(toCmd(t, &cmd.RecordDistributedTaskUnitCompletionRequest{
		Namespace: ns, Id: id, Version: 1, NodeId: node, UnitId: unit, Error: errMsg,
		FinishedAtUnixMillis: compactAt.UnixMilli(),
	})))
}

func ackCompactTestGroup(t *testing.T, m *Manager, id, group, node string, success bool) {
	t.Helper()
	require.NoError(t, m.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
		Namespace: "reindex", Id: id, Version: 1, GroupId: &group, NodeId: node,
		Success: success, Error: errIfFailed(success), AckedAtUnixMillis: compactAt.UnixMilli(),
	})))
}

func spec(id, group, node string) *cmd.UnitSpec {
	return &cmd.UnitSpec{Id: id, GroupId: group, Node: node}
}

func TestManager_LocalEndedTasks(t *testing.T) {
	m := NewManager(ManagerParameters{
		ConflictDetectors: map[string]ConflictDetector{"reindex": compactTestCodec{}, "other": compactTestCodec{}},
		TaskRules:         map[string]TaskRules{"reindex": &endRecordingRules{}},
		ClassLive:         func(string) map[string]map[string]uint64 { return nil },
	})
	const gen2 = `{"collection":"C","generation":2}`
	addCompactTestTask(t, m, "reindex", "failed", gen2, false,
		spec("u1", "g1", "n1"), spec("u2", "g1", "n1"), spec("u3", "g2", "n1"), spec("u4", "g3", "n2"))
	require.NoError(t, m.UpdateUnitProgress(toCmd(t, &cmd.UpdateDistributedTaskUnitProgressRequest{
		Namespace: "reindex", Id: "failed", Version: 1, NodeId: "n1", UnitId: "u2", Progress: 0.5,
		UpdatedAtUnixMillis: compactAt.UnixMilli(),
	})))
	finishCompactTestUnit(t, m, "reindex", "failed", "n1", "u1", "")
	finishCompactTestUnit(t, m, "reindex", "failed", "n1", "u3", "disk full")

	addCompactTestTask(t, m, "reindex", "no-generation", `{"collection":"C"}`, false, spec("u", "", "n1"))
	finishCompactTestUnit(t, m, "reindex", "no-generation", "n1", "u", "boom")

	addCompactTestTask(t, m, "reindex", "live", gen2, false, spec("u", "", "n1"))
	addCompactTestTask(t, m, "reindex", "other-collection", `{"collection":"D","generation":2}`, false, spec("u", "", "n1"))
	finishCompactTestUnit(t, m, "reindex", "other-collection", "n1", "u", "boom")
	addCompactTestTask(t, m, "other", "other-namespace", gen2, false, spec("u", "", "n1"))
	finishCompactTestUnit(t, m, "other", "other-namespace", "n1", "u", "boom")

	// Every dropped field is set, so the key check below sees each one dropped.
	stored := m.tasks["reindex"]["failed"]
	stored.PreparedGroups = map[string]map[string]PostCompletionAck{"g3": {"n2": {}}}
	stored.SwapDecidedGroups = map[string]time.Time{"g1": compactAt}
	stored.NodePreparationAcks = map[string]PostCompletionAck{"n1": {}}
	stored.PostCompletionAcks = map[string]PostCompletionAck{"n1": {}}

	copies := m.LocalEndedTasks("reindex", "C")
	byID := map[string]*Task{}
	for _, task := range copies {
		byID[task.ID] = task
	}

	for _, tc := range []struct {
		id       string
		captured bool
	}{
		{id: "failed", captured: true},
		{id: "no-generation"},
		{id: "live"},
		{id: "other-collection"},
		{id: "other-namespace"},
	} {
		t.Run("captures "+tc.id+"="+fmt.Sprint(tc.captured), func(t *testing.T) {
			_, ok := byID[tc.id]
			require.Equal(t, tc.captured, ok)
		})
	}

	t.Run("the copy carries the summary the end rules made of the units", func(t *testing.T) {
		require.Nil(t, stored.Units)
		require.Equal(t, &UnitSummary{
			Groups: 3,
			Counts: map[UnitStatus]int{
				UnitStatusCompleted: 1, UnitStatusInProgress: 1, UnitStatusFailed: 1, UnitStatusPending: 1,
			},
			Errors: []string{"u3: disk full"},
		}, byID["failed"].Summary)
	})

	t.Run("the copy keeps only the compact fields", func(t *testing.T) {
		var keys map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(mustMarshalJSON(t, byID["failed"]), &keys))
		require.ElementsMatch(t, []string{
			"namespace", "ID", "version", "payload", "needsPreparationBarrier", "status",
			"startedAt", "finishedAt", "error", "swapDecidedGroups", "summary",
		}, slices.Collect(maps.Keys(keys)))
	})
}

func TestManager_LocalTaskPrints(t *testing.T) {
	m := newCompactTestManager()
	addCompactTestTask(t, m, "reindex", "running", `{"collection":"C","generation":2}`, false, spec("u", "", "n1"))
	addCompactTestTask(t, m, "reindex", "failed", `{"collection":"c"}`, false, spec("u", "", "n1"))
	finishCompactTestUnit(t, m, "reindex", "failed", "n1", "u", "boom")
	addCompactTestTask(t, m, "reindex", "other-collection", `{"collection":"D"}`, false, spec("u", "", "n1"))
	addCompactTestTask(t, m, "other", "other-namespace", `{"collection":"C"}`, false, spec("u", "", "n1"))
	m.tasks["reindex"]["running"].SwapDecidedGroups = map[string]time.Time{"g1": compactAt, "g2": compactAt}

	require.Equal(t, []TaskPrint{
		{TaskDescriptor: TaskDescriptor{ID: "failed", Version: 1}, Status: TaskStatusFailed},
		{TaskDescriptor: TaskDescriptor{ID: "running", Version: 1}, Status: TaskStatusStarted, Switched: 2},
	}, m.LocalTaskPrints("reindex", "C"))
}

func TestManager_LocalEndedTasksCostFollowsTheSwitchedSet(t *testing.T) {
	const units, groups = 300_000, 100_000
	m := newCompactTestManager()
	specs := make([]*cmd.UnitSpec, units)
	for i := range specs {
		specs[i] = spec(fmt.Sprintf("u%d", i), fmt.Sprintf("g%d", i%groups), "n1")
	}
	addCompactTestTask(t, m, "reindex", "T", `{"collection":"C","generation":1}`, true, specs...)
	finishCompactTestUnit(t, m, "reindex", "T", "n1", "u0", "boom")
	switched := make(map[string]time.Time, groups)
	for i := range groups {
		switched[fmt.Sprintf("g%d", i)] = compactAt
	}
	m.tasks["reindex"]["T"].SwapDecidedGroups = switched

	cloneBytes := allocatedBytes(func() { _ = maps.Clone(switched) })
	var copies []*Task
	compactBytes := allocatedBytes(func() { copies = m.LocalEndedTasks("reindex", "C") })
	require.Len(t, copies, 1)
	require.LessOrEqual(t, compactBytes, 2*cloneBytes)
}

func allocatedBytes(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// A tenant delete writes an ended task's switched set while a backup encodes its copy.
func TestManager_LocalEndedTasksSharesNoSwitchedSet(t *testing.T) {
	m := newCompactTestManager()
	addCompactTestTask(t, m, "reindex", "T", `{"collection":"C","generation":1}`, true, spec("u1", "g1", "n1"), spec("u2", "g2", "n1"))
	finishCompactTestUnit(t, m, "reindex", "T", "n1", "u1", "")
	finishCompactTestUnit(t, m, "reindex", "T", "n1", "u2", "")
	ackCompactTestGroup(t, m, "T", "g1", "n1", true)
	ackCompactTestGroup(t, m, "T", "g2", "n1", false)
	require.Equal(t, TaskStatusFailed, m.tasks["reindex"]["T"].Status)

	copies := m.LocalEndedTasks("reindex", "C")
	require.Len(t, copies, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.ForgetGroupsForCollection("C", []string{"g1"})
	}()
	encoded := mustMarshalJSON(t, copies)
	<-done

	require.False(t, m.tasks["reindex"]["T"].GroupSwapDecided("g1"))
	var decoded []*Task
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Contains(t, decoded[0].SwapDecidedGroups, "g1")
	require.Contains(t, copies[0].SwapDecidedGroups, "g1")
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return raw
}
