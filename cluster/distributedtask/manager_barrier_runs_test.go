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
	"cmp"
	"testing"

	"github.com/stretchr/testify/require"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func TestManager_BarrierRunsOf(t *testing.T) {
	const ns = "ns"
	type run struct {
		id                   string
		version              uint64
		ungrouped, noBarrier bool
		end                  TaskStatus
	}
	type want struct {
		id      string
		status  TaskStatus
		decided bool
	}
	started := run{id: "task1", version: 10, end: TaskStatusStarted}
	for _, tc := range []struct {
		name                         string
		runs                         []run
		namespace, collection, shard string
		want                         []want
	}{
		{name: "a running task that decided the group", runs: []run{started}, want: []want{{"task1", TaskStatusStarted, true}}},
		{
			name: "a cancelled task keeps its decided group",
			runs: []run{{id: "task1", version: 10, end: TaskStatusCancelled}}, want: []want{{"task1", TaskStatusCancelled, true}},
		},
		{
			name: "a failed task keeps its decided group",
			runs: []run{{id: "task1", version: 10, end: TaskStatusFailed}}, want: []want{{"task1", TaskStatusFailed, true}},
		},
		{
			name: "a task without groups answers for every shard",
			runs: []run{{id: "task1", version: 10, ungrouped: true, end: TaskStatusSwapping}}, shard: "any",
			want: []want{{"task1", TaskStatusSwapping, true}},
		},
		{name: "an undecided group", runs: []run{started}, shard: "g2"},
		{
			name: "a finished task counts even where it decided nothing",
			runs: []run{{id: "task1", version: 10, end: TaskStatusFinished}}, shard: "g3",
			want: []want{{"task1", TaskStatusFinished, false}},
		},
		{
			name: "oldest first",
			runs: []run{{id: "task2", version: 12, end: TaskStatusStarted}, started},
			want: []want{{"task1", TaskStatusStarted, true}, {"task2", TaskStatusStarted, true}},
		},
		{name: "another collection", runs: []run{started}, collection: "Other"},
		{name: "another namespace", runs: []run{started}, namespace: "ns2"},
		{name: "a task without the barrier", runs: []run{{id: "task1", version: 10, noBarrier: true, end: TaskStatusStarted}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t)
			decode := decodeWith(func(p []byte) (string, bool) { return string(p[1 : len(p)-1]), true })
			h.headerDecoders = map[string]HeaderDecoder{ns: decode, "ns2": decode}
			h.init(t)
			for _, r := range tc.runs {
				addDecidedRun(t, h, ns, r.id, r.version, r.ungrouped, !r.noBarrier, r.end)
			}
			var expected []BarrierRun
			for _, w := range tc.want {
				version := map[string]uint64{"task1": 10, "task2": 12}[w.id]
				expected = append(expected, BarrierRun{
					TaskDescriptor: TaskDescriptor{ID: w.id, Version: version}, Status: w.status,
					GroupDecided: w.decided, Header: testHeader("Coll"),
				})
			}
			require.Equal(t, expected, h.manager.BarrierRunsOf(
				cmp.Or(tc.namespace, ns), cmp.Or(tc.collection, "coll"), cmp.Or(tc.shard, "g1")))
		})
	}
}

// addDecidedRun applies a task whose group g1 (or "" without groups) is decided,
// then drives it to end.
func addDecidedRun(t *testing.T, h *testHarness, ns, id string, version uint64, ungrouped, barrier bool, end TaskStatus) {
	t.Helper()
	g1, g2 := "g1", "g2"
	if ungrouped {
		g1, g2 = "", ""
	}
	u1, u2 := id+"-u1", id+"-u2"
	require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
		Namespace: ns, Id: id, Payload: []byte(`"Coll"`), SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
		UnitSpecs: []*cmd.UnitSpec{{Id: u1, GroupId: g1}, {Id: u2, GroupId: g2}}, NeedsPreparationBarrier: barrier,
	}), version))
	ack := func(group string) {
		require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
			Namespace: ns, Id: id, Version: version, GroupId: &group, NodeId: "n2", Success: true,
			AckedAtUnixMillis: h.clock.Now().UnixMilli(),
		})))
	}
	completeUnit(t, h, ns, id, version, "n2", u1)
	if ungrouped || end == TaskStatusFinished {
		completeUnit(t, h, ns, id, version, "n2", u2)
	}
	ack(g1)
	require.True(t, h.manager.tasks[ns][id].GroupSwapDecided(g1), "the run must start from a decided group")
	now := h.clock.Now().UnixMilli()
	switch end {
	case TaskStatusCancelled:
		require.NoError(t, h.manager.CancelTask(toCmd(t, &cmd.CancelDistributedTaskRequest{
			Namespace: ns, Id: id, Version: version, CancelledAtUnixMillis: now,
		})))
	case TaskStatusFailed:
		failUnit(t, h, ns, id, version, "n2", u2, "boom")
	case TaskStatusStarted, TaskStatusPreparing, TaskStatusSwapping:
	case TaskStatusFinished:
		ack(g2)
		require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
			Namespace: ns, Id: id, Version: version, FinalizedAtUnixMillis: now,
		})))
	}
	require.Equal(t, end, h.manager.tasks[ns][id].Status)
}
