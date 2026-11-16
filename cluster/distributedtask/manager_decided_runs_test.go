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

func TestManager_DecidedRunsOf(t *testing.T) {
	const ns, id, version = "ns", "task1", uint64(10)
	for _, tc := range []struct {
		name                    string
		ungrouped, noBarrier    bool
		finish                  bool
		shard, node, collection string
		want                    bool
	}{
		{name: "a group decided with its swap owed elsewhere", want: true},
		{name: "a run without groups answers for every shard", ungrouped: true, shard: "any", want: true},
		{name: "an undecided group", shard: "g2"},
		{name: "a node that still owes the swap", node: "n2"},
		{name: "another collection", collection: "Other"},
		{name: "a run without the barrier", noBarrier: true},
		{name: "a FINISHED run", finish: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHarness(t)
			h.headerDecoders = map[string]HeaderDecoder{ns: decodeWith(func(p []byte) (string, bool) {
				return string(p[1 : len(p)-1]), true
			})}
			h.init(t)
			g1, g2 := "g1", "g2"
			if tc.ungrouped {
				g1, g2 = "", ""
			}
			require.NoError(t, h.manager.AddTask(toCmd(t, &cmd.AddDistributedTaskRequest{
				Namespace: ns, Id: id, Payload: []byte(`"Coll"`), SubmittedAtUnixMillis: h.clock.Now().UnixMilli(),
				UnitSpecs:               []*cmd.UnitSpec{{Id: "u1", GroupId: g1}, {Id: "u2", GroupId: g2}},
				NeedsPreparationBarrier: !tc.noBarrier,
			}), version))
			ack := func(group string) {
				require.NoError(t, h.manager.RecordPreparationCompleteAck(toCmd(t, &cmd.RecordDistributedTaskPreparationCompleteAckRequest{
					Namespace: ns, Id: id, Version: version, GroupId: &group, NodeId: "n2", Success: true,
					AckedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
			}
			completeUnit(t, h, ns, id, version, "n2", "u1")
			if tc.ungrouped || tc.finish {
				completeUnit(t, h, ns, id, version, "n2", "u2")
			}
			ack(g1)
			require.True(t, h.manager.tasks[ns][id].GroupSwapDecided(g1), "the row must start from a decided group")
			if tc.finish {
				ack(g2)
				require.NoError(t, h.manager.MarkTaskFinalized(toCmd(t, &cmd.MarkTaskFinalizedRequest{
					Namespace: ns, Id: id, Version: version, FinalizedAtUnixMillis: h.clock.Now().UnixMilli(),
				})))
				require.Equal(t, TaskStatusFinished, h.manager.tasks[ns][id].Status)
			}

			var want []DecidedRun
			if tc.want {
				want = []DecidedRun{{TaskDescriptor: TaskDescriptor{ID: id, Version: version}, Header: testHeader("Coll")}}
			}
			require.Equal(t, want, h.manager.DecidedRunsOf(
				cmp.Or(tc.collection, "coll"), cmp.Or(tc.shard, "g1"), cmp.Or(tc.node, "n1")))
		})
	}
}
