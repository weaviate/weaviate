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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// The backup-gate rule lives here rather than with its REST wiring: it is
// a statement about what this package treats as a live reindex, and the
// transport layer only snapshots it.

// gateTaskOnC is a reindex task on collection C, in status, with one unit on shard-1.
func gateTaskOnC(t *testing.T, status distributedtask.TaskStatus) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(ReindexTaskPayload{MigrationType: ReindexTypeEnableFilterable, Collection: "C", Properties: []string{"foo"}})
	require.NoError(t, err)
	unitID := MigrationUnitID("shard-1", "node-1")
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
		Payload:        raw,
		Status:         status,
		Units:          map[string]*distributedtask.Unit{unitID: {ID: unitID, Shard: "shard-1", NodeID: "node-1"}},
	}
}

// gateLookup builds the backup gate as a node does: the leader's tasks without units, and units from local.
func gateLookup(t *testing.T, leader, local *distributedtask.Manager) ShardReindexActivityLookup {
	t.Helper()
	raw, err := leader.ListTaskHeadersPayload()
	require.NoError(t, err)
	headers, err := local.DecodeTaskHeaders(raw)
	require.NoError(t, err)
	hasUnit := func(desc distributedtask.TaskDescriptor, shard string) (bool, bool) {
		return local.TaskHasUnit(ReindexNamespace, desc, func(node string) string { return MigrationUnitID(shard, node) })
	}
	logger, _ := logrustest.NewNullLogger()
	return NewShardReindexActivityLookup(headers[ReindexNamespace], hasUnit, logger)
}

// Pins: the lookup treats an unrecognized status as in-flight.
func TestReindexLookups_LivenessRule(t *testing.T) {
	statuses := []struct {
		status   distributedtask.TaskStatus
		inFlight bool
	}{
		{distributedtask.TaskStatusStarted, true},
		{distributedtask.TaskStatusPreparing, true},
		{distributedtask.TaskStatusSwapping, true},
		{unknownFutureStatus, true},
		// The zero value: a task whose status field never got written.
		// Unrecognized like any other, so it is read as in flight.
		{distributedtask.TaskStatus(""), true},
		{distributedtask.TaskStatusFinished, false},
		{distributedtask.TaskStatusFailed, false},
		{distributedtask.TaskStatusCancelled, false},
	}

	for _, s := range statuses {
		name := string(s.status)
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			node := managerHolding(t, ReindexNamespace, gateTaskOnC(t, s.status))
			require.Equal(t, s.inFlight, gateLookup(t, node, node)("C", "shard-1"),
				"the backup gate must read %q as in-flight=%v", s.status, s.inFlight)
		})
	}
}

// Pins: a task this node has not applied yet covers every shard of its collection.
func TestShardReindexActivityLookup_KeyIsCollectionAndShard(t *testing.T) {
	live := managerHolding(t, ReindexNamespace, gateTaskOnC(t, distributedtask.TaskStatusStarted))
	ended := managerHolding(t, ReindexNamespace, gateTaskOnC(t, distributedtask.TaskStatusFinished))
	notApplied := managerHolding(t, ReindexNamespace)

	for _, tc := range []struct {
		name       string
		leader     *distributedtask.Manager
		local      *distributedtask.Manager
		collection string
		shard      string
		want       bool
	}{
		{"live task with a unit on the shard", live, live, "C", "shard-1", true},
		{"same shard name under another collection", live, live, "Other", "shard-1", false},
		{"shard of the collection without a unit", live, live, "C", "shard-2", false},
		{"ended task", ended, ended, "C", "shard-1", false},
		{"task this node has not applied, any shard of its collection", live, notApplied, "C", "shard-2", true},
		{"task this node has not applied, another collection", live, notApplied, "Other", "shard-1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, gateLookup(t, tc.leader, tc.local)(tc.collection, tc.shard))
		})
	}
}
