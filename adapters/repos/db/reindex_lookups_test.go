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

// lookupTask builds a task carrying payload, in status.
func lookupTask(t *testing.T, id string, status distributedtask.TaskStatus,
	payload ReindexTaskPayload,
) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: id, Version: 1},
		Payload:        raw,
		Status:         status,
	}
}

// Pins: the lookup treats an unrecognized status as in-flight.
func TestReindexLookups_LivenessRule(t *testing.T) {
	payload := ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable,
		Collection:    "C",
		Properties:    []string{"foo"},
		UnitToShard:   map[string]string{"u1": "shard-1"},
	}

	logger, _ := logrustest.NewNullLogger()

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
			task := lookupTask(t, "T1", s.status, payload)
			require.Equal(t, s.inFlight,
				NewShardReindexActivityLookup([]*distributedtask.Task{task}, logger)("C", "shard-1"),
				"the backup gate must read %q as in-flight=%v", s.status, s.inFlight)
		})
	}
}

// Pins: the lookup key includes collection, so a shared shard name
// doesn't falsely flag an unrelated collection as busy.
func TestShardReindexActivityLookup_KeyIsCollectionAndShard(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()

	task := lookupTask(t, "T1", distributedtask.TaskStatusStarted,
		ReindexTaskPayload{
			MigrationType: ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
			UnitToShard:   map[string]string{"u1": "shard-1"},
		})

	for _, tc := range []struct {
		name       string
		collection string
		shard      string
		want       bool
	}{
		{"migrating collection and shard", "C", "shard-1", true},
		{"same shard name under another collection", "Other", "shard-1", false},
		{"migrating collection, other shard", "C", "shard-2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want,
				NewShardReindexActivityLookup([]*distributedtask.Task{task}, logger)(tc.collection, tc.shard))
		})
	}
}
