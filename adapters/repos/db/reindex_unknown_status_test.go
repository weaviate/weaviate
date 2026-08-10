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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
)

// unknownFutureStatus stands in for a status a newer release introduced
// and this build has never heard of — what a mixed-version cluster sees
// during a rolling upgrade. Keep it a string no release will ever declare,
// or these tests silently start asserting facts about a real status.
const unknownFutureStatus distributedtask.TaskStatus = "UNKNOWN_FUTURE_STATE"

// TestIsLiveReindexTaskStatus_AgreesWithIsActive pins the two liveness
// predicates to a single answer. When they disagreed, an unrecognized
// status was live to the backup gate but invisible to conflict
// detection, so a second migration on the same property was admitted.
func TestIsLiveReindexTaskStatus_AgreesWithIsActive(t *testing.T) {
	cases := []struct {
		status distributedtask.TaskStatus
		live   bool
	}{
		{distributedtask.TaskStatusStarted, true},
		{distributedtask.TaskStatusPreparing, true},
		{distributedtask.TaskStatusSwapping, true},
		{distributedtask.TaskStatusFinished, false},
		{distributedtask.TaskStatusFailed, false},
		{distributedtask.TaskStatusCancelled, false},
		{unknownFutureStatus, true},
		{distributedtask.TaskStatus(""), true},
		{distributedtask.TaskStatus("started"), true},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			require.Equal(t, tc.live, IsLiveReindexTaskStatus(tc.status))
			require.Equal(t, IsLiveReindexTaskStatus(tc.status), tc.status.IsActive(),
				"%q: liveness predicates have drifted apart", tc.status)
		})
	}
}

// TestCheckConflict_RejectsAgainstUnknownStatusTask pins that a task in
// an unrecognized status blocks a second migration on the same property.
func TestCheckConflict_RejectsAgainstUnknownStatusTask(t *testing.T) {
	provider := &ReindexProvider{}

	newPayload, err := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeEnableRangeable,
		Properties:    []string{"num"},
	})
	require.NoError(t, err)

	existPayload, err := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"num"},
	})
	require.NoError(t, err)

	for _, status := range []distributedtask.TaskStatus{
		unknownFutureStatus,
		distributedtask.TaskStatus(""),
	} {
		t.Run(string(status), func(t *testing.T) {
			existing := []*distributedtask.Task{
				{
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
					Status:         status,
					Payload:        existPayload,
				},
			}
			err := provider.CheckConflict(newPayload, existing)
			require.Error(t, err, "a task this build cannot prove is done must block a new migration")
			require.Contains(t, err.Error(), "conflicts")
		})
	}
}
