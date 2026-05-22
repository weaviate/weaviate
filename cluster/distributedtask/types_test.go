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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		{TaskStatusSwapping, false},
		{TaskStatusFinished, true},
		{TaskStatusFailed, true},
		{TaskStatusCancelled, true},
		// Unknown / future statuses default to non-terminal so providers
		// don't accidentally skip work they're meant to do.
		{TaskStatus("UNKNOWN_FUTURE_STATE"), false},
		{TaskStatus(""), false},
	}
	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			assert.Equal(t, tc.terminal, tc.status.IsTerminal(),
				"%q.IsTerminal() should be %v", tc.status, tc.terminal)
		})
	}
}

// TestTask_Clone_DeepCopiesAckMaps pins that Clone() deep-copies every
// map field on Task, including PreparationCompletionAcks added in #11328.
// Regression guard: the scheduler tick mutates the local clone's ack
// maps to reflect FSM transitions in the same tick (see scheduler.go
// PHASE A.5 / Phase 1.5); if Clone aliased the map, those writes would
// bleed into the FSM-stored Task and break determinism / safety.
func TestTask_Clone_DeepCopiesAckMaps(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	orig := &Task{
		Units: map[string]*Unit{
			"u-1": {ID: "u-1", NodeID: "node-1", Status: UnitStatusCompleted},
		},
		PostCompletionAcks: map[string]PostCompletionAck{
			"node-1": {Success: true, AckedAt: now},
		},
		PreparationCompletionAcks: map[string]PostCompletionAck{
			"node-1": {Success: true, AckedAt: now},
		},
	}

	clone := orig.Clone()

	// Mutate every map on the clone.
	clone.Units["u-1"].Status = UnitStatusFailed
	clone.PostCompletionAcks["node-1"] = PostCompletionAck{Success: false, Error: "swap"}
	clone.PostCompletionAcks["node-2"] = PostCompletionAck{Success: true}
	clone.PreparationCompletionAcks["node-1"] = PostCompletionAck{Success: false, Error: "prep"}
	clone.PreparationCompletionAcks["node-2"] = PostCompletionAck{Success: true}

	// Original must be untouched on EVERY map.
	assert.Equal(t, UnitStatusCompleted, orig.Units["u-1"].Status, "Units value-level deep copy")
	assert.True(t, orig.PostCompletionAcks["node-1"].Success, "PostCompletionAcks must not alias clone")
	assert.NotContains(t, orig.PostCompletionAcks, "node-2", "PostCompletionAcks must not alias clone")
	assert.True(t, orig.PreparationCompletionAcks["node-1"].Success, "PreparationCompletionAcks must not alias clone")
	assert.NotContains(t, orig.PreparationCompletionAcks, "node-2", "PreparationCompletionAcks must not alias clone")
}
