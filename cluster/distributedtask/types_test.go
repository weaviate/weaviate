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
		{TaskStatusFinalizing, false},
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
