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
)

func TestManager_RestoreCollectionTasks(t *testing.T) {
	captured := func(version uint64, status TaskStatus, payload, errText string) *Task {
		return &Task{
			Namespace: "reindex", TaskDescriptor: TaskDescriptor{ID: "run", Version: version},
			Payload: []byte(payload), Status: status, StartedAt: compactAt, Error: errText,
			Units:             map[string]*Unit{"u1": {ID: "u1", GroupID: "t2", Status: UnitStatusInProgress}},
			SwapDecidedGroups: map[string]time.Time{"t1": compactAt},
		}
	}
	const gen2 = `{"collection":"C","generation":2}`
	for _, tc := range []struct {
		name      string
		held      []*Task
		captured  *Task
		wantVer   uint64
		wantStat  TaskStatus
		wantError string
		wantNone  bool
	}{
		{
			name: "an ended run is installed", captured: captured(5, TaskStatusCancelled, gen2, ""),
			wantVer: 5, wantStat: TaskStatusCancelled,
		},
		{
			name: "a run without a generation is skipped", captured: captured(5, TaskStatusFailed, `{"collection":"C"}`, ""),
			wantNone: true,
		},
		{
			name: "a run captured running fails", captured: captured(5, TaskStatusStarted, gen2, "unit u1 slow"),
			wantVer: 5, wantStat: TaskStatusFailed, wantError: "unit u1 slow; " + restoredRunningError,
		},
		{
			name: "a held run with a higher version wins", held: []*Task{captured(9, TaskStatusFailed, gen2, "held")},
			captured: captured(5, TaskStatusCancelled, gen2, ""), wantVer: 9, wantStat: TaskStatusFailed, wantError: "held",
		},
		{
			name: "a held run with a lower version is replaced", held: []*Task{captured(3, TaskStatusFailed, gen2, "held")},
			captured: captured(5, TaskStatusCancelled, gen2, ""), wantVer: 5, wantStat: TaskStatusCancelled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rules := &endRecordingRules{}
			m := newRulesTestManager(t, rules, tc.held...)
			raw, err := json.Marshal([]*Task{tc.captured})
			require.NoError(t, err)

			require.NoError(t, m.RestoreCollectionTasks(raw, "C", func(string) bool { return true }))

			task := taskByID(m, "run")
			if tc.wantNone {
				require.Nil(t, task)
				return
			}
			require.Equal(t, tc.wantVer, task.Version)
			require.Equal(t, tc.wantStat, task.Status)
			require.Equal(t, tc.wantError, task.Error)
			require.Nil(t, task.Units, "the end rules compact what a restore installs")
			require.Equal(t, map[string]time.Time{"t1": compactAt}, task.SwapDecidedGroups)
		})
	}

	t.Run("an unreadable task list fails the restore", func(t *testing.T) {
		m := newRulesTestManager(t, &endRecordingRules{})
		require.Error(t, m.RestoreCollectionTasks([]byte(`{`), "C", func(string) bool { return true }))
	})
}
