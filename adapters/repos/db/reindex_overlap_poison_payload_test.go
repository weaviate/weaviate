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
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

func poisonTask(t *testing.T, collection string, status distributedtask.TaskStatus, finishedAt time.Time) *distributedtask.Task {
	t.Helper()
	raw := poisonPayload(collection)
	var payload ReindexTaskPayload
	require.Error(t, json.Unmarshal(raw, &payload),
		"this fixture is only meaningful while the payload really is undecodable")

	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: collection + ":poison", Version: 1},
		Namespace:      ReindexNamespace,
		Status:         status,
		Payload:        raw,
		FinishedAt:     finishedAt,
		Units: map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress},
		},
	}
}

func unreadableTask(status distributedtask.TaskStatus, finishedAt time.Time) *distributedtask.Task {
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "unreadable", Version: 1},
		Namespace:      ReindexNamespace,
		Status:         status,
		Payload:        []byte("{not json"),
		FinishedAt:     finishedAt,
		Units: map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress},
		},
	}
}

// A payload no node can decode is a real state: a rolling upgrade that retypes a
// payload field produces it, and the check has to fail closed on it. What it
// must not do is fail closed on every collection. The task names exactly one,
// and refusing backups of all the others is a self-inflicted outage whose only
// exit is the completed-task TTL days later.
func TestReindexOverlapLookupScopesUnreadablePayloadsToTheirCollection(t *testing.T) {
	backupStart := time.Now().Add(-2 * time.Minute)
	insideWindow := backupStart.Add(time.Minute)

	tests := []struct {
		name      string
		task      func(t *testing.T) *distributedtask.Task
		backingUp []string
		// wantMsg is the exact refusal text. A refusal that merely happens is
		// not enough: "collection X was migrated" would be a claim this check
		// cannot make about a payload it could not read, and asserting only
		// that an error came back cannot tell the two apart.
		wantMsg string
		why     string
	}{
		{
			name: "poison on another collection",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "Actors", distributedtask.TaskStatusStarted, time.Time{})
			},
			backingUp: []string{"Movies"},
			why:       "the task names Actors; nothing about it says anything about a backup of Movies",
		},
		{
			name: "poison on the collection being backed up",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "Movies", distributedtask.TaskStatusStarted, time.Time{})
			},
			backingUp: []string{"Movies"},
			wantMsg:   `cannot rule out a runtime-reindex of collection "Movies" during this backup: its task payload is unreadable`,
			why:       "a live migration on this very collection cannot be ruled out, so the backup must fail",
		},
		{
			name: "poison on one of several collections being backed up",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "Actors", distributedtask.TaskStatusStarted, time.Time{})
			},
			backingUp: []string{"Movies", "Actors"},
			wantMsg:   `cannot rule out a runtime-reindex of collection "Actors" during this backup: its task payload is unreadable`,
			why:       "the backup covers the affected collection, so the uncertainty is inside its scope",
		},
		{
			name: "poison matched case-insensitively",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "movies", distributedtask.TaskStatusStarted, time.Time{})
			},
			backingUp: []string{"Movies"},
			wantMsg:   `cannot rule out a runtime-reindex of collection "movies" during this backup: its task payload is unreadable`,
			why:       "collection names fold, and a fail-closed gate must not be evadable by casing",
		},
		{
			name: "terminal poison that finished before the backup started",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "Movies", distributedtask.TaskStatusFinished, backupStart.Add(-time.Minute))
			},
			backingUp: []string{"Movies"},
			why:       "status and FinishedAt come off the task, not the payload; one that was over before the capture began cannot have spanned it",
		},
		{
			name: "terminal poison that finished inside the window",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "Movies", distributedtask.TaskStatusFinished, insideWindow)
			},
			backingUp: []string{"Movies"},
			wantMsg:   `cannot rule out a runtime-reindex of collection "Movies" during this backup: its task payload is unreadable`,
			why:       "it ran while the files were being copied",
		},
		{
			name: "collection unrecoverable, live",
			task: func(t *testing.T) *distributedtask.Task {
				return unreadableTask(distributedtask.TaskStatusStarted, time.Time{})
			},
			backingUp: []string{"Movies"},
			wantMsg:   "cannot rule out a runtime-reindex during this backup: a task payload is unreadable",
			why:       "nothing identifies what this task touches, so no collection can be declared clean",
		},
		{
			name: "collection unrecoverable, but finished before the backup started",
			task: func(t *testing.T) *distributedtask.Task {
				return unreadableTask(distributedtask.TaskStatusFinished, backupStart.Add(-time.Minute))
			},
			backingUp: []string{"Movies"},
			why:       "an unidentifiable task still cannot write after it finished; the timestamps alone clear it",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := tc.task(t)
			lookup := NewReindexOverlapLookup(func(context.Context) (map[string][]*distributedtask.Task, error) {
				return map[string][]*distributedtask.Task{ReindexNamespace: {task}}, nil
			}, time.Hour)

			err := lookup(context.Background(), tc.backingUp, backupStart)
			if tc.wantMsg == "" {
				require.NoError(t, err, tc.why)
				return
			}
			require.Error(t, err, tc.why)
			assert.Equal(t, tc.wantMsg, err.Error(), tc.why)
			assert.ErrorIs(t, err, entitiesbackup.ErrBackupSpannedReindex,
				"a caller that cannot match the sentinel treats a fail-closed refusal as an unrelated failure")
		})
	}
}
