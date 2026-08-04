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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

// TestRefuseIfAnyReindexInFlight_Unwired pins the startup-window default: allow + warn once.
func TestRefuseIfAnyReindexInFlight_Unwired(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background()))
	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background()))

	warnings := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel &&
			strings.Contains(entry.Message, "AnyReindexActivityLookup not yet installed") {
			warnings++
		}
	}
	assert.Equal(t, 1, warnings,
		"the unwired WARN must fire once per process, not once per restore")
}

func TestRefuseIfAnyReindexInFlight(t *testing.T) {
	lookupErr := errors.New("DTM unreachable")

	tests := []struct {
		name         string
		lookup       AnyReindexActivityLookup
		cleanup      AnyCleanupInProgressLookup
		wantRefusal  bool
		wantContains string
		wantCause    error
	}{
		{
			name:   "no live task admits the restore",
			lookup: func(context.Context) (bool, error) { return false, nil },
		},
		{
			name:    "no live task and no cleanup admits the restore",
			lookup:  func(context.Context) (bool, error) { return false, nil },
			cleanup: func() bool { return false },
		},
		{
			name:         "sidecar cleanup after a cancel refuses the restore",
			lookup:       func(context.Context) (bool, error) { return false, nil },
			cleanup:      func() bool { return true },
			wantRefusal:  true,
			wantContains: "still removing its temporary index files",
		},
		{
			name:         "live task refuses the restore",
			lookup:       func(context.Context) (bool, error) { return true, nil },
			wantRefusal:  true,
			wantContains: "retry after the migration finishes",
		},
		{
			name:         "lookup failure fails closed",
			lookup:       func(context.Context) (bool, error) { return false, lookupErr },
			wantRefusal:  true,
			wantContains: "the cluster task manager could not be queried",
			wantCause:    lookupErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			db := &DB{logger: logger}
			db.SetAnyReindexActivityLookup(tc.lookup)
			if tc.cleanup != nil {
				db.SetAnyCleanupInProgressLookup(tc.cleanup)
			}

			err := db.RefuseIfAnyReindexInFlight(context.Background())
			if !tc.wantRefusal {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight,
				"the refusal must carry the cluster-wide sentinel")
			assert.ErrorContains(t, err, tc.wantContains)
			if tc.wantCause != nil {
				assert.ErrorIs(t, err, tc.wantCause, "the underlying cause must stay reachable")
			}

			// The per-shard backup vocabulary doesn't apply to the cluster-wide gate.
			assert.NotContains(t, err.Error(), "backup")
			assert.NotContains(t, err.Error(), "restore")
			assert.NotContains(t, err.Error(), "this shard")
		})
	}
}

// TestRefuseIfAnyReindexInFlight_Wording pins the exact operator-facing text.
func TestRefuseIfAnyReindexInFlight_Wording(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	db.SetAnyReindexActivityLookup(func(context.Context) (bool, error) { return true, nil })

	err := db.RefuseIfAnyReindexInFlight(context.Background())
	require.Error(t, err)
	assert.Equal(t,
		`runtime-reindex in flight in the cluster: retry after the migration finishes `+
			`(poll GET /v1/schema/<class>/indexes until all indexes report status="ready") `+
			`or cancel it via PUT /v1/schema/<class>/indexes/<prop> {"<indexType>":{"cancel":true}}`,
		err.Error())
}

// TestRefuseIfAnyReindexInFlight_PropagatesContext pins that the caller's context reaches the lookup.
func TestRefuseIfAnyReindexInFlight_PropagatesContext(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db.SetAnyReindexActivityLookup(func(ctx context.Context) (bool, error) {
		return false, ctx.Err()
	})

	err := db.RefuseIfAnyReindexInFlight(ctx)
	require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
	assert.ErrorIs(t, err, context.Canceled)
}

// Pins: node names from a RAFT lookup failure must not leak into the
// restore-refusal body.
func TestRefuseIfAnyReindexInFlight_LookupFailureRedactsNodeNames(t *testing.T) {
	raftErr := errors.New("can not resolve nodes [weaviate-2,weaviate-1]")

	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	db.SetAnyReindexActivityLookup(func(context.Context) (bool, error) { return false, raftErr })

	err := db.RefuseIfAnyReindexInFlight(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
	require.ErrorIs(t, err, raftErr,
		"the cause must stay reachable for callers that classify it")

	body := err.Error()
	assert.Equal(t,
		"runtime-reindex in flight in the cluster (assumed): "+
			"the cluster task manager could not be queried; retry once it is reachable",
		body)
	for _, leaked := range []string{"weaviate-1", "weaviate-2", "can not resolve nodes"} {
		assert.NotContainsf(t, body, leaked, "the refusal body leaked %q", leaked)
	}

	var logged bool
	for _, entry := range hook.AllEntries() {
		if strings.Contains(entry.Message, raftErr.Error()) {
			logged = true
		}
	}
	assert.True(t, logged, "the detail must still reach the operator through the log")
}

func overlapTask(collection string, status distributedtask.TaskStatus, finishedAt time.Time) *distributedtask.Task {
	raw, _ := json.Marshal(ReindexTaskPayload{Collection: collection})
	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: collection + ":task", Version: 1},
		Namespace:      ReindexNamespace,
		Status:         status,
		Payload:        raw,
		FinishedAt:     finishedAt,
	}
}

// The check is overlap, not liveness: a task that ran entirely inside the
// backup window must still refuse it.
func TestReindexOverlapLookup(t *testing.T) {
	backupStart := time.Now().Add(-2 * time.Minute)
	const ttl = time.Hour

	tests := []struct {
		name       string
		tasks      []*distributedtask.Task
		listErr    error
		ttl        time.Duration
		since      time.Time
		wantRefuse bool
		wantMsg    string
	}{
		{
			name:  "no tasks at all",
			since: backupStart,
		},
		{
			name:  "task finished before the backup started",
			tasks: []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatusFinished, backupStart.Add(-time.Minute))},
			since: backupStart,
		},
		{
			// Ran and finished entirely inside the window.
			name:       "task finished after the backup started",
			tasks:      []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatusFinished, backupStart.Add(time.Minute))},
			since:      backupStart,
			wantRefuse: true,
			wantMsg:    "was migrated while this backup was being captured",
		},
		{
			name:       "task finished exactly when the backup started",
			tasks:      []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatusFinished, backupStart)},
			since:      backupStart,
			wantRefuse: true,
		},
		{
			name:       "task still running",
			tasks:      []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatusStarted, time.Time{})},
			since:      backupStart,
			wantRefuse: true,
		},
		{
			name:       "terminal task with no finish time is treated as overlapping",
			tasks:      []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatusFailed, time.Time{})},
			since:      backupStart,
			wantRefuse: true,
		},
		{
			name:  "task on a collection this backup does not cover",
			tasks: []*distributedtask.Task{overlapTask("Actors", distributedtask.TaskStatusFinished, backupStart.Add(time.Minute))},
			since: backupStart,
		},
		{
			name:       "collection match ignores case",
			tasks:      []*distributedtask.Task{overlapTask("movies", distributedtask.TaskStatusFinished, backupStart.Add(time.Minute))},
			since:      backupStart,
			wantRefuse: true,
		},
		{
			name:       "backup outlived the retention window",
			tasks:      nil,
			ttl:        time.Minute,
			since:      backupStart,
			wantRefuse: true,
			wantMsg:    "longer than the",
		},
		{
			name:       "task list unreadable",
			listErr:    errors.New("DTM unreachable"),
			since:      backupStart,
			wantRefuse: true,
			wantMsg:    "cannot rule out a runtime-reindex",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			taskTTL := ttl
			if tc.ttl != 0 {
				taskTTL = tc.ttl
			}
			lookup := NewReindexOverlapLookup(func(context.Context) (map[string][]*distributedtask.Task, error) {
				if tc.listErr != nil {
					return nil, tc.listErr
				}
				return map[string][]*distributedtask.Task{ReindexNamespace: tc.tasks}, nil
			}, taskTTL)

			err := lookup(context.Background(), []string{"Movies"}, tc.since)
			if !tc.wantRefuse {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tc.wantMsg != "" {
				assert.ErrorContains(t, err, tc.wantMsg)
			}
			assert.NotContains(t, err.Error(), "in flight",
				"the migration has usually finished by now; do not send the operator after a live task")
		})
	}
}

func overlapTaskWithUnits(collection string, status distributedtask.TaskStatus, finishedAt time.Time,
	units map[string]*distributedtask.Unit,
) *distributedtask.Task {
	task := overlapTask(collection, status, finishedAt)
	task.Units = units
	return task
}

// The submission side withdraws a task when a backup claims first, and the
// backup side asks whether a task overlapped it. Both fire on the same task, so
// the boundary has to be exact in three directions at once: too strict fails
// the backup that won the race, too loose passes a backup that spans a real
// migration.
func TestReindexOverlapLookupCountsTheRightTerminalTasks(t *testing.T) {
	backupStart := time.Now().Add(-2 * time.Minute)
	insideWindow := backupStart.Add(time.Minute)

	tests := []struct {
		name       string
		task       *distributedtask.Task
		wantRefuse bool
		why        string
	}{
		{
			name: "cancelled before completing, after a backup claimed first",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, insideWindow,
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
				}),
			wantRefuse: false,
			why:        "the backup won the race; failing it would punish the winner",
		},
		{
			name: "migration ran to completion inside the window",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusFinished, insideWindow,
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
				}),
			wantRefuse: true,
			why:        "nothing is running at commit, which is exactly why liveness is the wrong question",
		},
		{
			name: "migration still live at commit",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusStarted, time.Time{},
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress},
				}),
			wantRefuse: true,
			why:        "the capture and the migration are concurrent",
		},
		{
			name: "migration that failed inside the window",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusFailed, insideWindow,
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusFailed},
				}),
			wantRefuse: true,
			why:        "a failed migration may have written before it failed",
		},
		{
			name: "cancelled before this backup even started",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled,
				backupStart.Add(-time.Minute), nil),
			wantRefuse: false,
			why:        "no overlap at all",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lookup := NewReindexOverlapLookup(func(context.Context) (map[string][]*distributedtask.Task, error) {
				return map[string][]*distributedtask.Task{ReindexNamespace: {tc.task}}, nil
			}, time.Hour)

			err := lookup(context.Background(), []string{"Movies"}, backupStart)
			if tc.wantRefuse {
				require.Error(t, err, tc.why)
				return
			}
			require.NoError(t, err, tc.why)
		})
	}
}

// Pins the gap ReindexOverlapLookup documents: a migration cancelled part-way
// through is not counted, leaving a fail-open admission unguarded from
// shard-halt to commit (0-wi#473).
func TestReindexOverlapLookupResidual(t *testing.T) {
	backupStart := time.Now().Add(-2 * time.Minute)

	partlyRan := overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled,
		backupStart.Add(time.Minute),
		map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress, Progress: 0.4},
		})

	lookup := NewReindexOverlapLookup(func(context.Context) (map[string][]*distributedtask.Task, error) {
		return map[string][]*distributedtask.Task{ReindexNamespace: {partlyRan}}, nil
	}, time.Hour)

	require.NoError(t, lookup(context.Background(), []string{"Movies"}, backupStart),
		"documented residual: a partly-run cancelled migration is not counted")
}
