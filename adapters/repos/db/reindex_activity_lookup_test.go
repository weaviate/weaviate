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

// Asking whether a reindex is live at commit time misses every task that both
// started and finished while the files were being copied — the capture is just
// as inconsistent, and by the time anyone looks there is nothing running. The
// check has to be "did one overlap the backup window".
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
			// R3's mechanism: ran and finished entirely inside the window.
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
			tasks:      []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatusCancelled, time.Time{})},
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
