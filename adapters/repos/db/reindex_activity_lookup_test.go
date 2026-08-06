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

	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), nil))
	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), nil))

	warnings := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel &&
			strings.Contains(entry.Message, "AnyReindexActivityLookup not yet installed") {
			warnings++
		}
	}
	assert.Equal(t, 1, warnings,
		"the unwired WARN is budgeted once per hour per node, not once per restore")
}

func TestRefuseIfAnyReindexInFlight(t *testing.T) {
	lookupErr := errors.New("DTM unreachable")

	tests := []struct {
		name         string
		lookup       AnyReindexActivityLookup
		cleanup      AnyCleanupInProgressLookup
		wantRefusal  bool
		wantContains string
		wantAbsent   []string
		wantCause    error
	}{
		{
			name:   "no live task admits the restore",
			lookup: func(context.Context) (bool, error) { return false, nil },
		},
		{
			name:    "no live task and no cleanup admits the restore",
			lookup:  func(context.Context) (bool, error) { return false, nil },
			cleanup: func([]string) bool { return false },
		},
		{
			// The lookup cannot say whether the hold is a teardown or a
			// submission sweep, so the text must not claim either one.
			name:         "a node-local reindex hold refuses the restore",
			lookup:       func(context.Context) (bool, error) { return false, nil },
			cleanup:      func([]string) bool { return true },
			wantRefusal:  true,
			wantContains: "holding temporary index files on this node",
			wantAbsent:   []string{"a cancelled migration is still removing"},
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

			err := db.RefuseIfAnyReindexInFlight(context.Background(), nil)
			if !tc.wantRefusal {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight,
				"the refusal must carry the cluster-wide sentinel")
			assert.ErrorContains(t, err, tc.wantContains)
			for _, absent := range tc.wantAbsent {
				assert.NotContainsf(t, err.Error(), absent,
					"the refusal must not claim %q, which the lookup cannot tell apart", absent)
			}
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

	err := db.RefuseIfAnyReindexInFlight(context.Background(), nil)
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

	err := db.RefuseIfAnyReindexInFlight(ctx, nil)
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

	err := db.RefuseIfAnyReindexInFlight(context.Background(), nil)
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
			// The comparison is exact: no allowance widens the cleared window
			// to cover clock skew between the capturing node and the RAFT
			// proposer. That window is accepted, so a task finishing ten
			// seconds before the backup is as cleared as one finishing an hour
			// before. See docs/runtime-reindex.md.
			name:  "task finished shortly before the backup started",
			tasks: []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatusFinished, backupStart.Add(-10*time.Second))},
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
			// Every refusal this lookup produces, including the
			// retention-window one, has to be classifiable: a caller that
			// cannot match the sentinel treats it as an unrelated failure.
			assert.ErrorIs(t, err, entitiesbackup.ErrBackupSpannedReindex,
				"the refusal must carry the overlap sentinel")
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
			// Units that left PENDING on purpose: with nil units the
			// cancelled-and-untouched rule would exempt this row too, and
			// either rule alone would keep it green.
			name: "cancelled after writing, but before this backup started",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled,
				backupStart.Add(-time.Minute),
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
				}),
			wantRefuse: false,
			why:        "it wrote, but it was over before the capture began",
		},
		{
			name: "finished before this backup started",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusFinished,
				backupStart.Add(-time.Minute),
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
				}),
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

// Cancel only applies to a STARTED task, so a cancelled one may already have
// rebuilt buckets. Whether it did is what decides the backup, and unit state is
// the evidence: skipping every cancelled task let the submit path's own
// post-commit rollback manufacture the one state this backstop ignores.
func TestReindexOverlapLookupCountsCancelledTasksThatRan(t *testing.T) {
	backupStart := time.Now().Add(-2 * time.Minute)

	tests := []struct {
		name       string
		units      map[string]*distributedtask.Unit
		wantRefuse bool
		why        string
	}{
		{
			name: "a unit was claimed, so a worker may have written",
			units: map[string]*distributedtask.Unit{
				"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress, Progress: 0.4},
			},
			wantRefuse: true,
			why:        "a partly-run cancelled migration spans the backup and must fail it",
		},
		{
			name: "a unit finished before the cancel landed",
			units: map[string]*distributedtask.Unit{
				"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted, Progress: 1},
			},
			wantRefuse: true,
			why:        "a completed unit is the strongest evidence the buckets moved",
		},
		{
			name: "no unit ever left PENDING",
			units: map[string]*distributedtask.Unit{
				"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
			},
			wantRefuse: false,
			why:        "the post-commit rollback cancels before any worker claims a unit; failing backups on that would make the rollback worse than the race it repairs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			task := overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled,
				backupStart.Add(time.Minute), tc.units)

			lookup := NewReindexOverlapLookup(func(context.Context) (map[string][]*distributedtask.Task, error) {
				return map[string][]*distributedtask.Task{ReindexNamespace: {task}}, nil
			}, time.Hour)

			err := lookup(context.Background(), []string{"Movies"}, backupStart)
			if tc.wantRefuse {
				require.Error(t, err, tc.why)
				require.ErrorIs(t, err, entitiesbackup.ErrBackupSpannedReindex, tc.why)
				return
			}
			require.NoError(t, err, tc.why)
		})
	}
}

// poisonTask defeats the full payload decoder while leaving the collection
// readable, which is what a rolling upgrade that retypes a payload field
// produces.
func poisonTask(t *testing.T, collection string, status distributedtask.TaskStatus, finishedAt time.Time) *distributedtask.Task {
	t.Helper()
	raw := poisonPayload(collection)
	var payload ReindexTaskPayload
	require.Error(t, json.Unmarshal(raw, &payload),
		"this fixture is only meaningful while the payload really is undecodable")

	task := unreadableTask(status, finishedAt)
	task.ID = collection + ":poison"
	task.Payload = raw
	return task
}

// unreadableTask is the harder case: not even the collection can be recovered.
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

// The refusal is stored in the backup's failure meta and served from
// GET /v1/backups/{backend}/{id}, so it must not carry the RAFT and decoding
// internals its causes name. It must also stay scoped: a payload no node can
// decode has to fail the backups of the collection it names and no others,
// because refusing all of them is a self-inflicted outage whose only exit is
// the completed-task TTL days later.
//
// wantMsg is exact on purpose. A refusal that merely happens is not enough:
// "collection X was migrated" would be a claim this check cannot make about a
// payload it could not read, and asserting only that an error came back cannot
// tell the two apart. An empty wantMsg means the backup must be allowed.
func TestReindexOverlapLookupScopesAndRedactsUnreadableInputs(t *testing.T) {
	raftErr := errors.New("can not resolve nodes [weaviate-2,weaviate-1]")
	backupStart := time.Now().Add(-2 * time.Minute)
	insideWindow := backupStart.Add(time.Minute)

	tests := []struct {
		name      string
		listErr   error
		task      func(t *testing.T) *distributedtask.Task
		backingUp []string
		cause     error
		leaked    []string
		wantMsg   string
		why       string
	}{
		{
			name:    "task manager unreachable",
			listErr: raftErr,
			cause:   raftErr,
			leaked:  []string{"weaviate-1", "weaviate-2", "can not resolve nodes"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried",
			why:     "an unanswerable question is not an all-clear",
		},
		{
			name: "collection unrecoverable, live",
			task: func(*testing.T) *distributedtask.Task {
				return unreadableTask(distributedtask.TaskStatusStarted, time.Time{})
			},
			leaked:  []string{"not json", "invalid character"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: a task payload is unreadable",
			why:     "nothing identifies what this task touches, so no collection can be declared clean",
		},
		{
			name: "collection unrecoverable, but finished before the backup started",
			task: func(*testing.T) *distributedtask.Task {
				return unreadableTask(distributedtask.TaskStatusFinished, backupStart.Add(-time.Minute))
			},
			why: "an unidentifiable task still cannot write after it finished; the timestamps alone clear it",
		},
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
			leaked:  []string{"not json", "invalid character"},
			wantMsg: unreadablePayloadMsg("Movies"),
			why:     "a live migration on this very collection cannot be ruled out, so the backup must fail",
		},
		{
			name: "poison on one of several collections being backed up",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "Actors", distributedtask.TaskStatusStarted, time.Time{})
			},
			backingUp: []string{"Movies", "Actors"},
			wantMsg:   unreadablePayloadMsg("Actors"),
			why:       "the backup covers the affected collection, so the uncertainty is inside its scope",
		},
		{
			name: "poison matched case-insensitively",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "movies", distributedtask.TaskStatusStarted, time.Time{})
			},
			wantMsg: unreadablePayloadMsg("movies"),
			why:     "collection names fold, and a fail-closed gate must not be evadable by casing",
		},
		{
			name: "terminal poison that finished before the backup started",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "Movies", distributedtask.TaskStatusFinished, backupStart.Add(-time.Minute))
			},
			why: "status and FinishedAt come off the task, not the payload; one that was over before the capture began cannot have spanned it",
		},
		{
			name: "terminal poison that finished inside the window",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "Movies", distributedtask.TaskStatusFinished, insideWindow)
			},
			wantMsg: unreadablePayloadMsg("Movies"),
			why:     "it ran while the files were being copied",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backingUp := tc.backingUp
			if backingUp == nil {
				backingUp = []string{"Movies"}
			}
			lookup := NewReindexOverlapLookup(func(context.Context) (map[string][]*distributedtask.Task, error) {
				if tc.listErr != nil {
					return nil, tc.listErr
				}
				return map[string][]*distributedtask.Task{ReindexNamespace: {tc.task(t)}}, nil
			}, time.Hour)

			err := lookup(context.Background(), backingUp, backupStart)
			if tc.wantMsg == "" {
				require.NoError(t, err, tc.why)
				return
			}
			require.Error(t, err, tc.why)
			assert.Equal(t, tc.wantMsg, err.Error(), tc.why)
			for _, leaked := range tc.leaked {
				assert.NotContainsf(t, err.Error(), leaked, "the refusal body leaked %q", leaked)
			}
			if tc.cause != nil {
				assert.ErrorIs(t, err, tc.cause, "the cause must stay reachable for callers that classify it")
			}
			// Redacting the text must not also hide what kind of refusal this
			// is: a caller that cannot match the sentinel treats a fail-closed
			// overlap refusal as an unrelated failure.
			assert.ErrorIs(t, err, entitiesbackup.ErrBackupSpannedReindex,
				"the refusal must stay classifiable through the redaction wrapper")
		})
	}
}

// unreadablePayloadMsg is the collection-scoped refusal, spelled out here so a
// change to the operator's remedy has to be made deliberately.
func unreadablePayloadMsg(collection string) string {
	return `cannot rule out a runtime-reindex of collection "` + collection +
		`" during this backup: its task payload is unreadable; ` +
		`retry once every node runs the same server version, and report this to Weaviate if it persists`
}

// TestRefuseIfReindexOverlapped_Unwired pins the startup-window default: allow + warn once.
func TestRefuseIfReindexOverlapped_Unwired(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	require.NoError(t, db.RefuseIfReindexOverlapped(context.Background(), []string{"Movies"}, time.Now()))
	require.NoError(t, db.RefuseIfReindexOverlapped(context.Background(), []string{"Movies"}, time.Now()))

	warnings := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel &&
			strings.Contains(entry.Message, "lookup not yet installed") {
			warnings++
		}
	}
	assert.Equal(t, 1, warnings,
		"the unwired WARN is budgeted once per hour per node, not once per backup")
}

// The commit-time question is asked per backup, and a backup covers anywhere
// from zero to every collection in the cluster. A task outside that set must not
// fail the backup, and one anywhere inside it must.
func TestRefuseIfReindexOverlappedCollectionScope(t *testing.T) {
	backupStart := time.Now().Add(-2 * time.Minute)
	migrated := overlapTask("Actors", distributedtask.TaskStatusFinished, backupStart.Add(time.Minute))

	tests := []struct {
		name        string
		collections []string
		wantRefuse  bool
	}{
		{
			name:        "no collections in the backup",
			collections: nil,
			wantRefuse:  false,
		},
		{
			name:        "one collection, migrated",
			collections: []string{"Actors"},
			wantRefuse:  true,
		},
		{
			name:        "one collection, untouched",
			collections: []string{"Movies"},
			wantRefuse:  false,
		},
		{
			name:        "many collections, the migrated one is last",
			collections: []string{"Movies", "Directors", "Actors"},
			wantRefuse:  true,
		},
		{
			name:        "many collections, none migrated",
			collections: []string{"Movies", "Directors", "Studios"},
			wantRefuse:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			db := &DB{logger: logger}
			db.SetReindexOverlapLookup(NewReindexOverlapLookup(
				func(context.Context) (map[string][]*distributedtask.Task, error) {
					return map[string][]*distributedtask.Task{ReindexNamespace: {migrated}}, nil
				}, time.Hour))

			err := db.RefuseIfReindexOverlapped(context.Background(), tc.collections, backupStart)
			if !tc.wantRefuse {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorIs(t, err, entitiesbackup.ErrBackupSpannedReindex)
			assert.Contains(t, err.Error(), "Actors")
		})
	}
}

// The two halves compose into a forbidden outcome, so pin the composition and
// not just the halves.
//
// A client disconnect makes every post-commit probe fail. If that verdict is
// allowed to roll a committed migration back, and the backstop then skips every
// cancelled task, a backup captured across a migration that had already started
// rebuilding buckets is published as SUCCESS. Each half is defensible alone;
// together they publish a corrupt backup as a good one. The submit side no
// longer rolls back without a positive "busy" (see probeBackupActivity), and
// this side no longer ignores a cancelled task that ran.
func TestOverlapBackstopCatchesARolledBackMigrationThatRan(t *testing.T) {
	backupStart := time.Now().Add(-2 * time.Minute)

	rolledBackAfterWriting := overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled,
		backupStart.Add(30*time.Second),
		map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress, Progress: 0.6},
		})

	lookup := NewReindexOverlapLookup(func(context.Context) (map[string][]*distributedtask.Task, error) {
		return map[string][]*distributedtask.Task{ReindexNamespace: {rolledBackAfterWriting}}, nil
	}, time.Hour)

	err := lookup(context.Background(), []string{"Movies"}, backupStart)
	require.ErrorIs(t, err, entitiesbackup.ErrBackupSpannedReindex,
		"a backup spanning a migration that ran must never be published as SUCCESS, "+
			"however that migration ended")
}

// A worker that will not exit holds its collection's cleanup gate until the cap.
// If the restore gate asks that question blind, one stuck collection refuses
// restores of every other collection for the whole hold.
func TestRefuseIfAnyReindexInFlightScopesTheCleanupCheckByCollection(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	db.SetAnyReindexActivityLookup(func(context.Context) (bool, error) { return false, nil })
	db.SetAnyCleanupInProgressLookup(func(collections []string) bool {
		for _, c := range collections {
			if c == "Stuck" {
				return true
			}
		}
		return len(collections) == 0
	})

	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Stuck"}),
		"the collection whose teardown is wedged must still be refused")
	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Unrelated"}),
		"an unrelated collection must not be refused by another collection's wedged teardown")
	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), nil),
		"with no class list yet the check has to stay blind, so it still refuses")
}
