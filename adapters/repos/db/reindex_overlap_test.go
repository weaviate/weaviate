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
	"errors"
	"fmt"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	entitiesbackup "github.com/weaviate/weaviate/entities/backup"
)

var (
	captureStart = time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	commitTime   = captureStart.Add(10 * time.Minute)
)

func noLocalWorker(distributedtask.TaskDescriptor) bool { return false }

func overlapTask(status distributedtask.TaskStatus, finishedAt time.Time, units map[string]*distributedtask.Unit) *distributedtask.Task {
	task := reindexTask("t1", status, payloadFor("Movies"))
	task.FinishedAt, task.Units = finishedAt, units
	return task
}

// unattributableTask names no collection, so it reads as cluster-wide.
func unattributableTask() *distributedtask.Task {
	task := reindexTask("t1", distributedtask.TaskStatusStarted, `not json`)
	task.Units = units(distributedtask.UnitStatusInProgress)
	return task
}

func units(statuses ...distributedtask.UnitStatus) map[string]*distributedtask.Unit {
	out := make(map[string]*distributedtask.Unit, len(statuses))
	for i, status := range statuses {
		out[string(rune('a'+i))] = &distributedtask.Unit{Status: status}
	}
	return out
}

// TestReindexOverlapRules walks every shape the commit-time check has to decide.
func TestReindexOverlapRules(t *testing.T) {
	tests := []struct {
		name           string
		task           *distributedtask.Task
		classes        []string
		hasLocalWorker bool
		want           ReindexOverlapOutcome
		wantDetail     string
	}{
		{
			name:    "a task still running at commit overlapped the capture",
			task:    overlapTask(distributedtask.TaskStatusStarted, time.Time{}, units(distributedtask.UnitStatusInProgress)),
			classes: []string{"Movies"},
			want:    ReindexOverlapLive,
		},
		{
			name:    "a task that finished inside the window overlapped it",
			task:    overlapTask(distributedtask.TaskStatusFinished, captureStart.Add(time.Minute), units(distributedtask.UnitStatusCompleted)),
			classes: []string{"Movies"},
			want:    ReindexOverlapEnded,
		},
		{
			name:    "a task that finished before the capture began is clear",
			task:    overlapTask(distributedtask.TaskStatusFinished, captureStart.Add(-time.Minute), units(distributedtask.UnitStatusCompleted)),
			classes: []string{"Movies"},
		},
		{
			// One unit's failure fails the whole task and stamps FinishedAt,
			// but the units still running are never waited for: they keep
			// writing until the scheduler's next tick terminates them.
			name:           "a terminal task whose worker has not stopped is still writing",
			task:           overlapTask(distributedtask.TaskStatusFailed, captureStart.Add(-time.Minute), units(distributedtask.UnitStatusFailed)),
			classes:        []string{"Movies"},
			hasLocalWorker: true,
			want:           ReindexOverlapLive,
		},
		{
			// Inclusive: a task recorded as finishing at the same instant
			// the capture began may have been mid-write when it started.
			name:    "the boundary counts as an overlap",
			task:    overlapTask(distributedtask.TaskStatusFinished, captureStart, units(distributedtask.UnitStatusCompleted)),
			classes: []string{"Movies"},
			want:    ReindexOverlapEnded,
		},
		{
			name:       "a terminal task with no finish time cannot be judged",
			task:       overlapTask(distributedtask.TaskStatusFailed, time.Time{}, units(distributedtask.UnitStatusFailed)),
			classes:    []string{"Movies"},
			want:       ReindexOverlapUndetermined,
			wantDetail: "without recording when it finished",
		},
		{
			name:    "a cancelled task that never left PENDING wrote nothing",
			task:    overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), units(distributedtask.UnitStatusPending, distributedtask.UnitStatusPending)),
			classes: []string{"Movies"},
		},
		{
			name:    "a cancelled task with a claimed unit still overlapped",
			task:    overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), units(distributedtask.UnitStatusPending, distributedtask.UnitStatusInProgress)),
			classes: []string{"Movies"},
			want:    ReindexOverlapEnded,
		},
		{
			name:    "a cancelled task with a completed unit still overlapped",
			task:    overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), units(distributedtask.UnitStatusCompleted)),
			classes: []string{"Movies"},
			want:    ReindexOverlapEnded,
		},
		{
			name:           "a live local worker means all-PENDING is not proof nothing was written",
			task:           overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), units(distributedtask.UnitStatusPending)),
			classes:        []string{"Movies"},
			hasLocalWorker: true,
			want:           ReindexOverlapLive,
		},
		{
			name:    "a migration on a collection this backup did not capture",
			task:    overlapTask(distributedtask.TaskStatusStarted, time.Time{}, units(distributedtask.UnitStatusInProgress)),
			classes: []string{"Shows"},
		},
		{
			name:    "a task nothing can attribute fails a backup that captured something",
			task:    unattributableTask(),
			classes: []string{"Shows"},
			want:    ReindexOverlapLive,
		},
		{
			// The opposite of the restore gate's empty list: a backup that
			// captured nothing has nothing that could have been rewritten.
			name:    "a backup that captured no class cannot be overlapped",
			task:    unattributableTask(),
			classes: nil,
		},
		{
			name:    "collection matching ignores case",
			task:    overlapTask(distributedtask.TaskStatusStarted, time.Time{}, units(distributedtask.UnitStatusInProgress)),
			classes: []string{"MOVIES"},
			want:    ReindexOverlapLive,
		},
		{
			// The liveness predicate reads it as live, which would publish a
			// migration that ended before this capture as an observed overlap.
			name:       "a status a newer node introduced cannot be judged",
			task:       overlapTask(distributedtask.TaskStatus("REBALANCING"), captureStart.Add(-time.Hour), units(distributedtask.UnitStatusCompleted)),
			classes:    []string{"Movies"},
			want:       ReindexOverlapUndetermined,
			wantDetail: "a status this node cannot name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := func(distributedtask.TaskDescriptor) bool { return tt.hasLocalWorker }
			lookup := NewReindexOverlapLookup([]*distributedtask.Task{tt.task},
				24*time.Hour, worker, func() time.Time { return commitTime })

			verdict := lookup(tt.classes, captureStart)

			assert.Equal(t, tt.want, verdict.Outcome)
			if tt.wantDetail != "" {
				assert.Contains(t, verdict.Detail, tt.wantDetail)
			}
			if verdict.Outcome == ReindexOverlapUndetermined {
				assert.NotEmpty(t, verdict.Remedy,
					"an answer nobody can act on is worse than no answer")
			}
		})
	}
}

// TestListReindexTasksForOverlapRetries pins that a brief RAFT outage does not
// discard a finished upload.
func TestListReindexTasksForOverlapRetries(t *testing.T) {
	noDelays := []time.Duration{0, 0, 0}
	listed := map[string][]*distributedtask.Task{
		ReindexNamespace: {reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor("Movies"))},
	}

	t.Run("a later attempt answers", func(t *testing.T) {
		calls := 0
		got, err := ListReindexTasksForOverlap(context.Background(),
			func(context.Context) (map[string][]*distributedtask.Task, error) {
				calls++
				if calls <= len(noDelays) {
					return nil, errors.New("leader unknown")
				}
				return listed, nil
			}, noDelays)

		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, len(noDelays)+1, calls)
	})

	t.Run("every attempt fails", func(t *testing.T) {
		calls := 0
		_, err := ListReindexTasksForOverlap(context.Background(),
			func(context.Context) (map[string][]*distributedtask.Task, error) {
				calls++
				return nil, errors.New("leader unknown")
			}, noDelays)

		require.Error(t, err)
		assert.Equal(t, len(noDelays)+1, calls)
	})

	t.Run("a cancelled context stops the retries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		_, err := ListReindexTasksForOverlap(ctx,
			func(context.Context) (map[string][]*distributedtask.Task, error) {
				calls++
				cancel()
				return nil, errors.New("leader unknown")
			}, OverlapListRetryDelays)

		require.Error(t, err)
		assert.Equal(t, 1, calls, "no waiting out a 30s schedule after a cancel")
	})
}

// TestReindexOverlapRetentionWindow pins the one answer the check cannot give:
// a backup that outlived the retention window cannot be cleared, because the
// task that would have failed it may already have been dropped.
func TestReindexOverlapRetentionWindow(t *testing.T) {
	liveMatching := overlapTask(distributedtask.TaskStatusStarted, time.Time{},
		units(distributedtask.UnitStatusInProgress))
	// Defensive, not a state anything produces: a cancel writes the unit map,
	// so an empty one is a record this build cannot read.
	cancelledNoUnits := overlapTask(distributedtask.TaskStatusCancelled,
		captureStart.Add(time.Minute), nil)

	tests := []struct {
		name       string
		tasks      []*distributedtask.Task
		ttl        time.Duration
		age        time.Duration
		want       ReindexOverlapOutcome
		wantDetail string
		notDetail  string
		wantRemedy string
	}{
		{
			name: "inside the window an empty list clears the capture",
			ttl:  time.Hour, age: time.Hour - time.Minute,
		},
		{
			name: "at the window it can no longer be cleared",
			ttl:  time.Hour, age: time.Hour,
			want: ReindexOverlapUndetermined, wantDetail: "window in which a finished migration stays listed",
			wantRemedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
		},
		{
			name: "a zero window retains nothing, so nothing can be cleared",
			ttl:  0, age: time.Minute,
			want: ReindexOverlapUndetermined, wantDetail: "window in which a finished migration stays listed",
			wantRemedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
		},
		{
			name: "a negative window is a zero window",
			ttl:  -time.Hour, age: time.Minute,
			want: ReindexOverlapUndetermined, wantDetail: "window in which a finished migration stays listed",
			wantRemedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
		},
		{
			name:  "a listed task answers even past the window",
			tasks: []*distributedtask.Task{liveMatching},
			ttl:   time.Hour, age: time.Hour + time.Minute,
			want: ReindexOverlapLive, notDetail: "window in which a finished migration",
		},
		{
			name:  "a listed task's own unanswerable reason beats the window's",
			tasks: []*distributedtask.Task{cancelledNoUnits},
			ttl:   time.Hour, age: time.Hour + time.Minute,
			want: ReindexOverlapUndetermined, wantDetail: "recorded no units",
			notDetail:  "window in which a finished migration",
			wantRemedy: "until the cluster task list drops it",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verdict := NewReindexOverlapLookup(tt.tasks, tt.ttl, noLocalWorker,
				func() time.Time { return captureStart.Add(tt.age) })([]string{"Movies"}, captureStart)

			assert.Equal(t, tt.want, verdict.Outcome)
			if tt.wantDetail != "" {
				assert.Contains(t, verdict.Detail, tt.wantDetail)
			}
			if tt.notDetail != "" {
				assert.NotContains(t, verdict.Detail, tt.notDetail)
			}
			if tt.wantRemedy != "" {
				assert.Contains(t, verdict.Remedy, tt.wantRemedy)
			}
		})
	}
}

// TestReindexOverlapRanksTheStrongestAnswer pins that the scan does not stop at
// the first task that refuses. Ties keep the lower task id and ids carry a
// random suffix, so a weaker answer sorting first must not win.
func TestReindexOverlapRanksTheStrongestAnswer(t *testing.T) {
	// No units reads as undetermined; a completed one as an overlap that ended.
	cancelled := func(id string, us map[string]*distributedtask.Unit) *distributedtask.Task {
		task := reindexTask(id, distributedtask.TaskStatusCancelled, payloadFor("Movies"))
		task.FinishedAt, task.Units = captureStart.Add(time.Minute), us
		return task
	}
	ended := units(distributedtask.UnitStatusCompleted)
	live := reindexTask("b", distributedtask.TaskStatusStarted, payloadFor("Movies"))
	live.Units = units(distributedtask.UnitStatusInProgress)

	// A record nothing here can judge may be a migration still running, so it
	// must not lose to one known to be over either.
	tests := []struct {
		want  ReindexOverlapOutcome
		tasks []*distributedtask.Task
	}{
		{ReindexOverlapLive, []*distributedtask.Task{cancelled("a", nil), live}},
		{ReindexOverlapLive, []*distributedtask.Task{cancelled("a", ended), live}},
		{ReindexOverlapUndetermined, []*distributedtask.Task{cancelled("a", ended), cancelled("b", nil)}},
	}

	for _, tt := range tests {
		verdict := NewReindexOverlapLookup(tt.tasks, 24*time.Hour, noLocalWorker,
			func() time.Time { return commitTime })([]string{"Movies"}, captureStart)

		require.Equal(t, tt.want, verdict.Outcome)
		require.Equal(t, "b", verdict.TaskID, "the stronger answer is the one to name")
	}
}

// TestBackupableRefusesWhenTheOverlapCheckCannotAnswer pins that a backup is
// refused at admission when the commit-time check could never answer.
func TestBackupableRefusesWhenTheOverlapCheckCannotAnswer(t *testing.T) {
	tests := []struct {
		name        string
		disabled    bool
		unwired     bool
		ttl         time.Duration
		wantRefused bool
	}{
		{name: "the feature is on and nothing is retained", ttl: 0, wantRefused: true},
		{name: "the feature is on and the window is wide", ttl: 120 * time.Hour},
		{name: "the feature is off, so no check needs the evidence", disabled: true, ttl: 0},
		{
			name: "the check is not installed", unwired: true, ttl: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{config: Config{RuntimeReindexDisabled: tt.disabled, CompletedTaskTTL: tt.ttl}}
			if !tt.unwired {
				db.SetReindexOverlapLookup(func(context.Context) ReindexOverlapLookup {
					return NewReindexOverlapLookup(nil, tt.ttl, noLocalWorker, time.Now)
				})
			}

			err := db.Backupable(context.Background(), nil)

			if !tt.wantRefused {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, entitiesbackup.ErrReindexOverlapCheckUnanswerable)
			require.NotErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex,
				"nothing is in flight; that sentinel makes the coordinator promise a migration will end")
			assert.Contains(t, err.Error(), "DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS")
			assert.NotContains(t, err.Error(), "RUNTIME_REINDEX_ENABLED=false",
				"that flag gates the submit endpoints, not a migration already running")
		})
	}
}

// TestReindexOverlapNamesTheSameTask pins that two nodes committing the
// same backup name the same task, whatever order the list arrived in.
func TestReindexOverlapNamesTheSameTask(t *testing.T) {
	mk := func(id string) *distributedtask.Task {
		return reindexTask(id, distributedtask.TaskStatusStarted, payloadFor("Movies"))
	}
	ascending := []*distributedtask.Task{mk("task-a"), mk("task-b")}
	descending := []*distributedtask.Task{mk("task-b"), mk("task-a")}

	for _, tasks := range [][]*distributedtask.Task{ascending, descending} {
		verdict := NewReindexOverlapLookup(tasks, time.Hour, noLocalWorker,
			func() time.Time { return commitTime })([]string{"Movies"}, captureStart)
		require.Equal(t, ReindexOverlapLive, verdict.Outcome)
		require.Equal(t, "task-a", verdict.TaskID)
	}
}

// TestReindexOverlapRefusalWording pins what every refusal has to say and that
// the observed and undetermined answers stay distinguishable all the way out.
func TestReindexOverlapRefusalWording(t *testing.T) {
	tests := []struct {
		name         string
		verdict      ReindexOverlapVerdict
		classes      []string
		wantSentinel error
		notSentinel  error
		wantFinding  string
		wantRemedy   string
		notContains  []string
	}{
		{
			name:         "an overlap on a collection this backup captured",
			verdict:      ReindexOverlapVerdict{Outcome: ReindexOverlapLive, Collection: "Movies", TaskID: "t1"},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlappedBackup,
			notSentinel:  entitiesbackup.ErrReindexOverlapUndetermined,
			wantFinding:  `collection "Movies" was migrated while this backup was being captured`,
			wantRemedy:   "GET /v1/schema/Movies/indexes",
		},
		{
			name:         "an overlap the caller spelled differently",
			verdict:      ReindexOverlapVerdict{Outcome: ReindexOverlapLive, Collection: "Movies"},
			classes:      []string{"movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlappedBackup,
			wantFinding:  `collection "movies" was migrated`,
			wantRemedy:   "GET /v1/schema/movies/indexes",
			notContains:  []string{`"Movies"`},
		},
		{
			// The check never attributed this to a captured collection, so the
			// text may not claim it did.
			name:         "an overlap on a collection this backup never captured",
			verdict:      ReindexOverlapVerdict{Outcome: ReindexOverlapLive, Collection: "Secret"},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlappedBackup,
			wantFinding:  "cannot be attributed to a collection",
			wantRemedy:   "wait for that migration to finish",
			notContains: []string{
				"Secret", "/v1/schema",
				"a collection this backup captured was migrated",
			},
		},
		{
			name:         "an overlap on a task that named no collection at all",
			verdict:      ReindexOverlapVerdict{Outcome: ReindexOverlapLive},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlappedBackup,
			wantFinding:  "cannot be attributed to a collection",
			wantRemedy:   "wait for that migration to finish",
			notContains:  []string{"Movies", "/v1/schema"},
		},
		{
			// The migration ended before the check ran, so every step of the
			// live remedy - poll the indexes, cancel while STARTED - is a step
			// on a task that is already gone.
			name:         "an overlap by a migration that is already over",
			verdict:      ReindexOverlapVerdict{Outcome: ReindexOverlapEnded, Collection: "Movies"},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlappedBackup,
			wantFinding:  `collection "Movies" was migrated`,
			wantRemedy:   "that migration is already over",
			notContains:  []string{"wait for that migration to finish", "/v1/schema"},
		},
		{
			name: "a backup that outlived the retention window",
			verdict: ReindexOverlapVerdict{
				Outcome: ReindexOverlapUndetermined,
				Detail: "this backup ran for 2h0m0s and reached the 1h0m0s window in which a " +
					"finished migration stays listed",
				Remedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS above the time a backup takes",
			},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlapUndetermined,
			notSentinel:  entitiesbackup.ErrReindexOverlappedBackup,
			wantFinding:  "window in which a finished migration stays listed",
			wantRemedy:   "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
			notContains: []string{
				"overlapped this backup",
				// Nothing is in flight here; the task may already be gone.
				"in flight",
			},
		},
		{
			// Regression: a cancel-quoting detail must not read as an operator abort.
			name: "a detail that quotes a cancelled context",
			verdict: ReindexOverlapVerdict{
				Outcome: ReindexOverlapUndetermined,
				Detail:  "the cluster task manager could not be listed: " + context.Canceled.Error(),
				Remedy:  "restore RAFT reachability from this node",
			},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlapUndetermined,
			wantFinding:  "the cluster task manager could not be listed",
			wantRemedy:   "restore RAFT reachability",
			notContains:  []string{context.Canceled.Error()},
		},
		{
			name: "a migration record too incomplete to judge",
			verdict: ReindexOverlapVerdict{
				Outcome: ReindexOverlapUndetermined,
				Detail:  "a cancelled migration recorded no units, so nothing says whether it wrote",
				Remedy:  ReindexOverlapIncompleteRecordRemedy,
				TaskID:  "t1",
			},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlapUndetermined,
			notSentinel:  entitiesbackup.ErrReindexOverlappedBackup,
			wantFinding:  "recorded no units",
			wantRemedy:   "until the cluster task list drops it",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := reindexOverlapRefusal(tt.verdict, tt.classes)

			require.ErrorIs(t, err, tt.wantSentinel)
			if tt.notSentinel != nil {
				require.NotErrorIs(t, err, tt.notSentinel)
			}
			assert.Contains(t, err.Error(), tt.wantFinding)
			assert.Contains(t, err.Error(), tt.wantRemedy)
			assert.Contains(t, err.Error(), "This backup id is spent",
				"a same-id retry answers 422 already exists, so saying only retry is wrong")
			assert.Contains(t, err.Error(), "is not removed automatically",
				"nothing else tells an operator the partial upload is still there")
			assert.NotContains(t, err.Error(), "t1",
				"the task id stays in the WARN; one disclosure rule for the whole subsystem")
			for _, unwanted := range tt.notContains {
				assert.NotContains(t, err.Error(), unwanted)
			}
		})
	}
}

func TestRefuseIfReindexOverlapped(t *testing.T) {
	overlappingTasks := []*distributedtask.Task{
		overlapTask(distributedtask.TaskStatusStarted, time.Time{}, units(distributedtask.UnitStatusInProgress)),
	}

	t.Run("fails the backup and keeps the task id out of it", func(t *testing.T) {
		db, hook, built := gatedDB(t, gateFixtures{overlap: overlappingTasks})

		err := db.RefuseIfReindexOverlapped(context.Background(), []string{"Movies"}, captureStart)
		require.Error(t, err)
		require.ErrorIs(t, err, entitiesbackup.ErrReindexOverlappedBackup)
		assert.NotContains(t, err.Error(), "t1")
		assert.Equal(t, 1, built.overlap, "the check runs once per commit")

		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, "t1", hook.AllEntries()[0].Data["task_id"])
		assert.Equal(t, reindexReasonOverlapObserved, hook.AllEntries()[0].Data["reason"])
	})

	t.Run("passes a backup nothing rewrote", func(t *testing.T) {
		db, hook, _ := gatedDB(t, gateFixtures{overlap: overlappingTasks})

		require.NoError(t, db.RefuseIfReindexOverlapped(context.Background(), []string{"Shows"}, captureStart))
		require.Empty(t, hook.AllEntries())
	})

	t.Run("the feature flag skips the check", func(t *testing.T) {
		db, _, built := gatedDB(t, gateFixtures{overlap: overlappingTasks})
		db.config.RuntimeReindexDisabled = true

		require.NoError(t, db.RefuseIfReindexOverlapped(context.Background(), []string{"Movies"}, captureStart))
		assert.Zero(t, built.overlap)
	})

	t.Run("a builder that answers with no lookup passes and reports", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()
		db := &DB{logger: logger}
		db.SetReindexOverlapLookup(func(context.Context) ReindexOverlapLookup { return nil })
		overlapCheckWarnBudget = reindexGateWarnBudget{}

		require.NoError(t, db.RefuseIfReindexOverlapped(context.Background(), []string{"Movies"}, captureStart))
		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, "commit-time overlap", hook.AllEntries()[0].Data["gate"])
	})

	t.Run("an operator cancel is not an unanswerable check", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		logger, hook := logrustest.NewNullLogger()
		db := &DB{logger: logger}
		db.SetReindexOverlapLookup(func(context.Context) ReindexOverlapLookup {
			cancel()
			return func([]string, time.Time) ReindexOverlapVerdict {
				return ReindexOverlapVerdict{Outcome: ReindexOverlapUndetermined, Detail: "the cluster task manager could not be listed"}
			}
		})

		err := db.RefuseIfReindexOverlapped(ctx, []string{"Movies"}, captureStart)

		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, entitiesbackup.ErrReindexOverlapUndetermined,
			"the operator stopped this backup; it must publish as cancelled, not failed")
		require.Empty(t, hook.AllEntries())
	})

	t.Run("an observed overlap outranks an operator cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		db, _, _ := gatedDB(t, gateFixtures{overlap: overlappingTasks})

		err := db.RefuseIfReindexOverlapped(ctx, []string{"Movies"}, captureStart)

		require.ErrorIs(t, err, entitiesbackup.ErrReindexOverlappedBackup)
		require.NotErrorIs(t, err, context.Canceled,
			"a cancelled backup id can be posted again over the torn capture")
	})

	t.Run("an expired deadline is still an unanswerable check", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), captureStart)
		defer cancel()
		logger, _ := logrustest.NewNullLogger()
		db := &DB{logger: logger}
		db.SetReindexOverlapLookup(func(context.Context) ReindexOverlapLookup {
			return func([]string, time.Time) ReindexOverlapVerdict {
				return ReindexOverlapVerdict{Outcome: ReindexOverlapUndetermined, Detail: "d", Remedy: "r"}
			}
		})

		err := db.RefuseIfReindexOverlapped(ctx, []string{"Movies"}, captureStart)

		require.ErrorIs(t, err, entitiesbackup.ErrReindexOverlapUndetermined)
		require.NotErrorIs(t, err, context.DeadlineExceeded,
			"nobody aborted this backup, and a bare timeout names no sentinel and no remedy")
	})

	t.Run("a wide backup logs one bounded entry", func(t *testing.T) {
		classes := make([]string, 0, 500)
		for i := range 500 {
			classes = append(classes, "Class"+string(rune('A'+i%26)))
		}
		classes = append(classes, "Movies")
		db, hook, _ := gatedDB(t, gateFixtures{overlap: overlappingTasks})

		require.Error(t, db.RefuseIfReindexOverlapped(context.Background(), classes, captureStart))
		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, len(classes), hook.AllEntries()[0].Data["captured_class_count"])
		assert.Len(t, hook.AllEntries()[0].Data["captured_classes"], reindexRefusalSampleLimit)
	})
}

// TestAdmissionAndCommitCheckDisagree walks the payload shapes where the shard
// gate that admits a backup and the commit-time check that judges it disagree.
// Every disagreement here is a backup admitted and then failed after the whole
// upload.
func TestAdmissionAndCommitCheckDisagree(t *testing.T) {
	tests := []struct {
		name                 string
		payload              string
		wantAdmissionRefuses bool
		wantCommitRefuses    bool
	}{
		{
			name:                 "a task naming the collection and its shards",
			payload:              `{"collection":"Movies","unitToShard":{"u1":"shard-1"}}`,
			wantAdmissionRefuses: true,
			wantCommitRefuses:    true,
		},
		{
			// Admission skips a task it cannot decode; the commit-time check
			// reads the same task as naming no collection and refuses every
			// backup. Deliberate, filed as weaviate/0-weaviate-issues#573;
			// red the day that is fixed.
			name:              "a task whose shard set did not decode",
			payload:           `{"collection":"Movies","unitToShard":"shard-1"}`,
			wantCommitRefuses: true,
		},
		{
			name:              "a task that cannot be attributed at all",
			payload:           `not json`,
			wantCommitRefuses: true,
		},
		{
			name:    "a task on another collection",
			payload: `{"collection":"Shows","unitToShard":{"u1":"shard-1"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := reindexTask("t1", distributedtask.TaskStatusStarted, tt.payload)
			logger, _ := logrustest.NewNullLogger()

			admissionRefuses := NewShardReindexActivityLookup(
				[]*distributedtask.Task{task}, logger)("Movies", "shard-1")
			commitRefuses := NewReindexOverlapLookup([]*distributedtask.Task{task},
				24*time.Hour, noLocalWorker,
				func() time.Time { return commitTime })([]string{"Movies"}, captureStart).Outcome != ReindexOverlapNone

			assert.Equal(t, tt.wantAdmissionRefuses, admissionRefuses, "admission gate")
			assert.Equal(t, tt.wantCommitRefuses, commitRefuses, "commit-time check")
		})
	}
}

// recordingRecorder answers every call and remembers the order they came in.
type recordingRecorder struct {
	calls       []string
	progressErr error
}

func (r *recordingRecorder) UpdateDistributedTaskUnitProgress(
	_ context.Context, _, _ string, _ uint64, _, unitID string, progress float32,
) error {
	r.calls = append(r.calls, fmt.Sprintf("progress %s %v", unitID, progress))
	return r.progressErr
}

func (r *recordingRecorder) RecordDistributedTaskUnitCompletion(
	_ context.Context, _, _ string, _ uint64, _, unitID string,
) error {
	r.calls = append(r.calls, "completed "+unitID)
	return nil
}

func (r *recordingRecorder) RecordDistributedTaskUnitFailure(
	_ context.Context, _, _ string, _ uint64, _, unitID, _ string,
) error {
	r.calls = append(r.calls, "failed "+unitID)
	return nil
}

// TestReindexWorkerClaimsBeforeItWrites pins the invariant: a unit leaves
// PENDING only once the worker's claim (0.0 progress via RAFT) lands, before
// it ever touches a shard. index is nil so any later step panics.
func TestReindexWorkerClaimsBeforeItWrites(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	p := &ReindexProvider{logger: logger, localNode: "node-1"}
	task := reindexTask("t1", distributedtask.TaskStatusStarted, payloadFor("Movies"))
	payload := &ReindexTaskPayload{
		Collection:  "Movies",
		UnitToNode:  map[string]string{"u1": "node-1"},
		UnitToShard: map[string]string{"u1": "s1"},
	}
	recorder := &recordingRecorder{progressErr: errors.New("no raft leader")}

	require.NotPanics(t, func() {
		p.processOneUnit(context.Background(), task, payload, nil, "u1", recorder, &selectedPropsFailures{})
	}, "the worker reached the shard before claiming the unit")

	assert.Equal(t, []string{"progress u1 0"}, recorder.calls,
		"the claim is the first thing the worker does, and a claim it could not land stops it")
}

// TestHasActiveWorker walks the disjunct behind the all-PENDING waiver.
func TestHasActiveWorker(t *testing.T) {
	desc := distributedtask.TaskDescriptor{ID: "t1", Version: 1}
	other := distributedtask.TaskDescriptor{ID: "t2", Version: 1}

	tests := []struct {
		name           string
		activeWorkers  map[distributedtask.TaskDescriptor]map[string]bool
		runningHandles map[distributedtask.TaskDescriptor]*reindexTaskHandle
		want           bool
	}{
		{name: "nothing running"},
		{
			name:          "a unit goroutine is running",
			activeWorkers: map[distributedtask.TaskDescriptor]map[string]bool{desc: {"u1": true}},
			want:          true,
		},
		{
			name:           "the task is registered but no unit has started",
			runningHandles: map[distributedtask.TaskDescriptor]*reindexTaskHandle{desc: {}},
			want:           true,
		},
		{
			name:           "another task is running",
			activeWorkers:  map[distributedtask.TaskDescriptor]map[string]bool{other: {"u1": true}},
			runningHandles: map[distributedtask.TaskDescriptor]*reindexTaskHandle{other: {}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ReindexProvider{
				activeWorkers:  map[distributedtask.TaskDescriptor]map[string]bool{},
				runningHandles: map[distributedtask.TaskDescriptor]*reindexTaskHandle{},
			}
			for k, v := range tt.activeWorkers {
				p.activeWorkers[k] = v
			}
			for k, v := range tt.runningHandles {
				p.runningHandles[k] = v
			}

			require.Equal(t, tt.want, p.HasActiveWorker(desc))
		})
	}
}
