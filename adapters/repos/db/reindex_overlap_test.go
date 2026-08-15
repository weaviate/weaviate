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

// noLocalWorker is the answer a node with nothing running gives.
func noLocalWorker(distributedtask.TaskDescriptor) bool { return false }

func overlapTask(status distributedtask.TaskStatus, finishedAt time.Time, units map[string]*distributedtask.Unit) *distributedtask.Task {
	task := reindexTask("t1", status, payloadFor("Movies"))
	task.FinishedAt, task.Units = finishedAt, units
	return task
}

func units(statuses ...distributedtask.UnitStatus) map[string]*distributedtask.Unit {
	out := make(map[string]*distributedtask.Unit, len(statuses))
	for i, status := range statuses {
		out[string(rune('a'+i))] = &distributedtask.Unit{Status: status}
	}
	return out
}

// TestReindexOverlapRules walks every shape the commit-time check has to
// decide. The question is overlap, not liveness: a migration that started
// and finished inside the capture window is gone from every "is anything
// running?" answer and still left the captured files half-rewritten.
func TestReindexOverlapRules(t *testing.T) {
	tests := []struct {
		name             string
		task             *distributedtask.Task
		classes          []string
		hasLocalWorker   bool
		wantOverlapped   bool
		wantUndetermined bool
		wantDetail       string
	}{
		{
			name:           "a task still running at commit overlapped the capture",
			task:           overlapTask(distributedtask.TaskStatusStarted, time.Time{}, units(distributedtask.UnitStatusInProgress)),
			classes:        []string{"Movies"},
			wantOverlapped: true,
		},
		{
			name:           "a task that finished inside the window overlapped it",
			task:           overlapTask(distributedtask.TaskStatusFinished, captureStart.Add(time.Minute), units(distributedtask.UnitStatusCompleted)),
			classes:        []string{"Movies"},
			wantOverlapped: true,
		},
		{
			name:    "a task that finished before the capture began is clear",
			task:    overlapTask(distributedtask.TaskStatusFinished, captureStart.Add(-time.Minute), units(distributedtask.UnitStatusCompleted)),
			classes: []string{"Movies"},
		},
		{
			// Inclusive: a task recorded as finishing at the same instant
			// the capture began may have been mid-write when it started.
			name:           "the boundary counts as an overlap",
			task:           overlapTask(distributedtask.TaskStatusFinished, captureStart, units(distributedtask.UnitStatusCompleted)),
			classes:        []string{"Movies"},
			wantOverlapped: true,
		},
		{
			name:             "a terminal task with no finish time cannot be judged",
			task:             overlapTask(distributedtask.TaskStatusFailed, time.Time{}, units(distributedtask.UnitStatusFailed)),
			classes:          []string{"Movies"},
			wantUndetermined: true,
			wantDetail:       "without recording when it finished",
		},
		{
			// What a submission that lost the race to a backup produces: the
			// cancel landed before any unit was claimed, so nothing was written.
			name:    "a cancelled task that never left PENDING wrote nothing",
			task:    overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), units(distributedtask.UnitStatusPending, distributedtask.UnitStatusPending)),
			classes: []string{"Movies"},
		},
		{
			// A cancel that landed after a worker claimed a unit did not
			// arrive before the writes it was meant to prevent.
			name:           "a cancelled task with a claimed unit still overlapped",
			task:           overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), units(distributedtask.UnitStatusPending, distributedtask.UnitStatusInProgress)),
			classes:        []string{"Movies"},
			wantOverlapped: true,
		},
		{
			name:           "a cancelled task with a completed unit still overlapped",
			task:           overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), units(distributedtask.UnitStatusCompleted)),
			classes:        []string{"Movies"},
			wantOverlapped: true,
		},
		{
			name:             "a cancelled task with no units is unknown, not untouched",
			task:             overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), nil),
			classes:          []string{"Movies"},
			wantUndetermined: true,
			wantDetail:       "recorded no units",
		},
		{
			name:             "a cancelled task with an empty unit map is unknown too",
			task:             overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), map[string]*distributedtask.Unit{}),
			classes:          []string{"Movies"},
			wantUndetermined: true,
			wantDetail:       "recorded no units",
		},
		{
			name: "a nil unit entry is unknown rather than a panic",
			task: overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute),
				map[string]*distributedtask.Unit{"a": nil}),
			classes:          []string{"Movies"},
			wantUndetermined: true,
			wantDetail:       "a unit with no state",
		},
		{
			// A worker registers before its first progress report flips a
			// unit out of PENDING, so all-PENDING units and a live worker
			// is not the same state as all-PENDING and nothing running.
			name:           "a live local worker means all-PENDING is not proof nothing was written",
			task:           overlapTask(distributedtask.TaskStatusCancelled, captureStart.Add(time.Minute), units(distributedtask.UnitStatusPending)),
			classes:        []string{"Movies"},
			hasLocalWorker: true,
			wantOverlapped: true,
		},
		{
			name:    "a migration on a collection this backup did not capture",
			task:    overlapTask(distributedtask.TaskStatusStarted, time.Time{}, units(distributedtask.UnitStatusInProgress)),
			classes: []string{"Shows"},
		},
		{
			// The opposite of the restore gate's empty list: a backup that
			// captured nothing has nothing that could have been rewritten.
			name:    "a backup that captured no class cannot be overlapped",
			task:    overlapTask(distributedtask.TaskStatusStarted, time.Time{}, units(distributedtask.UnitStatusInProgress)),
			classes: nil,
		},
		{
			name:           "collection matching ignores case",
			task:           overlapTask(distributedtask.TaskStatusStarted, time.Time{}, units(distributedtask.UnitStatusInProgress)),
			classes:        []string{"MOVIES"},
			wantOverlapped: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			worker := func(distributedtask.TaskDescriptor) bool { return tt.hasLocalWorker }
			lookup := NewReindexOverlapLookup([]*distributedtask.Task{tt.task},
				24*time.Hour, worker, func() time.Time { return commitTime })

			verdict := lookup(tt.classes, captureStart)

			assert.Equal(t, tt.wantOverlapped, verdict.Overlapped)
			assert.Equal(t, tt.wantUndetermined, verdict.Undetermined)
			if tt.wantDetail != "" {
				assert.Contains(t, verdict.Detail, tt.wantDetail)
			}
			if verdict.Undetermined {
				assert.NotEmpty(t, verdict.Remedy,
					"an answer nobody can act on is worse than no answer")
			}
		})
	}
}

// TestListReindexTasksForOverlapRetries pins that a brief RAFT outage does
// not discard a finished upload: the list is retried before the check gives
// up, and a cancel stops the wait instead of sitting out the schedule.
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

// TestReindexOverlapUnattributableTask pins both sides of a task whose
// payload names no collection: it fails any backup that captured
// something, because nothing says which collection it rewrote, and it
// still cannot fail a backup that captured nothing.
func TestReindexOverlapUnattributableTask(t *testing.T) {
	task := reindexTask("t1", distributedtask.TaskStatusStarted, `{"unitToShard":{"u1":"s1"}}`)
	lookup := NewReindexOverlapLookup([]*distributedtask.Task{task},
		24*time.Hour, noLocalWorker, func() time.Time { return commitTime })

	assert.True(t, lookup([]string{"Shows"}, captureStart).Overlapped)
	assert.True(t, lookup(nil, captureStart).allowsBackup(),
		"a backup that captured nothing has nothing a migration could have rewritten")
}

// TestReindexOverlapRetentionWindow pins the one answer the check cannot
// give, and where in the order it gives it. A backup that outlived the
// window in which finished tasks stay listed cannot be cleared, because
// the task that would have failed it may already have been dropped. A
// task that is still listed answers first, whatever the window says.
func TestReindexOverlapRetentionWindow(t *testing.T) {
	liveMatching := overlapTask(distributedtask.TaskStatusStarted, time.Time{},
		units(distributedtask.UnitStatusInProgress))
	cancelledNoUnits := overlapTask(distributedtask.TaskStatusCancelled,
		captureStart.Add(time.Minute), nil)

	tests := []struct {
		name             string
		tasks            []*distributedtask.Task
		ttl              time.Duration
		age              time.Duration
		wantOverlapped   bool
		wantUndetermined bool
		wantDetail       string
		notDetail        string
		wantRemedy       string
	}{
		{
			name: "inside the window an empty list clears the capture",
			ttl:  time.Hour, age: time.Hour - time.Minute,
		},
		{
			name: "at the window it can no longer be cleared",
			ttl:  time.Hour, age: time.Hour,
			wantUndetermined: true, wantDetail: "window in which a finished migration stays listed",
			wantRemedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
		},
		{
			name: "past the window either",
			ttl:  time.Hour, age: time.Hour + time.Minute,
			wantUndetermined: true, wantDetail: "window in which a finished migration stays listed",
			wantRemedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
		},
		{
			// The value under which the evidence is guaranteed gone: every
			// terminal task is collectable on the next scheduler tick.
			name: "a zero window retains nothing, so nothing can be cleared",
			ttl:  0, age: time.Minute,
			wantUndetermined: true, wantDetail: "window in which a finished migration stays listed",
			wantRemedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
		},
		{
			name: "a negative window is a zero window",
			ttl:  -time.Hour, age: time.Minute,
			wantUndetermined: true, wantDetail: "window in which a finished migration stays listed",
			wantRemedy: "raise DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS",
		},
		{
			name:  "a listed task answers even past the window",
			tasks: []*distributedtask.Task{liveMatching},
			ttl:   time.Hour, age: time.Hour + time.Minute,
			wantOverlapped: true, notDetail: "window in which a finished migration",
		},
		{
			name:  "a listed task's own unanswerable reason beats the window's",
			tasks: []*distributedtask.Task{cancelledNoUnits},
			ttl:   time.Hour, age: time.Hour + time.Minute,
			wantUndetermined: true, wantDetail: "recorded no units",
			notDetail:  "window in which a finished migration",
			wantRemedy: "until the cluster task list drops it",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			verdict := NewReindexOverlapLookup(tt.tasks, tt.ttl, noLocalWorker,
				func() time.Time { return captureStart.Add(tt.age) })([]string{"Movies"}, captureStart)

			assert.Equal(t, tt.wantOverlapped, verdict.Overlapped)
			assert.Equal(t, tt.wantUndetermined, verdict.Undetermined)
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

// TestReindexOverlapRanksTheStrongestAnswer pins that the scan does not
// stop at the first task that refuses. An earlier task nobody can judge
// must not hide a later one that proves the capture was rewritten.
func TestReindexOverlapRanksTheStrongestAnswer(t *testing.T) {
	unanswerable := reindexTask("a", distributedtask.TaskStatusCancelled, payloadFor("Movies"))
	unanswerable.FinishedAt = captureStart.Add(time.Minute)
	live := reindexTask("b", distributedtask.TaskStatusStarted, payloadFor("Movies"))
	live.Units = units(distributedtask.UnitStatusInProgress)

	verdict := NewReindexOverlapLookup([]*distributedtask.Task{unanswerable, live},
		24*time.Hour, noLocalWorker, func() time.Time { return commitTime })([]string{"Movies"}, captureStart)

	require.True(t, verdict.Overlapped)
	require.False(t, verdict.Undetermined)
	require.Equal(t, "b", verdict.TaskID)
}

// TestBackupableRefusesWhenTheOverlapCheckCannotAnswer pins the admission
// half of the same rule. Refusing here costs an operator one 422; refusing
// at commit time costs them the whole upload and the backup id with it.
func TestBackupableRefusesWhenTheOverlapCheckCannotAnswer(t *testing.T) {
	tests := []struct {
		name        string
		disabled    bool
		ttl         time.Duration
		wantRefused bool
	}{
		{name: "the feature is on and nothing is retained", ttl: 0, wantRefused: true},
		{name: "the feature is on and the window is wide", ttl: 120 * time.Hour},
		{name: "the feature is off, so no check needs the evidence", disabled: true, ttl: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &DB{config: Config{RuntimeReindexDisabled: tt.disabled, CompletedTaskTTL: tt.ttl}}

			err := db.Backupable(context.Background(), nil)

			if !tt.wantRefused {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, entitiesbackup.ErrBackupBlockedByInFlightReindex)
			assert.Contains(t, err.Error(), "DISTRIBUTED_TASKS_COMPLETED_TASK_TTL_HOURS")
			assert.Contains(t, err.Error(), "RUNTIME_REINDEX_ENABLED")
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
		require.True(t, verdict.Overlapped)
		require.Equal(t, "task-a", verdict.TaskID)
	}
}

// TestReindexOverlapRefusalWording pins the three things every refusal has
// to say, whichever answer produced it: what the check found, that this
// backup id cannot be reused and what has to change before a new one will
// work, and that the bytes already uploaded are still sitting there. It
// also pins that the two answers stay distinguishable all the way out — a
// caller matching the observed sentinel on an undetermined answer would
// report a backup nobody could judge as one a migration is known to have
// torn.
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
			verdict:      ReindexOverlapVerdict{Overlapped: true, Collection: "Movies", TaskID: "t1"},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlappedBackup,
			notSentinel:  entitiesbackup.ErrReindexOverlapUndetermined,
			wantFinding:  `collection "Movies" was migrated while this backup was being captured`,
			wantRemedy:   "GET /v1/schema/Movies/indexes",
		},
		{
			// The caller's spelling, not the task's: the task's would
			// disclose a name this backup never captured.
			name:         "an overlap the caller spelled differently",
			verdict:      ReindexOverlapVerdict{Overlapped: true, Collection: "Movies"},
			classes:      []string{"movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlappedBackup,
			wantFinding:  `collection "movies" was migrated`,
			wantRemedy:   "GET /v1/schema/movies/indexes",
			notContains:  []string{`"Movies"`},
		},
		{
			name:         "an overlap on a collection this backup never captured",
			verdict:      ReindexOverlapVerdict{Overlapped: true, Collection: "Secret"},
			classes:      []string{"Movies"},
			wantSentinel: entitiesbackup.ErrReindexOverlappedBackup,
			wantFinding:  "a collection this backup captured was migrated",
			wantRemedy:   "wait for that migration to finish",
			notContains:  []string{"Secret", "/v1/schema"},
		},
		{
			name: "a backup that outlived the retention window",
			verdict: ReindexOverlapVerdict{
				Undetermined: true,
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
			name: "a migration record too incomplete to judge",
			verdict: ReindexOverlapVerdict{
				Undetermined: true,
				Detail:       "a cancelled migration recorded no units, so nothing says whether it wrote",
				Remedy:       ReindexOverlapIncompleteRecordRemedy,
				TaskID:       "t1",
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
		assert.NotContains(t, err.Error(), "node-7")
		assert.Equal(t, 1, built.overlap, "the check runs once per commit")

		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, "t1", hook.AllEntries()[0].Data["task_id"])
		assert.Equal(t, "overlap_observed", hook.AllEntries()[0].Data["reason"])
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

	t.Run("an unwired check passes and reports", func(t *testing.T) {
		logger, hook := logrustest.NewNullLogger()
		db := &DB{logger: logger}
		overlapCheckWarnBudget = reindexGateWarnBudget{}

		require.NoError(t, db.RefuseIfReindexOverlapped(context.Background(), []string{"Movies"}, captureStart))
		require.Len(t, hook.AllEntries(), 1)
		assert.Equal(t, "commit-time overlap", hook.AllEntries()[0].Data["gate"])
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
				return ReindexOverlapVerdict{Undetermined: true, Detail: "the cluster task manager could not be listed"}
			}
		})

		err := db.RefuseIfReindexOverlapped(ctx, []string{"Movies"}, captureStart)

		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, entitiesbackup.ErrReindexOverlapUndetermined,
			"the operator stopped this backup; it must publish as cancelled, not failed")
		require.Empty(t, hook.AllEntries())
	})

	t.Run("an expired deadline is still an unanswerable check", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), captureStart)
		defer cancel()
		logger, _ := logrustest.NewNullLogger()
		db := &DB{logger: logger}
		db.SetReindexOverlapLookup(func(context.Context) ReindexOverlapLookup {
			return func([]string, time.Time) ReindexOverlapVerdict {
				return ReindexOverlapVerdict{Undetermined: true, Detail: "d", Remedy: "r"}
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

// TestAdmissionAndCommitCheckDisagree walks the payload shapes the two
// backup checks decide from: the shard gate that admits a backup, and the
// commit-time check that judges it after the capture. Where they disagree
// the backup is admitted and then fails after the whole upload, so each
// disagreement is written down here rather than left to be found that way.
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
			// backup. Deliberate current behavior, filed as
			// weaviate/0-weaviate-issues#573; red the day that is fixed.
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
				func() time.Time { return commitTime })([]string{"Movies"}, captureStart).Overlapped

			assert.Equal(t, tt.wantAdmissionRefuses, admissionRefuses, "admission gate")
			assert.Equal(t, tt.wantCommitRefuses, commitRefuses, "commit-time check")
		})
	}
}

// TestReindexOverlapScopingReadsTheCollectionFieldAlone pins how the check
// scopes each payload shape. A payload naming no collection scopes to the
// whole cluster, including one truncated mid-name: the name is there in the
// bytes and must stay unread.
func TestReindexOverlapScopingReadsTheCollectionFieldAlone(t *testing.T) {
	payloads := []struct {
		name           string
		payload        string
		wantCollection string
	}{
		{
			name:    "collection and shards both decode",
			payload: `{"collection":"Movies","unitToShard":{"u1":"s1"}}`, wantCollection: "Movies",
		},
		{
			name:    "a field this build does not know",
			payload: `{"collection":"Movies","futureField":{"a":1}}`, wantCollection: "Movies",
		},
		{
			name:    "the shard map retyped by a newer node",
			payload: `{"collection":"Movies","unitToShard":"s1"}`, wantCollection: "Movies",
		},
		{
			name:    "the collection field renamed by a newer node",
			payload: `{"class":"Movies","unitToShard":{"u1":"s1"}}`,
		},
		{name: "not json at all", payload: `not json`},
		{name: "an empty collection", payload: `{"collection":"","unitToShard":{"u1":"s1"}}`},
		{
			// The name is right there in the bytes and must stay unread: a
			// truncated payload gives no grounds to leave any other
			// collection open to a backup.
			name:    "truncated with the name still in the bytes",
			payload: `{"collection":"Movies","unitToShard":{"u1":"sha`,
		},
	}

	for _, p := range payloads {
		t.Run(p.name, func(t *testing.T) {
			collection, named := ExtractReindexTaskCollection([]byte(p.payload))

			require.Equal(t, p.wantCollection, collection)
			require.Equal(t, p.wantCollection != "", named,
				"cluster-wide is exactly the payload that names no collection")
		})
	}
}

// TestReindexOverlapRefusalNamesOnlyACapturedClass pins that the published
// refusal echoes the caller's own spelling of a class the caller asked
// about. Echoing the task's instead would disclose a collection this
// backup never captured whenever a task is attributed to one.
func TestReindexOverlapRefusalNamesOnlyACapturedClass(t *testing.T) {
	t.Run("the captured spelling wins over the task's", func(t *testing.T) {
		err := reindexOverlapRefusal(
			ReindexOverlapVerdict{Overlapped: true, Collection: "Movies"}, []string{"movies"})

		assert.Contains(t, err.Error(), `collection "movies"`)
		assert.NotContains(t, err.Error(), `"Movies"`)
	})

	t.Run("a name this backup did not capture is not named", func(t *testing.T) {
		err := reindexOverlapRefusal(
			ReindexOverlapVerdict{Overlapped: true, Collection: "Secret"}, []string{"Movies"})

		assert.Contains(t, err.Error(), "a collection this backup captured")
		assert.NotContains(t, err.Error(), "Secret")
	})
}
