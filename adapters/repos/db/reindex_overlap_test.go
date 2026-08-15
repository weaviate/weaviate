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
// give. A backup that outlived the window in which finished tasks stay
// listed cannot be cleared, because the task that would have failed it
// may already have been dropped.
func TestReindexOverlapRetentionWindow(t *testing.T) {
	const ttl = time.Hour
	lookup := func(age time.Duration) ReindexOverlapVerdict {
		return NewReindexOverlapLookup(nil, ttl, noLocalWorker,
			func() time.Time { return captureStart.Add(age) })([]string{"Movies"}, captureStart)
	}

	assert.True(t, lookup(ttl).Undetermined, "at the window it can no longer be cleared")
	assert.True(t, lookup(ttl+time.Minute).Undetermined)
	assert.False(t, lookup(ttl-time.Minute).Undetermined)

	verdict := lookup(ttl)
	assert.Contains(t, verdict.Detail, "longer than the")

	assert.False(t, verdict.Overlapped, "an unanswerable check observed no overlap")
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

// TestReindexOverlapRefusalWording pins that the two answers stay
// distinguishable all the way out. A caller that matched the observed
// sentinel on an undetermined answer would report a backup nobody could
// judge as one a migration is known to have torn.
func TestReindexOverlapRefusalWording(t *testing.T) {
	t.Run("observed", func(t *testing.T) {
		err := reindexOverlapRefusal(ReindexOverlapVerdict{Overlapped: true, Collection: "Movies"})

		require.ErrorIs(t, err, entitiesbackup.ErrReindexOverlappedBackup)
		require.NotErrorIs(t, err, entitiesbackup.ErrReindexOverlapUndetermined)
		assert.Contains(t, err.Error(), `collection "Movies"`)
		assert.Contains(t, err.Error(), "was migrated while this backup was being captured")
	})

	t.Run("undetermined", func(t *testing.T) {
		err := reindexOverlapRefusal(ReindexOverlapVerdict{
			Undetermined: true,
			Detail:       "this backup ran for 2h0m0s, longer than the 1h0m0s a finished migration stays listed",
		})

		require.ErrorIs(t, err, entitiesbackup.ErrReindexOverlapUndetermined)
		require.NotErrorIs(t, err, entitiesbackup.ErrReindexOverlappedBackup,
			"the check observed no overlap, so it must not claim one")
		assert.NotContains(t, err.Error(), "overlapped this backup")
		assert.NotContains(t, err.Error(), "in flight",
			"nothing is in flight on this path; the task may already be gone")
		assert.Contains(t, err.Error(), "longer than the")
	})

	t.Run("observed but unattributable", func(t *testing.T) {
		err := reindexOverlapRefusal(ReindexOverlapVerdict{Overlapped: true})

		require.ErrorIs(t, err, entitiesbackup.ErrReindexOverlappedBackup)
		assert.Contains(t, err.Error(), "a collection this backup captured")
	})
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

// TestGateAndCommitCheckAgree pins that the restore gate and the commit-time
// check read the same task payloads the same way, so the two never disagree
// about whether a task touches a collection.
func TestGateAndCommitCheckAgree(t *testing.T) {
	tests := []struct {
		name        string
		payload     string
		wantRefused bool
	}{
		{
			name:        "a task naming the collection and its shards",
			payload:     `{"collection":"Movies","unitToShard":{"u1":"shard-1"}}`,
			wantRefused: true,
		},
		{
			name:        "a task whose shard set did not decode",
			payload:     `{"collection":"Movies","unitToShard":"shard-1"}`,
			wantRefused: true,
		},
		{
			name:        "a task that cannot be attributed at all",
			payload:     `not json`,
			wantRefused: true,
		},
		{
			name:        "a task on another collection",
			payload:     `{"collection":"Shows","unitToShard":{"u1":"shard-1"}}`,
			wantRefused: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := reindexTask("t1", distributedtask.TaskStatusStarted, tt.payload)

			// The restore gate reads the same task list through the same decoder.
			decoded := DecodeReindexTaskPayload(task.Payload)
			_, live := NewAnyReindexActivityLookup([]*distributedtask.Task{task})([]string{"Movies"})
			gateRefuses := decoded.Scope == ReindexPayloadScopeCluster || live

			commitCheck := NewReindexOverlapLookup([]*distributedtask.Task{task},
				24*time.Hour, noLocalWorker, func() time.Time { return commitTime })
			commitCheckRefuses := commitCheck([]string{"Movies"}, captureStart).Overlapped

			assert.Equal(t, tt.wantRefused, gateRefuses, "restore gate")
			assert.Equal(t, tt.wantRefused, commitCheckRefuses, "commit-time check")
		})
	}
}
