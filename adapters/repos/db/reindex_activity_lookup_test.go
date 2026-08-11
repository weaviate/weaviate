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
	// A real RAFT failure names nodes; the redaction receipt only bites if the cause does too.
	lookupErr := errors.New("can not resolve nodes [weaviate-2,weaviate-1]")

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
			lookup: func(context.Context, []string) (bool, error) { return false, nil },
		},
		{
			// The lookup cannot say whether the hold is a teardown or a
			// submission sweep, so the text must not claim either one.
			name:         "a node-local reindex hold refuses the restore",
			lookup:       func(context.Context, []string) (bool, error) { return false, nil },
			cleanup:      func([]string) bool { return true },
			wantRefusal:  true,
			wantContains: "holding temporary index files on this node",
			wantAbsent:   []string{"a cancelled migration is still removing"},
		},
		{
			name:         "live task refuses the restore",
			lookup:       func(context.Context, []string) (bool, error) { return true, nil },
			wantRefusal:  true,
			wantContains: "retry after the migration finishes",
		},
		{
			name:         "lookup failure fails closed",
			lookup:       func(context.Context, []string) (bool, error) { return false, lookupErr },
			wantRefusal:  true,
			wantContains: "the cluster task manager could not be queried",
			wantAbsent:   []string{"weaviate-1", "weaviate-2", "can not resolve nodes"},
			wantCause:    lookupErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
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
				// The body hides the cause by design; the log is the operator's only channel.
				var logged bool
				for _, e := range hook.AllEntries() {
					logged = logged || strings.Contains(e.Message, tc.wantCause.Error())
				}
				assert.True(t, logged, "the full lookup error must reach the node's own log")
			}

			// The per-shard backup vocabulary doesn't apply to the cluster-wide gate.
			assert.NotContains(t, err.Error(), "backup")
			assert.NotContains(t, err.Error(), "restore")
			assert.NotContains(t, err.Error(), "this shard")
		})
	}
}

// TestRefuseIfAnyReindexInFlight_PropagatesContext pins that the caller's context reaches the lookup.
func TestRefuseIfAnyReindexInFlight_PropagatesContext(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db.SetAnyReindexActivityLookup(func(ctx context.Context, _ []string) (bool, error) {
		return false, ctx.Err()
	})

	err := db.RefuseIfAnyReindexInFlight(ctx, nil)
	require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
	assert.ErrorIs(t, err, context.Canceled)
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
		// wantUndetermined separates the refusals that observed an overlap from
		// the ones that only failed to rule one out. The backup publishes
		// different text for each, and this sentinel is how it tells them apart.
		wantUndetermined bool
	}{
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
			name:             "backup outlived the retention window",
			tasks:            nil,
			ttl:              time.Minute,
			since:            backupStart,
			wantRefuse:       true,
			wantMsg:          "longer than the",
			wantUndetermined: true,
		},
		{
			// An empty list from an unreachable task manager looks exactly like
			// an empty list from a quiet cluster, so it must not read as one.
			name:             "the task manager could not be listed",
			listErr:          errors.New("no leader: node weaviate-2 unreachable"),
			since:            backupStart,
			wantRefuse:       true,
			wantMsg:          "the cluster task manager could not be queried",
			wantUndetermined: true,
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
			assert.Equal(t, tc.wantUndetermined,
				errors.Is(err, entitiesbackup.ErrReindexOverlapUndetermined),
				"a refusal that never observed an overlap must say so, and one that did must not")
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
			name: "migration that failed inside the window",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusFailed, insideWindow,
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusFailed},
				}),
			wantRefuse: true,
			why:        "a failed migration may have written before it failed",
		},
		{
			// Units that left PENDING on purpose: it is the finish time that
			// waives this row, and a populated unit list is what stops the
			// cancelled-and-untouched rule from waiving it for the wrong reason.
			name: "cancelled after writing, but before this backup started",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled,
				backupStart.Add(-time.Minute),
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
				}),
			wantRefuse: false,
			why:        "it wrote, but it was over before the capture began",
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
			name: "one unit of several left PENDING",
			units: map[string]*distributedtask.Unit{
				"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
				"u2": {ID: "u2", Status: distributedtask.UnitStatusCompleted, Progress: 1},
			},
			wantRefuse: true,
			why:        "the question is whether ANY worker ran, so one claimed unit decides it for the whole task",
		},
		{
			name: "no unit ever left PENDING",
			units: map[string]*distributedtask.Unit{
				"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
			},
			wantRefuse: false,
			why:        "the post-commit rollback cancels before any worker claims a unit; failing backups on that would make the rollback worse than the race it repairs",
		},
		{
			name:       "no unit list at all",
			units:      nil,
			wantRefuse: true,
			why:        "every real task carries its units from submission, so an empty list is unknown rather than untouched",
		},
		{
			name: "a nil entry beside a pending one",
			units: map[string]*distributedtask.Unit{
				"u1": nil,
				"u2": {ID: "u2", Status: distributedtask.UnitStatusPending},
			},
			wantRefuse: false,
			why:        "a nil entry says nothing about a worker, and reading it must not take the gate down with a panic",
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

// renamedFieldTask is the half of the rolling-upgrade shape that decodes
// CLEANLY. A newer node that renames the payload's collection field produces
// JSON this build unmarshals without error into an empty Collection, because
// Go ignores unknown fields. Nothing about it is "unreadable" to the decoder,
// yet nothing in it names what the task touches.
func renamedFieldTask(t *testing.T, status distributedtask.TaskStatus) *distributedtask.Task {
	t.Helper()
	raw := []byte(`{"collektion":"Movies","propertyName":"title","migrationType":"enable-rangeable"}`)

	var probe ReindexTaskPayload
	require.NoError(t, json.Unmarshal(raw, &probe),
		"this fixture is only meaningful while the payload decodes without error")
	require.Empty(t, probe.Collection,
		"this fixture is only meaningful while the decoded collection is empty")

	return &distributedtask.Task{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "renamed", Version: 1},
		Namespace:      ReindexNamespace,
		Status:         status,
		Payload:        raw,
		Units: map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress},
		},
	}
}

// Pins: the API-facing refusal message must not leak RAFT/decoding
// internals, and an unreadable payload must scope the refusal to the
// collection it names, not fail every backup. wantMsg is exact so a false
// "was migrated" claim can't slip past an assertion that only checks err != nil.
func TestReindexOverlapLookupScopesAndRedactsUnreadableInputs(t *testing.T) {
	raftErr := errors.New("can not resolve nodes [weaviate-2,weaviate-1]")
	backupStart := time.Now().Add(-2 * time.Minute)

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
			wantMsg: "cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried; " +
				"retry once it is reachable",
			why: "an unanswerable question is not an all-clear",
		},
		{
			name: "collection unrecoverable, live",
			task: func(*testing.T) *distributedtask.Task {
				return unreadableTask(distributedtask.TaskStatusStarted, time.Time{})
			},
			leaked: []string{"not json", "invalid character"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: a task payload is unreadable; " +
				"retry once every node runs the same server version, and report this to Weaviate if it persists",
			why: "nothing identifies what this task touches, so no collection can be declared clean",
		},
		{
			name: "collection field renamed by a newer node, live",
			task: func(t *testing.T) *distributedtask.Task {
				return renamedFieldTask(t, distributedtask.TaskStatusStarted)
			},
			backingUp: []string{"Movies"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: a task payload is unreadable; " +
				"retry once every node runs the same server version, and report this to Weaviate if it persists",
			why: "a renamed field decodes without error, so keying recovery on decodeErr lets a LIVE task " +
				"read as no-overlap and the backup publish over it",
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
			wantMsg: unreadablePayloadMsg("Movies"),
			why:     "no teardown ran for a payload nothing can read, so the refusal outlives the task and stays on the one collection it names",
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
			// Every refusal in this table comes from an input the check could
			// not read, so none of them observed a migration. Saying one ran
			// sends the operator after a task that may never have existed.
			assert.ErrorIs(t, err, entitiesbackup.ErrReindexOverlapUndetermined,
				"a refusal built on an unreadable input must not claim an overlap was seen")
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

// Both restore refusals point the operator at a cancel, so both have to render
// a route they can actually call. Only a single-collection restore knows which
// class that is.
func TestRefuseIfAnyReindexInFlightRendersTheRemediationURL(t *testing.T) {
	tests := []struct {
		name        string
		collections []string
		cleanupHold bool
		wantURL     string
	}{
		{
			name:        "live task, one collection",
			collections: []string{"Movies"},
			wantURL:     "PUT /v1/schema/Movies/indexes/<prop>",
		},
		{
			name:        "teardown hold, one collection",
			collections: []string{"Movies"},
			cleanupHold: true,
			wantURL:     "PUT /v1/schema/Movies/indexes/<prop>",
		},
		{
			name:        "live task, no class list yet",
			collections: nil,
			wantURL:     "PUT /v1/schema/<class>/indexes/<prop>",
		},
		{
			name:        "live task, several collections",
			collections: []string{"Movies", "Actors"},
			wantURL:     "PUT /v1/schema/<class>/indexes/<prop>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := logrustest.NewNullLogger()
			db := &DB{logger: logger}
			db.SetAnyReindexActivityLookup(func(context.Context, []string) (bool, error) {
				return !tc.cleanupHold, nil
			})
			db.SetAnyCleanupInProgressLookup(func([]string) bool { return tc.cleanupHold })

			err := db.RefuseIfAnyReindexInFlight(context.Background(), tc.collections)
			require.Error(t, err)
			require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight)
			assert.Contains(t, err.Error(), tc.wantURL)
			// DTM refuses a cancel only for a task that already reached a
			// terminal status, and such a task holds neither gate. Sending the
			// operator to a cluster restart instead costs them the cluster.
			assert.NotContains(t, err.Error(), "RUNTIME_REINDEX_ENABLED")
			assert.NotContains(t, err.Error(), "can only be waited out")
		})
	}
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
			name:        "several collections, one of them migrated",
			collections: []string{"Movies", "Actors", "Reviews"},
			wantRefuse:  true,
		},
		{
			name:        "several collections, none of them migrated",
			collections: []string{"Movies", "Reviews"},
			wantRefuse:  false,
		},
		{
			// A backup naming no class covers nothing, so no migration can
			// span it.
			name:        "no collections at all",
			collections: nil,
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

// A worker that will not exit holds its collection's cleanup gate until the cap.
// If the restore gate asks that question blind, one stuck collection refuses
// restores of every other collection for the whole hold.
func TestRefuseIfAnyReindexInFlightScopesTheCleanupCheckByCollection(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	db.SetAnyReindexActivityLookup(func(context.Context, []string) (bool, error) { return false, nil })
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

// A migration can run for days. The gate has to hand its class list to the
// cluster-wide lookup too, or one collection's migration refuses every restore
// in the cluster for that whole time.
func TestRefuseIfAnyReindexInFlightScopesTheClusterCheckByCollection(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	var asked [][]string
	db.SetAnyReindexActivityLookup(func(_ context.Context, collections []string) (bool, error) {
		asked = append(asked, collections)
		for _, c := range collections {
			if c == "Logs" {
				return true, nil
			}
		}
		return len(collections) == 0, nil
	})

	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Docs"}),
		"a restore of a collection no migration touches must be admitted")
	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Docs", "Logs"}),
		"a restore that includes the migrating collection must be refused")
	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), nil),
		"with no class list yet the question stays cluster-wide")
	require.Equal(t, [][]string{{"Docs"}, {"Docs", "Logs"}, nil}, asked,
		"the gate must forward the restore's class list unchanged")
}
