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
	"slices"
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

// IsLiveReindexTaskStatus must not re-enumerate the DTM statuses: when it and
// TaskStatus.IsActive each owned a switch they disagreed on the unrecognized
// case, so a task from a newer node was live to the backup gate but invisible
// to conflict detection.
func TestIsLiveReindexTaskStatus_UnrecognizedStatusIsLive(t *testing.T) {
	assert.True(t, IsLiveReindexTaskStatus(unknownFutureStatus),
		"a status this build cannot prove is terminal must read as live")
	assert.False(t, IsLiveReindexTaskStatus(distributedtask.TaskStatusFinished))
}

// countWarnings reports how many WARN lines carry substr.
func countWarnings(hook *logrustest.Hook, substr string) int {
	n := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, substr) {
			n++
		}
	}
	return n
}

// Both gates share the startup-window default: allow, and warn once per hour
// per node rather than once per request.
func TestReindexGatesWarnOnceWhileUnwired(t *testing.T) {
	tests := []struct {
		name string
		warn string
		call func(*DB) error
	}{
		{
			name: "restore gate",
			warn: "AnyReindexActivityLookup not yet installed",
			call: func(db *DB) error {
				return db.RefuseIfAnyReindexInFlight(context.Background(), nil)
			},
		},
		{
			name: "commit-time overlap backstop",
			warn: "lookup not yet installed",
			call: func(db *DB) error {
				return db.RefuseIfReindexOverlapped(context.Background(), []string{"Movies"}, time.Now())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			db := &DB{logger: logger}

			require.NoError(t, tc.call(db))
			require.NoError(t, tc.call(db))

			assert.Equal(t, 1, countWarnings(hook, tc.warn),
				"the unwired WARN is budgeted once per hour per node, not once per request")
		})
	}
}

// The restore refusals both point the operator at a cancel, so both have to
// render a route they can actually call. The blocking task's own collection is
// used whenever the caller's request listed it; only when it cannot be named
// does the class fall back to a placeholder.
func TestRefuseIfAnyReindexInFlight(t *testing.T) {
	// A real RAFT failure names nodes; the redaction receipt only bites if the cause does too.
	lookupErr := errors.New("can not resolve nodes [weaviate-2,weaviate-1]")

	tests := []struct {
		name        string
		collections []string
		// live installs a blocking task, holding the collection its payload
		// names — empty for a task whose payload names none.
		live        bool
		holding     string
		cleanupHold bool
		lookupErr   error

		wantRefusal  bool
		wantContains string
		// wantURL must appear in the refusal, so the operator has a route to call.
		wantURL string
		// wantAbsent guards against naming a collection the caller never asked
		// about, and against leaking a RAFT cause into the response body.
		wantAbsent []string
		wantCause  error
	}{
		{
			name:        "no live task admits the restore",
			collections: []string{"Movies"},
		},
		{
			// The lookup cannot say whether the hold is a teardown or a
			// submission sweep, so the text must not claim either one.
			name:         "a node-local teardown hold refuses the restore",
			collections:  []string{"Movies"},
			cleanupHold:  true,
			wantRefusal:  true,
			wantContains: "holding temporary index files on this node",
			wantURL:      "PUT /v1/schema/Movies/indexes/<prop>",
			wantAbsent:   []string{"a cancelled migration is still removing"},
		},
		{
			// No class list yet, so the refusal cannot name the blocking
			// collection: the caller never asked about it.
			name:         "a live task refuses the restore, with no class list yet",
			live:         true,
			holding:      "Movies",
			wantRefusal:  true,
			wantContains: "retry after the migration finishes",
			wantURL:      "PUT /v1/schema/<class>/indexes/<prop>",
			wantAbsent:   []string{"Movies"},
		},
		{
			// The caller listed it and was authorized against that list, so
			// naming it back discloses nothing new.
			name:         "several collections, the blocker is one of them",
			collections:  []string{"Movies", "Actors"},
			live:         true,
			holding:      "Actors",
			wantRefusal:  true,
			wantContains: "Actors",
			wantURL:      "PUT /v1/schema/Actors/indexes/<prop>",
		},
		{
			name:         "lookup failure fails closed",
			collections:  []string{"Movies"},
			lookupErr:    lookupErr,
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
			db.SetAnyReindexActivityLookup(func(context.Context, []string) (*ReindexActivityHold, error) {
				switch {
				case tc.lookupErr != nil:
					return nil, tc.lookupErr
				case tc.live:
					return &ReindexActivityHold{Collection: tc.holding, TaskID: "task-1"}, nil
				default:
					return nil, nil
				}
			})
			db.SetAnyCleanupInProgressLookup(func([]string) bool { return tc.cleanupHold })

			err := db.RefuseIfAnyReindexInFlight(context.Background(), tc.collections)
			if !tc.wantRefusal {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.ErrorIs(t, err, entitiesbackup.ErrReindexInFlight,
				"the refusal must carry the cluster-wide sentinel")
			assert.ErrorContains(t, err, tc.wantContains)
			if tc.wantURL != "" {
				assert.Contains(t, err.Error(), tc.wantURL)
			}
			for _, absent := range tc.wantAbsent {
				assert.NotContainsf(t, err.Error(), absent,
					"the refusal must not disclose or claim %q", absent)
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
			if tc.live {
				var named bool
				for _, e := range hook.AllEntries() {
					named = named || (e.Level == logrus.WarnLevel && e.Data["task_id"] == "task-1")
				}
				assert.True(t, named,
					"the operator's only handle on the blocking task is a WARN naming its id")
			}

			// The per-shard backup vocabulary doesn't apply to the cluster-wide gate.
			assert.NotContains(t, err.Error(), "backup")
			assert.NotContains(t, err.Error(), "restore")
			assert.NotContains(t, err.Error(), "this shard")
			// DTM refuses a cancel only for a task that already reached a
			// terminal status, and such a task holds neither gate. Sending the
			// operator to a cluster restart instead costs them the cluster.
			assert.NotContains(t, err.Error(), "RUNTIME_REINDEX_ENABLED")
			assert.NotContains(t, err.Error(), "can only be waited out")
		})
	}
}

// TestRefuseIfAnyReindexInFlight_PropagatesContext pins that the caller's context reaches the lookup.
func TestRefuseIfAnyReindexInFlight_PropagatesContext(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db.SetAnyReindexActivityLookup(func(ctx context.Context, _ []string) (*ReindexActivityHold, error) {
		return nil, ctx.Err()
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

func overlapTaskWithUnits(collection string, status distributedtask.TaskStatus, finishedAt time.Time,
	units map[string]*distributedtask.Unit,
) *distributedtask.Task {
	task := overlapTask(collection, status, finishedAt)
	task.Units = units
	return task
}

// The check is overlap, not liveness: a task that ran entirely inside the
// backup window must still refuse it. Cancelled tasks are decided by unit
// state instead, since a cancelled one may already have rebuilt buckets.
func TestReindexOverlapLookup(t *testing.T) {
	backupStart := time.Now().Add(-2 * time.Minute)
	insideWindow := backupStart.Add(time.Minute)
	beforeWindow := backupStart.Add(-time.Minute)
	const ttl = time.Hour

	tests := []struct {
		name       string
		task       *distributedtask.Task
		ttl        time.Duration
		wantRefuse bool
		wantMsg    string
		// wantUndetermined separates the refusals that observed an overlap from
		// the ones that only failed to rule one out. The backup publishes
		// different text for each, and this sentinel is how it tells them apart.
		wantUndetermined bool
		why              string
	}{
		{
			name: "task finished before the backup started",
			task: overlapTask("Movies", distributedtask.TaskStatusFinished, beforeWindow),
		},
		{
			// Ran and finished entirely inside the window.
			name:       "task finished after the backup started",
			task:       overlapTask("Movies", distributedtask.TaskStatusFinished, insideWindow),
			wantRefuse: true,
			wantMsg:    "was migrated while this backup was being captured",
		},
		{
			name:       "task finished exactly when the backup started",
			task:       overlapTask("Movies", distributedtask.TaskStatusFinished, backupStart),
			wantRefuse: true,
		},
		{
			name:       "task still running",
			task:       overlapTask("Movies", distributedtask.TaskStatusStarted, time.Time{}),
			wantRefuse: true,
		},
		{
			name:       "terminal task with no finish time is treated as overlapping",
			task:       overlapTask("Movies", distributedtask.TaskStatusFailed, time.Time{}),
			wantRefuse: true,
		},
		{
			name: "task on a collection this backup does not cover",
			task: overlapTask("Actors", distributedtask.TaskStatusFinished, insideWindow),
		},
		{
			name:             "backup outlived the retention window",
			ttl:              time.Minute,
			wantRefuse:       true,
			wantMsg:          "longer than the",
			wantUndetermined: true,
		},
		{
			name: "cancelled after a unit was claimed",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, insideWindow,
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress, Progress: 0.4},
				}),
			wantRefuse: true,
			why:        "a partly-run cancelled migration spans the backup and must fail it",
		},
		{
			name: "cancelled with one unit of several left PENDING",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, insideWindow,
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
					"u2": {ID: "u2", Status: distributedtask.UnitStatusCompleted, Progress: 1},
				}),
			wantRefuse: true,
			why:        "the question is whether ANY worker ran, so one claimed unit decides it for the whole task",
		},
		{
			name: "cancelled with no unit ever out of PENDING",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, insideWindow,
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
				}),
			why: "the post-commit rollback cancels before any worker claims a unit; failing backups on that would make the rollback worse than the race it repairs",
		},
		{
			name:       "cancelled with no unit list at all",
			task:       overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, insideWindow, nil),
			wantRefuse: true,
			why:        "every real task carries its units from submission, so an empty list is unknown rather than untouched",
		},
		{
			name: "cancelled with a nil entry beside a pending one",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, insideWindow,
				map[string]*distributedtask.Unit{
					"u1": nil,
					"u2": {ID: "u2", Status: distributedtask.UnitStatusPending},
				}),
			why: "a nil entry says nothing about a worker, and reading it must not take the gate down with a panic",
		},
		{
			// Units that left PENDING on purpose: it is the finish time that
			// waives this row, and a populated unit list is what stops the
			// cancelled-and-untouched rule from waiving it for the wrong reason.
			name: "cancelled after writing, but before this backup started",
			task: overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, beforeWindow,
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
				}),
			why: "it wrote, but it was over before the capture began",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			taskTTL := ttl
			if tc.ttl != 0 {
				taskTTL = tc.ttl
			}
			var tasks []*distributedtask.Task
			if tc.task != nil {
				tasks = []*distributedtask.Task{tc.task}
			}
			lookup := NewReindexOverlapLookup(func(context.Context) (map[string][]*distributedtask.Task, error) {
				return map[string][]*distributedtask.Task{ReindexNamespace: tasks}, nil
			}, taskTTL)

			err := lookup(context.Background(), []string{"Movies"}, backupStart)
			if !tc.wantRefuse {
				require.NoError(t, err, tc.why)
				return
			}
			require.Error(t, err, tc.why)
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

// poisonTask defeats the full payload decoder while leaving the collection
// readable, which is what a rolling upgrade that retypes a payload field
// produces.
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
			name: "collection field renamed by a newer node, live",
			task: func(t *testing.T) *distributedtask.Task {
				return renamedFieldTask(t, distributedtask.TaskStatusStarted)
			},
			backingUp: []string{"Movies"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: a task payload is unreadable; " +
				"retry once every node runs the same server version, and report this to Weaviate if it persists",
			why: "a renamed field decodes without error, so keying recovery on decodeErr lets a LIVE task " +
				"read as no-overlap and the backup publish over it; and nothing identifies what it touches, " +
				"so no collection can be declared clean",
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
			name: "poison on the collection being backed up, matched case-insensitively",
			task: func(t *testing.T) *distributedtask.Task {
				return poisonTask(t, "movies", distributedtask.TaskStatusStarted, time.Time{})
			},
			leaked:  []string{"not json", "invalid character"},
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
			// span it. The cluster-wide restore lookup reads an empty list the
			// other way round, as "every collection".
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

// Both inputs are scoped by collection, and the gate has to hand its class list
// to each. A migration can run for days, and a wedged teardown holds its gate
// until the cap — so a blind question refuses restores of every OTHER
// collection for that whole time.
func TestRefuseIfAnyReindexInFlightScopesBothInputsByCollection(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	db := &DB{logger: logger}
	// "Stuck" is wedged in teardown on this node; "Logs" is migrating in the
	// cluster. An empty class list asks about every collection, so both answer.
	var asked [][]string
	db.SetAnyCleanupInProgressLookup(func(collections []string) bool {
		return len(collections) == 0 || slices.Contains(collections, "Stuck")
	})
	db.SetAnyReindexActivityLookup(func(_ context.Context, collections []string) (*ReindexActivityHold, error) {
		asked = append(asked, collections)
		if len(collections) == 0 || slices.Contains(collections, "Logs") {
			return &ReindexActivityHold{Collection: "Logs", TaskID: "t1"}, nil
		}
		return nil, nil
	})

	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Stuck"}),
		"the collection whose teardown is wedged must be refused")
	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Docs", "Logs"}),
		"a restore that includes the migrating collection must be refused")
	require.NoError(t, db.RefuseIfAnyReindexInFlight(context.Background(), []string{"Unrelated"}),
		"an unrelated collection must be refused by neither")
	require.Error(t, db.RefuseIfAnyReindexInFlight(context.Background(), nil),
		"with no class list yet both questions stay blind, so it still refuses")

	require.Equal(t, [][]string{{"Docs", "Logs"}, {"Unrelated"}}, asked,
		"the gate must forward the restore's class list to the cluster unchanged, and "+
			"must not ask it at all once the node-local teardown has already refused")
}
