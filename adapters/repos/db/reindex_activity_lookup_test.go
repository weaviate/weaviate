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

// overlapTask builds a reindex task as DTM would report it. finishedAt is the
// stamp the proposing node wrote; the overlap backstop never reads it, and
// TestReindexOverlapVerdictIsInvariantUnderClockSkew is what holds that true.
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

// overlapObservations is one run of the two-observation backstop: before is
// what the cluster reported when the backup started, after what it reported at
// commit time.
type overlapObservations struct {
	collections []string
	ttl         time.Duration
	// capture is how long the backup spends copying files between the two
	// observations. Only the retention-window row needs a non-zero one.
	capture   time.Duration
	before    []*distributedtask.Task
	after     []*distributedtask.Task
	beforeErr error
	afterErr  error
}

// run drives the observer and its commit-time check the way a backup does.
func (o overlapObservations) run(t *testing.T) error {
	t.Helper()

	collections := o.collections
	if collections == nil {
		collections = []string{"Movies"}
	}
	ttl := o.ttl
	if ttl == 0 {
		ttl = time.Hour
	}

	calls := 0
	observer := NewReindexOverlapObserver(func(context.Context) (map[string][]*distributedtask.Task, error) {
		calls++
		if calls == 1 {
			if o.beforeErr != nil {
				return nil, o.beforeErr
			}
			return map[string][]*distributedtask.Task{ReindexNamespace: o.before}, nil
		}
		if o.afterErr != nil {
			return nil, o.afterErr
		}
		return map[string][]*distributedtask.Task{ReindexNamespace: o.after}, nil
	}, ttl)

	check := observer(context.Background(), collections)
	time.Sleep(o.capture)
	return check(context.Background())
}

// The check is overlap, not liveness: a task that ran entirely inside the
// backup window must still refuse it, and nothing is left running by the time
// the question is asked.
func TestReindexOverlapObserver(t *testing.T) {
	finishedBefore := overlapTask("Movies", distributedtask.TaskStatusFinished, time.Now().Add(-time.Hour))
	failedBefore := overlapTask("Movies", distributedtask.TaskStatusFailed, time.Time{})
	running := overlapTask("Movies", distributedtask.TaskStatusStarted, time.Time{})

	tests := []struct {
		name       string
		obs        overlapObservations
		wantRefuse bool
		wantMsg    string
		why        string
	}{
		{
			name: "no tasks at all",
			obs:  overlapObservations{},
		},
		{
			name: "task was already finished when the backup started and has not moved",
			obs: overlapObservations{
				before: []*distributedtask.Task{finishedBefore},
				after:  []*distributedtask.Task{finishedBefore},
			},
			why: "the post-migration backup contract; an unconditional blackout here would be a usability regression",
		},
		{
			name: "task appeared while the backup was being captured",
			obs: overlapObservations{
				before: nil,
				after: []*distributedtask.Task{
					overlapTask("Movies", distributedtask.TaskStatusFinished, time.Now()),
				},
			},
			wantRefuse: true,
			wantMsg:    "was migrated while this backup was being captured",
			why:        "it ran and finished inside the window, so liveness at commit sees nothing",
		},
		{
			name: "task was running when the backup started and finished during it",
			obs: overlapObservations{
				before: []*distributedtask.Task{running},
				after: []*distributedtask.Task{
					overlapTask("Movies", distributedtask.TaskStatusFinished, time.Now()),
				},
			},
			wantRefuse: true,
			why:        "the capture began while it was rebuilding buckets",
		},
		{
			name: "task still running at commit",
			obs: overlapObservations{
				before: []*distributedtask.Task{running},
				after:  []*distributedtask.Task{running},
			},
			wantRefuse: true,
		},
		{
			name: "task started running after the backup started",
			obs: overlapObservations{
				before: nil,
				after:  []*distributedtask.Task{running},
			},
			wantRefuse: true,
		},
		{
			name: "task had already failed when the backup started and has not moved",
			obs: overlapObservations{
				before: []*distributedtask.Task{failedBefore},
				after:  []*distributedtask.Task{failedBefore},
			},
			why: "terminal before the capture began, so it cannot have written during it — and it is admitted " +
				"even though it carries no finish stamp at all, which the timestamp rule could not do",
		},
		{
			name: "task failed while the backup was being captured",
			obs: overlapObservations{
				before: nil,
				after:  []*distributedtask.Task{failedBefore},
			},
			wantRefuse: true,
			why:        "a failed migration may have written before it failed",
		},
		{
			name: "task on a collection this backup does not cover",
			obs: overlapObservations{
				collections: []string{"Movies"},
				before:      nil,
				after: []*distributedtask.Task{
					overlapTask("Actors", distributedtask.TaskStatusFinished, time.Now()),
				},
			},
		},
		{
			name: "collection match ignores case",
			obs: overlapObservations{
				collections: []string{"Movies"},
				before:      nil,
				after: []*distributedtask.Task{
					overlapTask("movies", distributedtask.TaskStatusFinished, time.Now()),
				},
			},
			wantRefuse: true,
		},
		{
			name: "terminal task present at backup start but gone at commit",
			obs: overlapObservations{
				before: []*distributedtask.Task{finishedBefore},
				after:  nil,
			},
			why: "it was terminal when the backup started, so its expiry from the task list is not evidence " +
				"of anything; refusing here would fail backups that merely outlived an old task's retention",
		},
		{
			name: "running task present at backup start but gone at commit",
			obs: overlapObservations{
				before: []*distributedtask.Task{running},
				after:  nil,
			},
			wantRefuse: true,
			why: "DTM also forgets a task when its collection is deleted, whatever its status, so vanishing " +
				"is not proof it did nothing — and it was demonstrably running during the capture",
		},
		{
			name: "a status this build does not recognize",
			obs: overlapObservations{
				before: []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatus("WARPING"), time.Time{})},
				after:  []*distributedtask.Task{overlapTask("Movies", distributedtask.TaskStatus("WARPING"), time.Time{})},
			},
			wantRefuse: true,
			why:        "it comes from a newer node, and guessing 'not live' admits a backup over a live migration",
		},
		{
			name: "backup outlived the retention window",
			obs: overlapObservations{
				// A capture that outlasts the window, so the check refuses
				// before reading a single task.
				ttl:     time.Millisecond,
				capture: 5 * time.Millisecond,
				before:  nil,
				after:   nil,
			},
			wantRefuse: true,
			wantMsg:    "longer than the",
			why: "past the retention window an unchanged list stops being evidence: a task could have " +
				"started, finished, and aged out entirely inside the backup",
		},
		{
			name: "task list unreadable when the backup started",
			obs: overlapObservations{
				beforeErr: errors.New("DTM unreachable"),
				after:     nil,
			},
			wantRefuse: true,
			wantMsg:    "cannot rule out a runtime-reindex",
			why:        "no baseline means no comparison, so nothing can be ruled out",
		},
		{
			name: "task list unreadable at commit",
			obs: overlapObservations{
				before:   nil,
				afterErr: errors.New("DTM unreachable"),
			},
			wantRefuse: true,
			wantMsg:    "cannot rule out a runtime-reindex",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.obs.run(t)
			if !tc.wantRefuse {
				require.NoError(t, err, tc.why)
				return
			}
			require.Error(t, err, tc.why)
			if tc.wantMsg != "" {
				assert.ErrorContains(t, err, tc.wantMsg)
			}
			// Every refusal this backstop produces, including the
			// retention-window one, has to be classifiable: a caller that
			// cannot match the sentinel treats it as an unrelated failure.
			assert.ErrorIs(t, err, entitiesbackup.ErrBackupSpannedReindex,
				"the refusal must carry the overlap sentinel")
			assert.NotContains(t, err.Error(), "in flight",
				"the migration has usually finished by now; do not send the operator after a live task")
		})
	}
}

// A task's timestamps are stamped by whichever node proposed it, so the
// mechanism this replaces compared two machines' clocks. The verdict now comes
// from RAFT-ordered state alone, which makes the proposer's offset irrelevant
// by construction — so the property to hold is invariance, not tolerance.
//
// Asserted that way on purpose: a tolerance table has a size, and a size can be
// derived from the very constant it is supposed to bite, leaving a table that
// stays green however wrong the constant is. Invariance has no size to get
// wrong, and the offsets below are absolute literals far beyond anything a real
// cluster drifts, so no allowance could satisfy them by accident.
func TestReindexOverlapVerdictIsInvariantUnderClockSkew(t *testing.T) {
	skews := []time.Duration{
		-876000 * time.Hour, // a century behind
		-24 * time.Hour,
		-31 * time.Second, // just outside the allowance the old mechanism used
		-29 * time.Second, // just inside it
		-time.Second,
		0,
		time.Second,
		29 * time.Second,
		31 * time.Second,
		24 * time.Hour,
		876000 * time.Hour, // a century ahead
	}

	scenarios := []struct {
		name string
		// stamped is where the proposer claims the migration finished.
		build      func(stamped time.Time) (before, after []*distributedtask.Task)
		wantRefuse bool
		why        string
	}{
		{
			name: "migration ran entirely inside the backup window",
			build: func(stamped time.Time) ([]*distributedtask.Task, []*distributedtask.Task) {
				return nil, []*distributedtask.Task{
					overlapTask("Movies", distributedtask.TaskStatusFinished, stamped),
				}
			},
			wantRefuse: true,
			why: "the original fail-open: a proposer running behind stamps a finish that predates the backup " +
				"start it really followed, and the torn backup is published as clean",
		},
		{
			name: "migration was live when the backup started and finished during it",
			build: func(stamped time.Time) ([]*distributedtask.Task, []*distributedtask.Task) {
				return []*distributedtask.Task{
						overlapTask("Movies", distributedtask.TaskStatusStarted, time.Time{}),
					}, []*distributedtask.Task{
						overlapTask("Movies", distributedtask.TaskStatusFinished, stamped),
					}
			},
			wantRefuse: true,
		},
		{
			name: "migration finished before the backup started",
			build: func(stamped time.Time) ([]*distributedtask.Task, []*distributedtask.Task) {
				done := overlapTask("Movies", distributedtask.TaskStatusFinished, stamped)
				return []*distributedtask.Task{done}, []*distributedtask.Task{done}
			},
			wantRefuse: false,
			why: "a skew allowance turns this into a post-migration backup blackout, which every deployment " +
				"pays for on every backup taken after a migration",
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			for _, skew := range skews {
				before, after := sc.build(time.Now().Add(skew))
				err := overlapObservations{before: before, after: after}.run(t)
				if sc.wantRefuse {
					require.Errorf(t, err, "proposer skew %s: %s", skew, sc.why)
					require.ErrorIsf(t, err, entitiesbackup.ErrBackupSpannedReindex, "proposer skew %s", skew)
					continue
				}
				require.NoErrorf(t, err, "proposer skew %s: %s", skew, sc.why)
			}
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
func TestReindexOverlapObserverCountsTheRightTerminalTasks(t *testing.T) {
	stamp := time.Now()

	tests := []struct {
		name       string
		obs        overlapObservations
		wantRefuse bool
		why        string
	}{
		{
			name: "cancelled before completing, after a backup claimed first",
			obs: overlapObservations{
				after: []*distributedtask.Task{
					overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, stamp,
						map[string]*distributedtask.Unit{
							"u1": {ID: "u1", Status: distributedtask.UnitStatusPending},
						}),
				},
			},
			wantRefuse: false,
			why:        "the backup won the race; failing it would punish the winner",
		},
		{
			name: "migration ran to completion inside the window",
			obs: overlapObservations{
				after: []*distributedtask.Task{
					overlapTaskWithUnits("Movies", distributedtask.TaskStatusFinished, stamp,
						map[string]*distributedtask.Unit{
							"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
						}),
				},
			},
			wantRefuse: true,
			why:        "nothing is running at commit, which is exactly why liveness is the wrong question",
		},
		{
			name: "migration still live at commit",
			obs: overlapObservations{
				after: []*distributedtask.Task{
					overlapTaskWithUnits("Movies", distributedtask.TaskStatusStarted, time.Time{},
						map[string]*distributedtask.Unit{
							"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress},
						}),
				},
			},
			wantRefuse: true,
			why:        "the capture and the migration are concurrent",
		},
		{
			name: "migration that failed inside the window",
			obs: overlapObservations{
				after: []*distributedtask.Task{
					overlapTaskWithUnits("Movies", distributedtask.TaskStatusFailed, stamp,
						map[string]*distributedtask.Unit{
							"u1": {ID: "u1", Status: distributedtask.UnitStatusFailed},
						}),
				},
			},
			wantRefuse: true,
			why:        "a failed migration may have written before it failed",
		},
		{
			// Units that left PENDING on purpose: with nil units the
			// cancelled-and-untouched rule would exempt this row too, and
			// either rule alone would keep it green.
			name: "cancelled after writing, but before this backup started",
			obs: func() overlapObservations {
				task := overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, stamp,
					map[string]*distributedtask.Unit{
						"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
					})
				return overlapObservations{before: []*distributedtask.Task{task}, after: []*distributedtask.Task{task}}
			}(),
			wantRefuse: false,
			why:        "it wrote, but it was over before the capture began",
		},
		{
			name: "finished before this backup started",
			obs: func() overlapObservations {
				task := overlapTaskWithUnits("Movies", distributedtask.TaskStatusFinished, stamp,
					map[string]*distributedtask.Unit{
						"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted},
					})
				return overlapObservations{before: []*distributedtask.Task{task}, after: []*distributedtask.Task{task}}
			}(),
			wantRefuse: false,
			why:        "no overlap at all",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.obs.run(t)
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
func TestReindexOverlapObserverCountsCancelledTasksThatRan(t *testing.T) {
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
			// Submitted and cancelled while the backup ran: absent from the
			// baseline, present at commit.
			task := overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled, time.Now(), tc.units)
			err := overlapObservations{after: []*distributedtask.Task{task}}.run(t)
			if tc.wantRefuse {
				require.Error(t, err, tc.why)
				require.ErrorIs(t, err, entitiesbackup.ErrBackupSpannedReindex, tc.why)
				return
			}
			require.NoError(t, err, tc.why)
		})
	}
}

// The overlap refusal is stored in the backup's failure meta and served from
// GET /v1/backups/{backend}/{id}. Its causes are RAFT and decoding errors that
// name nodes and task internals, which a backup caller is granted nothing on.
func TestReindexOverlapObserverRedactsItsCauses(t *testing.T) {
	raftErr := errors.New("can not resolve nodes [weaviate-2,weaviate-1]")
	unreadable := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "t", Version: 1},
		Namespace:      ReindexNamespace,
		Status:         distributedtask.TaskStatusStarted,
		Payload:        []byte("{not json"),
	}}

	tests := []struct {
		name    string
		obs     overlapObservations
		cause   error
		leaked  []string
		wantMsg string
	}{
		{
			name:    "task manager unreachable at backup start",
			obs:     overlapObservations{beforeErr: raftErr},
			cause:   raftErr,
			leaked:  []string{"weaviate-1", "weaviate-2", "can not resolve nodes"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried",
		},
		{
			name:    "task manager unreachable at commit",
			obs:     overlapObservations{afterErr: raftErr},
			cause:   raftErr,
			leaked:  []string{"weaviate-1", "weaviate-2", "can not resolve nodes"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: the cluster task manager could not be queried",
		},
		{
			name:    "task payload unreadable at backup start",
			obs:     overlapObservations{before: unreadable},
			leaked:  []string{"not json", "invalid character"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: a task payload is unreadable",
		},
		{
			name:    "task payload unreadable at commit",
			obs:     overlapObservations{after: unreadable},
			leaked:  []string{"not json", "invalid character"},
			wantMsg: "cannot rule out a runtime-reindex during this backup: a task payload is unreadable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.obs.run(t)
			require.Error(t, err)

			assert.Equal(t, tc.wantMsg, err.Error())
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

// TestObserveReindexOverlap_Unwired pins the startup-window default: allow + warn once.
func TestObserveReindexOverlap_Unwired(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()
	db := &DB{logger: logger}

	require.NoError(t, db.ObserveReindexOverlap(context.Background(), []string{"Movies"})(context.Background()))
	require.NoError(t, db.ObserveReindexOverlap(context.Background(), []string{"Movies"})(context.Background()))

	warnings := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel &&
			strings.Contains(entry.Message, "observer not yet installed") {
			warnings++
		}
	}
	assert.Equal(t, 1, warnings,
		"the unwired WARN is budgeted once per hour per node, not once per backup")
}

// The commit-time question is asked per backup, and a backup covers anywhere
// from zero to every collection in the cluster. A task outside that set must not
// fail the backup, and one anywhere inside it must.
func TestObserveReindexOverlapCollectionScope(t *testing.T) {
	migrated := overlapTask("Actors", distributedtask.TaskStatusFinished, time.Now())

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
			calls := 0
			db.SetReindexOverlapObserver(NewReindexOverlapObserver(
				func(context.Context) (map[string][]*distributedtask.Task, error) {
					calls++
					if calls == 1 {
						// The migration was submitted after the backup started.
						return map[string][]*distributedtask.Task{ReindexNamespace: nil}, nil
					}
					return map[string][]*distributedtask.Task{ReindexNamespace: {migrated}}, nil
				}, time.Hour))

			err := db.ObserveReindexOverlap(context.Background(), tc.collections)(context.Background())
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
	rolledBackAfterWriting := overlapTaskWithUnits("Movies", distributedtask.TaskStatusCancelled,
		time.Now(),
		map[string]*distributedtask.Unit{
			"u1": {ID: "u1", Status: distributedtask.UnitStatusInProgress, Progress: 0.6},
		})

	err := overlapObservations{after: []*distributedtask.Task{rolledBackAfterWriting}}.run(t)
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
