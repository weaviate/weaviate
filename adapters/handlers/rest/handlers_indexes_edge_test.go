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

package rest

// Edge-case tests for the synthetic "indexing"/"pending" entry that
// getIndexes / mergeReindexStatus emits when a property's schema flag is
// false but a reindex task is targeting it. These tests intentionally only
// exercise the pure helper mergeReindexStatus, since that is where the
// synthetic-entry decision is made.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// helper: build a *distributedtask.Task with the given payload + status + units.
func buildTask(t *testing.T, id string, status distributedtask.TaskStatus,
	payload db.ReindexTaskPayload, units map[string]*distributedtask.Unit,
) *distributedtask.Task {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return &distributedtask.Task{
		Namespace: db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{
			ID:      id,
			Version: 1,
		},
		Payload:   raw,
		Status:    status,
		StartedAt: time.Now(),
		Units:     units,
	}
}

func tasksMap(tasks ...*distributedtask.Task) []parsedReindexTask {
	return parseReindexTasks(tasks)
}

// Once a unit transitions to IN_PROGRESS the synthetic entry must read
// "indexing" even if no progress checkpoint has fired yet. The first
// per-shard checkpoint can lag the unit-claim transition by tens of
// seconds while bucket-open + compaction-pause + analyzer-overlay setup
// drains; a "pending" pill that lingers for that long is indistinguishable
// from "stuck".
//
// Compare with TestMergeReindexStatus_StartedNoProgress_ShowsPending —
// that case correctly stays "pending" because the unit hasn't been
// claimed yet (genuinely queued).
func TestMergeReindexStatus_UnitInProgressZeroProgress_ShowsIndexing(t *testing.T) {
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			// Unit has been claimed by the scheduler (IN_PROGRESS) but
			// hasn't reported its first checkpoint yet — Progress is still
			// the initial-claim 0.0 value.
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusInProgress, Progress: 0},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "indexing", idx.Status,
		"unit IN_PROGRESS without a checkpoint must surface as 'indexing', not 'pending' — work has started")
	require.Equal(t, float32(0), idx.Progress,
		"progress stays at 0 until the first checkpoint; the frontend renders 'Indexing' with no percent in that case")
}

// TestMergeReindexStatus_OneUnitInProgressAmongPending_ShowsIndexing
// covers a multi-shard task where one shard has started but the others
// haven't yet. The any-unit-working rule must pick up the lone IN_PROGRESS
// without needing every unit to advance — otherwise a slow-to-claim shard
// would hold the status at "pending" for the whole task.
func TestMergeReindexStatus_OneUnitInProgressAmongPending_ShowsIndexing(t *testing.T) {
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusInProgress, Progress: 0},
			"unit2": {ID: "unit2", Status: distributedtask.UnitStatusPending, Progress: 0},
			"unit3": {ID: "unit3", Status: distributedtask.UnitStatusPending, Progress: 0},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "indexing", idx.Status)
}

// Edge case 1: Task in STARTED state but no unit has reported progress yet.
// The payload claims enable-filterable on prop "foo"; the units map is
// non-empty but all units have Progress=0. Expectation: status="pending",
// Progress=0. This documents the "happy" early-state behavior the synthetic
// entry is supposed to give.
func TestMergeReindexStatus_StartedNoProgress_ShowsPending(t *testing.T) {
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusPending, Progress: 0},
			"unit2": {ID: "unit2", Status: distributedtask.UnitStatusPending, Progress: 0},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "pending", idx.Status, "STARTED task with zero progress should show pending")
	require.Equal(t, float32(0), idx.Progress)
}

// Pins: an unrecognized status reads as "indexing", not "pending", at zero progress.
func TestMergeReindexStatus_UnknownStatusNoProgress_ShowsIndexing(t *testing.T) {
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		unknownFutureStatus,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusPending, Progress: 0},
			"unit2": {ID: "unit2", Status: distributedtask.UnitStatusPending, Progress: 0},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "indexing", idx.Status)
	require.Equal(t, float32(0), idx.Progress)
}

// Edge case 2: Orphaned / crashed STARTED task. The RAFT FSM still records
// the task as STARTED but the Scheduler isn't actually executing it (e.g.
// server restarted between FSM apply and the Scheduler pickup). Units are
// PENDING with Progress=0 and have not been touched for hours. The
// synthetic entry will still report "pending" forever — there is no
// staleness check. Demonstrates the bug: a long-stale task is
// indistinguishable from a freshly submitted one.
func TestMergeReindexStatus_StaleStartedTask_StillShowsPending(t *testing.T) {
	staleTime := time.Now().Add(-72 * time.Hour)
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusPending, Progress: 0, UpdatedAt: staleTime},
		},
	)
	task.StartedAt = staleTime

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	// A 72h-old STARTED task that has not made a byte of progress is
	// reported as "pending" — same as a brand-new task. There is no
	// staleness signal in the response.
	require.Equal(t, "pending", idx.Status,
		"stale STARTED task is indistinguishable from a brand-new one; this is the bug")
}

// Edge case 2b: Same as above but with some progress. The synthetic entry
// reports "indexing" — again no staleness hint.
func TestMergeReindexStatus_StaleIndexing_StillShowsIndexing(t *testing.T) {
	staleTime := time.Now().Add(-72 * time.Hour)
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusInProgress, Progress: 0.4, UpdatedAt: staleTime},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "indexing", idx.Status)
	require.InDelta(t, 0.4, idx.Progress, 0.0001)
}

// Edge case 3: FAILED task. mergeReindexStatus surfaces a "failed"
// synthetic entry so the user can see from the /indexes endpoint that a
// previous attempt failed (and inspect /distributed-tasks for the error).
func TestMergeReindexStatus_FailedTask_ShowsFailedEntry(t *testing.T) {
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		distributedtask.TaskStatusFailed,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusFailed, Error: "disk full"},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "failed", idx.Status,
		"FAILED task must surface as the 'failed' synthetic status; "+
			"this is how the user learns a previous build attempt failed")
}

// Edge case 4: CANCELLED task. Same situation as FAILED — the synthetic
// entry surfaces a "cancelled" status so the caller can tell the build
// was explicitly stopped (vs. never requested).
func TestMergeReindexStatus_CancelledTask_ShowsCancelledEntry(t *testing.T) {
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		distributedtask.TaskStatusCancelled,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusCompleted, Progress: 0.5},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "cancelled", idx.Status,
		"CANCELLED task must surface as the 'cancelled' synthetic status")
	require.InDelta(t, 0.5, idx.Progress, 0.0001,
		"progress recorded before cancellation is preserved")
}

// FINISHED never surfaces a synthetic entry: the schema flag alone decides.
func TestMergeReindexStatus_FinishedTask_SurfacesNoSyntheticEntry(t *testing.T) {
	task := buildTask(t, "C:enable-filterable:foo:abcd",
		distributedtask.TaskStatusFinished,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"unit1": {ID: "unit1", Status: distributedtask.UnitStatusCompleted, Progress: 1.0},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "ready", idx.Status)
	require.Equal(t, float32(0), idx.Progress)
	require.Empty(t, idx.TaskID,
		"a FINISHED task must not stamp a task ID onto the base entry")
}

// Edge case 6: Two overlapping STARTED tasks targeting the same property.
// One is enable-filterable (progress 0.2), the other is change-tokenization
// (progress 0.9). For indexType="filterable", both match. The current
// implementation iterates `tasks` in map-list order and `return`s on the
// When two STARTED tasks for the same (collection, prop, indexType)
// coexist, the most recently started one wins regardless of slice
// order. The runtime delivers tasks in map iteration order which is
// non-deterministic per call, so first-in-list ordering would mean
// polling could see the answer change request-to-request. The
// StartedAt tiebreak keeps the response stable.
//
// In practice checkReindexConflict rejects overlapping STARTED tasks
// on the same bucket, but a runtime fault (e.g. cluster forwarding
// edge cases) could in theory produce this state and the response must
// still be stable.
func TestMergeReindexStatus_OverlappingStartedTasks_NewestWins(t *testing.T) {
	now := time.Now()

	enableTask := buildTask(t, "C:enable-filterable:foo:0001",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"u": {ID: "u", Status: distributedtask.UnitStatusInProgress, Progress: 0.2},
		},
	)
	enableTask.StartedAt = now.Add(-1 * time.Hour) // older

	changeTokTask := buildTask(t, "C:change-tokenization:foo:0002",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType:      db.ReindexTypeChangeTokenization,
			Collection:         "C",
			Properties:         []string{"foo"},
			TargetTokenization: "lowercase",
		},
		map[string]*distributedtask.Unit{
			"u": {ID: "u", Status: distributedtask.UnitStatusInProgress, Progress: 0.9},
		},
	)
	changeTokTask.StartedAt = now // newer

	for _, order := range []struct {
		name  string
		tasks []*distributedtask.Task
	}{
		{"older-first", []*distributedtask.Task{enableTask, changeTokTask}},
		{"newer-first", []*distributedtask.Task{changeTokTask, enableTask}},
	} {
		t.Run(order.name, func(t *testing.T) {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "C", "foo", "filterable", parseReindexTasks(order.tasks), nil)

			require.InDelta(t, 0.9, idx.Progress, 0.0001,
				"newest STARTED task (change-tokenization) must win regardless of slice order")
			require.Equal(t, "lowercase", idx.TargetTokenization,
				"the winning task's TargetTokenization must be reflected")
		})
	}
}

// In-flight beats any terminal attempt regardless of slice order.
func TestMergeReindexStatus_StartedBeatsTerminal(t *testing.T) {
	now := time.Now()

	for _, terminalStatus := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusFinished,
	} {
		for _, liveStatus := range []distributedtask.TaskStatus{
			distributedtask.TaskStatusStarted,
			unknownFutureStatus,
		} {
			t.Run(string(terminalStatus)+"/"+string(liveStatus), func(t *testing.T) {
				oldAttempt := buildTask(t, "C:enable-filterable:foo:0001",
					terminalStatus,
					db.ReindexTaskPayload{
						MigrationType: db.ReindexTypeEnableFilterable,
						Collection:    "C",
						Properties:    []string{"foo"},
					},
					map[string]*distributedtask.Unit{
						"u": {ID: "u", Status: distributedtask.UnitStatusFailed, Progress: 0.4, Error: "disk full"},
					},
				)
				oldAttempt.StartedAt = now.Add(-2 * time.Hour)

				liveRetry := buildTask(t, "C:enable-filterable:foo:0002",
					liveStatus,
					db.ReindexTaskPayload{
						MigrationType: db.ReindexTypeEnableFilterable,
						Collection:    "C",
						Properties:    []string{"foo"},
					},
					map[string]*distributedtask.Unit{
						"u": {ID: "u", Status: distributedtask.UnitStatusInProgress, Progress: 0.1},
					},
				)
				liveRetry.StartedAt = now

				for _, order := range []struct {
					name  string
					tasks []*distributedtask.Task
				}{
					{"terminal-first", []*distributedtask.Task{oldAttempt, liveRetry}},
					{"live-first", []*distributedtask.Task{liveRetry, oldAttempt}},
				} {
					t.Run(order.name, func(t *testing.T) {
						idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
						mergeReindexStatus(idx, "C", "foo", "filterable", parseReindexTasks(order.tasks), nil)

						require.Equal(t, "indexing", idx.Status,
							"the live retry must beat the older terminal attempt regardless of slice order")
						require.InDelta(t, 0.1, idx.Progress, 0.0001,
							"the stale attempt's progress must not be surfaced")
					})
				}
			})
		}
	}
}

// Newest terminal task wins regardless of slice order; that's why FINISHED
// tasks stay in the merge loop instead of being dropped.
func TestMergeReindexStatus_NewestTerminalWins(t *testing.T) {
	now := time.Now()

	tests := []struct {
		newerStatus  distributedtask.TaskStatus
		wantStatus   string
		wantProgress float32
	}{
		{distributedtask.TaskStatusFailed, "failed", 0.7},
		{distributedtask.TaskStatusFinished, "ready", 0},
	}

	for _, tt := range tests {
		t.Run(string(tt.newerStatus), func(t *testing.T) {
			oldFail := buildTask(t, "C:enable-filterable:foo:0001",
				distributedtask.TaskStatusFailed,
				db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeEnableFilterable,
					Collection:    "C",
					Properties:    []string{"foo"},
				},
				map[string]*distributedtask.Unit{
					"u": {ID: "u", Status: distributedtask.UnitStatusFailed, Progress: 0.3, Error: "old: disk full"},
				},
			)
			oldFail.StartedAt = now.Add(-2 * time.Hour)

			newer := buildTask(t, "C:enable-filterable:foo:0002",
				tt.newerStatus,
				db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeEnableFilterable,
					Collection:    "C",
					Properties:    []string{"foo"},
				},
				map[string]*distributedtask.Unit{
					"u": {ID: "u", Status: distributedtask.UnitStatusFailed, Progress: 0.7, Error: "new: permission denied"},
				},
			)
			newer.StartedAt = now

			for _, order := range []struct {
				name  string
				tasks []*distributedtask.Task
			}{
				{"old-first", []*distributedtask.Task{oldFail, newer}},
				{"new-first", []*distributedtask.Task{newer, oldFail}},
			} {
				t.Run(order.name, func(t *testing.T) {
					idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
					mergeReindexStatus(idx, "C", "foo", "filterable", parseReindexTasks(order.tasks), nil)

					require.Equal(t, tt.wantStatus, idx.Status)
					require.InDelta(t, tt.wantProgress, idx.Progress, 0.0001,
						"the newer terminal attempt must win the tiebreak regardless of slice order")
				})
			}
		})
	}
}

// Edge case 7: A task whose payload.Properties is empty. Every migration
// type treats it as targeting nothing, so no synthetic entry appears. The
// current REST handler always populates Properties with exactly one entry,
// so an empty list only arrives via direct cluster payload authoring.
//
// This agrees across migration types but deliberately disagrees with
// db.ReindexPropsOverlap, which reads the same empty list as "all
// properties" (see TestPropsOverlap_EmptyMeansAllProperties). Making the
// two agree fans a synthetic "indexing" entry across the whole collection
// for a task that does no work on any property.
//
// Test split into two parts to assert symmetry:
//
//	a) enable-filterable with empty Properties → no synthetic entry.
//	b) repair-searchable with empty Properties → no synthetic entry
//	   (previously: matched every property — now matches none).
func TestMergeReindexStatus_EmptyProperties_EnableDoesNothing(t *testing.T) {
	task := buildTask(t, "C:enable-filterable::abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    nil, // empty
		},
		map[string]*distributedtask.Unit{
			"u": {ID: "u", Status: distributedtask.UnitStatusInProgress, Progress: 0.3},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "C", "anyprop", "filterable", tasksMap(task), nil)

	require.Equal(t, "ready", idx.Status,
		"empty Properties is treated uniformly as 'match nothing'")
}

func TestMergeReindexStatus_EmptyProperties_RepairAlsoMatchesNothing(t *testing.T) {
	task := buildTask(t, "C:repair-searchable::abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeChangeAlgorithm,
			Collection:    "C",
			Properties:    nil, // empty — previously matched every property
		},
		map[string]*distributedtask.Unit{
			"u": {ID: "u", Status: distributedtask.UnitStatusInProgress, Progress: 0.5},
		},
	)

	// Three different properties — none should be reported as "indexing".
	// Previously repair-* matched every property in the collection.
	for _, propName := range []string{"alpha", "beta", "gamma"} {
		idx := &models.IndexStatus{Type: "searchable", Status: "ready"}
		mergeReindexStatus(idx, "C", propName, "searchable", tasksMap(task), nil)
		require.Equal(t, "ready", idx.Status,
			"empty Properties + repair-searchable must match no property (here: %s)", propName)
		require.Equal(t, float32(0), idx.Progress)
	}
}

// Sanity test: confirm mergeReindexStatus matches case-insensitively on
// collection name (it uses strings.EqualFold). Documents this minor
// edge case as intentional — case mismatch alone is not a bug.
func TestMergeReindexStatus_CollectionCaseInsensitive(t *testing.T) {
	task := buildTask(t, "MyClass:enable-filterable:foo:abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "MyClass",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"u": {ID: "u", Status: distributedtask.UnitStatusInProgress, Progress: 0.1},
		},
	)

	idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idx, "myclass", "foo", "filterable", tasksMap(task), nil)

	require.Equal(t, "indexing", idx.Status, "collection name match is case-insensitive")
}

// repair-searchable on a property must surface TargetAlgorithm="blockmax"
// on the IndexStatus while the task is in flight. This is the algorithm
// equivalent of change-tokenization's TargetTokenization and is what lets
// the UI render the in-flight WAND -> Block Max WAND switch.
func TestMergeReindexStatus_RepairSearchable_SetsTargetAlgorithm(t *testing.T) {
	tests := []struct {
		name           string
		taskStatus     distributedtask.TaskStatus
		unitStatus     distributedtask.UnitStatus
		expectStatus   string
		expectAlgoSet  bool
		progress       float32
		expectProgress float32
	}{
		{
			name:           "started but no unit has claimed yet emits pending + target algorithm",
			taskStatus:     distributedtask.TaskStatusStarted,
			unitStatus:     distributedtask.UnitStatusPending,
			expectStatus:   "pending",
			expectAlgoSet:  true,
			progress:       0,
			expectProgress: 0,
		},
		{
			name:           "started with unit in progress but no checkpoint yet emits indexing + target algorithm",
			taskStatus:     distributedtask.TaskStatusStarted,
			unitStatus:     distributedtask.UnitStatusInProgress,
			expectStatus:   "indexing",
			expectAlgoSet:  true,
			progress:       0,
			expectProgress: 0,
		},
		{
			name:           "started with progress emits indexing + target algorithm",
			taskStatus:     distributedtask.TaskStatusStarted,
			unitStatus:     distributedtask.UnitStatusInProgress,
			expectStatus:   "indexing",
			expectAlgoSet:  true,
			progress:       0.42,
			expectProgress: 0.42,
		},
		{
			name:           "failed task still surfaces target algorithm for the failed attempt",
			taskStatus:     distributedtask.TaskStatusFailed,
			unitStatus:     distributedtask.UnitStatusFailed,
			expectStatus:   "failed",
			expectAlgoSet:  true,
			progress:       0.5,
			expectProgress: 0.5,
		},
		{
			name:           "cancelled task still surfaces target algorithm for the cancelled attempt",
			taskStatus:     distributedtask.TaskStatusCancelled,
			unitStatus:     distributedtask.UnitStatusInProgress,
			expectStatus:   "cancelled",
			expectAlgoSet:  true,
			progress:       0.3,
			expectProgress: 0.3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := buildTask(t, "C:repair-searchable:foo:abcd",
				tt.taskStatus,
				db.ReindexTaskPayload{
					MigrationType: db.ReindexTypeChangeAlgorithm,
					Collection:    "C",
					Properties:    []string{"foo"},
				},
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Status: tt.unitStatus, Progress: tt.progress},
				},
			)

			idx := &models.IndexStatus{Type: "searchable", Status: "ready"}
			mergeReindexStatus(idx, "C", "foo", "searchable", tasksMap(task), nil)

			require.Equal(t, tt.expectStatus, idx.Status)
			require.InDelta(t, tt.expectProgress, idx.Progress, 0.0001)
			if tt.expectAlgoSet {
				require.Equal(t, models.IndexStatusTargetAlgorithmBlockmax, idx.TargetAlgorithm,
					"repair-searchable must surface targetAlgorithm=blockmax for honest UI rendering of the in-flight WAND -> Block Max WAND switch")
			}
			require.Empty(t, idx.Algorithm,
				"merge does not write Algorithm; that field is sourced from the class config in getIndexes")
		})
	}
}

// repair-filterable / repair-rangeable / enable-* must NOT populate
// TargetAlgorithm on the IndexStatus. The algorithm field is searchable-only;
// adding it to other index types would mislead the UI into showing a BM25
// algorithm switch for an index that has no BM25 algorithm.
func TestMergeReindexStatus_NonSearchableTypes_DoNotSetTargetAlgorithm(t *testing.T) {
	tests := []struct {
		name          string
		migrationType db.ReindexMigrationType
		indexType     string
	}{
		{"repair-filterable", db.ReindexTypeRepairFilterable, "filterable"},
		{"repair-rangeable", db.ReindexTypeRepairRangeable, "rangeable"},
		{"enable-filterable", db.ReindexTypeEnableFilterable, "filterable"},
		{"enable-rangeable", db.ReindexTypeEnableRangeable, "rangeable"},
		{"enable-searchable", db.ReindexTypeEnableSearchable, "searchable"},
		{"change-tokenization", db.ReindexTypeChangeTokenization, "searchable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := buildTask(t, "C:"+string(tt.migrationType)+":foo:abcd",
				distributedtask.TaskStatusStarted,
				db.ReindexTaskPayload{
					MigrationType:      tt.migrationType,
					Collection:         "C",
					Properties:         []string{"foo"},
					TargetTokenization: "word",
				},
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Progress: 0.5},
				},
			)

			idx := &models.IndexStatus{Type: tt.indexType, Status: "ready"}
			mergeReindexStatus(idx, "C", "foo", tt.indexType, tasksMap(task), nil)

			require.Empty(t, idx.TargetAlgorithm,
				"%s must not set TargetAlgorithm — algorithm is a searchable-only concept", tt.migrationType)
		})
	}
}

// PREPARING and SWAPPING both surface as "indexing@100%" in the user-
// visible status while the cluster-wide PrepCompleteAck barrier (PREPARING)
// or the per-node atomic swap + schema flip (SWAPPING) is still in
// flight. Both paint the synthetic side-effect fields the same way
// STARTED does so the UI keeps rendering the in-flight pill with the
// target tokenization preview.
//
// This guards two regressions:
//   - Forgetting PREPARING in [mergeReindexStatus]'s status switch
//     (would leave PREPARING tasks with the base "ready" status and no
//     synthetic targetTokenization, blanking the UI mid-barrier).
//   - Forgetting PREPARING in [taskStatusPriority] (would let an older
//     terminal task outrank a fresh PREPARING task, surfacing the wrong
//     attempt's signal).
func TestMergeReindexStatus_PreparingAndSwappingSurfaceAsIndexing(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status distributedtask.TaskStatus
	}{
		{"PREPARING", distributedtask.TaskStatusPreparing},
		{"SWAPPING", distributedtask.TaskStatusSwapping},
		// An unrecognized status must land here too.
		{"UNKNOWN", unknownFutureStatus},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := buildTask(t, "C:change-tokenization:foo:"+tt.name,
				tt.status,
				db.ReindexTaskPayload{
					MigrationType:      db.ReindexTypeChangeTokenization,
					Collection:         "C",
					Properties:         []string{"foo"},
					TargetTokenization: "lowercase",
				},
				map[string]*distributedtask.Unit{
					// Units all terminal — PREP/SWAP barrier work happens
					// in scheduler callbacks, not via per-unit progress.
					"u1": {ID: "u1", Status: distributedtask.UnitStatusCompleted, Progress: 1.0},
				},
			)

			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task), nil)

			require.Equal(t, "indexing", idx.Status,
				"%s must surface as 'indexing' — the cluster-wide post-completion barrier is still gating the schema flip", tt.name)
			require.InDelta(t, 1.0, idx.Progress, 0.0001,
				"%s implies units all done; progress must read 100%%", tt.name)
			require.Equal(t, "lowercase", idx.TargetTokenization,
				"%s must paint the targetTokenization synthetic side-effect — UI needs it to render the in-flight tokenization preview", tt.name)
		})
	}
}

// Pins: every non-terminal status, including an unrecognized one, ranks
// above every terminal status.
func TestTaskStatusPriority_InFlightStatesRankAboveTerminal(t *testing.T) {
	mkTask := func(status distributedtask.TaskStatus) *distributedtask.Task {
		return &distributedtask.Task{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "t", Version: 1},
			Status:         status,
		}
	}
	for _, tt := range []struct {
		name   string
		status distributedtask.TaskStatus
		want   int
	}{
		{"STARTED", distributedtask.TaskStatusStarted, 2},
		{"PREPARING", distributedtask.TaskStatusPreparing, 2},
		{"SWAPPING", distributedtask.TaskStatusSwapping, 2},
		{"unrecognized", unknownFutureStatus, 2},
		{"FAILED", distributedtask.TaskStatusFailed, 1},
		{"CANCELLED", distributedtask.TaskStatusCancelled, 1},
		{"FINISHED", distributedtask.TaskStatusFinished, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, taskStatusPriority(mkTask(tt.status)),
				"status %s must rank at priority %d", tt.status, tt.want)
		})
	}
}

// unknownFutureStatus simulates a status a newer node introduced that this
// build doesn't recognize. Deliberately not a capitalised present
// participle, so it can never collide with a real status name.
//
// A const is fine outside cluster/distributedtask. Inside it, the
// exhaustive linter reads every TaskStatus const in the package as an
// enum member, so the copy there is a var.
const unknownFutureStatus distributedtask.TaskStatus = "UNKNOWN_FUTURE_STATE"

// Pins the wire response of every cancel arm: the apply is reached for
// STARTED alone, 409 for the coordination phases and for a status this
// build cannot name, 202 NO_OP when nothing matches. Each arm's audit
// line is pinned with it, and the two 409 conditions are held apart:
// they need different bodies because only one of them can honestly tell
// the operator to wait.
func TestCancelPreflight_WireResponsePerStatus(t *testing.T) {
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeEnableFilterable,
		Collection:    "C",
		Properties:    []string{"foo"},
	}

	for _, tc := range []struct {
		name       string
		status     distributedtask.TaskStatus
		properties []string
		wantCode   int
		wantReason string
	}{
		{"STARTED", distributedtask.TaskStatusStarted, payload.Properties, 0, ""},
		// Refused, not proposed: only a build that can name the status
		// knows whether stopping is safe, so the FSM refuses it on every
		// node and REST must not spend a RAFT apply finding that out.
		// "Wait for it to reach a terminal state" is the wrong advice
		// here — nothing on this node advances a status it cannot name.
		{
			"unrecognized", unknownFutureStatus, payload.Properties,
			http.StatusConflict, "cannot classify that status",
		},
		{
			"PREPARING", distributedtask.TaskStatusPreparing, payload.Properties,
			http.StatusConflict, "wait for it to reach a terminal state",
		},
		{
			"SWAPPING", distributedtask.TaskStatusSwapping, payload.Properties,
			http.StatusConflict, "wait for it to reach a terminal state",
		},
		// The three terminal rows never reach the refusal logic: they
		// are filtered out as cancel targets, so what they pin is that a
		// finished task is a NO_OP rather than a 409.
		{"FINISHED", distributedtask.TaskStatusFinished, payload.Properties, http.StatusAccepted, ""},
		{"FAILED", distributedtask.TaskStatusFailed, payload.Properties, http.StatusAccepted, ""},
		{"CANCELLED", distributedtask.TaskStatusCancelled, payload.Properties, http.StatusAccepted, ""},
		// Empty Properties is the reserved whole-collection form; it has
		// to match the queried property or the operator gets no cancel
		// target for a task that blocks their mutation.
		{"empty properties", distributedtask.TaskStatusStarted, nil, 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			h := &indexesHandlers{appState: &state.State{Logger: logger}}

			tcPayload := payload
			tcPayload.Properties = tc.properties
			task := buildTask(t, "T1", tc.status, tcPayload, nil)

			target, gotPayload := findCancelTarget(
				[]*distributedtask.Task{task}, "C", "foo", "filterable", logger)
			resp := h.cancelPreflight(target, "C", "foo", "filterable", nil)

			if tc.wantCode == 0 {
				require.Nil(t, resp, "%q must reach the cancel apply", tc.status)
				require.Equal(t, "T1", target.ID)
				require.Equal(t, db.ReindexTypeEnableFilterable, gotPayload.MigrationType)
				return
			}

			require.NotNil(t, resp, "%q must be answered before the cancel apply", tc.status)
			rec := httptest.NewRecorder()
			resp.WriteResponse(rec, runtime.JSONProducer())
			require.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode == http.StatusAccepted {
				require.Contains(t, rec.Body.String(), reindexCancelStatusNoOp)
				require.Equal(t, "reindex_task_cancel_noop", auditEvent(t, hook))
				return
			}

			body := rec.Body.String()
			// Nothing on this node advances a status it cannot name, so
			// the coordination-phase advice must not leak onto that arm.
			// Asserted first on purpose: a Contains failing ahead of it
			// would abort the subtest and leave the advice unchecked on
			// exactly the arm that leaked it.
			if !tc.status.IsRecognized() {
				require.NotContains(t, body, "wait for it to reach a terminal state")
			}
			require.Contains(t, body, "T1", "the refusal must name the task it refuses")
			require.Contains(t, body, string(tc.status), "the refusal must name the phase")
			require.Contains(t, body, tc.wantReason)
			require.Equal(t, "reindex_task_cancel_refused", auditEvent(t, hook))
		})
	}
}

// auditEvent returns the audit_event field of the single audit line the
// hook captured.
func auditEvent(t *testing.T, hook *logrustest.Hook) string {
	t.Helper()
	var events []string
	for _, e := range hook.AllEntries() {
		if v, ok := e.Data["audit_event"]; ok {
			events = append(events, v.(string))
		}
	}
	require.Len(t, events, 1, "the cancel arm must emit exactly one audit line")
	return events[0]
}

// Pins: a cancel refused at apply time answers with the pre-flight's status
// code, not a 500 that leaked the sentinel's internal marker.
func TestCancelApplyFailureResponder_MapsFSMRejections(t *testing.T) {
	target := buildTask(t, "T1", distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		}, nil)

	// The target still reads STARTED — the pre-flight would not have let the
	// request reach the apply otherwise — and the apply has just proved it is
	// not STARTED any more. The raced arm cannot tell which status it moved
	// to, so it must name none of them.
	var everyStatus []string
	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
		distributedtask.TaskStatusFinished,
		distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
	} {
		everyStatus = append(everyStatus, status.String())
	}
	// The coordination-phase advice is wrong on the raced arm for the same
	// reason: the task may have raced to the very terminal state that advice
	// tells the operator to wait for. cancelRefusalReason is where it comes
	// from, so a refactor routing this arm through it must fail here.
	racedAbsent := append([]string{"wait for it to reach a terminal state"}, everyStatus...)

	for _, tc := range []struct {
		name      string
		err       error
		wantCode  int
		wantAudit string
		wantBody  string
		// wantAbsent is text the body must not carry, for the arms
		// where the obvious wording would be wrong.
		wantAbsent []string
		// wantStatusAtRead is the audit line's record of the status the
		// list read saw, under a name that says it is stale.
		wantStatusAtRead string
	}{
		{
			// The FSM stamps the on-wire marker into the message, and it
			// survives the gRPC round trip, so the error this arm really
			// receives carries one. Built with it here, otherwise the
			// "no marker in the body" assertion below has nothing to
			// leak.
			name: "task is no longer running",
			err: fmt.Errorf("executing command: %w",
				fmt.Errorf("[dtm-perm/task-not-running] task reindex/T1/1 is no longer running: %w",
					distributedtask.ErrTaskNotRunning)),
			wantCode:         http.StatusConflict,
			wantAudit:        "reindex_task_cancel_raced",
			wantBody:         "T1",
			wantAbsent:       racedAbsent,
			wantStatusAtRead: "STARTED",
		},
		{
			// Same arm reached by a sentinel no marker was stamped on, since
			// the switch classifies on errors.Is alone.
			name:             "task is no longer running, unmarked",
			err:              fmt.Errorf("executing command: %w", distributedtask.ErrTaskNotRunning),
			wantCode:         http.StatusConflict,
			wantAudit:        "reindex_task_cancel_raced",
			wantBody:         "T1",
			wantAbsent:       racedAbsent,
			wantStatusAtRead: "STARTED",
		},
		{
			name: "task does not exist",
			err: fmt.Errorf("executing command: %w",
				fmt.Errorf("[dtm-perm/task-not-exist] task reindex/T1/1 does not exist: %w",
					distributedtask.ErrTaskDoesNotExist)),
			wantCode:  http.StatusAccepted,
			wantAudit: "reindex_task_cancel_noop",
			wantBody:  reindexCancelStatusNoOp,
		},
		{
			name:     "anything else",
			err:      errors.New("raft unavailable"),
			wantCode: http.StatusInternalServerError,
			wantBody: "raft unavailable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()
			h := &indexesHandlers{appState: &state.State{Logger: logger}}

			rec := httptest.NewRecorder()
			h.cancelApplyFailureResponder(tc.err, target, "C", "foo", "filterable", nil).
				WriteResponse(rec, runtime.JSONProducer())

			// Asserted first on purpose: a status assertion failing
			// ahead of it would abort the subtest and leave the marker
			// unchecked on exactly the arm that leaked it.
			require.NotContains(t, rec.Body.String(), "dtm-perm/",
				"the sentinel's internal marker is not user-facing")
			require.Equal(t, tc.wantCode, rec.Code)
			require.Contains(t, rec.Body.String(), tc.wantBody)
			for _, absent := range tc.wantAbsent {
				require.NotContains(t, rec.Body.String(), absent)
			}
			if tc.wantAudit == "" {
				return
			}
			require.Equal(t, tc.wantAudit, auditEvent(t, hook))
			if tc.wantStatusAtRead != "" {
				require.Equal(t, tc.wantStatusAtRead, hook.LastEntry().Data["status_at_read"],
					"the audit line keeps the status the read saw, under a name that says it is stale")
			}
		})
	}
}

// Pins the selection policy when several in-flight tasks match: a
// cancellable one wins, so the operator never gets a 409 while a task
// the FSM would have cancelled sits further down the list. With none
// cancellable the first match is reported, so the refusal names a task
// they can look up.
func TestFindCancelTarget_PrefersACancellableTask(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	payload := db.ReindexTaskPayload{
		MigrationType: db.ReindexTypeEnableFilterable,
		Collection:    "C",
		Properties:    []string{"foo"},
	}
	task := func(id string, status distributedtask.TaskStatus) *distributedtask.Task {
		return buildTask(t, id, status, payload, nil)
	}

	for _, tc := range []struct {
		name   string
		tasks  []*distributedtask.Task
		wantID string
	}{
		{
			name: "a cancellable task behind a coordination phase",
			tasks: []*distributedtask.Task{
				task("swapping", distributedtask.TaskStatusSwapping),
				task("started", distributedtask.TaskStatusStarted),
			},
			wantID: "started",
		},
		{
			name: "none cancellable, so the first match is refused",
			tasks: []*distributedtask.Task{
				task("swapping", distributedtask.TaskStatusSwapping),
				task("preparing", distributedtask.TaskStatusPreparing),
			},
			wantID: "swapping",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, _ := findCancelTarget(tc.tasks, "C", "foo", "filterable", logger)
			require.NotNil(t, target)
			require.Equal(t, tc.wantID, target.ID)
		})
	}
}

// Pins: an in-flight task whose payload will not decode is logged rather
// than skipped in silence. It may be the very task the operator is trying
// to cancel, and the answer they get is a NO_OP.
func TestFindCancelTarget_LogsUndecodablePayload(t *testing.T) {
	logger, hook := logrustest.NewNullLogger()

	target, _ := findCancelTarget([]*distributedtask.Task{{
		Namespace:      db.ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_bad", Version: 1},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        []byte("not json"),
	}}, "C", "foo", "filterable", logger)

	require.Nil(t, target)
	require.Len(t, hook.AllEntries(), 1)
	require.Equal(t, "T_bad", hook.AllEntries()[0].Data["task_id"])
}
