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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	mergeReindexStatus(idx, "C", "foo", "filterable", false, tasksMap(task), nil)

	require.Equal(t, "pending", idx.Status, "STARTED task with zero progress should show pending")
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
	mergeReindexStatus(idx, "C", "foo", "filterable", false, tasksMap(task), nil)

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
	mergeReindexStatus(idx, "C", "foo", "filterable", false, tasksMap(task), nil)

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
	mergeReindexStatus(idx, "C", "foo", "filterable", false, tasksMap(task), nil)

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
	mergeReindexStatus(idx, "C", "foo", "filterable", false, tasksMap(task), nil)

	require.Equal(t, "cancelled", idx.Status,
		"CANCELLED task must surface as the 'cancelled' synthetic status")
	require.InDelta(t, 0.5, idx.Progress, 0.0001,
		"progress recorded before cancellation is preserved")
}

// Edge case 5: Task moved to FINISHED but the schema flag flip
// (IndexFilterable=true) hasn't propagated yet. Real-life cause: the DTM
// transitions a semantic task to FINISHED once every unit is COMPLETED,
// but OnGroupCompleted's swap+schema-flip runs after that on each node.
// During the gap, the schema flag is still false on this node.
//
// Pre-fix this case produced no entry at all (idx stayed "ready" but
// flagOn=false meant the caller dropped it), so the UI rendered "None"
// for a few ms. The fix here emits "indexing@1.0" until the flag flips,
// closing the visible gap. Once flagOn flips to true, the base "ready"
// override wins (verified by the second sub-test below).
func TestMergeReindexStatus_FinishedBeforeSchemaFlip_KeepsFinalizingEntry(t *testing.T) {
	mkTask := func() *distributedtask.Task {
		return buildTask(t, "C:enable-filterable:foo:abcd",
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
	}

	t.Run("flag-off (swap not propagated yet)", func(t *testing.T) {
		idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
		mergeReindexStatus(idx, "C", "foo", "filterable", false, tasksMap(mkTask()), nil)

		// "indexing@100%" so the caller emits a synthetic entry (the flag
		// is still false). Closes the brief visible "None" gap that
		// frontend-claude reported (issue #10675).
		require.Equal(t, "indexing", idx.Status)
		require.InDelta(t, 1.0, idx.Progress, 0.0001)
	})

	t.Run("flag-on (schema already caught up)", func(t *testing.T) {
		idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
		mergeReindexStatus(idx, "C", "foo", "filterable", true, tasksMap(mkTask()), nil)

		// Base case wins — stale FINISHED task must not override the
		// post-flip "ready" state.
		require.Equal(t, "ready", idx.Status)
		require.Equal(t, float32(0), idx.Progress)
	})
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
			mergeReindexStatus(idx, "C", "foo", "filterable", false,
				parseReindexTasks(order.tasks), nil)

			require.InDelta(t, 0.9, idx.Progress, 0.0001,
				"newest STARTED task (change-tokenization) must win regardless of slice order")
			require.Equal(t, "lowercase", idx.TargetTokenization,
				"the winning task's TargetTokenization must be reflected")
		})
	}
}

// A retried migration produces two tasks for the same (collection,
// prop, indexType): the old FAILED attempt and the new STARTED one
// (terminal tasks deliberately do not block fresh submits). The
// in-flight STARTED task wins regardless of slice order — otherwise
// the user who just retried would see "failed" on alternate polls.
func TestMergeReindexStatus_StartedBeatsTerminal(t *testing.T) {
	now := time.Now()

	failedAttempt := buildTask(t, "C:enable-filterable:foo:0001",
		distributedtask.TaskStatusFailed,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"u": {ID: "u", Status: distributedtask.UnitStatusFailed, Progress: 0.4, Error: "disk full"},
		},
	)
	failedAttempt.StartedAt = now.Add(-2 * time.Hour)

	startedRetry := buildTask(t, "C:enable-filterable:foo:0002",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"u": {ID: "u", Status: distributedtask.UnitStatusInProgress, Progress: 0.1},
		},
	)
	startedRetry.StartedAt = now

	for _, order := range []struct {
		name  string
		tasks []*distributedtask.Task
	}{
		{"failed-first", []*distributedtask.Task{failedAttempt, startedRetry}},
		{"started-first", []*distributedtask.Task{startedRetry, failedAttempt}},
	} {
		t.Run(order.name, func(t *testing.T) {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "C", "foo", "filterable", false,
				parseReindexTasks(order.tasks), nil)

			require.Equal(t, "indexing", idx.Status,
				"STARTED retry must beat older FAILED attempt regardless of slice order")
			require.InDelta(t, 0.1, idx.Progress, 0.0001)
		})
	}
}

// Two FAILED attempts for the same (collection, prop, indexType) can
// coexist if a user retried after the first failure and the second
// retry also failed. The newer attempt is the more useful one to
// surface (its error is the latest the user saw) so it must win the
// tiebreak regardless of slice order.
func TestMergeReindexStatus_TwoFailedTasks_NewestWins(t *testing.T) {
	now := time.Now()

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

	newFail := buildTask(t, "C:enable-filterable:foo:0002",
		distributedtask.TaskStatusFailed,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeEnableFilterable,
			Collection:    "C",
			Properties:    []string{"foo"},
		},
		map[string]*distributedtask.Unit{
			"u": {ID: "u", Status: distributedtask.UnitStatusFailed, Progress: 0.7, Error: "new: permission denied"},
		},
	)
	newFail.StartedAt = now

	for _, order := range []struct {
		name  string
		tasks []*distributedtask.Task
	}{
		{"old-first", []*distributedtask.Task{oldFail, newFail}},
		{"new-first", []*distributedtask.Task{newFail, oldFail}},
	} {
		t.Run(order.name, func(t *testing.T) {
			idx := &models.IndexStatus{Type: "filterable", Status: "ready"}
			mergeReindexStatus(idx, "C", "foo", "filterable", false,
				parseReindexTasks(order.tasks), nil)

			require.Equal(t, "failed", idx.Status)
			require.InDelta(t, 0.7, idx.Progress, 0.0001,
				"newer FAILED attempt must win the tiebreak regardless of slice order")
		})
	}
}

// Edge case 7: A task whose payload.Properties is empty.
// Previously this branch was asymmetric: repair-* matched every property
// in the collection (fan-out: a single payload could mark dozens of
// properties "indexing"), while enable-* and change-tokenization matched
// nothing. After the fix every migration type rejects empty Properties
// consistently — the task is treated as targeting nothing, producing no
// synthetic entry. The current REST handler always populates Properties
// with exactly one entry, so the empty-means-all branch was only
// reachable via direct cluster payload authoring.
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
	mergeReindexStatus(idx, "C", "anyprop", "filterable", false, tasksMap(task), nil)

	require.Equal(t, "ready", idx.Status,
		"empty Properties is treated uniformly as 'match nothing'")
}

func TestMergeReindexStatus_EmptyProperties_RepairAlsoMatchesNothing(t *testing.T) {
	task := buildTask(t, "C:repair-searchable::abcd",
		distributedtask.TaskStatusStarted,
		db.ReindexTaskPayload{
			MigrationType: db.ReindexTypeRepairSearchable,
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
		mergeReindexStatus(idx, "C", propName, "searchable", false, tasksMap(task), nil)
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
	mergeReindexStatus(idx, "myclass", "foo", "filterable", false, tasksMap(task), nil)

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
		expectStatus   string
		expectAlgoSet  bool
		progress       float32
		expectProgress float32
	}{
		{
			name:           "started no progress emits pending + target algorithm",
			taskStatus:     distributedtask.TaskStatusStarted,
			expectStatus:   "pending",
			expectAlgoSet:  true,
			progress:       0,
			expectProgress: 0,
		},
		{
			name:           "started with progress emits indexing + target algorithm",
			taskStatus:     distributedtask.TaskStatusStarted,
			expectStatus:   "indexing",
			expectAlgoSet:  true,
			progress:       0.42,
			expectProgress: 0.42,
		},
		{
			name:           "failed task still surfaces target algorithm for the failed attempt",
			taskStatus:     distributedtask.TaskStatusFailed,
			expectStatus:   "failed",
			expectAlgoSet:  true,
			progress:       0.5,
			expectProgress: 0.5,
		},
		{
			name:           "cancelled task still surfaces target algorithm for the cancelled attempt",
			taskStatus:     distributedtask.TaskStatusCancelled,
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
					MigrationType: db.ReindexTypeRepairSearchable,
					Collection:    "C",
					Properties:    []string{"foo"},
				},
				map[string]*distributedtask.Unit{
					"u1": {ID: "u1", Progress: tt.progress},
				},
			)

			idx := &models.IndexStatus{Type: "searchable", Status: "ready"}
			mergeReindexStatus(idx, "C", "foo", "searchable", false, tasksMap(task), nil)

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
			mergeReindexStatus(idx, "C", "foo", tt.indexType, false, tasksMap(task), nil)

			require.Empty(t, idx.TargetAlgorithm,
				"%s must not set TargetAlgorithm — algorithm is a searchable-only concept", tt.migrationType)
		})
	}
}
