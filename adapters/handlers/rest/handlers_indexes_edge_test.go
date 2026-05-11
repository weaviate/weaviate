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

func tasksMap(tasks ...*distributedtask.Task) map[string][]*distributedtask.Task {
	return map[string][]*distributedtask.Task{
		db.ReindexNamespace: tasks,
	}
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
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task))

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
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task))

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
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task))

	require.Equal(t, "indexing", idx.Status)
	require.InDelta(t, 0.4, idx.Progress, 0.0001)
}

// Edge case 3: FAILED task. mergeReindexStatus only considers STARTED
// tasks, so a FAILED enable-filterable task produces no synthetic entry.
// In getIndexes, the filterable=false branch then drops the entry — so the
// user has no way to learn from this endpoint that a previous attempt
// failed. Demonstrates a silent-failure UX problem.
func TestMergeReindexStatus_FailedTask_NoSyntheticEntry(t *testing.T) {
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
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task))

	// Status is unchanged (still "ready"). Combined with the
	// IndexFilterable=false branch in getIndexes that drops "ready"
	// synthetic entries, this means the user never sees that an attempt
	// failed.
	require.Equal(t, "ready", idx.Status,
		"FAILED task produces no synthetic state; user can't tell from this endpoint that a build failed")
	require.Equal(t, float32(0), idx.Progress)
}

// Edge case 4: CANCELLED task. Same situation as FAILED — only STARTED is
// considered. A user who cancelled enable-filterable mid-flight cannot
// distinguish "never asked for it" from "asked for it and cancelled" via
// this endpoint.
func TestMergeReindexStatus_CancelledTask_NoSyntheticEntry(t *testing.T) {
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
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task))

	require.Equal(t, "ready", idx.Status,
		"CANCELLED task produces no synthetic state; user can't tell the build was cancelled")
}

// Edge case 5: Task moved to FINISHED but the schema flag flip
// (IndexFilterable=true) hasn't propagated yet. In real life this is the
// time gap between AllUnitsTerminal and OnGroupCompleted/OnTaskCompleted
// running. From this endpoint's perspective:
//   - schema flag is still false  → falls into the synthetic branch
//   - task.Status is FINISHED     → mergeReindexStatus skips it
//
// So the synthetic entry stays "ready" → getIndexes drops it. Net effect:
// the property's filterable index has finished building but the
// /indexes response shows nothing at all for it. From the caller's POV
// the index simply disappears for the gap window.
func TestMergeReindexStatus_FinishedBeforeSchemaFlip_DisappearsFromResponse(t *testing.T) {
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
	mergeReindexStatus(idx, "C", "foo", "filterable", tasksMap(task))

	// Status is unchanged → caller of getIndexes will drop the entry
	// (schema flag is false, idx.Status is "ready" not "indexing"/"pending").
	require.Equal(t, "ready", idx.Status)
	require.Equal(t, float32(0), idx.Progress)
}

// Edge case 6: Two overlapping STARTED tasks targeting the same property.
// One is enable-filterable (progress 0.2), the other is change-tokenization
// (progress 0.9). For indexType="filterable", both match. The current
// implementation iterates `tasks` in map-list order and `return`s on the
// first match — so which task "wins" is non-deterministic depending on
// list ordering. This test demonstrates the picking behavior in a
// controlled order: whichever task is first in the slice wins, regardless
// of which would be more informative. (In practice this should be
// impossible because checkReindexConflict rejects overlapping tasks, but
// mergeReindexStatus does not enforce that invariant.)
func TestMergeReindexStatus_OverlappingTasks_FirstInListWins(t *testing.T) {
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

	// Order A: enable first.
	idxA := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idxA, "C", "foo", "filterable", tasksMap(enableTask, changeTokTask))

	// Order B: change-tokenization first.
	idxB := &models.IndexStatus{Type: "filterable", Status: "ready"}
	mergeReindexStatus(idxB, "C", "foo", "filterable", tasksMap(changeTokTask, enableTask))

	// The same input set produces different observable results purely
	// based on slice order — neither is "wrong" per se, but the answer is
	// not deterministic across requests.
	require.InDelta(t, 0.2, idxA.Progress, 0.0001, "first-task-in-list wins (enable)")
	require.Empty(t, idxA.TargetTokenization, "enable-filterable doesn't set TargetTokenization")

	require.InDelta(t, 0.9, idxB.Progress, 0.0001, "first-task-in-list wins (change-tok)")
	require.Equal(t, "lowercase", idxB.TargetTokenization,
		"change-tokenization sets TargetTokenization")

	require.NotEqual(t, idxA.Progress, idxB.Progress,
		"same task set, different order → different progress reported")
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
	mergeReindexStatus(idx, "C", "anyprop", "filterable", tasksMap(task))

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
		mergeReindexStatus(idx, "C", propName, "searchable", tasksMap(task))
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
	mergeReindexStatus(idx, "myclass", "foo", "filterable", tasksMap(task))

	require.Equal(t, "indexing", idx.Status, "collection name match is case-insensitive")
}
