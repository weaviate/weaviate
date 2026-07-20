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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// TestReindexPropsOverlap pins the property-overlap rule that
// typesConflictReason depends on: empty sets mean "all properties"
// and overlap with everything; non-empty sets overlap iff they share
// at least one element.
func TestReindexPropsOverlap(t *testing.T) {
	tests := []struct {
		name string
		a, b []string
		want bool
	}{
		{name: "both empty → overlap (both target all)", a: nil, b: nil, want: true},
		{name: "a empty → overlap (a targets all)", a: nil, b: []string{"p"}, want: true},
		{name: "b empty → overlap (b targets all)", a: []string{"p"}, b: nil, want: true},
		{name: "disjoint → no overlap", a: []string{"p"}, b: []string{"q"}, want: false},
		{name: "single shared → overlap", a: []string{"p"}, b: []string{"p"}, want: true},
		{name: "many disjoint → no overlap", a: []string{"a", "b", "c"}, b: []string{"x", "y"}, want: false},
		{name: "many one shared → overlap", a: []string{"a", "b", "c"}, b: []string{"c", "d"}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ReindexPropsOverlap(tc.a, tc.b))
		})
	}
}

// TestTypesConflictReason pins the migration-pair conflict rule.
// Same-type same-prop → conflict reason mentions "already running".
// Different-type same-prop → conflict reason mentions both types.
// Different-prop → empty (no conflict).
func TestTypesConflictReason(t *testing.T) {
	tests := []struct {
		name           string
		newType        ReindexMigrationType
		newProps       []string
		existType      ReindexMigrationType
		existProps     []string
		wantNonEmpty   bool
		wantSubstrings []string
	}{
		{
			name:         "same type same prop → conflict",
			newType:      ReindexTypeChangeTokenization,
			newProps:     []string{"text"},
			existType:    ReindexTypeChangeTokenization,
			existProps:   []string{"text"},
			wantNonEmpty: true,
			wantSubstrings: []string{
				"already running",
				"change-tokenization",
			},
		},
		{
			name:         "different type same prop → conflict (parallel-migration bug)",
			newType:      ReindexTypeEnableRangeable,
			newProps:     []string{"num"},
			existType:    ReindexTypeEnableFilterable,
			existProps:   []string{"num"},
			wantNonEmpty: true,
			wantSubstrings: []string{
				"already running",
				"enable-filterable",
				"enable-rangeable",
			},
		},
		{
			name:         "different prop → no conflict",
			newType:      ReindexTypeChangeTokenization,
			newProps:     []string{"text"},
			existType:    ReindexTypeChangeTokenization,
			existProps:   []string{"other"},
			wantNonEmpty: false,
		},
		{
			name:         "empty new props (all) vs single existing → conflict",
			newType:      ReindexTypeChangeAlgorithm,
			newProps:     nil,
			existType:    ReindexTypeChangeTokenization,
			existProps:   []string{"text"},
			wantNonEmpty: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := typesConflictReason(tc.newType, tc.newProps, tc.existType, tc.existProps)
			if !tc.wantNonEmpty {
				require.Empty(t, got)
				return
			}
			require.NotEmpty(t, got)
			for _, sub := range tc.wantSubstrings {
				require.Contains(t, got, sub)
			}
		})
	}
}

// TestTouchesBucket_RebuildSearchableIsExhaustive pins rebuild-searchable's
// presence in both bucket-touch switches; a missing case panics via default.
func TestTouchesBucket_RebuildSearchableIsExhaustive(t *testing.T) {
	require.NotPanics(t, func() {
		require.True(t, TouchesSearchable(ReindexTypeRebuildSearchable))
		require.False(t, TouchesFilterable(ReindexTypeRebuildSearchable))
	})
}

// TestTypesConflictReason_RebuildSearchableDoesNotPanic pins that an
// overlapping rebuild-searchable task yields a conflict reason, not a panic.
func TestTypesConflictReason_RebuildSearchableDoesNotPanic(t *testing.T) {
	var reason string
	require.NotPanics(t, func() {
		reason = typesConflictReason(
			ReindexTypeRebuildSearchable, []string{"text"},
			ReindexTypeChangeTokenization, []string{"text"},
		)
	})
	require.NotEmpty(t, reason, "overlapping in-flight task must yield a conflict reason")
}

// TestSearchablePropertyBlockmaxFromRAFT pins the RAFT-only derivation:
// class flag OR a FINISHED blockmax-producing task on this property — nothing
// else (other statuses, migration types, properties, or collections) counts.
func TestSearchablePropertyBlockmaxFromRAFT(t *testing.T) {
	task := func(collection string, mt ReindexMigrationType, status distributedtask.TaskStatus, props ...string) *distributedtask.Task {
		payload, err := json.Marshal(ReindexTaskPayload{Collection: collection, MigrationType: mt, Properties: props})
		require.NoError(t, err)
		return &distributedtask.Task{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: string(mt) + ":" + status.String()},
			Status:         status,
			Payload:        payload,
		}
	}

	tests := []struct {
		name      string
		classFlag bool
		tasks     []*distributedtask.Task
		want      bool
	}{
		{name: "class flag on, no task → blockmax", classFlag: true, want: true},
		{name: "class flag off, no task → WAND", classFlag: false, want: false},
		{
			name:  "finished change-algorithm on P → blockmax",
			tasks: []*distributedtask.Task{task("C", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusFinished, "text")},
			want:  true,
		},
		{
			name:  "finished enable-searchable on P → blockmax (created as blockmax)",
			tasks: []*distributedtask.Task{task("C", ReindexTypeEnableSearchable, distributedtask.TaskStatusFinished, "text")},
			want:  true,
		},
		{
			name:  "finished rebuild-searchable on P → blockmax",
			tasks: []*distributedtask.Task{task("C", ReindexTypeRebuildSearchable, distributedtask.TaskStatusFinished, "text")},
			want:  true,
		},
		{
			name:  "finished change-tokenization on P → not blockmax (strategy preserved)",
			tasks: []*distributedtask.Task{task("C", ReindexTypeChangeTokenization, distributedtask.TaskStatusFinished, "text")},
			want:  false,
		},
		{
			name:  "started change-algorithm on P → not yet blockmax",
			tasks: []*distributedtask.Task{task("C", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusStarted, "text")},
			want:  false,
		},
		{
			name:  "swapping change-algorithm on P → not yet blockmax",
			tasks: []*distributedtask.Task{task("C", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusSwapping, "text")},
			want:  false,
		},
		{
			name:  "failed change-algorithm on P → not blockmax",
			tasks: []*distributedtask.Task{task("C", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusFailed, "text")},
			want:  false,
		},
		{
			name:  "cancelled change-algorithm on P → not blockmax",
			tasks: []*distributedtask.Task{task("C", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusCancelled, "text")},
			want:  false,
		},
		{
			name:  "finished change-algorithm on a different property → P still WAND",
			tasks: []*distributedtask.Task{task("C", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusFinished, "other")},
			want:  false,
		},
		{
			name:  "finished change-algorithm on a different collection → P still WAND",
			tasks: []*distributedtask.Task{task("Other", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusFinished, "text")},
			want:  false,
		},
		{
			name:  "collection match is case-insensitive",
			tasks: []*distributedtask.Task{task("c", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusFinished, "text")},
			want:  true,
		},
		{
			name: "class flag on wins even with a failed task present",
			tasks: []*distributedtask.Task{
				task("C", ReindexTypeChangeAlgorithm, distributedtask.TaskStatusFailed, "text"),
			},
			classFlag: true,
			want:      true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SearchablePropertyBlockmaxFromRAFT(tc.classFlag, "C", "text", tc.tasks)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestSearchablePropertyIsBlockmax_TruthTable pins the stamp-first resolver over
// the full (classFlag, stamp, taskList) space. The load-bearing row is the one
// both QA rounds missed: a stamped-blockmax property with the class flag OFF and
// an EMPTY (aged-out) task list must resolve to blockmax — the legacy
// derivation returned WAND there, silently corrupting BM25 on the next
// change-tokenization.
func TestSearchablePropertyIsBlockmax_TruthTable(t *testing.T) {
	finishedBlockmaxTask := func() []*distributedtask.Task {
		payload, err := json.Marshal(ReindexTaskPayload{
			Collection: "C", MigrationType: ReindexTypeChangeAlgorithm, Properties: []string{"text"},
		})
		require.NoError(t, err)
		return []*distributedtask.Task{{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "t"},
			Status:         distributedtask.TaskStatusFinished,
			Payload:        payload,
		}}
	}
	ptr := func(b bool) *bool { return &b }

	tests := []struct {
		name      string
		classFlag bool
		stamp     *bool
		tasks     []*distributedtask.Task
		want      bool
	}{
		{name: "stamp true, class flag off, aged-out (empty) task list → blockmax [the fix]", classFlag: false, stamp: ptr(true), tasks: nil, want: true},
		{name: "stamp true, class flag off, unrelated live tasks → blockmax", classFlag: false, stamp: ptr(true), tasks: finishedBlockmaxTask(), want: true},
		{name: "stamp false, class flag off → WAND", classFlag: false, stamp: ptr(false), tasks: nil, want: false},
		{name: "stamp false, class flag on → stamp-first wins (unreachable in prod; stamp is only ever written true)", classFlag: true, stamp: ptr(false), tasks: nil, want: false},
		{name: "nil stamp, class flag on → blockmax (nil-backfill / all-migrated + snapshot from old node)", classFlag: true, stamp: nil, tasks: nil, want: true},
		{name: "nil stamp, class flag off, finished blockmax task → blockmax (legacy fallback, task still in TTL window)", classFlag: false, stamp: nil, tasks: finishedBlockmaxTask(), want: true},
		{name: "nil stamp, class flag off, empty task list → WAND (legacy hole; reachable only for pre-stamp migrations, closed by read-repair)", classFlag: false, stamp: nil, tasks: nil, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			class := &models.Class{
				Class:               "C",
				InvertedIndexConfig: &models.InvertedIndexConfig{UsingBlockMaxWAND: tc.classFlag},
				Properties:          []*models.Property{{Name: "text", SearchableBlockmax: tc.stamp}},
			}
			require.Equal(t, tc.want, SearchablePropertyIsBlockmax(class, "text", tc.tasks))
		})
	}
}

// TestProducesBlockmaxSearchable_FailLoud pins the exhaustive switch: known
// blockmax-producing types return true, known non-blockmax types return false,
// and a newly-added (unlisted) type PANICS rather than being silently read as
// WAND.
func TestProducesBlockmaxSearchable_FailLoud(t *testing.T) {
	for _, mt := range []ReindexMigrationType{
		ReindexTypeChangeAlgorithm, ReindexTypeEnableSearchable, ReindexTypeRebuildSearchable,
	} {
		require.True(t, producesBlockmaxSearchable(mt), "%s should produce blockmax", mt)
	}
	for _, mt := range []ReindexMigrationType{
		ReindexTypeChangeTokenization, ReindexTypeChangeTokenizationFilterable,
		ReindexTypeRepairFilterable, ReindexTypeEnableFilterable,
		ReindexTypeEnableRangeable, ReindexTypeRepairRangeable,
	} {
		require.False(t, producesBlockmaxSearchable(mt), "%s should not produce blockmax", mt)
	}
	require.Panics(t, func() { producesBlockmaxSearchable(ReindexMigrationType("bogus-future-type")) })
}

// TestCheckConflict_AcceptsNonOverlapping pins the happy path:
// CheckConflict returns nil when the new payload doesn't overlap with
// any STARTED existing task.
func TestCheckConflict_AcceptsNonOverlapping(t *testing.T) {
	provider := &ReindexProvider{}

	newP := ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"newProp"},
	}
	newPayload, _ := json.Marshal(newP)

	existP := ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"otherProp"},
	}
	existPayload, _ := json.Marshal(existP)

	existing := []*distributedtask.Task{
		{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        existPayload,
		},
	}

	require.NoError(t, provider.CheckConflict(newPayload, existing))
}

// TestCheckConflict_RejectsParallelOnSameProp pins the
// parallel-migration bug (weaviate/0-weaviate-issues#54): two different
// migration types on the same property must be rejected throughout the
// in-flight window — STARTED, PREPARING, or SWAPPING — because each of
// these states leaves on-disk migration state that a parallel migration
// would race on.
func TestCheckConflict_RejectsParallelOnSameProp(t *testing.T) {
	provider := &ReindexProvider{}

	newP := ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeEnableRangeable,
		Properties:    []string{"num"},
	}
	newPayload, _ := json.Marshal(newP)

	existP := ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"num"},
	}
	existPayload, _ := json.Marshal(existP)

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
	} {
		t.Run(string(status), func(t *testing.T) {
			existing := []*distributedtask.Task{
				{
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
					Status:         status,
					Payload:        existPayload,
				},
			}
			err := provider.CheckConflict(newPayload, existing)
			require.Error(t, err)
			require.Contains(t, err.Error(), "conflicts")
			require.Contains(t, err.Error(), "enable-filterable")
			require.Contains(t, err.Error(), "enable-rangeable")
		})
	}
}

// TestCheckConflict_IgnoresNonStartedTasks pins that FINISHED / FAILED
// / CANCELLED tasks are not consulted — only STARTED tasks can
// conflict.
func TestCheckConflict_IgnoresNonStartedTasks(t *testing.T) {
	provider := &ReindexProvider{}

	newP := ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"text"},
	}
	newPayload, _ := json.Marshal(newP)

	existP := ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"text"},
	}
	existPayload, _ := json.Marshal(existP)

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusFinished,
		distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
	} {
		existing := []*distributedtask.Task{
			{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
				Status:         status,
				Payload:        existPayload,
			},
		}
		require.NoError(t, provider.CheckConflict(newPayload, existing),
			"status=%s must NOT block a new task — only STARTED tasks conflict", status)
	}
}

// TestCheckConflict_IgnoresDifferentCollection pins that conflicts are
// scoped to (collection, property) — same property name in two
// different collections is not a conflict.
func TestCheckConflict_IgnoresDifferentCollection(t *testing.T) {
	provider := &ReindexProvider{}

	newP := ReindexTaskPayload{
		Collection:    "CollectionA",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"text"},
	}
	newPayload, _ := json.Marshal(newP)

	existP := ReindexTaskPayload{
		Collection:    "CollectionB",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"text"},
	}
	existPayload, _ := json.Marshal(existP)

	existing := []*distributedtask.Task{
		{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        existPayload,
		},
	}

	require.NoError(t, provider.CheckConflict(newPayload, existing))
}

// TestCheckConflict_CaseInsensitiveCollection pins that the collection
// match is case-insensitive — Weaviate's internal lookups
// case-fold class names, so a parallel submit on the same property of
// the same class with different casing must still be rejected.
func TestCheckConflict_CaseInsensitiveCollection(t *testing.T) {
	provider := &ReindexProvider{}

	newP := ReindexTaskPayload{
		Collection:    "MyCollection",
		MigrationType: ReindexTypeEnableRangeable,
		Properties:    []string{"num"},
	}
	newPayload, _ := json.Marshal(newP)

	existP := ReindexTaskPayload{
		Collection:    "mycollection",
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"num"},
	}
	existPayload, _ := json.Marshal(existP)

	existing := []*distributedtask.Task{
		{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        existPayload,
		},
	}

	err := provider.CheckConflict(newPayload, existing)
	require.Error(t, err)
}

// TestCheckConflict_UnparseableNewPayload pins that a corrupted new
// payload is rejected up-front rather than silently allowed through.
func TestCheckConflict_UnparseableNewPayload(t *testing.T) {
	provider := &ReindexProvider{}
	err := provider.CheckConflict([]byte("not json"), nil)
	require.Error(t, err)
}

// TestCheckConflict_UnparseableExistingPayloadRejects pins that a
// running task with an unparseable payload causes the new submit to
// be rejected. The safer choice — we cannot prove non-conflict, so
// refuse rather than allow a race.
func TestCheckConflict_UnparseableExistingPayloadRejects(t *testing.T) {
	provider := &ReindexProvider{}

	newP := ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"text"},
	}
	newPayload, _ := json.Marshal(newP)

	existing := []*distributedtask.Task{
		{
			TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
			Status:         distributedtask.TaskStatusStarted,
			Payload:        []byte("garbage"),
		},
	}

	err := provider.CheckConflict(newPayload, existing)
	require.Error(t, err)
	require.Contains(t, err.Error(), "T1")
	require.Contains(t, err.Error(), "unparseable")
}

// TestCheckPropertyUpdate_NoInFlightTasksAllows pins the empty-state
// behavior: with no tasks (or no STARTED/FINALIZING tasks) on the
// target property, CheckPropertyUpdate must return nil so external
// schema mutations are not spuriously rejected.
func TestCheckPropertyUpdate_NoInFlightTasksAllows(t *testing.T) {
	provider := &ReindexProvider{}

	require.NoError(t, provider.CheckPropertyUpdate("C", "name", nil))

	// FINISHED / FAILED / CANCELLED in the task list also must not block.
	terminalPayload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})
	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusFinished,
		distributedtask.TaskStatusFailed,
		distributedtask.TaskStatusCancelled,
	} {
		t.Run(string(status), func(t *testing.T) {
			tasks := []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
				Status:         status,
				Payload:        terminalPayload,
			}}
			require.NoError(t, provider.CheckPropertyUpdate("C", "name", tasks))
		})
	}
}

// TestCheckPropertyUpdate_InFlightOnSamePropertyRejects pins the
// load-bearing case (https://github.com/weaviate/0-weaviate-issues/issues/218): a STARTED or FINALIZING
// reindex task on the same (collection, property) must reject every
// external property mutation. Without this, a DELETE-searchable mid-
// migration wipes the in-flight searchable_retokenize working dir and
// produces a torn filterable bucket.
func TestCheckPropertyUpdate_InFlightOnSamePropertyRejects(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
	} {
		t.Run(string(status), func(t *testing.T) {
			tasks := []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_change_tok", Version: 1},
				Status:         status,
				Payload:        payload,
			}}
			err := provider.CheckPropertyUpdate("C", "name", tasks)
			require.Error(t, err)
			require.Contains(t, err.Error(), "T_change_tok")
			require.Contains(t, err.Error(), "change-tokenization")
			require.Contains(t, err.Error(), "C")
			require.Contains(t, err.Error(), "name")
			require.Contains(t, err.Error(), string(status))
		})
	}
}

// TestCheckPropertyUpdate_DifferentPropertyAllows pins the per-property
// scope: an in-flight reindex on property "name" must not block schema
// mutations on a different property "category" in the same collection.
// Without this the guard would block legitimate parallel schema work.
func TestCheckPropertyUpdate_DifferentPropertyAllows(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})

	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_change_tok", Version: 1},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        payload,
	}}

	require.NoError(t, provider.CheckPropertyUpdate("C", "category", tasks))
	require.NoError(t, provider.CheckPropertyUpdate("C", "title", tasks))
}

// TestCheckPropertyUpdate_DifferentCollectionAllows pins the per-
// collection scope: an in-flight reindex on collection "A" must not
// block schema mutations on collection "B" — they share no on-disk
// state.
func TestCheckPropertyUpdate_DifferentCollectionAllows(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "A",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})

	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_change_tok", Version: 1},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        payload,
	}}

	require.NoError(t, provider.CheckPropertyUpdate("B", "name", tasks))
}

// TestCheckPropertyUpdate_EveryMigrationTypeRejects walks every reindex
// type that can be in flight (per ReindexTypeChangeTokenization etc.)
// and confirms the guard rejects an external update on the same
// property. This is the "blanket policy" guarantee — once any reindex
// is in flight, no schema mutation on that property is allowed.
//
// Symmetry test for the matrix QA Claude is enumerating; failure of any
// row here means the corresponding combination in the QA matrix would
// pass through to the bucket↔schema inversion path.
func TestCheckPropertyUpdate_EveryMigrationTypeRejects(t *testing.T) {
	migrationTypes := []ReindexMigrationType{
		ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeEnableRangeable,
		ReindexTypeChangeAlgorithm,
		ReindexTypeRepairFilterable,
		ReindexTypeRepairRangeable,
	}

	provider := &ReindexProvider{}

	for _, mt := range migrationTypes {
		t.Run(string(mt), func(t *testing.T) {
			payload, _ := json.Marshal(ReindexTaskPayload{
				Collection:    "C",
				MigrationType: mt,
				Properties:    []string{"name"},
			})
			tasks := []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T", Version: 1},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        payload,
			}}
			err := provider.CheckPropertyUpdate("C", "name", tasks)
			require.Error(t, err, "migration type %s in flight on name must block schema mutations on name", mt)
			require.Contains(t, err.Error(), string(mt))
		})
	}
}

// TestCheckPropertyUpdate_EmptyPropertiesPayloadIsWildcard pins that
// an in-flight task with empty Properties (the reserved "all
// properties" / whole-collection rebuild) blocks every property in
// that collection. Mirrors the wildcard semantics in ReindexPropsOverlap.
func TestCheckPropertyUpdate_EmptyPropertiesPayloadIsWildcard(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		// Empty Properties → wildcard ("all properties").
		Properties: nil,
	})

	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_wildcard", Version: 1},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        payload,
	}}

	for _, prop := range []string{"name", "category", "title", "description"} {
		t.Run(prop, func(t *testing.T) {
			err := provider.CheckPropertyUpdate("C", prop, tasks)
			require.Error(t, err)
			require.Contains(t, err.Error(), "T_wildcard")
		})
	}
}

// TestCheckPropertyUpdate_UnparseablePayloadIsHardReject pins the
// epistemic safety: an in-flight task with a payload we can't decode
// (older binary, schema drift) cannot prove non-conflict, so the
// guard refuses the mutation rather than silently allow it through.
// Mirrors the same convention in CheckConflict above.
func TestCheckPropertyUpdate_UnparseablePayloadIsHardReject(t *testing.T) {
	provider := &ReindexProvider{}

	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_garbage", Version: 1},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        []byte("garbage"),
	}}

	err := provider.CheckPropertyUpdate("C", "name", tasks)
	require.Error(t, err)
	require.Contains(t, err.Error(), "T_garbage")
	require.Contains(t, err.Error(), "unparseable")
}

// TestCheckClassMutation_* pin the class-wide guard
// (DeleteClass family; https://github.com/weaviate/0-weaviate-issues/issues/219). Stricter than
// CheckPropertyUpdate — any in-flight reindex on the class is a
// conflict, regardless of which property the migration targets.

func TestCheckClassMutation_NoInFlightTasksAllows(t *testing.T) {
	provider := &ReindexProvider{}
	require.NoError(t, provider.CheckClassMutation("C", nil))

	terminalPayload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})
	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_finished", Version: 1},
		Status:         distributedtask.TaskStatusFinished,
		Payload:        terminalPayload,
	}}
	require.NoError(t, provider.CheckClassMutation("C", tasks),
		"FINISHED tasks must not block DeleteClass")
}

func TestCheckClassMutation_InFlightOnSameClassRejects(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		// Migration is on "name" but DeleteClass is class-wide, so
		// any property in flight blocks the mutation.
		Properties: []string{"name"},
	})

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
	} {
		t.Run(string(status), func(t *testing.T) {
			tasks := []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_class", Version: 1},
				Status:         status,
				Payload:        payload,
			}}
			err := provider.CheckClassMutation("C", tasks)
			require.Error(t, err)
			require.Contains(t, err.Error(), "T_class")
			require.Contains(t, err.Error(), "bucket↔schema inversion")
		})
	}
}

func TestCheckClassMutation_DifferentClassAllows(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "A",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})

	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_class", Version: 1},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        payload,
	}}

	require.NoError(t, provider.CheckClassMutation("B", tasks),
		"in-flight reindex on class A must not block DeleteClass on class B")
}

func TestCheckClassMutation_UnparseablePayloadIsHardReject(t *testing.T) {
	provider := &ReindexProvider{}
	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_garbage", Version: 1},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        []byte("garbage"),
	}}
	err := provider.CheckClassMutation("C", tasks)
	require.Error(t, err)
	require.Contains(t, err.Error(), "T_garbage")
	require.Contains(t, err.Error(), "unparseable")
}

// TestCheckTenantMutation_* pin the tenant-level guard
// (DeleteTenants / UpdateTenants-away-from-ACTIVE).

func TestCheckTenantMutation_NoInFlightTasksAllows(t *testing.T) {
	provider := &ReindexProvider{}
	require.NoError(t, provider.CheckTenantMutation("C", []string{"t1"}, nil))
}

func TestCheckTenantMutation_InFlightOnSameClassRejects(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})

	for _, status := range []distributedtask.TaskStatus{
		distributedtask.TaskStatusStarted,
		distributedtask.TaskStatusPreparing,
		distributedtask.TaskStatusSwapping,
	} {
		t.Run(string(status), func(t *testing.T) {
			tasks := []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_tenant", Version: 1},
				Status:         status,
				Payload:        payload,
			}}
			err := provider.CheckTenantMutation("C", []string{"t1", "t2"}, tasks)
			require.Error(t, err)
			require.Contains(t, err.Error(), "T_tenant")
			require.Contains(t, err.Error(), "[t1 t2]",
				"error must name the tenants being mutated so the operator knows the blast radius")
		})
	}
}

func TestCheckTenantMutation_DifferentClassAllows(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "A",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})
	tasks := []*distributedtask.Task{{
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_tenant", Version: 1},
		Status:         distributedtask.TaskStatusStarted,
		Payload:        payload,
	}}

	require.NoError(t, provider.CheckTenantMutation("B", []string{"t1"}, tasks),
		"in-flight reindex on class A must not block tenant mutation on class B")
}

// TestCheckPropertyUpdate_EmptyMigrationTypeOrCollectionRejects pins
// that informationally-empty payloads (Collection or MigrationType
// missing post-unmarshal) trigger the same hard-reject as unparseable
// payloads. Most realistic cause: an older binary wrote a payload
// shape we no longer recognize and the missing fields dropped to zero
// values during Unmarshal.
func TestCheckPropertyUpdate_EmptyMigrationTypeOrCollectionRejects(t *testing.T) {
	provider := &ReindexProvider{}

	tests := []struct {
		name    string
		payload ReindexTaskPayload
	}{
		{
			name: "empty Collection",
			payload: ReindexTaskPayload{
				MigrationType: ReindexTypeChangeTokenization,
				Properties:    []string{"name"},
			},
		},
		{
			name: "empty MigrationType",
			payload: ReindexTaskPayload{
				Collection: "C",
				Properties: []string{"name"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := json.Marshal(tc.payload)
			tasks := []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_empty", Version: 1},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        b,
			}}
			err := provider.CheckPropertyUpdate("C", "name", tasks)
			require.Error(t, err)
			require.Contains(t, err.Error(), "T_empty")
		})
	}
}
