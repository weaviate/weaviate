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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
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
		Collection:         "C",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "word",
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
	require.Contains(t, err.Error(), "unparseable")
	// An unreadable payload also hides which collection (and namespace) the
	// task belongs to, so its ID must not reach a caller who may not be
	// entitled to see it.
	require.NotContains(t, err.Error(), "T_garbage")
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
			require.Contains(t, err.Error(),
				"deleting this class would abort the migration on every replica")
		})
	}
}

// The tenant gate must name the consequence for the migration type in
// flight, not always claim a schema inversion — tenants outside the
// mutation keep whatever state the abort leaves behind.
//
// The class gate names no per-type consequence at all: DeleteClass takes
// the data with it, so there is no surviving state to describe.
func TestClassAndTenantGatesNameTheConsequenceOfTheTypeInFlight(t *testing.T) {
	cases := []struct {
		migrationType ReindexMigrationType
		wantInTenant  string
		notInTenant   []string
	}{
		{
			migrationType: ReindexTypeChangeTokenization,
			wantInTenant:  "bucket↔schema inversion",
			notInTenant:   []string{"half-applied", "cannot name"},
		},
		{
			migrationType: ReindexTypeRepairFilterable,
			wantInTenant:  "half-applied",
			notInTenant:   []string{"bucket↔schema inversion", "cannot name"},
		},
		{
			// IsSemanticMigration is a positive allowlist, so without its own
			// arm a newer node's type would claim the format-only cost.
			migrationType: "a-type-from-a-newer-node",
			wantInTenant:  "have a consequence this build cannot name",
			notInTenant:   []string{"half-applied", "bucket↔schema inversion"},
		},
	}

	provider := &ReindexProvider{}

	for _, tc := range cases {
		t.Run(string(tc.migrationType), func(t *testing.T) {
			payload, _ := json.Marshal(ReindexTaskPayload{
				Collection:    "C",
				MigrationType: tc.migrationType,
				Properties:    []string{"name"},
			})
			tasks := []*distributedtask.Task{{
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_consequence", Version: 1},
				Status:         distributedtask.TaskStatusStarted,
				Payload:        payload,
			}}

			tenantErr := provider.CheckTenantMutation("C", []string{"t1"}, tasks)
			require.Error(t, tenantErr)
			require.Contains(t, tenantErr.Error(), tc.wantInTenant)
			for _, unwanted := range tc.notInTenant {
				require.NotContains(t, tenantErr.Error(), unwanted)
			}

			classErr := provider.CheckClassMutation("C", tasks)
			require.Error(t, classErr)
			require.Contains(t, classErr.Error(),
				"the interrupted migration's partial state is removed with "+
					"the class, so nothing is left to repair")
			// A consequence for surviving data would be a false
			// counterfactual on a gate whose mutation removes the data.
			for _, unwanted := range []string{
				"bucket↔schema inversion", "half-applied", "cannot name",
			} {
				require.NotContains(t, classErr.Error(), unwanted)
			}
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
			require.Contains(t, err.Error(), "empty Collection or MigrationType")
			// Unattributable task, so its ID stays out of the message.
			require.NotContains(t, err.Error(), "T_empty")
		})
	}
}

// unknownFutureStatus simulates a status a newer node introduced that
// this build doesn't recognize. Must never become a real status name.
const unknownFutureStatus distributedtask.TaskStatus = "UNKNOWN_FUTURE_STATE"

// Pins: all four schema-mutation guards block on every non-terminal
// status, including an unrecognized one.
func TestReindexGuards_BlockOnEveryInFlightStatus(t *testing.T) {
	provider := &ReindexProvider{}

	newPayload, err := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeEnableRangeable,
		Properties:    []string{"num"},
	})
	require.NoError(t, err)

	existPayload, err := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"num"},
	})
	require.NoError(t, err)

	guards := []struct {
		name  string
		check func(existing []*distributedtask.Task) error
	}{
		{"CheckConflict", func(e []*distributedtask.Task) error {
			return provider.CheckConflict(newPayload, e)
		}},
		{"CheckPropertyUpdate", func(e []*distributedtask.Task) error {
			return provider.CheckPropertyUpdate("C", "num", e)
		}},
		{"CheckClassMutation", func(e []*distributedtask.Task) error {
			return provider.CheckClassMutation("C", e)
		}},
		{"CheckTenantMutation", func(e []*distributedtask.Task) error {
			return provider.CheckTenantMutation("C", []string{"t1"}, e)
		}},
	}

	statuses := []struct {
		status  distributedtask.TaskStatus
		blocked bool
	}{
		{distributedtask.TaskStatusStarted, true},
		{distributedtask.TaskStatusPreparing, true},
		{distributedtask.TaskStatusSwapping, true},
		{unknownFutureStatus, true},
		{distributedtask.TaskStatus(""), true},
		{distributedtask.TaskStatusFinished, false},
		{distributedtask.TaskStatusFailed, false},
		{distributedtask.TaskStatusCancelled, false},
	}

	for _, g := range guards {
		for _, s := range statuses {
			t.Run(g.name+"/"+string(s.status), func(t *testing.T) {
				existing := []*distributedtask.Task{
					{
						TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
						Status:         s.status,
						Payload:        existPayload,
					},
				}
				err := g.check(existing)
				if !s.blocked {
					require.NoError(t, err,
						"%s must ignore a task this build knows is done", g.name)
					return
				}
				require.Error(t, err,
					"%s must block against a task this build cannot prove is done", g.name)
				require.Contains(t, err.Error(), "T1")
			})
		}
	}
}

// TestSchemaGateRemedyMatchesWhatCancelActuallyOffers pins that each schema
// gate's remedy sentence matches what cancel actually offers for that
// status: nameable vs. not, and mid-run vs. past-units cost.
func TestSchemaGateRemedyMatchesWhatCancelActuallyOffers(t *testing.T) {
	provider := &ReindexProvider{}

	payload, _ := json.Marshal(ReindexTaskPayload{
		Collection:         "C",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "word",
	})

	// findCancelTarget requires a named property, so a whole-collection
	// task (empty Properties) can't be cancelled through it.
	wholeCollectionPayload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
	})

	// On a namespace-enabled cluster the payload's Collection is stored
	// qualified, and every gate is called with the qualified name too.
	qualifiedPayload, _ := json.Marshal(ReindexTaskPayload{
		Collection:         "customer1:C",
		MigrationType:      ReindexTypeChangeTokenization,
		Properties:         []string{"name"},
		TargetTokenization: "word",
	})

	gates := schemaMutationGates(provider)

	// Every part is filled in from the task; a guessed placeholder would
	// land the operator on a 202 NO_OP.
	cancelCall := `cancel it via PUT /v1/schema/C/indexes/name {"searchable":{"cancel":true}}`
	cancelWhileRunning := []string{
		cancelCall + ", or wait for it to finish",
	}
	// That sentence is a strict prefix of the format-only one, so the rows
	// only tell the two apart by refusing its continuations.
	notFormatOnly := []string{
		"no cluster-wide cutover",
		"re-submit it via",
	}
	// A format-only migration flips no schema, so nothing it leaves behind
	// can be inverted against one.
	notInverted := []string{
		"already merged on disk",
		"the next restart promotes it",
		"re-running the migration",
	}
	// Past STARTED the remedy leads with "wait" and names the repair (the
	// original submit body, not a rebuild — see [ReindexRepairCall]).
	// PREPARING is included on purpose: the merge, not the swap, opens the
	// window where cancel no longer drops the buckets.
	cancelPastUnits := []string{
		cancelCall,
		"wait for it to finish",
		// Hedged on purpose: PREPARING is entered before runtimePrepare
		// writes merged.mig, so for the first part of it nothing is.
		"may already be merged on disk",
		"the next restart promotes it",
		`re-running the migration via PUT /v1/schema/C/indexes/name {"searchable":{"tokenization":"word"}}`,
	}
	// Same task, minus the target tokenization an older binary would not have
	// written: no repair call is nameable, so none is printed.
	noTargetPayload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})
	repairUnnameable := []string{
		cancelCall,
		"re-running the migration, which this build cannot name",
	}
	// A format-only migration has no PREPARING and no cutover, so neither
	// STARTED nor SWAPPING can promise "nothing has happened yet" — and
	// SWAPPING, which it does reach, has no schema flip to skip either.
	formatOnlyPayload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeRepairFilterable,
		Properties:    []string{"name"},
	})
	// The re-submit is rendered as a full call, not described: the reader of
	// a DeleteClass or tenant-mutation refusal is often not the submitter.
	formatOnly := []string{
		`cancel it via PUT /v1/schema/C/indexes/name {"filterable":{"cancel":true}}`,
		"its shards commit one by one",
		`re-submit it via PUT /v1/schema/C/indexes/name {"filterable":{"rebuild":true}}`,
		"re-runs every shard, the ones that already finished included",
	}
	// enable-rangeable is the one format-only type whose own progress
	// invalidates its submit precondition, so "re-submit the same request" is
	// not the whole story for it.
	rangeablePayload, _ := json.Marshal(ReindexTaskPayload{
		Collection:    "C",
		MigrationType: ReindexTypeEnableRangeable,
		Properties:    []string{"name"},
	})
	formatOnlyRangeable := []string{
		`cancel it via PUT /v1/schema/C/indexes/name {"rangeable":{"cancel":true}}`,
		"its shards commit one by one",
		`re-submit it via PUT /v1/schema/C/indexes/name {"rangeable":{"enabled":true}} while no shard has finished yet`,
		`or via PUT /v1/schema/C/indexes/name {"rangeable":{"rebuild":true}} once one has (both need RUNTIME_REINDEX_ENABLED=true, unlike cancel)`,
		"sets indexRangeFilters on the property",
	}
	// The types whose preconditions partial progress leaves alone must not
	// borrow enable-rangeable's caveat.
	notPerShardSchemaFlip := []string{
		"while no shard has finished yet",
		"sets indexRangeFilters on the property",
	}
	cancelUnnameable := []string{
		"the cancel endpoint is keyed on one collection, property and index type",
		"so this build can only tell you to wait it out",
		"if a newer node submitted this migration type, read the task there instead",
	}
	cancelUnknown := []string{
		"this build does not know that status",
		"read the task on a node that knows the status",
	}
	// Data-dropping gates get told to cancel and retry, not a follow-up
	// call that would 404 once the collection/tenants are gone.
	noFollowUp := []string{
		"re-submit it via",
		"re-running the migration",
	}
	cancelPastUnitsDropping := []string{
		cancelCall,
		"wait for it to finish",
		"may already be merged on disk",
		"goes with the data you are removing",
		"then re-issue this request",
	}
	formatOnlyDropping := []string{
		`cancel it via PUT /v1/schema/C/indexes/name {"filterable":{"cancel":true}}`,
		"its shards commit one by one",
		"nothing to finish afterwards",
		"then re-issue this request",
	}
	formatOnlyRangeableDropping := []string{
		`cancel it via PUT /v1/schema/C/indexes/name {"rangeable":{"cancel":true}}`,
		"its shards commit one by one",
		"nothing to finish afterwards",
	}

	statuses := []struct {
		name      string
		status    distributedtask.TaskStatus
		className string
		payload   []byte
		want      []string
		notWant   []string
		// wantDropping / notWantDropping replace want / notWant on data-dropping
		// gates; nil means the remedy doesn't depend on that.
		wantDropping    []string
		notWantDropping []string
	}{
		{
			"STARTED", distributedtask.TaskStatusStarted, "C", payload,
			cancelWhileRunning, concat(cancelUnnameable, cancelUnknown, notFormatOnly), nil, nil,
		},
		{
			"PREPARING", distributedtask.TaskStatusPreparing, "C", payload,
			cancelPastUnits, concat(cancelUnnameable, cancelUnknown, notFormatOnly),
			cancelPastUnitsDropping, concat(cancelUnnameable, cancelUnknown, notFormatOnly, noFollowUp),
		},
		{
			"SWAPPING", distributedtask.TaskStatusSwapping, "C", payload,
			cancelPastUnits, concat(cancelUnnameable, cancelUnknown, notFormatOnly),
			cancelPastUnitsDropping, concat(cancelUnnameable, cancelUnknown, notFormatOnly, noFollowUp),
		},
		{
			"SWAPPING without a target tokenization", distributedtask.TaskStatusSwapping, "C", noTargetPayload,
			repairUnnameable, concat(cancelUnnameable, cancelUnknown),
			cancelPastUnitsDropping, concat(cancelUnnameable, cancelUnknown, noFollowUp),
		},
		{
			"STARTED format-only", distributedtask.TaskStatusStarted, "C", formatOnlyPayload,
			formatOnly, concat(cancelUnnameable, cancelUnknown, notInverted, notPerShardSchemaFlip),
			formatOnlyDropping, concat(cancelUnnameable, cancelUnknown, notInverted, notPerShardSchemaFlip, noFollowUp),
		},
		// Format-only tasks skip PREPARING but do reach SWAPPING, where the
		// gates still see them.
		{
			"SWAPPING format-only", distributedtask.TaskStatusSwapping, "C", formatOnlyPayload,
			formatOnly, concat(cancelUnnameable, cancelUnknown, notInverted, notPerShardSchemaFlip),
			formatOnlyDropping, concat(cancelUnnameable, cancelUnknown, notInverted, notPerShardSchemaFlip, noFollowUp),
		},
		{
			"STARTED format-only enable-rangeable", distributedtask.TaskStatusStarted, "C", rangeablePayload,
			formatOnlyRangeable, concat(cancelUnnameable, cancelUnknown, notInverted),
			formatOnlyRangeableDropping, concat(cancelUnnameable, cancelUnknown, notInverted, notPerShardSchemaFlip, noFollowUp),
		},
		{
			"SWAPPING format-only enable-rangeable", distributedtask.TaskStatusSwapping, "C", rangeablePayload,
			formatOnlyRangeable, concat(cancelUnnameable, cancelUnknown, notInverted),
			formatOnlyRangeableDropping, concat(cancelUnnameable, cancelUnknown, notInverted, notPerShardSchemaFlip, noFollowUp),
		},
		{
			"STARTED whole-collection", distributedtask.TaskStatusStarted, "C", wholeCollectionPayload,
			cancelUnnameable, concat([]string{cancelCall}, cancelUnknown), nil, nil,
		},
		{
			"SWAPPING whole-collection", distributedtask.TaskStatusSwapping, "C", wholeCollectionPayload,
			cancelUnnameable, concat([]string{cancelCall}, cancelUnknown), nil, nil,
		},
		{
			string(unknownFutureStatus), unknownFutureStatus, "C", payload,
			cancelUnknown, concat([]string{cancelCall}, cancelUnnameable), nil, nil,
		},
		// Namespace-qualified: the URL keeps the prefix. A global operator has
		// to type it, and the REST error path removes it again for the
		// namespace-confined caller who must not.
		{
			"STARTED namespace-qualified", distributedtask.TaskStatusStarted, "customer1:C", qualifiedPayload,
			[]string{`cancel it via PUT /v1/schema/customer1:C/indexes/name {"searchable":{"cancel":true}}`},
			[]string{"/v1/schema/C/"},
			nil, nil,
		},
		{
			"SWAPPING namespace-qualified", distributedtask.TaskStatusSwapping, "customer1:C", qualifiedPayload,
			[]string{
				`cancel it via PUT /v1/schema/customer1:C/indexes/name {"searchable":{"cancel":true}}`,
				`re-running the migration via PUT /v1/schema/customer1:C/indexes/name {"searchable":{"tokenization":"word"}}`,
			},
			[]string{"/v1/schema/C/"},
			[]string{`cancel it via PUT /v1/schema/customer1:C/indexes/name {"searchable":{"cancel":true}}`},
			concat([]string{"/v1/schema/C/"}, noFollowUp),
		},
	}

	for _, gate := range gates {
		for _, st := range statuses {
			t.Run(gate.name+"/"+st.name, func(t *testing.T) {
				tasks := []*distributedtask.Task{{
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_remedy", Version: 1},
					Status:         st.status,
					Payload:        st.payload,
				}}
				err := gate.call(st.className, tasks)
				require.Error(t, err)
				want, notWant := st.want, st.notWant
				if gate.dropsTheData && st.wantDropping != nil {
					want, notWant = st.wantDropping, st.notWantDropping
				}
				for _, w := range want {
					require.Contains(t, err.Error(), w)
				}
				for _, unwanted := range notWant {
					require.NotContains(t, err.Error(), unwanted)
				}
			})
		}
	}
}

// schemaMutationGate is one of the three gates that refuse a schema mutation
// while a reindex task is in flight, reduced to a common call shape so a
// table can exercise all three with the same rows.
type schemaMutationGate struct {
	name string
	call func(className string, tasks []*distributedtask.Task) error
	// dropsTheData marks gates whose caller destroys the shards the
	// migration works on, changing the remedy rendered.
	dropsTheData bool
}

func schemaMutationGates(provider *ReindexProvider) []schemaMutationGate {
	return []schemaMutationGate{
		{
			name: "property update",
			call: func(className string, tasks []*distributedtask.Task) error {
				return provider.CheckPropertyUpdate(className, "name", tasks)
			},
		},
		{
			name: "delete class",
			call: func(className string, tasks []*distributedtask.Task) error {
				return provider.CheckClassMutation(className, tasks)
			},
			dropsTheData: true,
		},
		{
			name: "tenant mutation",
			call: func(className string, tasks []*distributedtask.Task) error {
				return provider.CheckTenantMutation(className, []string{"t1"}, tasks)
			},
			dropsTheData: true,
		},
	}
}

func concat(sets ...[]string) []string {
	var out []string
	for _, s := range sets {
		out = append(out, s...)
	}
	return out
}

// TestCheckConflict_EveryMigrationTypeSurvivesTheConflictCheck pins that no
// migration type crashes the conflict check, which runs on the RAFT apply
// path, so a panic there is a cluster-wide crash loop. The "not known to
// this build" row is the one that generalizes to a newer node's type during
// a rolling upgrade — exactly how rebuild-searchable once crashed this path.
func TestCheckConflict_EveryMigrationTypeSurvivesTheConflictCheck(t *testing.T) {
	migrationTypes := append(allDeclaredReindexMigrationTypes(t),
		"a-type-from-a-newer-node")

	provider := &ReindexProvider{}

	// Both sides of the check reach the predicates, so every ordered pair
	// has to survive, not just every type in the submitted position.
	for _, newType := range migrationTypes {
		for _, existType := range migrationTypes {
			t.Run(string(newType)+" while "+string(existType)+" is in flight", func(t *testing.T) {
				newPayload, err := json.Marshal(ReindexTaskPayload{
					Collection: "C", MigrationType: newType, Properties: []string{"prop"},
				})
				require.NoError(t, err)
				existPayload, err := json.Marshal(ReindexTaskPayload{
					Collection: "C", MigrationType: existType, Properties: []string{"prop"},
				})
				require.NoError(t, err)

				existing := []*distributedtask.Task{{
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "T1", Version: 1},
					Status:         distributedtask.TaskStatusStarted,
					Payload:        existPayload,
				}}

				require.NotPanics(t, func() {
					// Overlapping properties, so this is the conflict the
					// check exists for: it must refuse, not crash.
					require.Error(t, provider.CheckConflict(newPayload, existing))
				})
			})
		}
	}
}

// TestTypesConflictReason_UnknownTypeFailsClosed pins the RAFT-safe
// handling of a migration type this build does not recognize: conflict when
// the properties overlap, silence when they don't, and never a panic.
// Overlap is decided from the property sets alone, which stays correct
// whatever the types are.
func TestTypesConflictReason_UnknownTypeFailsClosed(t *testing.T) {
	const unknown = ReindexMigrationType("a-type-from-a-newer-node")

	cases := []struct {
		name         string
		newType      ReindexMigrationType
		newProps     []string
		existType    ReindexMigrationType
		existProps   []string
		wantConflict bool
	}{
		{
			name:    "unknown submitted against known, overlapping props",
			newType: unknown, newProps: []string{"p"},
			existType: ReindexTypeChangeTokenization, existProps: []string{"p"},
			wantConflict: true,
		},
		{
			name:    "known submitted against unknown in flight, overlapping props",
			newType: ReindexTypeChangeTokenization, newProps: []string{"p"},
			existType: unknown, existProps: []string{"p"},
			wantConflict: true,
		},
		{
			name:    "both unknown, overlapping props",
			newType: unknown, newProps: []string{"p"},
			existType: ReindexMigrationType("another-new-type"), existProps: []string{"p"},
			wantConflict: true,
		},
		{
			name:    "unknown but no property overlap",
			newType: unknown, newProps: []string{"a"},
			existType: ReindexTypeChangeTokenization, existProps: []string{"b"},
			wantConflict: false,
		},
		{
			// Empty props is the reserved whole-collection wildcard, so it
			// overlaps even against an unknown type.
			name:    "unknown against the whole-collection wildcard",
			newType: unknown, newProps: []string{"a"},
			existType: ReindexTypeChangeTokenization, existProps: nil,
			wantConflict: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var reason string
			require.NotPanics(t, func() {
				reason = typesConflictReason(tc.newType, tc.newProps, tc.existType, tc.existProps)
			})
			if !tc.wantConflict {
				require.Empty(t, reason)
				return
			}
			require.NotEmpty(t, reason)
			require.Contains(t, reason, "is not known to this build")
		})
	}
}

// allDeclaredReindexMigrationTypes reads the migration types straight out of
// the package source, so a new type is picked up without anyone remembering
// to extend a list here. A hand-maintained copy is what let
// rebuild-searchable reach the apply path with no mapping arm.
func allDeclaredReindexMigrationTypes(t *testing.T) []ReindexMigrationType {
	t.Helper()
	// The whole package, not just the file that holds them today: a
	// constant declared elsewhere would otherwise go uncounted and the
	// count below would still match.
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	// Also matches the grouped-const form (`ReindexTypeFoo = "foo"`, no
	// repeated type), or a type declared that way goes uncounted.
	// Value charset kept wide: a narrower one would silently skip a new
	// constant, and the count guard below would still pass at 9.
	re := regexp.MustCompile(`ReindexType\w+\s+(?:ReindexMigrationType\s+)?= "([a-zA-Z0-9_-]+)"`)
	var out []ReindexMigrationType
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		for _, m := range re.FindAllStringSubmatch(string(src), -1) {
			out = append(out, ReindexMigrationType(m[1]))
		}
	}
	// Assert each known value by name: a regex that swapped which
	// constants it matches would still pass a count-only check.
	for _, known := range []ReindexMigrationType{
		ReindexTypeEnableSearchable, ReindexTypeChangeAlgorithm,
		ReindexTypeRebuildSearchable, ReindexTypeEnableFilterable,
		ReindexTypeRepairFilterable, ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableRangeable, ReindexTypeRepairRangeable,
		ReindexTypeChangeTokenization,
	} {
		require.Contains(t, out, known,
			"the source scan missed %q; the regex no longer matches how it is declared", known)
	}
	require.Len(t, out, 9,
		"expected 9 declared migration types; update this count with the constant")
	return out
}

// TestReindexTargetIndexes pins every arm of the mapping cancel and on-disk
// cleanup rely on. The table is the full set of types this build declares, so
// a new type added without an arm here fails loud instead of silently
// returning nil and disabling cancel rendering and cleanup for it.
func TestReindexTargetIndexes(t *testing.T) {
	cases := []struct {
		migrationType ReindexMigrationType
		want          []string
	}{
		{ReindexTypeEnableSearchable, []string{"searchable"}},
		{ReindexTypeChangeAlgorithm, []string{"searchable"}},
		{ReindexTypeRebuildSearchable, []string{"searchable"}},
		{ReindexTypeEnableFilterable, []string{"filterable"}},
		{ReindexTypeRepairFilterable, []string{"filterable"}},
		{ReindexTypeChangeTokenizationFilterable, []string{"filterable"}},
		{ReindexTypeEnableRangeable, []string{"rangeable"}},
		{ReindexTypeRepairRangeable, []string{"rangeable"}},
		{ReindexTypeChangeTokenization, []string{"searchable", "filterable"}},
	}

	for _, tc := range cases {
		t.Run(string(tc.migrationType), func(t *testing.T) {
			require.Equal(t, tc.want, ReindexTargetIndexes(tc.migrationType))
		})
	}

	t.Run("the table covers every declared type", func(t *testing.T) {
		covered := map[ReindexMigrationType]bool{}
		for _, tc := range cases {
			covered[tc.migrationType] = true
		}
		for _, mt := range allDeclaredReindexMigrationTypes(t) {
			require.True(t, covered[mt],
				"migration type %q is declared but not pinned here", mt)
		}
	})

	t.Run("a type this build does not know", func(t *testing.T) {
		require.Nil(t, ReindexTargetIndexes("invent-index"))
	})
}

// TestFormatOnlyRemedyAlwaysRendersACall pins that the format-only remedy
// never prints its re-submit sentence with an empty call in it.
func TestFormatOnlyRemedyAlwaysRendersACall(t *testing.T) {
	for _, mt := range allDeclaredReindexMigrationTypes(t) {
		if IsSemanticMigration(mt) {
			continue
		}
		t.Run(string(mt), func(t *testing.T) {
			remedy := ReindexGateRemedy(distributedtask.TaskStatusStarted, ReindexTaskPayload{
				Collection:    "C",
				MigrationType: mt,
				Properties:    []string{"name"},
			}, "name", false)
			require.Contains(t, remedy, "re-submit it via PUT /v1/schema/C/indexes/name {")
			require.NotContains(t, remedy, "via  ")
		})
	}
}

// TestReindexCancelCall_OnlyRendersWhatItCanFillIn pins the rule the gate
// messages rely on: either the whole call is filled in from the task, or
// nothing is rendered. A half-filled URL costs the operator a 202 NO_OP.
func TestReindexCancelCall_OnlyRendersWhatItCanFillIn(t *testing.T) {
	cases := []struct {
		name          string
		payload       ReindexTaskPayload
		askedProperty string
		want          string
	}{
		{
			name:    "single property, known type",
			payload: ReindexTaskPayload{Collection: "C", MigrationType: ReindexTypeEnableRangeable, Properties: []string{"num"}},
			want:    `PUT /v1/schema/C/indexes/num {"rangeable":{"cancel":true}}`,
		},
		{
			name:    "type touching two indexes names one that cancels",
			payload: ReindexTaskPayload{Collection: "C", MigrationType: ReindexTypeChangeTokenization, Properties: []string{"name"}},
			want:    `PUT /v1/schema/C/indexes/name {"searchable":{"cancel":true}}`,
		},
		{
			// Cancel is task-scoped, so naming the first property cancels
			// the whole task.
			name:    "several properties, the first one cancels all of them",
			payload: ReindexTaskPayload{Collection: "C", MigrationType: ReindexTypeEnableRangeable, Properties: []string{"a", "b"}},
			want:    `PUT /v1/schema/C/indexes/a {"rangeable":{"cancel":true}}`,
		},
		{
			// Collection is stored qualified and stays qualified: a global
			// operator has to type the prefix, and the REST error path
			// strips it again for the namespace-confined caller.
			name:    "namespace-qualified collection keeps its prefix",
			payload: ReindexTaskPayload{Collection: "customer1:C", MigrationType: ReindexTypeEnableRangeable, Properties: []string{"num"}},
			want:    `PUT /v1/schema/customer1:C/indexes/num {"rangeable":{"cancel":true}}`,
		},
		{
			// The refusal is about "b", so the cancel call names "b" — a
			// call naming some other property of the same task reads like
			// a bug even though cancel is task-scoped.
			name:          "the property the caller asked about is the one named",
			payload:       ReindexTaskPayload{Collection: "C", MigrationType: ReindexTypeEnableRangeable, Properties: []string{"a", "b"}},
			askedProperty: "b",
			want:          `PUT /v1/schema/C/indexes/b {"rangeable":{"cancel":true}}`,
		},
		{
			name:          "a property the task does not carry falls back to the first",
			payload:       ReindexTaskPayload{Collection: "C", MigrationType: ReindexTypeEnableRangeable, Properties: []string{"a", "b"}},
			askedProperty: "elsewhere",
			want:          `PUT /v1/schema/C/indexes/a {"rangeable":{"cancel":true}}`,
		},
		{
			// Whole-collection rebuild: findCancelTarget requires a named
			// property, so there's nothing to render. Reserved — no
			// shipping route produces an empty Properties payload today,
			// the branch is defense in depth.
			name:    "no property to name",
			payload: ReindexTaskPayload{Collection: "C", MigrationType: ReindexTypeEnableRangeable},
			want:    "",
		},
		{
			name:    "no collection to address",
			payload: ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable, Properties: []string{"num"}},
			want:    "",
		},
		{
			name:    "migration type this build cannot map",
			payload: ReindexTaskPayload{Collection: "C", MigrationType: "invent-index", Properties: []string{"num"}},
			want:    "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, ReindexCancelCall(tc.payload, tc.askedProperty))
		})
	}
}

// An unattributable task's ID may belong to a namespace the caller cannot
// see, so every gate withholds it.
func TestReindexGuards_WithholdTheIDOfAnUnattributableTask(t *testing.T) {
	provider := &ReindexProvider{}

	emptyCollection, err := json.Marshal(ReindexTaskPayload{
		MigrationType: ReindexTypeChangeTokenization,
		Properties:    []string{"name"},
	})
	require.NoError(t, err)

	payloads := []struct {
		name    string
		payload []byte
		want    string
	}{
		{"a payload that will not parse", []byte("garbage"), "unparseable"},
		{"a payload written by an older binary", emptyCollection, "empty Collection or MigrationType"},
	}

	for _, gate := range schemaMutationGates(provider) {
		for _, p := range payloads {
			t.Run(gate.name+"/"+p.name, func(t *testing.T) {
				err := gate.call("C", []*distributedtask.Task{{
					TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_unattributable", Version: 1},
					Status:         distributedtask.TaskStatusStarted,
					Payload:        p.payload,
				}})
				require.Error(t, err)
				require.Contains(t, err.Error(), p.want)
				require.NotContains(t, err.Error(), "T_unattributable")
				require.Contains(t, err.Error(), "GET /v1/tasks",
					"withholding the ID has to leave the caller somewhere to look")
			})
		}
	}
}

// TestReindexRepairCall pins the repair body per migration type against the
// precondition a bare rebuild would fail on the terminal state.
func TestReindexRepairCall(t *testing.T) {
	cases := []struct {
		migrationType ReindexMigrationType
		// why names the precondition a bare rebuild would fail on, or why
		// no body is renderable at all.
		why string
		// want is the body, or "" when the type renders no repair call.
		want string
	}{
		{
			ReindexTypeEnableSearchable,
			"IndexSearchable is still false, which searchable.rebuild rejects; the " +
				"tokenization is part of the enable verb and the handler rejects an empty one",
			`{"searchable":{"enabled":true,"tokenization":"word"}}`,
		},
		{
			ReindexTypeEnableFilterable,
			"IndexFilterable is still false, which filterable.rebuild rejects",
			`{"filterable":{"enabled":true}}`,
		},
		{
			ReindexTypeChangeAlgorithm,
			"the algorithm is still WAND, which searchable.rebuild rejects",
			`{"searchable":{"algorithm":"blockmax"}}`,
		},
		{
			ReindexTypeChangeTokenization,
			"the tokenization is still the old one, so the same change is still valid",
			`{"searchable":{"tokenization":"word"}}`,
		},
		{
			ReindexTypeChangeTokenizationFilterable,
			"same, scoped to the filterable bucket",
			`{"filterable":{"tokenization":"word"}}`,
		},
		{
			ReindexTypeRebuildSearchable,
			"no schema bit was going to change; the original request is the repair",
			`{"searchable":{"rebuild":true}}`,
		},
		{
			ReindexTypeRepairFilterable,
			"same",
			`{"filterable":{"rebuild":true}}`,
		},
		{
			ReindexTypeEnableRangeable,
			"the strategy flips IndexRangeFilters per shard, so enabled 400s once " +
				"any shard finished and rebuild 400s while none has",
			"",
		},
		{
			ReindexTypeRepairRangeable,
			"no schema bit was going to change",
			`{"rangeable":{"rebuild":true}}`,
		},
	}

	for _, tc := range cases {
		t.Run(string(tc.migrationType), func(t *testing.T) {
			got := ReindexRepairCall(ReindexTaskPayload{
				Collection:         "C",
				MigrationType:      tc.migrationType,
				Properties:         []string{"name"},
				TargetTokenization: "word",
			}, "name")
			if tc.want == "" {
				require.Empty(t, got, tc.why)
				return
			}
			require.Equal(t, "PUT /v1/schema/C/indexes/name "+tc.want, got, tc.why)
		})
	}

	t.Run("the table covers every declared type", func(t *testing.T) {
		covered := map[ReindexMigrationType]bool{}
		for _, tc := range cases {
			covered[tc.migrationType] = true
		}
		for _, mt := range allDeclaredReindexMigrationTypes(t) {
			require.True(t, covered[mt],
				"migration type %q is declared but is not pinned here", mt)
		}
	})

	t.Run("nothing is rendered when the call cannot be filled in", func(t *testing.T) {
		unnameable := []struct {
			name    string
			payload ReindexTaskPayload
		}{
			{"a type this build does not know", ReindexTaskPayload{
				Collection: "C", MigrationType: "a-type-from-a-newer-node",
				Properties: []string{"name"},
			}},
			{"a tokenization change with no target", ReindexTaskPayload{
				Collection: "C", MigrationType: ReindexTypeChangeTokenization,
				Properties: []string{"name"},
			}},
			{"an enable-searchable with no target tokenization", ReindexTaskPayload{
				Collection: "C", MigrationType: ReindexTypeEnableSearchable,
				Properties: []string{"name"},
			}},
			{"a filterable tokenization change with no target", ReindexTaskPayload{
				Collection: "C", MigrationType: ReindexTypeChangeTokenizationFilterable,
				Properties: []string{"name"},
			}},
			{"no collection", ReindexTaskPayload{
				MigrationType: ReindexTypeEnableFilterable, Properties: []string{"name"},
			}},
			{"no property", ReindexTaskPayload{
				Collection: "C", MigrationType: ReindexTypeEnableFilterable,
			}},
		}
		for _, tc := range unnameable {
			t.Run(tc.name, func(t *testing.T) {
				require.Empty(t, ReindexRepairCall(tc.payload, "name"))
			})
		}
	})
}
