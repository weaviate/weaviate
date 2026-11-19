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
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
)

// ReindexBucketEffect is the single source of truth for which buckets a
// migration type touches (gates conflict/idempotency checks).
//
// ok is false only for an unrecognized type — forward-compat for a rolling
// upgrade observing a newer peer's task; must never panic on peer input. Such
// types fail safe: touches=true rejects a racing submit. Exhaustiveness lives in
// TestReindexBucketEffect_Exhaustive.
func ReindexBucketEffect(t ReindexMigrationType) (touchesSearchable, touchesFilterable, ok bool) {
	switch t {
	case ReindexTypeChangeAlgorithm, ReindexTypeRebuildSearchable, ReindexTypeEnableSearchable:
		return true, false, true
	case ReindexTypeChangeTokenization:
		return true, true, true
	case ReindexTypeChangeTokenizationFilterable, ReindexTypeRepairFilterable, ReindexTypeEnableFilterable:
		return false, true, true
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return false, false, true
	default:
		return true, true, false
	}
}

// SearchablePropertyIsBlockmax resolves whether (class, propName)'s searchable
// bucket is blockmax: stamp if set, else class flag. Every read site must route through this resolver.
func SearchablePropertyIsBlockmax(class *models.Class, propName string) bool {
	for _, p := range class.Properties {
		if p.Name == propName {
			if p.SearchableBlockmax != nil {
				return *p.SearchableBlockmax
			}
			break
		}
	}
	return class.InvertedIndexConfig != nil && class.InvertedIndexConfig.UsingBlockMaxWAND
}

// ReindexDetector decides whether a RAFT apply refuses a new reindex task or a
// schema mutation that overlaps an in-flight reindex. It needs no RAFT handle,
// so MakeAppState builds it before the cluster service opens RAFT.
type ReindexDetector struct{}

// CheckConflict implements [distributedtask.ConflictDetector] for the
// reindex namespace. Called under [Manager.mu] from the RAFT-apply
// AddTask path BEFORE the new task is appended to FSM-stored state.
// Returns a non-nil error iff `newPayload` would conflict with an
// already-STARTED task in `existingTasks`.
//
// FSM-determinism: every node applies the same RAFT log entry, sees
// the same `existingTasks` snapshot, and runs this same function — so
// every node reaches the same accept/reject decision. The function
// must remain a pure transform of its arguments.
//
// Conflict rule: any two reindex migrations on overlapping properties
// of the same collection conflict, regardless of which bucket type
// they primarily write to. See [typesConflictReason] for the
// rationale.
func (ReindexDetector) CheckConflict(newPayload []byte, existingTasks []*distributedtask.Task) error {
	var newP ReindexTaskPayload
	if err := json.Unmarshal(newPayload, &newP); err != nil {
		return fmt.Errorf("unmarshal new reindex payload: %w", err)
	}
	if newP.Collection == "" || newP.MigrationType == "" {
		return fmt.Errorf("new reindex payload missing Collection or MigrationType")
	}

	for _, task := range existingTasks {
		// PREPARING and SWAPPING are the subtle ones: every unit has
		// reached terminal state, but the post-completion callbacks have
		// not yet committed. A new migration on the same property could
		// land before the schema flip commits, leaving it and the
		// unfinished swap racing on the same bucket pointers.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			// Existing task has an unparseable payload. We can't prove
			// non-conflict, so reject — the alternative (silently
			// allow) would let two real migrations race on shared
			// bucket state.
			return fmt.Errorf(
				"in-flight reindex task %q has unparseable payload; cannot verify conflict",
				task.ID)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			return fmt.Errorf(
				"in-flight reindex task %q has empty Collection or MigrationType",
				task.ID)
		}
		if !strings.EqualFold(existP.Collection, newP.Collection) {
			continue
		}

		if reason := typesConflictReason(newP.MigrationType, newP.Properties,
			existP.MigrationType, existP.Properties); reason != "" {
			return fmt.Errorf("reindex task %q conflicts: %s", task.ID, reason)
		}
	}
	return nil
}

// typesConflictReason returns a non-empty reason if two reindex migrations
// on the same collection target overlapping properties, whatever their types.
// The UpdateProperty completing a semantic migration ([IsSemanticMigration])
// runs updatePropertyBuckets, which deletes the working dirs of an overlapping
// enable-* migration whose flag is still false (weaviate/weaviate#10675).
//
// Empty props means "all properties" (reserved for a future
// whole-collection rebuild) and overlaps with everything.
func typesConflictReason(newType ReindexMigrationType, newProps []string,
	existType ReindexMigrationType, existProps []string,
) string {
	// Conflict is property-overlap only; it doesn't consult bucket-touch
	// predicates, so an unknown (peer-written) type on an overlapping
	// property still conflicts without needing recognition here.
	if !ReindexPropsOverlap(newProps, existProps) {
		return ""
	}
	if newType == existType {
		return fmt.Sprintf("already running %s for overlapping properties", newType)
	}
	return fmt.Sprintf("already running %s for overlapping properties; "+
		"concurrent %s on the same property would race on shared on-disk "+
		"migration state — wait for the in-flight task to finish before "+
		"submitting another", existType, newType)
}

// TypesConflictReason is the package-public alias for typesConflictReason,
// used by the REST handlers' pre-flight conflict check. Inline so
// internal callers (CheckConflict, CheckPropertyUpdate) continue to use
// the lowercase symbol without indirection.
func TypesConflictReason(newType ReindexMigrationType, newProps []string,
	existType ReindexMigrationType, existProps []string,
) string {
	return typesConflictReason(newType, newProps, existType, existProps)
}

// ReindexPropsOverlap returns true if two property sets overlap. An
// empty set means "all properties", which overlaps with everything.
//
// Public so REST handlers can use the same predicate as the
// FSM-deterministic conflict check.
//
// Every caller acts on a match: refusing a conflicting submit, refusing
// a schema mutation, or picking the task an operator asked to cancel.
// Over-matching costs a retryable conflict error or one extra cancel
// candidate; under-matching lets a schema change race an in-flight
// migration on shared on-disk state. That asymmetry, not the literal
// reading of an empty list, is why empty means "all" here.
//
// The status endpoint reads the same field the opposite way on purpose;
// see mergeReindexStatus in adapters/handlers/rest/handlers_indexes.go.
func ReindexPropsOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for _, ap := range a {
		for _, bp := range b {
			if ap == bp {
				return true
			}
		}
	}
	return false
}

// TouchesSearchable reports whether migration type t writes to the searchable
// bucket. Thin accessor over [ReindexBucketEffect] (see there for the
// unknown-type policy).
func TouchesSearchable(t ReindexMigrationType) bool {
	touchesSearchable, _, _ := ReindexBucketEffect(t)
	return touchesSearchable
}

// TouchesFilterable reports whether migration type t writes to the filterable
// bucket. Thin accessor over [ReindexBucketEffect].
func TouchesFilterable(t ReindexMigrationType) bool {
	_, touchesFilterable, _ := ReindexBucketEffect(t)
	return touchesFilterable
}

func MutationRemedy(status distributedtask.TaskStatus, whenCancellable string) string {
	if status.IsCancellable() {
		return whenCancellable
	}
	if status == distributedtask.TaskStatusSwapping {
		return "the task is SWAPPING and ends once every replica that owes a swap has swapped, so wait for it; a node lost for good, or a swap that never succeeds, holds it"
	}
	return "this build cannot classify that status, so a cancel via the reindex REST API is " +
		"refused on every node — the task has to reach a terminal state on the nodes that do " +
		"recognize it"
}

// CheckPropertyUpdate implements [distributedtask.SchemaMutationDetector]
// for the reindex namespace: called from the schema FSM's UpdateProperty
// apply path to reject external property mutations racing an in-flight
// reindex migration on the same (collection, property).
//
// Without it, an external UpdateProperty such as DELETE /index/searchable
// runs updatePropertyBuckets, which deletes the working dirs of an in-flight
// enable-* migration whose flag is still false. Mid change-tokenization, the
// same DELETE wipes the searchable sub-task while the filterable sub-task
// commits, leaving the schema at the old tokenization and the filterable
// bucket on the new one.
//
// A migration's own flip goes through [ReindexDetector.CheckMigrationPropertyUpdate] instead.
//
// Must stay a pure function of (className, propertyName, existingTasks) for
// RAFT determinism; an unparseable payload is a hard reject, same as
// [ConflictDetector.CheckConflict].
func (ReindexDetector) CheckPropertyUpdate(className, propertyName string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		// Same in-flight semantics as CheckConflict.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			return fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; "+
					"cannot verify whether property update on %s.%s would "+
					"conflict: %w",
				task.ID, className, propertyName, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			return fmt.Errorf(
				"in-flight reindex task %q has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether property update "+
					"on %s.%s would conflict",
				task.ID, className, propertyName)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		if !ReindexPropsOverlap(existP.Properties, []string{propertyName}) {
			continue
		}
		return fmt.Errorf(
			"reindex task %q (%s) is in flight on %s.%s (status=%s); "+
				"schema mutations on this property are blocked until the "+
				"reindex completes or is cancelled — %s",
			task.ID, existP.MigrationType,
			existP.Collection, propertyName, task.Status,
			MutationRemedy(task.Status, "wait for the task to reach a terminal state, or "+
				"cancel it via the reindex REST API before retrying"))
	}
	if msg := ReindexPartialRunConflict(existingTasks, className, propertyName, nil, false); msg != "" {
		return fmt.Errorf("%s", msg)
	}
	return nil
}

// CheckClassMutation implements
// [distributedtask.SchemaMutationDetector] for class-wide
// destructive mutations (DeleteClass). Stricter than
// CheckPropertyUpdate — any reindex task on the class (regardless of
// which property) is a conflict, because dropping the class destroys
// every property's bucket state at once including the in-flight
// migration's working dirs and canonical bucket pointers.
//
// Class-wide blast radius: DeleteClass arriving mid-reindex is the
// catastrophic extension of the per-property bucket↔schema inversion
// — it destroys every property's bucket state at once.
//
// Same FSM-determinism contract as CheckPropertyUpdate. Unparseable
// in-flight payloads are treated as a hard reject (we cannot prove
// non-conflict).
func (ReindexDetector) CheckClassMutation(className string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		// Same in-flight semantics as CheckConflict.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			return fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; "+
					"cannot verify whether DeleteClass on %s would "+
					"conflict: %w",
				task.ID, className, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			return fmt.Errorf(
				"in-flight reindex task %q has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether DeleteClass on "+
					"%s would conflict",
				task.ID, className)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"reindex task %q (%s) is in flight on %s (status=%s); "+
				"deleting this class would destroy the migration's "+
				"working state and produce a bucket↔schema inversion "+
				"on every replica — %s",
			task.ID, existP.MigrationType, existP.Collection, task.Status,
			MutationRemedy(task.Status,
				"cancel the reindex via the REST API before deleting the class"))
	}
	return nil
}

// CheckTenantMutation implements
// [distributedtask.SchemaMutationDetector] for tenant-level
// mutations that make tenant shards locally unavailable
// (DeleteTenants, UpdateTenants transitioning away from ACTIVE).
//
// Today's reindex task payload names a collection but not a specific
// tenant — a migration submitted on a multi-tenant collection
// applies to whatever shards exist for that collection. So the
// conservative implementation is "block every tenant mutation on a
// class with any in-flight reindex": if a reindex is running on the
// class, we cannot prove the tenant being mutated is not part of
// its working set without a more granular payload.
//
// Same FSM-determinism contract as CheckPropertyUpdate.
//
// `tenants` is informational — the rejection error names them so
// the caller knows which tenants would be affected.
func (ReindexDetector) CheckTenantMutation(className string, tenants []string, existingTasks []*distributedtask.Task) error {
	for _, task := range existingTasks {
		// Same in-flight semantics as CheckConflict.
		if !task.Status.IsActive() {
			continue
		}

		var existP ReindexTaskPayload
		if err := json.Unmarshal(task.Payload, &existP); err != nil {
			return fmt.Errorf(
				"in-flight reindex task %q has an unparseable payload; "+
					"cannot verify whether tenant mutation on %s/%v "+
					"would conflict: %w",
				task.ID, className, tenants, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			return fmt.Errorf(
				"in-flight reindex task %q has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether tenant "+
					"mutation on %s/%v would conflict",
				task.ID, className, tenants)
		}
		if !strings.EqualFold(existP.Collection, className) {
			continue
		}
		return fmt.Errorf(
			"reindex task %q (%s) is in flight on %s (status=%s); "+
				"mutating tenants %v would make their shards locally "+
				"unavailable and produce a bucket↔schema inversion — %s",
			task.ID, existP.MigrationType, existP.Collection,
			task.Status, tenants,
			MutationRemedy(task.Status,
				"cancel the reindex via the REST API before mutating these tenants"))
	}
	return nil
}
