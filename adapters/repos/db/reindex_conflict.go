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
)

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
func (p *ReindexProvider) CheckConflict(newPayload []byte, existingTasks []*distributedtask.Task) error {
	var newP ReindexTaskPayload
	if err := json.Unmarshal(newPayload, &newP); err != nil {
		return fmt.Errorf("unmarshal new reindex payload: %w", err)
	}
	if newP.Collection == "" || newP.MigrationType == "" {
		return fmt.Errorf("new reindex payload missing Collection or MigrationType")
	}

	for _, task := range existingTasks {
		// Every non-terminal status counts as in-flight (via
		// [distributedtask.TaskStatus.IsActive]). PREPARING and SWAPPING
		// are the subtle ones: every unit has reached terminal state, but
		// the post-completion callbacks (per-node PREP, cluster-wide
		// PrepCompleteAck barrier, per-node swap, cluster-wide schema
		// flip) have not yet committed. Submitting a new migration on the
		// same property during either window could land before
		// MarkDistributedTaskFinalized commits the schema flip, leaving
		// the new task and the unfinished swap of the prior one racing on
		// the same bucket pointers.
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

// typesConflictReason returns a non-empty reason string if two reindex
// migrations on the same collection target overlapping properties.
//
// Earlier versions allowed parallel migrations as long as they wrote
// to different bucket types (e.g. enable-filterable + enable-rangeable
// on the same property). That was a real Sev 1: when one of those
// migrations completed, its OnMigrationComplete fired an
// UpdateProperty RAFT command whose MergeProps preserved the
// still-false sibling flag (the other migration hasn't flipped its
// flag yet). On apply, Migrator.UpdateProperty →
// Shard.updatePropertyBuckets ran cleanStaleMigrationDirs for every
// index whose flag was now false, removing the in-flight migration's
// .migrations/<dir>/ working directory and causing the next
// markProgress to fail with "progress.mig.000000001: no such file or
// directory" → task FAILED. https://github.com/weaviate/weaviate/issues/10675 frontend repro on
// parallel enable-filterable + enable-rangeable hit this.
//
// Closing the window at submit time is correct: reject any new task
// whose property set overlaps an in-flight task's property set, so the
// caller gets a clean conflict error and can serialize the operations.
// Empty props means "all properties" (reserved for a future
// whole-collection rebuild) and overlaps with everything.
func typesConflictReason(newType ReindexMigrationType, newProps []string,
	existType ReindexMigrationType, existProps []string,
) string {
	// Sanity-check the migration types via the exhaustive bucket-touch
	// predicates so an unknown ReindexMigrationType still panics
	// loudly at the conflict-check boundary rather than slipping
	// through as "no conflict". Result values are intentionally
	// discarded — the conflict rule below does not depend on which
	// buckets are touched, only that both types are known.
	_ = TouchesSearchable(newType)
	_ = TouchesFilterable(newType)
	_ = TouchesSearchable(existType)
	_ = TouchesFilterable(existType)

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

// TouchesSearchable reports whether migration type t writes to the
// searchable bucket. Implemented as an exhaustive switch so that a
// newly-added [ReindexMigrationType] cannot silently be treated as
// "doesn't touch searchable" — the default case panics with a clear
// message, surfacing the gap on the first request that exercises the
// new type. This matters because [typesConflictReason] relies on
// these answers (via the sanity-check at its entry) to gate
// concurrent reindex submissions: a positive-list miss would allow
// conflicting writes to the same bucket through.
func TouchesSearchable(t ReindexMigrationType) bool {
	switch t {
	case ReindexTypeChangeAlgorithm,
		ReindexTypeChangeTokenization,
		ReindexTypeEnableSearchable,
		ReindexTypeRebuildSearchable:
		return true
	case ReindexTypeRepairFilterable,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableFilterable,
		ReindexTypeEnableRangeable,
		ReindexTypeRepairRangeable:
		return false
	default:
		panic(fmt.Sprintf("TouchesSearchable: unknown ReindexMigrationType %q — add it to this switch", t))
	}
}

// TouchesFilterable reports whether migration type t writes to the
// filterable bucket. Same exhaustive-switch contract as
// [TouchesSearchable].
func TouchesFilterable(t ReindexMigrationType) bool {
	switch t {
	case ReindexTypeRepairFilterable,
		ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableFilterable:
		return true
	case ReindexTypeChangeAlgorithm,
		ReindexTypeEnableSearchable,
		ReindexTypeRebuildSearchable,
		ReindexTypeEnableRangeable,
		ReindexTypeRepairRangeable:
		return false
	default:
		panic(fmt.Sprintf("TouchesFilterable: unknown ReindexMigrationType %q — add it to this switch", t))
	}
}

// ReindexTargetIndexes lists the index keys the cancel endpoint accepts for
// a migration type, or nil for a type this build does not know. Same mapping
// as migrationTypeTargetsIndex in the REST handlers, which decides whether a
// cancel request matches a task; a test in that package pins the two together.
func ReindexTargetIndexes(t ReindexMigrationType) []string {
	switch t {
	case ReindexTypeEnableSearchable, ReindexTypeChangeAlgorithm,
		ReindexTypeRebuildSearchable:
		return []string{"searchable"}
	case ReindexTypeEnableFilterable, ReindexTypeRepairFilterable,
		ReindexTypeChangeTokenizationFilterable:
		return []string{"filterable"}
	case ReindexTypeEnableRangeable, ReindexTypeRepairRangeable:
		return []string{"rangeable"}
	case ReindexTypeChangeTokenization:
		return []string{"searchable", "filterable"}
	}
	return nil
}

// ReindexCancelCall renders a request that cancels the task described by p,
// or "" when this build cannot name one.
//
// A caller that gets "" must print no URL at all. The cancel endpoint matches
// a task only when collection, property and index key all line up, and
// answers 202 with Status NO_OP otherwise — so a placeholder an operator has
// to guess buys them a success-shaped response for a task that is still
// running. The three inputs that can be missing:
//
//   - Collection: nothing to address the request to.
//   - Properties: the endpoint matches on one named property, so a task with
//     none (the reserved whole-collection rebuild) cannot be cancelled
//     through it at all.
//   - MigrationType: a type this build cannot map has no index key, and the
//     endpoint's own matcher will not recognize it either.
//
// One property and one index key are named even when the task touches
// several. Cancel is task-scoped, not property-scoped: the handler matches on
// any one of the task's properties and any index key the migration targets,
// then cancels the whole task, and the post-terminal sweep cleans every
// property it touched. So naming the first of each cancels all of it.
func ReindexCancelCall(p ReindexTaskPayload) string {
	indexes := ReindexTargetIndexes(p.MigrationType)
	if p.Collection == "" || len(p.Properties) == 0 || len(indexes) == 0 {
		return ""
	}
	return fmt.Sprintf(`PUT /v1/schema/%s/indexes/%s {"%s":{"cancel":true}}`,
		p.Collection, p.Properties[0], indexes[0])
}

// ReindexGateRemedy is the closing sentence every reindex schema gate ends
// with, in one place so the refusals cannot drift apart. Exported because the
// REST property-mutation pre-check answers the same question one hop before
// the RAFT apply path reaches the gates below, and the two must agree.
//
// Cancel works for every status the gates block on: both the REST handler's
// findCancelTarget and DTM's Manager.CancelTask refuse only a terminal task,
// so STARTED, PREPARING and SWAPPING are all cancellable. What differs is
// what cancel does, which is why the sentence is status-aware.
//
// In PREPARING and SWAPPING the per-shard work is already done and the task
// is in the scheduler's coordination phases. Cancelling there stops the
// cluster-wide schema flip (OnTaskCompleted runs it only for SWAPPING) and
// each node clears the partial state it can. Shards whose swap already
// committed keep their new buckets while the schema stays pre-migration, so
// the property may need a rebuild afterwards — unlike the FAILED path, a
// cancel logs no repair guidance.
//
// The one case with no remedy to name is a task [ReindexCancelCall] cannot
// address, which for the most part means the endpoint cannot match it either
// — see that function for which input is missing and why.
//
// A status this build does not know reaches here too: the gates admit
// anything [distributedtask.TaskStatus.IsActive] accepts, which is every
// status that is not terminal, so a newer node's status during a rolling
// upgrade lands in the default arm. It gets a sentence that claims nothing
// about cancel either way.
//
// cancelCall is [ReindexCancelCall] for the offending task, or "" when the
// caller cannot name one.
func ReindexGateRemedy(status distributedtask.TaskStatus, cancelCall string) string {
	started := status == distributedtask.TaskStatusStarted
	if !started && !status.IsCoordinationPhase() {
		return "this build does not know that status, most likely because a " +
			"newer node reported it, so it cannot tell you whether cancel " +
			"still applies; read the task on a node that knows the status"
	}
	if cancelCall == "" {
		return "the cancel endpoint is keyed on one collection, property and " +
			"index type, and this task names none this build can fill in, " +
			"so it can only be waited out"
	}
	if started {
		return "cancel it via " + cancelCall + ", or wait for it to finish"
	}
	return "cancel it via " + cancelCall + " — its per-shard work is already " +
		"done, so cancelling now skips the schema change and leaves the " +
		"property needing a rebuild — or wait for it to finish"
}

// CheckPropertyUpdate implements
// [distributedtask.SchemaMutationDetector] for the reindex namespace.
// Called from the schema FSM's UpdateProperty apply path under
// [Manager.mu] to reject external property mutations while a reindex
// migration on the same (collection, property) is in any non-terminal
// state (STARTED, PREPARING, or SWAPPING).
//
// Motivating failure mode: a `change-tokenization` migration spawns
// separate per-shard sub-tasks for the searchable and filterable
// indexes. A DELETE `/index/searchable` arriving mid-flight applies
// `cleanStaleMigrationDirs("<prop>", "searchable")`, which wipes the
// searchable sub-task's working dir under the still-running
// runtimeSwap → searchable sub-unit FAILs → sibling filterable
// sub-unit commits its local swap → per-shard ack barrier sees mixed
// acks → task FAILED → `flipSemanticMigrationSchema` skipped →
// schema stays at OLD tokenization while the filterable bucket on
// disk holds NEW-tokenized data. Bucket↔schema inversion — same
// family as the ack-barrier failure mode but triggered by an external
// schema mutation instead of a crash.
//
// Rule: blanket reject any property mutation overlapping an in-flight
// reindex task's properties on the same collection. Migration-driven
// schema flips bypass this guard via
// [api.UpdatePropertyRequest.FromInFlightMigration] = true, set by
// [Raft.UpdatePropertyFromMigration] from the scheduler's
// OnTaskCompleted dispatch. So the guard rejects external mutations
// without breaking the migration's own scheduled completion flip.
//
// FSM-determinism: pure function of (className, propertyName,
// existingTasks). Unparseable in-flight payloads are treated as a
// hard reject (same as [ConflictDetector.CheckConflict]) — the
// alternative (silently allow) would let a real bucket-level conflict
// slip through and re-open the race this guard exists to close.
func (p *ReindexProvider) CheckPropertyUpdate(className, propertyName string, existingTasks []*distributedtask.Task) error {
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
				"reindex reaches a terminal state — %s",
			task.ID, existP.MigrationType,
			existP.Collection, propertyName, task.Status,
			ReindexGateRemedy(task.Status, ReindexCancelCall(existP)))
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
func (p *ReindexProvider) CheckClassMutation(className string, existingTasks []*distributedtask.Task) error {
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
			ReindexGateRemedy(task.Status, ReindexCancelCall(existP)))
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
func (p *ReindexProvider) CheckTenantMutation(className string, tenants []string, existingTasks []*distributedtask.Task) error {
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
			ReindexGateRemedy(task.Status, ReindexCancelCall(existP)))
	}
	return nil
}
