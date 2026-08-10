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
	"slices"
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
//
// Fails closed on an unrecognized migration type (e.g. a newer node during a
// rolling upgrade): this runs on the RAFT apply path, replayed on restart on
// every node, so panicking would crash-loop the cluster instead of just
// rejecting a request. Overlap is checked first since it depends only on
// property sets, which stay correct regardless of type. Safe only because
// every overlap currently conflicts; a future "these types are compatible"
// exception would make an older node reject what a newer one accepts,
// splitting the FSM.
func typesConflictReason(newType ReindexMigrationType, newProps []string,
	existType ReindexMigrationType, existProps []string,
) string {
	if !ReindexPropsOverlap(newProps, existProps) {
		return ""
	}
	if unknown := firstUnknownMigrationType(newType, existType); unknown != "" {
		return fmt.Sprintf(
			"already running %s for overlapping properties; migration type %q "+
				"is not known to this build (most likely submitted by a newer "+
				"node), so it cannot be proven safe to run alongside — wait for "+
				"the in-flight task to finish, or retry on a node that knows the type",
			existType, unknown)
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
// used by the REST handlers' pre-flight conflict check. Inline so the
// internal caller (CheckConflict) continues to use the lowercase symbol
// without indirection.
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

// ReindexTargetIndexes lists the inverted-index keys a migration type writes
// to, or nil for an unknown type.
//
// Single source of truth for that mapping: [ReindexCancelCall],
// [firstUnknownMigrationType], the REST cancel matcher and submit-time
// cleanup, and the two disk-deleting cleanup paths (autoCleanupAfterTerminal,
// the restart orphan audit) all read it. A mismatch between them risks
// silent data loss, so new migration types are added only here.
//
// Not semanticMigrationIndexTypes (reindex_provider.go), which answers a
// different question — which types cross the swap barrier — and returns
// nil for format-only types.
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

// firstUnknownMigrationType returns the first of ts this build does not
// recognize, or "" when it knows them all. "Known" is defined as "named by
// [ReindexTargetIndexes]", which keeps the single-source-of-truth mapping
// the only place a new type has to be registered.
func firstUnknownMigrationType(ts ...ReindexMigrationType) ReindexMigrationType {
	for _, t := range ts {
		if len(ReindexTargetIndexes(t)) == 0 {
			return t
		}
	}
	return ""
}

// ReindexCancelCall renders a request that cancels the task described by p,
// or "" when this build cannot name one. Callers must not print a
// placeholder for "": the cancel endpoint 202s with NO_OP on a non-matching
// request, so a guessed URL would look like success for a still-running task.
//
// "" happens when p.Collection is empty, p.Properties is empty (reserved
// whole-collection rebuild — no shipping route produces this today), or
// p.MigrationType maps to no index via [ReindexTargetIndexes].
//
// The collection segment keeps its namespace-qualified prefix as stored: a
// global operator (the only server-log reader) needs it to reach the right
// collection, and [namespacing.StripErrorMessage] already strips a confined
// caller's own prefix from the whole message on the REST path.
//
// askedProperty is named when the task carries it, falling back to the
// first property otherwise — cancel is task-scoped, so which one is named
// doesn't change what gets cancelled.
func ReindexCancelCall(p ReindexTaskPayload, askedProperty string) string {
	indexes := ReindexTargetIndexes(p.MigrationType)
	if p.Collection == "" || len(p.Properties) == 0 || len(indexes) == 0 {
		return ""
	}
	return fmt.Sprintf(`PUT /v1/schema/%s/indexes/%s {"%s":{"cancel":true}}`,
		p.Collection, reindexNamedProperty(p, askedProperty), indexes[0])
}

// ReindexRebuildCall renders the request that repairs the property after a
// cancel left the buckets ahead of the schema, or "" when this build cannot
// name one. Same rendering rules as [ReindexCancelCall].
//
// The body rebuilds every index the migration could have torn: the gate
// cannot tell which shards got how far, and rebuild is idempotent on a
// healthy index.
func ReindexRebuildCall(p ReindexTaskPayload, askedProperty string) string {
	if p.Collection == "" || len(p.Properties) == 0 ||
		len(ReindexTargetIndexes(p.MigrationType)) == 0 {
		return ""
	}
	return fmt.Sprintf("PUT /v1/schema/%s/indexes/%s %s",
		p.Collection, reindexNamedProperty(p, askedProperty),
		reindexRepairBody(p.MigrationType))
}

// reindexNamedProperty picks the property a rendered call should name:
// askedProperty when the task actually carries it, the first one otherwise.
func reindexNamedProperty(p ReindexTaskPayload, askedProperty string) string {
	if slices.Contains(p.Properties, askedProperty) {
		return askedProperty
	}
	return p.Properties[0]
}

// ReindexGateRemedy is the closing sentence every reindex schema gate (RAFT
// apply path and REST pre-check) appends to its refusal, kept in one place
// so the two can't drift. Not used by the backup gate, which is shard-keyed
// and never sees the task (its wording is in reindexInFlightError).
//
// p is the offending task's payload; askedProperty is the property named in
// the caller's request, or "" when the refusal isn't property-scoped
// (DeleteClass, tenant mutations).
//
// The sentence depends on status and on [IsSemanticMigration]:
//   - unrecognized status (e.g. a newer node's, or terminal — no caller
//     exercises terminal since every gate pre-filters on
//     [distributedtask.TaskStatus.IsActive]): claims nothing about cancel.
//   - no cancel call nameable: task can only be waited out.
//   - STARTED on a semantic (barrier) migration: nothing is on disk yet, so
//     cancel or wait are both safe.
//   - STARTED on a format-only migration: PREPARING never exists
//     ([distributedtask.Task.NeedsPreparationBarrier] == [IsSemanticMigration]),
//     so per-shard swaps can already have committed while status still
//     reads STARTED — cancelling leaves some shards rebuilt, others not.
//   - PREPARING/SWAPPING: steer toward waiting and name the rebuild cancel
//     makes necessary. The window opens at the MERGE, not the swap:
//     [FinalizeCompletedMigrations] promotes on merged.mig alone, which
//     runtimePrepare writes during PREPARING before any shard swaps — so a
//     PREPARING-time cancel already leaves promotion-eligible data (and,
//     since only the swap sets [Shard.tokenizationOverlay], there is no
//     in-memory mask to lose either way). Cancel does not roll this back:
//     CleanStalePartialReindexState deliberately preserves a merged/tidied
//     generation (wiping it is #10675-shape data loss), so the next restart
//     promotes it into the same bucket↔schema inversion [CheckClassMutation]
//     calls catastrophic. Tracked at weaviate/weaviate#12575.
func ReindexGateRemedy(status distributedtask.TaskStatus, p ReindexTaskPayload, askedProperty string) string {
	started := status == distributedtask.TaskStatusStarted
	if !started && !status.IsCoordinationPhase() {
		return "this build does not know that status, most likely because a " +
			"newer node reported it, so it cannot tell you whether cancel " +
			"still applies; read the task on a node that knows the status"
	}
	cancelCall := ReindexCancelCall(p, askedProperty)
	if cancelCall == "" {
		return "the cancel endpoint is keyed on one collection, property and " +
			"index type, and this task names none this build can fill in, " +
			"so it can only be waited out"
	}
	if started && IsSemanticMigration(p.MigrationType) {
		return "cancel it via " + cancelCall + ", or wait for it to finish"
	}
	rebuildCall := ReindexRebuildCall(p, askedProperty)
	if started {
		return "cancel it via " + cancelCall + ", or wait for it to finish; " +
			"this migration has no cluster-wide cutover, so its shards commit " +
			"one by one while it still reads STARTED and cancelling leaves the " +
			"ones that already finished rebuilt and the rest untouched — " +
			"re-submit the same request to finish the remainder"
	}
	return "wait for it to finish: its per-shard work is already merged on " +
		"disk, so cancelling now skips the schema change but does not drop " +
		"that data, and the next restart promotes it — which leaves those " +
		"buckets holding the new format under the old schema, repairable " +
		"only by rebuilding the property via " + rebuildCall +
		" — if you accept that, cancel it via " + cancelCall
}

// CheckPropertyUpdate implements
// [distributedtask.SchemaMutationDetector] for the reindex namespace.
// Called from the schema FSM's UpdateProperty apply path under
// [Manager.mu] to reject external property mutations while a reindex
// migration on the same (collection, property) is in any non-terminal
// state — per [distributedtask.TaskStatus.IsActive], so STARTED,
// PREPARING, SWAPPING, and any status this build does not recognize
// because a newer node reported it.
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
			// Task ID withheld: an unreadable payload also hides which
			// namespace the task belongs to.
			return fmt.Errorf(
				"an in-flight reindex task has an unparseable payload; "+
					"cannot verify whether property update on %s.%s would "+
					"conflict (see GET /v1/tasks): %w",
				className, propertyName, err)
		}
		if existP.Collection == "" || existP.MigrationType == "" {
			// Task ID withheld for the same reason as above.
			return fmt.Errorf(
				"an in-flight reindex task has empty Collection or "+
					"MigrationType (payload may have been written by an "+
					"older binary); cannot verify whether property update "+
					"on %s.%s would conflict (see GET /v1/tasks)",
				className, propertyName)
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
			ReindexGateRemedy(task.Status, existP, propertyName))
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
			ReindexGateRemedy(task.Status, existP, ""))
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
			ReindexGateRemedy(task.Status, existP, ""))
	}
	return nil
}
