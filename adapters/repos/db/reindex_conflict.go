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
// Returns a non-nil error if `newPayload` would conflict with an
// already-STARTED task in `existingTasks`, or if either side names a
// migration type this build does not know. Rejecting the task is the
// only safe response to an unknown type here: this runs inside the
// RAFT FSM apply, where a panic takes down every node applying the
// entry and then replays on restart.
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
		// PREPARING and SWAPPING both count as in-flight (via
		// [distributedtask.TaskStatus.IsActive]): every unit has reached
		// terminal state, but the post-completion callbacks (per-node
		// PREP, cluster-wide PrepCompleteAck barrier, per-node swap,
		// cluster-wide schema flip) have not yet committed. Submitting
		// a new migration on the same property during either window
		// could land before MarkDistributedTaskFinalized commits the
		// schema flip, leaving the new task and the unfinished swap of
		// the prior one racing on the same bucket pointers.
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

		reason, err := typesConflictReason(newP.MigrationType, newP.Properties,
			existP.MigrationType, existP.Properties)
		if err != nil {
			return fmt.Errorf("reindex task %q: %w", task.ID, err)
		}
		if reason != "" {
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
) (string, error) {
	// Reject rather than guess if either side names a type this build
	// does not classify. "No conflict" would be the dangerous default:
	// it lets a second migration race the in-flight one on shared
	// on-disk state.
	if err := ValidateReindexMigrationType(newType); err != nil {
		return "", fmt.Errorf("new migration type: %w", err)
	}
	if err := ValidateReindexMigrationType(existType); err != nil {
		return "", fmt.Errorf("in-flight migration type: %w", err)
	}

	if !ReindexPropsOverlap(newProps, existProps) {
		return "", nil
	}
	if newType == existType {
		return fmt.Sprintf("already running %s for overlapping properties", newType), nil
	}
	return fmt.Sprintf("already running %s for overlapping properties; "+
		"concurrent %s on the same property would race on shared on-disk "+
		"migration state — wait for the in-flight task to finish before "+
		"submitting another", existType, newType), nil
}

// TypesConflictReason is the package-public alias for typesConflictReason,
// used by the REST handlers' pre-flight conflict check. Inline so
// internal callers (CheckConflict, CheckPropertyUpdate) continue to use
// the lowercase symbol without indirection.
func TypesConflictReason(newType ReindexMigrationType, newProps []string,
	existType ReindexMigrationType, existProps []string,
) (string, error) {
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

// reindexBucketTouch records which inverted-index buckets one migration
// type writes to.
type reindexBucketTouch struct {
	searchable bool
	filterable bool
}

// reindexBucketTouches classifies t, reporting ok=false for a migration
// type this build does not know — a payload written by a newer binary
// after a downgrade, or a constant added without a case below.
//
// The switch has no default arm on purpose. That is what makes the
// `exhaustive` linter (enabled repo-wide) fail the build when a new
// [ReindexMigrationType] constant is declared and left unclassified.
// The repo runs `exhaustive` with default-signifies-exhaustive: true,
// so adding a default arm here would silently switch the check off and
// put us back to discovering the gap at RAFT-apply time. Every
// constant therefore gets its own case, listing both bucket answers
// where the reader can see them together.
func reindexBucketTouches(t ReindexMigrationType) (reindexBucketTouch, bool) {
	switch t {
	case ReindexTypeChangeAlgorithm:
		// Map (WAND) → Inverted (BlockMax) rewrite of the searchable bucket.
		return reindexBucketTouch{searchable: true}, true
	case ReindexTypeRebuildSearchable:
		// Rebuilds an existing BlockMax searchable bucket in place from
		// the objects store, preserving algorithm and tokenization.
		return reindexBucketTouch{searchable: true}, true
	case ReindexTypeEnableSearchable:
		return reindexBucketTouch{searchable: true}, true
	case ReindexTypeChangeTokenization:
		// Retokenizes both buckets of a text property.
		return reindexBucketTouch{searchable: true, filterable: true}, true
	case ReindexTypeChangeTokenizationFilterable:
		return reindexBucketTouch{filterable: true}, true
	case ReindexTypeEnableFilterable:
		return reindexBucketTouch{filterable: true}, true
	case ReindexTypeRepairFilterable:
		return reindexBucketTouch{filterable: true}, true
	case ReindexTypeEnableRangeable:
		// Rangeable is its own bucket family; neither answer applies.
		return reindexBucketTouch{}, true
	case ReindexTypeRepairRangeable:
		return reindexBucketTouch{}, true
	}
	return reindexBucketTouch{}, false
}

// ValidateReindexMigrationType returns a non-nil error if t is not a
// migration type this build knows how to classify. Callers on the RAFT
// apply path use this to reject the task instead of crashing the FSM.
func ValidateReindexMigrationType(t ReindexMigrationType) error {
	if _, ok := reindexBucketTouches(t); !ok {
		return fmt.Errorf("unknown reindex migration type %q", t)
	}
	return nil
}

// TouchesSearchable reports whether migration type t writes to the
// searchable bucket. The error is non-nil for an unknown type; the
// bool is then meaningless, so callers must not fall back to it — a
// silent "false" would let two migrations race on the same bucket.
func TouchesSearchable(t ReindexMigrationType) (bool, error) {
	c, ok := reindexBucketTouches(t)
	if !ok {
		return false, fmt.Errorf("unknown reindex migration type %q", t)
	}
	return c.searchable, nil
}

// TouchesFilterable reports whether migration type t writes to the
// filterable bucket. Same error contract as [TouchesSearchable].
func TouchesFilterable(t ReindexMigrationType) (bool, error) {
	c, ok := reindexBucketTouches(t)
	if !ok {
		return false, fmt.Errorf("unknown reindex migration type %q", t)
	}
	return c.filterable, nil
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
				"reindex completes or is cancelled — wait for the task "+
				"to reach a terminal state, or cancel it via the reindex "+
				"REST API before retrying",
			task.ID, existP.MigrationType,
			existP.Collection, propertyName, task.Status)
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
				"on every replica — cancel the reindex via the REST "+
				"API before deleting the class",
			task.ID, existP.MigrationType, existP.Collection, task.Status)
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
				"unavailable and produce a bucket↔schema inversion — "+
				"cancel the reindex via the REST API before mutating "+
				"these tenants",
			task.ID, existP.MigrationType, existP.Collection,
			task.Status, tenants)
	}
	return nil
}
